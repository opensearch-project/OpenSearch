/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Page-level pruning using parquet page statistics.
//!
//! Thin wrapper around DataFusion's [`PruningPredicate`] and a
//! **multi-column** per-RG page-stats adapter. Replaces the previous
//! homegrown per-filter range-intersection logic, which silently dropped
//! unsupported expression shapes and could mis-prune `OR(...)` inside a
//! conjunct.
//!
//! # Correctness
//!
//! `PruningPredicate` rewrites the full boolean tree homomorphically:
//! - `a = v` → `a_min ≤ v AND a_max ≥ v` (page could contain `v`).
//! - `AND(x, y)` → `AND(rewrite(x), rewrite(y))`.
//! - `OR(x, y)` → `OR(rewrite(x), rewrite(y))`.
//! - `NOT(x)` → `NOT(rewrite(x))` (via its own rules).
//! - `IN`, `LIKE`, `IS NULL`, etc. handled by `PruningPredicate`'s own
//!   rewriters.
//! - Anything it can't translate becomes `Literal(true)`. Safe
//!   conservative fallback: can't prune → assume page matches.
//!
//! Crucially, the rewrite preserves boolean structure, so
//! `OR(a=5, b=10)` correctly prunes a page where `a` is entirely
//! outside `{5}` AND `b` is entirely outside `{10}`. The per-page stats
//! adapter below answers stats queries for any column in the file.
//!
//! # Per-RG cost
//!
//! One `PruningPredicate::prune` call per RG. Internally evaluates the
//! rewritten expression against per-page min/max/null-count arrays;
//! each array is read once per column per predicate. `PruningPredicate`
//! itself is built once per query at [`build_pruning_predicate`].

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::Operator;
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
#[cfg(test)]
use datafusion::physical_expr::expressions::Column as PhysColumn;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};

/// Per-row-group page pruner. Owns schema + metadata references; the
/// pruning expression itself lives in a [`PruningPredicate`] built once
/// per query by [`build_pruning_predicate`].
pub struct PagePruner {
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
}

impl PagePruner {
    pub fn new(schema: &SchemaRef, metadata: Arc<ParquetMetaData>) -> Self {
        Self {
            schema: schema.clone(),
            metadata,
        }
    }

    /// Prune an RG to a [`RowSelection`] using an arbitrary boolean
    /// predicate (wrapped as a [`PruningPredicate`]).
    ///
    /// Returns:
    /// - `Some(selection)` — per-page keep/skip over the RG. An empty
    ///   selection means no page can match; a single whole-RG `select`
    ///   means every page is kept.
    /// - `None` — pruning isn't applicable (no page index on the RG,
    ///   evaluation error, etc.). Caller treats as "scan the whole RG."
    pub fn prune_rg(
        &self,
        pruning_predicate: &PruningPredicate,
        rg_idx: usize,
        metrics: Option<&PagePruneMetrics>,
    ) -> Option<RowSelection> {
        let stats = match MultiColumnPagesPruningStats::try_new(
            rg_idx,
            &self.schema,
            &self.metadata,
        ) {
            Some(s) => s,
            None => {
                if let Some(m) = metrics {
                    if let Some(ref c) = m.page_pruning_unavailable {
                        c.add(1);
                    }
                }
                return None;
            }
        };
        let keep = match pruning_predicate.prune(&stats) {
            Ok(k) => k,
            Err(e) => {
                log::debug!("page pruning skipped for rg {}: {}", rg_idx, e);
                if let Some(m) = metrics {
                    if let Some(ref c) = m.page_pruning_unavailable {
                        c.add(1);
                    }
                }
                return None;
            }
        };
        if keep.len() != stats.page_row_counts.len() {
            log::debug!(
                "page pruning skipped for rg {}: keep len {} != page count {}",
                rg_idx,
                keep.len(),
                stats.page_row_counts.len()
            );
            if let Some(m) = metrics {
                if let Some(ref c) = m.page_pruning_unavailable {
                    c.add(1);
                }
            }
            return None;
        }
        if let Some(m) = metrics {
            let pruned = keep.iter().filter(|k| !**k).count();
            let total = keep.len();
            if let Some(ref c) = m.pages_pruned {
                c.add(pruned);
            }
            if let Some(ref c) = m.pages_total {
                c.add(total);
            }
        }
        Some(to_row_selection(keep, &stats.page_row_counts))
    }
}

/// Per-call counter bundle for [`PagePruner::prune_rg`]. Callers with
/// `StreamMetrics` build one via [`PagePruneMetrics::from_stream_metrics`].
#[derive(Default, Clone)]
pub struct PagePruneMetrics {
    pub pages_pruned: Option<datafusion::physical_plan::metrics::Count>,
    pub pages_total: Option<datafusion::physical_plan::metrics::Count>,
    pub page_pruning_unavailable: Option<datafusion::physical_plan::metrics::Count>,
}

impl PagePruneMetrics {
    pub fn from_stream_metrics(
        sm: &crate::indexed_table::metrics::StreamMetrics,
    ) -> Self {
        Self {
            pages_pruned: sm.pages_pruned.clone(),
            pages_total: sm.pages_total.clone(),
            page_pruning_unavailable: sm.page_pruning_unavailable.clone(),
        }
    }
}

/// Build an [`PruningPredicate`] from an arbitrary physical boolean
/// expression. Returns `None` for always-true predicates (nothing to
/// prune) or translation failures (safe fallback: no pruning).
///
/// Use for the multi-filter tree path's whole residual subtree or for
/// the single-collector path's residual (non-Collector portion).
pub fn build_pruning_predicate(
    expr: &Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
) -> Option<Arc<PruningPredicate>> {
    let pruning_predicate = match PruningPredicate::try_new(Arc::clone(expr), schema) {
        Ok(pp) => pp,
        Err(e) => {
            log::debug!(
                "PruningPredicate::try_new failed for {:?}: {}",
                expr,
                e
            );
            return None;
        }
    };
    if pruning_predicate.always_true() {
        log::trace!("PruningPredicate collapsed to always_true for {:?}", expr);
        return None;
    }
    Some(Arc::new(pruning_predicate))
}

/// Lower our `BoolNode` tree to an `Arc<dyn PhysicalExpr>` for
/// [`PruningPredicate`]. Every `Collector` leaf is replaced with
/// `Literal(true)` — Collectors constrain only the backend's bitset
/// view of the data, not the parquet page contents, so for page
/// pruning they're always-true placeholders.
///
/// NOT handling: DataFusion's `PruningPredicate` only rewrites `NotExpr`
/// when it wraps a bare `Column`; `NOT(OR(...))`, `NOT(a > 5)` etc.
/// fall through to `Literal(true)`, collapsing the whole predicate to
/// always-true. We preempt that by pushing NOT down De Morgan-style
/// and, at the leaves, flipping the comparison operator when the leaf
/// is a recognizable `BinaryExpr(col, op, literal)`. For leaves we
/// can't invert, the NOT becomes `NotExpr` — DataFusion will replace
/// that with `Literal(true)` later and the surrounding expression
/// degrades gracefully.
pub fn bool_tree_to_pruning_expr(
    tree: &crate::indexed_table::bool_tree::BoolNode,
    _schema: &SchemaRef,
) -> Option<Arc<dyn PhysicalExpr>> {
    use crate::indexed_table::bool_tree::BoolNode;
    use datafusion::physical_expr::expressions::NotExpr;

    fn lit_true() -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
    }

    fn fold_and_or(op: Operator, mut exprs: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
        let mut acc = exprs.remove(0);
        for e in exprs {
            acc = Arc::new(BinaryExpr::new(acc, op, e));
        }
        acc
    }

    fn negate_op(op: Operator) -> Option<Operator> {
        Some(match op {
            Operator::Eq => Operator::NotEq,
            Operator::NotEq => Operator::Eq,
            Operator::Lt => Operator::GtEq,
            Operator::LtEq => Operator::Gt,
            Operator::Gt => Operator::LtEq,
            Operator::GtEq => Operator::Lt,
            _ => return None,
        })
    }

    /// Negate a `PhysicalExpr` cheaply when it's a recognizable
    /// comparison (`col op literal`), otherwise wrap with `NotExpr`.
    fn negate_expr(expr: &Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        if let Some(bin) = expr
            .as_any()
            .downcast_ref::<BinaryExpr>()
        {
            if let Some(flipped) = negate_op(*bin.op()) {
                return Arc::new(BinaryExpr::new(
                    Arc::clone(bin.left()),
                    flipped,
                    Arc::clone(bin.right()),
                ));
            }
        }
        Arc::new(NotExpr::new(Arc::clone(expr)))
    }

    /// Walk the `BoolNode`. The `negate` flag tracks whether an odd
    /// number of NOTs surround this node; we push it through
    /// AND/OR/Predicate/Collector by flipping structure. This keeps
    /// the output mostly `NotExpr`-free so `PruningPredicate::try_new`
    /// can actually rewrite the result.
    fn walk(
        node: &crate::indexed_table::bool_tree::BoolNode,
        negate: bool,
    ) -> Arc<dyn PhysicalExpr> {
        match node {
            // NOT(Collector) is also `true` (Collector is conservative
            // `true` at the page-prune layer; NOT of that is still
            // conservative `true`).
            BoolNode::Collector { .. } => lit_true(),
            BoolNode::Predicate(expr) => {
                if negate {
                    negate_expr(expr)
                } else {
                    Arc::clone(expr)
                }
            }
            BoolNode::Not(child) => walk(child, !negate),
            BoolNode::And(children) => {
                let op = if negate { Operator::Or } else { Operator::And };
                let exprs: Vec<_> = children.iter().map(|c| walk(c, negate)).collect();
                fold_and_or(op, exprs)
            }
            BoolNode::Or(children) => {
                let op = if negate { Operator::And } else { Operator::Or };
                let exprs: Vec<_> = children.iter().map(|c| walk(c, negate)).collect();
                fold_and_or(op, exprs)
            }
        }
    }

    Some(walk(tree, false))
}

/// Convert a per-page keep/skip decision + per-page row counts into a
/// compacted `RowSelection`. Adjacent runs of the same decision are
/// merged.
fn to_row_selection(keep: Vec<bool>, row_counts: &[usize]) -> RowSelection {
    let mut out: Vec<RowSelector> = Vec::with_capacity(keep.len());
    for (k, rc) in keep.into_iter().zip(row_counts.iter().copied()) {
        let selector = if k {
            RowSelector::select(rc)
        } else {
            RowSelector::skip(rc)
        };
        match out.last_mut() {
            Some(last) if last.skip == selector.skip => {
                last.row_count += selector.row_count;
            }
            _ => out.push(selector),
        }
    }
    RowSelection::from(out)
}

// ═══════════════════════════════════════════════════════════════════════
// MultiColumnPagesPruningStats
//
// Exposes one `PruningStatistics` value that can answer min/max/nulls
// for *any* column in the file. `PruningPredicate` calls
// `stats.min_values(col)` with the column name it needs; we build the
// right `StatisticsConverter` on demand.
//
// Page-row-count is shared across all columns in an RG (parquet
// guarantees page alignment across columns). We derive it once from
// the first column's offset index and cache for subsequent calls.
// ═══════════════════════════════════════════════════════════════════════
struct MultiColumnPagesPruningStats<'a> {
    row_group_index: usize,
    schema: &'a SchemaRef,
    parquet_metadata: &'a ParquetMetaData,
    /// Per-page row counts, stable across columns in the same RG.
    page_row_counts: Vec<usize>,
}

impl<'a> MultiColumnPagesPruningStats<'a> {
    fn try_new(
        row_group_index: usize,
        schema: &'a SchemaRef,
        parquet_metadata: &'a ParquetMetaData,
    ) -> Option<Self> {
        parquet_metadata.column_index()?;
        let offset_index = parquet_metadata.offset_index()?;
        let rg_offsets = offset_index.get(row_group_index)?;
        // All columns in an RG share page boundaries — any column's
        // offset index is enough to compute per-page row counts.
        let first_col_offsets = rg_offsets.first()?.page_locations();
        if first_col_offsets.is_empty() {
            return None;
        }
        let rg_meta = parquet_metadata.row_groups().get(row_group_index)?;
        let num_rows_in_rg = rg_meta.num_rows() as usize;
        let mut page_row_counts = Vec::with_capacity(first_col_offsets.len());
        for pair in first_col_offsets.windows(2) {
            let start = pair[0].first_row_index as usize;
            let end = pair[1].first_row_index as usize;
            page_row_counts.push(end - start);
        }
        let last_start = first_col_offsets.last()?.first_row_index as usize;
        page_row_counts.push(num_rows_in_rg - last_start);
        Some(Self {
            row_group_index,
            schema,
            parquet_metadata,
            page_row_counts,
        })
    }

    fn converter_for(&self, column: &Column) -> Option<StatisticsConverter<'a>> {
        StatisticsConverter::try_new(
            column.name(),
            self.schema,
            self.parquet_metadata.file_metadata().schema_descr(),
        )
        .ok()
    }
}

impl PruningStatistics for MultiColumnPagesPruningStats<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let conv = self.converter_for(column)?;
        conv.data_page_mins(
            self.parquet_metadata.column_index()?,
            self.parquet_metadata.offset_index()?,
            [&self.row_group_index],
        )
        .ok()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let conv = self.converter_for(column)?;
        conv.data_page_maxes(
            self.parquet_metadata.column_index()?,
            self.parquet_metadata.offset_index()?,
            [&self.row_group_index],
        )
        .ok()
    }

    fn num_containers(&self) -> usize {
        self.page_row_counts.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let conv = self.converter_for(column)?;
        conv.data_page_null_counts(
            self.parquet_metadata.column_index()?,
            self.parquet_metadata.offset_index()?,
            [&self.row_group_index],
        )
        .ok()
        .map(|a| Arc::new(a) as ArrayRef)
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let arr = Int64Array::from_iter_values(self.page_row_counts.iter().map(|c| *c as i64));
        Some(Arc::new(arr) as ArrayRef)
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        // Optional bloom-filter-style hook. Not implemented; bounds
        // pruning is sufficient for our expressions.
        None
    }
}

// ─────────────────────────────────────────────────────────────────────
// Test helpers kept pub so tests_e2e can reach them without contortion.
// ─────────────────────────────────────────────────────────────────────

/// Marker type kept for backward-compat with the old
/// `build_single_column_pruning_predicates` signature (now unused).
/// Removed when callers migrate.
#[allow(dead_code)]
type _PhantomUnused = HashMap<usize, Arc<PruningPredicate>>;

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    /// 32-row parquet with two int columns, one RG, four data pages of 8
    /// rows each. Page-level stats enabled.
    fn two_col_fixture() -> (PagePruner, SchemaRef, Arc<ParquetMetaData>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
            Field::new("qty", DataType::Int32, false),
        ]));
        // prices: 0..32 (pages: 0..8, 8..16, 16..24, 24..32)
        // qtys:   100..132 (pages: 100..108, 108..116, 116..124, 124..132)
        let prices: Vec<i32> = (0..32).collect();
        let qtys: Vec<i32> = (100..132).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(prices)), Arc::new(Int32Array::from(qtys))],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        let arc_meta = meta.metadata().clone();
        let pruner = PagePruner::new(&schema, Arc::clone(&arc_meta));
        (pruner, schema, arc_meta)
    }

    fn count_rows_kept(sel: &RowSelection) -> usize {
        sel.iter().filter(|s| !s.skip).map(|s| s.row_count).sum()
    }

    fn col(name: &str, idx: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(PhysColumn::new(name, idx))
    }
    fn lit_int(v: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int32(Some(v))))
    }
    fn bin(
        l: Arc<dyn PhysicalExpr>,
        op: Operator,
        r: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(l, op, r))
    }

    #[test]
    fn single_col_eq_prunes_to_overlapping_page() {
        // price = 5: only page 0 (0..8) overlaps.
        let (pruner, schema, _) = two_col_fixture();
        let expr = bin(col("price", 0), Operator::Eq, lit_int(5));
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 8);
    }

    #[test]
    fn multi_col_and_intersects_pages() {
        // price > 20 AND qty < 110: price>20 keeps pages 2,3 (16..32);
        // qty<110 keeps page 0 (100..108). Intersection is empty.
        let (pruner, schema, _) = two_col_fixture();
        let p_gt_20 = bin(col("price", 0), Operator::Gt, lit_int(20));
        let q_lt_110 = bin(col("qty", 1), Operator::Lt, lit_int(110));
        let expr = bin(p_gt_20, Operator::And, q_lt_110);
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 0, "AND of disjoint page sets prunes everything");
    }

    #[test]
    fn multi_col_or_unions_pages() {
        // price < 5 OR qty > 125: price<5 keeps page 0 (0..8);
        // qty>125 keeps page 3 (124..132). Union keeps pages 0 and 3.
        let (pruner, schema, _) = two_col_fixture();
        let p_lt_5 = bin(col("price", 0), Operator::Lt, lit_int(5));
        let q_gt_125 = bin(col("qty", 1), Operator::Gt, lit_int(125));
        let expr = bin(p_lt_5, Operator::Or, q_gt_125);
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        // Keep 2 pages × 8 rows = 16.
        assert_eq!(count_rows_kept(&sel), 16);
    }

    #[test]
    fn multi_col_or_both_miss_prunes_everything() {
        // price < -1 OR qty > 999: neither can hold on any page.
        let (pruner, schema, _) = two_col_fixture();
        let p = bin(col("price", 0), Operator::Lt, lit_int(-1));
        let q = bin(col("qty", 1), Operator::Gt, lit_int(999));
        let expr = bin(p, Operator::Or, q);
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 0, "OR of unreachable ranges prunes everything");
    }

    #[test]
    fn nested_and_of_or_of_different_columns() {
        // (price < 5 OR qty > 125) AND price > 24
        // Left side keeps pages 0, 3; right side keeps page 3 (24..32).
        // Intersection: page 3 only → 8 rows.
        let (pruner, schema, _) = two_col_fixture();
        let left = bin(
            bin(col("price", 0), Operator::Lt, lit_int(5)),
            Operator::Or,
            bin(col("qty", 1), Operator::Gt, lit_int(125)),
        );
        let right = bin(col("price", 0), Operator::Gt, lit_int(24));
        let expr = bin(left, Operator::And, right);
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 8);
    }

    // Helper: build a `BoolNode::Predicate(expr)` from a (col, op, value).
    fn pred_leaf(
        col_name: &str,
        op: Operator,
        v: i32,
        schema: &SchemaRef,
    ) -> crate::indexed_table::bool_tree::BoolNode {
        let col_idx = schema.index_of(col_name).unwrap();
        let left: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new(col_name, col_idx));
        let right: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(v))));
        crate::indexed_table::bool_tree::BoolNode::Predicate(Arc::new(BinaryExpr::new(
            left, op, right,
        )))
    }

    #[test]
    fn not_of_or_of_different_columns() {
        // NOT(price < 5 OR qty > 125)  ≡  price >= 5 AND qty <= 125.
        // Regression: ensure `bool_tree_to_pruning_expr` pushes NOT down
        // so DataFusion can rewrite the resulting leaf comparisons.
        let (pruner, schema, _) = two_col_fixture();
        use crate::indexed_table::bool_tree::BoolNode;
        let tree = BoolNode::Not(Box::new(BoolNode::Or(vec![
            pred_leaf("price", Operator::Lt, 5, &schema),
            pred_leaf("qty", Operator::Gt, 125, &schema),
        ])));
        let expr = bool_tree_to_pruning_expr(&tree, &schema).unwrap();
        let pp = build_pruning_predicate(&expr, schema)
            .expect("NOT push-down should leave a prunable expression");
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 32);
    }

    #[test]
    fn not_push_down_eliminates_pages() {
        // NOT(price >= 10) ≡ price < 10. Page 0 always; page 1 maybe.
        let (pruner, schema, _) = two_col_fixture();
        use crate::indexed_table::bool_tree::BoolNode;
        let tree = BoolNode::Not(Box::new(pred_leaf(
            "price",
            Operator::GtEq,
            10,
            &schema,
        )));
        let expr = bool_tree_to_pruning_expr(&tree, &schema).unwrap();
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        let kept = count_rows_kept(&sel);
        assert!(kept >= 8 && kept <= 16, "expected 8–16 rows kept, got {}", kept);
    }

    #[test]
    fn not_of_not_cancels_to_identity() {
        // NOT(NOT(price = 5)) ≡ price = 5 — only page 0 matches.
        let (pruner, schema, _) = two_col_fixture();
        use crate::indexed_table::bool_tree::BoolNode;
        let tree = BoolNode::Not(Box::new(BoolNode::Not(Box::new(pred_leaf(
            "price",
            Operator::Eq,
            5,
            &schema,
        )))));
        let expr = bool_tree_to_pruning_expr(&tree, &schema).unwrap();
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 8, "double NOT cancels");
    }

    // ─────────────────────────────────────────────────────────────────
    // IN / NOT IN — DataFusion expands to OR / AND of equalities and
    // prunes homomorphically. Our substrait path doesn't emit IN today
    // but `build_pruning_predicate` accepts arbitrary PhysicalExprs, so
    // we cover it here for future callers and as a regression fence.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn in_list_prunes_via_or_of_eq() {
        // price IN (5, 15). price=5 → page 0, price=15 → page 1.
        // Pages 2, 3 skipped.
        let (pruner, schema, _) = two_col_fixture();
        let c = col("price", 0);
        let list = vec![lit_int(5), lit_int(15)];
        let expr =
            datafusion::physical_expr::expressions::in_list(c, list, &false, &schema).unwrap();
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        // Pages 0 and 1 survive, 2 and 3 pruned.
        assert_eq!(count_rows_kept(&sel), 16);
    }

    #[test]
    fn not_in_list_prunes_via_and_of_neq() {
        // price NOT IN (-100, -200) — all pages match (nothing in RG is
        // < 0), so every page kept.
        let (pruner, schema, _) = two_col_fixture();
        let c = col("price", 0);
        let list = vec![lit_int(-100), lit_int(-200)];
        let expr =
            datafusion::physical_expr::expressions::in_list(c, list, &true, &schema).unwrap();
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 32);
    }

    #[test]
    fn in_list_empty_match_prunes_everything() {
        // price IN (-10, -20, -30) — nothing in RG matches, all pages
        // prunable.
        let (pruner, schema, _) = two_col_fixture();
        let c = col("price", 0);
        let list = vec![lit_int(-10), lit_int(-20), lit_int(-30)];
        let expr =
            datafusion::physical_expr::expressions::in_list(c, list, &false, &schema).unwrap();
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 0);
    }

    // ─────────────────────────────────────────────────────────────────
    // IS NULL / IS NOT NULL — DataFusion uses null-count stats.
    // Requires a schema with nullable columns to emit useful pruning.
    // Our fixture columns are non-nullable, so null_counts are always
    // 0. We test these for safety (no crash, consistent result); real
    // pruning would need a nullable-column fixture.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn is_null_over_non_nullable_column_keeps_nothing() {
        // Fixture columns are non-nullable; IS NULL can never be true,
        // so all pages get pruned.
        use datafusion::physical_expr::expressions::IsNullExpr;
        let (pruner, schema, _) = two_col_fixture();
        let expr: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(col("price", 0)));
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 0);
    }

    #[test]
    fn is_not_null_over_non_nullable_column_keeps_everything() {
        use datafusion::physical_expr::expressions::IsNotNullExpr;
        let (pruner, schema, _) = two_col_fixture();
        let expr: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(col("price", 0)));
        let pp = build_pruning_predicate(&expr, schema);
        // May be always-true (no pruning possible) → None, or may prune
        // to keep everything → Some with 32 rows. Both are correct.
        match pp {
            None => {}
            Some(pp) => {
                let sel = pruner.prune_rg(&pp, 0, None).unwrap();
                assert_eq!(count_rows_kept(&sel), 32);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // All six comparison operators, to pin down the supported surface.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn all_six_comparison_ops_prune_correctly() {
        let (pruner, schema, _) = two_col_fixture();
        // Helper: build a comparison, evaluate, return rows kept.
        let run = |op: Operator, v: i32| -> usize {
            let expr = bin(col("price", 0), op, lit_int(v));
            let pp = build_pruning_predicate(&expr, schema.clone()).unwrap();
            let sel = pruner.prune_rg(&pp, 0, None).unwrap();
            count_rows_kept(&sel)
        };
        // price = 5 → page 0 only (8 rows).
        assert_eq!(run(Operator::Eq, 5), 8);
        // price != 5 → likely all pages (not prunable: every page has
        // values != 5). 32 rows.
        assert_eq!(run(Operator::NotEq, 5), 32);
        // price < 10 → pages 0, 1 (max of page 1 is 15, min is 8 < 10).
        // Actually: page 0 (0..7) certainly has < 10, page 1 (8..15)
        // has 8,9 < 10, so both survive. 16 rows.
        assert_eq!(run(Operator::Lt, 10), 16);
        // price <= 7 → page 0 only (max 7 ≤ 7; page 1 min 8 > 7). 8 rows.
        assert_eq!(run(Operator::LtEq, 7), 8);
        // price > 24 → page 3 (24..31, max 31 > 24). 8 rows.
        assert_eq!(run(Operator::Gt, 24), 8);
        // price >= 24 → page 3 (24..31). 8 rows.
        assert_eq!(run(Operator::GtEq, 24), 8);
    }
    #[test]
    fn always_true_predicate_yields_none() {
        let (_, schema, _) = two_col_fixture();
        // A predicate that's structurally unusable for pruning — e.g.,
        // `Literal(true)` alone — becomes always-true after rewrite.
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        let pp = build_pruning_predicate(&expr, schema);
        assert!(pp.is_none());
    }

    // ─────────────────────────────────────────────────────────────────
    // Row-selection shape (adjacent merging, whole-RG, empty).
    // ─────────────────────────────────────────────────────────────────

    /// Count the number of selector runs in the selection — useful to
    /// verify `to_row_selection` merges adjacent same-decision pages.
    fn run_count(sel: &RowSelection) -> usize {
        sel.iter().count()
    }

    #[test]
    fn selection_merges_adjacent_same_decision_pages() {
        // price > -1: every page qualifies (`price_min < -1` is false
        // for all 4 pages). After merging, one run of `select(32)`.
        let (pruner, schema, _) = two_col_fixture();
        let expr = bin(col("price", 0), Operator::Gt, lit_int(-1));
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(run_count(&sel), 1, "all-select should coalesce");
        assert_eq!(count_rows_kept(&sel), 32);
    }

    #[test]
    fn selection_empty_when_no_page_survives() {
        // price < -100: no page could match.
        let (pruner, schema, _) = two_col_fixture();
        let expr = bin(col("price", 0), Operator::Lt, lit_int(-100));
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 0);
        assert_eq!(run_count(&sel), 1, "single skip run covers the whole RG");
    }

    #[test]
    fn selection_alternating_pages_keeps_run_granularity() {
        // price IN (5, 20) — picks pages 0 (contains 5) and 2 (contains
        // 20), skips pages 1 and 3. Two alternating patterns → four
        // runs: select/skip/select/skip.
        let (pruner, schema, _) = two_col_fixture();
        let c = col("price", 0);
        let list = vec![lit_int(5), lit_int(20)];
        let expr =
            datafusion::physical_expr::expressions::in_list(c, list, &false, &schema).unwrap();
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(count_rows_kept(&sel), 16, "2 pages × 8 rows");
        assert_eq!(run_count(&sel), 4, "expected select/skip/select/skip");
    }

    // ─────────────────────────────────────────────────────────────────
    // bool_tree_to_pruning_expr lowering.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn lowering_pure_collector_tree_yields_always_true() {
        // Tree with only Collectors → lowering produces Literal(true),
        // which `build_pruning_predicate` rejects.
        let (_, schema, _) = two_col_fixture();
        use crate::indexed_table::bool_tree::BoolNode;
        let tree = BoolNode::And(vec![
            BoolNode::Collector { query_bytes: Arc::from(&[0u8][..]) },
            BoolNode::Collector { query_bytes: Arc::from(&[1u8][..]) },
        ]);
        let expr = bool_tree_to_pruning_expr(&tree, &schema).unwrap();
        assert!(build_pruning_predicate(&expr, schema).is_none());
    }

    #[test]
    fn lowering_collector_mixed_with_predicate_keeps_predicate() {
        // AND(Collector, price > 20) → lowers to AND(true, price > 20)
        // → prunes on `price > 20` only.
        let (pruner, schema, _) = two_col_fixture();
        use crate::indexed_table::bool_tree::BoolNode;
        let tree = BoolNode::And(vec![
            BoolNode::Collector { query_bytes: Arc::from(&[0u8][..]) },
            pred_leaf("price", Operator::Gt, 20, &schema),
        ]);
        let expr = bool_tree_to_pruning_expr(&tree, &schema).unwrap();
        let pp = build_pruning_predicate(&expr, schema)
            .expect("non-Collector subtree should still be prunable");
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        // price > 20 → pages 2 (16..23 — max 23 > 20) + 3 (24..31).
        assert!(count_rows_kept(&sel) > 0);
        assert!(count_rows_kept(&sel) <= 16);
    }

    #[test]
    fn lowering_predicate_on_missing_column_falls_back() {
        // Predicate referencing a column not in schema — handled by
        // DataFusion: either try_new fails or the expression prunes as
        // always-true. Both outcomes preserve correctness.
        let (_, schema, _) = two_col_fixture();
        use crate::indexed_table::bool_tree::BoolNode;
        let missing_col: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new("missing", 99));
        let lit5: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Int32(Some(5))));
        let missing_pred = BoolNode::Predicate(Arc::new(
            BinaryExpr::new(missing_col, Operator::Eq, lit5),
        ));
        let tree = BoolNode::Or(vec![
            missing_pred,
            pred_leaf("price", Operator::Gt, 20, &schema),
        ]);
        let expr = bool_tree_to_pruning_expr(&tree, &schema).unwrap();
        // Either succeeds (pruning applied) or returns None (scan all).
        // Both are safe.
        let _ = build_pruning_predicate(&expr, schema);
    }

    // ─────────────────────────────────────────────────────────────────
    // Multi-RG: the pruner is stateless per RG; repeated calls on
    // different RGs of the same metadata handle each correctly.
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn multi_rg_fixture_prunes_each_rg_independently() {
        // Build a 2-RG parquet so we can exercise rg_idx=0 and rg_idx=1
        // with the same `PagePruner`.
        use datafusion::arrow::array::{Int32Array, RecordBatch};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
        ]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from((0..32).collect::<Vec<i32>>()))],
        ).unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from((100..132).collect::<Vec<i32>>()))],
        ).unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        w.write(&batch1).unwrap();
        w.flush().unwrap();
        w.write(&batch2).unwrap();
        w.close().unwrap();
        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        ).unwrap();
        assert_eq!(meta.metadata().num_row_groups(), 2);
        let pruner = PagePruner::new(&schema, meta.metadata().clone());
        // price > 50: RG0 (0..31) → nothing, RG1 (100..131) → all.
        let expr = bin(col("price", 0), Operator::Gt, lit_int(50));
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel0 = pruner.prune_rg(&pp, 0, None).unwrap();
        let sel1 = pruner.prune_rg(&pp, 1, None).unwrap();
        assert_eq!(count_rows_kept(&sel0), 0, "RG0 fully pruned");
        assert_eq!(count_rows_kept(&sel1), 32, "RG1 fully kept");
    }
}
