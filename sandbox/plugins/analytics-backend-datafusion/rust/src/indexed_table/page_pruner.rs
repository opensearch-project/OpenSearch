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

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Column, ScalarValue};
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use datafusion::parquet::file::metadata::ParquetMetaData;
#[cfg(test)]
use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
use datafusion::physical_expr::utils::collect_columns;
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
        let columns = collect_columns(pruning_predicate.orig_expr());
        if columns.is_empty() {
            return None;
        }

        // Early-exit if the file lacks a column/page index; without
        // both we can't produce page stats for pruning.
        self.metadata.column_index()?;
        let offset_index = self.metadata.offset_index()?;
        let rg_offsets = offset_index.get(rg_idx)?;
        let rg_meta = self.metadata.row_groups().get(rg_idx)?;
        let num_rows = rg_meta.num_rows() as usize;

        // Build a common page grid from the union of all referenced
        // columns' page boundaries. Each grid cell is a row range that
        // falls within a single page of every column.
        //
        // Columns referenced by the predicate that are NOT present in
        // the parquet file (schema evolution) contribute no boundaries —
        // their grid-cell stats are filled with Arrow-nulls later so
        // `PruningPredicate` treats them as unknown (conservative) while
        // still allowing other columns to prune.
        let mut boundary_set = std::collections::BTreeSet::new();
        boundary_set.insert(0i64);
        boundary_set.insert(num_rows as i64);

        let mut col_converters: Vec<(
            &datafusion::physical_expr::expressions::Column,
            Option<(StatisticsConverter<'_>, usize)>,
        )> = Vec::new();

        // Resolve against the segment's own schema (see eval_leaf): the full table
        // schema misaligns StatisticsConverter's positional column lookup under
        // dynamic-mapping schema drift.
        let descr = self.metadata.file_metadata().schema_descr();
        let seg_arrow_schema = match datafusion::parquet::arrow::parquet_to_arrow_schema(
            descr,
            self.metadata.file_metadata().key_value_metadata(),
        ) {
            Ok(s) => Arc::new(s),
            Err(_) => Arc::clone(&self.schema),
        };

        for col in &columns {
            let converter = match StatisticsConverter::try_new(col.name(), &seg_arrow_schema, descr)
            {
                Ok(c) => c,
                Err(_) => {
                    // Column not in Arrow schema either — nothing we can
                    // do. Treat as absent (fills with nulls).
                    col_converters.push((col, None));
                    continue;
                }
            };
            let parquet_col_idx = match converter.parquet_column_index() {
                Some(idx) => idx,
                None => {
                    // Column is in Arrow schema but absent from the
                    // parquet file. Fill with null stats so this column
                    // is "unknown" but others still prune.
                    col_converters.push((col, None));
                    continue;
                }
            };
            let col_locs = match rg_offsets.get(parquet_col_idx) {
                Some(oi) => oi.page_locations(),
                None => {
                    col_converters.push((col, None));
                    continue;
                }
            };
            for loc in col_locs {
                boundary_set.insert(loc.first_row_index);
            }
            col_converters.push((col, Some((converter, parquet_col_idx))));
        }

        // If no referenced column contributed real page boundaries (all
        // absent or had no page index), pruning can't do anything useful.
        if col_converters.iter().all(|(_, c)| c.is_none()) {
            if let Some(m) = metrics {
                if let Some(ref c) = m.page_pruning_unavailable {
                    c.add(1);
                }
            }
            return None;
        }

        let boundaries: Vec<i64> = boundary_set.into_iter().collect();
        let num_grid_cells = boundaries.len() - 1;
        if num_grid_cells == 0 {
            return None;
        }

        // For each column, build min/max/null_count arrays aligned to
        // the common grid. Each grid cell inherits stats from the page
        // that contains it.
        let stats = CommonGridPageStats::build(
            &boundaries,
            num_grid_cells,
            &col_converters,
            &self.schema,
            &self.metadata,
            rg_idx,
        )?;

        let keep = match pruning_predicate.prune(&stats) {
            Ok(k) => k,
            Err(e) => {
                native_bridge_common::log_debug!("page pruning error for rg {}: {}", rg_idx, e);
                if let Some(m) = metrics {
                    if let Some(ref c) = m.page_pruning_unavailable {
                        c.add(1);
                    }
                }
                return None;
            }
        };

        // Convert grid-level keep/skip to row selection using grid cell row counts
        let grid_row_counts: Vec<usize> = boundaries
            .windows(2)
            .map(|w| (w[1] - w[0]) as usize)
            .collect();

        if keep.len() != grid_row_counts.len() {
            return None;
        }

        if let Some(m) = metrics {
            let pruned = keep.iter().filter(|k| !**k).count();
            if let Some(ref c) = m.pages_pruned {
                c.add(pruned);
            }
            if let Some(ref c) = m.pages_total {
                c.add(keep.len());
            }
        }
        Some(to_row_selection(keep, &grid_row_counts))
    }

    /// Return per-page row counts for the given RG. Uses the first
    /// column with a populated page index — column 0 isn't guaranteed
    /// to have one (e.g., a BYTE_ARRAY column with page-index disabled
    /// but other columns enabled). Used for metrics and cost estimation;
    /// different columns in the same RG may have different page layouts,
    /// so this is an approximation good enough for counting.
    pub fn page_row_counts(&self, rg_idx: usize) -> Option<Vec<usize>> {
        let offset_index = self.metadata.offset_index()?;
        let rg_offsets = offset_index.get(rg_idx)?;
        let col_offsets = rg_offsets
            .iter()
            .map(|oi| oi.page_locations())
            .find(|locs| !locs.is_empty())?;
        let rg_meta = self.metadata.row_groups().get(rg_idx)?;
        let num_rows = rg_meta.num_rows() as usize;
        let mut counts = Vec::with_capacity(col_offsets.len());
        for pair in col_offsets.windows(2) {
            counts.push((pair[1].first_row_index - pair[0].first_row_index) as usize);
        }
        counts.push(num_rows - col_offsets.last()?.first_row_index as usize);
        Some(counts)
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
    pub fn from_stream_metrics(sm: &crate::indexed_table::metrics::StreamMetrics) -> Self {
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
            native_bridge_common::log_debug!(
                "PruningPredicate::try_new failed for {:?}: {}",
                expr,
                e
            );
            return None;
        }
    };
    if pruning_predicate.always_true() {
        native_bridge_common::log_debug!(
            "PruningPredicate collapsed to always_true for {:?}",
            expr
        );
        return None;
    }
    Some(Arc::new(pruning_predicate))
}

/// Page statistics aligned to a common grid across all referenced columns.
/// Each grid cell is a row range that falls within a single page of every
/// column. min/max for each column are inherited from the page containing
/// that grid cell.
struct CommonGridPageStats {
    /// Per-column min/max/null arrays, all of length `num_grid_cells`.
    /// Keyed by column name.
    col_stats: HashMap<String, (ArrayRef, ArrayRef, Option<ArrayRef>)>,
    grid_row_counts: Vec<usize>,
}

impl CommonGridPageStats {
    fn build(
        boundaries: &[i64],
        num_grid_cells: usize,
        col_converters: &[(
            &datafusion::physical_expr::expressions::Column,
            Option<(StatisticsConverter<'_>, usize)>,
        )],
        arrow_schema: &SchemaRef,
        parquet_metadata: &ParquetMetaData,
        rg_idx: usize,
    ) -> Option<Self> {
        let column_index = parquet_metadata.column_index()?;
        let offset_index = parquet_metadata.offset_index()?;
        let rg_offsets = offset_index.get(rg_idx)?;
        let rg_meta = parquet_metadata.row_groups().get(rg_idx)?;
        let num_rows = rg_meta.num_rows() as usize;
        let grid_row_counts: Vec<usize> = boundaries
            .windows(2)
            .map(|w| (w[1] - w[0]) as usize)
            .collect();

        let mut col_stats = HashMap::new();

        for (col, maybe_cv) in col_converters {
            let (converter, parquet_col_idx) = match maybe_cv {
                Some(cv) => cv,
                None => {
                    // Column not present in the parquet file. Provide
                    // all-null min/max arrays typed to match the Arrow
                    // schema so `PruningPredicate`'s comparison kernels
                    // see type-compatible nulls (treated as "unknown"
                    // for every grid cell). Null_counts is `None` so
                    // IS NULL / IS NOT NULL also can't prune.
                    let data_type = arrow_schema
                        .field_with_name(col.name())
                        .map(|f| f.data_type().clone())
                        .unwrap_or(datafusion::arrow::datatypes::DataType::Null);
                    let mins = datafusion::arrow::array::new_null_array(&data_type, num_grid_cells);
                    let maxs = datafusion::arrow::array::new_null_array(&data_type, num_grid_cells);
                    col_stats.insert(col.name().to_string(), (mins, maxs, None));
                    continue;
                }
            };
            let col_locs = rg_offsets.get(*parquet_col_idx)?.page_locations();

            // Get the raw per-page stats for this column
            let mins = converter
                .data_page_mins(column_index, offset_index, [&rg_idx])
                .ok()?;
            let maxs = converter
                .data_page_maxes(column_index, offset_index, [&rg_idx])
                .ok()?;
            let page_null_counts = converter
                .data_page_null_counts(column_index, offset_index, [&rg_idx])
                .ok();

            // Per-page row counts for this column (needed to know when a
            // page is "all null": `null_count == page_row_count`).
            let page_row_counts: Vec<usize> = {
                let mut v = Vec::with_capacity(col_locs.len());
                for pair in col_locs.windows(2) {
                    v.push((pair[1].first_row_index - pair[0].first_row_index) as usize);
                }
                if let Some(last) = col_locs.last() {
                    v.push(num_rows - last.first_row_index as usize);
                }
                v
            };

            // Map each grid cell to the page that contains it, then
            // replicate that page's stats to the grid cell.
            let mut grid_page_indices = Vec::with_capacity(num_grid_cells);
            let mut page_idx = 0usize;
            let mut next_page_start = if col_locs.len() > 1 {
                col_locs[1].first_row_index
            } else {
                i64::MAX
            };

            for &cell_start in boundaries.iter().take(num_grid_cells) {
                // Advance page_idx until this cell falls within the page
                while page_idx + 1 < col_locs.len() && cell_start >= next_page_start {
                    page_idx += 1;
                    next_page_start = if page_idx + 1 < col_locs.len() {
                        col_locs[page_idx + 1].first_row_index
                    } else {
                        i64::MAX
                    };
                }
                grid_page_indices.push(page_idx);
            }

            // Build grid-aligned min/max arrays by indexing into the
            // per-page arrays. `StatisticsConverter` already converts
            // `null_pages = true` entries into Arrow nulls, which
            // `take` propagates correctly.
            use datafusion::arrow::compute::take;
            let indices = datafusion::arrow::array::UInt32Array::from(
                grid_page_indices
                    .iter()
                    .map(|i| *i as u32)
                    .collect::<Vec<_>>(),
            );
            let grid_mins = take(&mins, &indices, None).ok()?;
            let grid_maxs = take(&maxs, &indices, None).ok()?;

            // Null count splitting: a page's `null_count` applies to the
            // whole page, not to any sub-range of it. When a cell is only
            // part of a page, the page's null count can't be attributed
            // directly. Rule:
            //   - page null_count == 0        → cell null_count = 0
            //     (no nulls anywhere in the page → none in any sub-cell)
            //   - page null_count == page_row_count → cell null_count = cell_row_count
            //     (all values are null → every sub-cell is all-null)
            //   - otherwise                    → cell null_count = null (unknown)
            //     `PruningPredicate` treats a null as "unknown" and falls
            //     back to the safe (non-pruning) branch, so `IS NULL` /
            //     `IS NOT NULL` stay correct on the split cell.
            let grid_nulls: Option<ArrayRef> = page_null_counts.map(|page_ncs| {
                use datafusion::arrow::array::UInt64Array;
                let mut builder = UInt64Array::builder(num_grid_cells);
                for (cell_idx, &pidx) in grid_page_indices.iter().enumerate() {
                    if pidx >= page_ncs.len() || page_ncs.is_null(pidx) {
                        builder.append_null();
                        continue;
                    }
                    let page_nc = page_ncs.value(pidx) as usize;
                    let page_rc = page_row_counts.get(pidx).copied().unwrap_or(0);
                    let cell_rc = grid_row_counts[cell_idx];
                    if page_nc == 0 {
                        builder.append_value(0);
                    } else if page_nc == page_rc {
                        // All values in the page are null → all values
                        // in any sub-cell are null too.
                        builder.append_value(cell_rc as u64);
                    } else {
                        // Mixed page split across multiple grid cells:
                        // can't attribute null_count exactly. Mark as
                        // unknown so `PruningPredicate` stays conservative.
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            });

            col_stats.insert(col.name().to_string(), (grid_mins, grid_maxs, grid_nulls));
        }

        Some(Self {
            col_stats,
            grid_row_counts,
        })
    }
}

impl PruningStatistics for CommonGridPageStats {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.col_stats
            .get(column.name())
            .map(|(m, _, _)| Arc::clone(m))
    }
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.col_stats
            .get(column.name())
            .map(|(_, m, _)| Arc::clone(m))
    }
    fn num_containers(&self) -> usize {
        self.grid_row_counts.len()
    }
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.col_stats
            .get(column.name())
            .and_then(|(_, _, n)| n.clone())
    }
    fn row_counts(&self) -> Option<ArrayRef> {
        let arr = Int64Array::from_iter_values(self.grid_row_counts.iter().map(|c| *c as i64));
        Some(Arc::new(arr) as ArrayRef)
    }
    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
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

/// Evaluate a single predicate against RG-level column stats for the given RGs.
/// Returns a `Vec<bool>` aligned to `rg_indices`.
///
/// Conservative on any error: returns `true` (can-match) for every RG rather
/// than falsely pruning. This means a stats extraction failure, missing column,
/// or `PruningPredicate::prune` error will never cause data loss — the RG will
/// simply not be skipped.
fn eval_leaf(
    pruning_predicate: &PruningPredicate,
    metadata: &ParquetMetaData,
    schema: &SchemaRef,
    rg_indices: &[usize],
) -> Vec<bool> {
    let num = rg_indices.len();
    if num == 0 {
        return vec![];
    }
    let columns = collect_columns(pruning_predicate.orig_expr());
    if columns.is_empty() {
        return vec![true; num];
    }
    // Resolve stats against the segment's OWN parquet schema, not the full table
    // schema. StatisticsConverter maps a column name to a parquet index positionally
    // (parquet crate `parquet_column`), so passing the full schema reads the wrong /
    // no column when the segment's schema is narrower or reordered (dynamic-mapping
    // schema drift) — yielding null stats that prune RGs that actually match.
    let descr = metadata.file_metadata().schema_descr();
    let seg_arrow_schema = match datafusion::parquet::arrow::parquet_to_arrow_schema(
        descr,
        metadata.file_metadata().key_value_metadata(),
    ) {
        Ok(s) => Arc::new(s),
        Err(_) => Arc::clone(schema),
    };
    let arrow_schema = seg_arrow_schema.as_ref();
    let rg_metas: Vec<_> = rg_indices
        .iter()
        .filter_map(|&idx| metadata.row_groups().get(idx))
        .collect();
    if rg_metas.len() != num {
        return vec![true; num];
    }
    let mut col_stats: HashMap<String, (ArrayRef, ArrayRef, Option<ArrayRef>)> = HashMap::new();
    for col in &columns {
        if arrow_schema.index_of(col.name()).is_err() {
            continue;
        }
        let converter = match StatisticsConverter::try_new(col.name(), arrow_schema, descr) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let min_arr = match converter.row_group_mins(rg_metas.iter().copied()) {
            Ok(arr) => arr,
            Err(_) => continue,
        };
        let max_arr = match converter.row_group_maxes(rg_metas.iter().copied()) {
            Ok(arr) => arr,
            Err(_) => continue,
        };
        let null_counts = converter
            .row_group_null_counts(rg_metas.iter().copied())
            .ok()
            .map(|arr| Arc::new(arr) as ArrayRef);
        col_stats.insert(col.name().to_string(), (min_arr, max_arr, null_counts));
    }
    if col_stats.is_empty() {
        return vec![true; num];
    }
    let stats = RgLevelStats {
        col_stats,
        num_rgs: num,
        row_counts: rg_metas.iter().map(|m| m.num_rows()).collect(),
    };
    match pruning_predicate.prune(&stats) {
        Ok(keep) => keep,
        Err(_) => vec![true; num],
    }
}

struct RgLevelStats {
    col_stats: HashMap<String, (ArrayRef, ArrayRef, Option<ArrayRef>)>,
    num_rgs: usize,
    row_counts: Vec<i64>,
}

impl PruningStatistics for RgLevelStats {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.col_stats
            .get(column.name())
            .map(|(m, _, _)| Arc::clone(m))
    }
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.col_stats
            .get(column.name())
            .map(|(_, m, _)| Arc::clone(m))
    }
    fn num_containers(&self) -> usize {
        self.num_rgs
    }
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.col_stats
            .get(column.name())
            .and_then(|(_, _, n)| n.clone())
    }
    fn row_counts(&self) -> Option<ArrayRef> {
        let arr = Int64Array::from_iter_values(self.row_counts.iter().copied());
        Some(Arc::new(arr) as ArrayRef)
    }
    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

/// Precomputed tree of per-RG match vectors built from column statistics.
/// Used for segment-level, RG-level, and subtree-level pruning.
///
/// # Example: 5 RGs, 9 leaves
///
/// Legend: `df` = Predicate leaf (parquet column stats), `luc` = Collector (always true).
/// Each node shows its `rg_can_match` bitset: `[RG0, RG1, RG2, RG3, RG4]`.
///
/// ```text
///                                  AND
///                                [00001]
///                /                  |                  \
///              OR                   OR                  AND
///           [01101]              [11111]              [00011]
///          /       \          /     |     \          /       \
///       AND         df     AND     luc     df      df        NOT
///     [01100]    [00001] [00100] [11111] [11110] [00011]   [11111]
///      /   \              /   \                              |
///    df     df          df     df                           df
/// [11100] [01111]    [11100] [00111]                     [11000]
/// ```
///
/// Bottom-up:
///   - AND(df[11100], df[01111]) = [01100]
///   - OR₁(AND[01100], df[00001]) = [01101]
///   - AND(df[11100], df[00111]) = [00100]
///   - OR₂(AND[00100], luc[11111], df[11110]) = [11111]  ← luc dominates
///   - NOT(df[11000]) = [11111]  ← conservative: can't prune through negation
///   - AND₃(df[00011], NOT[11111]) = [00011]
///   - Root AND(OR₁[01101], OR₂[11111], AND₃[00011]) = [00001]
///
/// Result: only RG4 survives. NOT is always [11111] because inverting
/// stats-based pruning is unsound (superset inverted = subset).
/// Subtree vectors enable short-circuiting: for RG0, AND₃=[0....] so the
/// bitmap-tree walk skips that entire subtree (no FFM collector calls).
#[derive(Clone)]
pub struct StatsPruneTree {
    /// Per-RG: `false` means this subtree provably can't match that RG.
    pub rg_can_match: Vec<bool>,
    pub children: Vec<StatsPruneTree>,
}

impl StatsPruneTree {
    pub fn build_from_bool_node(
        node: &super::bool_tree::BoolNode,
        leaf_predicates: &HashMap<usize, Arc<PruningPredicate>>,
        metadata: &ParquetMetaData,
        schema: &SchemaRef,
        rg_indices: &[usize],
    ) -> Self {
        use super::bool_tree::BoolNode;
        let num = rg_indices.len();
        match node {
            BoolNode::And(children) => {
                let annotated_children: Vec<_> = children
                    .iter()
                    .map(|c| {
                        Self::build_from_bool_node(c, leaf_predicates, metadata, schema, rg_indices)
                    })
                    .collect();
                let mut rg_can_match = vec![true; num];
                for child in &annotated_children {
                    for (r, c) in rg_can_match.iter_mut().zip(child.rg_can_match.iter()) {
                        *r &= c;
                    }
                }
                Self {
                    rg_can_match,
                    children: annotated_children,
                }
            }
            BoolNode::Or(children) => {
                let annotated_children: Vec<_> = children
                    .iter()
                    .map(|c| {
                        Self::build_from_bool_node(c, leaf_predicates, metadata, schema, rg_indices)
                    })
                    .collect();
                let mut rg_can_match = vec![false; num];
                for child in &annotated_children {
                    for (r, c) in rg_can_match.iter_mut().zip(child.rg_can_match.iter()) {
                        *r |= c;
                    }
                }
                Self {
                    rg_can_match,
                    children: annotated_children,
                }
            }
            BoolNode::Not(child) => {
                let annotated_child = Self::build_from_bool_node(
                    child,
                    leaf_predicates,
                    metadata,
                    schema,
                    rg_indices,
                );
                // NOT is conservatively all-true: negating stats-based pruning is unsound
                // because stats give a superset, and inverting a superset is a subset.
                native_bridge_common::log_debug!("StatsPruneTree: NOT node → all-true (conservative, cannot prune through negation)");
                Self {
                    rg_can_match: vec![true; num],
                    children: vec![annotated_child],
                }
            }
            BoolNode::Predicate(expr) => {
                let key = Arc::as_ptr(expr) as *const () as usize;
                let rg_can_match = match leaf_predicates.get(&key) {
                    Some(pp) => eval_leaf(pp, metadata, schema, rg_indices),
                    None => vec![true; num],
                };
                Self {
                    rg_can_match,
                    children: vec![],
                }
            }
            BoolNode::DelegationPossible { original_expr, .. } => {
                let key = Arc::as_ptr(original_expr) as *const () as usize;
                let rg_can_match = match leaf_predicates.get(&key) {
                    Some(pp) => eval_leaf(pp, metadata, schema, rg_indices),
                    None => vec![true; num],
                };
                Self {
                    rg_can_match,
                    children: vec![],
                }
            }
            BoolNode::Collector { .. } => {
                // Collectors have no column stats — always all-true.
                native_bridge_common::log_debug!(
                    "StatsPruneTree: Collector node → all-true (no column stats available)"
                );
                Self {
                    rg_can_match: vec![true; num],
                    children: vec![],
                }
            }
        }
    }
}

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
            vec![
                Arc::new(Int32Array::from(prices)),
                Arc::new(Int32Array::from(qtys)),
            ],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
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
        assert_eq!(
            count_rows_kept(&sel),
            0,
            "AND of disjoint page sets prunes everything"
        );
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
        assert_eq!(
            count_rows_kept(&sel),
            0,
            "OR of unreachable ranges prunes everything"
        );
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
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int32,
            false,
        )]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from((0..32).collect::<Vec<i32>>()))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from((100..132).collect::<Vec<i32>>()))],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        w.write(&batch1).unwrap();
        w.flush().unwrap();
        w.write(&batch2).unwrap();
        w.close().unwrap();
        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
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

    // ─────────────────────────────────────────────────────────────────
    // Edge case coverage added for common grid correctness.
    // ─────────────────────────────────────────────────────────────────

    /// Columns with misaligned per-column page boundaries. Each column's
    /// page layout is independent; the common grid unions the boundaries.
    /// We verify that a single-column predicate on each column still
    /// prunes correctly despite the union grid containing extra cell
    /// boundaries contributed by the *other* column.
    #[test]
    fn misaligned_page_boundaries_prune_correctly() {
        use datafusion::arrow::array::StringArray;
        // `price` is Int32 (tiny bytes/value). `tag` is Utf8 with
        // very large values. A small page-size budget forces `tag`
        // to flush frequently while `price` stays as one page.
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
            Field::new("tag", DataType::Utf8, false),
        ]));
        let prices: Vec<i32> = (0..32).collect();
        // ~4 KiB per tag so the byte budget triggers many page flushes.
        let tags: Vec<String> = (0..32)
            .map(|i| format!("{}{}", i, "x".repeat(4000)))
            .collect();
        let tags_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(prices)),
                Arc::new(StringArray::from(tags_refs)),
            ],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            // Extreme byte budget forces a page flush after nearly
            // every row for `tag`. Dictionary encoding would collapse
            // the strings to small indices, defeating the budget, so
            // disable it for this test.
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(100)
            .set_data_page_row_count_limit(32)
            .set_write_batch_size(1)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        let arc_meta = meta.metadata().clone();

        // Sanity check that the two columns have different page counts.
        let oi = arc_meta.offset_index().expect("offset index");
        let price_pages = oi[0][0].page_locations().len();
        let tag_pages = oi[0][1].page_locations().len();
        assert!(
            price_pages != tag_pages,
            "fixture must produce misaligned per-column page layouts; got price={} tag={}",
            price_pages,
            tag_pages
        );

        let pruner = PagePruner::new(&schema, Arc::clone(&arc_meta));

        // `price > 100` is definitively false for all rows; every grid
        // cell inherits stats from the single price page (min=0, max=31)
        // → every cell prunes → 0 rows. This verifies the grid-cell
        // mapping correctly carries min/max through cells created by
        // the *other* column's boundaries.
        let expr_false = bin(col("price", 0), Operator::Gt, lit_int(100));
        let pp = build_pruning_predicate(&expr_false, schema.clone()).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(
            count_rows_kept(&sel),
            0,
            "price > 100 must prune the entire RG regardless of tag layout"
        );

        // `price >= 0` is definitively true for all rows; every grid
        // cell keeps → 32 rows. Same shape, opposite outcome.
        let expr_true = bin(col("price", 0), Operator::GtEq, lit_int(0));
        let pp = build_pruning_predicate(&expr_true, schema);
        // Note: `price >= 0` on a column with min=0 may collapse to
        // always-true at predicate-construction time.
        match pp {
            None => { /* always_true — no pruning needed */ }
            Some(pp) => {
                let sel = pruner.prune_rg(&pp, 0, None).unwrap();
                assert_eq!(
                    count_rows_kept(&sel),
                    32,
                    "price >= 0 keeps every grid cell"
                );
            }
        }
    }

    /// Nullable column with an all-null page. Verify that `IS NULL`
    /// keeps that page (null_count == row_count) and `IS NOT NULL`
    /// prunes it. With the common-grid null-count splitting fix, a
    /// fully-null page retains its IS-NOT-NULL pruning behaviour even
    /// when split across grid cells.
    #[test]
    fn nullable_all_null_page_prunes_is_not_null() {
        use datafusion::arrow::array::Int32Array;
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        // 32 rows, 4 pages of 8 rows each. Pages 0, 2, 3 have real
        // values (0..7, 16..23, 24..31); page 1 (rows 8..15) is all-null.
        let vals: Vec<Option<i32>> = (0..32)
            .map(|i| if (8..16).contains(&i) { None } else { Some(i) })
            .collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vals))]).unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        let pruner = PagePruner::new(&schema, meta.metadata().clone());

        // IS NOT NULL: page 1 (all-null) should be pruned → 24 rows kept.
        use datafusion::physical_expr::expressions::IsNotNullExpr;
        let expr: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(col("x", 0)));
        let pp = build_pruning_predicate(&expr, schema.clone()).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(
            count_rows_kept(&sel),
            24,
            "IS NOT NULL must prune the all-null page"
        );

        // IS NULL: only the all-null page keeps rows → 8 rows.
        use datafusion::physical_expr::expressions::IsNullExpr;
        let expr: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(col("x", 0)));
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(
            count_rows_kept(&sel),
            8,
            "IS NULL must keep the all-null page and prune the others"
        );
    }

    /// Predicate references a column absent from the parquet file
    /// (schema drift: the predicate's Arrow schema has an extra column).
    /// Verify the present column still prunes and the absent column
    /// contributes "unknown" stats without disabling pruning for the RG.
    #[test]
    fn missing_column_does_not_disable_other_columns() {
        use datafusion::arrow::array::Int32Array;
        // File has only `price`. The predicate also references `extra`,
        // which doesn't exist in the parquet file.
        let parquet_schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int32,
            false,
        )]));
        // Arrow schema handed to the pruner includes both columns —
        // this simulates "predicate knows about a column we haven't
        // written yet" (schema evolution).
        let predicate_schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
            Field::new("extra", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            parquet_schema.clone(),
            vec![Arc::new(Int32Array::from((0..32).collect::<Vec<i32>>()))],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), parquet_schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        let pruner = PagePruner::new(&predicate_schema, meta.metadata().clone());

        // `price < 5 AND extra = 10`. Only page 0 (0..7) can contain
        // price < 5. The `extra = 10` clause evaluates to unknown for
        // every cell (absent column → all-null stats), so AND collapses
        // to `(pruned_by_price) AND unknown`, which PruningPredicate
        // treats as "can't prove false" → keeps the page (conservative).
        // But `price >= 5` (pages 1, 2, 3) evaluates to definitively
        // false regardless of `extra`, so those pages are still pruned.
        let expr = bin(
            bin(col("price", 0), Operator::Lt, lit_int(5)),
            Operator::And,
            bin(col("extra", 1), Operator::Eq, lit_int(10)),
        );
        let pp = build_pruning_predicate(&expr, predicate_schema).unwrap();
        let sel = pruner.prune_rg(&pp, 0, None).unwrap();
        assert_eq!(
            count_rows_kept(&sel),
            8,
            "price-side pruning still applies even though extra is absent"
        );
    }

    /// Multi-RG fixture where each RG has a different per-page row
    /// layout. Verify the pruner reads each RG's own offset index
    /// rather than reusing one RG's layout for both.
    #[test]
    fn multi_rg_different_page_layouts_per_rg() {
        use datafusion::arrow::array::Int32Array;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int32,
            false,
        )]));
        // RG0: 32 rows, 8-row pages → 4 pages.
        // RG1: 32 rows, 16-row pages → 2 pages.
        // The easiest way to get different per-RG page layouts is to
        // change writer props between flushes.
        let tmp = NamedTempFile::new().unwrap();
        let props_rg0 = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        // Start the writer with RG0 props and flush after the first
        // batch to close RG0.
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from((0..32).collect::<Vec<i32>>()))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from((100..132).collect::<Vec<i32>>()))],
        )
        .unwrap();
        // ArrowWriter doesn't support mid-file prop changes; page row
        // count is file-wide. Simulate "different per-RG page layouts"
        // by writing batch1 with 8-row batches and batch2 with 16-row
        // batches so the effective page row count differs in each RG.
        // The `write_batch_size` acts as an upper bound on page row
        // count enforcement, which we exploit here.
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props_rg0)).unwrap();
        w.write(&batch1).unwrap();
        w.flush().unwrap();
        // For RG1, write in a single larger batch so it hits a bigger
        // page layout. We can't truly swap writer properties mid-file
        // with the public API; fall back to asserting that each RG's
        // layout is read from its own offset index entry. Even if the
        // two RGs end up with the same page counts, we still exercise
        // per-RG lookup because `page_row_counts(1)` is computed from
        // rg_offsets.get(1), not the first RG.
        w.write(&batch2).unwrap();
        w.close().unwrap();

        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        assert_eq!(meta.metadata().num_row_groups(), 2);
        let pruner = PagePruner::new(&schema, meta.metadata().clone());

        // Verify page_row_counts returns a valid layout for each RG.
        let rc0 = pruner.page_row_counts(0).unwrap();
        let rc1 = pruner.page_row_counts(1).unwrap();
        assert_eq!(rc0.iter().sum::<usize>(), 32, "RG0 row count");
        assert_eq!(rc1.iter().sum::<usize>(), 32, "RG1 row count");

        // Prune `price < 4` against each RG independently. RG0
        // (0..31) keeps only page 0 (max=7 ≥ 4, but page 1 has min=8
        // so page 1's min≮4 → pruned). 8 rows.
        // RG1 (100..131) keeps no page. 0 rows.
        let expr = bin(col("price", 0), Operator::Lt, lit_int(4));
        let pp = build_pruning_predicate(&expr, schema).unwrap();
        let sel0 = pruner.prune_rg(&pp, 0, None).unwrap();
        let sel1 = pruner.prune_rg(&pp, 1, None).unwrap();
        assert_eq!(
            count_rows_kept(&sel0),
            8,
            "RG0: only page 0 (min=0, max=7) overlaps price<4"
        );
        assert_eq!(count_rows_kept(&sel1), 0, "RG1: no page has values < 4");
    }

    /// Matches the tree from the doc comment on `StatsPruneTree`:
    ///
    /// ```text
    ///                                  AND
    ///                                [00001]
    ///                /                  |                  \
    ///              OR                   OR                  AND
    ///           [01101]              [11111]              [00011]
    ///          /       \          /     |     \          /       \
    ///       AND         df     AND     luc     df      df        NOT
    ///     [01100]    [00001] [00100] [11111] [11110] [00011]   [11111]
    ///      /   \              /   \                              |
    ///    df     df          df     df                           df
    /// [11100] [01111]    [11100] [00111]                     [11000]
    /// ```
    ///
    /// Uses 5 RGs (price ranges [0..9],[10..19],[20..29],[30..39],[40..49])
    /// to verify the full shape and bitset propagation.
    #[test]
    fn stats_prune_tree_five_leaf_shape() {
        use crate::indexed_table::bool_tree::BoolNode;

        // 5 RGs: price [0..9], [10..19], [20..29], [30..39], [40..49]
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int32,
            false,
        )]));
        let tmp = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(10)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        for i in 0..5i32 {
            let vals: Vec<i32> = (i * 10..(i + 1) * 10).collect();
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vals))])
                    .unwrap();
            w.write(&batch).unwrap();
        }
        w.close().unwrap();
        let meta =
            ArrowReaderMetadata::load(&tmp.reopen().unwrap(), ArrowReaderOptions::new()).unwrap();
        let arc_meta = meta.metadata().clone();
        assert_eq!(arc_meta.num_row_groups(), 5);
        let rg_indices: Vec<usize> = (0..5).collect();

        // Leaf predicates and their expected rg_can_match bitsets:
        let p1 = pred_leaf("price", Operator::Lt, 30, &schema); // [11100]
        let p2 = pred_leaf("price", Operator::GtEq, 10, &schema); // [01111]
        let p3 = pred_leaf("price", Operator::Gt, 45, &schema); // [00001]
        let p4 = pred_leaf("price", Operator::Lt, 25, &schema); // [11100]
        let p5 = pred_leaf("price", Operator::GtEq, 20, &schema); // [00111]
        let p6 = pred_leaf("price", Operator::Lt, 40, &schema); // [11110]
        let p7 = pred_leaf("price", Operator::Gt, 30, &schema); // [00011]
        let p8 = pred_leaf("price", Operator::Lt, 15, &schema); // [11000]

        // Tree: AND(OR(AND(p1,p2), p3), OR(AND(p4,p5), Collector, p6), AND(p7, NOT(p8)))
        let collector = BoolNode::Collector { annotation_id: 0 };
        let tree = BoolNode::And(vec![
            BoolNode::Or(vec![
                BoolNode::And(vec![p1.clone(), p2.clone()]),
                p3.clone(),
            ]),
            BoolNode::Or(vec![
                BoolNode::And(vec![p4.clone(), p5.clone()]),
                collector,
                p6.clone(),
            ]),
            BoolNode::And(vec![p7.clone(), BoolNode::Not(Box::new(p8.clone()))]),
        ]);

        // Build PruningPredicates for all df leaves.
        let mut leaf_predicates: HashMap<usize, Arc<PruningPredicate>> = HashMap::new();
        for node in [&p1, &p2, &p3, &p4, &p5, &p6, &p7, &p8] {
            if let BoolNode::Predicate(expr) = node {
                let key = Arc::as_ptr(expr) as *const () as usize;
                let pp = build_pruning_predicate(expr, schema.clone()).unwrap();
                leaf_predicates.insert(key, pp);
            }
        }

        let spt = StatsPruneTree::build_from_bool_node(
            &tree,
            &leaf_predicates,
            &arc_meta,
            &schema,
            &rg_indices,
        );

        // Verify bottom-up:
        //   AND(p1[11100], p2[01111]) = [01100]
        //   OR₁(AND[01100], p3[00001]) = [01101]
        //   AND(p4[11100], p5[00111]) = [00100]
        //   OR₂(AND[00100], luc[11111], p6[11110]) = [11111]  ← luc dominates
        //   NOT(p8[11000]) = [11111]  ← conservative
        //   AND₃(p7[00011], NOT[11111]) = [00011]
        //   Root AND(OR₁[01101], OR₂[11111], AND₃[00011]) = [00001]

        // Root
        assert_eq!(spt.rg_can_match, vec![false, false, false, false, true]);
        assert_eq!(spt.children.len(), 3);

        // Child 0: OR₁
        assert_eq!(
            spt.children[0].rg_can_match,
            vec![false, true, true, false, true]
        );
        assert_eq!(spt.children[0].children.len(), 2);
        // OR₁/AND
        assert_eq!(
            spt.children[0].children[0].rg_can_match,
            vec![false, true, true, false, false]
        );
        // OR₁/AND/p1
        assert_eq!(
            spt.children[0].children[0].children[0].rg_can_match,
            vec![true, true, true, false, false]
        );
        // OR₁/AND/p2
        assert_eq!(
            spt.children[0].children[0].children[1].rg_can_match,
            vec![false, true, true, true, true]
        );
        // OR₁/p3
        assert_eq!(
            spt.children[0].children[1].rg_can_match,
            vec![false, false, false, false, true]
        );

        // Child 1: OR₂ — luc makes it all-true
        assert_eq!(
            spt.children[1].rg_can_match,
            vec![true, true, true, true, true]
        );

        // Child 2: AND₃
        assert_eq!(
            spt.children[2].rg_can_match,
            vec![false, false, false, true, true]
        );
        assert_eq!(spt.children[2].children.len(), 2);
        // AND₃/p7
        assert_eq!(
            spt.children[2].children[0].rg_can_match,
            vec![false, false, false, true, true]
        );
        // AND₃/NOT → always true
        assert_eq!(
            spt.children[2].children[1].rg_can_match,
            vec![true, true, true, true, true]
        );
        // AND₃/NOT/p8
        assert_eq!(
            spt.children[2].children[1].children[0].rg_can_match,
            vec![true, true, false, false, false]
        );
    }

    // Schema drift: the table schema orders columns differently from the segment's own
    // parquet file, so resolving `severity` positionally against the table schema lands on
    // a DIFFERENT real file column (`neg`, all-negative). The always-true `severity >= 0`
    // then reads neg's stats (max < 0) and `eval_leaf` wrongly prunes the RG. Resolving
    // against the segment's own schema reads the real `severity` stats and keeps it.
    #[test]
    fn eval_leaf_resolves_stats_against_segment_schema_under_drift() {
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Int32, false),
            Field::new("neg", DataType::Int32, false),
            Field::new("severity", DataType::Int32, false),
        ]));
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Int32, false),
            Field::new("severity", DataType::Int32, false),
            Field::new("neg", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![-9, -8, -7, -6])),
                Arc::new(Int32Array::from(vec![0, 5, 10, 17])),
            ],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), file_schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let md = ArrowReaderMetadata::load(&tmp.reopen().unwrap(), ArrowReaderOptions::new())
            .unwrap()
            .metadata()
            .clone();

        let expr = bin(col("severity", 1), Operator::GtEq, lit_int(0));
        let pp = build_pruning_predicate(&expr, table_schema.clone()).unwrap();
        assert_eq!(
            eval_leaf(&pp, &md, &table_schema, &[0]),
            vec![true],
            "severity >= 0 is always true; eval_leaf must not prune under schema drift"
        );
    }

    /// Verifies that `build_from_bool_node` works correctly when
    /// `rg_indices` is a subset that doesn't start at 0 (e.g. chunk
    /// contains RGs [2,3,4] out of a 5-RG file). The `rg_can_match`
    /// vector should be 3 elements long, indexed 0..2 mapping to
    /// absolute RGs 2,3,4. Consumers use a reverse map to translate.
    #[test]
    fn stats_prune_tree_offset_rg_indices() {
        use crate::indexed_table::bool_tree::BoolNode;

        // 5 RGs: price [0..9], [10..19], [20..29], [30..39], [40..49]
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int32,
            false,
        )]));
        let tmp = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(10)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        for i in 0..5i32 {
            let vals: Vec<i32> = (i * 10..(i + 1) * 10).collect();
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vals))])
                    .unwrap();
            w.write(&batch).unwrap();
        }
        w.close().unwrap();
        let meta =
            ArrowReaderMetadata::load(&tmp.reopen().unwrap(), ArrowReaderOptions::new()).unwrap();
        let arc_meta = meta.metadata().clone();
        assert_eq!(arc_meta.num_row_groups(), 5);

        // Chunk only has RGs [2, 3, 4] (prices [20..49])
        let rg_indices: Vec<usize> = vec![2, 3, 4];

        // price < 35 → full file would be [T,T,T,T,F]; subset [2,3,4] → [T,T,F]
        let p1 = pred_leaf("price", Operator::Lt, 35, &schema);
        // price >= 30 → full file would be [F,F,F,T,T]; subset [2,3,4] → [F,T,T]
        let p2 = pred_leaf("price", Operator::GtEq, 30, &schema);

        // AND(p1, p2) on subset → [T,T,F] & [F,T,T] = [F,T,F]
        let tree = BoolNode::And(vec![p1.clone(), p2.clone()]);

        let mut leaf_predicates: HashMap<usize, Arc<PruningPredicate>> = HashMap::new();
        for node in [&p1, &p2] {
            if let BoolNode::Predicate(expr) = node {
                let key = Arc::as_ptr(expr) as *const () as usize;
                let pp = build_pruning_predicate(expr, schema.clone()).unwrap();
                leaf_predicates.insert(key, pp);
            }
        }

        let spt = StatsPruneTree::build_from_bool_node(
            &tree,
            &leaf_predicates,
            &arc_meta,
            &schema,
            &rg_indices,
        );

        // rg_can_match is 3 elements (one per chunk RG), relative indexing.
        assert_eq!(spt.rg_can_match.len(), 3);
        // Position 0 → absolute RG 2 (price [20..29]): p1=T, p2=F → AND=F
        // Position 1 → absolute RG 3 (price [30..39]): p1=T, p2=T → AND=T
        // Position 2 → absolute RG 4 (price [40..49]): p1=F, p2=T → AND=F
        assert_eq!(spt.rg_can_match, vec![false, true, false]);

        // Verify consumer-side reverse map lookup works correctly:
        let rg_index_to_pos: HashMap<usize, usize> = rg_indices
            .iter()
            .enumerate()
            .map(|(pos, &idx)| (idx, pos))
            .collect();

        // Absolute RG 3 should map to position 1 → can_match = true
        let pos = rg_index_to_pos.get(&3).unwrap();
        assert_eq!(spt.rg_can_match[*pos], true);

        // Absolute RG 2 should map to position 0 → can_match = false
        let pos = rg_index_to_pos.get(&2).unwrap();
        assert_eq!(spt.rg_can_match[*pos], false);

        // Absolute RG 0 (not in chunk) should have no entry
        assert!(rg_index_to_pos.get(&0).is_none());
    }
}
