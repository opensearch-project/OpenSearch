/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Indexed query executor — decodes substrait, classifies the filter tree,
//! builds providers per leaf, runs the query.
//!
//! Per-leaf lifecycle at query time (one compiled-query + per-segment matcher
//! per Collector leaf):
//!   1. `createProvider(annotation_id)` FFM upcall → `provider_key`  (once per
//!      Collector leaf, once per query).
//!   2. `createCollector(provider_key, seg, min, max)` FFM upcall → collector
//!      (once per SegmentChunk × Collector leaf).
//!   3. `collectDocs(collector, min, max, out)` FFM upcall (once per row group).
//!   4. `releaseCollector(collector)` when RG scan completes.
//!   5. `releaseProvider(provider_key)` when the tree is dropped.

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::tree_node::{TreeNode, TreeNodeRecursion},
    common::DataFusionError,
    datasource::{TableProvider, TableType},
    execution::memory_pool::MemoryPool,
    execution::object_store::ObjectStoreUrl,
    logical_expr::Expr,
    physical_expr::expressions::Column,
    physical_expr::PhysicalExpr,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::displayable,
    physical_plan::execute_stream,
    physical_plan::stream::RecordBatchStreamAdapter,
    physical_plan::ExecutionPlan,
};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use native_bridge_common::log_debug;
use prost::Message;
use substrait::proto::Plan;

use crate::api::DataFusionRuntime;
use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::helper::{
    build_query_runtime_env_with_store, build_query_session_context, register_listing_table,
};
use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::eval::bitmap_tree::{BitmapTreeEvaluator, CollectorLeafBitmaps};
use crate::indexed_table::eval::single_collector::SingleCollectorEvaluator;
use crate::indexed_table::eval::{CollectorCallStrategy, RowGroupBitsetSource, TreeBitsetSource};
use crate::indexed_table::ffm_callbacks::{create_provider, FfmSegmentCollector, ProviderHandle};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::segment_info::build_segments;
use crate::indexed_table::substrait_to_tree::{
    classify_filter, expr_to_bool_tree, extract_filter_expr, ExtractionResult, FilterClass,
};
use crate::indexed_table::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::api::ShardView;
use crate::cache::page_index;
use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::indexed_table::bool_tree::residual_bool_to_physical_expr;
use crate::indexed_table::metrics::StreamMetrics;
use crate::indexed_table::page_pruner::{
    build_pruning_predicate, PagePruneMetrics, StatsPruneTree,
};
use crate::parquet_page_cache::{
    load_scoped_page_index_cols, resolve_predicate_parquet_columns_pair,
};

/// Execute an indexed query.
///
/// `shard_view` carries the segment's parquet paths (populated when the reader
/// was built from a catalog snapshot). `query_memory_pool` is the per-query
/// tracker (same as vanilla path) — `None` disables tracking and uses the
/// global pool.
// TODO: remove this function once all callers migrate to the instruction-based path
// TODO: remove once api.rs migrates to instruction-based path directly.
// Kept as thin wrapper to make existing tests exercise execute_indexed_with_context
// with minimal changes.
pub async fn execute_indexed_query(
    substrait_bytes: Vec<u8>,
    table_name: String,
    shard_view: &ShardView,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
    query_memory_pool: Option<Arc<dyn MemoryPool>>,
    query_config: Arc<DatafusionQueryConfig>,
    context_id: i64,
) -> Result<i64, DataFusionError> {
    let num_partitions = query_config.target_partitions.max(1);
    // Build the per-query RuntimeEnv (list-files cache pre-populated, optional
    // per-query pool overlay) and register the shard object store — shared with
    // the vanilla path. File-metadata and file-statistics caches are inherited
    // from the global runtime for cross-query reuse.
    let runtime_env = build_query_runtime_env_with_store(
        runtime,
        &shard_view.table_path,
        shard_view.object_metas.as_ref(),
        Arc::clone(&shard_view.store),
        query_memory_pool,
    )?;

    // Build a fresh session context per query. The indexed path fans out via
    // IndexedExec partitions (derived from num_partitions), not DataFusion's, but
    // DF still wants a sane value for any post-scan operators it may add. The
    // `indexed_path` flag also drops the combine-partial-final optimizer pass and
    // registers the indexed-only index_filter / delegation_possible UDFs.
    let ctx = build_query_session_context(&query_config, runtime_env, num_partitions, true);

    // Register default ListingTable so substrait consumer can resolve the table.
    // No sort-order declaration on the indexed path (empty slices) — the indexed
    // executor drives ordering itself via IndexedExec.
    register_listing_table(&ctx, &table_name, shard_view.table_path.clone(), &[], &[]).await?;

    // Build SessionContextHandle and delegate to execute_indexed_with_context
    let handle = crate::session_context::SessionContextHandle {
        ctx,
        table_path: shard_view.table_path.clone(),
        object_metas: shard_view.object_metas.clone(),
        writer_generations: shard_view.writer_generations.clone(),
        sort_fields: shard_view.sort_fields.clone(),
        sort_orders: shard_view.sort_orders.clone(),
        query_context: crate::query_tracker::QueryTrackingContext::new(
            context_id,
            runtime.runtime_env.memory_pool.clone(),
            crate::query_tracker::QueryType::Shard,
        ),
        table_name: table_name.clone(),
        indexed_config: None, // derive classification from tree
        query_config: Arc::unwrap_or_clone(query_config),
        io_handle: tokio::runtime::Handle::current(),
        aggregate_mode: crate::agg_mode::Mode::Default,
        has_topk: false,
        prepared_plan: None,
        phantom_reservation: None,
    };
    let ptr = Box::into_raw(Box::new(handle)) as i64;

    // NOTE: gate acquired on CPU here — acceptable for this deprecated benchmark-only path.
    // Production uses df_execute_with_context which acquires the gate on IO for backpressure.
    let partition_weight = num_partitions.max(1) as u32;
    let gate = cpu_executor.concurrency_gate().clone();
    let max_p = gate.max_permits();
    let permit = gate.acquire_many(partition_weight.min(max_p)).await;

    unsafe { execute_indexed_with_context(ptr, substrait_bytes, cpu_executor, permit).await }
}

// ── Helpers ───────────────────────────────────────────────────────────

/// Result of walking a logical plan looking for the leading top-of-plan ORDER BY.
///
/// `column` is the bare column name (no qualifier — we compare against `index.sort.field`
/// which is also unqualified). `descending` is `true` for `ORDER BY x DESC`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopSort {
    pub column: String,
    pub descending: bool,
}

/// Walk through the top of a logical plan to find the leading sort expression.
///
/// Descends through `Projection`, `Limit`, `SubqueryAlias`, `Distinct`, and `Filter` —
/// nodes that don't reorder rows or rewrite sort key columns. On reaching
/// `LogicalPlan::Sort`, returns the leading sort key's bare column name and direction.
/// Returns `None` if the plan has no top-level Sort, or if the first Sort key is not
/// a plain `Expr::Column` (e.g. `ORDER BY lower(x)` — we can't claim catalog monotonicity
/// after a function).
pub(crate) fn analyze_top_sort(plan: &datafusion::logical_expr::LogicalPlan) -> Option<TopSort> {
    use datafusion::logical_expr::{Expr, LogicalPlan};
    let mut current = plan;
    loop {
        match current {
            LogicalPlan::Sort(s) => {
                let leading = s.expr.first()?;
                let col = match &leading.expr {
                    Expr::Column(c) => c.name.clone(),
                    _ => return None,
                };
                return Some(TopSort {
                    column: col,
                    descending: !leading.asc,
                });
            }
            LogicalPlan::Projection(p) => current = p.input.as_ref(),
            LogicalPlan::Limit(l) => current = l.input.as_ref(),
            LogicalPlan::SubqueryAlias(a) => current = a.input.as_ref(),
            LogicalPlan::Distinct(d) => match d {
                datafusion::logical_expr::Distinct::All(input) => current = input.as_ref(),
                datafusion::logical_expr::Distinct::On(on) => current = on.input.as_ref(),
            },
            LogicalPlan::Filter(f) => current = f.input.as_ref(),
            _ => return None,
        }
    }
}

/// Decide whether to flip segment iteration order for the indexed scan.
///
/// Returns `true` iff the catalog has a sort declaration, the query has a top-level
/// ORDER BY whose leading key matches the catalog's leading sort field by name, and
/// the query's direction is the **opposite** of the catalog's. In that case the
/// segments — laid down newest-last by the writer — are in reverse order from what
/// the query wants, so iterating them tail-first feeds the largest values to a
/// `TopK` first and parquet page stats prune the rest.
///
/// All comparisons are case-sensitive on the field name (matching PR #22041's
/// `Column::from_name`). Direction comparison is case-insensitive on `"asc"`/`"desc"`.
pub(crate) fn should_reverse_segments(
    top_sort: Option<&TopSort>,
    sort_fields: &[String],
    sort_orders: &[String],
) -> bool {
    let Some(top) = top_sort else { return false };
    let Some(catalog_field) = sort_fields.first() else {
        return false;
    };
    let Some(catalog_order) = sort_orders.first() else {
        return false;
    };
    if top.column != *catalog_field {
        return false;
    }
    let catalog_descending = catalog_order.eq_ignore_ascii_case("desc");
    top.descending != catalog_descending
}

/// Reverse the iteration order of `segments` in place. Per-segment `global_base`
/// values are deliberately **left untouched** so each segment keeps its
/// catalog-order shard-global row ID space.
///
/// Why not recompute global_base after reversing?
///
/// `global_base` is the additive offset used to compute the QTF `__row_id__`:
/// `id = segment.global_base + position`. The fetch phase (`api::fetch_by_row_ids`)
/// rebuilds segments via `build_segments` against `ShardView.object_metas` (always
/// in catalog order) and reverses the mapping back via `partition_point` on
/// `global_base`. For the round trip to hold, both phases must agree on each
/// segment's `global_base` — and the only way to guarantee that without changing
/// the fetch path is to keep query-phase `global_base` at its catalog-order value.
///
/// Reversing only the iteration order — i.e. the order in which chunks/RGs are
/// scheduled by `compute_assignments` — is the *whole* point of this optimization
/// (newest-segment-first feeds TopK earlier and lets page stats prune older
/// segments). The `global_base` values are unrelated to that pruning win.
pub(crate) fn reverse_segment_iteration_order(segments: &mut [SegmentFileInfo]) {
    segments.reverse();
}

/// Collect all `Predicate(expr)` leaves in DFS order. Used by the
/// dispatcher to build a per-leaf `PruningPredicate` cache keyed by
/// `Arc::as_ptr` identity.
fn collect_predicate_exprs(tree: &BoolNode, out: &mut Vec<Arc<dyn PhysicalExpr>>) {
    match tree {
        BoolNode::And(c) | BoolNode::Or(c) => {
            c.iter().for_each(|ch| collect_predicate_exprs(ch, out))
        }
        BoolNode::Not(inner) => collect_predicate_exprs(inner, out),
        BoolNode::Collector { .. } => {}
        // Performance-delegated leaves contribute their original expression to the
        // PruningPredicate cache exactly like a plain Predicate — DF prunes pages
        // using the original expr; only the per-RG decision differs.
        BoolNode::DelegationPossible { original_expr, .. } => out.push(Arc::clone(original_expr)),
        BoolNode::Predicate(expr) => out.push(Arc::clone(expr)),
    }
}

/// Collect leaf predicate exprs from the extraction tree in a single traversal.
fn collect_leaf_exprs(extraction: Option<&ExtractionResult>) -> Vec<Arc<dyn PhysicalExpr>> {
    let Some(e) = extraction else { return vec![] };
    let mut exprs = Vec::new();
    collect_predicate_exprs(&e.tree, &mut exprs);
    exprs
}

fn collect_predicate_column_indices(extraction: Option<&ExtractionResult>) -> Vec<usize> {
    let Some(e) = extraction else { return vec![] };
    let mut exprs = Vec::new();
    collect_predicate_exprs(&e.tree, &mut exprs);
    collect_predicate_column_indices_from_exprs(&exprs)
}

fn collect_predicate_column_indices_from_exprs(exprs: &[Arc<dyn PhysicalExpr>]) -> Vec<usize> {
    let mut indices = HashSet::new();
    for expr in exprs {
        let _ = expr.apply(|node| {
            if let Some(col) = node.downcast_ref::<Column>() {
                indices.insert(col.index());
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    indices.into_iter().collect()
}

fn collect_predicate_column_names(
    extraction: Option<&ExtractionResult>,
    schema: &SchemaRef,
) -> Vec<String> {
    let Some(e) = extraction else { return vec![] };
    let mut exprs = Vec::new();
    collect_predicate_exprs(&e.tree, &mut exprs);
    let mut names = HashSet::new();
    for expr in &exprs {
        let _ = expr.apply(|node| {
            if let Some(col) = node.downcast_ref::<Column>() {
                if let Some(field) = schema.fields().get(col.index()) {
                    names.insert(field.name().to_string());
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    names.into_iter().collect()
}

fn collect_plan_column_names(plan: &datafusion::logical_expr::LogicalPlan) -> Vec<String> {
    let mut names = HashSet::new();
    let _ = plan.apply(|node| {
        // Output-schema columns of every node: this is what each node actually
        // emits / reads. Critically this captures `SELECT *` (and any projection
        // pushed into the scan), where no Projection expression lists the columns
        // but every column is still read. Expression-only collection misses them,
        // and a read column that gets only a placeholder OffsetIndex (instead of
        // its real multi-page one) corrupts the page read: arrow decodes the whole
        // column chunk as a single page → "output too small for decompressed data"
        // (or, upstream, the (0,0)-page byte-range subtract underflow).
        for field in node.schema().fields() {
            names.insert(field.name().to_string());
        }
        let _ = node.apply_expressions(|expr| {
            let _ = expr.apply(|e| {
                if let Expr::Column(col) = e {
                    names.insert(col.name().to_string());
                }
                Ok(TreeNodeRecursion::Continue)
            });
            Ok(TreeNodeRecursion::Continue)
        });
        Ok(TreeNodeRecursion::Continue)
    });
    names.into_iter().collect()
}

/// Build the `prune_tree_config` tuple from a BoolNode tree and schema.
/// Builds per-leaf PruningPredicates from pre-collected leaf exprs.
fn build_prune_tree_config(
    tree: &Arc<BoolNode>,
    schema: &SchemaRef,
    leaf_exprs: &[Arc<dyn PhysicalExpr>],
) -> Option<(
    Arc<BoolNode>,
    Arc<HashMap<usize, Arc<PruningPredicate>>>,
    SchemaRef,
)> {
    let leaf_predicates: HashMap<usize, Arc<PruningPredicate>> = leaf_exprs
        .iter()
        .filter_map(|expr| {
            build_pruning_predicate(expr, Arc::clone(schema))
                .map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
        })
        .collect();
    if leaf_predicates.is_empty() {
        return None;
    }
    Some((
        Arc::clone(tree),
        Arc::new(leaf_predicates),
        Arc::clone(schema),
    ))
}

/// For a tree classified as `SingleCollector`, walk it to find the single
/// Collector leaf and return its query bytes.
fn single_collector_id(tree: &BoolNode) -> Option<i32> {
    match tree {
        BoolNode::Collector { annotation_id } => Some(*annotation_id),
        BoolNode::And(children) => {
            for child in children {
                if let Some(id) = single_collector_id(child) {
                    return Some(id);
                }
            }
            None
        }
        _ => None,
    }
}

/// For a tree classified as `SingleCollector`, return the residual
/// (all non-Collector parts of the AND tree, re-assembled into a
/// single BoolNode). Recursively strips Collector leaves from nested
/// ANDs. Returns `None` if the tree is a bare Collector or the entire
/// tree is collectors-only (no residual predicates).
fn extract_single_collector_residual(tree: &BoolNode) -> Option<BoolNode> {
    fn strip_collectors(node: &BoolNode) -> Option<BoolNode> {
        match node {
            BoolNode::Collector { .. } => None,
            BoolNode::Predicate(_) => Some(node.clone()),
            BoolNode::And(children) => {
                let residuals: Vec<BoolNode> =
                    children.iter().filter_map(strip_collectors).collect();
                match residuals.len() {
                    0 => None,
                    1 => Some(residuals.into_iter().next().unwrap()),
                    _ => Some(BoolNode::And(residuals)),
                }
            }
            // OR/NOT with no collectors pass through unchanged (they're
            // pure-predicate subtrees in a SingleCollector-classified tree).
            other => Some(other.clone()),
        }
    }
    strip_collectors(tree)
}

// ── Placeholder provider used only for substrait consume pass ─────────

struct PlaceholderProvider {
    schema: SchemaRef,
}

impl fmt::Debug for PlaceholderProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderProvider").finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for PlaceholderProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Internal(
            "PlaceholderProvider should not be scanned".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::bool_tree::BoolNode;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
    use datafusion::physical_expr::PhysicalExpr;
    use std::sync::Arc;

    fn collector(id: i32) -> BoolNode {
        BoolNode::Collector { annotation_id: id }
    }

    fn pred() -> BoolNode {
        let left: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new("price", 0));
        let right: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(0))));
        BoolNode::Predicate(Arc::new(BinaryExpr::new(left, Operator::Eq, right)))
    }

    fn is_predicate(node: &BoolNode) -> bool {
        matches!(node, BoolNode::Predicate(_))
    }

    // ── extract_single_collector_residual ─────────────────────────────

    #[test]
    fn residual_bare_collector_is_none() {
        assert!(extract_single_collector_residual(&collector(10)).is_none());
    }

    #[test]
    fn residual_and_collector_plus_predicate() {
        let tree = BoolNode::And(vec![collector(10), pred()]);
        let r = extract_single_collector_residual(&tree).unwrap();
        assert!(is_predicate(&r));
    }

    #[test]
    fn residual_and_only_collectors_is_none() {
        let tree = BoolNode::And(vec![collector(10), collector(11)]);
        assert!(extract_single_collector_residual(&tree).is_none());
    }

    #[test]
    fn residual_nested_and_strips_collectors() {
        // AND(C₁, AND(C₂, P)) → residual is P
        let tree = BoolNode::And(vec![
            collector(10),
            BoolNode::And(vec![collector(11), pred()]),
        ]);
        let r = extract_single_collector_residual(&tree).unwrap();
        assert!(is_predicate(&r));
    }

    #[test]
    fn residual_deeply_nested_and() {
        // AND(P₁, AND(C₁, AND(C₂, P₂))) → AND(P₁, P₂)
        let p1 = pred();
        let p2 = pred();
        let tree = BoolNode::And(vec![
            p1,
            BoolNode::And(vec![collector(0), BoolNode::And(vec![collector(1), p2])]),
        ]);
        let r = extract_single_collector_residual(&tree).unwrap();
        match r {
            BoolNode::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(children.iter().all(is_predicate));
            }
            _ => panic!("expected AND, got {:?}", r),
        }
    }

    #[test]
    fn residual_nested_and_with_or_predicate() {
        // AND(C, AND(P, OR(P, P))) → AND(P, OR(P, P))
        let tree = BoolNode::And(vec![
            collector(10),
            BoolNode::And(vec![pred(), BoolNode::Or(vec![pred(), pred()])]),
        ]);
        let r = extract_single_collector_residual(&tree).unwrap();
        match r {
            BoolNode::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(is_predicate(&children[0]));
                assert!(matches!(children[1], BoolNode::Or(_)));
            }
            _ => panic!("expected AND, got {:?}", r),
        }
    }

    #[test]
    fn residual_nested_and_all_collectors_is_none() {
        // AND(AND(C₁, C₂), AND(C₃, C₄)) → no residual
        let tree = BoolNode::And(vec![
            BoolNode::And(vec![collector(0), collector(1)]),
            BoolNode::And(vec![collector(2), collector(3)]),
        ]);
        assert!(extract_single_collector_residual(&tree).is_none());
    }

    // ── analyze_top_sort / should_reverse_segments ────────────────────

    fn build_logical_plan(sql: &str) -> datafusion::logical_expr::LogicalPlan {
        use datafusion::execution::context::SessionContext;
        use datafusion::execution::SessionStateBuilder;
        let state = SessionStateBuilder::new().with_default_features().build();
        let ctx = SessionContext::new_with_state(state);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int32, false),
        ]));
        ctx.register_batch(
            "t",
            datafusion::arrow::record_batch::RecordBatch::new_empty(schema),
        )
        .expect("register_batch");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let df = rt.block_on(ctx.sql(sql)).expect("sql");
        df.into_unoptimized_plan()
    }

    #[test]
    fn analyze_top_sort_finds_leading_desc() {
        let plan = build_logical_plan("SELECT id FROM t ORDER BY id DESC");
        let ts = analyze_top_sort(&plan).expect("expected Sort");
        assert_eq!(ts.column, "id");
        assert!(ts.descending);
    }

    #[test]
    fn analyze_top_sort_descends_through_limit() {
        let plan = build_logical_plan("SELECT id FROM t ORDER BY id ASC LIMIT 10");
        let ts = analyze_top_sort(&plan).expect("expected Sort");
        assert_eq!(ts.column, "id");
        assert!(!ts.descending);
    }

    #[test]
    fn analyze_top_sort_descends_through_projection() {
        let plan = build_logical_plan("SELECT v FROM t ORDER BY ts DESC");
        let ts = analyze_top_sort(&plan).expect("expected Sort");
        assert_eq!(ts.column, "ts");
        assert!(ts.descending);
    }

    #[test]
    fn analyze_top_sort_returns_none_when_no_sort() {
        let plan = build_logical_plan("SELECT id FROM t");
        assert!(analyze_top_sort(&plan).is_none());
    }

    // ── collect_plan_column_names ─────────────────────────────────────

    /// Regression: `SELECT *` (and any scan that reads columns no Projection
    /// expression names) must yield ALL output columns. The scoped page-index
    /// load gives only these columns a REAL multi-page OffsetIndex; columns left
    /// out get a single-page placeholder, which is fine for pruning but CORRUPTS
    /// a real read — arrow decodes the whole chunk as one page ("output too small
    /// for decompressed data"), or underflows the read byte range. The
    /// `match() | sort | head` shape (q23) hit this: it reads every column but no
    /// expression lists them. Collecting each plan node's OUTPUT SCHEMA fixes it.
    #[test]
    fn collect_plan_column_names_includes_select_star_columns() {
        // No projection expressions name id/ts/v, but `SELECT *` reads all three.
        let plan = build_logical_plan("SELECT * FROM t WHERE v = 0 ORDER BY ts");
        let mut names = collect_plan_column_names(&plan);
        names.sort();
        assert_eq!(
            names,
            vec!["id".to_string(), "ts".to_string(), "v".to_string()],
            "every read column (full output schema) must be collected, not just expression columns"
        );
    }

    /// A narrow projection still collects exactly the columns the query touches
    /// (projected `v` + filter `id` + sort `ts`) — the scoping benefit is retained.
    #[test]
    fn collect_plan_column_names_collects_projected_and_referenced() {
        let plan = build_logical_plan("SELECT v FROM t WHERE id = 1 ORDER BY ts");
        let names = collect_plan_column_names(&plan);
        for expected in ["v", "id", "ts"] {
            assert!(
                names.iter().any(|n| n == expected),
                "expected column `{expected}` in collected names {names:?}"
            );
        }
    }

    #[test]
    fn analyze_top_sort_returns_none_for_function_sort_key() {
        // `ORDER BY abs(v)` — leading sort key isn't a plain column. Catalog monotonicity
        // doesn't transfer through a function, so we conservatively decline to reverse.
        let plan = build_logical_plan("SELECT id FROM t ORDER BY abs(v)");
        assert!(analyze_top_sort(&plan).is_none());
    }

    #[test]
    fn should_reverse_segments_matches_leading_field_opposite_direction() {
        let top = TopSort {
            column: "id".to_string(),
            descending: true,
        };
        let fields = vec!["id".to_string()];
        let orders = vec!["asc".to_string()];
        assert!(should_reverse_segments(Some(&top), &fields, &orders));
    }

    #[test]
    fn should_reverse_segments_matches_leading_field_same_direction() {
        let top = TopSort {
            column: "id".to_string(),
            descending: false,
        };
        let fields = vec!["id".to_string()];
        let orders = vec!["asc".to_string()];
        assert!(!should_reverse_segments(Some(&top), &fields, &orders));
    }

    #[test]
    fn should_reverse_segments_catalog_desc_query_asc() {
        let top = TopSort {
            column: "id".to_string(),
            descending: false,
        };
        let fields = vec!["id".to_string()];
        let orders = vec!["desc".to_string()];
        assert!(should_reverse_segments(Some(&top), &fields, &orders));
    }

    #[test]
    fn should_reverse_segments_no_query_sort() {
        let fields = vec!["id".to_string()];
        let orders = vec!["asc".to_string()];
        assert!(!should_reverse_segments(None, &fields, &orders));
    }

    #[test]
    fn should_reverse_segments_no_catalog_sort() {
        let top = TopSort {
            column: "id".to_string(),
            descending: true,
        };
        assert!(!should_reverse_segments(Some(&top), &[], &[]));
    }

    #[test]
    fn should_reverse_segments_query_sort_on_non_leading_catalog_field() {
        // Catalog: [a ASC, b ASC]; query: ORDER BY b DESC. Segments are monotonic on `a`
        // (the leading key), not `b`. Reversing won't help — decline.
        let top = TopSort {
            column: "b".to_string(),
            descending: true,
        };
        let fields = vec!["a".to_string(), "b".to_string()];
        let orders = vec!["asc".to_string(), "asc".to_string()];
        assert!(!should_reverse_segments(Some(&top), &fields, &orders));
    }

    #[test]
    fn should_reverse_segments_field_name_case_sensitive() {
        // Match PR #22041 — `Column::from_name` is case-sensitive. If casing differs,
        // we don't claim the catalog ordering applies. Safe default: no reversal.
        let top = TopSort {
            column: "ID".to_string(),
            descending: true,
        };
        let fields = vec!["id".to_string()];
        let orders = vec!["asc".to_string()];
        assert!(!should_reverse_segments(Some(&top), &fields, &orders));
    }

    // ── reverse_segment_iteration_order ───────────────────────────────

    fn dummy_segment(max_doc: i64, global_base: u64) -> SegmentFileInfo {
        use datafusion::parquet::file::metadata::{FileMetaData, ParquetMetaData};
        // Build a minimal ParquetMetaData. We never read it back in these tests.
        let schema = std::sync::Arc::new(
            datafusion::parquet::schema::types::SchemaDescriptor::new(std::sync::Arc::new(
                datafusion::parquet::schema::types::Type::group_type_builder("schema")
                    .build()
                    .unwrap(),
            )),
        );
        let file_meta = FileMetaData::new(0, 0, None, None, schema, None);
        let pq_meta = ParquetMetaData::new(file_meta, vec![]);
        SegmentFileInfo {
            writer_generation: global_base as i64 + 1, // arbitrary, just to vary
            max_doc,
            object_path: object_store::path::Path::from(format!("seg-{}.parquet", global_base)),
            parquet_size: 0,
            row_groups: vec![],
            metadata: std::sync::Arc::new(pq_meta),
            global_base,
            sort_min: None,
            sort_max: None,
        }
    }

    #[test]
    fn reverse_segments_preserves_global_base() {
        // Original: A(max_doc=10, base=0), B(max_doc=20, base=10), C(max_doc=30, base=30).
        // Reversal must keep each segment's original `global_base` intact so QTF row IDs
        // emitted in query phase remain interpretable by the fetch phase (which always
        // computes catalog-order bases).
        let mut segs = vec![
            dummy_segment(10, 0),
            dummy_segment(20, 10),
            dummy_segment(30, 30),
        ];
        reverse_segment_iteration_order(&mut segs);
        assert_eq!(segs.len(), 3);
        // New iteration order: C, B, A.
        assert_eq!(segs[0].max_doc, 30);
        assert_eq!(segs[0].global_base, 30); // C's catalog base, unchanged.
        assert_eq!(segs[1].max_doc, 20);
        assert_eq!(segs[1].global_base, 10); // B's catalog base, unchanged.
        assert_eq!(segs[2].max_doc, 10);
        assert_eq!(segs[2].global_base, 0); // A's catalog base, unchanged.
    }

    #[test]
    fn reverse_segments_empty_is_noop() {
        let mut segs: Vec<SegmentFileInfo> = vec![];
        reverse_segment_iteration_order(&mut segs);
        assert!(segs.is_empty());
    }

    #[test]
    fn reverse_segments_single_keeps_its_base() {
        let mut segs = vec![dummy_segment(42, 7)];
        reverse_segment_iteration_order(&mut segs);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].global_base, 7);
        assert_eq!(segs[0].max_doc, 42);
    }
}

/// Instruction-based indexed execution path. Consumes a pre-configured SessionContextHandle
/// (with UDF registered and IndexedExecutionConfig set) and routes to the appropriate
/// evaluator based on the Java-provided FilterTreeShape.
///
/// TODO: extract shared logic with `execute_indexed_query` to avoid duplication.
/// For now this delegates to the existing function by reconstructing the needed args
/// from the handle.
pub async unsafe fn execute_indexed_with_context(
    session_ctx_ptr: i64,
    substrait_bytes: Vec<u8>,
    cpu_executor: DedicatedExecutor,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<i64, DataFusionError> {
    let handle =
        *Box::from_raw(session_ctx_ptr as *mut crate::session_context::SessionContextHandle);
    let context_id = handle.query_context.context_id();
    let token = crate::query_tracker::get_cancellation_token(context_id);

    let query_future =
        execute_indexed_with_context_inner(handle, substrait_bytes, cpu_executor, permit);
    crate::cancellation::cancellable(token.as_ref(), context_id, query_future)
        .await
        .map_err(DataFusionError::Execution)
}

async unsafe fn execute_indexed_with_context_inner(
    handle: crate::session_context::SessionContextHandle,
    substrait_bytes: Vec<u8>,
    cpu_executor: DedicatedExecutor,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<i64, DataFusionError> {
    // Permit was acquired by the caller (ffm.rs) on the IO runtime before
    // spawning on the CPU runtime, so the Java search thread blocks at the
    // gate when it is full — creating backpressure at the Java threadpool level.

    // Empty shard: skip build_segments (errors on zero files) and emit an
    // empty stream. Mirrors the guard in query_executor::execute_with_context.
    if handle.object_metas.is_empty() {
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::ExecutionPlan;
        let context_id_early = handle.query_context.context_id();
        // engine-native-merge: borrow the partial-state schema from the prepared plan so the
        // empty stream matches the populated shards' wire shape (e.g. Binary HLL for dc()).
        let plan_schema: arrow::datatypes::SchemaRef =
            if let Some(prepared) = handle.prepared_plan.as_ref() {
                Arc::new(prepared.schema().as_ref().clone())
            } else {
                let plan = Plan::decode(substrait_bytes.as_slice())
                    .map_err(|e| DataFusionError::Execution(format!("decode substrait: {}", e)))?;
                let logical_plan = from_substrait_plan(&handle.ctx.state(), &plan).await?;
                Arc::new(logical_plan.schema().as_arrow().clone())
            };
        let plan_schema = crate::schema_coerce::coerce_inferred_schema(plan_schema);
        let empty_exec = EmptyExec::new(Arc::clone(&plan_schema));
        let df_stream = empty_exec.execute(0, handle.ctx.task_ctx())?;
        let (cross_rt_stream, abort_handle, _task_done) =
            CrossRtStream::new_with_df_error_stream_cancellable(
                df_stream,
                cpu_executor.clone(),
                None,
            );
        if let Some(h) = abort_handle {
            crate::query_tracker::set_abort_handle(context_id_early, h);
        }
        if let Some(rt) = cpu_executor.handle() {
            crate::query_tracker::set_cpu_runtime_handle(context_id_early, rt);
        }
        let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            cross_rt_stream.schema(),
            cross_rt_stream,
        );
        let stream_handle = crate::api::QueryStreamHandle::with_session_context(
            wrapped,
            handle.query_context,
            handle.ctx,
            Some(permit),
        );
        return Ok(Box::into_raw(Box::new(stream_handle)) as i64);
    }

    // Java-side QTF signal: scan must emit __row_id__. Captured before consuming indexed_config below.
    let requests_row_ids = handle
        .indexed_config
        .as_ref()
        .is_some_and(|c| c.requests_row_ids);
    let classification_override = handle.indexed_config.map(|config| {
        // FilterTreeShape: 1 = CONJUNCTIVE → SingleCollector, 2 = INTERLEAVED → Tree.
        match (config.tree_shape, config.delegated_predicate_count) {
            (1, _) => FilterClass::SingleCollector,
            (2, _) => FilterClass::Tree,
            _ => FilterClass::None,
        }
    });

    let query_config = Arc::new(handle.query_config);
    let num_partitions = query_config.target_partitions.max(1);
    let aggregate_mode = handle.aggregate_mode;
    let ctx = handle.ctx;
    let table_name = handle.table_name;
    let table_path = handle.table_path;
    let object_metas = handle.object_metas;
    let writer_generations = handle.writer_generations;
    let sort_fields = handle.sort_fields;
    let sort_orders = handle.sort_orders;
    let query_context = handle.query_context;
    let io_handle = handle.io_handle;
    // Extract context_id early so it can be captured by the per-segment closures
    // below. The closures pass it through every FFM upcall so Java can route each
    // callback to the correct per-query FilterDelegationHandle and DelegationThreadTracker.
    let context_id = query_context.context_id();

    // The substrait scan binds to the plan's NamedTable name (the alias/pattern like "tb-1,tb-2"
    // for multi-index queries, the concrete name otherwise), not the per-shard table_name.
    // create_session_context registered the provider under that name, so re-register under the
    // same name. Using table_name for a multi-index query leaves the scan unbound, so DataFusion
    // never consults supports_filters_pushdown and keeps the delegated_predicate FilterExec —
    // which then executes the marker UDF and errors.
    let register_name = crate::api::first_named_table_name(substrait_bytes.as_slice())
        .unwrap_or_else(|| table_name.clone());

    // SessionContext already has RuntimeEnv, caches, memory pool, UDF from create_session_context_indexed.
    // Deregister the default ListingTable (registered by create_session_context) — will be replaced
    // with IndexedTableProvider after plan decoding.
    ctx.deregister_table(&register_name)?;

    let store = ctx.state().runtime_env().object_store(&table_path)?;

    let state = ctx.state();
    let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();

    let (segments, schema) = build_segments(
        &state,
        Arc::clone(&store),
        object_metas.as_ref(),
        writer_generations.as_ref(),
        metadata_cache,
        &sort_fields,
    )
    .await
    .map_err(DataFusionError::Execution)?;
    let schema = crate::schema_coerce::coerce_inferred_schema(schema);
    // Widen to the plan's base_schema so columns absent from this shard's parquet (cross-shard drift) are null-filled at read time.
    let schema = crate::session_context::widen_schema_from_plan(
        &ctx,
        &substrait_bytes,
        &register_name,
        &schema,
    );

    let placeholder: Arc<dyn TableProvider> = Arc::new(PlaceholderProvider {
        schema: schema.clone(),
    });
    ctx.register_table(&register_name, placeholder)?;

    let plan = Plan::decode(substrait_bytes.as_slice())
        .map_err(|e| DataFusionError::Execution(format!("decode substrait: {}", e)))?;
    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;

    // Sort-aware segment iteration. Mirror of `ContextIndexSearcher.shouldUseTimeSeriesDescSortOptimization`
    // for the indexed-parquet path. When the index has `index.sort.field` and the query's leading
    // ORDER BY runs counter to the catalog's stored direction, reverse the segment vector so a
    // TopK above us pulls the highest-priority segment first and parquet page stats prune the rest.
    //
    // QTF safe: `reverse_segment_iteration_order` deliberately does NOT recompute `global_base`
    // — each segment retains its catalog-order base, so the row IDs query phase emits are still
    // interpretable by `api::fetch_by_row_ids` (which builds its own segments from
    // `ShardView.object_metas` in catalog order).
    let mut segments = segments;
    if should_reverse_segments(
        analyze_top_sort(&logical_plan).as_ref(),
        &sort_fields,
        &sort_orders,
    ) {
        log_debug!(
            "indexed_executor: reversing segment iteration (catalog leading sort={:?} {:?}, query opposite)",
            sort_fields.first(),
            sort_orders.first()
        );
        reverse_segment_iteration_order(&mut segments);
    }

    let emit_row_ids = requests_row_ids;
    let filter_expr = extract_filter_expr(&logical_plan);
    let extraction = match filter_expr {
        None => None,
        Some(ref expr) => Some(
            expr_to_bool_tree(expr, &schema, &state)
                .map_err(|e| DataFusionError::Execution(format!("expr_to_bool_tree: {}", e)))?,
        ),
    };

    // Resolve classification: from Java config if available, otherwise derive from tree
    let classification = match classification_override {
        Some(c) => c,
        None => match &extraction {
            None => FilterClass::None,
            Some(e) => classify_filter(&e.tree),
        },
    };
    // Derive the parquet pushdown predicate from the BoolNode tree.
    // `scan()` ignores DataFusion's filters argument (which contains
    // the `delegated_predicate` UDF marker whose body panics) and uses this
    // field instead.
    //
    // SingleCollector: residual (non-Collector top-AND children) →
    //   PhysicalExpr for `ParquetSource::with_predicate`. In
    //   row-granular mode parquet narrows Collector-matching rows via
    //   RowSelection and drops residual-failing rows via pushdown.
    //   In block-granular mode the evaluator's `on_batch_mask` applies
    //   both mask and residual post-decode, and pushdown is suppressed
    //   by the stream's `will_build_mask` guard (to avoid misalignment).
    // Tree: None — BitmapTreeEvaluator walks the whole BoolNode in
    //   `on_batch_mask` using arrow kernels; no pushdown needed.
    let pushdown_predicate: Option<Arc<dyn PhysicalExpr>> = match &classification {
        FilterClass::SingleCollector => extraction.as_ref().and_then(|e| {
            let residual_bool = extract_single_collector_residual(&e.tree);
            residual_bool
                .as_ref()
                .and_then(residual_bool_to_physical_expr)
        }),
        FilterClass::None => {
            // Predicate-only: push the whole tree (may be an unfoldable constant);
            // None = no filter = full scan.
            extraction
                .as_ref()
                .and_then(|e| residual_bool_to_physical_expr(&e.tree))
        }
        FilterClass::Tree => None,
    };

    let leaf_exprs = collect_leaf_exprs(extraction.as_ref());

    let predicate_columns = collect_predicate_column_indices_from_exprs(&leaf_exprs);

    // Augment each segment's footer-only metadata with a scoped page index so
    // the indexed PagePruner can page-prune. Both predicate (→ ColumnIndex) and
    // projection (→ OffsetIndex) are wired — a match()-only query still needs a
    // scoped OffsetIndex so the reader fetches only matched pages.
    if page_index::is_scoped_page_index_enabled() {
        let predicate_column_names = collect_predicate_column_names(extraction.as_ref(), &schema);
        let projection_column_names = collect_plan_column_names(&logical_plan);
        if !predicate_column_names.is_empty() || !projection_column_names.is_empty() {
            for segment in segments.iter_mut() {
                let (parquet_cols, offset_cols) = resolve_predicate_parquet_columns_pair(
                    &schema,
                    &segment.metadata,
                    &predicate_column_names,
                    &projection_column_names,
                );
                if parquet_cols.is_empty() && offset_cols.is_empty() {
                    continue;
                }
                if let Some(augmented) = load_scoped_page_index_cols(
                    &store,
                    &segment.object_path,
                    &segment.metadata,
                    &parquet_cols,
                    &offset_cols,
                )
                .await
                {
                    segment.metadata = augmented;
                }
            }
        }
    }

    let (factory, prune_tree_config): (EvaluatorFactory, _) = match classification {
        FilterClass::None => {
            // Predicate-only scan: page-pruned universe, residual applied in
            // on_batch_mask. Also covers an unfoldable constant (e.g. mktime('...') >
            // N) — no index column, but every row scanned and the constant applied as
            // residual (pushdown is Exact, so DataFusion drops the FilterExec).
            // Previously errored here when emit_row_ids was false (indexed path only).
            let schema_for_pruner = schema.clone();
            let prune_tree_config = extraction
                .as_ref()
                .and_then(|e| build_prune_tree_config(&e.tree, &schema_for_pruner, &leaf_exprs));
            let residual_expr: Option<Arc<dyn PhysicalExpr>> = extraction
                .as_ref()
                .and_then(|e| residual_bool_to_physical_expr(&e.tree));
            let residual_pruning_predicate: Option<Arc<PruningPredicate>> = residual_expr
                .as_ref()
                .and_then(|expr| build_pruning_predicate(expr, Arc::clone(&schema_for_pruner)));

            (
                Arc::new(
                    move |segment: &SegmentFileInfo,
                          chunk,
                          stream_metrics: &StreamMetrics,
                          stats_prune_tree: Option<&Arc<StatsPruneTree>>| {
                        let pruner = Arc::new(PagePruner::new(
                            &schema_for_pruner,
                            Arc::clone(&segment.metadata),
                        ));
                        let rg_index_to_pos: HashMap<usize, usize> = chunk
                            .row_group_indices
                            .iter()
                            .enumerate()
                            .map(|(pos, &idx)| (idx, pos))
                            .collect();
                        let eval: Arc<dyn RowGroupBitsetSource> =
                        Arc::new(crate::indexed_table::eval::predicate_evaluator::PredicateOnlyEvaluator::new(
                            pruner,
                            residual_pruning_predicate.clone(),
                            residual_expr.clone(),
                            Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                            stats_prune_tree.cloned(),
                            rg_index_to_pos,
                        ));
                        Ok(eval)
                    },
                ),
                prune_tree_config,
            )
        }
        FilterClass::SingleCollector => {
            let extraction = extraction.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "classify_filter returned SingleCollector but extraction is None".into(),
                )
            })?;
            let schema_for_pruner = schema.clone();
            let prune_tree_config =
                build_prune_tree_config(&extraction.tree, &schema_for_pruner, &leaf_exprs);

            // Correctness-delegated provider (eager). `None` when the query has only
            // performance-delegated leaves and no Collector at all.
            let correctness_provider: Option<Arc<ProviderHandle>> =
                match single_collector_id(&extraction.tree) {
                    Some(annotation_id) => Some(Arc::new(
                        create_provider(context_id, annotation_id)
                            .map_err(|e| DataFusionError::External(e.into()))?,
                    )),
                    None => None,
                };

            // Performance-delegated provider locks (lazy). Built ONCE per query,
            // shared across all per-(segment×chunk) closures via Arc::clone — so
            // multiple DataFusion threads racing to populate the same Lucene
            // Weight do so once per (query × annotation_id), not per chunk.
            // Drop releases the Lucene Weight via `releaseProvider`.
            let performance_provider_locks: Arc<
                std::collections::HashMap<i32, Arc<std::sync::OnceLock<ProviderHandle>>>,
            > = {
                let leaves = extraction.tree.delegation_possible_leaves();
                let mut map = std::collections::HashMap::with_capacity(leaves.len());
                for (annotation_id, _expr) in &leaves {
                    map.entry(*annotation_id)
                        .or_insert_with(|| Arc::new(std::sync::OnceLock::new()));
                }
                Arc::new(map)
            };

            // Extract the residual (non-Collector children of top-level
            // AND) as a BoolNode and convert to PhysicalExpr. Used for:
            //   - Page-stats pruning in candidate stage (via PruningPredicate).
            //   - Parquet `with_predicate` pushdown in row-granular mode.
            //   - `on_batch_mask` refinement in block-granular mode.
            //
            // SingleCollector is AND(Collector?, DelegationPossible*, residual*) so
            // the residual has zero Collectors — no Literal(true) substitution
            // needed (unlike bool_tree_to_pruning_expr which handles arbitrary
            // trees). DelegationPossible leaves contribute their original_expr
            // to the residual so DF gets to evaluate them natively.
            let residual_bool = extract_single_collector_residual(&extraction.tree);
            let residual_expr = residual_bool
                .as_ref()
                .and_then(residual_bool_to_physical_expr);
            let residual_pruning_predicate: Option<Arc<PruningPredicate>> = residual_expr
                .as_ref()
                .and_then(|expr| build_pruning_predicate(expr, Arc::clone(&schema_for_pruner)));

            let call_strategy = CollectorCallStrategy::PageRangeSplit;
            let bloom_store = Arc::clone(&store);
            let bloom_schema = schema.clone();
            (
                Arc::new(
                    move |segment: &SegmentFileInfo,
                          chunk,
                          stream_metrics: &StreamMetrics,
                          stats_prune_tree: Option<&Arc<StatsPruneTree>>| {
                        let collector_opt: Option<Arc<dyn RowGroupDocsCollector>> =
                            match &correctness_provider {
                                Some(provider) => {
                                    let collector = FfmSegmentCollector::create(
                                context_id,
                                provider.key(),
                                segment.writer_generation,
                                chunk.doc_min,
                                chunk.doc_max,
                            )
                            .map_err(|e| {
                                format!(
                                    "FfmSegmentCollector::create(context_id={}, provider={}, writer_generation={}, doc_range=[{},{})): {}",
                                    context_id,
                                    provider.key(),
                                    segment.writer_generation,
                                    chunk.doc_min,
                                    chunk.doc_max,
                                    e
                                )
                            })?;
                                    Some(Arc::new(collector) as Arc<dyn RowGroupDocsCollector>)
                                }
                                None => None,
                            };
                        let pruner = Arc::new(PagePruner::new(
                            &schema_for_pruner,
                            Arc::clone(&segment.metadata),
                        ));
                        // Bloom-filter row-group pruning is always enabled on the indexed read path.
                        let bloom_config =
                            Some(crate::indexed_table::eval::single_collector::BloomConfig {
                                store: Arc::clone(&bloom_store),
                                object_path: segment.object_path.clone(),
                                metadata: Arc::clone(&segment.metadata),
                                arrow_schema: Arc::clone(&bloom_schema),
                                io_handle: io_handle.clone(),
                                rg_bloom_pruned: stream_metrics.rg_bloom_pruned.clone(),
                                bloom_filter_eval_time: stream_metrics
                                    .bloom_filter_eval_time
                                    .clone(),
                            });
                        let eval: Arc<dyn RowGroupBitsetSource> =
                        Arc::new(SingleCollectorEvaluator::new(
                            collector_opt,
                            pruner,
                            residual_pruning_predicate.clone(),
                            residual_expr.clone(),
                            Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                            stream_metrics.ffm_collector_calls.clone(),
                            call_strategy,
                            Arc::clone(&performance_provider_locks),
                            segment.writer_generation,
                            Arc::new(crate::indexed_table::eval::single_collector::FfmDelegatedBackendCollectorFactory),
                            context_id,
                            bloom_config,
                            stats_prune_tree.cloned(),
                            chunk.row_group_indices.iter().enumerate().map(|(pos, &idx)| (idx, pos)).collect(),
                        ));
                        Ok(eval)
                    },
                ),
                prune_tree_config,
            )
        }
        FilterClass::Tree => {
            let extraction = extraction.ok_or_else(|| {
                DataFusionError::Internal(
                    "classify_filter returned Tree but extraction is None".into(),
                )
            })?;
            // Normalize: push NOTs to leaves (De Morgan) then flatten nested
            // same-kind connectives. Flatten after push_not_down so the
            // connective changes from De Morgan (e.g. NOT(AND(...)) -> OR(NOT...))
            // get absorbed into the surrounding Or if applicable.
            let tree = Arc::try_unwrap(extraction.tree)
                .unwrap()
                .push_not_down()
                .flatten();
            // One provider per Collector leaf (DFS order).
            let leaf_ids = tree.collector_leaves();
            let mut providers: Vec<Arc<ProviderHandle>> = Vec::with_capacity(leaf_ids.len());
            for annotation_id in &leaf_ids {
                providers.push(Arc::new(
                    create_provider(context_id, *annotation_id)
                        .map_err(|e| DataFusionError::External(e.into()))?,
                ));
            }
            let tree = Arc::new(tree);
            let schema_for_pruner = schema.clone();
            let cost_predicate = query_config.cost_predicate;
            let cost_collector = query_config.cost_collector;
            let max_collector_parallelism = 1;
            let collector_strategy = CollectorCallStrategy::TightenOuterBounds;

            // Build one `PruningPredicate` per unique `Predicate` leaf
            // in the tree. Key = `Arc::as_ptr(expr) as usize` — the
            // same `Arc<PhysicalExpr>` reaches the tree walker at
            // candidate stage. Predicates that fail to translate or
            // resolve to always-true are omitted; the walker's
            // fallback treats missing entries as "no pruning for this
            // leaf" (safe: universe bitmap).
            let mut leaf_exprs: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
            collect_predicate_exprs(&tree, &mut leaf_exprs);
            let pruning_predicates: Arc<HashMap<usize, Arc<PruningPredicate>>> = Arc::new(
                leaf_exprs
                    .iter()
                    .filter_map(|expr| {
                        let result = build_pruning_predicate(expr, Arc::clone(&schema_for_pruner));
                        result.map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
                    })
                    .collect(),
            );

            // Build prune_tree_config from the normalized tree. This ensures
            // StatsPruneTree children indices align with ResolvedNode children
            // (same push_not_down + flatten normalization applied above).
            let prune_tree_config = if pruning_predicates.is_empty() {
                None
            } else {
                Some((
                    Arc::clone(&tree),
                    Arc::clone(&pruning_predicates),
                    schema_for_pruner.clone(),
                ))
            };

            (
                Arc::new(
                    move |segment: &SegmentFileInfo,
                          chunk,
                          stream_metrics: &StreamMetrics,
                          stats_prune_tree: Option<&Arc<StatsPruneTree>>| {
                        // Build one collector per Collector leaf for this chunk.
                        let mut per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> =
                            Vec::with_capacity(providers.len());
                        for (idx, provider) in providers.iter().enumerate() {
                            let collector = FfmSegmentCollector::create(
                                context_id,
                                provider.key(),
                                segment.writer_generation,
                                chunk.doc_min,
                                chunk.doc_max,
                            )
                            .map_err(|e| format!("leaf {} collector: {}", idx, e))?;
                            per_leaf.push((
                                provider.key(),
                                Arc::new(collector) as Arc<dyn RowGroupDocsCollector>,
                            ));
                        }

                        let resolved = tree.resolve(&per_leaf).map_err(|e| {
                            format!(
                                "tree.resolve for segment gen={}: {}",
                                segment.writer_generation, e
                            )
                        })?;
                        let resolved = Arc::new(resolved);

                        let pruner = Arc::new(PagePruner::new(
                            &schema_for_pruner,
                            Arc::clone(&segment.metadata),
                        ));

                        let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                            tree: resolved,
                            evaluator: Arc::new(BitmapTreeEvaluator),
                            leaves: Arc::new(CollectorLeafBitmaps {
                                ffm_collector_calls: stream_metrics.ffm_collector_calls.clone(),
                            }),
                            page_pruner: pruner,
                            cost_predicate,
                            cost_collector,
                            max_collector_parallelism,
                            pruning_predicates: Arc::clone(&pruning_predicates),
                            page_prune_metrics: Some(PagePruneMetrics::from_stream_metrics(
                                stream_metrics,
                            )),
                            collector_strategy,
                            stats_prune_tree: stats_prune_tree.cloned(),
                            rg_index_to_pos: chunk
                                .row_group_indices
                                .iter()
                                .enumerate()
                                .map(|(pos, &idx)| (idx, pos))
                                .collect(),
                        });
                        Ok(eval)
                    },
                ),
                prune_tree_config,
            )
        }
    };

    ctx.deregister_table(&register_name)?;
    // Extract the scheme+authority portion of the table URL for
    // DataFusion's FileScanConfig. The full URL includes the path
    // (e.g. "file:///Users/.../parquet/"); ObjectStoreUrl wants only
    // the scheme+authority ("file:///").
    let url_str = table_path.as_str();
    let parsed = url::Url::parse(url_str)
        .map_err(|e| DataFusionError::Execution(format!("parse table_path URL: {}", e)))?;
    let store_url = ObjectStoreUrl::parse(format!("{}://{}", parsed.scheme(), parsed.authority()))?;

    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
        store: Arc::clone(&store),
        store_url,
        evaluator_factory: factory,
        pushdown_predicate,
        query_config: Arc::clone(&query_config),
        predicate_columns,
        emit_row_ids,
        prune_tree_config,
        sort_fields: sort_fields.clone(),
        sort_orders: sort_orders.clone(),
        cancellation_token: crate::query_tracker::get_cancellation_token(context_id),
    }));
    ctx.register_table(&register_name, provider)?;

    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;
    log_debug!(
        "DataFusion logical plan:\n{}",
        logical_plan.display_indent()
    );
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    // Retag bit-compatible Int↔UInt output mismatches to match the substrait-declared
    // types. The target is schema_coerce::coerce_inferred_schema(physical_schema) — same
    // narrowing the partition-stream registration uses, so consumer-side StreamingTable
    // and producer-side batches agree by construction (see crate::relabel_exec).
    // Apply aggregate mode stripping when prepare_partial_plan was called (engine-native-merge).
    // This makes the indexed executor produce Binary HLL state (Partial) instead of Int64 (Final).
    let physical_plan = if aggregate_mode != crate::agg_mode::Mode::Default {
        crate::agg_mode::apply_aggregate_mode(physical_plan, aggregate_mode, handle.has_topk)?
    } else {
        physical_plan
    };
    let target_schema = crate::schema_coerce::coerce_inferred_schema(physical_plan.schema());
    let physical_plan = crate::relabel_exec::wrap_if_relabel_needed(physical_plan, target_schema)?;
    log_debug!(
        "DataFusion physical plan:\n{}",
        displayable(physical_plan.as_ref()).indent(true)
    );
    let df_stream = execute_stream(physical_plan.clone(), ctx.task_ctx())
        .map_err(|e| DataFusionError::Execution(format!("execute_stream: {}", e)))?;

    let (cross_rt_stream, abort_handle, _task_done) =
        CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor.clone(), None);

    if let Some(h) = abort_handle {
        crate::query_tracker::set_abort_handle(context_id, h);
    }
    if let Some(rt) = cpu_executor.handle() {
        crate::query_tracker::set_cpu_runtime_handle(context_id, rt);
    }

    let schema = cross_rt_stream.schema();
    let wrapped = RecordBatchStreamAdapter::new(schema, cross_rt_stream);
    let stream_handle = crate::api::QueryStreamHandle::with_physical_plan(
        wrapped,
        query_context,
        ctx,
        Some(permit),
        physical_plan,
    );
    Ok(Box::into_raw(Box::new(stream_handle)) as i64)
}
