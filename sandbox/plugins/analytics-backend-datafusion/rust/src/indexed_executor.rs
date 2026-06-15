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

use native_bridge_common::log_debug;
use datafusion::{
    physical_plan::displayable,
    physical_plan::execute_stream,
    execution::SessionStateBuilder,
    execution::runtime_env::RuntimeEnvBuilder,
    execution::context::SessionContext,
    common::DataFusionError,
    prelude::*,
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::tree_node::{TreeNode, TreeNodeRecursion},
    datasource::{TableProvider, TableType},
    execution::cache::cache_manager::{CacheManagerConfig, CachedFileList},
    execution::cache::{CacheAccessor, DefaultListFilesCache, TableScopedPath},
    execution::memory_pool::MemoryPool,
    execution::object_store::ObjectStoreUrl,
    logical_expr::Expr,
    physical_expr::expressions::Column,
    physical_expr::PhysicalExpr,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::stream::RecordBatchStreamAdapter,
    physical_plan::ExecutionPlan
};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use prost::Message;
use substrait::proto::Plan;

use crate::api::DataFusionRuntime;
use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::eval::bitmap_tree::{BitmapTreeEvaluator, CollectorLeafBitmaps};
use crate::indexed_table::eval::single_collector::SingleCollectorEvaluator;
use crate::indexed_table::eval::{CollectorCallStrategy, RowGroupBitsetSource, TreeBitsetSource};
use crate::indexed_table::ffm_callbacks::{create_provider, FfmSegmentCollector, ProviderHandle};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::segment_info::build_segments;
use crate::indexed_table::substrait_to_tree::{
    classify_filter, create_index_filter_udf, expr_to_bool_tree,
    extract_filter_expr, ExtractionResult, FilterClass,
};
use crate::indexed_table::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use crate::api::ShardView;
use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::indexed_table::bool_tree::residual_bool_to_physical_expr;
use crate::indexed_table::metrics::StreamMetrics;
use crate::indexed_table::page_pruner::{build_pruning_predicate, PagePruneMetrics, StatsPruneTree};

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
    // Share caches with the global runtime (same as vanilla path): list-files
    // pre-populated with the reader's object_metas, file-metadata and
    // file-statistics inherited from the global runtime for cross-query reuse.
    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    let table_scoped_path = TableScopedPath {
        table: None,
        path: shard_view.table_path.prefix().clone(),
    };
    list_file_cache.put(&table_scoped_path, CachedFileList::new(shard_view.object_metas.as_ref().clone()));

    let mut runtime_env_builder = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_metadata_cache_limit(
                    runtime.runtime_env.cache_manager.get_metadata_cache_limit(),
                )
                .with_file_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        );
    if let Some(pool) = query_memory_pool {
        runtime_env_builder = runtime_env_builder.with_memory_pool(pool);
    }
    let runtime_env = runtime_env_builder
        .build()
        .map_err(|e| DataFusionError::Execution(format!("runtime env: {}", e)))?;

    // Register shard-specific object store on file:// scheme for this query.
    runtime_env.register_object_store(
        &url::Url::parse("file://").unwrap(),
        Arc::clone(&shard_view.store),
    );

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = query_config.parquet_pushdown_filters;
    // Indexed path fans out via IndexedExec partitions (derived from
    // num_partitions), not DataFusion's. But DF wants a sane value here
    // for any post-scan operators it may add.
    config.options_mut().execution.target_partitions = num_partitions.max(1);
    config.options_mut().execution.batch_size = query_config.batch_size;
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .with_physical_optimizer_rules(crate::agg_mode::physical_optimizer_rules_without_combine())
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(create_index_filter_udf());
    ctx.register_udf(crate::indexed_table::substrait_to_tree::create_delegation_possible_udf());
    crate::udf::register_all(&ctx);
    crate::udaf::register_all(&ctx);

    // Register default ListingTable so substrait consumer can resolve the table
    let listing_options = datafusion::datasource::listing::ListingOptions::new(
        Arc::new(datafusion::datasource::file_format::parquet::ParquetFormat::new()))
        .with_file_extension(".parquet")
        .with_collect_stat(true);
    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &shard_view.table_path)
        .await?;
    let resolved_schema = crate::schema_coerce::coerce_inferred_schema(resolved_schema);
    let table_config = datafusion::datasource::listing::ListingTableConfig::new(shard_view.table_path.clone())
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);
    let provider = Arc::new(datafusion::datasource::listing::ListingTable::try_new(table_config)?);
    ctx.register_table(&table_name, provider)?;

    // Build SessionContextHandle and delegate to execute_indexed_with_context
    let handle = crate::session_context::SessionContextHandle {
        ctx,
        table_path: shard_view.table_path.clone(),
        object_metas: shard_view.object_metas.clone(),
        writer_generations: shard_view.writer_generations.clone(),
        sort_fields: shard_view.sort_fields.clone(),
        sort_orders: shard_view.sort_orders.clone(),
        query_context: crate::query_tracker::QueryTrackingContext::new(context_id, runtime.runtime_env.memory_pool.clone(), crate::query_tracker::QueryType::Shard),
        table_name: table_name.clone(),
        indexed_config: None, // derive classification from tree
        query_config: Arc::unwrap_or_clone(query_config),
        io_handle: tokio::runtime::Handle::current(),
        aggregate_mode: crate::agg_mode::Mode::Default,
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

fn collect_predicate_column_indices(extraction: Option<&ExtractionResult>) -> Vec<usize> {
    let Some(e) = extraction else { return vec![] };
    let mut exprs = Vec::new();
    collect_predicate_exprs(&e.tree, &mut exprs);
    let mut indices = BTreeSet::new();
    for expr in &exprs {
        let _ = expr.apply(|node| {
            if let Some(col) = node.downcast_ref::<Column>() {
                indices.insert(col.index());
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    indices.into_iter().collect()
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
        BoolNode::Collector {
            annotation_id: id,
        }
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
            BoolNode::And(vec![
                collector(0),
                BoolNode::And(vec![collector(1), p2]),
            ]),
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
            BoolNode::And(vec![
                pred(),
                BoolNode::Or(vec![pred(), pred()]),
            ]),
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
    let handle = *Box::from_raw(session_ctx_ptr as *mut crate::session_context::SessionContextHandle);
    let context_id = handle.query_context.context_id();
    let token = crate::query_tracker::get_cancellation_token(context_id);

    let query_future = execute_indexed_with_context_inner(handle, substrait_bytes, cpu_executor, permit);
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
        let plan = Plan::decode(substrait_bytes.as_slice())
            .map_err(|e| DataFusionError::Execution(format!("decode substrait: {}", e)))?;
        let logical_plan = from_substrait_plan(&handle.ctx.state(), &plan).await?;
        let plan_schema: arrow::datatypes::SchemaRef =
            Arc::new(logical_plan.schema().as_arrow().clone());
        let plan_schema = crate::schema_coerce::coerce_inferred_schema(plan_schema);
        let empty_exec = EmptyExec::new(Arc::clone(&plan_schema));
        let df_stream = empty_exec.execute(0, handle.ctx.task_ctx())?;
        let (cross_rt_stream, abort_handle, _task_done) =
            CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor);
        if let Some(h) = abort_handle {
            crate::query_tracker::set_abort_handle(context_id_early, h);
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
    let requests_row_ids = handle.indexed_config.as_ref().is_some_and(|c| c.requests_row_ids);
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

    // SessionContext already has RuntimeEnv, caches, memory pool, UDF from create_session_context_indexed.
    // Deregister the default ListingTable (registered by create_session_context) — will be replaced
    // with IndexedTableProvider after plan decoding.
    ctx.deregister_table(&table_name)?;

    let store = ctx
        .state()
        .runtime_env()
        .object_store(&table_path)?;

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
    let schema = crate::session_context::widen_schema_from_plan(&ctx, &substrait_bytes, &table_name, &schema);

    let placeholder: Arc<dyn TableProvider> = Arc::new(PlaceholderProvider {
        schema: schema.clone(),
    });
    ctx.register_table(&table_name, placeholder)?;

    let plan = Plan::decode(substrait_bytes.as_slice())
        .map_err(|e| DataFusionError::Execution(format!("decode substrait: {}", e)))?;
    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;

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
            extraction.as_ref().and_then(|e| {
                residual_bool_to_physical_expr(&e.tree)
            })
        }
        FilterClass::Tree => None,
    };

    let prune_tree_config = extraction.as_ref().and_then(|e| {
        let mut leaf_exprs: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        collect_predicate_exprs(&e.tree, &mut leaf_exprs);
        let leaf_predicates: HashMap<usize, Arc<PruningPredicate>> = leaf_exprs
            .iter()
            .filter_map(|expr| {
                build_pruning_predicate(expr, schema.clone())
                    .map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
            })
            .collect();
        if leaf_predicates.is_empty() {
            return None;
        }
        Some((e.tree.clone(), Arc::new(leaf_predicates), schema.clone()))
    });

    let predicate_columns = collect_predicate_column_indices(extraction.as_ref());

    let factory: EvaluatorFactory = match classification {
        FilterClass::None => {
            // Predicate-only scan: page-pruned universe, residual applied in
            // on_batch_mask. Also covers an unfoldable constant (e.g. mktime('...') >
            // N) — no index column, but every row scanned and the constant applied as
            // residual (pushdown is Exact, so DataFusion drops the FilterExec).
            // Previously errored here when emit_row_ids was false (indexed path only).
            let schema_for_pruner = schema.clone();
            let residual_expr: Option<Arc<dyn PhysicalExpr>> = extraction.as_ref().and_then(|e| {
                residual_bool_to_physical_expr(&e.tree)
            });
            let residual_pruning_predicate: Option<Arc<PruningPredicate>> = residual_expr
                .as_ref()
                .and_then(|expr| build_pruning_predicate(expr, Arc::clone(&schema_for_pruner)));

            Arc::new(
                move |segment: &SegmentFileInfo, _chunk, stream_metrics: &StreamMetrics, stats_prune_tree: Option<&StatsPruneTree>| {
                    let pruner = Arc::new(PagePruner::new(
                        &schema_for_pruner,
                        Arc::clone(&segment.metadata),
                    ));
                    let eval: Arc<dyn RowGroupBitsetSource> =
                        Arc::new(crate::indexed_table::eval::predicate_evaluator::PredicateOnlyEvaluator::new(
                            pruner,
                            residual_pruning_predicate.clone(),
                            residual_expr.clone(),
                            Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                            stats_prune_tree.cloned(),
                        ));
                    Ok(eval)
                },
            )
        }
        FilterClass::SingleCollector => {
            let extraction = extraction.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "classify_filter returned SingleCollector but extraction is None".into(),
                )
            })?;
            let schema_for_pruner = schema.clone();

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

            let call_strategy = query_config.single_collector_strategy;
            let bloom_store = Arc::clone(&store);
            let bloom_schema = schema.clone();
            let bloom_on_read = query_config.bloom_filter_on_read;
            Arc::new(
                move |segment: &SegmentFileInfo, chunk, stream_metrics: &StreamMetrics, stats_prune_tree: Option<&StatsPruneTree>| {
                    let collector_opt: Option<Arc<dyn RowGroupDocsCollector>> = match &correctness_provider {
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
                    let bloom_config = if bloom_on_read {
                        Some(crate::indexed_table::eval::single_collector::BloomConfig {
                            store: Arc::clone(&bloom_store),
                            object_path: segment.object_path.clone(),
                            metadata: Arc::clone(&segment.metadata),
                            arrow_schema: Arc::clone(&bloom_schema),
                            io_handle: io_handle.clone(),
                            rg_bloom_pruned: stream_metrics.rg_bloom_pruned.clone(),
                            bloom_filter_eval_time: stream_metrics.bloom_filter_eval_time.clone(),
                        })
                    } else {
                        None
                    };
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
                        ));
                    Ok(eval)
                },
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
            let tree = extraction.tree.push_not_down().flatten();
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
            let max_collector_parallelism = query_config.max_collector_parallelism;
            let collector_strategy = query_config.tree_collector_strategy;

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

            Arc::new(
                move |segment: &SegmentFileInfo, chunk, stream_metrics: &StreamMetrics, stats_prune_tree: Option<&StatsPruneTree>| {
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
                        format!("tree.resolve for segment gen={}: {}", segment.writer_generation, e)
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
                    });
                    Ok(eval)
                },
            )
        }
    };

    ctx.deregister_table(&table_name)?;
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
    }));
    ctx.register_table(&table_name, provider)?;

    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;
    log_debug!("DataFusion logical plan:\n{}", logical_plan.display_indent());
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    // Retag bit-compatible Int↔UInt output mismatches to match the substrait-declared
    // types. The target is schema_coerce::coerce_inferred_schema(physical_schema) — same
    // narrowing the partition-stream registration uses, so consumer-side StreamingTable
    // and producer-side batches agree by construction (see crate::relabel_exec).
    // Apply aggregate mode stripping when prepare_partial_plan was called (engine-native-merge).
    // This makes the indexed executor produce Binary HLL state (Partial) instead of Int64 (Final).
    let physical_plan = if aggregate_mode != crate::agg_mode::Mode::Default {
        crate::agg_mode::apply_aggregate_mode(physical_plan, aggregate_mode)?
    } else {
        physical_plan
    };
    let target_schema = crate::schema_coerce::coerce_inferred_schema(physical_plan.schema());
    let physical_plan = crate::relabel_exec::wrap_if_relabel_needed(physical_plan, target_schema)?;
    log_debug!("DataFusion physical plan:\n{}", displayable(physical_plan.as_ref()).indent(true));
    let df_stream = execute_stream(physical_plan.clone(), ctx.task_ctx())
        .map_err(|e| DataFusionError::Execution(format!("execute_stream: {}", e)))?;

    let (cross_rt_stream, abort_handle, _task_done) =
        CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor);

    if let Some(h) = abort_handle {
        crate::query_tracker::set_abort_handle(context_id, h);
    }

    let schema = cross_rt_stream.schema();
    let wrapped = RecordBatchStreamAdapter::new(schema, cross_rt_stream);
    let stream_handle = crate::api::QueryStreamHandle::with_physical_plan(wrapped, query_context, ctx, Some(permit), physical_plan);
    Ok(Box::into_raw(Box::new(stream_handle)) as i64)
}
