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
//!   1. `createProvider(query_bytes)` FFM upcall → `provider_key`  (once per
//!      Collector leaf, once per query).
//!   2. `createCollector(provider_key, seg, min, max)` FFM upcall → collector
//!      (once per SegmentChunk × Collector leaf).
//!   3. `collectDocs(collector, min, max, out)` FFM upcall (once per row group).
//!   4. `releaseCollector(collector)` when RG scan completes.
//!   5. `releaseProvider(provider_key)` when the tree is dropped.

use std::sync::Arc;

use datafusion::{
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
    execution::cache::cache_manager::CacheManagerConfig,
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
use crate::indexed_table::eval::{RowGroupBitsetSource, TreeBitsetSource};
use crate::indexed_table::ffm_callbacks::{create_provider, FfmSegmentCollector, ProviderHandle};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::segment_info::build_segments;
use crate::indexed_table::substrait_to_tree::{
    classify_filter, create_index_filter_udf, expr_to_bool_tree, extract_filter_expr,
    ExtractionResult, FilterClass,
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
use crate::indexed_table::page_pruner::{build_pruning_predicate, PagePruneMetrics};

/// Execute an indexed query.
///
/// `shard_view` carries the segment's parquet paths (populated when the reader
/// was built from a catalog snapshot). `num_partitions` comes from the caller's
/// session config. `query_memory_pool` is the per-query tracker (same as
/// vanilla path) — `None` disables tracking and uses the global pool.
pub async fn execute_indexed_query(
    substrait_bytes: Vec<u8>,
    table_name: String,
    shard_view: &ShardView,
    num_partitions: usize,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
    query_memory_pool: Option<Arc<dyn MemoryPool>>,
    query_config: Arc<DatafusionQueryConfig>,
) -> Result<i64, DataFusionError> {
    // Share caches with the global runtime (same as vanilla path): list-files
    // pre-populated with the reader's object_metas, file-metadata and
    // file-statistics inherited from the global runtime for cross-query reuse.
    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    let table_scoped_path = TableScopedPath {
        table: None,
        path: shard_view.table_path.prefix().clone(),
    };
    list_file_cache.put(&table_scoped_path, shard_view.object_metas.clone());

    let mut runtime_env_builder = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_files_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        );
    if let Some(pool) = query_memory_pool {
        runtime_env_builder = runtime_env_builder.with_memory_pool(pool);
    }
    let runtime_env = runtime_env_builder
        .build()
        .map_err(|e| DataFusionError::Execution(format!("runtime env: {}", e)))?;

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
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(create_index_filter_udf());

    // Resolve the object store for this shard's table URL (file://, s3://,
    // gs://, ... whatever the global runtime has registered). We pass this
    // store down to the parquet bridge so per-RG reads go through it instead
    // of hitting the local filesystem directly.
    let store = ctx
        .state()
        .runtime_env()
        .object_store(&shard_view.table_path)?;

    let (segments, schema) = build_segments(Arc::clone(&store), shard_view.object_metas.as_ref())
        .await
        .map_err(DataFusionError::Execution)?;

    let placeholder: Arc<dyn TableProvider> = Arc::new(PlaceholderProvider {
        schema: schema.clone(),
    });
    ctx.register_table(&table_name, placeholder)?;

    let plan = Plan::decode(substrait_bytes.as_slice())
        .map_err(|e| DataFusionError::Execution(format!("decode substrait: {}", e)))?;
    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;

    let filter_expr = extract_filter_expr(&logical_plan);
    let extraction = match filter_expr {
        None => None,
        Some(ref expr) => Some(
            expr_to_bool_tree(expr, &schema)
                .map_err(|e| DataFusionError::Execution(format!("expr_to_bool_tree: {}", e)))?,
        ),
    };
    let classification = match &extraction {
        None => FilterClass::None,
        Some(e) => classify_filter(&e.tree),
    };

    // Derive the parquet pushdown predicate from the BoolNode tree.
    // `scan()` ignores DataFusion's filters argument (which contains
    // the `index_filter` UDF marker whose body panics) and uses this
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
        FilterClass::Tree | FilterClass::None => None,
    };

    let predicate_columns = collect_predicate_column_indices(extraction.as_ref());

    let factory: EvaluatorFactory = match classification {
        FilterClass::None => {
            return Err(DataFusionError::Execution(
                "execute_indexed_query called with no index_filter(...) in plan".into(),
            ));
        }
        FilterClass::SingleCollector => {
            let extraction = extraction.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "classify_filter returned SingleCollector but extraction is None".into(),
                )
            })?;
            let bytes = single_collector_bytes(&extraction.tree).ok_or_else(|| {
                DataFusionError::Internal(
                    "SingleCollector classified but leaf extraction failed".into(),
                )
            })?;
            let provider =
                Arc::new(create_provider(&bytes).map_err(|e| DataFusionError::External(e.into()))?);
            let schema_for_pruner = schema.clone();

            // Extract the residual (non-Collector children of top-level
            // AND) as a BoolNode and convert to PhysicalExpr. Used for:
            //   - Page-stats pruning in candidate stage (via PruningPredicate).
            //   - Parquet `with_predicate` pushdown in row-granular mode.
            //   - `on_batch_mask` refinement in block-granular mode.
            //
            // SingleCollector is always AND(Collector, residual...) so
            // the residual has zero Collectors — no Literal(true)
            // substitution needed (unlike bool_tree_to_pruning_expr
            // which handles arbitrary trees).
            let residual_bool = extract_single_collector_residual(&extraction.tree);
            let residual_expr = residual_bool
                .as_ref()
                .and_then(residual_bool_to_physical_expr);
            let residual_pruning_predicate: Option<Arc<PruningPredicate>> = residual_expr
                .as_ref()
                .and_then(|expr| build_pruning_predicate(expr, Arc::clone(&schema_for_pruner)));

            Arc::new(
                move |segment: &SegmentFileInfo, chunk, stream_metrics: &StreamMetrics| {
                    let collector = FfmSegmentCollector::create(
                        provider.key(),
                        segment.segment_ord,
                        chunk.doc_min,
                        chunk.doc_max,
                    )
                    .map_err(|e| {
                        format!(
                        "FfmSegmentCollector::create(provider={}, seg={}, doc_range=[{},{})): {}",
                        provider.key(),
                        segment.segment_ord,
                        chunk.doc_min,
                        chunk.doc_max,
                        e
                    )
                    })?;
                    let pruner = Arc::new(PagePruner::new(
                        &schema_for_pruner,
                        Arc::clone(&segment.metadata),
                    ));
                    let eval: Arc<dyn RowGroupBitsetSource> =
                        Arc::new(SingleCollectorEvaluator::new(
                            Arc::new(collector) as Arc<dyn RowGroupDocsCollector>,
                            pruner,
                            residual_pruning_predicate.clone(),
                            residual_expr.clone(),
                            Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                            stream_metrics.ffm_collector_calls.clone(),
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
            let leaf_bytes = tree.collector_leaves();
            let mut providers: Vec<Arc<ProviderHandle>> = Vec::with_capacity(leaf_bytes.len());
            for bytes in &leaf_bytes {
                providers.push(Arc::new(
                    create_provider(bytes).map_err(|e| DataFusionError::External(e.into()))?,
                ));
            }
            let tree = Arc::new(tree);
            let schema_for_pruner = schema.clone();
            let cost_predicate = query_config.cost_predicate;
            let cost_collector = query_config.cost_collector;
            let max_collector_parallelism = query_config.max_collector_parallelism;

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
                        build_pruning_predicate(expr, Arc::clone(&schema_for_pruner))
                            .map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
                    })
                    .collect(),
            );

            Arc::new(
                move |segment: &SegmentFileInfo, chunk, stream_metrics: &StreamMetrics| {
                    // Build one collector per Collector leaf for this chunk.
                    let mut per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> =
                        Vec::with_capacity(providers.len());
                    for (idx, provider) in providers.iter().enumerate() {
                        let collector = FfmSegmentCollector::create(
                            provider.key(),
                            segment.segment_ord,
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
                        format!("tree.resolve for segment {}: {}", segment.segment_ord, e)
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
    let url_str = shard_view.table_path.as_str();
    let parsed = url::Url::parse(url_str)
        .map_err(|e| DataFusionError::Execution(format!("parse table_path URL: {}", e)))?;
    let store_url = ObjectStoreUrl::parse(format!("{}://{}", parsed.scheme(), parsed.authority()))?;
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
        store: Arc::clone(&store),
        store_url,
        evaluator_factory: factory,
        target_partitions: num_partitions.max(1),
        force_strategy: query_config.force_strategy,
        force_pushdown: query_config.force_pushdown,
        pushdown_predicate,
        query_config: Arc::clone(&query_config),
        predicate_columns,
    }));
    ctx.register_table(&table_name, provider)?;

    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let df_stream = execute_stream(physical_plan, ctx.task_ctx())
        .map_err(|e| DataFusionError::Execution(format!("execute_stream: {}", e)))?;

    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
    let schema = cross_rt_stream.schema();
    let wrapped = RecordBatchStreamAdapter::new(schema, cross_rt_stream);
    Ok(Box::into_raw(Box::new(wrapped)) as i64)
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
            if let Some(col) = node.as_any().downcast_ref::<Column>() {
                indices.insert(col.index());
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    indices.into_iter().collect()
}
/// For a tree classified as `SingleCollector`, walk it to find the single
/// Collector leaf and return its query bytes.
fn single_collector_bytes(tree: &BoolNode) -> Option<Arc<[u8]>> {
    match tree {
        BoolNode::Collector { query_bytes } => Some(Arc::clone(query_bytes)),
        BoolNode::And(children) => {
            let mut all: Vec<Arc<[u8]>> = Vec::new();
            for child in children {
                if let Some(b) = single_collector_bytes(child) {
                    all.push(b);
                }
            }
            match all.len() {
                0 => None,
                1 => Some(all.remove(0)),
                _ => {
                    let mut merged = Vec::new();
                    for (i, b) in all.iter().enumerate() {
                        if i > 0 {
                            merged.push(b'\n');
                        }
                        merged.extend_from_slice(b);
                    }
                    Some(Arc::from(merged.as_slice()))
                }
            }
        }
        _ => None,
    }
}

/// For a tree classified as `SingleCollector`, return the residual
/// (all non-Collector children of the top-level AND, re-assembled into
/// a single BoolNode). Returns `None` if the tree is a bare Collector
/// (no residual).
fn extract_single_collector_residual(tree: &BoolNode) -> Option<BoolNode> {
    let children = match tree {
        BoolNode::And(c) => c,
        _ => return None,
    };
    let residuals: Vec<BoolNode> = children
        .iter()
        .filter(|c| !matches!(c, BoolNode::Collector { .. }))
        .cloned()
        .collect();
    match residuals.len() {
        0 => None,
        1 => Some(residuals.into_iter().next().unwrap()),
        _ => Some(BoolNode::And(residuals)),
    }
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
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
