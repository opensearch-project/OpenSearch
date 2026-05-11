/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Fuzz harness: run a generated tree through the real
//! `IndexedStream` pipeline and compare against the oracle.
//!
//! Shares one parquet corpus across iterations — the corpus build cost
//! is paid once per test, then many iterations run cheap tree
//! generation + execution against the same file.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array};
use datafusion::execution::context::SessionContext;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use futures::StreamExt;

use super::corpus::Corpus;
use super::oracle::oracle_evaluate;
use super::tree_gen::{collect_collector_tags, GeneratedTree};

use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::eval::bitmap_tree::{BitmapTreeEvaluator, CollectorLeafBitmaps};
use crate::indexed_table::eval::single_collector::SingleCollectorEvaluator;
use crate::indexed_table::eval::{RowGroupBitsetSource, TreeBitsetSource};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::stream::{FilterStrategy, RowGroupInfo};
use crate::indexed_table::substrait_to_tree::{classify_filter, FilterClass};
use crate::indexed_table::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};

/// Same mock collector as the rest of tests_e2e uses. Takes a
/// pre-computed set of absolute doc ids and produces a packed u64
/// bitset over `[min_doc, max_doc)`.
#[derive(Debug)]
struct MockCollector {
    matching: Vec<i32>,
}

impl RowGroupDocsCollector for MockCollector {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        let span = (max_doc - min_doc) as usize;
        let mut out = vec![0u64; span.div_ceil(64)];
        for &doc in &self.matching {
            if doc >= min_doc && doc < max_doc {
                let rel = (doc - min_doc) as usize;
                out[rel / 64] |= 1u64 << (rel % 64);
            }
        }
        Ok(out)
    }
}

/// A set of loaded segments derived from the corpus. Built once per
/// fuzz test and reused across all iterations.
pub(in crate::indexed_table::tests_e2e) struct LoadedSegment {
    pub segments: Vec<SegmentFileInfo>,
    pub schema: datafusion::arrow::datatypes::SchemaRef,
}

/// Load the corpus's parquet files into `SegmentFileInfo`s. Each
/// segment gets `segment_ord = i` and a `first_row` reflecting its
/// offset in the global doc-id space (so Collector doc-ids keep
/// working across segments).
pub(in crate::indexed_table::tests_e2e) fn load_segment(corpus: &Corpus) -> LoadedSegment {
    let mut segments = Vec::with_capacity(corpus.parquet_files.len());
    let mut schema_out: Option<datafusion::arrow::datatypes::SchemaRef> = None;
    let mut global_first_row: i64 = 0;
    for (i, tmp) in corpus.parquet_files.iter().enumerate() {
        let path = tmp.path().to_path_buf();
        let size = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::File::open(&path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();
        if schema_out.is_none() {
            schema_out = Some(meta.schema().clone());
        }
        let parquet_meta = meta.metadata().clone();
        let mut rgs = Vec::new();
        let mut offset = global_first_row;
        let seg_rows = corpus.segment_row_counts[i];
        for j in 0..parquet_meta.num_row_groups() {
            let n = parquet_meta.row_group(j).num_rows();
            rgs.push(RowGroupInfo {
                index: j,
                first_row: offset,
                num_rows: n,
            });
            offset += n;
        }
        let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
        segments.push(SegmentFileInfo {
            segment_ord: i as i32,
            max_doc: seg_rows as i64,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            global_base: 0,
        });
        global_first_row += seg_rows as i64;
    }
    LoadedSegment {
        segments,
        schema: schema_out.expect("at least one segment"),
    }
}

/// Execute one tree end-to-end: wire mock collectors, build the table
/// provider, query it, return the set of `__doc_id` values that came
/// back.
pub(in crate::indexed_table::tests_e2e) async fn execute_tree(
    _corpus: &Corpus,
    loaded: &LoadedSegment,
    tree: &GeneratedTree,
) -> Vec<i32> {
    execute_tree_with(_corpus, loaded, tree, None).await
}

pub(in crate::indexed_table::tests_e2e) async fn execute_tree_with(
    _corpus: &Corpus,
    loaded: &LoadedSegment,
    tree: &GeneratedTree,
    force_strategy: Option<FilterStrategy>,
) -> Vec<i32> {
    execute_tree_with_plan(_corpus, loaded, tree, force_strategy)
        .await
        .0
}

/// Like `execute_tree_with` but also returns the ExecutionPlan so
/// tests can inspect metrics.
pub(in crate::indexed_table::tests_e2e) async fn execute_tree_with_plan(
    _corpus: &Corpus,
    loaded: &LoadedSegment,
    tree: &GeneratedTree,
    force_strategy: Option<FilterStrategy>,
) -> (Vec<i32>, Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    execute_tree_with_plan_pushdown(_corpus, loaded, tree, force_strategy, Some(false)).await
}

/// Like `execute_tree_with_plan` but allows overriding `force_pushdown`.
/// Used for diagnostic tests that want to exercise pushdown=ON on the
/// BitmapTreeEvaluator path, which default harness turns OFF.
pub(in crate::indexed_table::tests_e2e) async fn execute_tree_with_plan_pushdown(
    _corpus: &Corpus,
    loaded: &LoadedSegment,
    tree: &GeneratedTree,
    force_strategy: Option<FilterStrategy>,
    force_pushdown: Option<bool>,
) -> (Vec<i32>, Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    execute_tree_with_plan_pushdown_filter(
        _corpus,
        loaded,
        tree,
        force_strategy,
        force_pushdown,
        None,
    )
    .await
}

/// Full-control execution: optionally attach a logical `Expr` as the
/// query WHERE clause. When `Some`, DataFusion's planner pushes that
/// predicate through to `IndexedTableProvider::scan(filters)`, which
/// conjoins into `QueryShardExec.predicate` → `IndexedStream.predicate`
/// → parquet's `with_predicate`.
///
/// This mirrors production: in production, the substrait LogicalPlan
/// has a `Filter` node containing the original WHERE (both the
/// `index_filter(...)` UDF call AND parquet-native predicates).
/// DataFusion pushes the full conjunct down to `scan()`.
pub(in crate::indexed_table::tests_e2e) async fn execute_tree_with_plan_pushdown_filter(
    _corpus: &Corpus,
    loaded: &LoadedSegment,
    tree: &GeneratedTree,
    force_strategy: Option<FilterStrategy>,
    force_pushdown: Option<bool>,
    where_expr: Option<datafusion::logical_expr::Expr>,
) -> (Vec<i32>, Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    let bool_tree = tree.tree.clone().push_not_down();

    // Wire one mock collector per Collector leaf, matching DFS order.
    let tags = collect_collector_tags(&bool_tree);
    let collectors: Vec<Arc<dyn RowGroupDocsCollector>> = tags
        .iter()
        .map(|&tag| {
            Arc::new(MockCollector {
                matching: tree.collector_matches[tag as usize].clone(),
            }) as Arc<dyn RowGroupDocsCollector>
        })
        .collect();
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = collectors
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let bool_tree = Arc::new(bool_tree);
    let num_tags = tags.len();
    let seed = _corpus.config.seed;
    let cfg_max_parallelism = _corpus.config.max_collector_parallelism;
    let cfg_batch_size = _corpus.config.batch_size;
    let cfg_target_partitions = _corpus.config.target_partitions;

    let factory: EvaluatorFactory = {
        let per_leaf = per_leaf.clone();
        let tree = Arc::clone(&bool_tree);
        let schema = loaded.schema.clone();
        // Build per-leaf PruningPredicates the same way indexed_executor.rs
        // does in production, so our harness exercises real page-pruning
        // behavior instead of silently falling back to universe bitmaps.
        let mut leaf_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = Vec::new();
        collect_predicate_exprs_harness(&bool_tree, &mut leaf_exprs);
        let pruning_predicates: Arc<
            std::collections::HashMap<
                usize,
                Arc<datafusion::physical_optimizer::pruning::PruningPredicate>,
            >,
        > = Arc::new(
            leaf_exprs
                .iter()
                .filter_map(|expr| {
                    crate::indexed_table::page_pruner::build_pruning_predicate(expr, schema.clone())
                        .map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
                })
                .collect(),
        );
        Arc::new(move |segment, _chunk, stream_metrics| {
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(CollectorLeafBitmaps {
                    ffm_collector_calls: stream_metrics.ffm_collector_calls.clone(),
                }),
                page_pruner: pruner,
                cost_predicate: 1,
                cost_collector: 10,
                max_collector_parallelism: cfg_max_parallelism.unwrap_or(if num_tags > 1 {
                    // Multi-collector tree: randomly pick 1 (sequential) or
                    // up to 4 (parallel) to exercise PrecomputedLeafCache.
                    [1, 1, 2, 4][seed as usize % 4]
                } else {
                    1
                }),
                pruning_predicates: Arc::clone(&pruning_predicates),
                page_prune_metrics: Some(
                    crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(
                        stream_metrics,
                    ),
                ),
                collector_strategy: [
                    crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
                    crate::indexed_table::eval::CollectorCallStrategy::FullRange,
                    crate::indexed_table::eval::CollectorCallStrategy::PageRangeSplit,
                ][seed as usize % 3],
            });
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(cfg_target_partitions.max(1))
        .force_strategy(force_strategy)
        .force_pushdown(force_pushdown)
        .batch_size(cfg_batch_size.unwrap_or([128, 1024, 8192][seed as usize % 3]))
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: loaded.schema.clone(),
        segments: loaded.segments.clone(),
        store,
        store_url,
        evaluator_factory: factory,
        pushdown_predicate: None,
        query_config: Arc::new(qc),
        predicate_columns: collect_predicate_column_indices(&bool_tree),
        emit_row_ids: false,
    }));

    let ctx = SessionContext::new();
    // Register the index_filter UDF so any Expr::ScalarFunction
    // referencing it (when where_expr is set) type-checks.
    ctx.register_udf(crate::indexed_table::substrait_to_tree::create_index_filter_udf());
    ctx.register_table("t", provider).unwrap();
    let df = if let Some(filter) = where_expr {
        ctx.table("t").await.unwrap().filter(filter).unwrap()
    } else {
        ctx.sql("SELECT * FROM t").await.unwrap()
    };
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), task_ctx).unwrap();
    let mut doc_ids: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        // `__doc_id` is always column 0 in our corpus schema; SELECT *
        // preserves the schema order.
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("__doc_id is Int32");
        for i in 0..arr.len() {
            assert!(arr.is_valid(i), "__doc_id is non-null");
            doc_ids.push(arr.value(i));
        }
    }
    doc_ids.sort_unstable();
    (doc_ids, plan)
}

/// Run the tree through `SingleCollectorEvaluator` (production's fast
/// path). Works by:
///
/// 1. Extracting the single `Collector` tag + residual (non-Collector
///    children of the top AND).
/// 2. Converting the residual `BoolNode` to a logical `Expr` that
///    DataFusion's planner understands — `DataFrame::filter(expr)`
///    then wraps a `FilterExec` around our scan and passes the
///    predicate into `IndexedTableProvider::scan()` where it becomes
///    the physical `predicate` on `QueryShardExec`.
/// 3. Building the evaluator factory with a `MockCollector` replaying
///    the pre-picked match set, plus a residual `PruningPredicate` for
///    page-level pruning during prefetch.
/// 4. Parquet's decode-time `with_predicate` + DataFusion's outer
///    `FilterExec` enforce the residual on returned rows — the
///    evaluator itself only produces the Collector candidate bitmap.
///
/// Returns `None` when the tree doesn't classify as `SingleCollector`
/// (bare Collector, multi-Collector, OR at top level, NOT above
/// top-level AND, etc.).
pub(in crate::indexed_table::tests_e2e) async fn execute_tree_single_collector(
    _corpus: &Corpus,
    loaded: &LoadedSegment,
    tree: &GeneratedTree,
    force_strategy: Option<FilterStrategy>,
    call_strategy: crate::indexed_table::eval::single_collector::CollectorCallStrategy,
) -> Option<Vec<i32>> {
    // Match production: classify the tree in its un-normalized form.
    // Only proceed for trees that classify as SingleCollector WITHOUT
    // any De Morgan normalization — otherwise we'd be exercising a
    // code path that production never dispatches to for this shape.
    if !matches!(classify_filter(&tree.tree), FilterClass::SingleCollector) {
        return None;
    }
    let bool_tree = tree.tree.clone().push_not_down();

    // Extract Collector tag + residual (everything under top AND except
    // the Collector).
    let (tag, residual_bool) = extract_single_collector(&bool_tree)?;
    let residual_logical = bool_to_logical(&residual_bool)?;
    let residual_physical =
        crate::indexed_table::bool_tree::residual_bool_to_physical_expr(&residual_bool);
    let matching = tree.collector_matches[tag as usize].clone();
    let collector: Arc<dyn RowGroupDocsCollector> = Arc::new(MockCollector { matching });

    // Build residual page-pruning predicate (same as production's
    // SingleCollector path).
    use crate::indexed_table::page_pruner::build_pruning_predicate;
    let schema = loaded.schema.clone();
    let residual_pp = residual_physical
        .as_ref()
        .and_then(|expr| build_pruning_predicate(expr, schema.clone()));

    let factory: EvaluatorFactory = {
        let collector = Arc::clone(&collector);
        let schema = schema.clone();
        let residual_pp = residual_pp.clone();
        let residual_physical = residual_physical.clone();
        Arc::new(move |segment, _chunk, stream_metrics| {
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(SingleCollectorEvaluator::new(
                Arc::clone(&collector),
                pruner,
                residual_pp.clone(),
                residual_physical.clone(),
                Some(
                    crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(
                        stream_metrics,
                    ),
                ),
                stream_metrics.ffm_collector_calls.clone(),
                call_strategy,
            ));
            let _ = segment;
            Ok(eval)
        })
    };

    Some(run_single_collector_query(loaded, factory, residual_logical, force_strategy).await)
}

/// Execute `SELECT * FROM t WHERE <residual>` so DataFusion's planner
/// builds a `FilterExec` around `QueryShardExec`. The FilterExec
/// enforces the residual predicate authoritatively; parquet's own
/// `with_predicate` (threaded via `scan()`) does the decode-time
/// pushdown.
async fn run_single_collector_query(
    loaded: &LoadedSegment,
    factory: EvaluatorFactory,
    residual: datafusion::logical_expr::Expr,
    force_strategy: Option<FilterStrategy>,
) -> Vec<i32> {
    // Convert the residual logical Expr to a PhysicalExpr and stash
    // as pushdown_predicate — mirrors what `execute_indexed_query`
    // does in production via `residual_bool_to_physical_expr`.
    let df_schema = datafusion::common::DFSchema::try_from(loaded.schema.as_ref().clone()).unwrap();
    let execution_props = datafusion::execution::context::ExecutionProps::new();
    let pushdown_predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = Some(
        datafusion::physical_expr::create_physical_expr(&residual, &df_schema, &execution_props)
            .unwrap(),
    );

    let pred_cols: Vec<usize> = {
        use datafusion::common::tree_node::TreeNode;
        let mut indices = std::collections::BTreeSet::new();
        if let Some(ref pp) = pushdown_predicate {
            let _ = pp.apply(|node| {
                if let Some(col) = node
                    .as_any()
                    .downcast_ref::<datafusion::physical_expr::expressions::Column>()
                {
                    indices.insert(col.index());
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            });
        }
        indices.into_iter().collect()
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .force_strategy(force_strategy)
        .force_pushdown(Some(true))
        .batch_size([128, 1024, 8192][loaded.segments.len() % 3])
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: loaded.schema.clone(),
        segments: loaded.segments.clone(),
        store,
        store_url,
        evaluator_factory: factory,
        pushdown_predicate,
        query_config: Arc::new(qc),
        predicate_columns: pred_cols,
        emit_row_ids: false,
    }));
    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    // No WHERE clause — the pushdown_predicate above carries the
    // residual. `scan()` ignores filters anyway.
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), task_ctx).unwrap();
    let mut doc_ids: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        // Locate __doc_id column by name (filter path may reorder).
        let schema = b.schema();
        let idx = schema.index_of("__doc_id").expect("__doc_id in batch");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("__doc_id is Int32");
        for i in 0..arr.len() {
            assert!(arr.is_valid(i), "__doc_id is non-null");
            doc_ids.push(arr.value(i));
        }
    }
    doc_ids.sort_unstable();
    doc_ids
}

/// Extract `(tag, residual)` from a tree that classifies as
/// `SingleCollector`. `residual` is the AND of all non-Collector
/// children.
fn extract_single_collector(tree: &BoolNode) -> Option<(u8, BoolNode)> {
    let children = match tree {
        BoolNode::And(c) => c,
        _ => return None,
    };
    let mut tag: Option<u8> = None;
    let mut residuals: Vec<BoolNode> = Vec::new();
    for child in children {
        match child {
            BoolNode::Collector { annotation_id } => {
                if tag.is_some() {
                    return None;
                }
                tag = Some(*annotation_id as u8);
            }
            other => residuals.push(other.clone()),
        }
    }
    let t = tag?;
    let residual = match residuals.len() {
        0 => BoolNode::And(vec![]),
        1 => residuals.into_iter().next().unwrap(),
        _ => BoolNode::And(residuals),
    };
    Some((t, residual))
}

/// Convert a `BoolNode` (with no Collector leaves) into a DataFusion
/// logical `Expr` suitable for `DataFrame::filter`. Returns `None` if
/// the tree contains a Collector (shouldn't happen on a residual from
/// `extract_single_collector`) or an expression shape we can't lift.
fn bool_to_logical(node: &BoolNode) -> Option<datafusion::logical_expr::Expr> {
    use datafusion::logical_expr::{col, lit, Expr, Operator};
    use datafusion::physical_expr::expressions::{
        BinaryExpr as PhysBinaryExpr, Column as PhysColumn, InListExpr, IsNullExpr, LikeExpr,
        Literal as PhysLiteral,
    };

    fn lift_phys_to_logical(
        expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    ) -> Option<Expr> {
        let any = expr.as_any();
        if let Some(bin) = any.downcast_ref::<PhysBinaryExpr>() {
            let l = lift_phys_to_logical(bin.left())?;
            let r = lift_phys_to_logical(bin.right())?;
            return Some(Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                Box::new(l),
                *bin.op(),
                Box::new(r),
            )));
        }
        if let Some(c) = any.downcast_ref::<PhysColumn>() {
            return Some(col(c.name()));
        }
        if let Some(l) = any.downcast_ref::<PhysLiteral>() {
            return Some(lit(l.value().clone()));
        }
        if let Some(in_list) = any.downcast_ref::<InListExpr>() {
            let target = lift_phys_to_logical(in_list.expr())?;
            let list: Option<Vec<Expr>> = in_list.list().iter().map(lift_phys_to_logical).collect();
            return Some(Expr::InList(datafusion::logical_expr::expr::InList::new(
                Box::new(target),
                list?,
                in_list.negated(),
            )));
        }
        if let Some(is_null) = any.downcast_ref::<IsNullExpr>() {
            let inner = lift_phys_to_logical(is_null.arg())?;
            return Some(Expr::IsNull(Box::new(inner)));
        }
        if let Some(like) = any.downcast_ref::<LikeExpr>() {
            let target = lift_phys_to_logical(like.expr())?;
            let pattern = lift_phys_to_logical(like.pattern())?;
            return Some(Expr::Like(datafusion::logical_expr::expr::Like::new(
                like.negated(),
                Box::new(target),
                Box::new(pattern),
                None,
                like.case_insensitive(),
            )));
        }
        None
    }

    match node {
        BoolNode::And(children) => {
            // And of zero children = TRUE tautology.
            if children.is_empty() {
                return Some(lit(true));
            }
            let mut iter = children.iter();
            let mut acc = bool_to_logical(iter.next().unwrap())?;
            for c in iter {
                let next = bool_to_logical(c)?;
                acc = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                    Box::new(acc),
                    Operator::And,
                    Box::new(next),
                ));
            }
            Some(acc)
        }
        BoolNode::Or(children) => {
            if children.is_empty() {
                return Some(lit(false));
            }
            let mut iter = children.iter();
            let mut acc = bool_to_logical(iter.next().unwrap())?;
            for c in iter {
                let next = bool_to_logical(c)?;
                acc = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                    Box::new(acc),
                    Operator::Or,
                    Box::new(next),
                ));
            }
            Some(acc)
        }
        BoolNode::Not(inner) => {
            let e = bool_to_logical(inner)?;
            Some(Expr::Not(Box::new(e)))
        }
        BoolNode::Collector { .. } => None,
        BoolNode::Predicate(expr) => lift_phys_to_logical(expr),
    }
}

/// Shared tail: given a factory, build provider, SELECT *, return doc ids.
/// `force_pushdown` parameter forces on/off parquet's RowFilter pushdown
/// at decode time. `pushdown_predicate` provides the residual predicate
/// to hand parquet via `with_predicate` — mirrors what
/// `execute_indexed_query` computes from the BoolNode for production.
async fn run_with_factory(
    loaded: &LoadedSegment,
    factory: EvaluatorFactory,
    force_strategy: Option<FilterStrategy>,
    pushdown_predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
) -> Vec<i32> {
    run_with_factory_plan(
        loaded,
        factory,
        force_strategy,
        Some(false),
        pushdown_predicate,
    )
    .await
    .0
}

async fn run_with_factory_plan(
    loaded: &LoadedSegment,
    factory: EvaluatorFactory,
    force_strategy: Option<FilterStrategy>,
    force_pushdown: Option<bool>,
    pushdown_predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
) -> (Vec<i32>, Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .force_strategy(force_strategy)
        .force_pushdown(force_pushdown)
        .batch_size([256, 1024, 8192][loaded.segments.len() % 3])
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: loaded.schema.clone(),
        segments: loaded.segments.clone(),
        store,
        store_url,
        evaluator_factory: factory,
        pushdown_predicate,
        query_config: Arc::new(qc),
        predicate_columns: vec![],
        emit_row_ids: false,
    }));
    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), task_ctx).unwrap();
    let mut doc_ids: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("__doc_id is Int32");
        for i in 0..arr.len() {
            assert!(arr.is_valid(i), "__doc_id is non-null");
            doc_ids.push(arr.value(i));
        }
    }
    doc_ids.sort_unstable();
    (doc_ids, plan)
}

/// Pretty-print a tree to a debug string. Used in failure messages.
pub(in crate::indexed_table::tests_e2e) fn format_tree(tree: &BoolNode) -> String {
    format!("{:?}", tree)
}

/// Same walker as `indexed_executor::collect_predicate_exprs`, inlined
/// here so the harness can mirror production's per-leaf pruning setup.
fn collect_predicate_exprs_harness(
    tree: &BoolNode,
    out: &mut Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
) {
    match tree {
        BoolNode::And(c) | BoolNode::Or(c) => c
            .iter()
            .for_each(|ch| collect_predicate_exprs_harness(ch, out)),
        BoolNode::Not(inner) => collect_predicate_exprs_harness(inner, out),
        BoolNode::Collector { .. } => {}
        BoolNode::Predicate(expr) => out.push(Arc::clone(expr)),
    }
}

/// Mirrors `indexed_executor::collect_predicate_column_indices` — returns
/// the set of column indices referenced only by Predicate leaves.
fn collect_predicate_column_indices(tree: &BoolNode) -> Vec<usize> {
    use datafusion::common::tree_node::TreeNode;
    let mut exprs = Vec::new();
    collect_predicate_exprs_harness(tree, &mut exprs);
    let mut indices = std::collections::BTreeSet::new();
    for expr in &exprs {
        let _ = expr.apply(|node| {
            if let Some(col) = node
                .as_any()
                .downcast_ref::<datafusion::physical_expr::expressions::Column>()
            {
                indices.insert(col.index());
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        });
    }
    indices.into_iter().collect()
}

/// One iteration: generate a tree, evaluate via oracle + pipeline,
/// assert equal. Returns `Err(message)` on mismatch so the caller can
/// embed seed info in a panic.
pub(in crate::indexed_table::tests_e2e) async fn run_iteration(
    corpus: &Corpus,
    loaded: &LoadedSegment,
    tree: &GeneratedTree,
) -> Result<(), String> {
    run_iteration_impl(corpus, loaded, tree, /*determinism_check=*/ false).await
}

/// Like `run_iteration` but ALSO runs each strategy twice and asserts
/// identical output — catches non-determinism across partitions or
/// re-runs (ordering flakes, race conditions).
pub(in crate::indexed_table::tests_e2e) async fn run_iteration_twice(
    corpus: &Corpus,
    loaded: &LoadedSegment,
    tree: &GeneratedTree,
) -> Result<(), String> {
    run_iteration_impl(corpus, loaded, tree, /*determinism_check=*/ true).await
}

async fn run_iteration_impl(
    corpus: &Corpus,
    loaded: &LoadedSegment,
    tree: &GeneratedTree,
    determinism_check: bool,
) -> Result<(), String> {
    let expected = oracle_evaluate(tree, corpus);
    for strategy in [
        None,
        Some(FilterStrategy::RowSelection),
        Some(FilterStrategy::BooleanMask),
    ] {
        let actual = execute_tree_with(corpus, loaded, tree, strategy).await;
        if expected != actual {
            let diff_info = summarize_diff(&expected, &actual);
            return Err(format!(
                "BitmapTreeEvaluator vs oracle mismatch (strategy={:?}):\n  tree = {}\n  {}\n",
                strategy,
                format_tree(&tree.tree),
                diff_info,
            ));
        }
        if determinism_check {
            let actual2 = execute_tree_with(corpus, loaded, tree, strategy).await;
            if actual != actual2 {
                return Err(format!(
                    "non-deterministic output (strategy={:?}):\n  tree = {}\n  run1.len={} run2.len={}\n",
                    strategy,
                    format_tree(&tree.tree),
                    actual.len(),
                    actual2.len(),
                ));
            }
        }
    }
    // Cross-check: when the tree classifies as SingleCollector, run
    // through SingleCollectorEvaluator with every FilterStrategy ×
    // CollectorCallStrategy combination and assert all agree with the
    // oracle. This ensures FullRange, TightenOuterBounds, and
    // PageRangeSplit all produce identical results.
    use crate::indexed_table::eval::single_collector::CollectorCallStrategy;
    for strategy in [
        None,
        Some(FilterStrategy::RowSelection),
        Some(FilterStrategy::BooleanMask),
    ] {
        for call_strat in [
            CollectorCallStrategy::FullRange,
            CollectorCallStrategy::TightenOuterBounds,
            CollectorCallStrategy::PageRangeSplit,
        ] {
            if let Some(actual) =
                execute_tree_single_collector(corpus, loaded, tree, strategy, call_strat).await
            {
                if expected != actual {
                    let diff_info = summarize_diff(&expected, &actual);
                    return Err(format!(
                        "SingleCollectorEvaluator vs oracle mismatch (strategy={:?}, call={:?}):\n  tree = {}\n  {}\n",
                        strategy,
                        call_strat,
                        format_tree(&tree.tree),
                        diff_info,
                    ));
                }
            }
        }
    }
    // Dispatch fuzz: verify classify_filter's decision is consistent
    // with the evaluator paths. Tree-classified trees must NOT be
    // accepted by execute_tree_single_collector (which guards on
    // classify_filter internally). SingleCollector-classified trees
    // that the harness can handle (AND(Collector, predicates...)) are
    // already cross-checked above; bare Collectors and AND-of-only-
    // Collectors are valid SingleCollector shapes that the harness
    // doesn't implement, so we don't assert on those.
    let classification = classify_filter(&tree.tree);
    if classification == FilterClass::Tree {
        let sc_result = execute_tree_single_collector(
            corpus,
            loaded,
            tree,
            None,
            CollectorCallStrategy::FullRange,
        )
        .await;
        if sc_result.is_some() {
            return Err(format!(
                "classify_filter returned Tree but execute_tree_single_collector \
                 accepted the tree (should have returned None):\n  tree = {}\n",
                format_tree(&tree.tree),
            ));
        }
    }
    Ok(())
}

fn summarize_diff(expected: &[i32], actual: &[i32]) -> String {
    use std::collections::BTreeSet;
    let e: BTreeSet<i32> = expected.iter().copied().collect();
    let a: BTreeSet<i32> = actual.iter().copied().collect();
    let missing: Vec<_> = e.difference(&a).take(10).copied().collect();
    let extra: Vec<_> = a.difference(&e).take(10).copied().collect();
    format!(
        "expected={} actual={} missing(first 10)={:?} extra(first 10)={:?}",
        expected.len(),
        actual.len(),
        missing,
        extra
    )
}

#[cfg(test)]
mod tests {
    use super::super::{build_corpus, generate_tree, FixtureConfig};
    use super::*;
    use crate::indexed_table::eval::single_collector::CollectorCallStrategy;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[tokio::test]
    async fn harness_smoke_single_iteration() {
        let corpus = build_corpus(FixtureConfig::small(0xdead_beef_cafe));
        let loaded = load_segment(&corpus);
        let mut rng = StdRng::seed_from_u64(0xdead_beef_cafe);
        let tree = generate_tree(&mut rng, &corpus);
        run_iteration(&corpus, &loaded, &tree)
            .await
            .expect("one iteration should round-trip cleanly");
    }

    /// Metrics invariant: for a tree with at least one predicate on a
    /// numeric column, `pages_total > 0` means page pruning actually
    /// attempted to evaluate. If it's 0 → regression (pruner skipped
    /// silently).
    #[tokio::test]
    async fn harness_pages_total_nonzero_with_predicate() {
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_expr::PhysicalExpr;

        let corpus = build_corpus(FixtureConfig::small(0x4444));
        let loaded = load_segment(&corpus);

        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("price", 3));
        let lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(500))));
        let predicate = BoolNode::Predicate(Arc::new(BinaryExpr::new(col, Operator::Lt, lit)));
        let gt = GeneratedTree {
            tree: predicate,
            collector_matches: vec![],
        };
        // Use the normal harness path — it now wires pruning_predicates.
        let (_rows, plan) = execute_tree_with_plan(&corpus, &loaded, &gt, None).await;
        let pages_total = get_counter_from_plan(&plan, "pages_total");
        assert!(
            pages_total > 0,
            "pages_total was 0; page pruner never ran on price<500 predicate — regression?"
        );
    }

    // ... existing harness tests stay below

    /// Degenerate tree: AND(Collector, Predicate(price < 1000)).
    #[tokio::test]
    async fn harness_simple_and_collector_predicate() {
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_expr::PhysicalExpr;

        let corpus = build_corpus(FixtureConfig::small(0x1111));
        let loaded = load_segment(&corpus);
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("price", 3));
        let lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(1000))));
        let predicate = BoolNode::Predicate(Arc::new(BinaryExpr::new(col, Operator::Lt, lit)));
        let collector = BoolNode::Collector {
            annotation_id: 0,
        };
        let tree_node = BoolNode::And(vec![collector, predicate]);
        let matching: Vec<i32> = (0..100i32).collect();
        let gt = GeneratedTree {
            tree: tree_node,
            collector_matches: vec![matching.clone()],
        };
        run_iteration(&corpus, &loaded, &gt)
            .await
            .expect("simple AND(Collector, price<1000) must round-trip");
    }

    #[tokio::test]
    async fn harness_bare_collector() {
        let corpus = build_corpus(FixtureConfig::small(0x2222));
        let loaded = load_segment(&corpus);
        let collector = BoolNode::Collector {
            annotation_id: 0,
        };
        let matching: Vec<i32> = (0..100i32).collect();
        let gt = GeneratedTree {
            tree: collector,
            collector_matches: vec![matching.clone()],
        };
        run_iteration(&corpus, &loaded, &gt)
            .await
            .expect("bare Collector must round-trip");
    }

    #[tokio::test]
    async fn harness_bare_predicate() {
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_expr::PhysicalExpr;

        let corpus = build_corpus(FixtureConfig::small(0x3333));
        let loaded = load_segment(&corpus);
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("price", 3));
        let lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(500))));
        let predicate = BoolNode::Predicate(Arc::new(BinaryExpr::new(col, Operator::Lt, lit)));
        let gt = GeneratedTree {
            tree: predicate,
            collector_matches: vec![],
        };
        run_iteration(&corpus, &loaded, &gt)
            .await
            .expect("bare Predicate must round-trip");
    }

    /// Pin the fix for the pushdown+current_mask alignment bug
    /// (regression from seed `INDEXED_E2E_SEED=1e94955ce24bc83d`).
    ///
    /// 5% density → auto-strategy picks `min_skip_run=1024` →
    /// `current_mask` built from candidate bitmap over delivered
    /// rows. Pushdown must be disabled for this RG so the delivered
    /// rowset matches what the mask assumes. Without the fix in
    /// `IndexedStream::poll_next`, this fails with rows from the
    /// wrong positions.
    ///
    /// Tests all three strategies to ensure identical semantics.
    #[tokio::test]
    async fn harness_single_collector_density_5pct_all_strategies() {
        run_single_collector_density_test(5, 0xaaaa, 0xbbbb).await;
    }

    /// Sibling: 1% density → auto-strategy picks `min_skip_run=1`
    /// (row-granular). No `current_mask` built; pushdown stays ON.
    /// This tests the OTHER branch of the fix — regression-proofing
    /// against an accidental "turn off pushdown everywhere" change
    /// which would be correct but slow.
    #[tokio::test]
    async fn harness_single_collector_density_1pct_all_strategies() {
        run_single_collector_density_test(1, 0xcccc, 0xdddd).await;
    }

    /// **Specific Regression test.**
    ///
    /// Setup:
    ///   - `AND(Collector[5% density], price < 1000)`
    ///   - `force_strategy = BooleanMask` (block-granular, coalesced
    ///     selection → `current_mask` gets built from candidate bitmap)
    ///   - `force_pushdown = Some(true)` (parquet `with_predicate`
    ///     filters rows at decode time)
    ///
    /// Bug: pushdown drops rows at decode; `current_mask` is indexed
    /// over pre-pushdown rows; mask slicing misaligns; wrong rows
    /// emitted.
    ///
    /// With the fix (`will_build_mask → push=false`), this should
    /// pass because pushdown is disabled when `current_mask` would
    /// be built. If someone removes the fix, this test fails
    /// immediately.
    #[tokio::test]
    async fn harness_single_collector_pushdown_block_granular_must_align() {
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_expr::PhysicalExpr;

        let corpus = build_corpus(FixtureConfig::small(0x1f00d));
        let loaded = load_segment(&corpus);

        let price_idx = corpus.schema.index_of("price").unwrap();
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("price", price_idx));
        let lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(1000))));
        let predicate = BoolNode::Predicate(Arc::new(BinaryExpr::new(col, Operator::Lt, lit)));
        let collector_leaf = BoolNode::Collector {
            annotation_id: 0,
        };
        let tree_node = BoolNode::And(vec![collector_leaf, predicate]);

        // 5% density, uniform.
        use rand::rngs::StdRng;
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        let mut rng = StdRng::seed_from_u64(0x1beef_2222);
        let mut candidates: Vec<i32> = (0..corpus.num_rows() as i32).collect();
        candidates.shuffle(&mut rng);
        candidates.truncate(corpus.num_rows() / 20);
        candidates.sort_unstable();

        let gt = GeneratedTree {
            tree: tree_node,
            collector_matches: vec![candidates],
        };

        let expected = oracle_evaluate(&gt, &corpus);
        // Force BooleanMask strategy through the SingleCollector path.
        let actual = execute_tree_single_collector(
            &corpus,
            &loaded,
            &gt,
            Some(FilterStrategy::BooleanMask),
            CollectorCallStrategy::PageRangeSplit,
        )
        .await
        .expect("tree classifies as SingleCollector");
        assert_eq!(
            expected,
            actual,
            "pushdown+BooleanMask alignment bug: expected {} rows, got {}",
            expected.len(),
            actual.len()
        );
    }

    /// **Diagnostic: BitmapTreeEvaluator with a real WHERE clause
    /// pushed to parquet.**
    ///
    /// This mirrors production: construct a logical `Expr` containing
    /// both the `index_filter(...)` UDF call and a parquet-native
    /// predicate (`price < 1000`), pass it via `.filter(...)` so
    /// DataFusion calls `IndexedTableProvider::scan(filters=[...])`
    /// which conjoins and stashes as `QueryShardExec.predicate`,
    /// then passed to parquet's `with_predicate` for decode-time
    /// filtering.
    ///
    /// If BitmapTreeEvaluator shares any pushdown-alignment bug with
    /// the SingleCollector path, this surfaces it. Additionally, this
    /// is the only test that actually invokes parquet with a non-null
    /// `predicate` on the BitmapTree path — closer to production
    /// behavior.
    #[tokio::test]
    async fn harness_bitmap_tree_pushdown_block_granular_must_align() {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::{col, lit, Expr, Operator};
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_expr::PhysicalExpr;

        let corpus = build_corpus(FixtureConfig::small(0xcafe_1111));
        let loaded = load_segment(&corpus);

        let price_idx = corpus.schema.index_of("price").unwrap();
        let phys_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("price", price_idx));
        let phys_lit: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1000))));
        let predicate =
            BoolNode::Predicate(Arc::new(BinaryExpr::new(phys_col, Operator::Lt, phys_lit)));

        // Multi-collector → classifies as Tree path.
        let c1 = BoolNode::Collector {
            annotation_id: 0,
        };
        let c2 = BoolNode::Collector {
            annotation_id: 1,
        };
        let tree_node = BoolNode::And(vec![BoolNode::Or(vec![c1, c2]), predicate]);

        // Two collectors, 5% density each, uniform.
        use rand::rngs::StdRng;
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        let mut rng = StdRng::seed_from_u64(0xbeef_1111);
        let mut mkset = |rng: &mut StdRng| -> Vec<i32> {
            let mut v: Vec<i32> = (0..corpus.num_rows() as i32).collect();
            v.shuffle(rng);
            v.truncate(corpus.num_rows() / 20);
            v.sort_unstable();
            v
        };
        let s1 = mkset(&mut rng);
        let s2 = mkset(&mut rng);
        let gt = GeneratedTree {
            tree: tree_node,
            collector_matches: vec![s1, s2],
        };

        // Build the WHERE clause exactly as production would — the
        // original LogicalPlan's filter containing BOTH UDF calls AND
        // the residual. `index_filter(Binary([0])) OR index_filter(Binary([1]))`
        // matches the tree's OR(C1, C2); `price < 1000` is the residual.
        let idx_filter_udf = crate::indexed_table::substrait_to_tree::create_index_filter_udf();
        let c1_expr =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(idx_filter_udf.clone()),
                vec![lit(ScalarValue::Int32(Some(0)))],
            ));
        let c2_expr =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(idx_filter_udf),
                vec![lit(ScalarValue::Int32(Some(1)))],
            ));
        let or_expr = datafusion::logical_expr::or(c1_expr, c2_expr);
        let price_lt = col("price").lt(lit(ScalarValue::Int32(Some(1000))));
        let where_expr = datafusion::logical_expr::and(or_expr, price_lt);
        let _ = DataType::Int32; // silence unused import

        let expected = oracle_evaluate(&gt, &corpus);
        // Force pushdown ON, BooleanMask strategy, with a real WHERE clause.
        let (actual, _plan) = execute_tree_with_plan_pushdown_filter(
            &corpus,
            &loaded,
            &gt,
            Some(FilterStrategy::BooleanMask),
            Some(true),       // pushdown ON
            Some(where_expr), // real WHERE clause pushed to scan(filters)
        )
        .await;
        assert_eq!(
            expected,
            actual,
            "BitmapTree + pushdown ON + BooleanMask + real WHERE: expected {} rows, got {}",
            expected.len(),
            actual.len()
        );
    }

    /// Parameterized helper: build a Collector at `pct`% density,
    /// run `AND(Collector, price<1000)` through all 3 strategies,
    /// assert each matches the oracle.
    async fn run_single_collector_density_test(pct: usize, corpus_seed: u64, collector_seed: u64) {
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_expr::PhysicalExpr;

        let corpus = build_corpus(FixtureConfig::small(corpus_seed));
        let loaded = load_segment(&corpus);

        let price_idx = corpus.schema.index_of("price").unwrap();
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("price", price_idx));
        let lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(1000))));
        let predicate = BoolNode::Predicate(Arc::new(BinaryExpr::new(col, Operator::Lt, lit)));
        let collector_leaf = BoolNode::Collector {
            annotation_id: 0,
        };
        let tree_node = BoolNode::And(vec![collector_leaf, predicate]);

        // Uniform random subset at `pct`% density.
        use rand::rngs::StdRng;
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        let mut rng = StdRng::seed_from_u64(collector_seed);
        let mut candidates: Vec<i32> = (0..corpus.num_rows() as i32).collect();
        candidates.shuffle(&mut rng);
        candidates.truncate(corpus.num_rows() * pct / 100);
        candidates.sort_unstable();

        let gt = GeneratedTree {
            tree: tree_node,
            collector_matches: vec![candidates],
        };

        let expected = oracle_evaluate(&gt, &corpus);
        for strategy in [
            None,
            Some(FilterStrategy::RowSelection),
            Some(FilterStrategy::BooleanMask),
        ] {
            let actual = execute_tree_single_collector(
                &corpus,
                &loaded,
                &gt,
                strategy,
                CollectorCallStrategy::PageRangeSplit,
            )
            .await
            .expect("tree classifies as SingleCollector");
            assert_eq!(
                expected,
                actual,
                "density={}% strategy={:?}: expected {} rows, got {}",
                pct,
                strategy,
                expected.len(),
                actual.len()
            );
        }
    }

    /// Walks the plan and sums the named counter off QueryShardExec.
    /// Same pattern as `metrics.rs::aggregate_metrics` but inlined.
    fn get_counter_from_plan(
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        name: &str,
    ) -> usize {
        use datafusion::physical_plan::metrics::{MetricType, MetricsSet};
        let mut set = MetricsSet::new();
        fn walk(p: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, out: &mut MetricsSet) {
            if p.name() == "QueryShardExec" {
                if let Some(m) = p.metrics() {
                    for metric in m.iter() {
                        out.push(Arc::clone(metric));
                    }
                }
            }
            for child in p.children() {
                walk(child, out);
            }
        }
        walk(plan, &mut set);
        set.sum(|m| m.value().name() == name && m.metric_type() == MetricType::DEV)
            .map(|v| v.as_usize())
            .unwrap_or(0)
    }
}
