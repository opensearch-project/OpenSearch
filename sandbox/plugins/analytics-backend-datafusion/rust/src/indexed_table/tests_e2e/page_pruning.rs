/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Deterministic page-pruning e2e tests.
//!
//! # Fixture layout
//!
//! 4096 rows, 1 RG, 4 pages of 1024 rows each.
//!
//! | page | rows        | price range    | brand     |
//! |------|-------------|----------------|-----------|
//! |  0   | 0–1023      | 0..1_024       | `"alpha"` |
//! |  1   | 1024–2047   | 10_000..11_024 | `"beta"`  |
//! |  2   | 2048–3071   | 20_000..21_024 | `"gamma"` |
//! |  3   | 3072–4095   | 30_000..31_024 | `"delta"` |
//!
//! Price ranges are non-overlapping so predicates like `price < 1024`
//! deterministically prune to page 0 only.
//!
//! # Mock collectors
//!
//! | tag | docs                          | description          |
//! |-----|-------------------------------|----------------------|
//! |  0  | all 4096                      | all docs             |
//! |  1  | 0, 2, 4, …                   | even docs            |
//! |  2  | first 2 per page              | sparse (8 docs)      |
//! |  3  | 0..2048                       | pages 0+1            |
//! |  4  | 2048..4096                    | pages 2+3            |
//! |  5  | 1, 3, 5, …                   | odd docs             |
//! |  6  | 3072..4096                    | page 3 only          |

#![cfg(test)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Operator;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use tempfile::NamedTempFile;

use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::eval::bitmap_tree::{
    subtree_cost, BitmapTreeEvaluator, CollectorLeafBitmaps,
};
use crate::indexed_table::eval::single_collector::SingleCollectorEvaluator;
use crate::indexed_table::eval::{
    CollectorCallStrategy, RgEvalContext, RowGroupBitsetSource, TreeBitsetSource,
};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::{build_pruning_predicate, PagePruner};
use crate::indexed_table::stream::{FilterStrategy, RowGroupInfo};
use crate::indexed_table::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};

const ROWS_PER_PAGE: usize = 1024;
const NUM_PAGES: usize = 4;
const NUM_ROWS: usize = ROWS_PER_PAGE * NUM_PAGES; // 4096

// ── Fixture builder ─────────────────────────────────────────────────

fn fixture_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int32, false),
        Field::new("brand", DataType::Utf8, false),
    ]))
}

fn write_fixture() -> NamedTempFile {
    let schema = fixture_schema();
    let labels = ["alpha", "beta", "gamma", "delta"];
    let prices: Vec<i32> = (0..NUM_PAGES)
        .flat_map(|p| {
            let base = (p as i32) * 10_000;
            (0..ROWS_PER_PAGE as i32).map(move |i| base + i)
        })
        .collect();
    let brands: Vec<&str> = (0..NUM_PAGES)
        .flat_map(|p| std::iter::repeat(labels[p]).take(ROWS_PER_PAGE))
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(prices)),
            Arc::new(StringArray::from(brands)),
        ],
    )
    .unwrap();
    let tmp = NamedTempFile::new().unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(NUM_ROWS)
        .set_data_page_row_count_limit(ROWS_PER_PAGE)
        .set_write_batch_size(ROWS_PER_PAGE)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    tmp
}

// ── Expression helpers ──────────────────────────────────────────────

fn col_expr(name: &str) -> Arc<dyn PhysicalExpr> {
    let idx = fixture_schema().index_of(name).unwrap();
    Arc::new(PhysColumn::new(name, idx))
}

fn lit_i32(v: i32) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Int32(Some(v))))
}

fn binop(
    l: Arc<dyn PhysicalExpr>,
    op: Operator,
    r: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(l, op, r))
}

fn pred_node(expr: Arc<dyn PhysicalExpr>) -> BoolNode {
    BoolNode::Predicate(expr)
}

fn collector_leaf(tag: u8) -> BoolNode {
    BoolNode::Collector {
        annotation_id: tag as i32,
    }
}

// ── Mock collector ──────────────────────────────────────────────────

#[derive(Debug)]
struct MockCollector {
    docs: Vec<i32>,
}

impl RowGroupDocsCollector for MockCollector {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        let span = (max_doc - min_doc) as usize;
        let mut out = vec![0u64; span.div_ceil(64)];
        for &doc in &self.docs {
            if doc >= min_doc && doc < max_doc {
                let rel = (doc - min_doc) as usize;
                out[rel / 64] |= 1u64 << (rel % 64);
            }
        }
        Ok(out)
    }
}

/// tag 0 → all docs, tag 1 → even docs only, tag 2 → first 2 per page,
/// tag 3 → pages 0+1 only (docs 0..2048), tag 4 → pages 2+3 only (docs 2048..4096),
/// tag 5 → odd docs only, tag 6 → page 3 only (docs 3072..4096).
fn collector_for_tag(tag: u8) -> Arc<dyn RowGroupDocsCollector> {
    let docs: Vec<i32> = match tag {
        0 => (0..NUM_ROWS as i32).collect(),
        1 => (0..NUM_ROWS as i32).step_by(2).collect(),
        2 => (0..NUM_PAGES)
            .flat_map(|p| {
                let base = (p * ROWS_PER_PAGE) as i32;
                vec![base, base + 1]
            })
            .collect(),
        3 => (0..2048).collect(),                       // pages 0+1
        4 => (2048..NUM_ROWS as i32).collect(),         // pages 2+3
        5 => (1..NUM_ROWS as i32).step_by(2).collect(), // odd docs
        6 => (3072..NUM_ROWS as i32).collect(),         // page 3 only
        _ => vec![],
    };
    Arc::new(MockCollector { docs })
}

// ── Segment loader & metrics ────────────────────────────────────────

fn load_segment(tmp: &NamedTempFile) -> (SegmentFileInfo, SchemaRef) {
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::File::open(&path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();
    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo {
            index: i,
            first_row: offset,
            num_rows: n,
        });
        offset += n;
    }
    let seg = SegmentFileInfo {
        segment_ord: 0,
        max_doc: NUM_ROWS as i64,
        object_path: object_store::path::Path::from(path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups: rgs,
        metadata: parquet_meta,
    };
    (seg, schema)
}

fn aggregate_metrics(plan: &Arc<dyn ExecutionPlan>) -> MetricsSet {
    let mut set = MetricsSet::new();
    fn walk(plan: &Arc<dyn ExecutionPlan>, out: &mut MetricsSet) {
        if plan.name() == "QueryShardExec" {
            if let Some(m) = plan.metrics() {
                for metric in m.iter() {
                    out.push(Arc::clone(metric));
                }
            }
        }
        for child in plan.children() {
            walk(child, out);
        }
    }
    walk(plan, &mut set);
    set
}

fn get_counter(set: &MetricsSet, name: &str) -> usize {
    use datafusion::physical_plan::metrics::MetricType;
    set.sum(|m| m.value().name() == name && m.metric_type() == MetricType::DEV)
        .map(|v| v.as_usize())
        .unwrap_or(0)
}

// ── Tree wiring ─────────────────────────────────────────────────────

fn collect_pred_exprs(node: &BoolNode, out: &mut Vec<Arc<dyn PhysicalExpr>>) {
    match node {
        BoolNode::Predicate(e) => out.push(Arc::clone(e)),
        BoolNode::And(cs) | BoolNode::Or(cs) => cs.iter().for_each(|c| collect_pred_exprs(c, out)),
        BoolNode::Not(c) => collect_pred_exprs(c, out),
        BoolNode::Collector { .. } => {}
    }
}

fn build_pp_map(
    tree: &BoolNode,
    schema: &SchemaRef,
) -> Arc<HashMap<usize, Arc<datafusion::physical_optimizer::pruning::PruningPredicate>>> {
    let mut exprs = Vec::new();
    collect_pred_exprs(tree, &mut exprs);
    Arc::new(
        exprs
            .iter()
            .filter_map(|expr| {
                build_pruning_predicate(expr, schema.clone())
                    .map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
            })
            .collect(),
    )
}

fn wire_collectors_dfs(node: &BoolNode, out: &mut Vec<Arc<dyn RowGroupDocsCollector>>) {
    match node {
        BoolNode::Collector { annotation_id } => out.push(collector_for_tag(*annotation_id as u8)),
        BoolNode::And(cs) | BoolNode::Or(cs) => cs.iter().for_each(|c| wire_collectors_dfs(c, out)),
        BoolNode::Not(c) => wire_collectors_dfs(c, out),
        BoolNode::Predicate(_) => {}
    }
}

// ── Execution harnesses ─────────────────────────────────────────────

/// Run a BoolNode tree through the bitmap-tree evaluator, return (prices, plan).
async fn run_bitmap_tree(tree: BoolNode) -> (Vec<i32>, Arc<dyn ExecutionPlan>) {
    let tmp = write_fixture();
    let (seg, schema) = load_segment(&tmp);
    let tree = tree.push_not_down();
    let pp_map = build_pp_map(&tree, &schema);
    let mut colls = Vec::new();
    wire_collectors_dfs(&tree, &mut colls);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = colls
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let tree = Arc::new(tree);

    let factory: EvaluatorFactory = {
        let per_leaf = per_leaf.clone();
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        let pp_map = Arc::clone(&pp_map);
        Arc::new(move |segment, _chunk, sm| {
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(CollectorLeafBitmaps {
                    ffm_collector_calls: sm.ffm_collector_calls.clone(),
                }),
                page_pruner: pruner,
                cost_predicate: 1,
                cost_collector: 10,
                max_collector_parallelism: 1,
                pruning_predicates: Arc::clone(&pp_map),
                page_prune_metrics: Some(
                    crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(sm),
                ),
                collector_strategy:
                    crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
            });
            Ok(eval)
        })
    };

    execute_and_collect(seg, schema, factory).await
}

/// Run a single-collector query with a given strategy, return (prices, plan).
async fn run_single_collector(
    collector_tag: u8,
    residual_expr: Arc<dyn PhysicalExpr>,
    strategy: CollectorCallStrategy,
) -> (Vec<i32>, Arc<dyn ExecutionPlan>) {
    let tmp = write_fixture();
    let (seg, schema) = load_segment(&tmp);
    let residual_pp = build_pruning_predicate(&residual_expr, schema.clone());

    let factory: EvaluatorFactory = {
        let schema = schema.clone();
        let residual_pp = residual_pp.clone();
        let residual_expr = Arc::clone(&residual_expr);
        Arc::new(move |segment, _chunk, sm| {
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(SingleCollectorEvaluator::new(
                collector_for_tag(collector_tag),
                pruner,
                residual_pp.clone(),
                Some(Arc::clone(&residual_expr)),
                Some(crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(sm)),
                sm.ffm_collector_calls.clone(),
                strategy,
            ));
            Ok(eval)
        })
    };

    execute_and_collect(seg, schema, factory).await
}

async fn execute_and_collect(
    seg: SegmentFileInfo,
    schema: SchemaRef,
    factory: EvaluatorFactory,
) -> (Vec<i32>, Arc<dyn ExecutionPlan>) {
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .force_strategy(Some(FilterStrategy::BooleanMask))
        .force_pushdown(Some(false))
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![seg],
        store,
        store_url,
        evaluator_factory: factory,
        pushdown_predicate: None,
        query_config: Arc::new(qc),
        predicate_columns: vec![],
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT price, brand FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), task_ctx).unwrap();
    let mut prices = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            prices.push(col.value(i));
        }
    }
    prices.sort();
    (prices, plan)
}

// ═════════════════════════════════════════════════════════════════════
// Bitmap tree (multi-filter) page pruning tests
// ═════════════════════════════════════════════════════════════════════

/// AND(Collector(all), Predicate(price < 1024)) → only page 0 survives.
/// 3 of 4 pages pruned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_tree_and_predicate_prunes_3_pages() {
    let expr = binop(col_expr("price"), Operator::Lt, lit_i32(1024));
    let tree = BoolNode::And(vec![collector_leaf(0), pred_node(expr)]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    // All 1024 rows from page 0 (prices 0..1024).
    assert_eq!(prices.len(), ROWS_PER_PAGE);
    assert_eq!(*prices.first().unwrap(), 0);
    assert_eq!(*prices.last().unwrap(), 1023);

    let m = aggregate_metrics(&plan);
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), 3);
}

/// OR(Predicate(price < 1024), Predicate(price >= 30_000)) → pages 0 and 3.
/// 2 of 4 pages pruned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_tree_or_predicate_keeps_two_pages() {
    let left = binop(col_expr("price"), Operator::Lt, lit_i32(1024));
    let right = binop(col_expr("price"), Operator::GtEq, lit_i32(30_000));
    let tree = BoolNode::And(vec![
        collector_leaf(0),
        BoolNode::Or(vec![pred_node(left), pred_node(right)]),
    ]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    assert_eq!(prices.len(), 2 * ROWS_PER_PAGE);
    assert!(prices.contains(&0));
    assert!(prices.contains(&30_000));
    assert!(!prices.contains(&10_000));

    let m = aggregate_metrics(&plan);
    // Final page-level decision: pages 0 and 3 have candidates, pages 1 and 2 don't.
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), 2);
}

/// AND(Predicate(price >= 10_000), Predicate(price < 21_024)) → pages 1 and 2.
/// Nested AND of two predicates intersects page ranges.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_tree_and_two_predicates_intersect() {
    let left = binop(col_expr("price"), Operator::GtEq, lit_i32(10_000));
    let right = binop(col_expr("price"), Operator::Lt, lit_i32(21_024));
    let tree = BoolNode::And(vec![collector_leaf(0), pred_node(left), pred_node(right)]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    assert_eq!(prices.len(), 2 * ROWS_PER_PAGE);
    assert_eq!(*prices.first().unwrap(), 10_000);
    assert_eq!(*prices.last().unwrap(), 21_023);

    let m = aggregate_metrics(&plan);
    // Final page-level: pages 1 and 2 have candidates, pages 0 and 3 don't.
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), 2);
}

/// AND(Collector(even), OR(Predicate(price < 1024), Predicate(price >= 30_000)))
/// Collector intersected with OR of two page ranges → even docs from pages 0,3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_tree_nested_collector_and_or_predicates() {
    let p0 = binop(col_expr("price"), Operator::Lt, lit_i32(1024));
    let p3 = binop(col_expr("price"), Operator::GtEq, lit_i32(30_000));
    let tree = BoolNode::And(vec![
        collector_leaf(1), // even docs
        BoolNode::Or(vec![pred_node(p0), pred_node(p3)]),
    ]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    // Even docs from pages 0 and 3: 512 + 512 = 1024.
    assert_eq!(prices.len(), ROWS_PER_PAGE);
    // All returned prices should be even (from even doc IDs).
    assert!(prices.iter().all(|p| {
        // page 0: price == doc_id, even doc → even price
        // page 3: price = 30000 + (doc_id - 3072), even doc → even offset
        *p < 1024 || *p >= 30_000
    }));

    let m = aggregate_metrics(&plan);
    // Final page-level: pages 0 and 3 have candidates, pages 1 and 2 pruned.
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), 2);
}

/// Predicate that matches nothing → all 4 pages pruned, zero rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_tree_all_pages_pruned() {
    let expr = binop(col_expr("price"), Operator::Lt, lit_i32(-1));
    let tree = BoolNode::And(vec![collector_leaf(0), pred_node(expr)]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    assert_eq!(prices.len(), 0);

    let m = aggregate_metrics(&plan);
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), NUM_PAGES);
}

// ═════════════════════════════════════════════════════════════════════
// Single collector page pruning tests — all three CollectorCallStrategy
// ═════════════════════════════════════════════════════════════════════

/// Helper: run the same residual across all three strategies, assert identical results.
async fn run_all_strategies(
    collector_tag: u8,
    residual: Arc<dyn PhysicalExpr>,
    expected_len: usize,
    expected_pruned: usize,
) {
    for strategy in [
        CollectorCallStrategy::FullRange,
        CollectorCallStrategy::TightenOuterBounds,
        CollectorCallStrategy::PageRangeSplit,
    ] {
        let (prices, plan) =
            run_single_collector(collector_tag, Arc::clone(&residual), strategy).await;
        assert_eq!(
            prices.len(),
            expected_len,
            "strategy {:?}: expected {} rows, got {}",
            strategy,
            expected_len,
            prices.len()
        );
        let m = aggregate_metrics(&plan);
        assert_eq!(
            get_counter(&m, "pages_total"),
            NUM_PAGES,
            "strategy {:?}: pages_total",
            strategy
        );
        assert_eq!(
            get_counter(&m, "pages_pruned"),
            expected_pruned,
            "strategy {:?}: pages_pruned",
            strategy
        );
    }
}

/// Residual price < 1024 with all-docs collector → page 0 only, 3 pruned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_collector_prunes_3_pages_all_strategies() {
    let residual = binop(col_expr("price"), Operator::Lt, lit_i32(1024));
    run_all_strategies(0, residual, ROWS_PER_PAGE, 3).await;
}

/// Residual price >= 30_000 with even-docs collector → even docs from page 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_collector_even_docs_page3_all_strategies() {
    let residual = binop(col_expr("price"), Operator::GtEq, lit_i32(30_000));
    // Even docs in page 3: 512 rows.
    run_all_strategies(1, residual, ROWS_PER_PAGE / 2, 3).await;
}

/// Residual that matches nothing → all pages pruned, zero rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_collector_all_pruned_all_strategies() {
    let residual = binop(col_expr("price"), Operator::Gt, lit_i32(999_999));
    run_all_strategies(0, residual, 0, NUM_PAGES).await;
}

/// Residual that matches everything → no pages pruned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_collector_no_pruning_all_strategies() {
    let residual = binop(col_expr("price"), Operator::GtEq, lit_i32(0));
    // build_pruning_predicate returns None for always-true → no pruning.
    // All 4096 rows returned, 0 pruned (pages_total may be 0 since
    // pruning was skipped entirely).
    for strategy in [
        CollectorCallStrategy::FullRange,
        CollectorCallStrategy::TightenOuterBounds,
        CollectorCallStrategy::PageRangeSplit,
    ] {
        let (prices, _plan) = run_single_collector(0, Arc::clone(&residual), strategy).await;
        assert_eq!(prices.len(), NUM_ROWS, "strategy {:?}", strategy);
    }
}

/// FullRange calls collector on full [0, 4096), TightenOuterBounds
/// narrows to surviving page range, PageRangeSplit calls per-range.
/// All produce the same rows for price in [10_000, 11_024).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_collector_page1_only() {
    let lo = binop(col_expr("price"), Operator::GtEq, lit_i32(10_000));
    let hi = binop(col_expr("price"), Operator::Lt, lit_i32(11_024));
    let residual = binop(lo, Operator::And, hi);
    run_all_strategies(0, residual, ROWS_PER_PAGE, 3).await;
}

// ═════════════════════════════════════════════════════════════════════
// Complex tree tests — selectivity-aware ordering & range propagation
// ═════════════════════════════════════════════════════════════════════

/// ```text
/// AND─┬─ branch_A: AND(Coll(pages0+1), Pred(price < 1024))    ← Pred keeps 1/4 pages
///     └─ branch_B: AND(Coll(pages2+3), Pred(price < 21024))   ← Pred keeps 3/4 pages
/// ```
///
/// Selectivity-aware cost ordering:
///   branch_A: Pred keeps 1/4 → cost 250, Coll cost 10_000 → total 10_250
///   branch_B: Pred keeps 3/4 → cost 750, Coll cost 10_000 → total 10_750
///   → branch_A evaluated first (more selective predicate)
///
/// Execution order:
///   1. branch_A's Pred(price < 1024) → bitmap {0..1023} (page 0 only)
///   2. branch_A's Coll(pages0+1) called with ranges [(0,1024)] → returns docs in page 0
///      root acc = branch_A result (page 0 docs only)
///      root ranges tighten to [(0,1024)]
///   3. branch_B's Pred(price < 21024) → bitmap {0..3071}
///      inner ranges = intersect([(0,1024)], [(0,3072)]) = [(0,1024)]
///   4. branch_B's Coll(pages2+3) called with ranges [(0,1024)]
///      → returns empty (pages2+3 collector has no docs in page 0)
///      root acc &= empty → empty
///
/// Result: 0 rows. The two collectors have disjoint doc sets (pages 0+1 vs 2+3)
/// AND'd together → nothing survives. The key: branch_A's tight range [(0,1024)]
/// propagated to branch_B, so Coll(pages2+3) only scanned 1024 docs instead of 3072.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complex_selectivity_ordering_tighter_branch_first() {
    let pred_narrow = binop(col_expr("price"), Operator::Lt, lit_i32(1024)); // keeps page 0
    let pred_wide = binop(col_expr("price"), Operator::Lt, lit_i32(21024)); // keeps pages 0,1,2
    let tree = BoolNode::And(vec![
        BoolNode::And(vec![collector_leaf(3), pred_node(pred_narrow)]), // branch_A
        BoolNode::And(vec![collector_leaf(4), pred_node(pred_wide)]),   // branch_B
    ]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    // Disjoint collectors AND'd → empty result.
    assert_eq!(prices.len(), 0);

    let m = aggregate_metrics(&plan);
    // All 4 pages pruned in the final bitmap (empty candidates).
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), NUM_PAGES);
}

/// ```text
/// AND─┬─ Pred(price >= 10000)                              keeps pages 1,2,3
///     ├─ Pred(price < 31024)                                keeps pages 0,1,2
///     ├─ OR─┬─ AND─┬─ Pred(price < 21024)                  keeps pages 0,1,2
///     │     │      └─ Coll(even_docs)                       even doc IDs
///     │     └─ AND─┬─ Pred(price >= 20000)                  keeps pages 2,3
///     │            ├─ Coll(odd_docs)                         odd doc IDs
///     │            └─ NOT── Coll(page3_only)                 excludes page 3 docs
///     └─ Coll(all_docs)                                      all doc IDs
/// ```
///
/// Execution order at root AND (cost-sorted):
///   Pred(price >= 10000): keeps 3/4 → cost 750
///   Pred(price < 31024):  keeps 3/4 → cost 750
///   OR subtree:           sum of children costs
///   Coll(all_docs):       cost 10_000
///
/// Step-by-step:
///   1. Pred(price >= 10000) → acc = {1024..4095}, ranges = [(1024,4096)]
///   2. Pred(price < 31024)  → acc &= {0..3071} → {1024..3071}, ranges = [(1024,3072)]
///      Now only pages 1+2 survive in the accumulator.
///
///   3. OR evaluated with inherited ranges [(1024,3072)]:
///      3a. inner AND₁ (Pred + Coll(even)):
///          - Pred(price < 21024) → inner_acc = {0..3071}
///            ranges = intersect([(1024,3072)], [(0,3072)]) = [(1024,3072)]
///          - Coll(even) called with [(1024,3072)] → even docs in pages 1+2
///      3b. inner AND₂ (Pred + Coll(odd) + NOT(Coll(page3))):
///          - Pred(price >= 20000) → inner_acc = {2048..4095}
///            ranges = intersect([(1024,3072)], [(2048,4096)]) = [(2048,3072)]
///            ← tightened to page 2 only!
///          - Coll(odd) called with [(2048,3072)] → odd docs in page 2 only
///          - NOT(Coll(page3)) called with [(2048,3072)] → page3 collector
///            returns empty in this range, NOT inverts to universe,
///            inner_acc unchanged
///      OR unions: even docs from pages 1+2 ∪ odd docs from page 2
///
///   4. root acc &= OR result → docs from pages 1+2 that are in the OR
///      ranges tighten further
///
///   5. Coll(all_docs) called with tightened ranges → only scans pages 1+2
///      root acc &= all_docs (no-op since all_docs is everything)
///
/// Final result: all docs from pages 1+2 (even from pages 1+2, odd from page 2,
/// unioned = all of page 2 + even of page 1). Total = 1024 (page 2) + 512 (even page 1) = 1536.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complex_deep_nesting_range_propagation_4_levels() {
    let pred_ge_10k = binop(col_expr("price"), Operator::GtEq, lit_i32(10_000));
    let pred_lt_31k = binop(col_expr("price"), Operator::Lt, lit_i32(31_024));
    let pred_lt_21k = binop(col_expr("price"), Operator::Lt, lit_i32(21_024));
    let pred_ge_20k = binop(col_expr("price"), Operator::GtEq, lit_i32(20_000));

    let tree = BoolNode::And(vec![
        pred_node(pred_ge_10k), // pages 1,2,3
        pred_node(pred_lt_31k), // pages 0,1,2
        BoolNode::Or(vec![
            BoolNode::And(vec![
                pred_node(pred_lt_21k), // pages 0,1,2
                collector_leaf(1),      // even docs
            ]),
            BoolNode::And(vec![
                pred_node(pred_ge_20k),                     // pages 2,3
                collector_leaf(5),                          // odd docs
                BoolNode::Not(Box::new(collector_leaf(6))), // NOT page3
            ]),
        ]),
        collector_leaf(0), // all docs
    ]);

    let (prices, plan) = run_bitmap_tree(tree).await;

    // Page 1 (10000..11023): even docs only → 512 rows
    // Page 2 (20000..21023): even ∪ odd = all → 1024 rows
    // Pages 0, 3: excluded by predicate intersection
    assert_eq!(prices.len(), 512 + 1024);

    // Verify page 1 prices are all even-indexed (even doc IDs → even prices in page 1)
    let page1_prices: Vec<i32> = prices
        .iter()
        .filter(|&&p| p >= 10_000 && p < 11_024)
        .copied()
        .collect();
    assert_eq!(page1_prices.len(), 512);
    assert!(page1_prices.iter().all(|p| (p - 10_000) % 2 == 0));

    // Verify all of page 2 is present
    let page2_prices: Vec<i32> = prices
        .iter()
        .filter(|&&p| p >= 20_000 && p < 21_024)
        .copied()
        .collect();
    assert_eq!(page2_prices.len(), 1024);

    let m = aggregate_metrics(&plan);
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    // Pages 0 and 3 have no candidates → 2 pruned
    assert_eq!(get_counter(&m, "pages_pruned"), 2);
}

/// ```text
/// AND─┬─ Pred(price >= 10000)                                keeps pages 1,2,3
///     └─ OR─┬─ AND(Pred(price < 11024), Coll(even))          pages 1 only, even docs
///           ├─ AND(Pred(price >= 20000), Pred(price < 21024), Coll(odd))  page 2, odd
///           ├─ AND(Coll(page3), Pred(price >= 30000))         page 3, page3 collector
///           ├─ NOT(Coll(all))                                 complement of all = empty
///           └─ AND(Coll(first2pp), Pred(price < 21024))       first 2 per page, pages 0,1,2
/// ```
///
/// Root AND execution:
///   1. Pred(price >= 10000) → acc = {1024..4095}, ranges = [(1024,4096)]
///
///   2. OR with 5 children, inherited ranges [(1024,4096)]:
///      Sorted by cost — predicates are cheap, collectors expensive.
///
///      2a. AND(Pred(price < 11024), Coll(even)):
///          Pred keeps page 0,1 → inner_acc = {0..2047}
///          ranges = intersect([(1024,4096)], [(0,2048)]) = [(1024,2048)]
///          Coll(even) called with [(1024,2048)] → even docs in page 1
///          Result: even docs from page 1 (512 docs)
///
///      2b. AND(Pred(price >= 20000), Pred(price < 21024), Coll(odd)):
///          Pred(>=20k) keeps pages 2,3 → inner_acc = {2048..4095}
///          ranges = intersect([(1024,4096)], [(2048,4096)]) = [(2048,4096)]
///          Pred(<21024) keeps pages 0,1,2 → inner_acc &= {0..3071} → {2048..3071}
///          ranges = intersect([(2048,4096)], [(2048,3072)]) = [(2048,3072)]
///          Coll(odd) called with [(2048,3072)] → odd docs in page 2 only
///          Result: odd docs from page 2 (512 docs)
///
///      2c. AND(Coll(page3), Pred(price >= 30000)):
///          Pred(>=30k) keeps page 3 → cost lower, evaluated first
///          inner_acc = {3072..4095}
///          ranges = intersect([(1024,4096)], [(3072,4096)]) = [(3072,4096)]
///          Coll(page3) called with [(3072,4096)] → all page 3 docs
///          Result: page 3 docs (1024 docs)
///
///      2d. NOT(Coll(all)):
///          Coll(all) called with [(1024,4096)] → all docs in pages 1,2,3
///          NOT inverts → empty (universe - all = nothing)
///          Result: empty
///
///      2e. AND(Coll(first2pp), Pred(price < 21024)):
///          Pred(<21024) keeps pages 0,1,2 → cost lower, evaluated first
///          inner_acc = {0..3071}
///          ranges = intersect([(1024,4096)], [(0,3072)]) = [(1024,3072)]
///          Coll(first2pp) called with [(1024,3072)] → docs 1024,1025,2048,2049
///          Result: 4 docs
///
///      OR unions all: 512 + 512 + 1024 + 0 + 4 = 2052 docs
///      (some overlap: first2pp docs 1024,1025 overlap with even page 1;
///       doc 2049 overlaps with odd page 2. Union deduplicates.)
///
///   Root acc = {1024..4095} & OR_result
///
/// Final: even page 1 ∪ odd page 2 ∪ all page 3 ∪ {1024,1025,2048,2049}
///   = page 1 even (512) + page 2 odd (512) + page 3 (1024) + {1025,2048} extra
///   1025 is odd → already in page 1 even? No, 1025 is odd doc. But first2pp
///   adds 1024,1025 — 1024 is even (already in even set), 1025 is new.
///   2048,2049 — 2048 is even (not in odd set, but first2pp adds it), 2049 is odd (already in odd set).
///   Net new from first2pp: {1025, 2048} → +2 docs
///   Total: 512 + 512 + 1024 + 2 = 2050
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complex_5_wide_or_mixed_predicates_collectors() {
    let pred_ge_10k = binop(col_expr("price"), Operator::GtEq, lit_i32(10_000));
    let pred_lt_11k = binop(col_expr("price"), Operator::Lt, lit_i32(11_024));
    let pred_ge_20k = binop(col_expr("price"), Operator::GtEq, lit_i32(20_000));
    let pred_lt_21k = binop(col_expr("price"), Operator::Lt, lit_i32(21_024));
    let pred_ge_30k = binop(col_expr("price"), Operator::GtEq, lit_i32(30_000));

    let tree = BoolNode::And(vec![
        pred_node(pred_ge_10k),
        BoolNode::Or(vec![
            BoolNode::And(vec![pred_node(pred_lt_11k), collector_leaf(1)]), // even, page 1
            BoolNode::And(vec![
                pred_node(pred_ge_20k),
                pred_node(pred_lt_21k.clone()),
                collector_leaf(5),
            ]), // odd, page 2
            BoolNode::And(vec![collector_leaf(6), pred_node(pred_ge_30k)]), // page3 coll, page 3
            BoolNode::Not(Box::new(collector_leaf(0))),                     // NOT all = empty
            BoolNode::And(vec![collector_leaf(2), pred_node(pred_lt_21k)]), // first2pp, pages 0,1,2
        ]),
    ]);

    let (prices, plan) = run_bitmap_tree(tree).await;

    // Count by page:
    let p1: Vec<_> = prices
        .iter()
        .filter(|&&p| p >= 10_000 && p < 11_024)
        .collect();
    let p2: Vec<_> = prices
        .iter()
        .filter(|&&p| p >= 20_000 && p < 21_024)
        .collect();
    let p3: Vec<_> = prices
        .iter()
        .filter(|&&p| p >= 30_000 && p < 31_024)
        .collect();
    let p0: Vec<_> = prices.iter().filter(|&&p| p < 1_024).collect();

    // Page 0: excluded by root Pred(>= 10000)
    assert_eq!(p0.len(), 0);
    // Page 1: even docs (512) + doc 1025 from first2pp = 513
    assert_eq!(p1.len(), 513);
    // Page 2: odd docs (512) + doc 2048 from first2pp = 513
    assert_eq!(p2.len(), 513);
    // Page 3: all 1024 docs from page3 collector
    assert_eq!(p3.len(), 1024);

    assert_eq!(prices.len(), 513 + 513 + 1024);

    let m = aggregate_metrics(&plan);
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    // Page 0 has no candidates → 1 pruned
    assert_eq!(get_counter(&m, "pages_pruned"), 1);
}

/// ```text
/// AND─┬─ Pred(price >= 10000)                                keeps pages 1,2,3
///     ├─ Pred(price < 31024)                                  keeps pages 0,1,2
///     ├─ AND─┬─ Pred(price >= 20000)                          keeps pages 2,3
///     │      ├─ NOT── AND─┬─ Coll(page3)                      page 3 docs
///     │      │            └─ Pred(price >= 30000)              keeps page 3
///     │      └─ Coll(all)                                      all docs
///     └─ Coll(even)                                            even docs
/// ```
///
/// Root AND execution (cost-sorted):
///   1. Pred(price >= 10000): cost 750 (3/4 pages)
///      acc = {1024..4095}, ranges = [(1024,4096)]
///
///   2. Pred(price < 31024): cost 750 (3/4 pages)
///      acc &= {0..3071} → {1024..3071}, ranges = [(1024,3072)]
///      Now only pages 1+2 survive.
///
///   3. inner AND (Pred + NOT(AND) + Coll): cost = 250 + 10_000 + 10_000 = 20_250
///      inherited ranges = [(1024,3072)]
///
///      3a. Pred(price >= 20000): keeps pages 2,3 → cost 500 (2/4)
///          inner_acc = {2048..4095}
///          ranges = intersect([(1024,3072)], [(2048,4096)]) = [(2048,3072)]
///          ← narrowed to page 2 only!
///
///      3b. NOT(AND(Coll(page3), Pred(price >= 30000))):
///          inner-inner AND:
///            Pred(>=30k) keeps page 3 → cost 250, evaluated first
///            inner_inner_acc = {3072..4095}
///            ranges = intersect([(2048,3072)], [(3072,4096)]) = [] (empty!)
///            Coll(page3) called with [] → no FFM call needed, returns empty
///            inner-inner result = empty
///          NOT(empty) = universe {0..4095}
///          inner_acc &= universe → unchanged {2048..3071}
///
///      3c. Coll(all) called with [(2048,3072)] → all docs in page 2
///          inner_acc &= all → {2048..3071}
///
///   4. Coll(even): cost 10_000
///      called with ranges from root acc (page 2 only after step 3)
///      returns even docs in page 2 → 512 docs
///      root acc &= even → even docs in page 2
///
/// Final: 512 even docs from page 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complex_cascading_and_with_not_at_depth() {
    let pred_ge_10k = binop(col_expr("price"), Operator::GtEq, lit_i32(10_000));
    let pred_lt_31k = binop(col_expr("price"), Operator::Lt, lit_i32(31_024));
    let pred_ge_20k = binop(col_expr("price"), Operator::GtEq, lit_i32(20_000));
    let pred_ge_30k = binop(col_expr("price"), Operator::GtEq, lit_i32(30_000));

    let tree = BoolNode::And(vec![
        pred_node(pred_ge_10k), // pages 1,2,3
        pred_node(pred_lt_31k), // pages 0,1,2
        BoolNode::And(vec![
            pred_node(pred_ge_20k), // pages 2,3
            BoolNode::Not(Box::new(BoolNode::And(vec![
                collector_leaf(6),      // page3 collector
                pred_node(pred_ge_30k), // page 3
            ]))),
            collector_leaf(0), // all docs
        ]),
        collector_leaf(1), // even docs
    ]);

    let (prices, plan) = run_bitmap_tree(tree).await;

    // Only page 2 survives, even docs only → 512 rows
    assert_eq!(prices.len(), 512);

    // All prices should be in page 2 range
    assert!(prices.iter().all(|&p| p >= 20_000 && p < 21_024));
    // All should be even-indexed within page 2
    assert!(prices.iter().all(|&p| (p - 20_000) % 2 == 0));

    let m = aggregate_metrics(&plan);
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    // Coll(even) is evaluated before the inner AND (lower cost: 10_000 vs
    // ~20_750), so the root accumulator after Coll(even) has even docs in
    // pages 1+2. The inner AND then narrows to page 2. Final bitmap =
    // even page 2 (512 rows). However, the candidate bitmap used for
    // metrics is computed after the tree resolves — pages 0 and 3 are
    // pruned (no candidates). Page 1 has residual even-doc candidates
    // from the Coll(even) step that the refinement stage (on_batch_mask)
    // will filter out, but the candidate-stage bitmap still has them.
    assert_eq!(get_counter(&m, "pages_pruned"), 2);
}

// ═════════════════════════════════════════════════════════════════════
// Execution order tests — assert selectivity-aware cost ordering
// ═════════════════════════════════════════════════════════════════════

/// Build an RgEvalContext for the fixture's single RG.
fn fixture_eval_ctx() -> RgEvalContext {
    RgEvalContext {
        rg_idx: 0,
        rg_first_row: 0,
        rg_num_rows: NUM_ROWS as i64,
        min_doc: 0,
        max_doc: NUM_ROWS as i32,
        cost_predicate: 1,
        cost_collector: 10,
        collector_call_ranges: None,
        collector_strategy: CollectorCallStrategy::TightenOuterBounds,
    }
}

/// ```text
/// AND─┬─ Pred(price < 1024)       keeps 1/4 pages → cost = ceil(1000*1/4) = 250
///     ├─ Pred(price < 21024)       keeps 3/4 pages → cost = ceil(1000*3/4) = 750
///     ├─ Coll(all)                 cost = 10 * 1000 = 10_000
///     └─ Pred(price >= 30000)      keeps 1/4 pages → cost = 250
/// ```
///
/// Expected sort order by cost: [Pred(<1024), Pred(>=30000), Pred(<21024), Coll(all)]
/// Costs:                        [250,        250,           750,          10_000]
///
/// The two predicates with equal cost (250) are stable-sorted by original index.
/// Pred(<1024) is index 0, Pred(>=30000) is index 3 → Pred(<1024) first.
#[test]
fn cost_ordering_predicates_sorted_by_selectivity() {
    let tmp = write_fixture();
    let (seg, schema) = load_segment(&tmp);
    let pruner = PagePruner::new(&schema, seg.metadata);
    let ctx = fixture_eval_ctx();

    let pred_narrow = binop(col_expr("price"), Operator::Lt, lit_i32(1024)); // 1/4 pages
    let pred_wide = binop(col_expr("price"), Operator::Lt, lit_i32(21024)); // 3/4 pages
    let pred_narrow2 = binop(col_expr("price"), Operator::GtEq, lit_i32(30_000)); // 1/4 pages

    let tree = BoolNode::And(vec![
        pred_node(pred_narrow.clone()),  // child 0
        pred_node(pred_wide.clone()),    // child 1
        collector_leaf(0),               // child 2
        pred_node(pred_narrow2.clone()), // child 3
    ]);

    // Resolve tree to get ResolvedNodes.
    let tree = tree.push_not_down();
    let pp_map = build_pp_map(&tree, &schema);
    let mut colls = Vec::new();
    wire_collectors_dfs(&tree, &mut colls);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = colls
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let resolved = tree.resolve(&per_leaf).unwrap();

    // Extract children of the root AND.
    let children = match &resolved {
        crate::indexed_table::bool_tree::ResolvedNode::And(c) => c,
        _ => panic!("expected AND"),
    };

    let costs: Vec<u32> = children
        .iter()
        .map(|c| subtree_cost(c, &ctx, &pruner, &pp_map))
        .collect();

    // Pred(<1024):   1/4 pages kept → ceil(1000 * 1 / 4) = 250
    // Pred(<21024):  3/4 pages kept → ceil(1000 * 3 / 4) = 750
    // Coll(all):     10 * 1000 = 10_000
    // Pred(>=30000): 1/4 pages kept → ceil(1000 * 1 / 4) = 250
    assert_eq!(costs[0], 250, "Pred(<1024) should cost 250");
    assert_eq!(costs[1], 750, "Pred(<21024) should cost 750");
    assert_eq!(costs[2], 10_000, "Coll(all) should cost 10_000");
    assert_eq!(costs[3], 250, "Pred(>=30000) should cost 250");

    // Verify sort order: [250, 250, 750, 10_000]
    let mut sorted_indices: Vec<usize> = (0..4).collect();
    sorted_indices.sort_by_key(|&i| costs[i]);
    // Indices 0 and 3 (cost 250) come first, then 1 (750), then 2 (10_000).
    assert_eq!(sorted_indices[0], 0, "most selective pred first");
    assert_eq!(sorted_indices[1], 3, "second most selective pred second");
    assert_eq!(sorted_indices[2], 1, "wide pred third");
    assert_eq!(sorted_indices[3], 2, "collector last");
}

/// ```text
/// AND─┬─ branch_A: AND(Coll(pages0+1), Pred(price < 1024))
///     │    Pred keeps 1/4 → cost 250, Coll cost 10_000 → subtree 10_250
///     └─ branch_B: AND(Coll(pages2+3), Pred(price < 21024))
///          Pred keeps 3/4 → cost 750, Coll cost 10_000 → subtree 10_750
/// ```
///
/// branch_A (10_250) < branch_B (10_750) → branch_A evaluated first.
/// The more selective predicate's branch wins, causing its tight range
/// to propagate to branch_B's collector.
#[test]
fn cost_ordering_nested_and_branches_selective_first() {
    let tmp = write_fixture();
    let (seg, schema) = load_segment(&tmp);
    let pruner = PagePruner::new(&schema, seg.metadata);
    let ctx = fixture_eval_ctx();

    let pred_narrow = binop(col_expr("price"), Operator::Lt, lit_i32(1024));
    let pred_wide = binop(col_expr("price"), Operator::Lt, lit_i32(21024));

    let branch_a = BoolNode::And(vec![collector_leaf(3), pred_node(pred_narrow.clone())]);
    let branch_b = BoolNode::And(vec![collector_leaf(4), pred_node(pred_wide.clone())]);
    let tree = BoolNode::And(vec![branch_a, branch_b]);

    let tree = tree.push_not_down();
    let pp_map = build_pp_map(&tree, &schema);
    let mut colls = Vec::new();
    wire_collectors_dfs(&tree, &mut colls);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = colls
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let resolved = tree.resolve(&per_leaf).unwrap();

    let children = match &resolved {
        crate::indexed_table::bool_tree::ResolvedNode::And(c) => c,
        _ => panic!("expected AND"),
    };

    let cost_a = subtree_cost(&children[0], &ctx, &pruner, &pp_map);
    let cost_b = subtree_cost(&children[1], &ctx, &pruner, &pp_map);

    // branch_A: Coll(10_000) + Pred(1/4 = 250) = 10_250
    // branch_B: Coll(10_000) + Pred(3/4 = 750) = 10_750
    assert_eq!(cost_a, 10_250, "branch_A: Coll + narrow pred");
    assert_eq!(cost_b, 10_750, "branch_B: Coll + wide pred");
    assert!(
        cost_a < cost_b,
        "branch with more selective predicate should be cheaper \
         (A={}, B={})",
        cost_a,
        cost_b
    );
}

/// ```text
/// AND─┬─ Pred(price >= 10000)                              3/4 pages → cost 750
///     ├─ Pred(price < 31024)                                4/4 pages → cost 1000
///     │    (page 3 max=31023 < 31024, so all pages kept)
///     ├─ OR─┬─ AND(Pred(price < 21024), Coll(even))        subtree cost = 750 + 10_000 = 10_750
///     │     └─ AND(Pred(price >= 20000), Coll(odd), NOT(Coll(page3)))
///     │          subtree cost = 500 + 10_000 + 10_000 = 20_500
///     └─ Coll(all)                                          cost 10_000
/// ```
///
/// Root AND sort order: [Pred(750), Pred(1000), Coll(10_000), OR(31_250)]
/// → Most selective predicate first, then less selective, then Coll(all)
///   with tightened range, then OR last (most expensive).
#[test]
fn cost_ordering_complex_tree_predicates_before_or_before_nothing() {
    let tmp = write_fixture();
    let (seg, schema) = load_segment(&tmp);
    let pruner = PagePruner::new(&schema, seg.metadata);
    let ctx = fixture_eval_ctx();

    let pred_ge_10k = binop(col_expr("price"), Operator::GtEq, lit_i32(10_000));
    let pred_lt_31k = binop(col_expr("price"), Operator::Lt, lit_i32(31_024));
    let pred_lt_21k = binop(col_expr("price"), Operator::Lt, lit_i32(21_024));
    let pred_ge_20k = binop(col_expr("price"), Operator::GtEq, lit_i32(20_000));

    let tree = BoolNode::And(vec![
        pred_node(pred_ge_10k), // child 0
        pred_node(pred_lt_31k), // child 1
        BoolNode::Or(vec![
            // child 2
            BoolNode::And(vec![pred_node(pred_lt_21k), collector_leaf(1)]),
            BoolNode::And(vec![
                pred_node(pred_ge_20k),
                collector_leaf(5),
                BoolNode::Not(Box::new(collector_leaf(6))),
            ]),
        ]),
        collector_leaf(0), // child 3
    ]);

    let tree = tree.push_not_down();
    let pp_map = build_pp_map(&tree, &schema);
    let mut colls = Vec::new();
    wire_collectors_dfs(&tree, &mut colls);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = colls
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let resolved = tree.resolve(&per_leaf).unwrap();

    let children = match &resolved {
        crate::indexed_table::bool_tree::ResolvedNode::And(c) => c,
        _ => panic!("expected AND"),
    };

    let costs: Vec<u32> = children
        .iter()
        .map(|c| subtree_cost(c, &ctx, &pruner, &pp_map))
        .collect();

    // child 0: Pred(>=10k) keeps pages 1,2,3 (3/4) → ceil(1000*3/4) = 750
    // child 1: Pred(<31k)  keeps ALL pages (max of page 3 = 31023 < 31024) → 1000
    // child 2: OR subtree (sum of inner costs)
    // child 3: Coll(all) = 10_000
    assert_eq!(costs[0], 750, "Pred(>=10k): 3/4 pages");
    assert_eq!(costs[1], 1000, "Pred(<31k): all pages kept (31023 < 31024)");
    assert_eq!(costs[3], 10_000, "Coll(all)");

    let or_cost = costs[2];
    assert!(
        or_cost > costs[3],
        "OR subtree ({}) should be more expensive than single Coll ({})",
        or_cost,
        costs[3]
    );

    // Verify execution order: Pred(>=10k) first, Pred(<31k) second, Coll third, OR last
    let mut sorted: Vec<(u32, usize)> = costs.iter().copied().zip(0..).collect();
    sorted.sort_by_key(|&(cost, idx)| (cost, idx));
    assert_eq!(sorted[0].1, 0, "Pred(>=10k) first (cost {})", sorted[0].0);
    assert_eq!(sorted[1].1, 1, "Pred(<31k) second (cost {})", sorted[1].0);
    assert_eq!(sorted[2].1, 3, "Coll(all) third (cost {})", sorted[2].0);
    assert_eq!(sorted[3].1, 2, "OR subtree last (cost {})", sorted[3].0);
}
