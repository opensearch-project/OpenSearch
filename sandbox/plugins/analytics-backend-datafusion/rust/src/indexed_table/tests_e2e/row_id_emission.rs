/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! End-to-end tests for `emit_row_ids` mode.
//! Verifies that the indexed query path can return global row IDs
//! instead of actual data columns.

use datafusion::arrow::array::UInt64Array;

use super::*;

/// Helper: run a tree with `emit_row_ids: true` and return the collected row IDs.
async fn run_tree_row_ids(tree: BoolNode) -> Vec<u64> {
    let tmp = write_fixture_parquet();
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

    let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
    let segment = SegmentFileInfo {
        segment_ord: 0,
        max_doc: 16,
        object_path,
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
        global_base: 0,
    };

    let tree = tree.push_not_down();
    let collectors = wire_collectors(&tree);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = collectors
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let tree = Arc::new(tree);
    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_leaf = per_leaf.clone();
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics| {
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(
                    crate::indexed_table::eval::bitmap_tree::CollectorLeafBitmaps {
                        ffm_collector_calls: _stream_metrics.ffm_collector_calls.clone(),
                    },
                ),
                page_pruner: pruner,
                cost_predicate: 1,
                cost_collector: 10,
                max_collector_parallelism: 1,
                pruning_predicates: Arc::new(std::collections::HashMap::new()),
                page_prune_metrics: Some(
                    crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(
                        _stream_metrics,
                    ),
                ),
                collector_strategy:
                    crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
            });
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store,
        store_url,
        evaluator_factory: factory,
        pushdown_predicate: None,
        query_config: Arc::new({
            let mut qc = crate::datafusion_query_config::DatafusionQueryConfig::test_default();
            qc.target_partitions = 1;
            qc.force_strategy = Some(FilterStrategy::BooleanMask);
            qc.force_pushdown = Some(false);
            qc
        }),
        predicate_columns: vec![0, 1, 2, 3],
        emit_row_ids: true,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    // SELECT * — the schema is overridden to [_row_id: UInt64] by the flag
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();
    let mut row_ids: Vec<u64> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        assert_eq!(b.num_columns(), 1, "should have only _row_id column");
        assert_eq!(b.schema().field(0).name(), "_row_id");
        let col = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        for i in 0..b.num_rows() {
            row_ids.push(col.value(i));
        }
    }
    row_ids.sort();
    row_ids
}

// ── Tests ────────────────────────────────────────────────────────────

/// brand="amazon" matches rows 0,1,2,3,12 — verify we get those row IDs back.
#[tokio::test]
async fn test_emit_row_ids_single_collector_amazon() {
    // Collector tag 0 = brand_eq("amazon")
    let tree = BoolNode::And(vec![index_leaf(0)]);
    let ids = run_tree_row_ids(tree).await;
    assert_eq!(ids, vec![0, 1, 2, 3, 12]);
}

/// brand="apple" matches rows 4,5,6,7,13.
#[tokio::test]
async fn test_emit_row_ids_single_collector_apple() {
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let ids = run_tree_row_ids(tree).await;
    assert_eq!(ids, vec![4, 5, 6, 7, 13]);
}

/// AND(brand="amazon", price > 100) matches rows where brand=amazon AND price>100.
/// amazon rows: 0(50), 1(150), 2(80), 3(120), 12(30) → price>100: rows 1, 3.
#[tokio::test]
async fn test_emit_row_ids_collector_and_predicate() {
    let tree = BoolNode::And(vec![
        index_leaf(0), // amazon
        pred_int("price", Operator::Gt, 100),
    ]);
    let ids = run_tree_row_ids(tree).await;
    assert_eq!(ids, vec![1, 3]);
}

/// OR(brand="amazon", brand="apple") matches rows 0-7, 12, 13.
#[tokio::test]
async fn test_emit_row_ids_or_two_collectors() {
    let tree = BoolNode::Or(vec![index_leaf(0), index_leaf(1)]);
    let ids = run_tree_row_ids(tree).await;
    assert_eq!(ids, vec![0, 1, 2, 3, 4, 5, 6, 7, 12, 13]);
}

/// AND(brand="apple", status="archived") — apple rows: 4,5,6,7,13;
/// archived rows: 1,5,9,12,13. Intersection: 5, 13.
#[tokio::test]
async fn test_emit_row_ids_two_collectors_and() {
    let tree = BoolNode::And(vec![index_leaf(1), index_leaf(2)]);
    let ids = run_tree_row_ids(tree).await;
    assert_eq!(ids, vec![5, 13]);
}

/// Verify global_base offset works: set global_base=1000 and check IDs are shifted.
async fn run_tree_row_ids_with_global_base(tree: BoolNode, global_base: u64) -> Vec<u64> {
    let tmp = write_fixture_parquet();
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

    let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
    let segment = SegmentFileInfo {
        segment_ord: 0,
        max_doc: 16,
        object_path,
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
        global_base,
    };

    let tree = tree.push_not_down();
    let collectors = wire_collectors(&tree);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = collectors
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let tree = Arc::new(tree);
    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_leaf = per_leaf.clone();
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics| {
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(
                    crate::indexed_table::eval::bitmap_tree::CollectorLeafBitmaps {
                        ffm_collector_calls: _stream_metrics.ffm_collector_calls.clone(),
                    },
                ),
                page_pruner: pruner,
                cost_predicate: 1,
                cost_collector: 10,
                max_collector_parallelism: 1,
                pruning_predicates: Arc::new(std::collections::HashMap::new()),
                page_prune_metrics: Some(
                    crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(
                        _stream_metrics,
                    ),
                ),
                collector_strategy:
                    crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
            });
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store,
        store_url,
        evaluator_factory: factory,
        pushdown_predicate: None,
        query_config: Arc::new({
            let mut qc = crate::datafusion_query_config::DatafusionQueryConfig::test_default();
            qc.target_partitions = 1;
            qc.force_strategy = Some(FilterStrategy::BooleanMask);
            qc.force_pushdown = Some(false);
            qc
        }),
        predicate_columns: vec![0, 1, 2, 3],
        emit_row_ids: true,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();
    let mut row_ids: Vec<u64> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let col = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        for i in 0..b.num_rows() {
            row_ids.push(col.value(i));
        }
    }
    row_ids.sort();
    row_ids
}

/// With global_base=1000, brand="amazon" (rows 0,1,2,3,12) should give 1000,1001,1002,1003,1012.
#[tokio::test]
async fn test_emit_row_ids_with_global_base_offset() {
    let tree = BoolNode::And(vec![index_leaf(0)]);
    let ids = run_tree_row_ids_with_global_base(tree, 1000).await;
    assert_eq!(ids, vec![1000, 1001, 1002, 1003, 1012]);
}

// ── Comprehensive correctness: verify emitted row IDs match actual positions ──
//
// For each query type (SingleCollector, Collector+Predicate, Tree OR, Tree AND,
// Predicate-only), we compute the EXPECTED row IDs by scanning the fixture data
// directly, then compare against what emit_row_ids produces.
//
// The fixture (16 rows):
// | row | brand  | price | status    | category    |
// |  0  | amazon |    50 | active    | electronics |
// |  1  | amazon |   150 | archived  | electronics |
// |  2  | amazon |    80 | active    | books       |
// |  3  | amazon |   120 | active    | electronics |
// |  4  | apple  |    90 | active    | electronics |
// |  5  | apple  |    95 | archived  | electronics |
// |  6  | apple  |   200 | active    | books       |
// |  7  | apple  |    60 | active    | electronics |
// |  8  | google |    40 | active    | electronics |
// |  9  | google |   300 | archived  | electronics |
// | 10  | samsung|    70 | active    | electronics |
// | 11  | samsung|   150 | active    | books       |
// | 12  | amazon |    30 | archived  | electronics |
// | 13  | apple  |    45 | archived  | electronics |
// | 14  | samsung|    99 | active    | electronics |
// | 15  | google |    55 | active    | electronics |

/// Helper: compute expected row positions from fixture data given a filter.
fn expected_rows(filter: impl Fn(usize) -> bool) -> Vec<u64> {
    (0..16).filter(|&i| filter(i)).map(|i| i as u64).collect()
}

/// Exhaustive test: all query types produce correct row IDs matching fixture positions.
#[tokio::test]
async fn test_all_query_types_match_fixture_positions() {
    // SingleCollector: brand="amazon" → rows 0,1,2,3,12
    let ids = run_tree_row_ids(BoolNode::And(vec![index_leaf(0)])).await;
    let expected = expected_rows(|i| BRANDS[i] == "amazon");
    assert_eq!(ids, expected, "SingleCollector(amazon) mismatch");

    // SingleCollector: brand="apple" → rows 4,5,6,7,13
    let ids = run_tree_row_ids(BoolNode::And(vec![index_leaf(1)])).await;
    let expected = expected_rows(|i| BRANDS[i] == "apple");
    assert_eq!(ids, expected, "SingleCollector(apple) mismatch");

    // SingleCollector: status="archived" → rows 1,5,9,12,13
    let ids = run_tree_row_ids(BoolNode::And(vec![index_leaf(2)])).await;
    let expected = expected_rows(|i| STATUSES[i] == "archived");
    assert_eq!(ids, expected, "SingleCollector(archived) mismatch");

    // Collector + Predicate: amazon AND price > 100 → rows 1,3
    let ids = run_tree_row_ids(BoolNode::And(vec![
        index_leaf(0),
        pred_int("price", Operator::Gt, 100),
    ])).await;
    let expected = expected_rows(|i| BRANDS[i] == "amazon" && PRICES[i] > 100);
    assert_eq!(ids, expected, "Collector+Predicate(amazon,price>100) mismatch");

    // Collector + Predicate: apple AND price < 90 → rows 7,13
    let ids = run_tree_row_ids(BoolNode::And(vec![
        index_leaf(1),
        pred_int("price", Operator::Lt, 90),
    ])).await;
    let expected = expected_rows(|i| BRANDS[i] == "apple" && PRICES[i] < 90);
    assert_eq!(ids, expected, "Collector+Predicate(apple,price<90) mismatch");

    // Tree OR: amazon OR apple → rows 0-7,12,13
    let ids = run_tree_row_ids(BoolNode::Or(vec![index_leaf(0), index_leaf(1)])).await;
    let expected = expected_rows(|i| BRANDS[i] == "amazon" || BRANDS[i] == "apple");
    assert_eq!(ids, expected, "Tree OR(amazon,apple) mismatch");

    // Tree AND: apple AND archived → rows 5,13
    let ids = run_tree_row_ids(BoolNode::And(vec![index_leaf(1), index_leaf(2)])).await;
    let expected = expected_rows(|i| BRANDS[i] == "apple" && STATUSES[i] == "archived");
    assert_eq!(ids, expected, "Tree AND(apple,archived) mismatch");

    // Tree OR + Predicate: (amazon OR apple) AND price > 100 → rows 1,3,6,9? no...
    // amazon(0,1,2,3,12) OR apple(4,5,6,7,13) = 0-7,12,13. price>100: 1,3,6
    let ids = run_tree_row_ids(BoolNode::And(vec![
        BoolNode::Or(vec![index_leaf(0), index_leaf(1)]),
        pred_int("price", Operator::Gt, 100),
    ])).await;
    let expected = expected_rows(|i| (BRANDS[i] == "amazon" || BRANDS[i] == "apple") && PRICES[i] > 100);
    assert_eq!(ids, expected, "Tree OR+Predicate mismatch");

    // Predicate only: price >= 150 → rows 1,6,9,11
    let ids = run_tree_row_ids(BoolNode::And(vec![
        pred_int("price", Operator::GtEq, 150),
    ])).await;
    let expected = expected_rows(|i| PRICES[i] >= 150);
    assert_eq!(ids, expected, "Predicate-only(price>=150) mismatch");

    // Predicate only: price < 50 → rows 8,12,13
    let ids = run_tree_row_ids(BoolNode::And(vec![
        pred_int("price", Operator::Lt, 50),
    ])).await;
    let expected = expected_rows(|i| PRICES[i] < 50);
    assert_eq!(ids, expected, "Predicate-only(price<50) mismatch");

    // Multi-predicate: price > 50 AND price < 100 → rows 2,4,5,7,10,14,15
    let ids = run_tree_row_ids(BoolNode::And(vec![
        pred_int("price", Operator::Gt, 50),
        pred_int("price", Operator::Lt, 100),
    ])).await;
    let expected = expected_rows(|i| PRICES[i] > 50 && PRICES[i] < 100);
    assert_eq!(ids, expected, "Multi-predicate(50<price<100) mismatch");

    // String predicate: status = "active" → rows 0,2,3,4,6,7,8,10,11,14,15
    let ids = run_tree_row_ids(BoolNode::And(vec![
        pred_str("status", Operator::Eq, "active"),
    ])).await;
    let expected = expected_rows(|i| STATUSES[i] == "active");
    assert_eq!(ids, expected, "Predicate(status=active) mismatch");

    // Collector + string predicate: amazon AND category = "books" → rows 2
    let ids = run_tree_row_ids(BoolNode::And(vec![
        index_leaf(0),
        pred_str("category", Operator::Eq, "books"),
    ])).await;
    let expected = expected_rows(|i| BRANDS[i] == "amazon" && CATEGORIES[i] == "books");
    assert_eq!(ids, expected, "Collector+String(amazon,books) mismatch");

    // NOT(collector): NOT(amazon) → rows 4-11,13-15
    let ids = run_tree_row_ids(BoolNode::Not(Box::new(index_leaf(0)))).await;
    let expected = expected_rows(|i| BRANDS[i] != "amazon");
    assert_eq!(ids, expected, "NOT(amazon) mismatch");

    // Complex: (amazon AND price>80) OR (apple AND archived)
    let ids = run_tree_row_ids(BoolNode::Or(vec![
        BoolNode::And(vec![index_leaf(0), pred_int("price", Operator::Gt, 80)]),
        BoolNode::And(vec![index_leaf(1), index_leaf(2)]),
    ])).await;
    let expected = expected_rows(|i| {
        (BRANDS[i] == "amazon" && PRICES[i] > 80)
            || (BRANDS[i] == "apple" && STATUSES[i] == "archived")
    });
    assert_eq!(ids, expected, "Complex OR(AND,AND) mismatch");
}

/// Verify UDF detection — `_global_row_id()` in SELECT triggers emit_row_ids mode.
#[tokio::test]
async fn test_udf_detection_global_row_id() {
    use crate::indexed_table::substrait_to_tree::{create_row_id_udf, plan_requests_row_ids};

    let tmp = write_fixture_parquet();
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();

    let file = std::fs::File::open(&path).unwrap();
    let meta = datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
        &file,
        datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_page_index(true),
    )
    .unwrap();
    let schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();
    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo { index: i, first_row: offset, num_rows: n });
        offset += n;
    }

    // Register the UDF and build a logical plan with SELECT _global_row_id() FROM t
    let ctx = SessionContext::new();
    ctx.register_udf(create_row_id_udf());

    // Create a simple table to query against
    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(datafusion::arrow::array::StringArray::from(BRANDS.to_vec())),
            Arc::new(datafusion::arrow::array::Int32Array::from(PRICES.to_vec())),
            Arc::new(datafusion::arrow::array::StringArray::from(STATUSES.to_vec())),
            Arc::new(datafusion::arrow::array::StringArray::from(CATEGORIES.to_vec())),
        ],
    )
    .unwrap();
    let mem_table = datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(mem_table)).unwrap();

    // Plan with _global_row_id() in projection — should be detected
    let df = ctx.sql("SELECT _global_row_id() FROM t").await.unwrap();
    let plan = df.logical_plan();
    assert!(
        plan_requests_row_ids(plan),
        "Should detect _global_row_id() in projection"
    );

    // Plan without _global_row_id() — should NOT be detected
    let df2 = ctx.sql("SELECT brand FROM t").await.unwrap();
    let plan2 = df2.logical_plan();
    assert!(
        !plan_requests_row_ids(plan2),
        "Should not detect _global_row_id() in normal projection"
    );
}
