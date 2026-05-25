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

use datafusion::arrow::array::Int64Array;

use super::*;

// Why it is complex -->
// Optimizer + ComputeRowId --> @gbh --> optimizer --> for parquet only right

// Lot of query shapes --> for the query sent, how will that translate into at the data node side?


/// Helper: run a tree with `emit_row_ids: true` and return the collected row IDs.
async fn run_tree_row_ids(tree: BoolNode) -> Vec<i64> {
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
    // Project __row_id__ — it will be computed from position, not read from parquet
    let df = ctx.sql("SELECT \"__row_id__\" FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();
    let mut row_ids: Vec<i64> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        assert_eq!(b.num_columns(), 1, "should have only __row_id__ column");
        assert_eq!(b.schema().field(0).name(), "__row_id__");
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
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
async fn run_tree_row_ids_with_global_base(tree: BoolNode, global_base: u64) -> Vec<i64> {
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
    let df = ctx.sql("SELECT \"__row_id__\" FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();
    let mut row_ids: Vec<i64> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        assert_eq!(b.schema().field(0).name(), "__row_id__");
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
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
fn expected_rows(filter: impl Fn(usize) -> bool) -> Vec<i64> {
    (0..16).filter(|&i| filter(i)).map(|i| i as i64).collect()
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

/// Verify that __row_id__ is computed alongside data columns — not replacing them.
/// Projects [__row_id__, brand, price] and verifies all three are present and correct.
#[tokio::test]
async fn test_row_id_with_data_columns() {
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

    // Filter: brand = "amazon" (rows 0,1,2,3,12)
    let tree = BoolNode::And(vec![index_leaf(0)]).push_not_down();
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
    // Project __row_id__ alongside data columns
    let df = ctx
        .sql("SELECT \"__row_id__\", brand, price FROM t")
        .await
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();

    let mut rows: Vec<(i64, String, i32)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        assert_eq!(b.num_columns(), 3, "should have 3 columns: __row_id__, brand, price");
        assert_eq!(b.schema().field(0).name(), "__row_id__");
        assert_eq!(b.schema().field(1).name(), "brand");
        assert_eq!(b.schema().field(2).name(), "price");
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let brands = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let prices = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((ids.value(i), brands.value(i).to_string(), prices.value(i)));
        }
    }

    rows.sort_by_key(|r| r.0);

    // brand="amazon" rows: 0,1,2,3,12
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (0, "amazon".to_string(), 50));
    assert_eq!(rows[1], (1, "amazon".to_string(), 150));
    assert_eq!(rows[2], (2, "amazon".to_string(), 80));
    assert_eq!(rows[3], (3, "amazon".to_string(), 120));
    assert_eq!(rows[4], (12, "amazon".to_string(), 30));
}

/// Verify detection — `__row_id__` column in SELECT triggers emit_row_ids mode.
#[tokio::test]
async fn test_row_id_column_detection() {
    use crate::indexed_table::substrait_to_tree::plan_requests_row_ids;

    let schema = build_fixture_schema();
    let row_ids: Vec<i64> = (0..16).collect();
    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(datafusion::arrow::array::StringArray::from(BRANDS.to_vec())),
            Arc::new(datafusion::arrow::array::Int32Array::from(PRICES.to_vec())),
            Arc::new(datafusion::arrow::array::StringArray::from(STATUSES.to_vec())),
            Arc::new(datafusion::arrow::array::StringArray::from(CATEGORIES.to_vec())),
            Arc::new(datafusion::arrow::array::Int64Array::from(row_ids)),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let mem_table = datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(mem_table)).unwrap();

    // Plan with __row_id__ in projection — should be detected
    let df = ctx.sql("SELECT \"__row_id__\" FROM t").await.unwrap();
    let plan = df.logical_plan();
    assert!(
        plan_requests_row_ids(plan),
        "Should detect __row_id__ column in projection"
    );

    // Plan without __row_id__ — should NOT be detected
    let df2 = ctx.sql("SELECT brand FROM t").await.unwrap();
    let plan2 = df2.logical_plan();
    assert!(
        !plan_requests_row_ids(plan2),
        "Should not detect __row_id__ in normal projection"
    );
}

// ── Multi-segment row ID tests ──────────────────────────────────────────────

/// Build fixture schema with `__row_id__` column name (double underscore prefix
/// and suffix) matching what `IndexedTableProvider.scan()` looks for.
fn build_row_id_fixture_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("__row_id__", DataType::Int64, false),
    ]))
}

/// Write fixture parquet with `__row_id__` column name for emit_row_ids tests.
fn write_row_id_fixture_parquet() -> NamedTempFile {
    let schema = build_row_id_fixture_schema();
    let row_ids: Vec<i64> = (0..16).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(BRANDS.to_vec())),
            Arc::new(Int32Array::from(PRICES.to_vec())),
            Arc::new(StringArray::from(STATUSES.to_vec())),
            Arc::new(StringArray::from(CATEGORIES.to_vec())),
            Arc::new(Int64Array::from(row_ids)),
        ],
    )
    .unwrap();
    let tmp = NamedTempFile::new().unwrap();
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(8)
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    tmp
}

/// Helper: run a query with `emit_row_ids=true` across two segments and return sorted row IDs.
/// Each segment is a separate parquet file with 16 rows. Segment 1 has global_base=0,
/// segment 2 has global_base=16, so combined IDs span 0..31.
async fn run_two_segments_row_ids(tree: BoolNode) -> Vec<i64> {
    let tmp1 = write_row_id_fixture_parquet();
    let tmp2 = write_row_id_fixture_parquet();

    let path1 = tmp1.path().to_path_buf();
    let path2 = tmp2.path().to_path_buf();
    let size1 = std::fs::metadata(&path1).unwrap().len();
    let size2 = std::fs::metadata(&path2).unwrap().len();

    // Load metadata for segment 1
    let file1 = std::fs::File::open(&path1).unwrap();
    let meta1 =
        ArrowReaderMetadata::load(&file1, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let schema = meta1.schema().clone();
    let parquet_meta1 = meta1.metadata().clone();
    let mut rgs1 = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta1.num_row_groups() {
        let n = parquet_meta1.row_group(i).num_rows();
        rgs1.push(RowGroupInfo {
            index: i,
            first_row: offset,
            num_rows: n,
        });
        offset += n;
    }

    // Load metadata for segment 2
    let file2 = std::fs::File::open(&path2).unwrap();
    let meta2 =
        ArrowReaderMetadata::load(&file2, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let parquet_meta2 = meta2.metadata().clone();
    let mut rgs2 = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta2.num_row_groups() {
        let n = parquet_meta2.row_group(i).num_rows();
        rgs2.push(RowGroupInfo {
            index: i,
            first_row: offset,
            num_rows: n,
        });
        offset += n;
    }

    let object_path1 = object_store::path::Path::from(path1.to_string_lossy().as_ref());
    let object_path2 = object_store::path::Path::from(path2.to_string_lossy().as_ref());

    let segment1 = SegmentFileInfo {
        segment_ord: 0,
        max_doc: 16,
        object_path: object_path1,
        parquet_size: size1,
        row_groups: rgs1,
        metadata: Arc::clone(&parquet_meta1),
        global_base: 0,
    };
    let segment2 = SegmentFileInfo {
        segment_ord: 1,
        max_doc: 16,
        object_path: object_path2,
        parquet_size: size2,
        row_groups: rgs2,
        metadata: Arc::clone(&parquet_meta2),
        global_base: 16,
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
        segments: vec![segment1, segment2],
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
    let df = ctx.sql("SELECT \"__row_id__\" FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();
    let mut row_ids: Vec<i64> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        assert_eq!(b.num_columns(), 1, "should have only __row_id__ column");
        assert_eq!(b.schema().field(0).name(), "__row_id__");
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            row_ids.push(col.value(i));
        }
    }
    row_ids.sort();
    row_ids
}

/// Test: two segments produce globally unique row IDs.
/// Segment 1 has rows 0..15 (global_base=0), segment 2 has rows 0..15 (global_base=16).
/// Combined unfiltered query should produce IDs 0..31 with no gaps.
#[tokio::test]
async fn test_emit_row_ids_two_segments_global_base() {
    // No filter — use a predicate that matches all rows (price >= 0)
    let tree = BoolNode::And(vec![pred_int("price", Operator::GtEq, 0)]);
    let ids = run_two_segments_row_ids(tree).await;

    // Each segment has 16 rows. With global_base=0 and global_base=16,
    // we expect all IDs from 0 through 31 inclusive.
    let expected: Vec<i64> = (0..32).collect();
    assert_eq!(ids.len(), 32, "should have 32 row IDs (16 per segment)");
    assert_eq!(ids, expected, "row IDs should cover 0..31 with no gaps");

    // Verify uniqueness explicitly
    let unique: std::collections::HashSet<i64> = ids.iter().copied().collect();
    assert_eq!(unique.len(), 32, "all 32 row IDs should be unique");
}

/// Test: filtered query across two segments returns correct global IDs.
/// brand="amazon" matches rows 0,1,2,3,12 within each segment.
/// Segment 1 (global_base=0) gives IDs: 0,1,2,3,12
/// Segment 2 (global_base=16) gives IDs: 16,17,18,19,28
#[tokio::test]
async fn test_emit_row_ids_two_segments_with_filter() {
    // Collector tag 0 = brand_eq("amazon") — matches rows 0,1,2,3,12
    let tree = BoolNode::And(vec![index_leaf(0)]);
    let ids = run_two_segments_row_ids(tree).await;

    // Segment 1: amazon rows at positions 0,1,2,3,12 + global_base 0 = 0,1,2,3,12
    // Segment 2: amazon rows at positions 0,1,2,3,12 + global_base 16 = 16,17,18,19,28
    let expected: Vec<i64> = vec![0, 1, 2, 3, 12, 16, 17, 18, 19, 28];
    assert_eq!(
        ids.len(),
        10,
        "should have 10 row IDs (5 amazon rows per segment)"
    );
    assert_eq!(ids, expected, "filtered row IDs should be offset by global_base");
}
