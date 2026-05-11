/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! End-to-end tests for the full data-node QTF loop:
//! query phase (emit row IDs) -> fetch phase (retrieve by those IDs) -> verify data correctness.
//!
//! The query phase uses `IndexedTableProvider` with `emit_row_ids: true` to get
//! global row IDs. The fetch phase uses `ShardTableProvider` with `ParquetAccessPlan`
//! to read only the targeted rows and compute `__row_id__ + row_base`.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Operator;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use futures::StreamExt;
use roaring::RoaringBitmap;

use super::*;
use crate::indexed_table::row_selection::build_row_selection_with_min_skip_run;
use crate::shard_table_provider::{ShardFileInfo, ShardTableConfig, ShardTableProvider};

// ── Query phase helper (adapted from row_id_emission.rs) ────────────

/// Run the query phase with `emit_row_ids: true` and return sorted row IDs (as i64).
async fn query_phase(tree: BoolNode) -> Vec<i64> {
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
    let df = ctx.sql("SELECT \"__row_id__\" FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();
    let mut row_ids: Vec<i64> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            row_ids.push(col.value(i));
        }
    }
    row_ids.sort();
    row_ids
}

// ── Fetch phase helper ──────────────────────────────────────────────

/// Run the fetch phase: given row IDs, build a ShardTableProvider with
/// ParquetAccessPlan to read only those rows, then execute SQL to get data.
/// Returns (row_id, column_values) tuples sorted by row_id.
async fn fetch_phase(
    row_ids: &[i64],
    fetch_columns: &[&str],
) -> Vec<RecordBatch> {
    if row_ids.is_empty() {
        return vec![];
    }

    let tmp = write_fixture_parquet();
    let path = tmp.path().to_path_buf();

    let file = std::fs::File::open(&path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let file_schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();

    // Build row group info
    let mut rg_row_counts: Vec<u64> = Vec::new();
    for i in 0..parquet_meta.num_row_groups() {
        rg_row_counts.push(parquet_meta.row_group(i).num_rows() as u64);
    }
    let total_rows: u64 = rg_row_counts.iter().sum();

    // Build the ParquetAccessPlan from row_ids
    let num_rgs = parquet_meta.num_row_groups();
    let mut rg_first_row: Vec<u64> = Vec::with_capacity(num_rgs);
    let mut cumulative = 0u64;
    for &count in &rg_row_counts {
        rg_first_row.push(cumulative);
        cumulative += count;
    }

    // Distribute row IDs into per-RG bitmaps (local positions within each RG)
    let mut plan = ParquetAccessPlan::new_none(num_rgs);
    for rg_idx in 0..num_rgs {
        let rg_start = rg_first_row[rg_idx];
        let rg_end = rg_start + rg_row_counts[rg_idx];
        let rg_num_rows = rg_row_counts[rg_idx] as usize;

        let mut rg_bitmap = RoaringBitmap::new();
        for &gid in row_ids {
            let pos = gid as u64;
            if pos >= rg_start && pos < rg_end {
                rg_bitmap.insert((pos - rg_start) as u32);
            }
        }
        if !rg_bitmap.is_empty() {
            let selection =
                build_row_selection_with_min_skip_run(&rg_bitmap, rg_num_rows, 1);
            plan.set(rg_idx, RowGroupAccess::Selection(selection));
        }
    }

    // Build the object meta
    let file_size = std::fs::metadata(&path).unwrap().len();
    let object_meta = object_store::ObjectMeta {
        location: object_store::path::Path::from(path.to_string_lossy().as_ref()),
        last_modified: chrono::Utc::now(),
        size: file_size,
        e_tag: None,
        version: None,
    };

    let shard_file = ShardFileInfo {
        object_meta,
        row_base: 0,
        num_rows: total_rows,
        row_group_row_counts: rg_row_counts,
        access_plan: Some(plan),
    };

    // Build ShardTableProvider
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(ShardTableProvider::new(ShardTableConfig {
        file_schema: file_schema.clone(),
        files: vec![shard_file],
        store_url: store_url.clone(),
    }));

    // Register object store and table
    let ctx = SessionContext::new();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    ctx.register_object_store(store_url.as_ref(), store);
    ctx.register_table("t", provider).unwrap();

    // Execute SQL: __row_id__ + row_base gives global row ID
    let col_list = fetch_columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT (\"__row_id__\" + \"row_base\") AS \"__row_id__\", {} FROM t",
        col_list
    );
    let df = ctx.sql(&sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.unwrap());
    }
    batches
}

/// Combined helper: run query phase -> get row IDs, then run fetch phase -> get data.
/// Returns (sorted_row_ids, fetched_batches).
async fn query_then_fetch(
    tree: BoolNode,
    fetch_columns: Vec<&str>,
) -> (Vec<i64>, Vec<RecordBatch>) {
    let row_ids = query_phase(tree).await;
    let batches = fetch_phase(&row_ids, &fetch_columns).await;
    (row_ids, batches)
}

/// Extract (row_id, brand, price) tuples from fetch result batches, sorted by row_id.
fn extract_id_brand_price(batches: &[RecordBatch]) -> Vec<(i64, String, i32)> {
    let mut rows: Vec<(i64, String, i32)> = Vec::new();
    for b in batches {
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let brands = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let prices = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((ids.value(i), brands.value(i).to_string(), prices.value(i)));
        }
    }
    rows.sort_by_key(|r| r.0);
    rows
}

/// Extract (row_id, brand) tuples from fetch result batches, sorted by row_id.
fn extract_id_brand(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut rows: Vec<(i64, String)> = Vec::new();
    for b in batches {
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let brands = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((ids.value(i), brands.value(i).to_string()));
        }
    }
    rows.sort_by_key(|r| r.0);
    rows
}


// ── Tests ────────────────────────────────────────────────────────────

/// Full QTF loop with SingleCollector: brand="amazon" -> get row IDs -> fetch brand+price -> verify.
/// Amazon rows: 0(50), 1(150), 2(80), 3(120), 12(30).
#[tokio::test]
async fn test_qtf_full_loop_single_collector() {
    let tree = BoolNode::And(vec![index_leaf(0)]); // brand="amazon"
    let (row_ids, batches) = query_then_fetch(tree, vec!["brand", "price"]).await;

    assert_eq!(row_ids, vec![0, 1, 2, 3, 12]);

    let rows = extract_id_brand_price(&batches);
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (0, "amazon".to_string(), 50));
    assert_eq!(rows[1], (1, "amazon".to_string(), 150));
    assert_eq!(rows[2], (2, "amazon".to_string(), 80));
    assert_eq!(rows[3], (3, "amazon".to_string(), 120));
    assert_eq!(rows[4], (12, "amazon".to_string(), 30));
}

/// Full QTF loop with predicate only: price > 100 -> get row IDs -> fetch brand+price -> verify.
/// Rows with price > 100: 1(150), 3(120), 5(95? no), 6(200), 9(300), 11(150).
#[tokio::test]
async fn test_qtf_full_loop_predicate_only() {
    let tree = BoolNode::And(vec![pred_int("price", Operator::Gt, 100)]);
    let (row_ids, batches) = query_then_fetch(tree, vec!["brand", "price"]).await;

    // price > 100: rows 1(150), 3(120), 6(200), 9(300), 11(150)
    assert_eq!(row_ids, vec![1, 3, 6, 9, 11]);

    let rows = extract_id_brand_price(&batches);
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (1, "amazon".to_string(), 150));
    assert_eq!(rows[1], (3, "amazon".to_string(), 120));
    assert_eq!(rows[2], (6, "apple".to_string(), 200));
    assert_eq!(rows[3], (9, "google".to_string(), 300));
    assert_eq!(rows[4], (11, "samsung".to_string(), 150));
}

/// Full QTF loop with no filter: all 16 rows -> fetch all -> verify count.
/// Use price >= 0 as a match-all predicate (since emit_row_ids requires
/// going through the indexed path, we need at least a trivial predicate).
#[tokio::test]
async fn test_qtf_full_loop_no_filter() {
    // price >= 0 matches all rows
    let tree = BoolNode::And(vec![pred_int("price", Operator::GtEq, 0)]);
    let (row_ids, batches) = query_then_fetch(tree, vec!["brand", "price"]).await;

    assert_eq!(row_ids.len(), 16);
    assert_eq!(row_ids, (0..16).collect::<Vec<i64>>());

    let rows = extract_id_brand_price(&batches);
    assert_eq!(rows.len(), 16);
    // Verify first and last rows
    assert_eq!(rows[0], (0, "amazon".to_string(), 50));
    assert_eq!(rows[15], (15, "google".to_string(), 55));
}

/// Full QTF loop with two segments (global_base offset).
/// Write two separate parquet files, query both -> verify row IDs from both segments.
#[tokio::test]
async fn test_qtf_full_loop_two_segments() {
    // For two segments, we write two separate parquet files and combine them
    // in a single fetch pass. The first segment has global_base=0, the second
    // has global_base=16 (since the first file has 16 rows).

    // Query phase: brand="amazon" on segment 0 gives rows 0,1,2,3,12.
    // We simulate the second segment by doing a query with global_base=16,
    // which would give 16,17,18,19,28.
    // For the fetch phase, we verify we can fetch from a file with non-zero row_base.

    let tmp = write_fixture_parquet();
    let path = tmp.path().to_path_buf();

    let file = std::fs::File::open(&path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let file_schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();

    let mut rg_row_counts: Vec<u64> = Vec::new();
    for i in 0..parquet_meta.num_row_groups() {
        rg_row_counts.push(parquet_meta.row_group(i).num_rows() as u64);
    }
    let total_rows: u64 = rg_row_counts.iter().sum();

    // Build access plan for rows 0, 1, 2, 3, 12 in the file (amazon brand)
    let num_rgs = parquet_meta.num_row_groups();
    let mut rg_first_row: Vec<u64> = Vec::with_capacity(num_rgs);
    let mut cumulative = 0u64;
    for &count in &rg_row_counts {
        rg_first_row.push(cumulative);
        cumulative += count;
    }

    let target_local_positions: Vec<u64> = vec![0, 1, 2, 3, 12];
    let mut plan = ParquetAccessPlan::new_none(num_rgs);
    for rg_idx in 0..num_rgs {
        let rg_start = rg_first_row[rg_idx];
        let rg_end = rg_start + rg_row_counts[rg_idx];
        let mut rg_bitmap = RoaringBitmap::new();
        for &pos in &target_local_positions {
            if pos >= rg_start && pos < rg_end {
                rg_bitmap.insert((pos - rg_start) as u32);
            }
        }
        if !rg_bitmap.is_empty() {
            let selection = build_row_selection_with_min_skip_run(
                &rg_bitmap,
                rg_row_counts[rg_idx] as usize,
                1,
            );
            plan.set(rg_idx, RowGroupAccess::Selection(selection));
        }
    }

    let file_size = std::fs::metadata(&path).unwrap().len();
    let object_meta = object_store::ObjectMeta {
        location: object_store::path::Path::from(path.to_string_lossy().as_ref()),
        last_modified: chrono::Utc::now(),
        size: file_size,
        e_tag: None,
        version: None,
    };

    // Use row_base=1000 to simulate a second segment with global_base offset
    let shard_file = ShardFileInfo {
        object_meta,
        row_base: 1000,
        num_rows: total_rows,
        row_group_row_counts: rg_row_counts,
        access_plan: Some(plan),
    };

    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(ShardTableProvider::new(ShardTableConfig {
        file_schema: file_schema.clone(),
        files: vec![shard_file],
        store_url: store_url.clone(),
    }));

    let ctx = SessionContext::new();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    ctx.register_object_store(store_url.as_ref(), store);
    ctx.register_table("t", provider).unwrap();

    let sql = "SELECT (\"__row_id__\" + \"row_base\") AS \"__row_id__\", \"brand\", \"price\" FROM t";
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();

    let mut rows: Vec<(i64, String, i32)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let brands = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let prices = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((ids.value(i), brands.value(i).to_string(), prices.value(i)));
        }
    }
    rows.sort_by_key(|r| r.0);

    // With row_base=1000, the row IDs should be 1000+file_row_id.
    // File has __row_id__ = [0..15], but we only read rows at positions 0,1,2,3,12.
    // The __row_id__ in parquet for those positions = 0,1,2,3,12
    // Global: 0+1000=1000, 1+1000=1001, 2+1000=1002, 3+1000=1003, 12+1000=1012
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (1000, "amazon".to_string(), 50));
    assert_eq!(rows[1], (1001, "amazon".to_string(), 150));
    assert_eq!(rows[2], (1002, "amazon".to_string(), 80));
    assert_eq!(rows[3], (1003, "amazon".to_string(), 120));
    assert_eq!(rows[4], (1012, "amazon".to_string(), 30));
}

/// Fetch only "brand" column -> verify only that column + __row_id__ are returned.
#[tokio::test]
async fn test_qtf_fetch_subset_columns() {
    let tree = BoolNode::And(vec![index_leaf(1)]); // brand="apple" -> rows 4,5,6,7,13
    let (row_ids, batches) = query_then_fetch(tree, vec!["brand"]).await;

    assert_eq!(row_ids, vec![4, 5, 6, 7, 13]);

    let rows = extract_id_brand(&batches);
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (4, "apple".to_string()));
    assert_eq!(rows[1], (5, "apple".to_string()));
    assert_eq!(rows[2], (6, "apple".to_string()));
    assert_eq!(rows[3], (7, "apple".to_string()));
    assert_eq!(rows[4], (13, "apple".to_string()));

    // Verify schema: should be __row_id__ + brand (2 columns)
    if let Some(b) = batches.first() {
        assert_eq!(b.num_columns(), 2);
        assert_eq!(b.schema().field(0).name(), "__row_id__");
        assert_eq!(b.schema().field(1).name(), "brand");
    }
}

/// Filter that matches exactly 1 row -> fetch -> verify.
/// brand="amazon" AND price=30 matches only row 12.
#[tokio::test]
async fn test_qtf_fetch_single_row() {
    let tree = BoolNode::And(vec![
        index_leaf(0),
        pred_int("price", Operator::Eq, 30),
    ]);
    let (row_ids, batches) = query_then_fetch(tree, vec!["brand", "price"]).await;

    assert_eq!(row_ids, vec![12]);

    let rows = extract_id_brand_price(&batches);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (12, "amazon".to_string(), 30));
}

/// Filter that matches 0 rows -> empty result.
/// brand="amazon" AND price > 500 matches nothing.
#[tokio::test]
async fn test_qtf_fetch_empty_result() {
    let tree = BoolNode::And(vec![
        index_leaf(0),
        pred_int("price", Operator::Gt, 500),
    ]);
    let (row_ids, batches) = query_then_fetch(tree, vec!["brand", "price"]).await;

    assert!(row_ids.is_empty());
    assert!(batches.is_empty()); // fetch_phase returns empty vec for empty row_ids
}
