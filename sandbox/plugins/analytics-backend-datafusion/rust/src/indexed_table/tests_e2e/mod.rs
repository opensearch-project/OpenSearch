/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! End-to-end tests covering `IndexedTableProvider` → `IndexedExec` →
//! `IndexedStream` → `BitmapTreeEvaluator` with a complex boolean tree.
//!
//! Bypass the Java/FFM + substrait layers by constructing the tree and
//! collectors directly in Rust; that keeps the test hermetic while still
//! exercising the full streaming pipeline end-to-end.

#![cfg(test)]

use std::sync::Arc;
use std::sync::OnceLock;

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Operator;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use futures::StreamExt;
use tempfile::NamedTempFile;

use super::bool_tree::{BoolNode, ResolvedPredicate};
use super::eval::bitmap_tree::{CollectorLeafBitmaps, BitmapTreeEvaluator};
use super::eval::{RowGroupBitsetSource, TreeBitsetSource};
use super::index::RowGroupDocsCollector;
use super::page_pruner::PagePruner;
use super::stream::{FilterStrategy, RowGroupInfo};
use super::table_provider::{IndexedTableConfig, IndexedTableProvider, SegmentFileInfo};

mod boolean_algebra;
mod multi_segment;
mod null_columns;
mod schema_drift;
mod streaming_at_scale;

// ── Test fixture: parquet table with 16 rows ────────────────────────
//
// | row | brand  | price | status    | category    |
// |-----|--------|-------|-----------|-------------|
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

const BRANDS: [&str; 16] = [
    "amazon", "amazon", "amazon", "amazon",
    "apple", "apple", "apple", "apple",
    "google", "google", "samsung", "samsung",
    "amazon", "apple", "samsung", "google",
];
const PRICES: [i32; 16] = [50, 150, 80, 120, 90, 95, 200, 60, 40, 300, 70, 150, 30, 45, 99, 55];
const STATUSES: [&str; 16] = [
    "active", "archived", "active", "active",
    "active", "archived", "active", "active",
    "active", "archived", "active", "active",
    "archived", "archived", "active", "active",
];
const CATEGORIES: [&str; 16] = [
    "electronics", "electronics", "books", "electronics",
    "electronics", "electronics", "books", "electronics",
    "electronics", "electronics", "electronics", "books",
    "electronics", "electronics", "electronics", "electronics",
];

fn build_fixture_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
    ]))
}

fn write_fixture_parquet() -> NamedTempFile {
    let schema = build_fixture_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(BRANDS.to_vec())),
            Arc::new(Int32Array::from(PRICES.to_vec())),
            Arc::new(StringArray::from(STATUSES.to_vec())),
            Arc::new(StringArray::from(CATEGORIES.to_vec())),
        ],
    )
    .unwrap();
    let tmp = NamedTempFile::new().unwrap();
    // Use smallish row groups so there's > 1 and the streaming loop cycles.
    // Enable page index so PagePruner can prune predicates.
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(8)
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut w =
        ArrowWriter::try_new(tmp.reopen().unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    tmp
}

// ── Mock index-backend-like collector ─────────────────────────────────────
//
// Takes a pre-computed set of matching doc ids (absolute). Returns bits for
// doc ids in `[min_doc, max_doc)` as a packed u64[] bitset.

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

/// Build a collector that returns docs matching `brand == value`.
fn brand_eq(value: &str) -> Arc<dyn RowGroupDocsCollector> {
    let matching: Vec<i32> = BRANDS
        .iter()
        .enumerate()
        .filter(|(_, b)| **b == value)
        .map(|(i, _)| i as i32)
        .collect();
    Arc::new(MockCollector { matching })
}

/// Build a collector that returns docs matching `status == value`.
fn status_eq(value: &str) -> Arc<dyn RowGroupDocsCollector> {
    let matching: Vec<i32> = STATUSES
        .iter()
        .enumerate()
        .filter(|(_, s)| **s == value)
        .map(|(i, _)| i as i32)
        .collect();
    Arc::new(MockCollector { matching })
}

// ── Test runner: build provider + execute + collect rows ───────────

async fn run_tree(tree: BoolNode, predicates: Vec<ResolvedPredicate>) -> Vec<(String, i32, String, String)> {
    let tmp = write_fixture_parquet();
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();

    // Load parquet metadata for the SegmentFileInfo.
    let file = std::fs::File::open(&path).unwrap();
    let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
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

    let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
    let segment = SegmentFileInfo {
        segment_ord: 0,
        max_doc: 16,
        object_path,
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
    };

    // Normalize NOT push-down; build one collector per Collector leaf in DFS order.
    let tree = tree.push_not_down();
    let collectors = wire_collectors(&tree);
    // Test provider_key assignment: index in DFS order. Real provider keys
    // come from `createProvider` upcalls; tests don't cross FFM so any
    // distinct i32 per leaf works.
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = collectors
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let tree = Arc::new(tree);
    let predicates: Arc<Vec<Arc<ResolvedPredicate>>> =
        Arc::new(predicates.into_iter().map(Arc::new).collect());

    let factory: super::table_provider::EvaluatorFactory = {
        let per_leaf = per_leaf.clone();
        let predicates = Arc::clone(&predicates);
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk| {
            let resolved = tree.resolve(&per_leaf, &predicates)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata), &[]));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(CollectorLeafBitmaps),
                page_pruner: pruner,
                cost_predicate: 1,
                cost_collector: 10,
            });
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url =
        datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store,
        store_url,
        evaluator_factory: factory,
        target_partitions: 1,
        // Force BooleanMask so batches contain the entire RG and batch_offset
        // equals the row-index-within-RG. Phase 2 bitmap_to_batch_mask
        // relies on this alignment. RowSelection would still work for Path B
        // (no Phase-2 mask), but Path C tree eval requires BooleanMask today.
        force_strategy: Some(FilterStrategy::BooleanMask),
        force_pushdown: Some(false),
        query_config: std::sync::Arc::new(crate::datafusion_query_config::DatafusionQueryConfig::default()),
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT brand, price, status, category FROM t").await.unwrap();
    let mut stream = df.execute_stream().await.unwrap();
    let mut rows: Vec<(String, i32, String, String)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let brand = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let price = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let status = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let cat = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((
                brand.value(i).to_string(),
                price.value(i),
                status.value(i).to_string(),
                cat.value(i).to_string(),
            ));
        }
    }
    rows
}

// ── Tree-building helpers ──────────────────────────────────────────
//
// Test-only leaf encoding: `index_leaf(tag)` puts a single-byte tag into
// `BoolNode::Collector.query_bytes`. `wire_collectors_from_bytes` walks the
// tree in DFS order and returns one `Arc<dyn RowGroupDocsCollector>` per
// Collector leaf, matching the tag to a fixture-specific mock.

fn index_leaf(tag: u8) -> BoolNode {
    BoolNode::Collector { query_bytes: Arc::from(&[tag][..]) }
}
fn predicate_leaf(id: u16) -> BoolNode {
    BoolNode::Predicate { predicate_id: id }
}
fn pred_int(col: &str, op: Operator, v: i32) -> ResolvedPredicate {
    ResolvedPredicate {
        column: col.into(),
        op,
        value: ScalarValue::Int32(Some(v)),
    }
}
fn pred_str(col: &str, op: Operator, v: &str) -> ResolvedPredicate {
    ResolvedPredicate {
        column: col.into(),
        op,
        value: ScalarValue::Utf8(Some(v.into())),
    }
}

/// Push a ResolvedPredicate to the sidecar and return a Predicate leaf
/// pointing at it by index. Used by submodule tree lowerers.
fn push_pred(preds: &mut Vec<ResolvedPredicate>, p: ResolvedPredicate) -> BoolNode {
    let id = preds.len() as u16;
    preds.push(p);
    BoolNode::Predicate { predicate_id: id }
}

/// Walk `tree` in DFS order and return one collector per Collector leaf,
/// built from the leaf's `query_bytes` tag (0=amazon, 1=apple, 2=archived).
/// Result order matches `tree.collector_leaves()`.
fn wire_collectors(tree: &BoolNode) -> Vec<Arc<dyn RowGroupDocsCollector>> {
    let mut out = Vec::new();
    wire(tree, &mut out);
    out
}

fn wire(node: &BoolNode, out: &mut Vec<Arc<dyn RowGroupDocsCollector>>) {
    match node {
        BoolNode::And(c) | BoolNode::Or(c) => c.iter().for_each(|x| wire(x, out)),
        BoolNode::Not(inner) => wire(inner, out),
        BoolNode::Collector { query_bytes } => {
            let c: Arc<dyn RowGroupDocsCollector> = match query_bytes.first().copied() {
                Some(0) => brand_eq("amazon"),
                Some(1) => brand_eq("apple"),
                Some(2) => status_eq("archived"),
                other => panic!("unknown test collector tag {:?}", other),
            };
            out.push(c);
        }
        BoolNode::Predicate { .. } => {}
    }
}

