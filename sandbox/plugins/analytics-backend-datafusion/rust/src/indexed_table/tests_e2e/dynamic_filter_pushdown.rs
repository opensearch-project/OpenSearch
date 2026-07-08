/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! End-to-end verification of runtime dynamic-filter (TopK) pruning on the
//! indexed scan.
//!
//! Builds `SELECT ... ORDER BY price DESC LIMIT k` over a single segment with
//! four disjoint-range row groups, lets DataFusion's default physical optimizer
//! insert `SortExec { fetch }` + push its `DynamicFilterPhysicalExpr` into
//! `QueryShardExec`, executes, then asserts:
//!   1. results equal the full-scan top-k (correctness), and
//!   2. `dynamic_filter_rg_pruned > 0` (the optimization actually fired).

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::ArrowWriter;
use futures::StreamExt;
use tempfile::NamedTempFile;

use super::super::eval::RowGroupBitsetSource;
use super::super::index::RowGroupDocsCollector;
use super::super::page_pruner::PagePruner;
use super::super::stream::{FilterStrategy, RowGroupInfo};
use super::super::table_provider::{IndexedTableConfig, IndexedTableProvider, SegmentFileInfo};

/// 16 rows, `price` = 15..0 **descending** in file order, 4 rows per row group →
/// RG ranges (by row position) [15..12], [11..8], [7..4], [3..0]. The scan reads
/// row groups in index order, so the *highest* prices arrive first. That is the
/// "extreme values appear early" shape a DESC-TopK needs: after RG0 the heap
/// threshold is high enough to prune the later, lower-valued row groups.
///
/// This mirrors a time-ordered OpenSearch index queried `ORDER BY ts DESC LIMIT
/// k` where recent (high) values sit in the first row groups read.
const ROWS: usize = 16;
const RG_ROWS: usize = 4;

fn write_fixture() -> (NamedTempFile, SchemaRef) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Int32, false),
    ]));
    let brands: Vec<String> = (0..ROWS).map(|i| format!("b{i}")).collect();
    // Descending in file order: row 0 has the highest price (15), so RG0 holds
    // the top values and a DESC TopK can prune later RGs.
    let prices: Vec<i32> = (0..ROWS as i32).map(|i| ROWS as i32 - 1 - i).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(brands)),
            Arc::new(Int32Array::from(prices)),
        ],
    )
    .unwrap();
    let tmp = NamedTempFile::new().unwrap();
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(RG_ROWS)
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    (tmp, schema)
}

/// Match-all collector: every doc in `[min_doc, max_doc)` is a candidate, so
/// the only thing that can prune a row group is the dynamic filter.
#[derive(Debug)]
struct MatchAllCollector;

impl RowGroupDocsCollector for MatchAllCollector {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        let span = (max_doc - min_doc).max(0) as usize;
        let mut out = vec![0u64; span.div_ceil(64)];
        for rel in 0..span {
            out[rel / 64] |= 1u64 << (rel % 64);
        }
        Ok(out)
    }
}

/// Build the indexed provider over the fixture and run `sql`. Returns the
/// `(price)` rows in emission order plus the executed physical plan (for
/// reading metrics).
async fn run_indexed(sql: &str) -> (Vec<i32>, Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    let (tmp, schema) = write_fixture();
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::File::open(&path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
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
    assert!(
        rgs.len() >= 4,
        "fixture should have >=4 row groups, got {}",
        rgs.len()
    );

    let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
    let segment = SegmentFileInfo {
        writer_generation: 0,
        max_doc: offset,
        object_path,
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
        global_base: 0,
        sort_min: None,
        sort_max: None,
    };

    let factory: super::super::table_provider::EvaluatorFactory = {
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics, _stats_prune_tree| {
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let collector: Arc<dyn RowGroupDocsCollector> = Arc::new(MatchAllCollector);
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(
                crate::indexed_table::eval::single_collector::SingleCollectorEvaluator::new(
                    Some(collector),
                    pruner,
                    None,
                    None,
                    None,
                    None,
                    crate::indexed_table::eval::single_collector::CollectorCallStrategy::FullRange,
                    std::sync::Arc::new(std::collections::HashMap::new()),
                    segment.writer_generation,
                    std::sync::Arc::new(
                        crate::indexed_table::eval::single_collector::FfmDelegatedBackendCollectorFactory,
                    ),
                    0,
                    None,
                    None,
                    std::collections::HashMap::new(),
                ),
            );
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    // Row-granular so the parquet stream + dynamic filter behave like production;
    // single partition so all RGs flow through one stream.
    // One batch per row group (RG_ROWS): the stream flushes after each RG so
    // SortExec can tighten the dynamic filter between row groups, instead of
    // coalescing all rows into a single end-of-stream batch.
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .batch_size(RG_ROWS)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store,
        store_url,
        evaluator_factory: factory,
        pushdown_predicate: None,
        query_config: std::sync::Arc::new(qc),
        predicate_columns: vec![],
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        cancellation_token: None,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), task_ctx).unwrap();
    let mut prices: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let price = b
            .column(b.schema().index_of("price").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            prices.push(price.value(i));
        }
    }
    (prices, plan)
}

/// Recursively sum a named counter across the plan tree.
fn sum_metric(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, name: &str) -> usize {
    let mut total = 0usize;
    if let Some(metrics) = plan.metrics() {
        total += metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
    }
    for child in plan.children() {
        total += sum_metric(child, name);
    }
    total
}

fn rg_pruned_at_prefetch(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> usize {
    sum_metric(plan, "dynamic_filter_rg_pruned_at_prefetch")
}

fn rg_pruned_at_poll(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> usize {
    sum_metric(plan, "dynamic_filter_rg_pruned_at_poll")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topk_dynamic_filter_prunes_row_groups() {
    // ORDER BY price DESC LIMIT 2 → top-2 prices are 15, 14 (both in the last RG
    // [12..15]). As the TopK heap fills, the threshold rises and the earlier RGs
    // ([0..3], [4..7], [8..11]) become prunable.
    let (prices, plan) =
        run_indexed("SELECT brand, price FROM t ORDER BY price DESC LIMIT 2").await;

    // (1) Correctness: exactly the global top-2 by price, in DESC order.
    assert_eq!(prices, vec![15, 14], "top-2 DESC prices");

    // (2) Both prune phases fire for this query. The prefetch runs ~1 RG ahead,
    // so once RG0 fills the heap, the next RGs are pruned BEFORE their Lucene
    // eval (prefetch phase); the final RG is caught at the poll phase after the
    // filter tightens further. We assert each phase independently to prove both
    // code paths are exercised.
    let at_prefetch = rg_pruned_at_prefetch(&plan);
    let at_poll = rg_pruned_at_poll(&plan);
    assert!(
        at_prefetch > 0,
        "expected >=1 RG pruned at the PREFETCH phase (skipping the Lucene eval), \
         at_prefetch={at_prefetch} at_poll={at_poll}"
    );
    assert!(
        at_poll > 0,
        "expected >=1 RG pruned at the POLL phase (backstop after further tightening), \
         at_prefetch={at_prefetch} at_poll={at_poll}"
    );
    // Three of four RGs pruned (RG0 is processed to fill the heap).
    assert_eq!(at_prefetch + at_poll, 3, "expected 3 of 4 RGs pruned");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_limit_means_no_dynamic_filter_pruning() {
    // Without a fetch/limit there is no TopK and hence no dynamic filter, so
    // nothing should be pruned and all 16 rows are returned.
    let (prices, plan) = run_indexed("SELECT brand, price FROM t ORDER BY price DESC").await;
    assert_eq!(prices.len(), ROWS, "full result set without LIMIT");
    assert_eq!(prices.first().copied(), Some(15));
    assert_eq!(
        rg_pruned_at_prefetch(&plan),
        0,
        "no filter → no prefetch prune"
    );
    assert_eq!(rg_pruned_at_poll(&plan), 0, "no filter → no poll prune");
}
