/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! End-to-end verification of runtime dynamic-filter pruning on the indexed
//! scan, for both filter producers DataFusion has: a TopK sort and a hash join.
//!
//! Every test uses one segment with four disjoint-range row groups, lets the
//! default physical optimizer place the producer and push its
//! `DynamicFilterPhysicalExpr` into `QueryShardExec`, executes, then asserts
//! results are correct AND that the row-group prune counters moved as expected.
//! Correctness is asserted independently of pruning, so a test can never pass
//! by silently dropping rows.
//!
//! **TopK** (`ORDER BY price DESC LIMIT k`): the filter starts loose and
//! tightens as the heap fills, so pruning lands at both the prefetch and poll
//! phases.
//!
//! **Hash join** (`FROM d JOIN t ON t.price = d.k`, `d` a small `MemTable`):
//! this is the engine's broadcast-probe shape, where the build side is an Arrow
//! `MemTable` registered on the same session as the shard scan — the only shape
//! in which the native join dynamic filter can fire, since the filter is a
//! shared mutable cell and cannot cross a process boundary. The filter is
//! complete as soon as the build side is collected rather than tightening, so
//! pruning lands entirely at the prefetch phase, and it prunes by IN-list
//! membership rather than by a min/max envelope.

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
use crate::indexed_table::index::CollectDocsResult;

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
    fn collect_packed_u64_bitset(
        &self,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<CollectDocsResult, String> {
        let span = (max_doc - min_doc).max(0) as usize;
        let mut out = vec![0u64; span.div_ceil(64)];
        for rel in 0..span {
            out[rel / 64] |= 1u64 << (rel % 64);
        }
        Ok(out.into())
    }
}

/// Build the indexed provider over the fixture. The returned `NamedTempFile`
/// must outlive execution — the parquet file is read lazily, per row group.
fn indexed_provider() -> (NamedTempFile, SchemaRef, Arc<IndexedTableProvider>) {
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
        arrow_schema: schema.clone(),
        global_base: 0,
        sort_min: None,
        sort_max: None,
    };

    let factory: super::super::table_provider::EvaluatorFactory = {
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics, _stats_prune_tree| {
            let pruner = Arc::new(PagePruner::new(
                &schema,
                Arc::clone(&segment.metadata),
                schema.clone(),
            ));
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

    (tmp, schema, provider)
}

/// Plan `sql` against `ctx`, execute it, and return the `price` column in
/// emission order plus the executed physical plan (for reading metrics).
async fn run_sql(
    ctx: &SessionContext,
    sql: &str,
) -> (Vec<i32>, Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
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

/// Build the indexed provider over the fixture and run `sql` against it alone.
async fn run_indexed(sql: &str) -> (Vec<i32>, Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    let (_tmp, _schema, provider) = indexed_provider();
    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    run_sql(&ctx, sql).await
}

/// Broadcast-join shape: a tiny in-memory build side `d(k)` joined to the
/// indexed scan `t` on `t.price = d.k`. This mirrors the engine's
/// broadcast-probe fragment, where the build side is an Arrow `MemTable`
/// registered on the same session as the shard scan, so `HashJoinExec` and
/// `QueryShardExec` live in one plan in one process — the only shape in which
/// DataFusion's native join dynamic filter can possibly fire.
///
/// `d` is the LEFT input so it is the `CollectLeft` build side.
async fn run_broadcast_join(
    build_keys: Vec<i32>,
) -> (Vec<i32>, Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    let (_tmp, _schema, provider) = indexed_provider();
    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();

    let build_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    let build_batch = RecordBatch::try_new(
        build_schema.clone(),
        vec![Arc::new(Int32Array::from(build_keys))],
    )
    .unwrap();
    let build =
        datafusion::datasource::MemTable::try_new(build_schema, vec![vec![build_batch]]).unwrap();
    ctx.register_table("d", Arc::new(build)).unwrap();

    run_sql(
        &ctx,
        "SELECT t.brand, t.price FROM d JOIN t ON t.price = d.k",
    )
    .await
}

/// Number of dynamic filters `QueryShardExec` actually accepted, summed over the
/// plan tree. Zero means no filter was ever delivered to the leaf; non-zero with
/// zero prune counters means a filter arrived but proved nothing.
fn accepted_dynamic_filters(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> usize {
    let mut total = 0usize;
    if let Some(scan) = plan.downcast_ref::<super::super::table_provider::QueryShardExec>() {
        total += scan.test_dynamic_filters().len();
    }
    for child in plan.children() {
        total += accepted_dynamic_filters(child);
    }
    total
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

    // (2) Both prune phases fire: the prefetch runs ~1 RG ahead, so once RG0 fills the heap the next RGs
    // are pruned before their Lucene eval; the last is caught at the poll phase after the filter tightens.
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
async fn broadcast_join_dynamic_filter_reaches_indexed_scan() {
    // Build keys {14, 15} match only RG0 (prices 12..15). A MinMax filter
    // `price >= 14 AND price <= 15` therefore excludes RG1 (max 11), RG2 (max 7)
    // and RG3 (max 3); an IN-list filter excludes them too.
    let (mut prices, plan) = run_broadcast_join(vec![15, 14]).await;
    prices.sort_unstable();

    // (1) Correctness first, independent of whether any filter fired.
    assert_eq!(prices, vec![14, 15], "join result");

    let accepted = accepted_dynamic_filters(&plan);
    let at_prefetch = rg_pruned_at_prefetch(&plan);
    let at_poll = rg_pruned_at_poll(&plan);
    // Only rendered when an assertion below fails.
    let rendered = || {
        format!(
            "accepted={accepted} at_prefetch={at_prefetch} at_poll={at_poll}\nplan:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        )
    };

    // (2) The join's dynamic filter must reach the indexed scan — the whole point of the broadcast shape:
    // build side and probe scan share one plan in one process, so FilterPushdown can deliver it.
    assert!(
        accepted > 0,
        "expected the join's dynamic filter to reach QueryShardExec\n{}",
        rendered()
    );

    // (3) And it must prune. Build keys land only in RG0, so the other three are excluded at the prefetch
    // phase: a join filter is complete once the build side is collected, unlike a tightening TopK heap.
    assert_eq!(
        (at_prefetch, at_poll),
        (3, 0),
        "expected 3 of 4 RGs pruned, all at the prefetch phase\n{}",
        rendered()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_join_filter_prunes_by_membership_not_just_range() {
    // Build keys at both extremes — 15 in RG0, 0 in RG3 — so the build side's min/max envelope spans every
    // row group and a pure range filter would prune nothing. Two are pruned anyway, which shows the pushed
    // filter is an IN-list evaluated per value against each row group's statistics: RG1 (8..11) and RG2
    // (4..7) hold neither key.
    //
    // This is the mechanism a runtime filter inherits for free on broadcast shapes, and why a value-set
    // payload beats a MinMax one. It holds only below `hash_join_inlist_pushdown_max_distinct_values`
    // (default 150 per partition); above it DataFusion uses a hash lookup `PruningPredicate` cannot read.
    let (mut prices, plan) = run_broadcast_join(vec![15, 0]).await;
    prices.sort_unstable();
    assert_eq!(prices, vec![0, 15], "join result");
    assert_eq!(
        (rg_pruned_at_prefetch(&plan), rg_pruned_at_poll(&plan)),
        (2, 0),
        "RG1 and RG2 hold neither key → pruned despite the envelope spanning all RGs"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_join_with_a_key_in_every_row_group_prunes_nothing() {
    // Negative control: one build key per row group, so no row group is
    // provably excluded. Guards the two tests above against passing for a
    // reason unrelated to the build side's values.
    let (mut prices, plan) = run_broadcast_join(vec![15, 11, 7, 3]).await;
    prices.sort_unstable();
    assert_eq!(prices, vec![3, 7, 11, 15], "join result");
    assert_eq!(
        rg_pruned_at_prefetch(&plan) + rg_pruned_at_poll(&plan),
        0,
        "every row group holds a build key → nothing is provably excluded"
    );
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
