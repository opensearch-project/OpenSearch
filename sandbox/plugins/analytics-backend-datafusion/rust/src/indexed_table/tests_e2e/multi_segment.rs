/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Multi-segment tests. A shard typically holds many index segments; each
//! becomes one parquet file and one `SegmentFileInfo`. This module exercises:
//!
//! - Two segments in one shard (different content, same schema).
//! - `writer_generation != 0` propagation through collectors.
//! - Partition assignment spanning multiple segments (`compute_assignments`
//!   emits multi-chunk partitions → `UnionExec + CoalescePartitionsExec`
//!   wrapper path in `QueryShardExec::execute`).
//! - Per-segment doc ID locality (each segment has its own `[0, max_doc)`).

use super::*;

// ── Two-segment fixture ───────────────────────────────────────────────
//
// Segment 0: 8 rows of `amazon` brand (prices 50..58).
// Segment 1: 8 rows of `apple` brand (prices 100..108).
//
// Totally disjoint content so per-segment correctness is easy to check.

const SEG0_ROWS: usize = 8;
const SEG1_ROWS: usize = 8;

fn write_segment(brand: &'static str, base_price: i32, rows: usize) -> NamedTempFile {
    write_segment_rg(brand, base_price, rows, /*max_rg_rows*/ 4)
}

/// Write a parquet segment with a configurable max-row-group-size so tests
/// can produce multi-RG-per-segment layouts.
fn write_segment_rg(
    brand: &'static str,
    base_price: i32,
    rows: usize,
    max_rg_rows: usize,
) -> NamedTempFile {
    let schema = Arc::new(Schema::new(vec![
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Int32, false),
    ]));
    let brands: Vec<&str> = (0..rows).map(|_| brand).collect();
    let prices: Vec<i32> = (0..rows).map(|i| base_price + i as i32).collect();
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
        .set_max_row_group_size(max_rg_rows)
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    tmp
}

/// A collector whose matching set is parameterised by writer_generation: it records
/// the generations it was built for so tests can confirm the evaluator factory called
/// us with the right per-segment identity.
#[derive(Debug)]
struct PerSegmentCollector {
    matching: Vec<i32>,
}

impl RowGroupDocsCollector for PerSegmentCollector {
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

/// Build the 2-segment table provider. Each segment gets its own matching
/// set indexed by `writer_generation` (caller supplies one Vec<i32> per segment).
async fn run_two_segment_query(
    per_segment_matches: Vec<Vec<i32>>,
    num_partitions: usize,
) -> Vec<(i32, String, i32)> {
    let tmp0 = write_segment("amazon", 50, SEG0_ROWS);
    let tmp1 = write_segment("apple", 100, SEG1_ROWS);

    let mut segments: Vec<SegmentFileInfo> = Vec::new();
    let mut schema_opt: Option<SchemaRef> = None;

    for (ord, tmp) in [&tmp0, &tmp1].iter().enumerate() {
        let path = tmp.path().to_path_buf();
        let size = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::File::open(&path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();
        if schema_opt.is_none() {
            schema_opt = Some(meta.schema().clone());
        }
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
        segments.push(SegmentFileInfo {
            writer_generation: ord as i64,
            // Per-segment max_doc. Both happen to be 8 here.
            max_doc: offset,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            global_base: 0,
            sort_min: None,
            sort_max: None,
        });
    }

    let schema = schema_opt.unwrap();
    let per_segment_matches = Arc::new(per_segment_matches);

    // Factory produces a single-collector evaluator per chunk. The collector's
    // matching set is pulled from `per_segment_matches[segment.writer_generation]`,
    // so wrong writer_generation propagation would immediately produce wrong rows.
    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_segment_matches = Arc::clone(&per_segment_matches);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics, _stats_prune_tree| {
            let matching = per_segment_matches
                .get(segment.writer_generation as usize)
                .cloned()
                .unwrap_or_default();
            let collector: Arc<dyn RowGroupDocsCollector> =
                Arc::new(PerSegmentCollector { matching });
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(
                crate::indexed_table::eval::single_collector::SingleCollectorEvaluator::new(
                    Some(collector), pruner, None, None, None, None,
                    crate::indexed_table::eval::single_collector::CollectorCallStrategy::FullRange,
                    std::sync::Arc::new(std::collections::HashMap::new()),
                    segment.writer_generation,
                    std::sync::Arc::new(crate::indexed_table::eval::single_collector::FfmDelegatedBackendCollectorFactory),
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
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(num_partitions)
        .force_strategy(Some(FilterStrategy::BooleanMask))
        .indexed_pushdown_filters(false)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
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
    let df = ctx.sql("SELECT brand, price FROM t").await.unwrap();
    let mut stream = df.execute_stream().await.unwrap();
    let mut rows: Vec<(i32, String, i32)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let brand = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let price = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            // Infer writer_generation by brand for verification purposes.
            let ord = if brand.value(i) == "amazon" { 0 } else { 1 };
            rows.push((ord, brand.value(i).to_string(), price.value(i)));
        }
    }
    rows.sort();
    rows
}

// ── Tests ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_segments_each_contributes_its_own_docs() {
    // Segment 0: match docs {0, 3, 7} → amazon rows at prices 50, 53, 57
    // Segment 1: match docs {1, 5} → apple rows at prices 101, 105
    let rows = run_two_segment_query(vec![vec![0, 3, 7], vec![1, 5]], /*num_partitions*/ 2).await;
    assert_eq!(
        rows,
        vec![
            (0, "amazon".to_string(), 50),
            (0, "amazon".to_string(), 53),
            (0, "amazon".to_string(), 57),
            (1, "apple".to_string(), 101),
            (1, "apple".to_string(), 105),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_segments_empty_collector_in_one_segment() {
    // Segment 0: no matches. Segment 1: docs {0, 4}.
    let rows = run_two_segment_query(vec![vec![], vec![0, 4]], 2).await;
    assert_eq!(
        rows,
        vec![(1, "apple".to_string(), 100), (1, "apple".to_string(), 104),]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_segments_single_partition_exercises_union_coalesce_path() {
    // With num_partitions=1, both segments' chunks land in the same
    // partition, triggering UnionExec + CoalescePartitionsExec in
    // QueryShardExec::execute. Results must be identical to the 2-partition
    // case — the wrapper path must not lose, duplicate, or reorder rows.
    let matches = vec![vec![2, 6], vec![3, 7]];
    let with_one = run_two_segment_query(matches.clone(), /*num_partitions*/ 1).await;
    let with_two = run_two_segment_query(matches, /*num_partitions*/ 2).await;
    assert_eq!(with_one, with_two);
    assert_eq!(with_one.len(), 4);
}

/// Collector that wraps `PerSegmentCollector` and tracks concurrent
/// invocations of `collect_packed_u64_bitset` across *all* chunks in a
/// single query. Used to witness whether `QueryShardExec::execute`
/// runs the per-chunk streams in parallel within one partition.
#[derive(Debug)]
struct ConcurrencyWitnessCollector {
    inner: PerSegmentCollector,
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
}

impl RowGroupDocsCollector for ConcurrencyWitnessCollector {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        use std::sync::atomic::Ordering;
        let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        // Update high-water-mark with a CAS loop.
        let mut prev = self.max_in_flight.load(Ordering::SeqCst);
        while cur > prev {
            match self.max_in_flight.compare_exchange_weak(
                prev,
                cur,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(p) => prev = p,
            }
        }
        // Hold the slot long enough that any genuine parallelism would
        // be observed as in_flight > 1.
        std::thread::sleep(std::time::Duration::from_millis(25));
        let result = self.inner.collect_packed_u64_bitset(min_doc, max_doc);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

/// Variant of `run_two_segment_query` that wraps each segment's collector
/// in a `ConcurrencyWitnessCollector` sharing one global high-water-mark
/// counter. Returns `(rows, max_in_flight_observed)`.
async fn run_two_segment_query_witness(
    per_segment_matches: Vec<Vec<i32>>,
    num_partitions: usize,
) -> (Vec<(i32, String, i32)>, usize) {
    let tmp0 = write_segment("amazon", 50, SEG0_ROWS);
    let tmp1 = write_segment("apple", 100, SEG1_ROWS);

    let mut segments: Vec<SegmentFileInfo> = Vec::new();
    let mut schema_opt: Option<SchemaRef> = None;

    for (ord, tmp) in [&tmp0, &tmp1].iter().enumerate() {
        let path = tmp.path().to_path_buf();
        let size = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::File::open(&path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();
        if schema_opt.is_none() {
            schema_opt = Some(meta.schema().clone());
        }
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
        segments.push(SegmentFileInfo {
            writer_generation: ord as i64,
            max_doc: offset,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            global_base: 0,
            sort_min: None,
            sort_max: None,
        });
    }

    let schema = schema_opt.unwrap();
    let per_segment_matches = Arc::new(per_segment_matches);
    let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let max_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_segment_matches = Arc::clone(&per_segment_matches);
        let schema = schema.clone();
        let in_flight = Arc::clone(&in_flight);
        let max_in_flight = Arc::clone(&max_in_flight);
        Arc::new(move |segment, _chunk, _stream_metrics, _stats_prune_tree| {
            let matching = per_segment_matches
                .get(segment.writer_generation as usize)
                .cloned()
                .unwrap_or_default();
            let collector: Arc<dyn RowGroupDocsCollector> = Arc::new(ConcurrencyWitnessCollector {
                inner: PerSegmentCollector { matching },
                in_flight: Arc::clone(&in_flight),
                max_in_flight: Arc::clone(&max_in_flight),
            });
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(
                crate::indexed_table::eval::single_collector::SingleCollectorEvaluator::new(
                    Some(collector), pruner, None, None, None, None,
                    crate::indexed_table::eval::single_collector::CollectorCallStrategy::FullRange,
                    std::sync::Arc::new(std::collections::HashMap::new()),
                    segment.writer_generation,
                    std::sync::Arc::new(crate::indexed_table::eval::single_collector::FfmDelegatedBackendCollectorFactory),
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
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(num_partitions)
        .force_strategy(Some(FilterStrategy::BooleanMask))
        .indexed_pushdown_filters(false)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
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
    let df = ctx.sql("SELECT brand, price FROM t").await.unwrap();
    let mut stream = df.execute_stream().await.unwrap();
    let mut rows: Vec<(i32, String, i32)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let brand = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let price = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            let ord = if brand.value(i) == "amazon" { 0 } else { 1 };
            rows.push((ord, brand.value(i).to_string(), price.value(i)));
        }
    }
    rows.sort();
    let max_observed = max_in_flight.load(std::sync::atomic::Ordering::SeqCst);
    (rows, max_observed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_partition_runs_chunks_sequentially_no_inner_parallelism() {
    // With num_partitions=1 both segments land in one partition's
    // assignment as separate `SegmentChunk`s. Pre-fix: `QueryShardExec::execute`
    // wrapped them in `UnionExec` + `CoalescePartitionsExec`, which spawns
    // one task per child stream and drives them concurrently — the witness
    // collector would observe `max_in_flight >= 2`. Post-fix: chunks are
    // chained sequentially via `futures::stream::iter(streams).flatten()`,
    // so only the currently-active chunk's `prefetch_rg` (and therefore the
    // collector) is ever in flight → `max_in_flight == 1`.
    let matches = vec![vec![2, 6], vec![3, 7]];
    let (rows, max_in_flight) = run_two_segment_query_witness(matches, /*num_partitions*/ 1).await;
    assert_eq!(rows.len(), 4, "result correctness sanity check");
    assert_eq!(
        max_in_flight, 1,
        "chunks within a single partition must run sequentially \
         (max concurrent collector invocations should be 1, was {})",
        max_in_flight,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_partition_parallelism_is_preserved() {
    // Sanity-check the inverse: the cross-partition parallelism that
    // `QueryShardExec` exposes via `Partitioning::UnknownPartitioning(N)`
    // is unchanged. With num_partitions=2, the two segments' chunks land
    // in *different* partitions and DataFusion drives them concurrently,
    // so the witness collector should observe `max_in_flight == 2`.
    let matches = vec![vec![2, 6], vec![3, 7]];
    let (rows, max_in_flight) = run_two_segment_query_witness(matches, /*num_partitions*/ 2).await;
    assert_eq!(rows.len(), 4, "result correctness sanity check");
    assert_eq!(
        max_in_flight, 2,
        "cross-partition parallelism should still let both segments run \
         concurrently (expected max_in_flight=2, was {})",
        max_in_flight,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_segments_doc_ids_are_segment_local() {
    // Critical correctness invariant: each segment has doc IDs in [0, max_doc),
    // NOT some shard-wide concatenation. If a bug made seg0's collector see
    // doc_max=16 instead of 8, it would try to match docs 8..16 which don't
    // exist in its file and we'd get wrong/missing rows.
    //
    // Here: give each segment the same set of doc IDs {0, 4}. If doc-ID
    // locality is correct, seg0 returns amazon rows at prices 50, 54 AND
    // seg1 returns apple rows at prices 100, 104. If the implementation
    // mixed up doc-ID spaces, results would differ.
    let rows = run_two_segment_query(vec![vec![0, 4], vec![0, 4]], 2).await;
    assert_eq!(
        rows,
        vec![
            (0, "amazon".to_string(), 50),
            (0, "amazon".to_string(), 54),
            (1, "apple".to_string(), 100),
            (1, "apple".to_string(), 104),
        ]
    );
}

// ══════════════════════════════════════════════════════════════════════
// Many-segment × many-RG × varied-partition tests
// ══════════════════════════════════════════════════════════════════════
//
// These extend the 2-segment fixture to 4-6 segments, with small RG sizes
// so each segment has multiple RGs, and exercise `num_partitions` from 1
// to 5. Goal: stress the interaction between partitioning, chunk packing,
// and per-segment lifetime.

/// Descriptor of a segment fixture for `run_segments`.
struct SegSpec {
    brand: &'static str,
    base_price: i32,
    rows: usize,
    /// Caps row group size; smaller → more RGs per segment.
    max_rg_rows: usize,
    /// Matching doc IDs in segment-local space [0, rows).
    matches: Vec<i32>,
}

/// Generic multi-segment runner. Builds one parquet per spec, wires them up
/// as SegmentFileInfo's, runs a SELECT over the whole table. Returns
/// `(writer_generation_inferred_from_brand, brand, price)` triples sorted so tests
/// can assert on set equality regardless of partition/chunk ordering.
async fn run_segments(specs: Vec<SegSpec>, num_partitions: usize) -> Vec<(i32, String, i32)> {
    let tmps: Vec<NamedTempFile> = specs
        .iter()
        .map(|s| write_segment_rg(s.brand, s.base_price, s.rows, s.max_rg_rows))
        .collect();
    let brand_to_ord: std::collections::HashMap<&'static str, i32> = specs
        .iter()
        .enumerate()
        .map(|(i, s)| (s.brand, i as i32))
        .collect();
    let per_segment_matches: Vec<Vec<i32>> = specs.iter().map(|s| s.matches.clone()).collect();

    let mut segments: Vec<SegmentFileInfo> = Vec::new();
    let mut schema_opt: Option<SchemaRef> = None;
    for (ord, tmp) in tmps.iter().enumerate() {
        let path = tmp.path().to_path_buf();
        let size = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::File::open(&path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();
        if schema_opt.is_none() {
            schema_opt = Some(meta.schema().clone());
        }
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
        segments.push(SegmentFileInfo {
            writer_generation: ord as i64,
            max_doc: offset,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            global_base: 0,
            sort_min: None,
            sort_max: None,
        });
    }

    let schema = schema_opt.unwrap();
    let per_segment_matches = Arc::new(per_segment_matches);
    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_segment_matches = Arc::clone(&per_segment_matches);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics, _stats_prune_tree| {
            let matching = per_segment_matches
                .get(segment.writer_generation as usize)
                .cloned()
                .unwrap_or_default();
            let collector: Arc<dyn RowGroupDocsCollector> =
                Arc::new(PerSegmentCollector { matching });
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(
                crate::indexed_table::eval::single_collector::SingleCollectorEvaluator::new(
                    Some(collector), pruner, None, None, None, None,
                    crate::indexed_table::eval::single_collector::CollectorCallStrategy::FullRange,
                    std::sync::Arc::new(std::collections::HashMap::new()),
                    segment.writer_generation,
                    std::sync::Arc::new(crate::indexed_table::eval::single_collector::FfmDelegatedBackendCollectorFactory),
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
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(num_partitions)
        .force_strategy(Some(FilterStrategy::BooleanMask))
        .indexed_pushdown_filters(false)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
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
    let df = ctx.sql("SELECT brand, price FROM t").await.unwrap();
    let mut stream = df.execute_stream().await.unwrap();
    let mut rows: Vec<(i32, String, i32)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let brand = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let price = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            let ord = *brand_to_ord.get(brand.value(i)).expect("known brand");
            rows.push((ord, brand.value(i).to_string(), price.value(i)));
        }
    }
    rows.sort();
    rows
}

/// Five-segment fixture used across the partition-sweep tests. Each segment
/// has ~16 rows and max_rg_rows=4 → 4 RGs per segment. Total across shard:
/// 5 segments × 4 RGs × 4 rows = 80 rows, 20 RGs.
///
/// Brands are distinct per segment so results stay attributable. Matches
/// pick a few docs in each segment covering multiple RGs (not just RG 0),
/// so partition assignment that packs RGs across segment boundaries gets
/// exercised.
fn five_segment_specs() -> Vec<SegSpec> {
    vec![
        SegSpec {
            brand: "amazon",
            base_price: 0,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![0, 5, 10, 15],
        },
        SegSpec {
            brand: "apple",
            base_price: 100,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![1, 6, 11],
        },
        SegSpec {
            brand: "google",
            base_price: 200,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![2, 7, 12],
        },
        SegSpec {
            brand: "meta",
            base_price: 300,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![3, 8, 13],
        },
        SegSpec {
            brand: "netflix",
            base_price: 400,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![4, 9, 14],
        },
    ]
}

fn expected_for_five_segments() -> Vec<(i32, String, i32)> {
    // Build expected rows: for each segment, the rows at its match offsets.
    let specs = five_segment_specs();
    let mut out = Vec::new();
    for (ord, s) in specs.iter().enumerate() {
        for &m in &s.matches {
            out.push((ord as i32, s.brand.to_string(), s.base_price + m));
        }
    }
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_segments_each_with_four_rgs_single_partition() {
    // num_partitions=1 → all 20 RGs land in one partition, producing a
    // 5-chunk UnionExec+CoalescePartitionsExec. Every segment boundary
    // inside the chunk list is a flush-point for partitioning.rs.
    let rows = run_segments(five_segment_specs(), 1).await;
    assert_eq!(rows, expected_for_five_segments());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_segments_each_with_four_rgs_three_partitions() {
    let rows = run_segments(five_segment_specs(), 3).await;
    assert_eq!(rows, expected_for_five_segments());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_segments_each_with_four_rgs_five_partitions() {
    // num_partitions=5, total_rows=80 → rows_per_partition=16 (ceil).
    // Typical assignment: one segment per partition (no cross-segment
    // chunks). Result identity must still hold.
    let rows = run_segments(five_segment_specs(), 5).await;
    assert_eq!(rows, expected_for_five_segments());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_count_does_not_affect_result_set() {
    // The same query under different partition counts must produce identical
    // row sets (modulo ordering, which we normalize by sort).
    let specs = five_segment_specs();
    let expected = expected_for_five_segments();
    for np in [1usize, 2, 3, 4, 5] {
        let rows = run_segments(
            // Clone specs for each run since run_segments consumes the Vec.
            specs
                .iter()
                .map(|s| SegSpec {
                    brand: s.brand,
                    base_price: s.base_price,
                    rows: s.rows,
                    max_rg_rows: s.max_rg_rows,
                    matches: s.matches.clone(),
                })
                .collect(),
            np,
        )
        .await;
        assert_eq!(rows, expected, "partition count {} produced wrong rows", np);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_segments_mixed_rg_sizes() {
    // Deliberately uneven: seg0 has 2 RGs, seg1 has 4 RGs, seg2 has 1 RG,
    // seg3 has 8 RGs. Total ~60 rows across 15 RGs. Uneven fan-out
    // stresses compute_assignments.
    let specs = vec![
        SegSpec {
            brand: "amazon",
            base_price: 0,
            rows: 8,
            max_rg_rows: 4,
            matches: vec![0, 7],
        }, // 2 RGs
        SegSpec {
            brand: "apple",
            base_price: 100,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![1, 9, 15],
        }, // 4 RGs
        SegSpec {
            brand: "google",
            base_price: 200,
            rows: 4,
            max_rg_rows: 8,
            matches: vec![2],
        }, // 1 RG
        SegSpec {
            brand: "meta",
            base_price: 300,
            rows: 32,
            max_rg_rows: 4,
            matches: vec![0, 12, 24, 31],
        }, // 8 RGs
    ];
    let expected: Vec<(i32, String, i32)> = {
        let mut out = Vec::new();
        for (ord, s) in specs.iter().enumerate() {
            for &m in &s.matches {
                out.push((ord as i32, s.brand.to_string(), s.base_price + m));
            }
        }
        out.sort();
        out
    };
    // Sweep partition counts; result should be stable.
    for np in [1usize, 2, 4] {
        let rows = run_segments(
            specs
                .iter()
                .map(|s| SegSpec {
                    brand: s.brand,
                    base_price: s.base_price,
                    rows: s.rows,
                    max_rg_rows: s.max_rg_rows,
                    matches: s.matches.clone(),
                })
                .collect(),
            np,
        )
        .await;
        assert_eq!(rows, expected, "np={} failed", np);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn six_segments_some_with_no_matches() {
    // Realistic shard: 6 segments, but only half contribute matches. Ensures
    // the factory handles zero-match segments cleanly (prefetch_rg returns
    // None → RG skip) without hanging or producing phantom rows.
    let specs = vec![
        SegSpec {
            brand: "amazon",
            base_price: 0,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![3, 11],
        },
        SegSpec {
            brand: "apple",
            base_price: 100,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![],
        },
        SegSpec {
            brand: "google",
            base_price: 200,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![0, 8],
        },
        SegSpec {
            brand: "meta",
            base_price: 300,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![],
        },
        SegSpec {
            brand: "netflix",
            base_price: 400,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![5, 10, 15],
        },
        SegSpec {
            brand: "oracle",
            base_price: 500,
            rows: 16,
            max_rg_rows: 4,
            matches: vec![],
        },
    ];
    let expected = vec![
        (0, "amazon".to_string(), 3),
        (0, "amazon".to_string(), 11),
        (2, "google".to_string(), 200),
        (2, "google".to_string(), 208),
        (4, "netflix".to_string(), 405),
        (4, "netflix".to_string(), 410),
        (4, "netflix".to_string(), 415),
    ];
    let mut expected = expected;
    expected.sort();
    let rows = run_segments(specs, 3).await;
    assert_eq!(rows, expected);
}

// ══════════════════════════════════════════════════════════════════════
// Wide-schema multi-segment tests
// ══════════════════════════════════════════════════════════════════════
//
// The earlier tests use a 2-column schema (brand, price). Production shards
// routinely have 5-20 columns with filters spanning several of them; this
// section widens to 5 cols and exercises trees that mix a collector leaf
// (on `brand`) with 1-3 predicate leaves on numeric/string/boolean columns.
//
// Schema: (brand Utf8, price Int32, qty Int32, region Utf8, active Boolean).
// Collector semantics: `brand == <segment's brand>` for every doc in segment.
// Predicate leaves: generated by the test, evaluated via Path C tree walker.

use datafusion::arrow::array::BooleanArray;

fn wide_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Int32, false),
        Field::new("qty", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn pred_wide_int(col: &str, op: Operator, v: i32) -> BoolNode {
    let schema = wide_schema();
    let col_idx = schema.index_of(col).expect("wide column");
    let left: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::Column::new(col, col_idx),
    );
    let right: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::Literal::new(ScalarValue::Int32(Some(v))),
    );
    BoolNode::Predicate(Arc::new(
        datafusion::physical_expr::expressions::BinaryExpr::new(left, op, right),
    ))
}

fn pred_wide_str(col: &str, op: Operator, v: &str) -> BoolNode {
    let schema = wide_schema();
    let col_idx = schema.index_of(col).expect("wide column");
    let left: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::Column::new(col, col_idx),
    );
    let right: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
        Arc::new(datafusion::physical_expr::expressions::Literal::new(
            ScalarValue::Utf8(Some(v.to_string())),
        ));
    BoolNode::Predicate(Arc::new(
        datafusion::physical_expr::expressions::BinaryExpr::new(left, op, right),
    ))
}

fn pred_wide_bool(col: &str, op: Operator, v: bool) -> BoolNode {
    let schema = wide_schema();
    let col_idx = schema.index_of(col).expect("wide column");
    let left: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::Column::new(col, col_idx),
    );
    let right: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::Literal::new(ScalarValue::Boolean(Some(v))),
    );
    BoolNode::Predicate(Arc::new(
        datafusion::physical_expr::expressions::BinaryExpr::new(left, op, right),
    ))
}

/// Write a 5-column segment. Deterministic per-row values driven by row index
/// so tests can compute the oracle truth without a second parquet read.
fn write_wide_segment(brand: &'static str, rows: usize, max_rg_rows: usize) -> NamedTempFile {
    let schema = wide_schema();
    let brands: Vec<&str> = (0..rows).map(|_| brand).collect();
    let prices: Vec<i32> = (0..rows).map(|i| (i as i32) * 10).collect(); // 0, 10, 20, ...
    let qtys: Vec<i32> = (0..rows).map(|i| (i as i32) % 7).collect(); // 0..6 cycling
    let regions: Vec<&str> = (0..rows)
        .map(|i| match i % 3 {
            0 => "us-east",
            1 => "us-west",
            _ => "eu-west",
        })
        .collect();
    let actives: Vec<bool> = (0..rows).map(|i| i % 2 == 0).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(brands)),
            Arc::new(Int32Array::from(prices)),
            Arc::new(Int32Array::from(qtys)),
            Arc::new(StringArray::from(regions)),
            Arc::new(BooleanArray::from(actives)),
        ],
    )
    .unwrap();
    let tmp = NamedTempFile::new().unwrap();
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(max_rg_rows)
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    tmp
}

/// Wide-schema segment spec. Collector leaf matches every doc whose brand
/// equals the segment's brand (i.e. all docs in that segment). Tests layer
/// predicate leaves on top to narrow.
struct WideSegSpec {
    brand: &'static str,
    rows: usize,
    max_rg_rows: usize,
}

/// Generic wide-schema runner. Builds one parquet per spec, wraps each in
/// a Path C `TreeBitsetSource` with the caller-supplied tree + predicates,
/// and returns all passing rows as tagged tuples.
async fn run_wide_segments(
    specs: Vec<WideSegSpec>,
    tree: BoolNode,
    num_partitions: usize,
) -> Vec<(i32, i32, i32, String, bool)> {
    // Collector: every doc in the segment matches (one-leaf trees then use
    // the collector alone; multi-leaf trees refine via predicates).
    #[derive(Debug)]
    struct AllDocs;
    impl RowGroupDocsCollector for AllDocs {
        fn collect_packed_u64_bitset(
            &self,
            min_doc: i32,
            max_doc: i32,
        ) -> Result<Vec<u64>, String> {
            let span = (max_doc - min_doc) as usize;
            let mut out = vec![0u64; span.div_ceil(64)];
            for i in 0..span {
                out[i / 64] |= 1u64 << (i % 64);
            }
            Ok(out)
        }
    }

    let tmps: Vec<NamedTempFile> = specs
        .iter()
        .map(|s| write_wide_segment(s.brand, s.rows, s.max_rg_rows))
        .collect();

    // Wire SegmentFileInfos.
    let mut segments: Vec<SegmentFileInfo> = Vec::new();
    let mut schema_opt: Option<SchemaRef> = None;
    for (ord, tmp) in tmps.iter().enumerate() {
        let path = tmp.path().to_path_buf();
        let size = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::File::open(&path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();
        if schema_opt.is_none() {
            schema_opt = Some(meta.schema().clone());
        }
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
        segments.push(SegmentFileInfo {
            writer_generation: ord as i64,
            max_doc: offset,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            global_base: 0,
            sort_min: None,
            sort_max: None,
        });
    }

    let schema = schema_opt.unwrap();
    let tree = Arc::new(tree.push_not_down().flatten());

    // Per-chunk evaluator: builds a Path C TreeBitsetSource with a fresh
    // all-docs collector for this segment.
    let factory: super::super::table_provider::EvaluatorFactory = {
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics, _stats_prune_tree| {
            // One (provider_key, collector) per Collector leaf — our trees
            // here use 1 collector leaf, so one pair.
            let leaf_count = tree.collector_leaf_count();
            let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = (0..leaf_count)
                .map(|i| {
                    let c: Arc<dyn RowGroupDocsCollector> = Arc::new(AllDocs);
                    (i as i32, c)
                })
                .collect();
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(
                crate::indexed_table::eval::TreeBitsetSource {
                    tree: Arc::new(resolved),
                    evaluator: Arc::new(
                        crate::indexed_table::eval::bitmap_tree::BitmapTreeEvaluator,
                    ),
                    leaves: Arc::new(
                        crate::indexed_table::eval::bitmap_tree::CollectorLeafBitmaps::without_metrics(),
                    ),
                    page_pruner: pruner,
                    cost_predicate: 1,
                    cost_collector: 10,
                    max_collector_parallelism: 1,
                    pruning_predicates: std::sync::Arc::new(std::collections::HashMap::new()),
                page_prune_metrics: None,
                    collector_strategy: crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
                stats_prune_tree: None, rg_index_to_pos: HashMap::new(),
                },
            );
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(num_partitions)
        .force_strategy(Some(FilterStrategy::BooleanMask))
        .indexed_pushdown_filters(false)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
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
    let df = ctx
        .sql("SELECT brand, price, qty, region, active FROM t")
        .await
        .unwrap();
    let mut stream = df.execute_stream().await.unwrap();
    let brand_to_ord: std::collections::HashMap<&'static str, i32> = specs
        .iter()
        .enumerate()
        .map(|(i, s)| (s.brand, i as i32))
        .collect();
    let mut rows: Vec<(i32, i32, i32, String, bool)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let brand = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let price = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let qty = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        let region = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let active = b.column(4).as_any().downcast_ref::<BooleanArray>().unwrap();
        for i in 0..b.num_rows() {
            let ord = *brand_to_ord.get(brand.value(i)).expect("known brand");
            rows.push((
                ord,
                price.value(i),
                qty.value(i),
                region.value(i).to_string(),
                active.value(i),
            ));
        }
    }
    rows.sort();
    rows
}

/// Oracle: compute expected rows by running the same boolean predicate
/// directly over the deterministic per-row values. Mirrors the shape of
/// `write_wide_segment` exactly.
fn wide_oracle<F: Fn(usize) -> bool>(
    specs: &[WideSegSpec],
    pred: F,
) -> Vec<(i32, i32, i32, String, bool)> {
    let mut out = Vec::new();
    for (ord, s) in specs.iter().enumerate() {
        for i in 0..s.rows {
            if pred(i) {
                let price = (i as i32) * 10;
                let qty = (i as i32) % 7;
                let region = match i % 3 {
                    0 => "us-east",
                    1 => "us-west",
                    _ => "eu-west",
                };
                let active = i % 2 == 0;
                out.push((ord as i32, price, qty, region.to_string(), active));
            }
        }
    }
    out.sort();
    out
}

fn wide_four_seg_specs() -> Vec<WideSegSpec> {
    vec![
        WideSegSpec {
            brand: "amazon",
            rows: 16,
            max_rg_rows: 4,
        },
        WideSegSpec {
            brand: "apple",
            rows: 16,
            max_rg_rows: 4,
        },
        WideSegSpec {
            brand: "google",
            rows: 16,
            max_rg_rows: 4,
        },
        WideSegSpec {
            brand: "meta",
            rows: 16,
            max_rg_rows: 4,
        },
    ]
}

fn clone_wide_specs(specs: &[WideSegSpec]) -> Vec<WideSegSpec> {
    specs
        .iter()
        .map(|s| WideSegSpec {
            brand: s.brand,
            rows: s.rows,
            max_rg_rows: s.max_rg_rows,
        })
        .collect()
}

// ── Wide-schema tests ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_multi_segment_collector_and_two_predicates() {
    // Tree: AND(all_brand_docs, price > 50, qty < 3)
    let specs = wide_four_seg_specs();
    let tree = BoolNode::And(vec![
        BoolNode::Collector { annotation_id: 0 },
        pred_wide_int("price", Operator::Gt, 50),
        pred_wide_int("qty", Operator::Lt, 3),
    ]);
    let expected = wide_oracle(&specs, |i| (i as i32) * 10 > 50 && (i as i32) % 7 < 3);
    for np in [1usize, 3, 5] {
        let rows = run_wide_segments(clone_wide_specs(&specs), tree.clone(), np).await;
        assert_eq!(rows, expected, "np={} failed", np);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_multi_segment_or_of_predicates_under_collector() {
    // Tree: AND(all_brand_docs, OR(region == "us-east", active == true))
    let specs = wide_four_seg_specs();
    let tree = BoolNode::And(vec![
        BoolNode::Collector { annotation_id: 0 },
        BoolNode::Or(vec![
            pred_wide_str("region", Operator::Eq, "us-east"),
            pred_wide_bool("active", Operator::Eq, true),
        ]),
    ]);
    let expected = wide_oracle(&specs, |i| i % 3 == 0 || i % 2 == 0);
    for np in [2usize, 4] {
        let rows = run_wide_segments(clone_wide_specs(&specs), tree.clone(), np).await;
        assert_eq!(rows, expected, "np={} failed", np);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_multi_segment_not_and_three_column_predicates() {
    // Tree: AND(all_brand_docs, NOT(price < 30), qty > 2, region != "eu-west")
    let specs = wide_four_seg_specs();
    let tree = BoolNode::And(vec![
        BoolNode::Collector { annotation_id: 0 },
        BoolNode::Not(Box::new(pred_wide_int("price", Operator::Lt, 30))),
        pred_wide_int("qty", Operator::Gt, 2),
        pred_wide_str("region", Operator::NotEq, "eu-west"),
    ]);
    let expected = wide_oracle(&specs, |i| {
        let i32_i = i as i32;
        !(i32_i * 10 < 30) && i32_i % 7 > 2 && i % 3 != 2
    });
    for np in [1usize, 3, 5] {
        let rows = run_wide_segments(clone_wide_specs(&specs), tree.clone(), np).await;
        assert_eq!(rows, expected, "np={} failed", np);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_multi_segment_deep_tree_four_predicate_columns() {
    // Tree: AND(collector, OR(AND(price >= 100, qty = 3), AND(region == "us-west", active == true)))
    let specs = wide_four_seg_specs();
    let tree = BoolNode::And(vec![
        BoolNode::Collector { annotation_id: 0 },
        BoolNode::Or(vec![
            BoolNode::And(vec![
                pred_wide_int("price", Operator::GtEq, 100),
                pred_wide_int("qty", Operator::Eq, 3),
            ]),
            BoolNode::And(vec![
                pred_wide_str("region", Operator::Eq, "us-west"),
                pred_wide_bool("active", Operator::Eq, true),
            ]),
        ]),
    ]);
    let expected = wide_oracle(&specs, |i| {
        let i32_i = i as i32;
        let region = match i % 3 {
            0 => "us-east",
            1 => "us-west",
            _ => "eu-west",
        };
        let active = i % 2 == 0;
        (i32_i * 10 >= 100 && i32_i % 7 == 3) || (region == "us-west" && active)
    });
    for np in [1usize, 2, 5] {
        let rows = run_wide_segments(clone_wide_specs(&specs), tree.clone(), np).await;
        assert_eq!(rows, expected, "np={} failed", np);
    }
}

// ── StatsPruneTree integration with offset RG indices ───────────────
//
// These tests exercise the full pipeline with `prune_tree_config: Some(...)`
// enabled, causing `table_provider` to build `StatsPruneTree` per chunk.
// Verifies correctness when chunks have offset RG indices and when AND
// subtrees with native predicates get stats-pruned while OR subtrees
// containing collectors still produce empty bitmaps.
//
// Tree shape: AND(Predicate(price > X), OR(Collector, Predicate(qty/active)))
// Mirrors the failing PPL pattern.

fn collect_pred_exprs(
    tree: &BoolNode,
    out: &mut Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
) {
    match tree {
        BoolNode::And(c) | BoolNode::Or(c) => c.iter().for_each(|ch| collect_pred_exprs(ch, out)),
        BoolNode::Not(inner) => collect_pred_exprs(inner, out),
        BoolNode::Collector { .. } => {}
        BoolNode::Predicate(expr) => out.push(Arc::clone(expr)),
        BoolNode::DelegationPossible { original_expr, .. } => out.push(Arc::clone(original_expr)),
    }
}

async fn run_wide_segments_with_stats_pruning(
    specs: Vec<WideSegSpec>,
    tree: BoolNode,
    num_partitions: usize,
) -> Vec<(i32, i32, i32, String, bool)> {
    #[derive(Debug)]
    struct AllDocs;
    impl RowGroupDocsCollector for AllDocs {
        fn collect_packed_u64_bitset(
            &self,
            min_doc: i32,
            max_doc: i32,
        ) -> Result<Vec<u64>, String> {
            let span = (max_doc - min_doc) as usize;
            let mut out = vec![0u64; span.div_ceil(64)];
            for i in 0..span {
                out[i / 64] |= 1u64 << (i % 64);
            }
            Ok(out)
        }
    }

    let tmps: Vec<NamedTempFile> = specs
        .iter()
        .map(|s| write_wide_segment(s.brand, s.rows, s.max_rg_rows))
        .collect();

    let mut segments: Vec<SegmentFileInfo> = Vec::new();
    let mut schema_opt: Option<SchemaRef> = None;
    for (ord, tmp) in tmps.iter().enumerate() {
        let path = tmp.path().to_path_buf();
        let size = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::File::open(&path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();
        if schema_opt.is_none() {
            schema_opt = Some(meta.schema().clone());
        }
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
        segments.push(SegmentFileInfo {
            writer_generation: ord as i64,
            max_doc: offset,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            global_base: 0,
            sort_min: None,
            sort_max: None,
        });
    }

    let schema = schema_opt.unwrap();
    let tree = tree.push_not_down().flatten();

    let mut leaf_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = Vec::new();
    collect_pred_exprs(&tree, &mut leaf_exprs);
    let pruning_predicates: Arc<
        HashMap<usize, Arc<datafusion::physical_optimizer::pruning::PruningPredicate>>,
    > = Arc::new(
        leaf_exprs
            .iter()
            .filter_map(|expr| {
                crate::indexed_table::page_pruner::build_pruning_predicate(expr, schema.clone())
                    .map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
            })
            .collect(),
    );

    let tree = Arc::new(tree);

    let factory: super::super::table_provider::EvaluatorFactory = {
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        let pruning_predicates = Arc::clone(&pruning_predicates);
        Arc::new(move |segment, chunk, stream_metrics, stats_prune_tree| {
            let leaf_count = tree.collector_leaf_count();
            let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = (0..leaf_count)
                .map(|i| {
                    (
                        i as i32,
                        Arc::new(AllDocs) as Arc<dyn RowGroupDocsCollector>,
                    )
                })
                .collect();
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let rg_index_to_pos: HashMap<usize, usize> = chunk
                .row_group_indices
                .iter()
                .enumerate()
                .map(|(pos, &idx)| (idx, pos))
                .collect();
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(
                crate::indexed_table::eval::TreeBitsetSource {
                    tree: Arc::new(resolved),
                    evaluator: Arc::new(crate::indexed_table::eval::bitmap_tree::BitmapTreeEvaluator),
                    leaves: Arc::new(crate::indexed_table::eval::bitmap_tree::CollectorLeafBitmaps::without_metrics()),
                    page_pruner: pruner,
                    cost_predicate: 1,
                    cost_collector: 10,
                    max_collector_parallelism: 1,
                    pruning_predicates: Arc::clone(&pruning_predicates),
                    page_prune_metrics: Some(
                        crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(stream_metrics),
                    ),
                    collector_strategy: crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
                    stats_prune_tree: stats_prune_tree.cloned(),
                    rg_index_to_pos,
                },
            );
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(num_partitions)
        .force_strategy(Some(FilterStrategy::BooleanMask))
        .indexed_pushdown_filters(false)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
        store,
        store_url,
        evaluator_factory: factory,
        pushdown_predicate: None,
        query_config: std::sync::Arc::new(qc),
        predicate_columns: vec![],
        emit_row_ids: false,
        prune_tree_config: Some((
            Arc::clone(&tree),
            Arc::clone(&pruning_predicates),
            schema.clone(),
        )),
        sort_fields: vec![],
        sort_orders: vec![],
        cancellation_token: None,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx
        .sql("SELECT brand, price, qty, region, active FROM t")
        .await
        .unwrap();
    let mut stream = df.execute_stream().await.unwrap();
    let mut rows: Vec<(i32, i32, i32, String, bool)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let brand = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let price = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let qty = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        let region = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let active = b
            .column(4)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            let ord = specs
                .iter()
                .position(|s| s.brand == brand.value(i))
                .unwrap_or(0) as i32;
            rows.push((
                ord,
                price.value(i),
                qty.value(i),
                region.value(i).to_string(),
                active.value(i),
            ));
        }
    }
    rows.sort();
    rows
}

/// Single segment (4 RGs of 4 rows), single partition.
/// Tree: AND(price > 50, OR(Collector, qty < 3))
/// RGs with prices [0..30] are stats-pruned by `price > 50`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_prune_single_segment_single_partition() {
    let specs = vec![WideSegSpec {
        brand: "amazon",
        rows: 16,
        max_rg_rows: 4,
    }];
    let tree = BoolNode::And(vec![
        pred_wide_int("price", Operator::Gt, 50),
        BoolNode::Or(vec![
            BoolNode::Collector { annotation_id: 0 },
            pred_wide_int("qty", Operator::Lt, 3),
        ]),
    ]);
    // Collector matches all docs; OR(all, qty<3) = all; so just price > 50.
    let expected = wide_oracle(&specs, |i| (i as i32) * 10 > 50);
    let rows = run_wide_segments_with_stats_pruning(specs, tree, 1).await;
    assert_eq!(rows, expected);
}

/// Single segment (4 RGs), multi partition (4 partitions → 1 RG per chunk).
/// Each chunk has rg_indices=[N] where N>0 for non-first chunks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_prune_single_segment_multi_partition() {
    let specs = vec![WideSegSpec {
        brand: "amazon",
        rows: 16,
        max_rg_rows: 4,
    }];
    let tree = BoolNode::And(vec![
        pred_wide_int("price", Operator::Gt, 50),
        BoolNode::Or(vec![
            BoolNode::Collector { annotation_id: 0 },
            pred_wide_int("qty", Operator::Lt, 3),
        ]),
    ]);
    let expected = wide_oracle(&specs, |i| (i as i32) * 10 > 50);
    let rows = run_wide_segments_with_stats_pruning(specs, tree, 4).await;
    assert_eq!(rows, expected);
}

/// Multi segment (4 segments × 4 RGs), single partition.
/// All segments in one partition — verifies stats pruning across segments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_prune_multi_segment_single_partition() {
    let specs = wide_four_seg_specs();
    let tree = BoolNode::And(vec![
        pred_wide_int("price", Operator::Gt, 80),
        BoolNode::Or(vec![
            BoolNode::Collector { annotation_id: 0 },
            pred_wide_bool("active", Operator::Eq, true),
        ]),
    ]);
    let expected = wide_oracle(&specs, |i| (i as i32) * 10 > 80);
    let rows = run_wide_segments_with_stats_pruning(clone_wide_specs(&specs), tree, 1).await;
    assert_eq!(rows, expected);
}

/// Multi segment (4 segments × 4 RGs), multi partition (2, 3, 5, 8).
/// Maximum fragmentation — chunks split across segments with offset RGs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_prune_multi_segment_multi_partition() {
    let specs = wide_four_seg_specs();
    let tree = BoolNode::And(vec![
        pred_wide_int("price", Operator::Gt, 80),
        BoolNode::Or(vec![
            BoolNode::Collector { annotation_id: 0 },
            pred_wide_bool("active", Operator::Eq, true),
        ]),
    ]);
    let expected = wide_oracle(&specs, |i| (i as i32) * 10 > 80);
    for np in [2usize, 3, 5, 8] {
        let rows =
            run_wide_segments_with_stats_pruning(clone_wide_specs(&specs), tree.clone(), np).await;
        assert_eq!(rows, expected, "np={} failed", np);
    }
}

/// Deep tree with NOT under multi-partition multi-segment.
/// AND(NOT(price < 40), OR(Collector, qty >= 5))
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_prune_multi_segment_multi_partition_with_not() {
    let specs = wide_four_seg_specs();
    let tree = BoolNode::And(vec![
        BoolNode::Not(Box::new(pred_wide_int("price", Operator::Lt, 40))),
        BoolNode::Or(vec![
            BoolNode::Collector { annotation_id: 0 },
            pred_wide_int("qty", Operator::GtEq, 5),
        ]),
    ]);
    // NOT is conservative in stats (always true), so actual predicate filters.
    let expected = wide_oracle(&specs, |i| !((i as i32) * 10 < 40));
    for np in [1usize, 3, 5] {
        let rows =
            run_wide_segments_with_stats_pruning(clone_wide_specs(&specs), tree.clone(), np).await;
        assert_eq!(rows, expected, "np={} failed", np);
    }
}

/// Directly exercises `prefetch_rg` to assert:
/// 1. RGs with stats that prove no-match return `None` (pruned)
/// 2. RGs at offset indices (index > 0) are correctly handled
/// 3. Empty collector bitsets are produced for pruned subtrees
///
/// Uses a single segment with 4 RGs (prices [0..30],[30..70],[70..110],[110..150])
/// and tree AND(price > 60, OR(Collector, qty < 2)).
/// RG0 and RG1 should be pruned (max price < 60). RG2 and RG3 survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_prune_direct_prefetch_asserts_pruning_and_empty_bitsets() {
    use crate::indexed_table::page_pruner::StatsPruneTree;

    // Build a segment with 4 RGs of 4 rows each.
    // prices: [0,10,20,30], [40,50,60,70], [80,90,100,110], [120,130,140,150]
    let spec = WideSegSpec {
        brand: "amazon",
        rows: 16,
        max_rg_rows: 4,
    };
    let tmp = write_wide_segment(spec.brand, spec.rows, spec.max_rg_rows);
    let path = tmp.path().to_path_buf();
    let file = std::fs::File::open(&path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let parquet_meta = meta.metadata().clone();
    let schema = meta.schema().clone();
    assert_eq!(parquet_meta.num_row_groups(), 4);

    // Tree: AND(price > 60, OR(Collector, qty < 2))
    let tree = BoolNode::And(vec![
        pred_wide_int("price", Operator::Gt, 60),
        BoolNode::Or(vec![
            BoolNode::Collector { annotation_id: 0 },
            pred_wide_int("qty", Operator::Lt, 2),
        ]),
    ]);
    let tree = tree.push_not_down().flatten();

    // Build pruning predicates.
    let mut leaf_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = Vec::new();
    collect_pred_exprs(&tree, &mut leaf_exprs);
    let pruning_predicates: HashMap<
        usize,
        Arc<datafusion::physical_optimizer::pruning::PruningPredicate>,
    > = leaf_exprs
        .iter()
        .filter_map(|expr| {
            crate::indexed_table::page_pruner::build_pruning_predicate(expr, schema.clone())
                .map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
        })
        .collect();

    // Simulate a chunk with RGs [2, 3] (offset — doesn't start at 0).
    let rg_indices: Vec<usize> = vec![2, 3];
    let spt = StatsPruneTree::build_from_bool_node(
        &tree,
        &pruning_predicates,
        &parquet_meta,
        &schema,
        &rg_indices,
    );

    // Assert: rg_can_match for position 0 (RG2, prices 80-110) should be true (price>60 matches).
    // Assert: rg_can_match for position 1 (RG3, prices 120-150) should be true.
    let rg_index_to_pos: HashMap<usize, usize> = rg_indices
        .iter()
        .enumerate()
        .map(|(pos, &idx)| (idx, pos))
        .collect();
    assert_eq!(spt.rg_can_match.len(), 2, "subset-relative: 2 RGs in chunk");
    assert!(
        spt.rg_can_match[0],
        "RG2 (prices 80-110) should match price>60"
    );
    assert!(
        spt.rg_can_match[1],
        "RG3 (prices 120-150) should match price>60"
    );

    // Now simulate a chunk with RGs [0, 1] — these SHOULD be pruned.
    let rg_indices_low: Vec<usize> = vec![0, 1];
    let spt_low = StatsPruneTree::build_from_bool_node(
        &tree,
        &pruning_predicates,
        &parquet_meta,
        &schema,
        &rg_indices_low,
    );
    // RG0 (prices 0-30): price>60 → false → AND=false
    assert!(
        !spt_low.rg_can_match[0],
        "RG0 (prices 0-30) should be pruned by price>60"
    );
    // RG1 (prices 40-70): price>60 might be true for row with price=70
    // (stats: min=40, max=70, so max > 60 → can_match=true at RG stats level)
    // This is expected: stats pruning is conservative.

    // Build a TreeBitsetSource with the offset chunk [2,3] and call prefetch_rg.
    #[derive(Debug)]
    struct AllDocs;
    impl RowGroupDocsCollector for AllDocs {
        fn collect_packed_u64_bitset(
            &self,
            min_doc: i32,
            max_doc: i32,
        ) -> Result<Vec<u64>, String> {
            let span = (max_doc - min_doc) as usize;
            let mut out = vec![0u64; span.div_ceil(64)];
            for i in 0..span {
                out[i / 64] |= 1u64 << (i % 64);
            }
            Ok(out)
        }
    }

    let tree_arc = Arc::new(tree);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> =
        vec![(0, Arc::new(AllDocs) as Arc<dyn RowGroupDocsCollector>)];
    let resolved = tree_arc.resolve(&per_leaf).unwrap();
    let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&parquet_meta)));

    let source = crate::indexed_table::eval::TreeBitsetSource {
        tree: Arc::new(resolved),
        evaluator: Arc::new(crate::indexed_table::eval::bitmap_tree::BitmapTreeEvaluator),
        leaves: Arc::new(
            crate::indexed_table::eval::bitmap_tree::CollectorLeafBitmaps::without_metrics(),
        ),
        page_pruner: pruner,
        cost_predicate: 1,
        cost_collector: 10,
        max_collector_parallelism: 1,
        pruning_predicates: Arc::new(pruning_predicates),
        page_prune_metrics: None,
        collector_strategy: crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
        stats_prune_tree: Some(Arc::new(spt_low)),
        rg_index_to_pos: rg_indices_low
            .iter()
            .enumerate()
            .map(|(pos, &idx)| (idx, pos))
            .collect(),
    };

    // Assert: prefetch_rg for RG0 (offset index 0, pruned) returns None.
    let rg0 = RowGroupInfo {
        index: 0,
        first_row: 0,
        num_rows: 4,
    };
    let result_rg0 = source.prefetch_rg(&rg0, 0, 4).unwrap();
    assert!(
        result_rg0.is_none(),
        "RG0 should be pruned by stats (price>60 fails for prices 0-30)"
    );

    // Assert: prefetch_rg for RG1 at offset index 1 — may or may not be pruned
    // depending on stats (max price in RG1 is 70 which > 60, so can_match=true).
    let rg1 = RowGroupInfo {
        index: 1,
        first_row: 4,
        num_rows: 4,
    };
    let result_rg1 = source.prefetch_rg(&rg1, 4, 8).unwrap();
    // RG1 (prices 40-70): max=70 > 60, so stats say can-match. Not pruned.
    assert!(
        result_rg1.is_some(),
        "RG1 (max price=70) should not be stats-pruned"
    );

    // Now build source for chunk [2,3] — both survive. Verify offset lookup works.
    let source2 = crate::indexed_table::eval::TreeBitsetSource {
        tree: source.tree.clone(),
        evaluator: Arc::new(crate::indexed_table::eval::bitmap_tree::BitmapTreeEvaluator),
        leaves: Arc::new(
            crate::indexed_table::eval::bitmap_tree::CollectorLeafBitmaps::without_metrics(),
        ),
        page_pruner: Arc::new(PagePruner::new(&schema, Arc::clone(&parquet_meta))),
        cost_predicate: 1,
        cost_collector: 10,
        max_collector_parallelism: 1,
        pruning_predicates: source.pruning_predicates.clone(),
        page_prune_metrics: None,
        collector_strategy: crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
        stats_prune_tree: Some(Arc::new(spt)),
        rg_index_to_pos: rg_index_to_pos,
    };

    // RG2 at absolute index 2: should NOT be pruned (prices 80-110, all > 60).
    let rg2 = RowGroupInfo {
        index: 2,
        first_row: 8,
        num_rows: 4,
    };
    let result_rg2 = source2.prefetch_rg(&rg2, 8, 12).unwrap();
    assert!(
        result_rg2.is_some(),
        "RG2 at offset index should not be pruned"
    );
    // Verify the collector bitmap is non-empty (all docs match).
    let prefetched = result_rg2.unwrap();
    assert!(
        !prefetched.candidates.is_empty(),
        "RG2 should have non-empty candidates"
    );

    // RG3 at absolute index 3: should NOT be pruned.
    let rg3 = RowGroupInfo {
        index: 3,
        first_row: 12,
        num_rows: 4,
    };
    let result_rg3 = source2.prefetch_rg(&rg3, 12, 16).unwrap();
    assert!(
        result_rg3.is_some(),
        "RG3 at offset index should not be pruned"
    );
}

/// Directly asserts that when a subtree under OR is stats-pruned, the
/// collector inside it gets an empty bitmap in `per_leaf`.
///
/// Tree: OR(AND(price > 100, Collector0), Collector1)
/// For RG1 (prices 40-70): AND subtree is pruned (price>100 fails).
/// Collector1 survives → RG not skipped → refinement runs.
/// Collector0 must have an empty per_leaf entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_prune_asserts_empty_collector_bitset_in_pruned_subtree() {
    use crate::indexed_table::eval::RowGroupBitsetSource;
    use crate::indexed_table::page_pruner::StatsPruneTree;

    let spec = WideSegSpec {
        brand: "amazon",
        rows: 16,
        max_rg_rows: 4,
    };
    let tmp = write_wide_segment(spec.brand, spec.rows, spec.max_rg_rows);
    let path = tmp.path().to_path_buf();
    let file = std::fs::File::open(&path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let parquet_meta = meta.metadata().clone();
    let schema = meta.schema().clone();

    // Tree: OR(AND(price > 100, Collector0), Collector1)
    // Collector0 is under AND with a predicate that fails for low-price RGs.
    // Collector1 always matches → OR always has candidates.
    let tree = BoolNode::Or(vec![
        BoolNode::And(vec![
            pred_wide_int("price", Operator::Gt, 100),
            BoolNode::Collector { annotation_id: 0 },
        ]),
        BoolNode::Collector { annotation_id: 1 },
    ]);
    let tree = tree.push_not_down().flatten();

    let mut leaf_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = Vec::new();
    collect_pred_exprs(&tree, &mut leaf_exprs);
    let pruning_predicates: HashMap<
        usize,
        Arc<datafusion::physical_optimizer::pruning::PruningPredicate>,
    > = leaf_exprs
        .iter()
        .filter_map(|expr| {
            crate::indexed_table::page_pruner::build_pruning_predicate(expr, schema.clone())
                .map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
        })
        .collect();

    // Chunk with RG1 only (prices 40-70). Offset index = 1.
    let rg_indices: Vec<usize> = vec![1];
    let spt = StatsPruneTree::build_from_bool_node(
        &tree,
        &pruning_predicates,
        &parquet_meta,
        &schema,
        &rg_indices,
    );
    // Root OR should still be true (Collector1 child is always-true).
    assert!(
        spt.rg_can_match[0],
        "OR root should be true (Collector1 always matches)"
    );
    // AND child should be false (price>100 fails for RG1 max=70).
    assert!(
        !spt.children[0].rg_can_match[0],
        "AND(price>100, Coll0) should be false for RG1"
    );
    // Collector1 child should be true.
    assert!(spt.children[1].rg_can_match[0], "Collector1 should be true");

    // Build evaluator and call prefetch_rg for RG1.
    #[derive(Debug)]
    struct AllDocs;
    impl RowGroupDocsCollector for AllDocs {
        fn collect_packed_u64_bitset(
            &self,
            min_doc: i32,
            max_doc: i32,
        ) -> Result<Vec<u64>, String> {
            let span = (max_doc - min_doc) as usize;
            let mut out = vec![0u64; span.div_ceil(64)];
            for i in 0..span {
                out[i / 64] |= 1u64 << (i % 64);
            }
            Ok(out)
        }
    }

    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = vec![
        (0, Arc::new(AllDocs) as Arc<dyn RowGroupDocsCollector>),
        (1, Arc::new(AllDocs) as Arc<dyn RowGroupDocsCollector>),
    ];
    let resolved = Arc::new(tree).resolve(&per_leaf).unwrap();
    let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&parquet_meta)));
    let rg_index_to_pos: HashMap<usize, usize> = rg_indices
        .iter()
        .enumerate()
        .map(|(pos, &idx)| (idx, pos))
        .collect();

    let source = crate::indexed_table::eval::TreeBitsetSource {
        tree: Arc::new(resolved),
        evaluator: Arc::new(crate::indexed_table::eval::bitmap_tree::BitmapTreeEvaluator),
        leaves: Arc::new(
            crate::indexed_table::eval::bitmap_tree::CollectorLeafBitmaps::without_metrics(),
        ),
        page_pruner: pruner,
        cost_predicate: 1,
        cost_collector: 10,
        max_collector_parallelism: 1,
        pruning_predicates: Arc::new(pruning_predicates),
        page_prune_metrics: None,
        collector_strategy: crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
        stats_prune_tree: Some(Arc::new(spt)),
        rg_index_to_pos,
    };

    // RG1 at absolute index 1: NOT pruned at root (OR is true).
    let rg1 = RowGroupInfo {
        index: 1,
        first_row: 4,
        num_rows: 4,
    };
    let result = source.prefetch_rg(&rg1, 4, 8).unwrap();
    assert!(
        result.is_some(),
        "RG1 should NOT be pruned at root (OR has surviving child)"
    );

    let prefetched = result.unwrap();
    // Candidates should be non-empty (Collector1 matches all docs).
    assert!(
        !prefetched.candidates.is_empty(),
        "Collector1 should produce non-empty candidates"
    );

    // Downcast context to TreePrefetch to inspect per_leaf bitmaps.
    let tree_prefetch = prefetched
        .context
        .downcast_ref::<crate::indexed_table::eval::TreePrefetch>()
        .expect("context should be TreePrefetch");

    // Critical assertion: per_leaf must have 2 entries (one per collector).
    assert_eq!(
        tree_prefetch.per_leaf.len(),
        2,
        "both collectors must have per_leaf entries; got {}",
        tree_prefetch.per_leaf.len()
    );

    // Collector0 (inside pruned AND subtree) must have EMPTY bitmap.
    let has_empty = tree_prefetch.per_leaf.iter().any(|(_, bm)| bm.is_empty());
    assert!(
        has_empty,
        "pruned Collector0 should have an empty bitmap in per_leaf"
    );

    // Collector1 (surviving) must have NON-EMPTY bitmap.
    let has_nonempty = tree_prefetch.per_leaf.iter().any(|(_, bm)| !bm.is_empty());
    assert!(
        has_nonempty,
        "surviving Collector1 should have a non-empty bitmap in per_leaf"
    );
}

/// Regression test for the flatten-misalignment bug: when the BoolNode tree
/// used for StatsPruneTree is not normalized (push_not_down + flatten), child
/// indices in spt.children don't match ResolvedNode children, causing the
/// wrong branch's rg_can_match to be applied — pruning rows that match via
/// a sibling OR branch.
///
/// Scenario: 3-way OR built as nested OR(OR(A,B), C) — after flatten becomes
/// OR(A, B, C). Each AND branch pairs a Collector with a Predicate. One
/// Predicate is prunable by RG stats. The test verifies that rows matching
/// a non-pruned branch still appear in results regardless of clause ordering.
///
/// Tree: OR(AND(Collector0, price > 100), AND(Collector1, region = "us-east"), AND(Collector2, qty > 5))
/// Data: 16 rows, 4 RGs of 4 rows each.
///   prices: 0,10,20,30 | 40,50,60,70 | 80,90,100,110 | 120,130,140,150
///   qtys: 0,1,2,3 | 4,5,6,0 | 1,2,3,4 | 5,6,0,1
///   regions: us-east,us-west,eu-west,us-east | us-west,eu-west,us-east,us-west | ...
///
/// RG0: price max=30 → price>100 pruned. RG0 has region="us-east" rows → branch1 should match.
/// Without the fix, branch0's pruning would incorrectly affect branch1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_prune_three_way_or_flatten_misalignment_regression() {
    let specs = vec![WideSegSpec {
        brand: "amazon",
        rows: 16,
        max_rg_rows: 4,
    }];

    // Build as nested OR(OR(A,B), C) — this is how convert_expr produces it
    // from `A OR B OR C` (left-associative binary OR).
    let tree = BoolNode::Or(vec![
        BoolNode::Or(vec![
            BoolNode::And(vec![
                BoolNode::Collector { annotation_id: 0 },
                pred_wide_int("price", Operator::Gt, 100),
            ]),
            BoolNode::And(vec![
                BoolNode::Collector { annotation_id: 1 },
                pred_wide_str("region", Operator::Eq, "us-east"),
            ]),
        ]),
        BoolNode::And(vec![
            BoolNode::Collector { annotation_id: 2 },
            pred_wide_int("qty", Operator::Gt, 5),
        ]),
    ]);

    // Oracle: row matches if (price > 100) OR (region == "us-east") OR (qty > 5)
    let expected = wide_oracle(&specs, |i| {
        let price = (i as i32) * 10;
        let qty = (i as i32) % 7;
        let region = match i % 3 {
            0 => "us-east",
            1 => "us-west",
            _ => "eu-west",
        };
        price > 100 || region == "us-east" || qty > 5
    });

    let rows = run_wide_segments_with_stats_pruning(specs, tree, 1).await;
    assert_eq!(
        rows, expected,
        "3-way OR with nested structure must produce correct results after flatten"
    );
}

/// Same as above but tests different clause orderings to ensure no
/// order-dependent pruning bugs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_prune_three_way_or_ordering_independence() {
    let specs = vec![WideSegSpec {
        brand: "amazon",
        rows: 16,
        max_rg_rows: 4,
    }];

    let expected = wide_oracle(&specs, |i| {
        let price = (i as i32) * 10;
        let qty = (i as i32) % 7;
        let region = match i % 3 {
            0 => "us-east",
            1 => "us-west",
            _ => "eu-west",
        };
        price > 100 || region == "us-east" || qty > 5
    });

    // All 3 orderings of a nested OR that flatten differently.
    let orderings: Vec<BoolNode> = vec![
        // OR(OR(A,B), C)
        BoolNode::Or(vec![
            BoolNode::Or(vec![
                BoolNode::And(vec![
                    BoolNode::Collector { annotation_id: 0 },
                    pred_wide_int("price", Operator::Gt, 100),
                ]),
                BoolNode::And(vec![
                    BoolNode::Collector { annotation_id: 1 },
                    pred_wide_str("region", Operator::Eq, "us-east"),
                ]),
            ]),
            BoolNode::And(vec![
                BoolNode::Collector { annotation_id: 2 },
                pred_wide_int("qty", Operator::Gt, 5),
            ]),
        ]),
        // OR(A, OR(B,C))
        BoolNode::Or(vec![
            BoolNode::And(vec![
                BoolNode::Collector { annotation_id: 0 },
                pred_wide_int("price", Operator::Gt, 100),
            ]),
            BoolNode::Or(vec![
                BoolNode::And(vec![
                    BoolNode::Collector { annotation_id: 1 },
                    pred_wide_str("region", Operator::Eq, "us-east"),
                ]),
                BoolNode::And(vec![
                    BoolNode::Collector { annotation_id: 2 },
                    pred_wide_int("qty", Operator::Gt, 5),
                ]),
            ]),
        ]),
        // OR(OR(B,C), A)
        BoolNode::Or(vec![
            BoolNode::Or(vec![
                BoolNode::And(vec![
                    BoolNode::Collector { annotation_id: 1 },
                    pred_wide_str("region", Operator::Eq, "us-east"),
                ]),
                BoolNode::And(vec![
                    BoolNode::Collector { annotation_id: 2 },
                    pred_wide_int("qty", Operator::Gt, 5),
                ]),
            ]),
            BoolNode::And(vec![
                BoolNode::Collector { annotation_id: 0 },
                pred_wide_int("price", Operator::Gt, 100),
            ]),
        ]),
    ];

    for (idx, tree) in orderings.into_iter().enumerate() {
        let rows = run_wide_segments_with_stats_pruning(
            vec![WideSegSpec {
                brand: "amazon",
                rows: 16,
                max_rg_rows: 4,
            }],
            tree,
            1,
        )
        .await;
        assert_eq!(rows, expected, "ordering {} produced wrong results", idx);
    }
}
