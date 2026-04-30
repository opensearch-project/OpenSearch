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
//! - `segment_ord != 0` propagation through collectors.
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

/// A collector whose matching set is parameterised by segment_ord: it records
/// the ords it was built for so tests can confirm the evaluator factory called
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
/// set indexed by `segment_ord` (caller supplies one Vec<i32> per segment).
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
            segment_ord: ord as i32,
            // Per-segment max_doc. Both happen to be 8 here.
            max_doc: offset,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
        });
    }

    let schema = schema_opt.unwrap();
    let per_segment_matches = Arc::new(per_segment_matches);

    // Factory produces a single-collector evaluator per chunk. The collector's
    // matching set is pulled from `per_segment_matches[segment.segment_ord]`,
    // so wrong segment_ord propagation would immediately produce wrong rows.
    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_segment_matches = Arc::clone(&per_segment_matches);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics| {
            let matching = per_segment_matches
                .get(segment.segment_ord as usize)
                .cloned()
                .unwrap_or_default();
            let collector: Arc<dyn RowGroupDocsCollector> =
                Arc::new(PerSegmentCollector { matching });
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(
                crate::indexed_table::eval::single_collector::SingleCollectorEvaluator::new(
                    collector, pruner, None, None, None, None,
                ),
            );
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
        store,
        store_url,
        evaluator_factory: factory,
        target_partitions: num_partitions,
        force_strategy: Some(FilterStrategy::BooleanMask),
        force_pushdown: Some(false),
        pushdown_predicate: None,
        query_config: std::sync::Arc::new(
            crate::datafusion_query_config::DatafusionQueryConfig::default(),
        ),
        predicate_columns: vec![],
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
            // Infer segment_ord by brand for verification purposes.
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
/// `(segment_ord_inferred_from_brand, brand, price)` triples sorted so tests
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
            segment_ord: ord as i32,
            max_doc: offset,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
        });
    }

    let schema = schema_opt.unwrap();
    let per_segment_matches = Arc::new(per_segment_matches);
    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_segment_matches = Arc::clone(&per_segment_matches);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics| {
            let matching = per_segment_matches
                .get(segment.segment_ord as usize)
                .cloned()
                .unwrap_or_default();
            let collector: Arc<dyn RowGroupDocsCollector> =
                Arc::new(PerSegmentCollector { matching });
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(
                crate::indexed_table::eval::single_collector::SingleCollectorEvaluator::new(
                    collector, pruner, None, None, None, None,
                ),
            );
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
        store,
        store_url,
        evaluator_factory: factory,
        target_partitions: num_partitions,
        force_strategy: Some(FilterStrategy::BooleanMask),
        force_pushdown: Some(false),
        pushdown_predicate: None,
        query_config: std::sync::Arc::new(
            crate::datafusion_query_config::DatafusionQueryConfig::default(),
        ),
        predicate_columns: vec![],
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
            segment_ord: ord as i32,
            max_doc: offset,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
        });
    }

    let schema = schema_opt.unwrap();
    let tree = Arc::new(tree.push_not_down().flatten());

    // Per-chunk evaluator: builds a Path C TreeBitsetSource with a fresh
    // all-docs collector for this segment.
    let factory: super::super::table_provider::EvaluatorFactory = {
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics| {
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
                },
            );
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
        store,
        store_url,
        evaluator_factory: factory,
        target_partitions: num_partitions,
        force_strategy: Some(FilterStrategy::BooleanMask),
        force_pushdown: Some(false),
        pushdown_predicate: None,
        query_config: std::sync::Arc::new(
            crate::datafusion_query_config::DatafusionQueryConfig::default(),
        ),
        predicate_columns: vec![],
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
        BoolNode::Collector {
            query_bytes: Arc::from(&[0u8][..]),
        },
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
        BoolNode::Collector {
            query_bytes: Arc::from(&[0u8][..]),
        },
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
        BoolNode::Collector {
            query_bytes: Arc::from(&[0u8][..]),
        },
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
        BoolNode::Collector {
            query_bytes: Arc::from(&[0u8][..]),
        },
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
