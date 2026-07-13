/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Row-id stability under sort-aware segment reversal.
//!
//! Pins the contract that lets QTF compose with the reversal optimization:
//! reversing the iteration order of `Vec<SegmentFileInfo>` must NOT change which
//! `__row_id__` values get emitted for the same matching rows. The only thing
//! that changes is the order in which segments are processed (which the TopK
//! above us re-sorts anyway).
//!
//! Why this matters: `api::fetch_by_row_ids` rebuilds segments via
//! `build_segments(ShardView.object_metas, ShardView.writer_generations)` —
//! always in catalog order, never reversed. For QTF query→fetch round-trip to
//! resolve to the right rows, the IDs query phase emits must agree with the
//! catalog-order `global_base` mapping fetch will compute. That holds only if
//! `reverse_segment_iteration_order` preserves each segment's original
//! `global_base`, which is exactly what these tests verify end-to-end.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::ArrowWriter;
use futures::StreamExt;
use tempfile::NamedTempFile;

use crate::indexed_executor::reverse_segment_iteration_order;
use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::eval::bitmap_tree::BitmapTreeEvaluator;
use crate::indexed_table::eval::{RowGroupBitsetSource, TreeBitsetSource};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::stream::{FilterStrategy, RowGroupInfo};
use crate::indexed_table::table_provider::{
    IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};

/// One segment's worth of synthetic rows. `tag` distinguishes segments at the
/// data layer; `match_local_offsets` are segment-local doc IDs that the mock
/// collector will return as matches.
struct SegSpec {
    tag: &'static str,
    rows: usize,
    match_local_offsets: Vec<i32>,
}

/// Per the writer-side invariant, every parquet segment carries the `__row_id__`
/// column. The indexed scan path strips it from the read projection when
/// `emit_row_ids: true` and recomputes shard-global IDs from per-segment position
/// (`global_base + rg.first_row + position_in_rg`). This test mirrors the
/// production fixture: the column exists and the indexed path replaces it.
fn segment_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tag", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
        Field::new(crate::ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]))
}

fn write_segment(spec: &SegSpec) -> NamedTempFile {
    let schema = segment_schema();
    let tags: Vec<&str> = (0..spec.rows).map(|_| spec.tag).collect();
    let vs: Vec<i32> = (0..spec.rows as i32).collect();
    // Segment-local IDs (0..rows) — production writer also stamps these
    // monotonically per segment. The indexed path doesn't read them; it
    // recomputes IDs from position. They're written so the column is valid.
    let row_ids: Vec<i64> = (0..spec.rows as i64).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(tags)),
            Arc::new(Int32Array::from(vs)),
            Arc::new(Int64Array::from(row_ids)),
        ],
    )
    .unwrap();
    let tmp = NamedTempFile::new().unwrap();
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(4)
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    tmp
}

/// Mock collector that returns the configured local doc offsets that fall in `[min_doc, max_doc)`.
#[derive(Debug)]
struct LocalOffsetCollector {
    matching: Vec<i32>,
}

impl RowGroupDocsCollector for LocalOffsetCollector {
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

/// Build a `Vec<SegmentFileInfo>` for the given specs, including catalog-order
/// `global_base` so `__row_id__ = global_base + local_pos` resolves to a unique
/// shard-global value per matching row.
fn build_segments(tmps: &[NamedTempFile], specs: &[SegSpec]) -> (Vec<SegmentFileInfo>, SchemaRef) {
    let mut segs = Vec::new();
    let mut schema_opt: Option<SchemaRef> = None;
    let mut cumulative: u64 = 0;
    for (i, tmp) in tmps.iter().enumerate() {
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
        for j in 0..parquet_meta.num_row_groups() {
            let n = parquet_meta.row_group(j).num_rows();
            rgs.push(RowGroupInfo {
                index: j,
                first_row: offset,
                num_rows: n,
            });
            offset += n;
        }
        let max_doc = offset;
        let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
        segs.push(SegmentFileInfo {
            writer_generation: i as i64,
            max_doc,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            global_base: cumulative,
            sort_min: None,
            sort_max: None,
        });
        cumulative += specs[i].rows as u64;
    }
    (segs, schema_opt.unwrap())
}

/// Collect emitted `__row_id__` values for the given segment vec.
async fn collect_row_ids(
    segments: Vec<SegmentFileInfo>,
    schema: SchemaRef,
    specs: &[SegSpec],
) -> Vec<i64> {
    // One collector per segment, keyed by writer_generation == segment ord at build time.
    // EvaluatorFactory selects the matching set for the segment it's asked about — so
    // this works whether segments are in natural order or reversed (it identifies the
    // segment by its writer_generation, which moves with the segment).
    let per_gen: Vec<Vec<i32>> = specs
        .iter()
        .map(|s| s.match_local_offsets.clone())
        .collect();
    let per_gen = Arc::new(per_gen);

    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_gen = Arc::clone(&per_gen);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics, _stats_prune_tree| {
            let matching = per_gen
                .get(segment.writer_generation as usize)
                .cloned()
                .unwrap_or_default();
            let collector: Arc<dyn RowGroupDocsCollector> =
                Arc::new(LocalOffsetCollector { matching });
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            // Single-collector tree.
            let tree = BoolNode::Collector { annotation_id: 0 };
            let tree = tree.push_not_down();
            let resolved = tree.resolve(&[(0, collector)])?;
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
                stats_prune_tree: None,
                rg_index_to_pos: HashMap::new(),
            });
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
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
        query_config: Arc::new(qc),
        predicate_columns: vec![],
        emit_row_ids: true,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        cancellation_token: None,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT \"__row_id__\" FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let mut stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
    let mut ids: Vec<i64> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            ids.push(col.value(i));
        }
    }
    ids
}

/// 3 segments × varying match offsets. Run with natural iteration order, then reversed.
/// The set of emitted `__row_id__` values must be identical — the only thing reversal
/// changes is the order in which segments are scanned (which the test ignores by
/// comparing sorted sets).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_segments_row_ids_invariant_under_reversal() {
    let specs = vec![
        SegSpec {
            tag: "A",
            rows: 8,
            match_local_offsets: vec![0, 3, 7],
        }, // global ids 0, 3, 7
        SegSpec {
            tag: "B",
            rows: 6,
            match_local_offsets: vec![1, 4],
        }, // global ids 9, 12
        SegSpec {
            tag: "C",
            rows: 10,
            match_local_offsets: vec![0, 5, 9],
        }, // global ids 14, 19, 23
    ];
    let tmps: Vec<NamedTempFile> = specs.iter().map(write_segment).collect();
    let (segs_natural, schema) = build_segments(&tmps, &specs);

    // Natural order.
    let mut ids_natural = collect_row_ids(segs_natural.clone(), schema.clone(), &specs).await;
    ids_natural.sort();

    // Reversed iteration order (the optimization's effect). Per-segment global_base
    // is preserved by `reverse_segment_iteration_order` — the central invariant.
    let mut segs_reversed = segs_natural;
    reverse_segment_iteration_order(&mut segs_reversed);
    let mut ids_reversed = collect_row_ids(segs_reversed, schema, &specs).await;
    ids_reversed.sort();

    let expected = vec![0i64, 3, 7, 9, 12, 14, 19, 23];
    assert_eq!(ids_natural, expected, "natural-order ids");
    assert_eq!(
        ids_reversed, expected,
        "reversed-order ids — same shard-global IDs as natural"
    );
}

/// 4 segments. Confirms the property holds at the larger end of the test contract
/// (the IT runs with 2-4 segments per shard depending on doc routing) and that
/// `global_base` cumulation matches catalog order on every segment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_segments_row_ids_invariant_under_reversal() {
    let specs = vec![
        SegSpec {
            tag: "A",
            rows: 5,
            match_local_offsets: vec![0, 4],
        }, // 0, 4
        SegSpec {
            tag: "B",
            rows: 5,
            match_local_offsets: vec![2],
        }, // 7
        SegSpec {
            tag: "C",
            rows: 5,
            match_local_offsets: vec![1, 3],
        }, // 11, 13
        SegSpec {
            tag: "D",
            rows: 5,
            match_local_offsets: vec![0, 2, 4],
        }, // 15, 17, 19
    ];
    let tmps: Vec<NamedTempFile> = specs.iter().map(write_segment).collect();
    let (segs_natural, schema) = build_segments(&tmps, &specs);

    let mut ids_natural = collect_row_ids(segs_natural.clone(), schema.clone(), &specs).await;
    ids_natural.sort();

    let mut segs_reversed = segs_natural;
    reverse_segment_iteration_order(&mut segs_reversed);
    let mut ids_reversed = collect_row_ids(segs_reversed, schema, &specs).await;
    ids_reversed.sort();

    let expected = vec![0i64, 4, 7, 11, 13, 15, 17, 19];
    assert_eq!(ids_natural, expected);
    assert_eq!(ids_reversed, expected);
}
