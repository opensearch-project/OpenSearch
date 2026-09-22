/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Sort-aware scan with `index.sort.field = [ts, tags]` where `ts` is a
//! scalar lead and `tags` is a multi-value (LIST) tiebreaker.
//!
//! The chain check (`segments_chain_on_sort_key`) only inspects the LEAD
//! sort field, so a LIST in a non-lead slot must not disable the
//! optimization. `build_projected_lex_ordering` must still reduce that LIST
//! key to `array_min`/`array_max` so the advertised ordering matches what
//! the writer physically produced (the writer reduces every LIST sort column
//! positionally via `max_sort_modes`) and what the query's `visit(Sort)`
//! override emits for the same tiebreaker.
//!
//! Uses the real `segment_info::build_segments` so `sort_min`/`sort_max`
//! for the lead come from parquet footer statistics, exactly as in
//! production.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, ListArray, StringArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use futures::StreamExt;
use roaring::RoaringBitmap;
use tempfile::TempDir;

use super::super::eval::{PrefetchedRg, RowGroupBitsetSource};
use super::super::row_selection::bitmap_to_packed_bits;
use super::super::stream::RowGroupInfo;
use super::super::table_provider::{IndexedTableConfig, IndexedTableProvider};

/// Evaluator that admits every row of every row group (no index filter).
#[derive(Debug)]
struct MatchAllEvaluator;

impl RowGroupBitsetSource for MatchAllEvaluator {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        _min_doc: i32,
        _max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let n = rg.num_rows as u32;
        let candidates: RoaringBitmap = (0..n).collect();
        let packed = bitmap_to_packed_bits(&candidates, n);
        Ok(Some(PrefetchedRg {
            candidates,
            eval_nanos: 0,
            context: Box::new(()),
            mask_buffer: Some(datafusion::arrow::buffer::Buffer::from_vec(packed)),
        }))
    }

    fn on_batch_mask(
        &self,
        _rg_state: &dyn std::any::Any,
        _rg_first_row: i64,
        _position_map: &super::super::row_selection::PositionMap,
        _batch_offset: usize,
        _batch_len: usize,
        _batch: &RecordBatch,
    ) -> Result<Option<datafusion::arrow::array::BooleanArray>, String> {
        Ok(None)
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
            false,
        ),
    ]))
}

/// One segment's rows, already in the writer's physical order:
/// `(ts ASC, array_min(tags) ASC)`. Lists are chosen so that
/// lexicographic LIST order and MIN order DISAGREE on the duplicate `ts`
/// rows, which is what makes the tiebreaker observable.
fn write_segment(dir: &TempDir, name: &str, rows: &[(i64, &[&str])]) -> std::path::PathBuf {
    let ts: Vec<i64> = rows.iter().map(|(t, _)| *t).collect();
    let mut offsets = vec![0_i32];
    let mut values: Vec<&str> = Vec::new();
    for (_, tags) in rows {
        values.extend_from_slice(tags);
        offsets.push(values.len() as i32);
    }
    let tags = ListArray::new(
        Arc::new(Field::new("element", DataType::Utf8, true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(StringArray::from(values)),
        None,
    );
    let batch = RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from(ts)), Arc::new(tags)],
    )
    .unwrap();
    let path = dir.path().join(name);
    let mut w =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    path
}

// Two segments with disjoint `ts` ranges (so the lead chains) and duplicate
// `ts` values inside each segment (so the tiebreaker matters). Within a
// duplicate group the row with the smaller MIN element comes first even
// though it is lexicographically LARGER: `["z","a"]` (min a) before `["b"]`.
const SEG_A: &[(i64, &[&str])] = &[
    (1, &["m"]),
    (2, &["z", "a"]), // min a  — lexicographically after ["b"]
    (2, &["b"]),      // min b
    (3, &["q", "c"]), // min c
    (3, &["d"]),      // min d
];
const SEG_B: &[(i64, &[&str])] = &[
    (5, &["y", "e"]), // min e
    (5, &["f"]),      // min f
    (6, &["g"]),
    (7, &["x", "h"]), // min h
    (7, &["k"]),      // min k
];

/// Production (`build_query_session_context`) sets the session's
/// `target_partitions` to the same value that gates `chain_ok`, so mirror
/// that here rather than inheriting the machine's CPU count.
fn session(target_partitions: usize) -> SessionContext {
    let config =
        datafusion::prelude::SessionConfig::new().with_target_partitions(target_partitions);
    SessionContext::new_with_config(config)
}

async fn build_provider(
    ctx: &SessionContext,
    target_partitions: usize,
    sort_fields: Vec<String>,
    sort_orders: Vec<String>,
) -> Arc<IndexedTableProvider> {
    // Leak the tempdir for the lifetime of the test process: the provider
    // reads the files lazily during execution.
    let dir = Box::leak(Box::new(TempDir::new().unwrap()));
    let a = write_segment(dir, "seg_a.parquet", SEG_A);
    let b = write_segment(dir, "seg_b.parquet", SEG_B);

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let object_meta = |path: &std::path::Path| object_store::ObjectMeta {
        location: object_store::path::Path::from(path.to_string_lossy().as_ref()),
        last_modified: chrono::Utc::now(),
        size: std::fs::metadata(path).unwrap().len(),
        e_tag: None,
        version: None,
    };
    let metas = vec![object_meta(&a), object_meta(&b)];
    let metadata_cache = ctx
        .state()
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache();
    let (segments, resolved_schema) = crate::indexed_table::segment_info::build_segments(
        &ctx.state(),
        Arc::clone(&store),
        &metas,
        &[0, 1],
        metadata_cache,
        &sort_fields,
    )
    .await
    .unwrap();
    assert_eq!(segments.len(), 2);

    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(target_partitions)
        .indexed_pushdown_filters(false)
        .build();
    Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: resolved_schema,
        segments,
        store,
        store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
        evaluator_factory: Arc::new(|_, _, _, _| Ok(Arc::new(MatchAllEvaluator))),
        pushdown_predicate: None,
        query_config: Arc::new(qc),
        predicate_columns: vec![],
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields,
        sort_orders,
        cancellation_token: None,
    }))
}

fn find_shard_exec_line(plan: &Arc<dyn ExecutionPlan>) -> String {
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    rendered
        .lines()
        .find(|l| l.contains("QueryShardExec"))
        .map(str::trim)
        .map(str::to_string)
        .unwrap_or_else(|| panic!("no QueryShardExec in plan:\n{rendered}"))
}

async fn collect_rows(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> Vec<(i64, String)> {
    let mut stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let ts = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let tmin = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((ts.value(i), tmin.value(i).to_string()));
        }
    }
    rows
}

/// Production plan shape for `sort ts, tags | head N`: a `Sort` whose
/// LIST key was already reduced to `array_min(tags)` by the Substrait
/// `visit(Sort)` override, directly over the shard read (no intermediate
/// projection of the key), followed by a limit. Built through the DataFrame
/// API so the logical plan matches what the Substrait consumer produces.
fn sort_ts_then_min_tags(
    ctx: &SessionContext,
    tie_nulls_first: bool,
) -> impl std::future::Future<Output = Arc<dyn ExecutionPlan>> + '_ {
    use datafusion::functions_nested::expr_fn::array_min;
    use datafusion::logical_expr::{cast, col};
    async move {
        ctx.table("t")
            .await
            .unwrap()
            .sort(vec![
                col("ts").sort(true, false),
                array_min(col("tags")).sort(true, tie_nulls_first),
            ])
            .unwrap()
            .limit(0, Some(6))
            .unwrap()
            .select(vec![
                col("ts"),
                cast(array_min(col("tags")), DataType::Utf8).alias("tmin"),
            ])
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }
}

/// `sort ts, tags | head N` on `index.sort.field=[ts, tags]`: the chain
/// holds on the scalar lead, the LIST tiebreaker is advertised as
/// `array_min(CAST(tags))` — the same expression the query's key coerces to —
/// so `EnforceSorting` drops the `SortExec`, keeps one partition per segment
/// and merges with `SortPreservingMergeExec`. Rows come out in
/// (ts, min(tags)) order, not lexicographic LIST order.
///
/// The tiebreaker is requested `NULLS FIRST` to match what the reader
/// currently advertises for ASC keys; see
/// `scalar_lead_list_tiebreaker_nulls_last_query_is_not_eliminated` for the
/// gap with the frontend's default `ASC NULLS LAST`.
#[tokio::test]
async fn scalar_lead_list_tiebreaker_advertises_ordering_and_eliminates_sort() {
    let ctx = session(4);
    let provider = build_provider(
        &ctx,
        4, // > number of segments: chain path eligible, no repartition needed
        vec!["ts".into(), "tags".into()],
        vec!["asc".into(), "asc".into()],
    )
    .await;
    ctx.register_table("t", provider).unwrap();

    let plan = sort_ts_then_min_tags(&ctx, true).await;
    let rendered = displayable(plan.as_ref()).indent(true).to_string();

    let shard = find_shard_exec_line(&plan);
    assert!(
        shard.contains("partitions=2, segments=2"),
        "expected one partition per segment, got: {shard}"
    );
    // The LIST key carries the same canonicalising CAST the TypeCoercion
    // analyzer puts on the query's `array_min(tags)` (parquet child field
    // `element` -> DataFusion's `item`), so the two compare equal.
    assert!(
        shard.contains("sorted=[ts@0 ASC, array_min(CAST(tags@1 AS List(Utf8View))) ASC]"),
        "LIST tiebreaker must be advertised as array_min(CAST(tags)), got: {shard}"
    );
    assert!(
        rendered.contains("SortPreservingMergeExec"),
        "chain path should yield a sort-preserving merge, plan:\n{rendered}"
    );
    assert!(
        !rendered.contains("SortExec"),
        "no re-sort expected when the advertised ordering satisfies the query, plan:\n{rendered}"
    );
    // The merge must read the scan's partitions directly: a round-robin
    // repartition between them would destroy the per-segment order. (A
    // repartition *above* the merge for the projection is fine.)
    let lines: Vec<&str> = rendered.lines().map(str::trim).collect();
    let spm = lines
        .iter()
        .position(|l| l.starts_with("SortPreservingMergeExec"))
        .expect("SortPreservingMergeExec present");
    assert!(
        lines[spm + 1].starts_with("QueryShardExec"),
        "SortPreservingMergeExec must sit directly on QueryShardExec, plan:\n{rendered}"
    );

    let rows = collect_rows(&ctx, plan).await;
    assert_eq!(
        rows,
        vec![
            (1, "m".to_string()),
            (2, "a".to_string()), // ["z","a"] first: MIN order, not lexicographic
            (2, "b".to_string()),
            (3, "c".to_string()),
            (3, "d".to_string()),
            (5, "e".to_string()),
        ]
    );
}

/// Control: same index, but `target_partitions < segments`, so the chain
/// path is ineligible. The scan advertises `unsorted` and the planner keeps a
/// `SortExec`. Proves the positive test is asserting the optimization, not
/// just the final row order (which is identical either way).
#[tokio::test]
async fn scalar_lead_list_tiebreaker_falls_back_when_partitions_too_low() {
    let ctx = session(1);
    let provider = build_provider(
        &ctx,
        1,
        vec!["ts".into(), "tags".into()],
        vec!["asc".into(), "asc".into()],
    )
    .await;
    ctx.register_table("t", provider).unwrap();

    let plan = sort_ts_then_min_tags(&ctx, true).await;
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    let shard = find_shard_exec_line(&plan);
    assert!(shard.contains("ordering=unsorted"), "got: {shard}");
    assert!(
        rendered.contains("SortExec"),
        "fallback must re-sort, plan:\n{rendered}"
    );

    let rows = collect_rows(&ctx, plan).await;
    assert_eq!(rows[1], (2, "a".to_string()));
    assert_eq!(rows[2], (2, "b".to_string()));
}

/// Known gap (pre-existing, not specific to LIST keys): the reader advertises
/// ASC keys as `NULLS FIRST`, while the PPL/SQL frontend emits
/// `ASC_NULLS_LAST` and the writer places nulls per `index.sort.missing`
/// (default `_last`). For a nullable key — and `array_min(..)` is always
/// nullable — `EnforceSorting` compares null placement exactly, so the
/// default-shaped query is NOT satisfied by the advertised ordering and a
/// `SortExec` remains even though the chain held. Fixing this means
/// plumbing `index.sort.missing` to the reader alongside `sort_orders`;
/// this test documents the current behaviour so the follow-up flips it.
#[tokio::test]
async fn scalar_lead_list_tiebreaker_nulls_last_query_is_not_eliminated() {
    let ctx = session(4);
    let provider = build_provider(
        &ctx,
        4,
        vec!["ts".into(), "tags".into()],
        vec!["asc".into(), "asc".into()],
    )
    .await;
    ctx.register_table("t", provider).unwrap();

    let plan = sort_ts_then_min_tags(&ctx, false).await;
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    let shard = find_shard_exec_line(&plan);
    // Chain held and the ordering was advertised...
    assert!(shard.contains("sorted=[ts@0 ASC, array_min(CAST(tags@1 AS List(Utf8View))) ASC]"));
    // ...but only the scalar prefix is recognised; the nullable LIST key's
    // NULLS LAST request does not match the advertised NULLS FIRST.
    assert!(
        rendered.contains("SortExec") && rendered.contains("sort_prefix=[ts@0 ASC NULLS LAST]"),
        "expected residual SortExec with satisfied ts prefix, plan:\n{rendered}"
    );
}

/// Q1 counterpart, for contrast: when the LIST field is the LEAD sort key,
/// footer stats are element-level and `compute_segment_sort_bounds` yields
/// `(None, None)`, so the chain cannot be proven and the scan stays
/// `unsorted` even with ample partitions. Tracked in
/// opensearch-project/OpenSearch#23095.
#[tokio::test]
async fn list_lead_does_not_chain_even_with_enough_partitions() {
    use datafusion::functions_nested::expr_fn::array_min;
    use datafusion::logical_expr::col;
    let ctx = session(4);
    let provider = build_provider(&ctx, 4, vec!["tags".into()], vec!["asc".into()]).await;
    ctx.register_table("t", provider).unwrap();

    let plan = ctx
        .table("t")
        .await
        .unwrap()
        .sort(vec![array_min(col("tags")).sort(true, true)])
        .unwrap()
        .limit(0, Some(3))
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let shard = find_shard_exec_line(&plan);
    assert!(shard.contains("ordering=unsorted"), "got: {shard}");
}
