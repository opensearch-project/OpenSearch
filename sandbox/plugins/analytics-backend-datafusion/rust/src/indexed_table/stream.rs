/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unified streaming execution for indexed parquet reads.
//!
//! One `IndexedExec` and `IndexedStream` for all paths. Parameterized by
//! `Arc<dyn RowGroupBitsetSource>`; the streaming loop is identical regardless
//! of which evaluator produces the bitset.
//!
//! # Per-RG streaming with prefetch overlap
//!
//! - `IndexReader` runs `evaluator.prefetch_rg(rg)` in a background task.
//! - While that's running, `IndexedStream` polls the current RG's parquet
//!   stream for record batches.
//! - When the parquet stream for the current RG finishes, the prefetched
//!   next-RG bitset is ready (or we wait briefly for it).
//!
//! # Post-decode mask (multi-filter tree path only)
//!
//! If `evaluator.on_batch_mask()` returns `Some(mask)`, we apply it via
//! `filter_record_batch`. If it returns `None` (single-collector path:
//! DataFusion's own predicate pushdown filtered during decode), we emit
//! the batch as-is.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, BooleanArray, UInt64Array};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use datafusion_common::DataFusionError;
use futures::{Future, Stream};
use tokio::sync::oneshot;

use super::eval::{PrefetchedRg, RowGroupBitsetSource};
use super::metrics::StreamMetrics;
use super::parquet_bridge::{self, RowGroupStreamConfig};
use super::row_selection::{build_mask, build_row_selection_with_min_skip_run, PositionMap};
use crate::datafusion_query_config::DatafusionQueryConfig;
use datafusion::physical_plan::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use std::time::{Duration, Instant};

/// Row group metadata.
#[derive(Debug, Clone)]
pub struct RowGroupInfo {
    pub index: usize,
    pub first_row: i64,
    pub num_rows: i64,
}


/// Test-only override for the per-RG `min_skip_run` selectivity heuristic.
/// `IndexedStream` normally picks `min_skip_run` from candidate
/// selectivity; setting `force_strategy` to one of these variants pins the
/// choice so tests can exercise either extreme.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FilterStrategy {
    /// Force row-granular selection (`min_skip_run = 1`).
    RowSelection,
    /// Force a single whole-RG select (`min_skip_run > rg_num_rows`).
    BooleanMask,
}

// ── Prefetched Row Group ─────────────────────────────────────────────

struct PrefetchedRowGroup {
    rg: RowGroupInfo,
    prefetched: PrefetchedRg,
}

type PrefetchResult = std::result::Result<Option<PrefetchedRowGroup>, String>;
type PrefetchHandle = oneshot::Receiver<PrefetchResult>;

// ── IndexReader (drives the evaluator RG-by-RG with prefetch overlap) ──

struct IndexReader {
    evaluator: Arc<dyn RowGroupBitsetSource>,
    row_groups: Vec<RowGroupInfo>,
    current_rg_idx: usize,
    pending_prefetch: Option<PrefetchHandle>,
    cached_result: Option<PrefetchResult>,
    doc_range: Option<(i32, i32)>,
    /// Counted once per RG whose prefetch returned `None` (candidate
    /// bitmap empty → RG skipped without a parquet read). Handle is
    /// cloned from the stream's `PartitionMetrics`.
    rg_skipped: Option<datafusion::physical_plan::metrics::Count>,
    /// Time the poll thread spent in `Poll::Pending` on the prefetch
    /// receiver — idle wall-clock attributable to slow Lucene.
    prefetch_wait_time: Option<datafusion::physical_plan::metrics::Time>,
    /// Count of times we hit `Poll::Pending` on the prefetch receiver.
    prefetch_wait_count: Option<datafusion::physical_plan::metrics::Count>,
    /// Wall-clock timestamp when the current pending prefetch was first
    /// polled (and returned Pending). Used to attribute wait time when
    /// the receiver eventually resolves.
    pending_since: Option<Instant>,
}

impl IndexReader {
    fn new(
        evaluator: Arc<dyn RowGroupBitsetSource>,
        row_groups: Vec<RowGroupInfo>,
        doc_range: Option<(i32, i32)>,
        rg_skipped: Option<datafusion::physical_plan::metrics::Count>,
        prefetch_wait_time: Option<datafusion::physical_plan::metrics::Time>,
        prefetch_wait_count: Option<datafusion::physical_plan::metrics::Count>,
    ) -> Self {
        Self {
            evaluator,
            row_groups,
            current_rg_idx: 0,
            pending_prefetch: None,
            cached_result: None,
            doc_range,
            rg_skipped,
            prefetch_wait_time,
            prefetch_wait_count,
            pending_since: None,
        }
    }

    fn fetch_row_group(
        evaluator: &Arc<dyn RowGroupBitsetSource>,
        row_groups: &[RowGroupInfo],
        rg_idx: usize,
        doc_range: Option<(i32, i32)>,
    ) -> std::result::Result<Option<PrefetchedRowGroup>, String> {
        if rg_idx >= row_groups.len() {
            return Ok(None);
        }
        let rg = row_groups[rg_idx].clone();
        let mut min_doc = rg.first_row as i32;
        let mut max_doc = (rg.first_row + rg.num_rows) as i32;
        if let Some((range_min, range_max)) = doc_range {
            min_doc = min_doc.max(range_min);
            max_doc = max_doc.min(range_max);
            if min_doc >= max_doc {
                return Ok(None);
            }
        }
        match evaluator.prefetch_rg(&rg, min_doc, max_doc)? {
            None => Ok(None),
            Some(prefetched) => Ok(Some(PrefetchedRowGroup { rg, prefetched })),
        }
    }

    fn start_prefetch(&mut self, rg_idx: usize) {
        if rg_idx >= self.row_groups.len() {
            return;
        }
        let evaluator = Arc::clone(&self.evaluator);
        let row_groups = self.row_groups.clone();
        let doc_range = self.doc_range;
        let (tx, rx) = oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let _ = tx.send(Self::fetch_row_group(
                &evaluator,
                &row_groups,
                rg_idx,
                doc_range,
            ));
        });
        self.pending_prefetch = Some(rx);
    }

    fn poll_next_row_group(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Option<PrefetchedRowGroup>, DataFusionError>> {
        loop {
            if self.current_rg_idx >= self.row_groups.len() {
                return Poll::Ready(Ok(None));
            }
            if let Some(result) = self.cached_result.take() {
                self.current_rg_idx += 1;
                self.start_prefetch(self.current_rg_idx);
                match result {
                    Ok(Some(p)) => return Poll::Ready(Ok(Some(p))),
                    Ok(None) => {
                        // RG had no candidates — skipped without a
                        // parquet read. Count for EXPLAIN ANALYZE.
                        if let Some(ref c) = self.rg_skipped {
                            c.add(1);
                        }
                        continue;
                    }
                    Err(e) => return Poll::Ready(Err(DataFusionError::External(e.into()))),
                }
            }
            if let Some(ref mut rx) = self.pending_prefetch {
                match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(result)) => {
                        // If we had parked on this receiver, account the
                        // elapsed wall-clock as prefetch_wait_time.
                        if let Some(started) = self.pending_since.take() {
                            if let Some(ref t) = self.prefetch_wait_time {
                                t.add_duration(started.elapsed());
                            }
                        }
                        self.pending_prefetch = None;
                        self.cached_result = Some(result);
                        continue;
                    }
                    Poll::Ready(Err(_)) => {
                        self.pending_prefetch = None;
                        self.pending_since = None;
                        self.start_prefetch(self.current_rg_idx);
                        return Poll::Pending;
                    }
                    Poll::Pending => {
                        // First time we see Pending for this prefetch →
                        // start the wait-clock.
                        if self.pending_since.is_none() {
                            self.pending_since = Some(Instant::now());
                            if let Some(ref c) = self.prefetch_wait_count {
                                c.add(1);
                            }
                        }
                        return Poll::Pending;
                    }
                }
            }
            self.start_prefetch(self.current_rg_idx);
            return Poll::Pending;
        }
    }

    fn init_prefetch(&mut self) {
        self.start_prefetch(0);
    }
}

// ── IndexedExec ──────────────────────────────────────────────────────

/// Execution plan for a single segment chunk (1+ row groups from one segment).
/// Streams RGs one at a time with prefetch overlap.
pub struct IndexedExec {
    pub(crate) schema: SchemaRef,
    pub(crate) full_schema: SchemaRef,
    pub(crate) object_path: object_store::path::Path,
    pub(crate) file_size: u64,
    pub(crate) store: Arc<dyn object_store::ObjectStore>,
    pub(crate) store_url: datafusion::execution::object_store::ObjectStoreUrl,
    pub(crate) row_groups: Vec<RowGroupInfo>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) properties: Arc<PlanProperties>,
    pub(crate) metadata: Arc<ParquetMetaData>,
    pub(crate) predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Pluggable bitset source (SingleCollector or RustTree).
    pub(crate) evaluator: std::sync::Mutex<Option<Arc<dyn RowGroupBitsetSource>>>,
    pub(crate) doc_range: Option<(i32, i32)>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) stream_metrics: StreamMetrics,
    /// Query-scoped tunables. Shared by Arc across IndexedExec instances
    /// from the same query; read once per RG into local fields inside
    /// `IndexedStream` so the hot path never touches the Arc.
    pub(crate) query_config: Arc<DatafusionQueryConfig>,
    /// Cumulative row offset for this segment within the shard.
    pub(crate) global_base: u64,
    /// When true, the `___row_id` column is computed from position instead of read.
    pub(crate) emit_row_ids: bool,
    /// Index in the OUTPUT schema where computed `___row_id` should be inserted.
    /// `None` when `emit_row_ids=false` or `___row_id` is not in projection.
    pub(crate) row_id_output_index: Option<usize>,
}

impl fmt::Debug for IndexedExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedExec")
            .field("row_groups", &self.row_groups.len())
            .field("has_predicate", &self.predicate.is_some())
            .finish()
    }
}

impl DisplayAs for IndexedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let total_rows: i64 = self.row_groups.iter().map(|rg| rg.num_rows).sum();
        let doc_range_str = match self.doc_range {
            Some((min, max)) => format!(", doc_range=[{}, {})", min, max),
            None => String::new(),
        };
        write!(
            f,
            "IndexedExec: rg={}, total_rows={}, predicate={}{}",
            self.row_groups.len(),
            total_rows,
            self.predicate.is_some(),
            doc_range_str,
        )
    }
}

impl ExecutionPlan for IndexedExec {
    fn name(&self) -> &str {
        "IndexedExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let evaluator = {
            let mut guard = self.evaluator.lock().unwrap();
            guard
                .take()
                .ok_or_else(|| DataFusionError::Internal("evaluator already consumed".into()))?
        };
        let index_reader = IndexReader::new(
            evaluator,
            self.row_groups.clone(),
            self.doc_range,
            self.stream_metrics.rg_skipped.clone(),
            self.stream_metrics.prefetch_wait_time.clone(),
            self.stream_metrics.prefetch_wait_count.clone(),
        );
        Ok(Box::pin(IndexedStream::new(
            self.schema.clone(),
            self.full_schema.clone(),
            self.object_path.clone(),
            self.file_size,
            Arc::clone(&self.store),
            self.store_url.clone(),
            index_reader,
            self.projection.clone(),
            Arc::clone(&self.metadata),
            self.predicate.clone(),
            self.stream_metrics.clone(),
            self.query_config.force_pushdown,
            self.query_config.force_strategy,
            self.query_config.min_skip_run_default,
            self.query_config.min_skip_run_selectivity_threshold,
            self.query_config.indexed_pushdown_filters,
            self.query_config.batch_size,
            self.global_base,
            self.emit_row_ids,
            self.row_id_output_index,
        )))
    }
}

// Indexed streams - Per segment stream

struct IndexedStream {
    schema: SchemaRef,
    full_schema: SchemaRef,
    object_path: object_store::path::Path,
    file_size: u64,
    store: Arc<dyn object_store::ObjectStore>,
    store_url: datafusion::execution::object_store::ObjectStoreUrl,
    index_reader: IndexReader,
    projection: Option<Vec<usize>>,
    current_stream: Option<SendableRecordBatchStream>,
    current_inner_plan: Option<Arc<dyn ExecutionPlan>>,
    current_mask: Option<BooleanArray>,
    current_rg_first_row: i64,
    /// Per-RG state carried from `PrefetchedRg.context` so `on_batch_mask`
    /// can reach into it during refinement (used by the multi-filter tree
    /// path to access per-leaf bitmaps keyed by `Arc::as_ptr` identity).
    current_rg_context: Option<Box<dyn Any + Send + Sync>>,
    /// Per-RG map from delivered batch-row index to RG-relative position.
    /// Used by `on_batch_mask` to translate the batch-coordinate under
    /// block-granular `RowSelection`. Rebuilt each RG from the selection
    /// we handed to parquet.
    current_position_map: Option<PositionMap>,
    mask_offset: usize,
    batch_offset: usize,
    finished: bool,
    metadata: Arc<ParquetMetaData>,
    predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    initialized: bool,
    metrics: StreamMetrics,
    force_pushdown: Option<bool>,
    force_strategy: Option<FilterStrategy>,
    /// Baseline `min_skip_run` used when neither selectivity nor
    /// `force_strategy` drives the choice. Extracted once from
    /// `DatafusionQueryConfig` so the hot path reads a local `usize`.
    min_skip_run_default: usize,
    /// Below this candidate selectivity, pin `min_skip_run = 1`
    /// (row-granular selection). Same hot-path discipline as above.
    min_skip_run_selectivity_threshold: f64,
    /// Whether to ask parquet to apply residual predicates during decode.
    /// `force_pushdown` still takes priority when set.
    indexed_pushdown_filters: bool,
    evaluator: Arc<dyn RowGroupBitsetSource>,
    /// Output coalescer — combines small post-filter batches up to
    /// `target_batch_size` so downstream operators see fewer, larger
    /// batches (matching FilterExec's behaviour). Post-filter batches
    /// are fed in via `push_batch`; completed batches are drained via
    /// `next_completed_batch`.
    batch_coalescer: LimitedBatchCoalescer,
    /// Upstream delivered `None` (all RGs consumed). We still need to
    /// call `finish()` on the coalescer and drain it before returning
    /// `Ready(None)` ourselves.
    upstream_done: bool,
    /// `finish()` has been called on the coalescer. Used to prevent
    /// calling it twice (assert panic) and to signal "no more input
    /// will arrive; drain remaining completed batches."
    coalescer_finished: bool,
    /// Cumulative row offset for this segment within the shard.
    global_base: u64,
    /// When true, the `___row_id` column is computed from position.
    emit_row_ids: bool,
    /// Index in the output schema where computed `___row_id` is inserted.
    row_id_output_index: Option<usize>,
}

impl IndexedStream {
    #[allow(clippy::too_many_arguments)]
    fn new(
        schema: SchemaRef,
        full_schema: SchemaRef,
        object_path: object_store::path::Path,
        file_size: u64,
        store: Arc<dyn object_store::ObjectStore>,
        store_url: datafusion::execution::object_store::ObjectStoreUrl,
        index_reader: IndexReader,
        projection: Option<Vec<usize>>,
        metadata: Arc<ParquetMetaData>,
        predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        metrics: StreamMetrics,
        force_pushdown: Option<bool>,
        force_strategy: Option<FilterStrategy>,
        min_skip_run_default: usize,
        min_skip_run_selectivity_threshold: f64,
        indexed_pushdown_filters: bool,
        target_batch_size: usize,
        global_base: u64,
        emit_row_ids: bool,
        row_id_output_index: Option<usize>,
    ) -> Self {
        let evaluator = Arc::clone(&index_reader.evaluator);
        let batch_coalescer =
            LimitedBatchCoalescer::new(schema.clone(), target_batch_size, None);
        Self {
            schema,
            full_schema,
            object_path,
            file_size,
            store,
            store_url,
            index_reader,
            projection,
            current_stream: None,
            current_inner_plan: None,
            current_mask: None,
            current_rg_first_row: 0,
            current_rg_context: None,
            current_position_map: None,
            mask_offset: 0,
            batch_offset: 0,
            finished: false,
            metadata,
            predicate,
            initialized: false,
            metrics,
            force_pushdown,
            force_strategy,
            min_skip_run_default,
            min_skip_run_selectivity_threshold,
            indexed_pushdown_filters,
            evaluator,
            batch_coalescer,
            upstream_done: false,
            coalescer_finished: false,
            global_base,
            emit_row_ids,
            row_id_output_index,
        }
    }

    fn bridge_config(&self) -> RowGroupStreamConfig {
        RowGroupStreamConfig {
            file_path: self.object_path.to_string(),
            file_size: self.file_size,
            store: Arc::clone(&self.store),
            store_url: self.store_url.clone(),
            full_schema: self.full_schema.clone(),
            metadata: Arc::clone(&self.metadata),
            projection: self.projection.clone(),
            predicate: self.predicate.clone(),
        }
    }

    fn create_row_selection_stream(
        &self,
        rg: &RowGroupInfo,
        selection: RowSelection,
        push_predicate: bool,
    ) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
        parquet_bridge::create_row_selection_stream(
            &self.bridge_config(),
            rg.index,
            selection,
            push_predicate,
        )
    }

    /// Take one parquet-delivered batch, apply candidate + refinement
    /// masks, strip predicate columns to match output schema, and return
    /// the filtered batch ready for the coalescer. Returns a zero-row
    /// batch if no rows survived (callers filter those out before
    /// push_batch). Advances per-batch offsets (mask/batch) in lockstep.
    fn finalize_batch(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let batch_len = batch.num_rows();

        // Ask the evaluator for a refinement-stage mask on the UNFILTERED
        // batch. With BooleanMask strategy, the batch contains all RG rows
        // for this chunk, so batch_offset is row-index-within-RG and the
        // refinement mask aligns one-to-one with the batch rows.
        //
        // Two cases:
        //   (A) `on_batch_mask` returns Some(m). The evaluator owns the final
        //       answer — apply `m` exclusively. Any `current_mask` from
        //       the candidate stage is a superset; the refinement mask is
        //       the exact result, and applying both would double-filter with
        //       misaligned indices. Ignore `current_mask`.
        //   (B) `on_batch_mask` returns None (single-collector path). Use
        //       `current_mask` from the candidate stage (or DataFusion
        //       pushdown if that's None too).

        static UNIT: () = ();
        let rg_state: &dyn std::any::Any = match &self.current_rg_context {
            Some(ctx) => ctx.as_ref(),
            None => &UNIT,
        };
        let empty_pos_map =
            PositionMap::from_selection(&RowSelection::from(Vec::<RowSelector>::new()));
        let position_map = self.current_position_map.as_ref().unwrap_or(&empty_pos_map);

        let t_on_batch = Instant::now();
        let eval_mask = self
            .evaluator
            .on_batch_mask(
                rg_state,
                self.current_rg_first_row,
                position_map,
                self.batch_offset,
                batch_len,
                &batch,
            )
            .map_err(|e| DataFusionError::External(e.into()))?;
        if let Some(ref t) = self.metrics.on_batch_mask_time {
            t.add_duration(t_on_batch.elapsed());
        }

        // Capture position info BEFORE mask is consumed (needed for row ID computation).
        let row_id_ctx = if self.row_id_output_index.is_some() {
            Some(super::fetch_row_id::RowIdContext {
                batch_offset: self.batch_offset,
                position_map: self.current_position_map.as_ref().cloned(),
                base: self.global_base + self.current_rg_first_row as u64,
                eval_mask: eval_mask.clone(),
            })
        } else {
            None
        };

        let output = match eval_mask {
            Some(mask) => {
                self.mask_offset += batch_len;
                self.batch_offset += batch_len;
                let t_filter = Instant::now();
                let filtered = filter_record_batch(&batch, &mask)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                if let Some(ref t) = self.metrics.filter_record_batch_time {
                    t.add_duration(t_filter.elapsed());
                }
                filtered
            }
            None => {
                let current = if let Some(ref mask) = self.current_mask {
                    let t_slice = Instant::now();
                    let mask_slice = mask.slice(self.mask_offset, batch_len);
                    let mask_slice = mask_slice
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("BooleanArray.slice must remain BooleanArray");
                    if let Some(ref t) = self.metrics.mask_slice_time {
                        t.add_duration(t_slice.elapsed());
                    }
                    self.mask_offset += batch_len;
                    let t_filter = Instant::now();
                    let filtered = filter_record_batch(&batch, mask_slice)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    if let Some(ref t) = self.metrics.filter_record_batch_time {
                        t.add_duration(t_filter.elapsed());
                    }
                    filtered
                } else {
                    batch
                };
                self.batch_offset += batch_len;
                current
            }
        };

        // Strip extra predicate columns and inject computed __row_id__.
        let t_proj = Instant::now();
        let output = if let Some(row_id_idx) = self.row_id_output_index {
            let ctx = row_id_ctx.unwrap();
            let mask_offset_before = self.mask_offset.saturating_sub(batch_len);
            super::fetch_row_id::inject_row_ids(
                &output,
                &ctx,
                batch_len,
                self.current_mask.as_ref(),
                mask_offset_before,
                row_id_idx,
                &self.schema,
            )?
        } else if output.num_columns() > self.schema.fields().len() {
            let n = self.schema.fields().len();
            if n == 0 {
                RecordBatch::try_new_with_options(
                    self.schema.clone(),
                    vec![],
                    &datafusion::arrow::record_batch::RecordBatchOptions::new()
                        .with_row_count(Some(output.num_rows())),
                )?
            } else {
                let indices: Vec<usize> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| output.schema().index_of(f.name()).unwrap_or(0))
                    .collect();
                output.project(&indices)?
            }
        } else {
            output
        };
        if let Some(ref t) = self.metrics.projection_fixup_time {
            t.add_duration(t_proj.elapsed());
        }

        Ok(output)
    }
}

impl Stream for IndexedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Manual timer for `elapsed_compute`: total wall time spent
        // inside this poll. Attributed to the operator for EXPLAIN
        // ANALYZE, separate from `index_time` / `parquet_time` which
        // time downstream work.
        let poll_start = Instant::now();

        if !self.initialized {
            self.index_reader.init_prefetch();
            self.initialized = true;
        }

        let result = self.as_mut().poll_inner(cx);

        if let Some(ref t) = self.metrics.elapsed_compute {
            t.add_duration(poll_start.elapsed());
        }
        result
    }
}

impl IndexedStream {
    fn poll_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            // 1. Drain any completed batch from the coalescer first.
            if let Some(batch) = self.batch_coalescer.next_completed_batch() {
                if let Some(ref counter) = self.metrics.output_rows {
                    counter.add(batch.num_rows());
                }
                if let Some(ref counter) = self.metrics.batches_produced {
                    counter.add(1);
                }
                return Poll::Ready(Some(Ok(batch)));
            }

            // 2. If upstream is done and coalescer has drained, we're done.
            if self.coalescer_finished && self.batch_coalescer.is_empty() {
                return Poll::Ready(None);
            }

            // 3. If upstream signalled done and we haven't finished the
            //    coalescer yet, finish it so it flushes its final buffered
            //    batch. Loop to drain via next_completed_batch().
            if self.upstream_done && !self.coalescer_finished {
                if let Err(e) = self.batch_coalescer.finish() {
                    return Poll::Ready(Some(Err(e)));
                }
                self.coalescer_finished = true;
                continue;
            }

            // If coalescer is finished but wasn't drained in step 1, the
            // top-of-loop `is_empty` check ends it on the next turn.
            if self.coalescer_finished {
                // Unreachable in practice — step 1 already drained or
                // step 2 already returned. Defensive.
                return Poll::Ready(None);
            }

            // 4. Pull the next filtered batch from upstream (parquet stream
            //    + evaluator), push into coalescer, loop.
            // Poll current stream
            if let Some(ref mut stream) = self.current_stream {
                let t_poll = Instant::now();
                let poll_result = Pin::new(stream).poll_next(cx);
                if let Some(ref t) = self.metrics.parquet_poll_time {
                    t.add_duration(t_poll.elapsed());
                }
                match poll_result {
                    Poll::Ready(Some(Ok(batch))) if batch.num_rows() > 0 => {
                        if let Some(ref c) = self.metrics.parquet_batches_received {
                            c.add(1);
                        }
                        let filtered = match self.as_mut().finalize_batch(batch) {
                            Ok(b) => b,
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        };
                        if filtered.num_rows() == 0 {
                            continue;
                        }
                        // Push into coalescer under a timer.
                        let t0 = Instant::now();
                        let status = self.batch_coalescer.push_batch(filtered);
                        if let Some(ref t) = self.metrics.coalesce_time {
                            t.add_duration(t0.elapsed());
                        }
                        if let Some(ref c) = self.metrics.batches_pre_coalesce {
                            c.add(1);
                        }
                        match status {
                            Ok(PushBatchStatus::Continue) => continue,
                            Ok(PushBatchStatus::LimitReached) => {
                                if !self.coalescer_finished {
                                    if let Err(e) = self.batch_coalescer.finish() {
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                    self.coalescer_finished = true;
                                }
                                self.upstream_done = true;
                                continue;
                            }
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                    Poll::Ready(Some(Ok(_))) => continue,
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        // Stream finished — collect inner parquet metrics
                        if let Some(inner_plan) = self.current_inner_plan.take() {
                            if let Some(inner_metrics) = inner_plan.metrics() {
                                if let Some(ref acc) = self.metrics.inner_parquet_metrics {
                                    if let Ok(mut vec) = acc.lock() {
                                        vec.push(inner_metrics);
                                    }
                                }
                            }
                        }
                        self.current_stream = None;
                        self.current_mask = None;
                        self.current_rg_context = None;
                        self.current_position_map = None;
                        self.mask_offset = 0;
                        self.batch_offset = 0;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            if self.finished {
                // Upstream fully consumed; let the coalescer flush.
                self.upstream_done = true;
                continue;
            }

            // Poll for next row group
            match self.index_reader.poll_next_row_group(cx) {
                Poll::Ready(Ok(Some(prefetched))) => {
                    let rg = prefetched.rg;
                    let candidates = prefetched.prefetched.candidates;
                    let prefetch_mask_buffer = prefetched.prefetched.mask_buffer;

                    if let Some(ref timer) = self.metrics.index_time {
                        timer.add_duration(Duration::from_nanos(prefetched.prefetched.eval_nanos));
                    }
                    if let Some(ref counter) = self.metrics.rows_matched {
                        counter.add(candidates.len() as usize);
                    }
                    if let Some(ref counter) = self.metrics.rows_pruned {
                        // Rows in this RG that the candidate stage
                        // dropped (either via Collector intersection or
                        // page-level pruning). `rg.num_rows - matched`.
                        let pruned =
                            (rg.num_rows as usize).saturating_sub(candidates.len() as usize);
                        counter.add(pruned);
                    }
                    if let Some(ref counter) = self.metrics.rg_processed {
                        counter.add(1);
                    }

                    self.current_rg_first_row = rg.first_row;
                    // Carried through to finalize_batch so the multi-filter
                    // tree path's on_batch_mask can reach into candidate-stage
                    // per-RG state.
                    self.current_rg_context = Some(prefetched.prefetched.context);
                    self.batch_offset = 0;

                    // Decide min_skip_run for this RG.
                    //
                    // - `force_strategy = RowSelection`: row-granular
                    //   (min_skip_run = 1) — "sparse" path.
                    // - `force_strategy = BooleanMask`: disable skipping
                    //   (min_skip_run > rg.num_rows) — full scan.
                    // - otherwise: pick based on selectivity. At low
                    //   selectivity every gap is worth skipping (1); at
                    //   higher selectivity noisy short gaps would explode
                    //   the selector Vec, so absorb anything smaller than
                    //   the default block size.
                    let selectivity = candidates.len() as f64 / rg.num_rows as f64;
                    let min_skip_run = match self.force_strategy {
                        Some(FilterStrategy::RowSelection) => 1,
                        Some(FilterStrategy::BooleanMask) => rg.num_rows as usize + 1,
                        None => {
                            if selectivity < self.min_skip_run_selectivity_threshold {
                                1
                            } else {
                                self.min_skip_run_default
                            }
                        }
                    };

                    // Metrics: track which regime we landed in, using the
                    // same counters as before so `EXPLAIN ANALYZE` output
                    // stays comparable.
                    if min_skip_run == 1 {
                        if let Some(ref counter) = self.metrics.min_skip_run_row_granular {
                            counter.add(1);
                        }
                    } else if let Some(ref counter) = self.metrics.min_skip_run_block_granular {
                        counter.add(1);
                    }

                    let selection = build_row_selection_with_min_skip_run(
                        &candidates,
                        rg.num_rows as usize,
                        min_skip_run,
                    );
                    // Share the bitmap between PositionMap (under
                    // row-granular regime) and build_mask without cloning
                    // the underlying data.
                    let candidates = Arc::new(candidates);
                    let position_map = PositionMap::from_candidates_with_selection(
                        Arc::clone(&candidates),
                        &selection,
                        min_skip_run,
                    );
                    // Metric: record which PositionMap variant this RG
                    // landed in. Useful for tuning min_skip_run and
                    // understanding per-query memory profiles.
                    match &position_map {
                        PositionMap::Identity { .. } => {
                            if let Some(ref c) = self.metrics.position_map_identity {
                                c.add(1);
                            }
                        }
                        PositionMap::Bitmap { .. } => {
                            if let Some(ref c) = self.metrics.position_map_bitmap {
                                c.add(1);
                            }
                        }
                        PositionMap::Runs { .. } => {
                            if let Some(ref c) = self.metrics.position_map_runs {
                                c.add(1);
                            }
                        }
                    }

                    let t_plan = Instant::now();
                    // Pushdown decision:
                    //
                    // Row-granular (min_skip_run == 1): RowSelection
                    // already narrowed to candidate rows; parquet's
                    // `with_predicate` applies the residual in lockstep
                    // with the decode. Delivered rows = candidate ∧
                    // residual = exact output. Pushdown is ON.
                    //
                    // Block-granular (min_skip_run > 1): RowSelection
                    // is coalesced. If the stream will build
                    // `current_mask` over delivered rows, or the
                    // evaluator's `on_batch_mask` will look up positions
                    // via PositionMap, pushdown would drop rows
                    // mid-decode and misalign those indices. Pushdown
                    // OFF; the evaluator applies the residual
                    // post-decode.
                    //
                    // `forbid_parquet_pushdown()` is a blanket opt-out
                    // that overrides the row-granular path too — used by
                    // BitmapTreeEvaluator because its `on_batch_mask`
                    // uses PositionMap on Collector leaves regardless of
                    // strategy, and because the outer FilterExec is
                    // dropped (supports_filters_pushdown = Exact) so
                    // there's no safety net if pushdown misbehaves on a
                    // UDF-containing predicate.
                    let base_push = self.force_pushdown.unwrap_or(self.indexed_pushdown_filters);
                    let alignment_risk = min_skip_run != 1 && self.evaluator.needs_row_mask();
                    let push =
                        base_push && !alignment_risk && !self.evaluator.forbid_parquet_pushdown();

                    match self.create_row_selection_stream(&rg, selection, push) {
                        Ok((stream, plan)) => {
                            if let Some(ref timer) = self.metrics.parquet_time {
                                timer.add_duration(t_plan.elapsed());
                            }
                            self.current_stream = Some(stream);
                            self.current_inner_plan = Some(plan);
                            // Under row-granular (min_skip_run == 1) every
                            // delivered row is by construction a candidate,
                            // so the mask would be all-true — skip building
                            // it. Under block/full regimes, build the mask
                            // only if the evaluator consumes it.
                            self.current_mask = if min_skip_run == 1 {
                                None
                            } else if self.evaluator.needs_row_mask() {
                                let t_build = Instant::now();
                                let m = if let Some(buf) = prefetch_mask_buffer.as_ref() {
                                    // Fast path: evaluator already produced
                                    // the packed bits. Wrap as BooleanArray
                                    // with zero per-RG work (just Arc clone
                                    // of the Buffer).
                                    let bb = datafusion::arrow::buffer::BooleanBuffer::new(
                                        buf.clone(),
                                        0,
                                        rg.num_rows as usize,
                                    );
                                    BooleanArray::new(bb, None)
                                } else {
                                    build_mask(&candidates, &position_map)
                                };
                                if let Some(ref t) = self.metrics.build_mask_time {
                                    t.add_duration(t_build.elapsed());
                                }
                                Some(m)
                            } else {
                                None
                            };
                            self.mask_offset = 0;
                            self.current_position_map = Some(position_map);
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                Poll::Ready(Ok(None)) => {
                    self.finished = true;
                    self.upstream_done = true;
                    continue;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for IndexedStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
