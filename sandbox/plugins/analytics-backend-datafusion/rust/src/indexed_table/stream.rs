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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
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
use super::row_selection::{
    build_mask, build_row_selection_with_min_skip_run, PositionMap,
};

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
}

impl IndexReader {
    fn new(
        evaluator: Arc<dyn RowGroupBitsetSource>,
        row_groups: Vec<RowGroupInfo>,
        doc_range: Option<(i32, i32)>,
    ) -> Self {
        Self {
            evaluator,
            row_groups,
            current_rg_idx: 0,
            pending_prefetch: None,
            cached_result: None,
            doc_range,
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
            let _ = tx.send(Self::fetch_row_group(&evaluator, &row_groups, rg_idx, doc_range));
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
                    Ok(None) => continue,
                    Err(e) => return Poll::Ready(Err(DataFusionError::External(e.into()))),
                }
            }
            if let Some(ref mut rx) = self.pending_prefetch {
                match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(result)) => {
                        self.pending_prefetch = None;
                        self.cached_result = Some(result);
                        continue;
                    }
                    Poll::Ready(Err(_)) => {
                        self.pending_prefetch = None;
                        self.start_prefetch(self.current_rg_idx);
                        return Poll::Pending;
                    }
                    Poll::Pending => return Poll::Pending,
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
    pub(crate) properties: PlanProperties,
    pub(crate) metadata: Arc<ParquetMetaData>,
    pub(crate) predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Pluggable bitset source (SingleCollector or RustTree).
    pub(crate) evaluator: std::sync::Mutex<Option<Arc<dyn RowGroupBitsetSource>>>,
    pub(crate) doc_range: Option<(i32, i32)>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) stream_metrics: StreamMetrics,
    pub(crate) force_pushdown: Option<bool>,
    pub(crate) force_strategy: Option<FilterStrategy>,
    /// Query-scoped tunables. Shared by Arc across IndexedExec instances
    /// from the same query; read once per RG into local fields inside
    /// `IndexedStream` so the hot path never touches the Arc.
    pub(crate) query_config: Arc<crate::datafusion_query_config::DatafusionQueryConfig>,
}

impl std::fmt::Debug for IndexedExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexedExec")
            .field("row_groups", &self.row_groups.len())
            .field("has_predicate", &self.predicate.is_some())
            .finish()
    }
}

impl DisplayAs for IndexedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
    fn properties(&self) -> &PlanProperties {
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
        let index_reader =
            IndexReader::new(evaluator, self.row_groups.clone(), self.doc_range);
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
            self.force_pushdown,
            self.force_strategy,
            self.query_config.min_skip_run_default,
            self.query_config.min_skip_run_selectivity_threshold,
            self.query_config.indexed_pushdown_filters,
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
    ) -> Self {
        let evaluator = Arc::clone(&index_reader.evaluator);
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
    ) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
        // Whether to push the residual predicate into parquet decode. At
        // narrow selectivity (row-granular RowSelection), decoder-time
        // predicate application is cheap and occasionally lets parquet skip
        // whole column chunks. For block-granular selection (higher
        // selectivity), pushdown would double-evaluate the predicate
        // (FilterExec re-runs it because we report `Inexact`), so we
        // disable it. Tests can override via `force_pushdown`.
        let push = self.force_pushdown.unwrap_or(self.indexed_pushdown_filters);
        parquet_bridge::create_row_selection_stream(&self.bridge_config(), rg.index, selection, push)
    }

    fn finalize_batch(&mut self, batch: RecordBatch) -> Poll<Option<Result<RecordBatch>>> {
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
        // Unwrap position_map for this RG. Always set alongside
        // current_stream by poll_next; use an empty default on the defensive
        // path.
        let empty_pos_map = PositionMap::from_selection(&RowSelection::from(
            Vec::<RowSelector>::new(),
        ));
        let position_map = self
            .current_position_map
            .as_ref()
            .unwrap_or(&empty_pos_map);

        let eval_mask = match self.evaluator.on_batch_mask(
            rg_state,
            self.current_rg_first_row,
            position_map,
            self.batch_offset,
            batch_len,
            &batch,
        ) {
            Ok(m) => m,
            Err(e) => return Poll::Ready(Some(Err(DataFusionError::External(e.into())))),
        };

        let output = match eval_mask {
            Some(mask) => {
                // Case (A): refinement mask is authoritative. Skip current_mask.
                self.mask_offset += batch_len;
                self.batch_offset += batch_len;
                match filter_record_batch(&batch, &mask) {
                    Ok(filtered) => filtered,
                    Err(e) => {
                        return Poll::Ready(Some(Err(DataFusionError::ArrowError(
                            Box::new(e),
                            None,
                        ))));
                    }
                }
            }
            None => {
                // Case (B): apply current_mask if present (single-collector
                // path with BooleanMask strategy), else emit as-is
                // (single-collector path with RowSelection strategy where
                // parquet decode already filtered, or a fall-through where
                // neither candidate nor refinement mask is produced).
                let current = if let Some(ref mask) = self.current_mask {
                    let mask_slice = mask.slice(self.mask_offset, batch_len);
                    let mask_slice = mask_slice
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("BooleanArray.slice must remain BooleanArray");
                    self.mask_offset += batch_len;
                    match filter_record_batch(&batch, mask_slice) {
                        Ok(filtered) => filtered,
                        Err(e) => {
                            return Poll::Ready(Some(Err(DataFusionError::ArrowError(
                                Box::new(e),
                                None,
                            ))));
                        }
                    }
                } else {
                    batch
                };
                self.batch_offset += batch_len;
                current
            }
        };

        if output.num_rows() > 0 {
            if let Some(ref counter) = self.metrics.output_rows {
                counter.add(output.num_rows());
            }
        }
        Poll::Ready(Some(Ok(output)))
    }
}

impl Stream for IndexedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.initialized {
            self.index_reader.init_prefetch();
            self.initialized = true;
        }

        loop {
            // Poll current stream
            if let Some(ref mut stream) = self.current_stream {
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) if batch.num_rows() > 0 => {
                        let result = self.as_mut().finalize_batch(batch);
                        match result {
                            Poll::Ready(Some(Ok(b))) if b.num_rows() == 0 => continue,
                            other => return other,
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
                return Poll::Ready(None);
            }

            // Poll for next row group
            match self.index_reader.poll_next_row_group(cx) {
                Poll::Ready(Ok(Some(prefetched))) => {
                    let rg = prefetched.rg;
                    let candidates = prefetched.prefetched.candidates;

                    if let Some(ref timer) = self.metrics.index_time {
                        timer.add_duration(std::time::Duration::from_nanos(
                            prefetched.prefetched.eval_nanos,
                        ));
                    }
                    if let Some(ref counter) = self.metrics.rows_matched {
                        counter.add(candidates.len() as usize);
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
                    let selectivity =
                        candidates.len() as f64 / rg.num_rows as f64;
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
                        if let Some(ref counter) = self.metrics.row_selection_count {
                            counter.add(1);
                        }
                    } else if let Some(ref counter) = self.metrics.boolean_mask_count {
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

                    let t_plan = std::time::Instant::now();
                    match self.create_row_selection_stream(&rg, selection) {
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
                                Some(build_mask(&candidates, &position_map))
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
                    return Poll::Ready(None);
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

