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
use datafusion::arrow::array::builder::BooleanBufferBuilder;
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

/// Row group metadata.
#[derive(Debug, Clone)]
pub struct RowGroupInfo {
    pub index: usize,
    pub first_row: i64,
    pub num_rows: i64,
}

/// Strategy for filtering rows within a row group — chosen per-RG by
/// `IndexedStream` based on selectivity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FilterStrategy {
    /// `RowSelection` during parquet decode — best for <3% selectivity.
    RowSelection,
    /// `BooleanArray` mask after decode — best for ≥3% selectivity.
    BooleanMask,
}

impl FilterStrategy {
    pub fn choose(num_selected: usize, total_rows: i64) -> Self {
        let selectivity = num_selected as f64 / total_rows as f64;
        if selectivity < 0.03 {
            FilterStrategy::RowSelection
        } else {
            FilterStrategy::BooleanMask
        }
    }
}

/// Convert sorted offsets to `RowSelection`.
pub fn offsets_to_row_selection(offsets: &[u64], num_rows: i64) -> RowSelection {
    if offsets.is_empty() {
        return RowSelection::from(vec![RowSelector::skip(num_rows as usize)]);
    }
    let mut selectors = Vec::new();
    let mut pos = 0u64;
    let mut i = 0;
    while i < offsets.len() {
        let start = offsets[i];
        if start > pos {
            selectors.push(RowSelector::skip((start - pos) as usize));
        }
        let mut run = 1usize;
        while i + run < offsets.len() && offsets[i + run] == start + run as u64 {
            run += 1;
        }
        selectors.push(RowSelector::select(run));
        pos = start + run as u64;
        i += run;
    }
    if pos < num_rows as u64 {
        selectors.push(RowSelector::skip((num_rows as u64 - pos) as usize));
    }
    RowSelection::from(selectors)
}

/// Build a BooleanArray mask from matching row-offsets within an RG.
///
/// Uses `BooleanBufferBuilder` — writes bits directly into the packed
/// bitmap instead of materializing a `Vec<bool>` (one byte per row) and
/// scanning it to pack. For a 100k-row RG this is ~8× less memory and
/// avoids the final scan-and-pack that `BooleanArray::from(Vec<bool>)`
/// would do.
fn build_mask(offsets: &[u64], num_rows: i64) -> BooleanArray {
    let n = num_rows as usize;
    let mut builder = BooleanBufferBuilder::new(n);
    builder.append_n(n, false);
    for &offset in offsets {
        if (offset as i64) < num_rows {
            builder.set_bit(offset as usize, true);
        }
    }
    BooleanArray::new(builder.finish(), None)
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
    mask_offset: usize,
    batch_offset: usize,
    finished: bool,
    metadata: Arc<ParquetMetaData>,
    predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    initialized: bool,
    metrics: StreamMetrics,
    force_pushdown: Option<bool>,
    force_strategy: Option<FilterStrategy>,
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
            mask_offset: 0,
            batch_offset: 0,
            finished: false,
            metadata,
            predicate,
            initialized: false,
            metrics,
            force_pushdown,
            force_strategy,
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
        offsets: &[u64],
    ) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
        let selection = offsets_to_row_selection(offsets, rg.num_rows);
        // RowSelection fires only at <3% candidate selectivity — parquet is
        // already decoding very few rows. Pushing the residual predicate
        // into decode is cheap for such a narrow set and occasionally lets
        // parquet skip whole column chunks the residual proves unneeded.
        // Tests use `force_pushdown = Some(false)` to disable when
        // exercising the non-pushdown path.
        let push = self.force_pushdown.unwrap_or(true);
        parquet_bridge::create_row_selection_stream(&self.bridge_config(), rg.index, selection, push)
    }

    fn create_full_scan_stream(
        &self,
        rg: &RowGroupInfo,
    ) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
        parquet_bridge::create_full_scan_stream(&self.bridge_config(), rg.index)
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
        let eval_mask = match self.evaluator.on_batch_mask(
            rg_state,
            self.current_rg_first_row,
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
                    let offsets = prefetched.prefetched.offsets;

                    if let Some(ref timer) = self.metrics.index_time {
                        timer.add_duration(std::time::Duration::from_nanos(
                            prefetched.prefetched.eval_nanos,
                        ));
                    }
                    if let Some(ref counter) = self.metrics.rows_matched {
                        counter.add(offsets.len());
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

                    let strategy = self
                        .force_strategy
                        .unwrap_or_else(|| FilterStrategy::choose(offsets.len(), rg.num_rows));
                    let t_plan = std::time::Instant::now();

                    match strategy {
                        FilterStrategy::RowSelection => {
                            if let Some(ref counter) = self.metrics.row_selection_count {
                                counter.add(1);
                            }
                            match self.create_row_selection_stream(&rg, &offsets) {
                                Ok((stream, plan)) => {
                                    if let Some(ref timer) = self.metrics.parquet_time {
                                        timer.add_duration(t_plan.elapsed());
                                    }
                                    self.current_stream = Some(stream);
                                    self.current_inner_plan = Some(plan);
                                    self.current_mask = None;
                                }
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                        }
                        FilterStrategy::BooleanMask => {
                            if let Some(ref counter) = self.metrics.boolean_mask_count {
                                counter.add(1);
                            }
                            match self.create_full_scan_stream(&rg) {
                                Ok((stream, plan)) => {
                                    if let Some(ref timer) = self.metrics.parquet_time {
                                        timer.add_duration(t_plan.elapsed());
                                    }
                                    self.current_stream = Some(stream);
                                    self.current_inner_plan = Some(plan);
                                    // Skip building the row-mask when the
                                    // evaluator won't use it (e.g. the
                                    // multi-filter tree path: its refinement
                                    // mask is authoritative and `finalize_batch`
                                    // ignores `current_mask` in that branch).
                                    self.current_mask = if self.evaluator.needs_row_mask() {
                                        Some(build_mask(&offsets, rg.num_rows))
                                    } else {
                                        None
                                    };
                                    self.mask_offset = 0;
                                }
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_mask_empty_offsets_produces_all_false() {
        let m = build_mask(&[], 8);
        assert_eq!(m.len(), 8);
        assert_eq!(m.true_count(), 0);
    }

    #[test]
    fn build_mask_sparse_offsets_set_only_named_bits() {
        let m = build_mask(&[0, 3, 7], 8);
        assert_eq!(m.len(), 8);
        let got: Vec<bool> = (0..m.len()).map(|i| m.value(i)).collect();
        assert_eq!(got, vec![true, false, false, true, false, false, false, true]);
    }

    #[test]
    fn build_mask_offsets_outside_range_are_ignored() {
        let m = build_mask(&[2, 9, 100], 4);
        assert_eq!(m.len(), 4);
        let got: Vec<bool> = (0..m.len()).map(|i| m.value(i)).collect();
        assert_eq!(got, vec![false, false, true, false]);
    }

    #[test]
    fn build_mask_dense_run_covers_every_position() {
        // All offsets set → all-true mask.
        let offsets: Vec<u64> = (0..16).collect();
        let m = build_mask(&offsets, 16);
        assert_eq!(m.true_count(), 16);
        assert_eq!(m.null_count(), 0);
    }
}
