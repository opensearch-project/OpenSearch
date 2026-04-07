/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
Streaming execution for boolean tree queries — hybrid two-phase evaluation.

Phase 1 (prefetch): `evaluate_tree_prefetch` produces a SUPERSET bitmap
per row group, plus per-Index-leaf bitmaps for Phase 2.
Row groups with zero candidates are skipped entirely.

Phase 2 (on-batch): `evaluate_tree_on_batch` evaluates the full tree on
actual RecordBatch data, producing an exact `BooleanArray` mask.

Architecture:
  TreeIndexReader (prefetch, background thread)
    -> Per row group:
         1. Phase 1: evaluate_tree_prefetch -> PrefetchResult
         2. If empty -> skip row group
         3. Full-scan columnar read
         4. Per batch: Phase 2 -> exact mask -> filter_record_batch
**/

use std::any::Any;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Future;

use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use datafusion_common::DataFusionError;
use futures::Stream;
use tokio::sync::oneshot;

use super::bool_tree::{BoolNode, ResolvedNode};
use super::index::RowGroupDocsCollector;
use super::metrics::StreamMetrics;
use super::page_pruner::PagePruner;
use super::parquet_bridge::{self, RowGroupStreamConfig};
use super::stream::RowGroupInfo;
use super::tree_eval::{self, EvalContext, CollectorBitmaps};

// ── Prefetched Row Group ───────────────────────────────────────────────

struct PrefetchedRowGroup {
    rg: RowGroupInfo,
    offsets: Vec<u64>,
    eval_nanos: u64,
    index_bitmaps: Arc<CollectorBitmaps>,
}

type PrefetchResultInternal = std::result::Result<Option<PrefetchedRowGroup>, String>;

// ── TreeIndexReader ────────────────────────────────────────────────────

struct TreeIndexReader {
    tree: Arc<ResolvedNode>,
    row_groups: Vec<RowGroupInfo>,
    current_rg_idx: usize,
    page_pruner: Arc<PagePruner>,
    pending_prefetch: Option<oneshot::Receiver<PrefetchResultInternal>>,
    cached_result: Option<PrefetchResultInternal>,
    doc_range: Option<(i32, i32)>,
}

impl TreeIndexReader {
    fn new(
        tree: Arc<ResolvedNode>,
        row_groups: Vec<RowGroupInfo>,
        page_pruner: PagePruner,
        doc_range: Option<(i32, i32)>,
    ) -> Self {
        Self {
            tree, row_groups, current_rg_idx: 0,
            page_pruner: Arc::new(page_pruner),
            pending_prefetch: None, cached_result: None, doc_range,
        }
    }

    fn fetch_row_group(
        tree: &Arc<ResolvedNode>,
        page_pruner: &PagePruner,
        row_groups: &[RowGroupInfo],
        rg_idx: usize,
        doc_range: Option<(i32, i32)>,
    ) -> std::result::Result<Option<PrefetchedRowGroup>, String> {
        if rg_idx >= row_groups.len() { return Ok(None); }
        let rg = row_groups[rg_idx].clone();
        let mut min_doc = rg.first_row as i32;
        let mut max_doc = (rg.first_row + rg.num_rows) as i32;
        if let Some((range_min, range_max)) = doc_range {
            min_doc = min_doc.max(range_min);
            max_doc = max_doc.min(range_max);
            if min_doc >= max_doc { return Ok(None); }
        }
        let t = std::time::Instant::now();
        let ctx = EvalContext { rg_idx: rg.index, rg_first_row: rg.first_row,
            rg_num_rows: rg.num_rows, min_doc, max_doc };
        let result = tree_eval::evaluate_tree_prefetch(tree, &ctx, page_pruner)?;
        if result.candidates.is_empty() { return Ok(None); }
        let offsets = tree_eval::bitmap_to_offsets(&result.candidates, rg.first_row);
        Ok(Some(PrefetchedRowGroup {
            rg, offsets, eval_nanos: t.elapsed().as_nanos() as u64,
            index_bitmaps: Arc::new(result.collector_bitmaps),
        }))
    }

    fn start_prefetch(&mut self, rg_idx: usize) {
        if rg_idx >= self.row_groups.len() { return; }
        let tree = Arc::clone(&self.tree);
        let pp = Arc::clone(&self.page_pruner);
        let rgs = self.row_groups.clone();
        let dr = self.doc_range;
        let (tx, rx) = oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let _ = tx.send(Self::fetch_row_group(&tree, &pp, &rgs, rg_idx, dr));
        });
        self.pending_prefetch = Some(rx);
    }

    fn poll_next_row_group(
        &mut self, cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Option<PrefetchedRowGroup>, DataFusionError>> {
        loop {
            if self.current_rg_idx >= self.row_groups.len() { return Poll::Ready(Ok(None)); }
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

    fn init_prefetch(&mut self) { self.start_prefetch(0); }
}

// ── TreeIndexedExec ────────────────────────────────────────────────────

/// Execution plan for a single segment chunk using boolean tree evaluation.
pub struct TreeIndexedExec {
    pub(crate) schema: SchemaRef,
    pub(crate) full_schema: SchemaRef,
    pub(crate) file_path: PathBuf,
    pub(crate) file_size: u64,
    pub(crate) row_groups: Vec<RowGroupInfo>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) properties: PlanProperties,
    pub(crate) metadata: Arc<ParquetMetaData>,
    pub(crate) bool_node: Arc<BoolNode>,
    pub(crate) collectors: std::sync::Mutex<Option<Vec<Arc<dyn RowGroupDocsCollector>>>>,
    pub(crate) predicates: Vec<super::bool_tree::ResolvedPredicate>,
    pub(crate) doc_range: Option<(i32, i32)>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) stream_metrics: StreamMetrics,
}

impl std::fmt::Debug for TreeIndexedExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeIndexedExec")
            .field("row_groups", &self.row_groups.len())
            .finish()
    }
}

impl DisplayAs for TreeIndexedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let total_rows: i64 = self.row_groups.iter().map(|rg| rg.num_rows).sum();
        write!(f, "TreeIndexedExec: rg={}, total_rows={}", self.row_groups.len(), total_rows)
    }
}

impl ExecutionPlan for TreeIndexedExec {
    fn name(&self) -> &str { "TreeIndexedExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn properties(&self) -> &PlanProperties { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
    fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> { Ok(self) }
    fn metrics(&self) -> Option<MetricsSet> { Some(self.metrics.clone_inner()) }

    fn execute(&self, _partition: usize, _context: Arc<datafusion::execution::TaskContext>) -> Result<SendableRecordBatchStream> {
        let collectors = {
            let mut guard = self.collectors.lock().unwrap();
            guard.take().ok_or_else(|| DataFusionError::Internal("collectors already consumed".into()))?
        };
        let normalized = (*self.bool_node).clone().push_not_down();
        let resolved = normalized.resolve(&collectors, &self.predicates)
            .map_err(|e| DataFusionError::Internal(format!("tree resolve: {}", e)))?;
        let tree = Arc::new(resolved);
        let page_pruner = PagePruner::new(&self.full_schema, Arc::clone(&self.metadata), &[]);
        let reader = TreeIndexReader::new(Arc::clone(&tree), self.row_groups.clone(), page_pruner, self.doc_range);
        Ok(Box::pin(TreeIndexedStream::new(
            self.schema.clone(), self.full_schema.clone(), self.file_path.clone(),
            self.file_size, reader, self.projection.clone(), Arc::clone(&self.metadata),
            tree, self.stream_metrics.clone(),
        )))
    }
}

// ── TreeIndexedStream ──────────────────────────────────────────────────

struct TreeIndexedStream {
    schema: SchemaRef,
    full_schema: SchemaRef,
    file_path: PathBuf,
    file_size: u64,
    index_reader: TreeIndexReader,
    projection: Option<Vec<usize>>,
    current_stream: Option<SendableRecordBatchStream>,
    current_inner_plan: Option<Arc<dyn ExecutionPlan>>,
    finished: bool,
    metadata: Arc<ParquetMetaData>,
    initialized: bool,
    metrics: StreamMetrics,
    tree: Arc<ResolvedNode>,
    current_index_bitmaps: Option<Arc<CollectorBitmaps>>,
    current_rg_first_row: i64,
    batch_offset: usize,
}

impl TreeIndexedStream {
    fn new(
        schema: SchemaRef, full_schema: SchemaRef, file_path: PathBuf, file_size: u64,
        index_reader: TreeIndexReader, projection: Option<Vec<usize>>,
        metadata: Arc<ParquetMetaData>, tree: Arc<ResolvedNode>, metrics: StreamMetrics,
    ) -> Self {
        Self {
            schema, full_schema, file_path, file_size, index_reader, projection,
            current_stream: None, current_inner_plan: None, finished: false,
            metadata, initialized: false, metrics, tree,
            current_index_bitmaps: None, current_rg_first_row: 0, batch_offset: 0,
        }
    }

    fn bridge_config(&self) -> RowGroupStreamConfig {
        RowGroupStreamConfig {
            file_path: self.file_path.to_string_lossy().to_string(),
            file_size: self.file_size,
            full_schema: self.full_schema.clone(),
            metadata: Arc::clone(&self.metadata),
            projection: self.projection.clone(),
            predicate: None,
        }
    }

    fn create_full_scan_stream(
        &self, rg: &RowGroupInfo,
    ) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
        parquet_bridge::create_full_scan_stream(&self.bridge_config(), rg.index)
    }
}

impl Stream for TreeIndexedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.initialized {
            self.index_reader.init_prefetch();
            self.initialized = true;
        }
        loop {
            // Poll current parquet stream
            if let Some(ref mut stream) = self.current_stream {
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) if batch.num_rows() > 0 => {
                        let bitmaps = self.current_index_bitmaps.as_ref().unwrap();
                        let batch_len = batch.num_rows();
                        match tree_eval::evaluate_tree_on_batch(
                            &self.tree, &batch, bitmaps,
                            self.current_rg_first_row, self.batch_offset, batch_len,
                        ) {
                            Ok(mask) => {
                                self.batch_offset += batch_len;
                                match filter_record_batch(&batch, &mask) {
                                    Ok(filtered) if filtered.num_rows() > 0 => {
                                        // Re-project to output schema
                                        let out = if filtered.schema().fields().len() != self.schema.fields().len() {
                                            let indices: Vec<usize> = self.schema.fields().iter()
                                                .filter_map(|f| filtered.schema().index_of(f.name()).ok())
                                                .collect();
                                            filtered.project(&indices)
                                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
                                        } else { filtered };
                                        if let Some(ref counter) = self.metrics.output_rows {
                                            counter.add(out.num_rows());
                                        }
                                        return Poll::Ready(Some(Ok(out)));
                                    }
                                    Ok(_) => continue,
                                    Err(e) => return Poll::Ready(Some(Err(DataFusionError::ArrowError(Box::new(e), None)))),
                                }
                            }
                            Err(e) => return Poll::Ready(Some(Err(DataFusionError::External(e.into())))),
                        }
                    }
                    Poll::Ready(Some(Ok(_))) => continue,
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        if let Some(inner_plan) = self.current_inner_plan.take() {
                            if let Some(inner_metrics) = inner_plan.metrics() {
                                if let Some(ref acc) = self.metrics.inner_parquet_metrics {
                                    if let Ok(mut vec) = acc.lock() { vec.push(inner_metrics); }
                                }
                            }
                        }
                        self.current_stream = None;
                        self.current_index_bitmaps = None;
                        self.batch_offset = 0;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            if self.finished { return Poll::Ready(None); }
            // Poll for next row group
            match self.index_reader.poll_next_row_group(cx) {
                Poll::Ready(Ok(Some(prefetched))) => {
                    let rg = prefetched.rg;
                    if let Some(ref timer) = self.metrics.lucene_time {
                        timer.add_duration(std::time::Duration::from_nanos(prefetched.eval_nanos));
                    }
                    if let Some(ref counter) = self.metrics.rows_matched { counter.add(prefetched.offsets.len()); }
                    if let Some(ref counter) = self.metrics.rg_processed { counter.add(1); }
                    let t = std::time::Instant::now();
                    match self.create_full_scan_stream(&rg) {
                        Ok((stream, plan)) => {
                            if let Some(ref timer) = self.metrics.parquet_time { timer.add_duration(t.elapsed()); }
                            self.current_stream = Some(stream);
                            self.current_inner_plan = Some(plan);
                            self.current_index_bitmaps = Some(prefetched.index_bitmaps);
                            self.current_rg_first_row = rg.first_row;
                            self.batch_offset = 0;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                Poll::Ready(Ok(None)) => { self.finished = true; return Poll::Ready(None); }
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for TreeIndexedStream {
    fn schema(&self) -> SchemaRef { self.schema.clone() }
}
