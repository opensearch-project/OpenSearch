/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
Streaming execution for indexed parquet reads. This is mainly the scan logic for one partition -
particularily one segment chunk of the partition.

Processes row groups one at a time, applying page pruning and either
RowSelection (v48) or BooleanMask (v46) based on selectivity.

Prefetch : While processing the current row group's parquet data, we prefetch doc IDs
for the next row group in a background task. This overlaps index query I/O with
parquet I/O for better throughput.

All DataFusion parquet-specific API calls (ParquetSource, FileScanConfigBuilder,
DataSourceExec, etc.) are delegated to `parquet_bridge`. This file only uses
stable Arrow types and the bridge's public API.
**/

use std::any::Any;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use datafusion_common::DataFusionError;
use futures::{Future, Stream};
use tokio::sync::oneshot;

use super::index::{BitsetMode, RowGroupDocsCollector};
use super::metrics::StreamMetrics;
use super::page_pruner::PagePruner;
use super::parquet_bridge::{self, RowGroupStreamConfig};

/// Row group metadata.
#[derive(Debug, Clone)]
pub struct RowGroupInfo {
    pub index: usize,
    pub first_row: i64,
    pub num_rows: i64,
}

/// Strategy for filtering rows within a row group.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FilterStrategy {
    /// RowSelection during decode — best for <3% selectivity.
    RowSelection,
    /// BooleanArray mask after decode — best for ≥3% selectivity.
    BooleanMask,
}

impl FilterStrategy {
    /// Choose strategy based on selectivity within a row group.
    pub fn choose(num_selected: usize, total_rows: i64) -> Self {
        let selectivity = num_selected as f64 / total_rows as f64;
        if selectivity < 0.03 {
            FilterStrategy::RowSelection
        } else {
            FilterStrategy::BooleanMask
        }
    }
}

/// Convert sorted offsets to RowSelection.
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

fn build_mask(offsets: &[u64], num_rows: i64) -> BooleanArray {
    let mut mask = vec![false; num_rows as usize];
    for &offset in offsets {
        if (offset as i64) < num_rows {
            mask[offset as usize] = true;
        }
    }
    BooleanArray::from(mask)
}

// ── Prefetched Row Group Data ──────────────────────────────────────────

struct PrefetchedRowGroup {
    rg: RowGroupInfo,
    offsets: Vec<u64>,
    /// Time spent in Lucene collect + page pruning (nanoseconds).
    lucene_nanos: u64,
}

type PrefetchResult = Result<Option<PrefetchedRowGroup>, String>;
type PrefetchHandle = oneshot::Receiver<PrefetchResult>;

/// Reads doc IDs from a single `SegmentCollector` with async prefetching.
///
/// Each row group: collect bitset, apply page pruning (AND or OR mode),
/// then hand off offsets for parquet reading.
struct IndexReader {
    collector: Arc<dyn RowGroupDocsCollector>,
    row_groups: Vec<RowGroupInfo>,
    current_rg_idx: usize,
    page_pruner: Arc<PagePruner>,
    bitset_mode: BitsetMode,
    pending_prefetch: Option<PrefetchHandle>,
    cached_result: Option<PrefetchResult>,
    doc_range: Option<(i32, i32)>,
}

impl IndexReader {
    fn new(
        collector: Arc<dyn RowGroupDocsCollector>,
        row_groups: Vec<RowGroupInfo>,
        page_pruner: PagePruner,
        bitset_mode: BitsetMode,
        doc_range: Option<(i32, i32)>,
    ) -> Self {
        Self {
            collector,
            row_groups,
            current_rg_idx: 0,
            page_pruner: Arc::new(page_pruner),
            bitset_mode,
            pending_prefetch: None,
            cached_result: None,
            doc_range,
        }
    }

    /// Fetch doc IDs for a specific row group.
    fn fetch_row_group(
        collector: &Arc<dyn RowGroupDocsCollector>,
        page_pruner: &PagePruner,
        bitset_mode: BitsetMode,
        row_groups: &[RowGroupInfo],
        rg_idx: usize,
        doc_range: Option<(i32, i32)>,
    ) -> Result<Option<PrefetchedRowGroup>, String> {
        if rg_idx >= row_groups.len() {
            return Ok(None);
        }

        let rg = row_groups[rg_idx].clone();
        let rg_start = rg.first_row as i32;
        let rg_end = (rg.first_row + rg.num_rows) as i32;

        let mut min_doc = rg_start;
        let mut max_doc = rg_end;

        if let Some((range_min, range_max)) = doc_range {
            min_doc = min_doc.max(range_min);
            max_doc = max_doc.min(range_max);
            if min_doc >= max_doc {
                return Ok(None);
            }
        }

        let t_lucene = std::time::Instant::now();

        // Collect bitset from the single collector
        let bitset = collector.collect(min_doc, max_doc)?;

        // Expand bitset to doc IDs
        let mut raw_ids: Vec<i64> = Vec::new();
        for (word_idx, &word) in bitset.iter().enumerate() {
            if word == 0 {
                continue;
            }
            let base = min_doc as i64 + (word_idx as i64 * 64);
            for bit in 0..64 {
                if (word >> bit) & 1 == 1 {
                    let doc_id = base + bit;
                    if doc_id < max_doc as i64 {
                        raw_ids.push(doc_id);
                    }
                }
            }
        }

        // Apply page pruning based on BitsetMode
        let final_ids = match bitset_mode {
            BitsetMode::And => {
                // Intersect: row must be in bitset AND in matching pages
                page_pruner.filter_row_ids(rg.index, &raw_ids, rg.first_row)
            }
            BitsetMode::Or => {
                // Union: row must be in bitset OR in matching pages
                let candidate_ids =
                    page_pruner.candidate_row_ids(rg.index, rg.first_row, rg.num_rows);
                sorted_union(&raw_ids, &candidate_ids)
            }
        };

        if final_ids.is_empty() {
            return Ok(None);
        }

        let lucene_nanos = t_lucene.elapsed().as_nanos() as u64;

        let offsets: Vec<u64> = final_ids
            .iter()
            .map(|&id| (id - rg.first_row) as u64)
            .collect();

        Ok(Some(PrefetchedRowGroup { rg, offsets, lucene_nanos }))
    }

    fn start_prefetch(&mut self, rg_idx: usize) {
        if rg_idx >= self.row_groups.len() {
            return;
        }

        let collector = Arc::clone(&self.collector);
        let page_pruner = Arc::clone(&self.page_pruner);
        let bitset_mode = self.bitset_mode;
        let row_groups = self.row_groups.clone();
        let doc_range = self.doc_range;

        let (tx, rx) = oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let result = Self::fetch_row_group(
                &collector,
                &page_pruner,
                bitset_mode,
                &row_groups,
                rg_idx,
                doc_range,
            );
            let _ = tx.send(result);
        });

        self.pending_prefetch = Some(rx);
    }

    fn poll_next_row_group(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<PrefetchedRowGroup>, DataFusionError>> {
        loop {
            if self.current_rg_idx >= self.row_groups.len() {
                return Poll::Ready(Ok(None));
            }

            if let Some(result) = self.cached_result.take() {
                self.current_rg_idx += 1;
                self.start_prefetch(self.current_rg_idx);

                match result {
                    Ok(Some(prefetched)) => return Poll::Ready(Ok(Some(prefetched))),
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

/// Merge two sorted `i64` slices into a sorted, deduplicated vec.
fn sorted_union(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                result.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result.extend_from_slice(&a[i..]);
    result.extend_from_slice(&b[j..]);
    result
}

/// Execution plan for a single segment's indexed parquet read.
///
/// Streams row groups one at a time, applying page pruning and adaptive
/// RowSelection/BooleanMask strategy per row group.
pub struct IndexedExec {
    pub(crate) schema: SchemaRef,
    pub(crate) full_schema: SchemaRef,
    pub(crate) file_path: PathBuf,
    pub(crate) file_size: u64,
    pub(crate) row_groups: Vec<RowGroupInfo>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) properties: PlanProperties,
    pub(crate) metadata: Arc<ParquetMetaData>,
    pub(crate) filters: Vec<Expr>,
    pub(crate) predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Pre-created collector for this segment (from ShardSearcher).
    pub(crate) collector: std::sync::Mutex<Option<Arc<dyn RowGroupDocsCollector>>>,
    /// How the bitset relates to page pruner filters.
    pub(crate) bitset_mode: BitsetMode,
    /// Optional doc ID range restriction `[min, max)`.
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
            .field("filters", &self.filters.len())
            .field("has_predicate", &self.predicate.is_some())
            .field("bitset_mode", &self.bitset_mode)
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
            "IndexedExec: rg={}, total_rows={}, filters={}, predicate={}, mode={:?}{}",
            self.row_groups.len(),
            total_rows,
            self.filters.len(),
            self.predicate.is_some(),
            self.bitset_mode,
            doc_range_str,
        )
    }
}

impl ExecutionPlan for IndexedExec {
    fn name(&self) -> &str { "IndexedExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn properties(&self) -> &PlanProperties { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

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
        let collector = {
            let mut guard = self.collector.lock().unwrap();
            guard.take().ok_or_else(|| {
                DataFusionError::Internal("IndexedExec: collector already consumed".to_string())
            })?
        };

        let page_pruner =
            PagePruner::new(&self.full_schema, Arc::clone(&self.metadata), &self.filters);

        let index_reader = IndexReader::new(
            collector,
            self.row_groups.clone(),
            page_pruner,
            self.bitset_mode,
            self.doc_range,
        );

        Ok(Box::pin(IndexedStream::new(
            self.schema.clone(),
            self.full_schema.clone(),
            self.file_path.clone(),
            self.file_size,
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

struct IndexedStream {
    schema: SchemaRef,
    full_schema: SchemaRef,
    file_path: PathBuf,
    file_size: u64,
    index_reader: IndexReader,
    projection: Option<Vec<usize>>,
    current_stream: Option<SendableRecordBatchStream>,
    current_inner_plan: Option<Arc<dyn ExecutionPlan>>,
    current_mask: Option<BooleanArray>,
    mask_offset: usize,
    finished: bool,
    metadata: Arc<ParquetMetaData>,
    predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    initialized: bool,
    metrics: StreamMetrics,
    force_pushdown: Option<bool>,
    force_strategy: Option<FilterStrategy>,
}

impl IndexedStream {
    fn new(
        schema: SchemaRef,
        full_schema: SchemaRef,
        file_path: PathBuf,
        file_size: u64,
        index_reader: IndexReader,
        projection: Option<Vec<usize>>,
        metadata: Arc<ParquetMetaData>,
        predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        metrics: StreamMetrics,
        force_pushdown: Option<bool>,
        force_strategy: Option<FilterStrategy>,
    ) -> Self {
        Self {
            schema,
            full_schema,
            file_path,
            file_size,
            index_reader,
            projection,
            current_stream: None,
            current_inner_plan: None,
            current_mask: None,
            mask_offset: 0,
            finished: false,
            metadata,
            predicate,
            initialized: false,
            metrics,
            force_pushdown,
            force_strategy,
        }
    }

    /// Build the bridge config (shared between v48 and v46 paths).
    fn bridge_config(&self) -> RowGroupStreamConfig {
        RowGroupStreamConfig {
            file_path: self.file_path.to_string_lossy().to_string(),
            file_size: self.file_size,
            full_schema: self.full_schema.clone(),
            metadata: Arc::clone(&self.metadata),
            projection: self.projection.clone(),
            predicate: self.predicate.clone(),
        }
    }

    /// RowSelection stream (v48) — predicate pushdown is safe here.
    fn create_row_selection_stream(
        &self,
        rg: &RowGroupInfo,
        offsets: &[u64],
    ) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
        let selection = offsets_to_row_selection(offsets, rg.num_rows);
        let push = self.force_pushdown.unwrap_or(true);
        parquet_bridge::create_row_selection_stream(&self.bridge_config(), rg.index, selection, push)
    }

    /// Full scan stream (v46) — predicate pushdown is NOT safe (mask offset misalignment).
    fn create_full_scan_stream(
        &self,
        rg: &RowGroupInfo,
    ) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
        parquet_bridge::create_full_scan_stream(&self.bridge_config(), rg.index)
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
                        if let Some(ref mask) = self.current_mask {
                            let batch_len = batch.num_rows();
                            let mask_slice = mask.slice(self.mask_offset, batch_len);
                            let mask_slice =
                                mask_slice.as_any().downcast_ref::<BooleanArray>().unwrap();
                            self.mask_offset += batch_len;
                            match filter_record_batch(&batch, mask_slice) {
                                Ok(filtered) if filtered.num_rows() > 0 => {
                                    if let Some(ref counter) = self.metrics.output_rows {
                                        counter.add(filtered.num_rows());
                                    }
                                    return Poll::Ready(Some(Ok(filtered)));
                                }
                                Ok(_) => continue,
                                Err(e) => {
                                    return Poll::Ready(Some(Err(DataFusionError::ArrowError(
                                        Box::new(e),
                                        None,
                                    ))));
                                }
                            }
                        } else {
                            if let Some(ref counter) = self.metrics.output_rows {
                                counter.add(batch.num_rows());
                            }
                            return Poll::Ready(Some(Ok(batch)));
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
                        self.mask_offset = 0;
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
                    let offsets = prefetched.offsets;

                    // Record Lucene/JNI collect time
                    if let Some(ref timer) = self.metrics.lucene_time {
                        timer.add_duration(std::time::Duration::from_nanos(prefetched.lucene_nanos));
                    }

                    if let Some(ref counter) = self.metrics.rows_matched {
                        counter.add(offsets.len());
                    }
                    if let Some(ref counter) = self.metrics.rg_processed {
                        counter.add(1);
                    }

                    let strategy = self
                        .force_strategy
                        .unwrap_or_else(|| FilterStrategy::choose(offsets.len(), rg.num_rows));

                    // Time plan creation (parquet bridge setup)
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
                                    self.current_mask = Some(build_mask(&offsets, rg.num_rows));
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
