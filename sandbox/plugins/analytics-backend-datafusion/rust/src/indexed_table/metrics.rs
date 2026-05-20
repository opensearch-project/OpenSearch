/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Metrics for indexed search execution plans.
//!
//! - [`PartitionMetrics`] — registered against the parent `ExecutionPlanMetricsSet`,
//!   visible in `EXPLAIN ANALYZE`.
//! - [`StreamMetrics`] — lightweight handles passed to each RG stream for recording.

use std::sync::Arc;

use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};

/// Lightweight metric handles passed from `IndexedExec` to the streaming loop.
///
/// All fields are `Option` because standalone uses of `IndexedExec` (i.e. not
/// under a multi-segment parent) have no shared parent metrics to update.
#[derive(Clone)]
pub struct StreamMetrics {
    pub output_rows: Option<Count>,
    pub elapsed_compute: Option<Time>,
    pub index_time: Option<Time>,
    pub parquet_time: Option<Time>,
    pub rows_matched: Option<Count>,
    pub rows_pruned: Option<Count>,
    /// RGs where `min_skip_run == 1` — row-granular RowSelection.
    pub min_skip_run_row_granular: Option<Count>,
    /// RGs where `min_skip_run > 1` — block-granular (coarser) RowSelection.
    pub min_skip_run_block_granular: Option<Count>,
    pub rg_processed: Option<Count>,
    pub rg_skipped: Option<Count>,
    /// Count of parquet pages the page-level pruner eliminated across
    /// all RGs in this partition.
    pub pages_pruned: Option<Count>,
    /// Total parquet pages considered by the page-level pruner. Ratio
    /// `pages_pruned / pages_total` gives pruning effectiveness.
    pub pages_total: Option<Count>,
    /// Count of `prune_rg` calls that couldn't apply pruning (no page
    /// index, column missing, or `PruningPredicate` rejected the
    /// expression). Diagnostic: high values mean pruning isn't happening.
    pub page_pruning_unavailable: Option<Count>,
    /// FFM round-trips into the Java backend collector. Once per
    /// Collector leaf per RG. This is the highest per-query cost
    /// component, useful for tuning backend query shapes.
    pub ffm_collector_calls: Option<Count>,
    /// Count of output `RecordBatch`es emitted from `poll_next`.
    /// Divergence from `parquet_batches_received` indicates refinement
    /// stage filtering (empty batches dropped).
    pub batches_produced: Option<Count>,
    /// Count of `RecordBatch`es received from the inner parquet stream,
    /// before mask filtering.
    pub parquet_batches_received: Option<Count>,
    /// Number of RGs whose `PositionMap` was the `Identity` variant
    /// (whole-RG selected, no skips).
    pub position_map_identity: Option<Count>,
    /// Number of RGs whose `PositionMap` was the `Bitmap` variant
    /// (row-granular; row-to-rg-pos via `RoaringBitmap::select`).
    pub position_map_bitmap: Option<Count>,
    /// Number of RGs whose `PositionMap` was the `Runs` variant
    /// (block-granular; explicit run table).
    pub position_map_runs: Option<Count>,
    /// Wall-clock time the poll thread spent blocked on the oneshot
    /// receiver for a pending prefetch. Zero if prefetch fully overlapped
    /// with upstream work; non-zero indicates idle time.
    pub prefetch_wait_time: Option<Time>,
    /// Count of `Poll::Pending` returns from the prefetch receiver (times
    /// the poll thread had to park waiting for Lucene).
    pub prefetch_wait_count: Option<Count>,
    /// Wall-clock time spent inside `LimitedBatchCoalescer::push_batch` +
    /// `next_completed_batch`. Helps isolate coalescing overhead.
    pub coalesce_time: Option<Time>,
    /// Count of batches fed into the coalescer (before combining).
    pub batches_pre_coalesce: Option<Count>,
    /// Time spent in `build_mask` (once per RG — builds RG-sized
    /// BooleanArray from candidate RoaringBitmap via PositionMap).
    pub build_mask_time: Option<Time>,
    /// Time spent in `filter_record_batch` per input batch.
    pub filter_record_batch_time: Option<Time>,
    /// Time spent in evaluator's `on_batch_mask` per input batch
    /// (returns None on pure-collector paths; cost of dispatch is
    /// visible here).
    pub on_batch_mask_time: Option<Time>,
    /// Time spent slicing + downcasting the current_mask per input
    /// batch.
    pub mask_slice_time: Option<Time>,
    /// Time spent in the projection fix-up at the bottom of
    /// `finalize_batch` (strip predicate columns).
    pub projection_fixup_time: Option<Time>,
    /// Time spent polling the inner parquet stream (pull decoded
    /// batch), isolating decode from our own processing.
    pub parquet_poll_time: Option<Time>,
    /// Accumulated inner `DataSourceExec` parquet metrics (shared across partitions).
    pub inner_parquet_metrics: Option<Arc<std::sync::Mutex<Vec<MetricsSet>>>>,
}

impl StreamMetrics {
    /// No-op metrics for standalone execution.
    pub fn empty() -> Self {
        Self {
            output_rows: None,
            elapsed_compute: None,
            index_time: None,
            parquet_time: None,
            rows_matched: None,
            rows_pruned: None,
            min_skip_run_row_granular: None,
            min_skip_run_block_granular: None,
            rg_processed: None,
            rg_skipped: None,
            pages_pruned: None,
            pages_total: None,
            page_pruning_unavailable: None,
            ffm_collector_calls: None,
            batches_produced: None,
            parquet_batches_received: None,
            position_map_identity: None,
            position_map_bitmap: None,
            position_map_runs: None,
            prefetch_wait_time: None,
            prefetch_wait_count: None,
            coalesce_time: None,
            batches_pre_coalesce: None,
            build_mask_time: None,
            filter_record_batch_time: None,
            on_batch_mask_time: None,
            mask_slice_time: None,
            projection_fixup_time: None,
            parquet_poll_time: None,
            inner_parquet_metrics: None,
        }
    }
}

/// Per-partition metrics registered against the parent `ExecutionPlanMetricsSet`.
pub struct PartitionMetrics {
    pub output_rows: Count,
    pub elapsed_compute: Time,
    pub index_time: Time,
    pub parquet_time: Time,
    pub rows_matched: Count,
    pub rows_pruned_by_page_index: Count,
    pub min_skip_run_row_granular: Count,
    pub min_skip_run_block_granular: Count,
    pub row_groups_processed: Count,
    pub row_groups_skipped: Count,
    pub pages_pruned: Count,
    pub pages_total: Count,
    pub page_pruning_unavailable: Count,
    pub ffm_collector_calls: Count,
    pub batches_produced: Count,
    pub parquet_batches_received: Count,
    pub position_map_identity: Count,
    pub position_map_bitmap: Count,
    pub position_map_runs: Count,
    pub prefetch_wait_time: Time,
    pub prefetch_wait_count: Count,
    pub coalesce_time: Time,
    pub batches_pre_coalesce: Count,
    pub build_mask_time: Time,
    pub filter_record_batch_time: Time,
    pub on_batch_mask_time: Time,
    pub mask_slice_time: Time,
    pub projection_fixup_time: Time,
    pub parquet_poll_time: Time,
}

impl PartitionMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let counter = |name: &'static str| MetricBuilder::new(metrics).counter(name, partition);
        Self {
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
            elapsed_compute: MetricBuilder::new(metrics).elapsed_compute(partition),
            index_time: MetricBuilder::new(metrics).subset_time("index_query_time", partition),
            parquet_time: MetricBuilder::new(metrics).subset_time("parquet_read_time", partition),
            rows_matched: counter("rows_matched"),
            rows_pruned_by_page_index: counter("rows_pruned_by_page_index"),
            min_skip_run_row_granular: counter("min_skip_run_row_granular"),
            min_skip_run_block_granular: counter("min_skip_run_block_granular"),
            row_groups_processed: counter("row_groups_processed"),
            row_groups_skipped: counter("row_groups_skipped"),
            pages_pruned: counter("pages_pruned"),
            pages_total: counter("pages_total"),
            page_pruning_unavailable: counter("page_pruning_unavailable"),
            ffm_collector_calls: counter("ffm_collector_calls"),
            batches_produced: counter("batches_produced"),
            parquet_batches_received: counter("parquet_batches_received"),
            position_map_identity: counter("position_map_identity"),
            position_map_bitmap: counter("position_map_bitmap"),
            position_map_runs: counter("position_map_runs"),
            prefetch_wait_time: MetricBuilder::new(metrics)
                .subset_time("prefetch_wait_time", partition),
            prefetch_wait_count: counter("prefetch_wait_count"),
            coalesce_time: MetricBuilder::new(metrics).subset_time("coalesce_time", partition),
            batches_pre_coalesce: counter("batches_pre_coalesce"),
            build_mask_time: MetricBuilder::new(metrics).subset_time("build_mask_time", partition),
            filter_record_batch_time: MetricBuilder::new(metrics)
                .subset_time("filter_record_batch_time", partition),
            on_batch_mask_time: MetricBuilder::new(metrics)
                .subset_time("on_batch_mask_time", partition),
            mask_slice_time: MetricBuilder::new(metrics).subset_time("mask_slice_time", partition),
            projection_fixup_time: MetricBuilder::new(metrics)
                .subset_time("projection_fixup_time", partition),
            parquet_poll_time: MetricBuilder::new(metrics)
                .subset_time("parquet_poll_time", partition),
        }
    }

    /// Convert into `StreamMetrics` for passing to the streaming loop.
    pub fn into_stream_metrics(
        self,
        inner_parquet_metrics: Option<Arc<std::sync::Mutex<Vec<MetricsSet>>>>,
    ) -> StreamMetrics {
        StreamMetrics {
            output_rows: Some(self.output_rows),
            elapsed_compute: Some(self.elapsed_compute),
            index_time: Some(self.index_time),
            parquet_time: Some(self.parquet_time),
            rows_matched: Some(self.rows_matched),
            rows_pruned: Some(self.rows_pruned_by_page_index),
            min_skip_run_row_granular: Some(self.min_skip_run_row_granular),
            min_skip_run_block_granular: Some(self.min_skip_run_block_granular),
            rg_processed: Some(self.row_groups_processed),
            rg_skipped: Some(self.row_groups_skipped),
            pages_pruned: Some(self.pages_pruned),
            pages_total: Some(self.pages_total),
            page_pruning_unavailable: Some(self.page_pruning_unavailable),
            ffm_collector_calls: Some(self.ffm_collector_calls),
            batches_produced: Some(self.batches_produced),
            parquet_batches_received: Some(self.parquet_batches_received),
            position_map_identity: Some(self.position_map_identity),
            position_map_bitmap: Some(self.position_map_bitmap),
            position_map_runs: Some(self.position_map_runs),
            prefetch_wait_time: Some(self.prefetch_wait_time),
            prefetch_wait_count: Some(self.prefetch_wait_count),
            coalesce_time: Some(self.coalesce_time),
            batches_pre_coalesce: Some(self.batches_pre_coalesce),
            build_mask_time: Some(self.build_mask_time),
            filter_record_batch_time: Some(self.filter_record_batch_time),
            on_batch_mask_time: Some(self.on_batch_mask_time),
            mask_slice_time: Some(self.mask_slice_time),
            projection_fixup_time: Some(self.projection_fixup_time),
            parquet_poll_time: Some(self.parquet_poll_time),
            inner_parquet_metrics,
        }
    }
}
