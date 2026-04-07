/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
Metrics for indexed search execution plans.

- `PartitionMetrics` — registered against the parent ExecutionPlan, visible in EXPLAIN ANALYZE
- `StreamMetrics` — lightweight handles passed to the stream for recording
**/

use std::sync::Arc;

use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};

/// Lightweight metric handles passed from the execution plan to the stream.
///
/// All fields are `Option` because standalone `IndexedExec` (not under a
/// multi-segment parent) has no shared parent metrics to update.
#[derive(Clone)]
pub struct StreamMetrics {
    pub output_rows: Option<Count>,
    pub elapsed_compute: Option<Time>,
    pub lucene_time: Option<Time>,
    pub parquet_time: Option<Time>,
    pub rows_matched: Option<Count>,
    pub rows_pruned: Option<Count>,
    pub row_selection_count: Option<Count>,
    pub boolean_mask_count: Option<Count>,
    pub rg_processed: Option<Count>,
    pub rg_skipped: Option<Count>,
    /// Accumulated inner DataSourceExec parquet metrics (shared across partitions).
    pub inner_parquet_metrics: Option<Arc<std::sync::Mutex<Vec<MetricsSet>>>>,
}

impl StreamMetrics {
    /// No-op metrics for standalone execution.
    pub fn empty() -> Self {
        Self {
            output_rows: None,
            elapsed_compute: None,
            lucene_time: None,
            parquet_time: None,
            rows_matched: None,
            rows_pruned: None,
            row_selection_count: None,
            boolean_mask_count: None,
            rg_processed: None,
            rg_skipped: None,
            inner_parquet_metrics: None,
        }
    }
}

/// Per-partition metrics registered against the parent `ExecutionPlanMetricsSet`.
pub struct PartitionMetrics {
    pub output_rows: Count,
    pub elapsed_compute: Time,
    pub weight_time: Time,
    pub scorer_time: Time,
    pub lucene_time: Time,
    pub parquet_time: Time,
    pub rows_matched: Count,
    pub rows_pruned_by_page_index: Count,
    pub row_selection_count: Count,
    pub boolean_mask_count: Count,
    pub row_groups_processed: Count,
    pub row_groups_skipped: Count,
}

impl PartitionMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
            elapsed_compute: MetricBuilder::new(metrics).elapsed_compute(partition),
            weight_time: MetricBuilder::new(metrics).subset_time("weight_creation_time", partition),
            scorer_time: MetricBuilder::new(metrics).subset_time("scorer_creation_time", partition),
            lucene_time: MetricBuilder::new(metrics).subset_time("lucene_query_time", partition),
            parquet_time: MetricBuilder::new(metrics).subset_time("parquet_read_time", partition),
            rows_matched: MetricBuilder::new(metrics).counter("rows_matched", partition),
            rows_pruned_by_page_index: MetricBuilder::new(metrics).counter("rows_pruned_by_page_index", partition),
            row_selection_count: MetricBuilder::new(metrics).counter("strategy_row_selection", partition),
            boolean_mask_count: MetricBuilder::new(metrics).counter("strategy_boolean_mask", partition),
            row_groups_processed: MetricBuilder::new(metrics).counter("row_groups_processed", partition),
            row_groups_skipped: MetricBuilder::new(metrics).counter("row_groups_skipped", partition),
        }
    }

    /// Convert into `StreamMetrics` for passing to the stream.
    pub fn into_stream_metrics(
        self,
        inner_parquet_metrics: Option<Arc<std::sync::Mutex<Vec<MetricsSet>>>>,
    ) -> StreamMetrics {
        StreamMetrics {
            output_rows: Some(self.output_rows),
            elapsed_compute: Some(self.elapsed_compute),
            lucene_time: Some(self.lucene_time),
            parquet_time: Some(self.parquet_time),
            rows_matched: Some(self.rows_matched),
            rows_pruned: Some(self.rows_pruned_by_page_index),
            row_selection_count: Some(self.row_selection_count),
            boolean_mask_count: Some(self.boolean_mask_count),
            rg_processed: Some(self.row_groups_processed),
            rg_skipped: Some(self.row_groups_skipped),
            inner_parquet_metrics,
        }
    }
}
