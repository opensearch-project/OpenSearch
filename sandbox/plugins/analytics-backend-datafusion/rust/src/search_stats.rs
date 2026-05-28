/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Process-global accumulator for indexed-path search execution metrics.
//!
//! [`SEARCH_STATS`] is updated once per partition stream lifetime from the
//! `Drop` impl of the partition stream wrapper in
//! [`crate::indexed_table::table_provider`]; values are read by
//! [`crate::stats::pack_search_stats`] when the FFM stats endpoint runs.

use std::sync::atomic::{AtomicI64, Ordering};

use once_cell::sync::Lazy;

use crate::indexed_table::metrics::StreamMetrics;
use crate::stats::SearchStatsRepr;

/// Cumulative indexed-search execution metrics. Field names match
/// [`SearchStatsRepr`] / `PartitionMetrics`.
pub struct SearchStatsAccumulator {
    pub queries_completed: AtomicI64,

    pub output_rows: AtomicI64,
    pub rows_matched: AtomicI64,
    pub rows_pruned_by_page_index: AtomicI64,
    pub row_groups_processed: AtomicI64,
    pub row_groups_skipped: AtomicI64,
    pub pages_pruned: AtomicI64,
    pub pages_total: AtomicI64,
    pub page_pruning_unavailable: AtomicI64,
    pub ffm_collector_calls: AtomicI64,
    pub batches_produced: AtomicI64,
    pub parquet_batches_received: AtomicI64,
    pub position_map_identity: AtomicI64,
    pub position_map_bitmap: AtomicI64,
    pub position_map_runs: AtomicI64,
    pub min_skip_run_row_granular: AtomicI64,
    pub min_skip_run_block_granular: AtomicI64,
    pub prefetch_wait_count: AtomicI64,
    pub batches_pre_coalesce: AtomicI64,

    pub elapsed_compute_ms: AtomicI64,
    pub index_time_ms: AtomicI64,
    pub parquet_time_ms: AtomicI64,
    pub prefetch_wait_time_ms: AtomicI64,
    pub coalesce_time_ms: AtomicI64,
    pub build_mask_time_ms: AtomicI64,
    pub filter_record_batch_time_ms: AtomicI64,
    pub on_batch_mask_time_ms: AtomicI64,
    pub mask_slice_time_ms: AtomicI64,
    pub projection_fixup_time_ms: AtomicI64,
    pub parquet_poll_time_ms: AtomicI64,
}

impl SearchStatsAccumulator {
    const fn new() -> Self {
        Self {
            queries_completed: AtomicI64::new(0),
            output_rows: AtomicI64::new(0),
            rows_matched: AtomicI64::new(0),
            rows_pruned_by_page_index: AtomicI64::new(0),
            row_groups_processed: AtomicI64::new(0),
            row_groups_skipped: AtomicI64::new(0),
            pages_pruned: AtomicI64::new(0),
            pages_total: AtomicI64::new(0),
            page_pruning_unavailable: AtomicI64::new(0),
            ffm_collector_calls: AtomicI64::new(0),
            batches_produced: AtomicI64::new(0),
            parquet_batches_received: AtomicI64::new(0),
            position_map_identity: AtomicI64::new(0),
            position_map_bitmap: AtomicI64::new(0),
            position_map_runs: AtomicI64::new(0),
            min_skip_run_row_granular: AtomicI64::new(0),
            min_skip_run_block_granular: AtomicI64::new(0),
            prefetch_wait_count: AtomicI64::new(0),
            batches_pre_coalesce: AtomicI64::new(0),
            elapsed_compute_ms: AtomicI64::new(0),
            index_time_ms: AtomicI64::new(0),
            parquet_time_ms: AtomicI64::new(0),
            prefetch_wait_time_ms: AtomicI64::new(0),
            coalesce_time_ms: AtomicI64::new(0),
            build_mask_time_ms: AtomicI64::new(0),
            filter_record_batch_time_ms: AtomicI64::new(0),
            on_batch_mask_time_ms: AtomicI64::new(0),
            mask_slice_time_ms: AtomicI64::new(0),
            projection_fixup_time_ms: AtomicI64::new(0),
            parquet_poll_time_ms: AtomicI64::new(0),
        }
    }

    /// Fold a completed partition stream's final `Count`/`Time` values into
    /// the cumulative counters. `Option` fields are skipped when `None`
    /// (standalone test paths with [`StreamMetrics::empty`]).
    pub fn accumulate(&self, m: &StreamMetrics) {
        self.queries_completed.fetch_add(1, Ordering::Relaxed);

        if let Some(ref c) = m.output_rows {
            self.output_rows.fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.rows_matched {
            self.rows_matched.fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.rows_pruned {
            self.rows_pruned_by_page_index
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.rg_processed {
            self.row_groups_processed
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.rg_skipped {
            self.row_groups_skipped
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.pages_pruned {
            self.pages_pruned.fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.pages_total {
            self.pages_total.fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.page_pruning_unavailable {
            self.page_pruning_unavailable
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.ffm_collector_calls {
            self.ffm_collector_calls
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.batches_produced {
            self.batches_produced
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.parquet_batches_received {
            self.parquet_batches_received
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.position_map_identity {
            self.position_map_identity
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.position_map_bitmap {
            self.position_map_bitmap
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.position_map_runs {
            self.position_map_runs
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.min_skip_run_row_granular {
            self.min_skip_run_row_granular
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.min_skip_run_block_granular {
            self.min_skip_run_block_granular
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.prefetch_wait_count {
            self.prefetch_wait_count
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.batches_pre_coalesce {
            self.batches_pre_coalesce
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }

        if let Some(ref t) = m.elapsed_compute {
            self.elapsed_compute_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
        if let Some(ref t) = m.index_time {
            self.index_time_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
        if let Some(ref t) = m.parquet_time {
            self.parquet_time_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
        if let Some(ref t) = m.prefetch_wait_time {
            self.prefetch_wait_time_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
        if let Some(ref t) = m.coalesce_time {
            self.coalesce_time_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
        if let Some(ref t) = m.build_mask_time {
            self.build_mask_time_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
        if let Some(ref t) = m.filter_record_batch_time {
            self.filter_record_batch_time_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
        if let Some(ref t) = m.on_batch_mask_time {
            self.on_batch_mask_time_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
        if let Some(ref t) = m.mask_slice_time {
            self.mask_slice_time_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
        if let Some(ref t) = m.projection_fixup_time {
            self.projection_fixup_time_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
        if let Some(ref t) = m.parquet_poll_time {
            self.parquet_poll_time_ms
                .fetch_add(ns_to_ms(t.value()), Ordering::Relaxed);
        }
    }

    /// Snapshot the cumulative counters into a [`SearchStatsRepr`] for FFM transport.
    pub fn snapshot(&self) -> SearchStatsRepr {
        SearchStatsRepr {
            queries_completed: self.queries_completed.load(Ordering::Relaxed),
            output_rows: self.output_rows.load(Ordering::Relaxed),
            rows_matched: self.rows_matched.load(Ordering::Relaxed),
            rows_pruned_by_page_index: self.rows_pruned_by_page_index.load(Ordering::Relaxed),
            row_groups_processed: self.row_groups_processed.load(Ordering::Relaxed),
            row_groups_skipped: self.row_groups_skipped.load(Ordering::Relaxed),
            pages_pruned: self.pages_pruned.load(Ordering::Relaxed),
            pages_total: self.pages_total.load(Ordering::Relaxed),
            page_pruning_unavailable: self.page_pruning_unavailable.load(Ordering::Relaxed),
            ffm_collector_calls: self.ffm_collector_calls.load(Ordering::Relaxed),
            batches_produced: self.batches_produced.load(Ordering::Relaxed),
            parquet_batches_received: self.parquet_batches_received.load(Ordering::Relaxed),
            position_map_identity: self.position_map_identity.load(Ordering::Relaxed),
            position_map_bitmap: self.position_map_bitmap.load(Ordering::Relaxed),
            position_map_runs: self.position_map_runs.load(Ordering::Relaxed),
            min_skip_run_row_granular: self.min_skip_run_row_granular.load(Ordering::Relaxed),
            min_skip_run_block_granular: self.min_skip_run_block_granular.load(Ordering::Relaxed),
            prefetch_wait_count: self.prefetch_wait_count.load(Ordering::Relaxed),
            batches_pre_coalesce: self.batches_pre_coalesce.load(Ordering::Relaxed),
            elapsed_compute_ms: self.elapsed_compute_ms.load(Ordering::Relaxed),
            index_time_ms: self.index_time_ms.load(Ordering::Relaxed),
            parquet_time_ms: self.parquet_time_ms.load(Ordering::Relaxed),
            prefetch_wait_time_ms: self.prefetch_wait_time_ms.load(Ordering::Relaxed),
            coalesce_time_ms: self.coalesce_time_ms.load(Ordering::Relaxed),
            build_mask_time_ms: self.build_mask_time_ms.load(Ordering::Relaxed),
            filter_record_batch_time_ms: self.filter_record_batch_time_ms.load(Ordering::Relaxed),
            on_batch_mask_time_ms: self.on_batch_mask_time_ms.load(Ordering::Relaxed),
            mask_slice_time_ms: self.mask_slice_time_ms.load(Ordering::Relaxed),
            projection_fixup_time_ms: self.projection_fixup_time_ms.load(Ordering::Relaxed),
            parquet_poll_time_ms: self.parquet_poll_time_ms.load(Ordering::Relaxed),
        }
    }
}

#[inline]
fn ns_to_ms(ns: usize) -> i64 {
    (ns / 1_000_000) as i64
}

/// Process-global accumulator. Read at FFM stats time, written from the
/// partition stream's `Drop` impl.
pub static SEARCH_STATS: Lazy<SearchStatsAccumulator> =
    Lazy::new(SearchStatsAccumulator::new);

/// Snapshot the global accumulator into the FFM wire format.
pub fn pack_search_stats() -> SearchStatsRepr {
    SEARCH_STATS.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::metrics::PartitionMetrics;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    #[test]
    fn accumulate_empty_stream_metrics_only_bumps_queries_completed() {
        let acc = SearchStatsAccumulator::new();
        acc.accumulate(&StreamMetrics::empty());
        assert_eq!(acc.queries_completed.load(Ordering::Relaxed), 1);
        assert_eq!(acc.output_rows.load(Ordering::Relaxed), 0);
        assert_eq!(acc.parquet_time_ms.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn accumulate_reads_count_and_time_values() {
        let acc = SearchStatsAccumulator::new();
        let metrics_set = ExecutionPlanMetricsSet::new();
        let pm = PartitionMetrics::new(&metrics_set, 0);

        pm.output_rows.add(100);
        pm.rows_matched.add(80);
        pm.row_groups_processed.add(2);
        pm.ffm_collector_calls.add(5);
        pm.parquet_time
            .add_duration(std::time::Duration::from_millis(250));
        pm.elapsed_compute
            .add_duration(std::time::Duration::from_millis(500));

        let stream_metrics = pm.into_stream_metrics(None);
        acc.accumulate(&stream_metrics);

        assert_eq!(acc.queries_completed.load(Ordering::Relaxed), 1);
        assert_eq!(acc.output_rows.load(Ordering::Relaxed), 100);
        assert_eq!(acc.rows_matched.load(Ordering::Relaxed), 80);
        assert_eq!(acc.row_groups_processed.load(Ordering::Relaxed), 2);
        assert_eq!(acc.ffm_collector_calls.load(Ordering::Relaxed), 5);
        assert_eq!(acc.parquet_time_ms.load(Ordering::Relaxed), 250);
        assert_eq!(acc.elapsed_compute_ms.load(Ordering::Relaxed), 500);

        let pm2 = PartitionMetrics::new(&metrics_set, 1);
        pm2.output_rows.add(50);
        let sm2 = pm2.into_stream_metrics(None);
        acc.accumulate(&sm2);
        assert_eq!(acc.queries_completed.load(Ordering::Relaxed), 2);
        assert_eq!(acc.output_rows.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn snapshot_returns_repr_with_all_fields() {
        let acc = SearchStatsAccumulator::new();
        acc.queries_completed.store(7, Ordering::Relaxed);
        acc.output_rows.store(1234, Ordering::Relaxed);
        acc.parquet_time_ms.store(99, Ordering::Relaxed);

        let repr = acc.snapshot();
        assert_eq!(repr.queries_completed, 7);
        assert_eq!(repr.output_rows, 1234);
        assert_eq!(repr.parquet_time_ms, 99);
        assert_eq!(repr.rows_matched, 0);
    }
}
