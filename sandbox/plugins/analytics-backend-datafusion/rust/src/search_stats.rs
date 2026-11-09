/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cumulative search execution counters exposed via `df_stats()`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};

use crate::indexed_table::metrics::StreamMetrics;
use crate::indexed_table::parquet_bridge::ReadIoStats;
use crate::stats::SearchStatsRepr;

/// Monotonic counters folded from per-query metrics. The process-wide instance is
/// `GLOBAL`; tests instantiate their own so they stay isolated from concurrently
/// running queries.
#[derive(Debug)]
pub struct SearchStatsCounters {
    listing_table_scan: AtomicI64,
    single_collector_scan: AtomicI64,
    bitmap_tree_scan: AtomicI64,
    delegation_calls: AtomicI64,
    rg_processed: AtomicI64,
    rg_skipped: AtomicI64,
    parquet_scan_total_time_ms: AtomicI64,
    parquet_scan_until_data_time_ms: AtomicI64,
    parquet_processing_time_ms: AtomicI64,
    parquet_bytes_scanned: AtomicI64,
    prefetch_wait_time_ms: AtomicI64,
    prefetch_wait_count: AtomicI64,
    elapsed_compute_ms: AtomicI64,
    build_mask_time_ms: AtomicI64,
    on_batch_mask_time_ms: AtomicI64,
    filter_record_batch_time_ms: AtomicI64,
    object_store_read_time_ms: AtomicI64,
}

static GLOBAL: SearchStatsCounters = SearchStatsCounters::new();

impl SearchStatsCounters {
    pub const fn new() -> Self {
        Self {
            listing_table_scan: AtomicI64::new(0),
            single_collector_scan: AtomicI64::new(0),
            bitmap_tree_scan: AtomicI64::new(0),
            delegation_calls: AtomicI64::new(0),
            rg_processed: AtomicI64::new(0),
            rg_skipped: AtomicI64::new(0),
            parquet_scan_total_time_ms: AtomicI64::new(0),
            parquet_scan_until_data_time_ms: AtomicI64::new(0),
            parquet_processing_time_ms: AtomicI64::new(0),
            parquet_bytes_scanned: AtomicI64::new(0),
            prefetch_wait_time_ms: AtomicI64::new(0),
            prefetch_wait_count: AtomicI64::new(0),
            elapsed_compute_ms: AtomicI64::new(0),
            build_mask_time_ms: AtomicI64::new(0),
            on_batch_mask_time_ms: AtomicI64::new(0),
            filter_record_batch_time_ms: AtomicI64::new(0),
            object_store_read_time_ms: AtomicI64::new(0),
        }
    }

    pub fn inc_listing_table_scan(&self) {
        self.listing_table_scan.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_single_collector_scan(&self) {
        self.single_collector_scan.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_bitmap_tree_scan(&self) {
        self.bitmap_tree_scan.fetch_add(1, Ordering::Relaxed);
    }

    pub fn accumulate(&self, m: &StreamMetrics) {
        if let Some(ref c) = m.ffm_collector_calls {
            self.delegation_calls
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.rg_processed {
            self.rg_processed
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.rg_skipped {
            self.rg_skipped
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref acc) = m.inner_parquet_metrics {
            if let Ok(sets) = acc.lock() {
                self.accumulate_inner_parquet_metrics(&sets);
            }
        }
        if let Some(ref t) = m.prefetch_wait_time {
            self.prefetch_wait_time_ms
                .fetch_add((t.value() / 1_000_000) as i64, Ordering::Relaxed);
        }
        if let Some(ref c) = m.prefetch_wait_count {
            self.prefetch_wait_count
                .fetch_add(c.value() as i64, Ordering::Relaxed);
        }
        if let Some(ref t) = m.elapsed_compute {
            self.elapsed_compute_ms
                .fetch_add((t.value() / 1_000_000) as i64, Ordering::Relaxed);
        }
        if let Some(ref t) = m.build_mask_time {
            self.build_mask_time_ms
                .fetch_add((t.value() / 1_000_000) as i64, Ordering::Relaxed);
        }
        if let Some(ref t) = m.on_batch_mask_time {
            self.on_batch_mask_time_ms
                .fetch_add((t.value() / 1_000_000) as i64, Ordering::Relaxed);
        }
        if let Some(ref t) = m.filter_record_batch_time {
            self.filter_record_batch_time_ms
                .fetch_add((t.value() / 1_000_000) as i64, Ordering::Relaxed);
        }
        if let Some(ref stats) = m.io_stats {
            self.object_store_read_time_ms.fetch_add(
                (stats.total_ns.load(Ordering::Relaxed) / 1_000_000) as i64,
                Ordering::Relaxed,
            );
        }
    }

    /// Accumulate from a `QueryShardExec`'s aggregated metrics at query completion.
    pub fn accumulate_from_exec(
        &self,
        metrics: &ExecutionPlanMetricsSet,
        inner_parquet_metrics: &Arc<Mutex<Vec<MetricsSet>>>,
        io_stats: &ReadIoStats,
    ) {
        let aggregated = metrics.clone_inner().aggregate_by_name();

        let count = |name: &str| -> i64 {
            aggregated
                .iter()
                .find(|m| m.value().name() == name)
                .map(|m| m.value().as_usize() as i64)
                .unwrap_or(0)
        };
        let time_ms = |name: &str| -> i64 {
            aggregated
                .iter()
                .find(|m| m.value().name() == name)
                .map(|m| (m.value().as_usize() / 1_000_000) as i64)
                .unwrap_or(0)
        };

        self.delegation_calls
            .fetch_add(count("ffm_collector_calls"), Ordering::Relaxed);
        self.rg_processed
            .fetch_add(count("row_groups_processed"), Ordering::Relaxed);
        self.rg_skipped
            .fetch_add(count("row_groups_skipped"), Ordering::Relaxed);
        self.prefetch_wait_time_ms
            .fetch_add(time_ms("prefetch_wait_time"), Ordering::Relaxed);
        self.prefetch_wait_count
            .fetch_add(count("prefetch_wait_count"), Ordering::Relaxed);
        self.elapsed_compute_ms
            .fetch_add(time_ms("elapsed_compute"), Ordering::Relaxed);
        self.build_mask_time_ms
            .fetch_add(time_ms("build_mask_time"), Ordering::Relaxed);
        self.on_batch_mask_time_ms
            .fetch_add(time_ms("on_batch_mask_time"), Ordering::Relaxed);
        self.filter_record_batch_time_ms
            .fetch_add(time_ms("filter_record_batch_time"), Ordering::Relaxed);

        if let Ok(sets) = inner_parquet_metrics.lock() {
            self.accumulate_inner_parquet_metrics(&sets);
        }

        self.object_store_read_time_ms.fetch_add(
            (io_stats.total_ns.load(Ordering::Relaxed) / 1_000_000) as i64,
            Ordering::Relaxed,
        );
    }

    fn accumulate_inner_parquet_metrics(&self, sets: &[MetricsSet]) {
        self.parquet_scan_total_time_ms.fetch_add(
            (sum_inner_metric_ns(sets, "time_elapsed_scanning_total") / 1_000_000) as i64,
            Ordering::Relaxed,
        );
        self.parquet_scan_until_data_time_ms.fetch_add(
            (sum_inner_metric_ns(sets, "time_elapsed_scanning_until_data") / 1_000_000) as i64,
            Ordering::Relaxed,
        );
        self.parquet_processing_time_ms.fetch_add(
            (sum_inner_metric_ns(sets, "time_elapsed_processing") / 1_000_000) as i64,
            Ordering::Relaxed,
        );
        self.parquet_bytes_scanned.fetch_add(
            sum_inner_metric_ns(sets, "bytes_scanned") as i64,
            Ordering::Relaxed,
        );
    }

    pub fn snapshot(&self) -> SearchStatsRepr {
        SearchStatsRepr {
            listing_table_scan: self.listing_table_scan.load(Ordering::Relaxed),
            single_collector_scan: self.single_collector_scan.load(Ordering::Relaxed),
            bitmap_tree_scan: self.bitmap_tree_scan.load(Ordering::Relaxed),
            delegation_calls: self.delegation_calls.load(Ordering::Relaxed),
            rg_processed: self.rg_processed.load(Ordering::Relaxed),
            rg_skipped: self.rg_skipped.load(Ordering::Relaxed),
            parquet_scan_total_time_ms: self.parquet_scan_total_time_ms.load(Ordering::Relaxed),
            parquet_scan_until_data_time_ms: self
                .parquet_scan_until_data_time_ms
                .load(Ordering::Relaxed),
            parquet_processing_time_ms: self.parquet_processing_time_ms.load(Ordering::Relaxed),
            parquet_bytes_scanned: self.parquet_bytes_scanned.load(Ordering::Relaxed),
            prefetch_wait_time_ms: self.prefetch_wait_time_ms.load(Ordering::Relaxed),
            prefetch_wait_count: self.prefetch_wait_count.load(Ordering::Relaxed),
            elapsed_compute_ms: self.elapsed_compute_ms.load(Ordering::Relaxed),
            build_mask_time_ms: self.build_mask_time_ms.load(Ordering::Relaxed),
            on_batch_mask_time_ms: self.on_batch_mask_time_ms.load(Ordering::Relaxed),
            filter_record_batch_time_ms: self.filter_record_batch_time_ms.load(Ordering::Relaxed),
            object_store_read_time_ms: self.object_store_read_time_ms.load(Ordering::Relaxed),
        }
    }
}

impl Default for SearchStatsCounters {
    fn default() -> Self {
        Self::new()
    }
}

pub fn inc_listing_table_scan() {
    GLOBAL.inc_listing_table_scan();
}

pub fn inc_single_collector_scan() {
    GLOBAL.inc_single_collector_scan();
}

pub fn inc_bitmap_tree_scan() {
    GLOBAL.inc_bitmap_tree_scan();
}

pub fn sum_inner_metric_ns(sets: &[MetricsSet], name: &str) -> u64 {
    let mut total = 0u64;
    for set in sets {
        let mut best = 0usize;
        for metric in set.iter() {
            if metric.value().name() == name {
                best = best.max(metric.value().as_usize());
            }
        }
        total += best as u64;
    }
    total
}

pub fn accumulate(m: &StreamMetrics) {
    GLOBAL.accumulate(m);
}

/// Accumulate from a `QueryShardExec`'s aggregated metrics at query completion.
pub fn accumulate_from_exec(
    metrics: &ExecutionPlanMetricsSet,
    inner_parquet_metrics: &Arc<Mutex<Vec<MetricsSet>>>,
    io_stats: &ReadIoStats,
) {
    GLOBAL.accumulate_from_exec(metrics, inner_parquet_metrics, io_stats);
}

pub fn snapshot() -> SearchStatsRepr {
    GLOBAL.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::metrics::PartitionMetrics;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    #[test]
    fn path_counters_increment() {
        let counters = SearchStatsCounters::new();
        counters.inc_listing_table_scan();
        counters.inc_single_collector_scan();
        counters.inc_single_collector_scan();
        counters.inc_bitmap_tree_scan();
        let stats = counters.snapshot();
        assert_eq!(stats.listing_table_scan, 1);
        assert_eq!(stats.single_collector_scan, 2);
        assert_eq!(stats.bitmap_tree_scan, 1);
    }

    #[test]
    fn accumulate_folds_partition_metrics() {
        let counters = SearchStatsCounters::new();
        let metrics_set = ExecutionPlanMetricsSet::new();
        let pm = PartitionMetrics::new(&metrics_set, 0);

        pm.elapsed_compute
            .add_duration(std::time::Duration::from_millis(50));
        pm.ffm_collector_calls.add(3);
        pm.row_groups_processed.add(2);
        pm.row_groups_skipped.add(1);
        pm.prefetch_wait_time
            .add_duration(std::time::Duration::from_millis(10));
        pm.prefetch_wait_count.add(2);

        counters.accumulate(&pm.into_stream_metrics(None));
        let stats = counters.snapshot();

        assert_eq!(stats.delegation_calls, 3);
        assert_eq!(stats.rg_processed, 2);
        assert_eq!(stats.rg_skipped, 1);
        assert!(stats.prefetch_wait_time_ms >= 10);
        assert_eq!(stats.prefetch_wait_count, 2);
    }

    /// A counter set folds exactly what it was given, no matter what the rest of the process does to
    /// the global counters at the same time. Regression test for the flake this file used to produce:
    /// the counters were process-global and these tests measured a before/after delta, so a
    /// concurrent `accumulate` (cargo runs test functions on parallel threads in one process, and
    /// `indexed_table::tests_e2e`'s queries fold through `accumulate_from_exec`) landed inside the
    /// measurement window and inflated it — `8 != 3`.
    #[test]
    fn counters_are_isolated_from_concurrent_global_activity() {
        let counters = SearchStatsCounters::new();

        let metrics_set = ExecutionPlanMetricsSet::new();
        let pm = PartitionMetrics::new(&metrics_set, 0);
        pm.ffm_collector_calls.add(3);

        // Stand in for any other query folding its metrics into the process-wide counters.
        std::thread::spawn(|| {
            let other_set = ExecutionPlanMetricsSet::new();
            let other_pm = PartitionMetrics::new(&other_set, 0);
            other_pm.ffm_collector_calls.add(5);
            accumulate(&other_pm.into_stream_metrics(None));
        })
        .join()
        .unwrap();

        counters.accumulate(&pm.into_stream_metrics(None));

        assert_eq!(counters.snapshot().delegation_calls, 3);
    }

    #[test]
    fn empty_stream_metrics_is_safe() {
        let counters = SearchStatsCounters::new();
        counters.accumulate(&StreamMetrics::empty());
        assert_eq!(counters.snapshot().delegation_calls, 0);
    }
}
