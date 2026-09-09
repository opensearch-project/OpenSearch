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

static LISTING_TABLE_SCAN: AtomicI64 = AtomicI64::new(0);
static SINGLE_COLLECTOR_SCAN: AtomicI64 = AtomicI64::new(0);
static BITMAP_TREE_SCAN: AtomicI64 = AtomicI64::new(0);
static DELEGATION_CALLS: AtomicI64 = AtomicI64::new(0);
static RG_PROCESSED: AtomicI64 = AtomicI64::new(0);
static RG_SKIPPED: AtomicI64 = AtomicI64::new(0);
static PARQUET_SCAN_TOTAL_TIME_MS: AtomicI64 = AtomicI64::new(0);
static PARQUET_SCAN_UNTIL_DATA_TIME_MS: AtomicI64 = AtomicI64::new(0);
static PARQUET_PROCESSING_TIME_MS: AtomicI64 = AtomicI64::new(0);
static PARQUET_BYTES_SCANNED: AtomicI64 = AtomicI64::new(0);
static PREFETCH_WAIT_TIME_MS: AtomicI64 = AtomicI64::new(0);
static PREFETCH_WAIT_COUNT: AtomicI64 = AtomicI64::new(0);
static ELAPSED_COMPUTE_MS: AtomicI64 = AtomicI64::new(0);
static BUILD_MASK_TIME_MS: AtomicI64 = AtomicI64::new(0);
static ON_BATCH_MASK_TIME_MS: AtomicI64 = AtomicI64::new(0);
static FILTER_RECORD_BATCH_TIME_MS: AtomicI64 = AtomicI64::new(0);
static OBJECT_STORE_READ_TIME_MS: AtomicI64 = AtomicI64::new(0);

pub fn inc_listing_table_scan() {
    LISTING_TABLE_SCAN.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_single_collector_scan() {
    SINGLE_COLLECTOR_SCAN.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_bitmap_tree_scan() {
    BITMAP_TREE_SCAN.fetch_add(1, Ordering::Relaxed);
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
    if let Some(ref c) = m.ffm_collector_calls {
        DELEGATION_CALLS.fetch_add(c.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref c) = m.rg_processed {
        RG_PROCESSED.fetch_add(c.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref c) = m.rg_skipped {
        RG_SKIPPED.fetch_add(c.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref acc) = m.inner_parquet_metrics {
        if let Ok(sets) = acc.lock() {
            PARQUET_SCAN_TOTAL_TIME_MS.fetch_add(
                (sum_inner_metric_ns(&sets, "time_elapsed_scanning_total") / 1_000_000) as i64,
                Ordering::Relaxed,
            );
            PARQUET_SCAN_UNTIL_DATA_TIME_MS.fetch_add(
                (sum_inner_metric_ns(&sets, "time_elapsed_scanning_until_data") / 1_000_000) as i64,
                Ordering::Relaxed,
            );
            PARQUET_PROCESSING_TIME_MS.fetch_add(
                (sum_inner_metric_ns(&sets, "time_elapsed_processing") / 1_000_000) as i64,
                Ordering::Relaxed,
            );
            PARQUET_BYTES_SCANNED.fetch_add(
                sum_inner_metric_ns(&sets, "bytes_scanned") as i64,
                Ordering::Relaxed,
            );
        }
    }
    if let Some(ref t) = m.prefetch_wait_time {
        PREFETCH_WAIT_TIME_MS.fetch_add((t.value() / 1_000_000) as i64, Ordering::Relaxed);
    }
    if let Some(ref c) = m.prefetch_wait_count {
        PREFETCH_WAIT_COUNT.fetch_add(c.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.elapsed_compute {
        ELAPSED_COMPUTE_MS.fetch_add((t.value() / 1_000_000) as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.build_mask_time {
        BUILD_MASK_TIME_MS.fetch_add((t.value() / 1_000_000) as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.on_batch_mask_time {
        ON_BATCH_MASK_TIME_MS.fetch_add((t.value() / 1_000_000) as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.filter_record_batch_time {
        FILTER_RECORD_BATCH_TIME_MS.fetch_add((t.value() / 1_000_000) as i64, Ordering::Relaxed);
    }
    if let Some(ref stats) = m.io_stats {
        OBJECT_STORE_READ_TIME_MS.fetch_add(
            (stats.total_ns.load(Ordering::Relaxed) / 1_000_000) as i64,
            Ordering::Relaxed,
        );
    }
}

/// Accumulate from a `QueryShardExec`'s aggregated metrics at query completion.
pub fn accumulate_from_exec(
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

    DELEGATION_CALLS.fetch_add(count("ffm_collector_calls"), Ordering::Relaxed);
    RG_PROCESSED.fetch_add(count("row_groups_processed"), Ordering::Relaxed);
    RG_SKIPPED.fetch_add(count("row_groups_skipped"), Ordering::Relaxed);
    PREFETCH_WAIT_TIME_MS.fetch_add(time_ms("prefetch_wait_time"), Ordering::Relaxed);
    PREFETCH_WAIT_COUNT.fetch_add(count("prefetch_wait_count"), Ordering::Relaxed);
    ELAPSED_COMPUTE_MS.fetch_add(time_ms("elapsed_compute"), Ordering::Relaxed);
    BUILD_MASK_TIME_MS.fetch_add(time_ms("build_mask_time"), Ordering::Relaxed);
    ON_BATCH_MASK_TIME_MS.fetch_add(time_ms("on_batch_mask_time"), Ordering::Relaxed);
    FILTER_RECORD_BATCH_TIME_MS.fetch_add(time_ms("filter_record_batch_time"), Ordering::Relaxed);

    if let Ok(sets) = inner_parquet_metrics.lock() {
        PARQUET_SCAN_TOTAL_TIME_MS.fetch_add(
            (sum_inner_metric_ns(&sets, "time_elapsed_scanning_total") / 1_000_000) as i64,
            Ordering::Relaxed,
        );
        PARQUET_SCAN_UNTIL_DATA_TIME_MS.fetch_add(
            (sum_inner_metric_ns(&sets, "time_elapsed_scanning_until_data") / 1_000_000) as i64,
            Ordering::Relaxed,
        );
        PARQUET_PROCESSING_TIME_MS.fetch_add(
            (sum_inner_metric_ns(&sets, "time_elapsed_processing") / 1_000_000) as i64,
            Ordering::Relaxed,
        );
        PARQUET_BYTES_SCANNED.fetch_add(
            sum_inner_metric_ns(&sets, "bytes_scanned") as i64,
            Ordering::Relaxed,
        );
    }

    OBJECT_STORE_READ_TIME_MS.fetch_add(
        (io_stats.total_ns.load(Ordering::Relaxed) / 1_000_000) as i64,
        Ordering::Relaxed,
    );
}

pub fn snapshot() -> SearchStatsRepr {
    SearchStatsRepr {
        listing_table_scan: LISTING_TABLE_SCAN.load(Ordering::Relaxed),
        single_collector_scan: SINGLE_COLLECTOR_SCAN.load(Ordering::Relaxed),
        bitmap_tree_scan: BITMAP_TREE_SCAN.load(Ordering::Relaxed),
        delegation_calls: DELEGATION_CALLS.load(Ordering::Relaxed),
        rg_processed: RG_PROCESSED.load(Ordering::Relaxed),
        rg_skipped: RG_SKIPPED.load(Ordering::Relaxed),
        parquet_scan_total_time_ms: PARQUET_SCAN_TOTAL_TIME_MS.load(Ordering::Relaxed),
        parquet_scan_until_data_time_ms: PARQUET_SCAN_UNTIL_DATA_TIME_MS.load(Ordering::Relaxed),
        parquet_processing_time_ms: PARQUET_PROCESSING_TIME_MS.load(Ordering::Relaxed),
        parquet_bytes_scanned: PARQUET_BYTES_SCANNED.load(Ordering::Relaxed),
        prefetch_wait_time_ms: PREFETCH_WAIT_TIME_MS.load(Ordering::Relaxed),
        prefetch_wait_count: PREFETCH_WAIT_COUNT.load(Ordering::Relaxed),
        elapsed_compute_ms: ELAPSED_COMPUTE_MS.load(Ordering::Relaxed),
        build_mask_time_ms: BUILD_MASK_TIME_MS.load(Ordering::Relaxed),
        on_batch_mask_time_ms: ON_BATCH_MASK_TIME_MS.load(Ordering::Relaxed),
        filter_record_batch_time_ms: FILTER_RECORD_BATCH_TIME_MS.load(Ordering::Relaxed),
        object_store_read_time_ms: OBJECT_STORE_READ_TIME_MS.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::metrics::PartitionMetrics;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    #[test]
    fn path_counters_increment() {
        let before = snapshot();
        inc_listing_table_scan();
        inc_single_collector_scan();
        inc_single_collector_scan();
        inc_bitmap_tree_scan();
        let after = snapshot();
        assert_eq!(after.listing_table_scan - before.listing_table_scan, 1);
        assert_eq!(
            after.single_collector_scan - before.single_collector_scan,
            2
        );
        assert_eq!(after.bitmap_tree_scan - before.bitmap_tree_scan, 1);
    }

    #[test]
    fn accumulate_folds_partition_metrics() {
        let before = snapshot();
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

        accumulate(&pm.into_stream_metrics(None));
        let after = snapshot();

        assert_eq!(after.delegation_calls - before.delegation_calls, 3);
        assert_eq!(after.rg_processed - before.rg_processed, 2);
        assert_eq!(after.rg_skipped - before.rg_skipped, 1);
        assert!(after.prefetch_wait_time_ms - before.prefetch_wait_time_ms >= 10);
        assert_eq!(after.prefetch_wait_count - before.prefetch_wait_count, 2);
    }

    #[test]
    fn empty_stream_metrics_is_safe() {
        let before = snapshot();
        accumulate(&StreamMetrics::empty());
        let after = snapshot();
        assert_eq!(after.delegation_calls, before.delegation_calls);
    }
}
