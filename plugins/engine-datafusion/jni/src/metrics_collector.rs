/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! MetricsCollector reads cumulative runtime metrics directly from
//! `tokio::runtime::Handle::metrics()` (requires `tokio_unstable`).
//!
//! Returns a flat `[i64; 6]` array using the layout constants from
//! `crate::metrics_layout`, ready for embedding into the JNI `jlongArray`.

use tokio::runtime::Handle;
use crate::metrics_layout;

/// Wraps a cloned `Handle` and reads cumulative metrics on demand.
pub struct MetricsCollector {
    handle: Handle,
}

impl MetricsCollector {
    /// Creates a new collector for the given runtime handle.
    pub fn new(handle: &Handle) -> Self {
        Self {
            handle: handle.clone(),
        }
    }

    /// Reads current cumulative metrics directly from the runtime,
    /// returning a `[i64; 6]` array indexed by the `metrics_layout::RUNTIME_*`
    /// constants, ready for flat-copy into the JNI long array.
    ///
    /// Uses `Handle::metrics()` which returns atomic counter snapshots —
    /// no interval tracking or accumulation needed.
    /// Fallback when `tokio_unstable` is not set — returns zeroed metrics
    /// so the crate still compiles outside the normal Gradle/Cargo pipeline.
    #[cfg(not(tokio_unstable))]
    pub fn snapshot(&self) -> [i64; metrics_layout::RUNTIME_SIZE] {
        [0i64; metrics_layout::RUNTIME_SIZE]
    }

    #[cfg(tokio_unstable)]
    pub fn snapshot(&self) -> [i64; metrics_layout::RUNTIME_SIZE] {
        let m = self.handle.metrics();
        let n = m.num_workers();

        // Only collect actionable metrics — totals across all workers.
        let mut total_overflow: u64 = 0;
        let mut total_polls: u64 = 0;
        let mut total_busy_ns: u128 = 0;

        for i in 0..n {
            total_overflow += m.worker_overflow_count(i);
            total_polls += m.worker_poll_count(i);
            total_busy_ns += m.worker_total_busy_duration(i).as_nanos();
        }

        let mut arr = [0i64; metrics_layout::RUNTIME_SIZE];
        arr[metrics_layout::RUNTIME_WORKERS_COUNT] = n as i64;
        arr[metrics_layout::RUNTIME_TOTAL_POLLS_COUNT] = total_polls as i64;
        // Intentionally truncates sub-ms precision; matches Java-side millisecond granularity.
        // Overflow after division is not a practical concern (~292M years of cumulative busy time).
        arr[metrics_layout::RUNTIME_TOTAL_BUSY_DURATION_MS] = (total_busy_ns / 1_000_000) as i64;
        arr[metrics_layout::RUNTIME_TOTAL_OVERFLOW_COUNT] = total_overflow as i64;
        arr[metrics_layout::RUNTIME_GLOBAL_QUEUE_DEPTH] = m.global_queue_depth() as i64;
        arr[metrics_layout::RUNTIME_BLOCKING_QUEUE_DEPTH] = m.blocking_queue_depth() as i64;
        arr
    }
}
