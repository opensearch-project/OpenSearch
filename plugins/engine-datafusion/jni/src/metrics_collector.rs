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
//! Returns the prost-generated `RuntimeMetrics` proto struct directly,
//! avoiding an intermediate Rust type.

use tokio::runtime::Handle;
use crate::datafusion_proto;

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
    /// returning the proto `RuntimeMetrics` ready for serialization.
    ///
    /// Uses `Handle::metrics()` which returns atomic counter snapshots —
    /// no interval tracking or accumulation needed.
    /// Fallback when `tokio_unstable` is not set — returns zeroed metrics
    /// so the crate still compiles outside the normal Gradle/Cargo pipeline.
    #[cfg(not(tokio_unstable))]
    pub fn snapshot(&self) -> datafusion_proto::RuntimeMetrics {
        datafusion_proto::RuntimeMetrics::default()
    }

    #[cfg(tokio_unstable)]
    pub fn snapshot(&self) -> datafusion_proto::RuntimeMetrics {
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

        datafusion_proto::RuntimeMetrics {
            workers_count: n as u64,
            total_overflow_count: total_overflow,
            total_polls_count: total_polls,
            total_busy_duration_ms: (total_busy_ns / 1_000_000) as u64,
            global_queue_depth: m.global_queue_depth() as u64,
            blocking_queue_depth: m.blocking_queue_depth() as u64,
        }
    }
}
