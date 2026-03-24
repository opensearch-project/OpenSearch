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

        // Per-worker aggregation
        let mut total_park: u64 = 0;
        let mut max_park: u64 = 0;
        let mut min_park: u64 = u64::MAX;

        let mut total_noop: u64 = 0;
        let mut max_noop: u64 = 0;
        let mut min_noop: u64 = u64::MAX;

        let mut total_steal: u64 = 0;
        let mut max_steal: u64 = 0;
        let mut min_steal: u64 = u64::MAX;

        let mut total_steal_ops: u64 = 0;

        let mut total_local_sched: u64 = 0;
        let mut max_local_sched: u64 = 0;
        let mut min_local_sched: u64 = u64::MAX;

        let mut total_overflow: u64 = 0;
        let mut max_overflow: u64 = 0;
        let mut min_overflow: u64 = u64::MAX;

        let mut total_polls: u64 = 0;
        let mut max_polls: u64 = 0;
        let mut min_polls: u64 = u64::MAX;

        let mut total_busy_ns: u128 = 0;
        let mut max_busy_ns: u128 = 0;
        let mut min_busy_ns: u128 = u128::MAX;

        let mut total_local_depth: usize = 0;
        let mut max_local_depth: usize = 0;
        let mut min_local_depth: usize = usize::MAX;

        for i in 0..n {
            let park = m.worker_park_count(i);
            total_park += park;
            max_park = max_park.max(park);
            min_park = min_park.min(park);

            let noop = m.worker_noop_count(i);
            total_noop += noop;
            max_noop = max_noop.max(noop);
            min_noop = min_noop.min(noop);

            let steal = m.worker_steal_count(i);
            total_steal += steal;
            max_steal = max_steal.max(steal);
            min_steal = min_steal.min(steal);

            total_steal_ops += m.worker_steal_operations(i);

            let local_sched = m.worker_local_schedule_count(i);
            total_local_sched += local_sched;
            max_local_sched = max_local_sched.max(local_sched);
            min_local_sched = min_local_sched.min(local_sched);

            let overflow = m.worker_overflow_count(i);
            total_overflow += overflow;
            max_overflow = max_overflow.max(overflow);
            min_overflow = min_overflow.min(overflow);

            let polls = m.worker_poll_count(i);
            total_polls += polls;
            max_polls = max_polls.max(polls);
            min_polls = min_polls.min(polls);

            let busy = m.worker_total_busy_duration(i).as_nanos();
            total_busy_ns += busy;
            max_busy_ns = max_busy_ns.max(busy);
            min_busy_ns = min_busy_ns.min(busy);

            let depth = m.worker_local_queue_depth(i);
            total_local_depth += depth;
            max_local_depth = max_local_depth.max(depth);
            min_local_depth = min_local_depth.min(depth);
        }

        // Handle edge case: 0 workers
        if n == 0 {
            min_park = 0;
            min_noop = 0;
            min_steal = 0;
            min_local_sched = 0;
            min_overflow = 0;
            min_polls = 0;
            min_busy_ns = 0;
            min_local_depth = 0;
        }

        datafusion_proto::RuntimeMetrics {
            workers_count: n as u64,
            total_park_count: total_park,
            max_park_count: max_park,
            min_park_count: min_park,
            total_noop_count: total_noop,
            max_noop_count: max_noop,
            min_noop_count: min_noop,
            total_steal_count: total_steal,
            max_steal_count: max_steal,
            min_steal_count: min_steal,
            total_steal_operations: total_steal_ops,
            total_local_schedule_count: total_local_sched,
            max_local_schedule_count: max_local_sched,
            min_local_schedule_count: min_local_sched,
            total_overflow_count: total_overflow,
            max_overflow_count: max_overflow,
            min_overflow_count: min_overflow,
            total_polls_count: total_polls,
            max_polls_count: max_polls,
            min_polls_count: min_polls,
            total_busy_duration_ms: (total_busy_ns / 1_000_000) as u64,
            max_busy_duration_ms: (max_busy_ns / 1_000_000) as u64,
            min_busy_duration_ms: (min_busy_ns / 1_000_000) as u64,
            total_local_queue_depth: total_local_depth as u64,
            max_local_queue_depth: max_local_depth as u64,
            min_local_queue_depth: min_local_depth as u64,
            global_queue_depth: m.global_queue_depth() as u64,
            blocking_queue_depth: m.blocking_queue_depth() as u64,
        }
    }
}
