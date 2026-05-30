/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Process-wide native runtime metrics for the parquet merge path.
//!
//! Exposes counters around the rayon merge pool and a snapshot of the tokio IO runtime metrics.
//! All atomics use Relaxed ordering — values are monotonic-counter or last-write-wins; readers
//! may see slightly stale values but never corrupt state.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::error::MergeError;

// =============================================================================
// Atomic counters around the rayon merge pool
// =============================================================================
// Atomic counters around the rayon merge pool
//
// Note: `RAYON_MERGE_WALL_MILLIS` is the cumulative wall-clock duration of every wrapped
// merge operation, including the parallel column-encoding work itself — NOT just the time
// spent waiting to install work on the rayon pool. The previous name `*_install_wait_millis`
// was misleading; saturation is better inferred from the ratio of wall time to thread count.
// =============================================================================

static RAYON_SUBMITTED: AtomicU64 = AtomicU64::new(0);
static RAYON_COMPLETED: AtomicU64 = AtomicU64::new(0);
static RAYON_PANICKED: AtomicU64 = AtomicU64::new(0);
static RAYON_MERGE_WALL_MILLIS: AtomicU64 = AtomicU64::new(0);

// =============================================================================
// Snapshot collected on every stats request
// =============================================================================

#[derive(Default, Debug, Clone, Copy)]
pub struct NativeRuntimeStats {
    pub rayon_configured_threads: i64,
    pub rayon_merge_tasks_submitted: i64,
    pub rayon_merge_tasks_completed: i64,
    pub rayon_merge_tasks_panicked: i64,
    pub rayon_merge_wall_millis: i64,
    pub tokio_num_workers: i64,
    pub tokio_num_blocking_threads: i64,
    pub tokio_active_tasks: i64,
    pub tokio_global_queue_depth: i64,
    pub tokio_spawned_tasks_total: i64,
    pub tokio_workers_busy_millis_total: i64,
}

/// Snapshot the current state of the rayon merge pool and tokio IO runtime.
/// Returns all-zeros if neither pool has been initialized yet (no merge has run).
pub fn collect() -> NativeRuntimeStats {
    let mut s = NativeRuntimeStats::default();

    // Rayon — only if MERGE_POOL has been initialized
    if let Some(pool) = super::io_task::MERGE_POOL.get() {
        s.rayon_configured_threads = pool.current_num_threads() as i64;
        s.rayon_merge_tasks_submitted = RAYON_SUBMITTED.load(Ordering::Relaxed) as i64;
        s.rayon_merge_tasks_completed = RAYON_COMPLETED.load(Ordering::Relaxed) as i64;
        s.rayon_merge_tasks_panicked = RAYON_PANICKED.load(Ordering::Relaxed) as i64;
        s.rayon_merge_wall_millis = RAYON_MERGE_WALL_MILLIS.load(Ordering::Relaxed) as i64;
    }

    // Tokio — only if IO_RUNTIME has been initialized
    if let Some(rt) = super::io_task::IO_RUNTIME.get() {
        let m = rt.metrics();
        s.tokio_num_workers = m.num_workers() as i64;
        s.tokio_num_blocking_threads = m.num_blocking_threads() as i64;
        s.tokio_active_tasks = m.num_alive_tasks() as i64;
        s.tokio_global_queue_depth = m.global_queue_depth() as i64;
        s.tokio_spawned_tasks_total = m.spawned_tasks_count() as i64;
        let busy_millis: i64 = (0..m.num_workers())
            .map(|i| m.worker_total_busy_duration(i).as_millis() as i64)
            .sum();
        s.tokio_workers_busy_millis_total = busy_millis;
    }

    s
}

/// Wraps a rayon-pool-using closure to record submission/completion/panic + wait time.
///
/// Counter semantics:
/// - `submitted` = number of times this wrapper was entered.
/// - `completed` = number of times the closure returned `Ok(_)`.
/// - `panicked`  = number of times the closure unwound (panicked); the panic is then re-raised.
/// - On `Err(_)`, neither `completed` nor `panicked` is incremented; `submitted - completed - panicked`
///   is the count of merges that terminated with a logical error.
/// - `merge_wall_millis` accumulates total wall-clock duration of every wrapped call (the work
///   itself, plus any rayon-pool install overhead, plus errored and panicked invocations).
pub fn record_merge<F, R>(f: F) -> Result<R, MergeError>
where
    F: FnOnce() -> Result<R, MergeError>,
{
    RAYON_SUBMITTED.fetch_add(1, Ordering::Relaxed);
    let start = Instant::now();
    // catch_unwind so a panic increments RAYON_PANICKED before being re-raised. AssertUnwindSafe
    // is acceptable here because the only state we touch in the closure is local to it.
    let result = catch_unwind(AssertUnwindSafe(f));
    let elapsed_ms = start.elapsed().as_millis() as u64;
    RAYON_MERGE_WALL_MILLIS.fetch_add(elapsed_ms, Ordering::Relaxed);

    match result {
        Ok(Ok(r)) => {
            RAYON_COMPLETED.fetch_add(1, Ordering::Relaxed);
            Ok(r)
        }
        Ok(Err(e)) => Err(e),
        Err(panic_payload) => {
            RAYON_PANICKED.fetch_add(1, Ordering::Relaxed);
            std::panic::resume_unwind(panic_payload);
        }
    }
}
