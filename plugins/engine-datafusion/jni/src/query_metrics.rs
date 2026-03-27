/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-query memory tracking via DataFusion's MemoryPool trait.
//!
//! Inspired by InfluxDB's per-query allocator approach: each query gets its own
//! `QueryMemoryPool` that wraps the global pool. All allocations flow through
//! the global pool (so the global limit is still enforced), but each query also
//! tracks its own current and peak usage independently.
//!
//! This avoids the need for thread-local tricks or a custom global allocator —
//! DataFusion's cooperative memory management does the bookkeeping for us.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use once_cell::sync::Lazy;
use vectorized_exec_spi::log_info;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::common::DataFusionError;

// ---------------------------------------------------------------------------
// mimalloc RSS check
// ---------------------------------------------------------------------------

/// Query mimalloc for the current process RSS via `mi_process_info`.
/// Returns `(current_rss, peak_rss)` in bytes.
fn get_mimalloc_rss() -> (usize, usize) {
    let mut elapsed_ms: usize = 0;
    let mut user_ms: usize = 0;
    let mut system_ms: usize = 0;
    let mut current_rss: usize = 0;
    let mut peak_rss: usize = 0;
    let mut current_commit: usize = 0;
    let mut peak_commit: usize = 0;
    let mut page_faults: usize = 0;

    unsafe {
        libmimalloc_sys::mi_process_info(
            &mut elapsed_ms,
            &mut user_ms,
            &mut system_ms,
            &mut current_rss,
            &mut peak_rss,
            &mut current_commit,
            &mut peak_commit,
            &mut page_faults,
        );
    }

    (current_rss, peak_rss)
}

// ---------------------------------------------------------------------------
// Per-query memory pool
// ---------------------------------------------------------------------------

/// A per-query MemoryPool that delegates to a shared global pool while
/// independently tracking this query's current and peak memory usage.
///
/// Similar to InfluxDB's per-query allocator pattern: the global pool enforces
/// the overall memory limit, while this wrapper gives per-query visibility.
#[derive(Debug)]
pub struct QueryMemoryPool {
    /// The shared global memory pool that enforces the overall limit.
    inner: Arc<dyn MemoryPool>,
    /// Current bytes reserved by this query.
    current_bytes: AtomicUsize,
    /// Peak bytes reserved by this query.
    peak_bytes: AtomicUsize,
}

impl QueryMemoryPool {
    /// Create a new per-query pool that delegates to the given global pool.
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            current_bytes: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
        }
    }

    /// Current bytes reserved by this query.
    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Peak bytes reserved by this query over its lifetime.
    pub fn peak_bytes(&self) -> usize {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    fn track_grow(&self, additional: usize) {
        let old = self.current_bytes.fetch_add(additional, Ordering::Relaxed);
        self.peak_bytes.fetch_max(old + additional, Ordering::Relaxed);
    }

    fn track_shrink(&self, shrink: usize) {
        self.current_bytes.fetch_sub(shrink, Ordering::Relaxed);
    }
}

impl MemoryPool for QueryMemoryPool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.track_grow(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.track_shrink(shrink);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<(), DataFusionError> {
        match self.inner.try_grow(reservation, additional) {
            Ok(()) => {
                self.track_grow(additional);
                Ok(())
            }
            Err(e) => {
                // Inner pool said no — check mimalloc process RSS as a fallback.
                let (current_rss, peak_rss) = get_mimalloc_rss();
                if current_rss.saturating_add(additional) <= peak_rss {
                    // mimalloc says there's room (RSS + request fits within peak),
                    // allow the allocation bypassing the cooperative pool limit.
                    log_info!(
                        "QueryMemoryPool: inner pool denied {}B, but mimalloc has headroom \
                         (rss={}MB, peak={}MB) — allowing",
                        additional,
                        current_rss / (1024 * 1024),
                        peak_rss / (1024 * 1024),
                    );
                    self.inner.grow(reservation, additional);
                    self.track_grow(additional);
                    Ok(())
                } else {
                    // mimalloc also says no — propagate the original error.
                    Err(e)
                }
            }
        }
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

// ---------------------------------------------------------------------------
// Per-query tracker (metrics + pool reference)
// ---------------------------------------------------------------------------

/// Holds per-query state: the memory pool and wall-clock start time.
#[derive(Debug)]
pub struct QueryTracker {
    /// Wall-clock instant when this query started.
    pub start_time: Instant,
    /// The context_id for this query.
    pub context_id: i64,
    /// The per-query memory pool (also installed in the RuntimeEnv).
    pub memory_pool: Arc<QueryMemoryPool>,
}

impl QueryTracker {
    pub fn wall_secs(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }
}

// ---------------------------------------------------------------------------
// Global registry
// ---------------------------------------------------------------------------

/// Global registry: context_id → QueryTracker
static QUERY_TRACKERS: Lazy<DashMap<i64, Arc<QueryTracker>>> = Lazy::new(DashMap::new);

/// Start tracking a query. Creates a per-query `QueryMemoryPool` wrapping the
/// given global pool. Returns the per-query pool (to install in RuntimeEnv)
/// and registers the tracker in the global registry.
///
/// Returns `None` for context_id == 0 (unset — no tracking).
pub fn start_query_tracking(
    context_id: i64,
    global_pool: Arc<dyn MemoryPool>,
) -> Option<Arc<QueryMemoryPool>> {
    if context_id == 0 {
        return None;
    }
    let query_pool = Arc::new(QueryMemoryPool::new(global_pool));
    let tracker = Arc::new(QueryTracker {
        start_time: Instant::now(),
        context_id,
        memory_pool: query_pool.clone(),
    });
    log_info!("Query memory tracking started: context_id={}", context_id);
    QUERY_TRACKERS.insert(context_id, tracker);
    Some(query_pool)
}

/// Stop tracking and log final metrics. Call from streamClose or on error.
/// If a global Monitor is provided, also logs overall memory pool usage.
pub fn stop_query_tracking(context_id: i64) -> Option<Arc<QueryTracker>> {
    QUERY_TRACKERS.remove(&context_id).map(|(_, tracker)| {
        log_info!(
            "Query ctx={} completed: wall={:.3}s, mem_current={}B, mem_peak={}B",
            context_id,
            tracker.wall_secs(),
            tracker.memory_pool.current_bytes(),
            tracker.memory_pool.peak_bytes(),
        );
        tracker
    })
}

/// Look up a running query's tracker.
pub fn get_query_tracker(context_id: i64) -> Option<Arc<QueryTracker>> {
    QUERY_TRACKERS.get(&context_id).map(|e| e.value().clone())
}

/// Log a summary of all currently tracked queries. Called from the monitoring loop.
pub fn log_active_queries() {
    let count = QUERY_TRACKERS.len();
    if count == 0 {
        return;
    }
    log_info!("=== Active Query Metrics ({} queries) ===", count);
    for entry in QUERY_TRACKERS.iter() {
        let id = entry.key();
        let t = entry.value();
        log_info!(
            "  Query ctx={}: wall={:.3}s, mem_current={}B, mem_peak={}B",
            id,
            t.wall_secs(),
            t.memory_pool.current_bytes(),
            t.memory_pool.peak_bytes(),
        );
    }
    log_info!("=============================================");
}
