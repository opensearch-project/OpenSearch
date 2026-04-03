/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-query memory tracking via DataFusion's MemoryPool trait.
//!
//! Each query gets its own `QueryMemoryPool` that wraps the global pool.
//! All allocations flow through the global pool (so the global limit is
//! still enforced), but each query also tracks its own current and peak
//! usage independently.
//!
//! This avoids the need for thread-local tricks or a custom global allocator —
//! DataFusion's cooperative memory management does the bookkeeping for us.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use dashmap::DashMap;
use once_cell::sync::Lazy;
use vectorized_exec_spi::log_info;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::common::DataFusionError;

// ---------------------------------------------------------------------------
// Global pool limit — set once at createGlobalRuntime, read by QueryMemoryPool
// ---------------------------------------------------------------------------

static POOL_LIMIT: OnceLock<usize> = OnceLock::new();

/// Set the global memory pool limit. Called once from createGlobalRuntime.
pub fn set_pool_limit(limit: usize) {
    POOL_LIMIT.get_or_init(|| limit);
}

fn pool_limit() -> usize {
    *POOL_LIMIT.get().unwrap_or(&0)
}

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
/// The global pool enforces the overall memory limit, while this wrapper
/// gives per-query visibility.
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
                // Inner pool said no — check mimalloc's committed memory against
                // the configured pool limit as a fallback. current_commit reflects
                // memory actually reserved by mimalloc (not process-wide RSS).
                let (current_rss, _) = get_mimalloc_rss();
                if current_rss.saturating_add(additional) <= pool_limit() {
                    log_info!(
                        "QueryMemoryPool: inner pool denied {}B, but mimalloc has headroom \
                         (rss={}MB, limit={}MB) — allowing",
                        additional,
                        current_rss / (1024 * 1024),
                        pool_limit() / (1024 * 1024),
                    );
                    self.inner.grow(reservation, additional);
                    self.track_grow(additional);
                    Ok(())
                } else {
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

/// Holds per-query state: the memory pool, wall-clock start time, and
/// completion status.
///
/// Lifecycle (mirrors Java's TaskResourceTrackingService pattern):
///   1. `start_query_tracking` → inserts tracker with `completed = false`
///   2. `stop_query_tracking`  → snapshots wall time, sets `completed = true`,
///      logs final stats, keeps tracker in registry for Java to retrieve via JNI
///
/// Stats remain readable after completion:
///   - `peak_bytes`: high-water mark, immutable once set — the key metric
///   - `current_bytes`: drifts to ~0 after stream drop (correct — cleanup happened)
///   - `wall_secs()`: frozen at stop time via `wall_nanos`
#[derive(Debug)]
pub struct QueryTracker {
    /// Wall-clock instant when this query started.
    pub start_time: Instant,
    /// The context_id for this query.
    pub context_id: i64,
    /// The per-query memory pool (also installed in the RuntimeEnv).
    pub memory_pool: Arc<QueryMemoryPool>,
    /// Whether this query has completed (stop_query_tracking was called).
    completed: AtomicBool,
    /// Wall-clock duration snapshotted at completion.
    /// Stored as nanos in an AtomicU64 (0 = not yet completed).
    wall_nanos: std::sync::atomic::AtomicU64,
}

impl QueryTracker {
    /// Wall-clock duration. Returns the frozen snapshot if completed,
    /// otherwise returns live elapsed time.
    pub fn wall_secs(&self) -> f64 {
        let nanos = self.wall_nanos.load(Ordering::Relaxed);
        if nanos > 0 {
            nanos as f64 / 1_000_000_000.0
        } else {
            self.start_time.elapsed().as_secs_f64()
        }
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
        completed: AtomicBool::new(false),
        wall_nanos: std::sync::atomic::AtomicU64::new(0),
    });
    QUERY_TRACKERS.insert(context_id, tracker);
    Some(query_pool)
}

/// Mark a query as completed and log its final metrics.
/// The tracker stays in the registry so Java can retrieve it via JNI.
/// This mirrors Java's TaskResourceTrackingService pattern where stats are
/// snapshotted onto the Task object before removal, and listeners consume
/// the enriched object.
///
/// Called from streamClose or on error paths.
pub fn stop_query_tracking(context_id: i64) {
    let stats = QUERY_TRACKERS.get(&context_id).map(|tracker| {
        let elapsed_nanos = tracker.start_time.elapsed().as_nanos() as u64;
        tracker.wall_nanos.store(elapsed_nanos, Ordering::Relaxed);
        tracker.completed.store(true, Ordering::Release);
        (
            elapsed_nanos as f64 / 1_000_000_000.0,
            tracker.memory_pool.current_bytes(),
            tracker.memory_pool.peak_bytes(),
        )
    });
    // Log after releasing the DashMap read guard
    if let Some((wall, current, peak)) = stats {
        log_info!(
            "Query completed ctx={}: wall={:.3}s, mem_current={}B, mem_peak={}B",
            context_id, wall, current, peak,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use std::thread;
    use std::time::Duration;

    /// Helper: create a global pool with the given limit.
    fn make_global_pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(limit))
    }

    /// Helper: create a MemoryConsumer registered with the pool and return its reservation.
    fn make_reservation(pool: &Arc<dyn MemoryPool>, name: &str) -> MemoryReservation {
        MemoryConsumer::new(name).register(pool)
    }

    // -----------------------------------------------------------------------
    // QueryMemoryPool tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_query_pool_tracks_current_and_peak() {
        let global = make_global_pool(1_000_000); // large enough to never trigger fallback
        let qp = Arc::new(QueryMemoryPool::new(global));
        let pool: Arc<dyn MemoryPool> = qp.clone();
        let mut reservation = make_reservation(&pool, "test");

        reservation.try_grow(1000).unwrap();
        assert_eq!(qp.current_bytes(), 1000);
        assert_eq!(qp.peak_bytes(), 1000);

        reservation.try_grow(500).unwrap();
        assert_eq!(qp.current_bytes(), 1500);
        assert_eq!(qp.peak_bytes(), 1500);

        // Shrink 800 → current=700, peak still 1500
        reservation.shrink(800);
        assert_eq!(qp.current_bytes(), 700);
        assert_eq!(qp.peak_bytes(), 1500);

        // Grow 200 → current=900, peak still 1500
        reservation.try_grow(200).unwrap();
        assert_eq!(qp.current_bytes(), 900);
        assert_eq!(qp.peak_bytes(), 1500);
    }

    #[test]
    fn test_query_pool_current_returns_to_zero_on_drop() {
        let global = make_global_pool(1_000_000);
        let qp = Arc::new(QueryMemoryPool::new(global));
        let pool: Arc<dyn MemoryPool> = qp.clone();

        {
            let mut reservation = make_reservation(&pool, "test");
            reservation.try_grow(5000).unwrap();
            assert_eq!(qp.current_bytes(), 5000);
            assert_eq!(qp.peak_bytes(), 5000);
        } // reservation dropped here — triggers shrink via Drop

        assert_eq!(qp.current_bytes(), 0);
        assert_eq!(qp.peak_bytes(), 5000);
    }

    #[test]
    fn test_query_pool_delegates_reserved_to_inner() {
        let global = make_global_pool(1_000_000);
        let qp = Arc::new(QueryMemoryPool::new(global));
        let pool: Arc<dyn MemoryPool> = qp.clone();
        let mut reservation = make_reservation(&pool, "test");

        reservation.try_grow(2000).unwrap();
        assert!(pool.reserved() >= 2000);
    }

    // -----------------------------------------------------------------------
    // QueryTracker lifecycle tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_start_tracking_returns_none_for_zero_context() {
        let global = make_global_pool(10_000);
        let result = start_query_tracking(0, global);
        assert!(result.is_none());
    }

    #[test]
    fn test_start_tracking_returns_pool_for_valid_context() {
        let global = make_global_pool(10_000);
        let ctx_id = 42_000;
        let result = start_query_tracking(ctx_id, global);
        assert!(result.is_some());
        assert!(QUERY_TRACKERS.contains_key(&ctx_id));

        // Cleanup
        QUERY_TRACKERS.remove(&ctx_id);
    }

    #[test]
    fn test_stop_tracking_marks_completed_and_freezes_wall_time() {
        let global = make_global_pool(10_000);
        let ctx_id = 42_001;
        let _pool = start_query_tracking(ctx_id, global);

        thread::sleep(Duration::from_millis(50));
        stop_query_tracking(ctx_id);

        // Use a block so the Ref guard is dropped before remove()
        {
            let tracker = QUERY_TRACKERS.get(&ctx_id).unwrap();
            assert!(tracker.completed.load(Ordering::Relaxed));
            assert!(tracker.wall_nanos.load(Ordering::Relaxed) > 0);

            let frozen = tracker.wall_secs();
            thread::sleep(Duration::from_millis(50));
            let after_sleep = tracker.wall_secs();
            assert!((after_sleep - frozen).abs() < 0.001);
        }

        QUERY_TRACKERS.remove(&ctx_id);
    }

    #[test]
    fn test_wall_secs_ticks_while_running() {
        let global = make_global_pool(10_000);
        let ctx_id = 42_002;
        let _pool = start_query_tracking(ctx_id, global);

        let t1 = { QUERY_TRACKERS.get(&ctx_id).unwrap().wall_secs() };
        thread::sleep(Duration::from_millis(50));
        let t2 = { QUERY_TRACKERS.get(&ctx_id).unwrap().wall_secs() };

        assert!(t2 - t1 >= 0.04);

        stop_query_tracking(ctx_id);
        QUERY_TRACKERS.remove(&ctx_id);
    }

    #[test]
    fn test_stop_tracking_nonexistent_context_is_noop() {
        stop_query_tracking(99_999);
    }

    #[test]
    fn test_tracker_stays_in_registry_after_stop() {
        let global = make_global_pool(10_000);
        let ctx_id = 42_003;
        let _pool = start_query_tracking(ctx_id, global);

        stop_query_tracking(ctx_id);
        assert!(QUERY_TRACKERS.contains_key(&ctx_id));

        // Cleanup
        QUERY_TRACKERS.remove(&ctx_id);
    }

    #[test]
    fn test_memory_tracking_through_full_lifecycle() {
        let global = make_global_pool(1_000_000);
        let ctx_id = 42_004;
        let qp = start_query_tracking(ctx_id, global).unwrap();
        let pool: Arc<dyn MemoryPool> = qp.clone();
        let mut reservation = make_reservation(&pool, "lifecycle_test");

        reservation.try_grow(5000).unwrap();
        assert_eq!(qp.current_bytes(), 5000);
        assert_eq!(qp.peak_bytes(), 5000);

        reservation.try_grow(3000).unwrap();
        assert_eq!(qp.current_bytes(), 8000);
        assert_eq!(qp.peak_bytes(), 8000);

        reservation.shrink(6000);
        assert_eq!(qp.current_bytes(), 2000);
        assert_eq!(qp.peak_bytes(), 8000);

        stop_query_tracking(ctx_id);

        // Block scope so Ref guard is dropped before remove()
        {
            let tracker = QUERY_TRACKERS.get(&ctx_id).unwrap();
            assert_eq!(tracker.memory_pool.current_bytes(), 2000);
            assert_eq!(tracker.memory_pool.peak_bytes(), 8000);
            assert!(tracker.wall_secs() > 0.0);
        }

        // Drop reservation — current goes to 0 via MemoryReservation::Drop, peak stays
        drop(reservation);
        assert_eq!(qp.current_bytes(), 0);
        assert_eq!(qp.peak_bytes(), 8000);

        QUERY_TRACKERS.remove(&ctx_id);
    }

    #[test]
    fn test_multiple_concurrent_queries() {
        let global = make_global_pool(1_000_000);
        let ctx_a = 42_005;
        let ctx_b = 42_006;

        let qp_a = start_query_tracking(ctx_a, Arc::clone(&global)).unwrap();
        let qp_b = start_query_tracking(ctx_b, Arc::clone(&global)).unwrap();

        let pool_a: Arc<dyn MemoryPool> = qp_a.clone();
        let pool_b: Arc<dyn MemoryPool> = qp_b.clone();

        let mut res_a = make_reservation(&pool_a, "query_a");
        res_a.try_grow(3000).unwrap();

        let mut res_b = make_reservation(&pool_b, "query_b");
        res_b.try_grow(7000).unwrap();

        // Each query tracks independently
        assert_eq!(qp_a.current_bytes(), 3000);
        assert_eq!(qp_a.peak_bytes(), 3000);
        assert_eq!(qp_b.current_bytes(), 7000);
        assert_eq!(qp_b.peak_bytes(), 7000);

        // Global pool sees the sum
        assert!(global.reserved() >= 10000);

        // Stop one, other keeps running
        stop_query_tracking(ctx_a);
        {
            assert!(QUERY_TRACKERS.get(&ctx_a).unwrap().completed.load(Ordering::Relaxed));
        }
        {
            assert!(!QUERY_TRACKERS.get(&ctx_b).unwrap().completed.load(Ordering::Relaxed));
        }

        // Cleanup
        drop(res_a);
        drop(res_b);
        QUERY_TRACKERS.remove(&ctx_a);
        QUERY_TRACKERS.remove(&ctx_b);
    }

    #[test]
    fn test_pool_limit_is_set_and_readable() {
        set_pool_limit(1_000_000);
        assert!(pool_limit() > 0);
    }
}
