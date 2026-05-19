/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-query memory tracking via DataFusion's MemoryPool trait.
//!
//! Each query gets its own [`QueryMemoryPool`] that wraps the global pool.
//! All allocations flow through the global pool (so the global limit is
//! still enforced), but each query also tracks its own current and peak
//! usage independently.
//!
//! [`QueryTrackingContext`] owns the per-query pool and tracker, auto-registers
//! in the global [`QueryRegistry`] on creation, and removes the entry
//! on [`Drop`].

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use dashmap::DashMap;
use log::debug;
use once_cell::sync::Lazy;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;

use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};

// ---------------------------------------------------------------------------
// Per-query memory pool
// ---------------------------------------------------------------------------

/// A per-query MemoryPool that delegates to a shared global pool while
/// independently tracking this query's current and peak memory usage.
#[derive(Debug)]
pub struct QueryMemoryPool {
    inner: Arc<dyn MemoryPool>,
    current_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
}

impl QueryMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            current_bytes: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
        }
    }

    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    pub fn peak_bytes(&self) -> usize {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    fn track_grow(&self, additional: usize) {
        let old = self.current_bytes.fetch_add(additional, Ordering::Relaxed);
        self.peak_bytes
            .fetch_max(old + additional, Ordering::Relaxed);
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

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        self.inner.try_grow(reservation, additional)?;
        self.track_grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

// ---------------------------------------------------------------------------
// Per-query tracker (metrics snapshot)
// ---------------------------------------------------------------------------

/// Holds per-query metrics: memory pool reference, wall-clock timing, and
/// completion status. Shared via `Arc` between the context and the registry.
#[derive(Debug)]
pub struct QueryTracker {
    pub start_time: Instant,
    pub context_id: i64,
    pub memory_pool: Arc<QueryMemoryPool>,
    pub cancellation_token: CancellationToken,
    /// CPU task abort handle, set after the stream is created.
    pub abort_handle: OnceLock<AbortHandle>,
    completed: AtomicBool,
    wall_nanos: std::sync::atomic::AtomicU64,
}

impl QueryTracker {
    /// Wall-clock duration. Returns the frozen snapshot if completed,
    /// otherwise returns live elapsed time.
    pub fn wall_secs(&self) -> f64 {
        let nanos = self.wall_nanos.load(Ordering::Acquire);
        if nanos > 0 {
            nanos as f64 / 1_000_000_000.0
        } else {
            self.start_time.elapsed().as_secs_f64()
        }
    }

    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    /// Snapshot wall time and mark completed.
    fn mark_completed(&self) {
        let elapsed_nanos = self.start_time.elapsed().as_nanos() as u64;
        self.wall_nanos.store(elapsed_nanos, Ordering::Release);
        self.completed.store(true, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Global registry
// ---------------------------------------------------------------------------

static QUERY_REGISTRY: Lazy<DashMap<i64, Arc<QueryTracker>>> = Lazy::new(DashMap::new);

/// Fire the cancellation token for the given context_id.
/// No-op for unknown or already-completed queries.
pub fn cancel_query(context_id: i64) {
    if let Some(tracker) = QUERY_REGISTRY.get(&context_id) {
        tracker.cancellation_token.cancel();
        if let Some(handle) = tracker.abort_handle.get() {
            handle.abort();
        }
    }
}

/// Clone the cancellation token for the given context_id, if registered.
pub fn get_cancellation_token(context_id: i64) -> Option<CancellationToken> {
    QUERY_REGISTRY.get(&context_id).map(|t| t.cancellation_token.clone())
}

/// Store the CPU task's AbortHandle for the given context_id.
pub fn set_abort_handle(context_id: i64, handle: AbortHandle) {
    if let Some(tracker) = QUERY_REGISTRY.get(&context_id) {
        tracker.abort_handle.set(handle).ok();
    }
}

// ---------------------------------------------------------------------------
// QueryTrackingContext
// ---------------------------------------------------------------------------

/// Per-query context that owns the memory pool and tracker.
///
/// - On creation: registers the tracker in the global registry.
/// - On [`Drop`]: removes the tracker from the registry and logs final metrics.
///
/// For `context_id == 0` (unset), no tracking is performed.
#[derive(Debug)]
pub struct QueryTrackingContext {
    tracker: Option<Arc<QueryTracker>>,
}

impl QueryTrackingContext {
    /// Create a new query context. If `context_id` is 0, tracking is
    /// disabled and `memory_pool()` returns `None`.
    pub fn new(context_id: i64, global_pool: Arc<dyn MemoryPool>) -> Self {
        if context_id == 0 {
            return Self { tracker: None };
        }
        let query_pool = Arc::new(QueryMemoryPool::new(global_pool));
        let tracker = Arc::new(QueryTracker {
            start_time: Instant::now(),
            context_id,
            memory_pool: query_pool,
            // CancellationToken is a thread-safe, cloneable handle that can be used to
            // signal cancellation to async tasks via `token.cancelled().await` in a
            // `tokio::select!` branch. Calling `token.cancel()` fires all waiters.
            // See: https://github.com/tokio-rs/tokio/blob/master/tokio-util/src/sync/cancellation_token/tree_node.rs
            cancellation_token: CancellationToken::new(),
            abort_handle: OnceLock::new(),
            completed: AtomicBool::new(false),
            wall_nanos: std::sync::atomic::AtomicU64::new(0),
        });
        QUERY_REGISTRY.insert(context_id, Arc::clone(&tracker));
        Self {
            tracker: Some(tracker),
        }
    }

    /// The per-query memory pool to install in a `RuntimeEnv`, or `None`
    /// if tracking is disabled.
    pub fn memory_pool(&self) -> Option<Arc<QueryMemoryPool>> {
        self.tracker.as_ref().map(|t| Arc::clone(&t.memory_pool))
    }

    /// The context_id for this query, or 0 if tracking is disabled.
    pub fn context_id(&self) -> i64 {
        self.tracker.as_ref().map_or(0, |t| t.context_id)
    }
}

impl Drop for QueryTrackingContext {
    fn drop(&mut self) {
        if let Some(tracker) = &self.tracker {
            tracker.mark_completed();
            debug!(
                "Query completed ctx={}: wall={:.3}s, mem_current={}B, mem_peak={}B",
                tracker.context_id,
                tracker.wall_secs(),
                tracker.memory_pool.current_bytes(),
                tracker.memory_pool.peak_bytes(),
            );
            QUERY_REGISTRY.remove(&tracker.context_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use std::thread;
    use std::time::Duration;

    fn make_global_pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(limit))
    }

    fn make_reservation(pool: &Arc<dyn MemoryPool>, name: &str) -> MemoryReservation {
        MemoryConsumer::new(name).register(pool)
    }

    // -----------------------------------------------------------------------
    // QueryMemoryPool tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_query_pool_tracks_current_and_peak() {
        let global = make_global_pool(1_000_000);
        let qp = Arc::new(QueryMemoryPool::new(global));
        let pool: Arc<dyn MemoryPool> = qp.clone();
        let mut reservation = make_reservation(&pool, "test");

        reservation.try_grow(1000).unwrap();
        assert_eq!(qp.current_bytes(), 1000);
        assert_eq!(qp.peak_bytes(), 1000);

        reservation.try_grow(500).unwrap();
        assert_eq!(qp.current_bytes(), 1500);
        assert_eq!(qp.peak_bytes(), 1500);

        reservation.shrink(800);
        assert_eq!(qp.current_bytes(), 700);
        assert_eq!(qp.peak_bytes(), 1500);

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
        }

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
    // QueryTrackingContext lifecycle tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_context_returns_none_pool_for_zero_id() {
        let global = make_global_pool(10_000);
        let ctx = QueryTrackingContext::new(0, global);
        assert!(ctx.memory_pool().is_none());
    }

    #[test]
    fn test_context_registers_and_removes_on_drop() {
        let global = make_global_pool(10_000);
        let ctx_id = 50_000;
        let ctx = QueryTrackingContext::new(ctx_id, global);
        assert!(ctx.memory_pool().is_some());
        assert!(QUERY_REGISTRY.contains_key(&ctx_id));

        drop(ctx);
        // Removed from registry after drop
        assert!(!QUERY_REGISTRY.contains_key(&ctx_id));
    }

    #[test]
    fn test_drop_removes_from_registry() {
        let global = make_global_pool(10_000);
        let ctx_id = 50_001;
        let ctx = QueryTrackingContext::new(ctx_id, global);

        assert!(QUERY_REGISTRY.contains_key(&ctx_id));
        thread::sleep(Duration::from_millis(50));
        drop(ctx);

        // Entry is gone after drop
        assert!(!QUERY_REGISTRY.contains_key(&ctx_id));
    }

    #[test]
    fn test_wall_secs_ticks_while_running() {
        let global = make_global_pool(10_000);
        let ctx_id = 50_002;
        let _ctx = QueryTrackingContext::new(ctx_id, global);

        let t1 = QUERY_REGISTRY.get(&ctx_id).unwrap().wall_secs();
        thread::sleep(Duration::from_millis(50));
        let t2 = QUERY_REGISTRY.get(&ctx_id).unwrap().wall_secs();
        assert!(t2 - t1 >= 0.04);

        drop(_ctx);
        // drop removes from registry automatically
    }

    #[test]
    fn test_memory_tracking_through_full_lifecycle() {
        let global = make_global_pool(1_000_000);
        let ctx_id = 50_004;
        let ctx = QueryTrackingContext::new(ctx_id, global);
        let qp = ctx.memory_pool().unwrap();
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

        // Drop context — removes from registry
        drop(ctx);
        assert!(!QUERY_REGISTRY.contains_key(&ctx_id));

        // Pool still works (Arc kept alive by qp)
        assert_eq!(qp.peak_bytes(), 8000);

        drop(reservation);
        assert_eq!(qp.current_bytes(), 0);
    }

    #[test]
    fn test_multiple_concurrent_queries() {
        let global = make_global_pool(1_000_000);
        let ctx_a_id = 50_005;
        let ctx_b_id = 50_006;

        let ctx_a = QueryTrackingContext::new(ctx_a_id, Arc::clone(&global));
        let ctx_b = QueryTrackingContext::new(ctx_b_id, Arc::clone(&global));

        let pool_a = ctx_a.memory_pool().unwrap();
        let pool_b = ctx_b.memory_pool().unwrap();

        let mut res_a = make_reservation(&(pool_a.clone() as Arc<dyn MemoryPool>), "query_a");
        res_a.try_grow(3000).unwrap();

        let mut res_b = make_reservation(&(pool_b.clone() as Arc<dyn MemoryPool>), "query_b");
        res_b.try_grow(7000).unwrap();

        assert_eq!(pool_a.current_bytes(), 3000);
        assert_eq!(pool_b.current_bytes(), 7000);
        assert!(global.reserved() >= 10000);

        // Drop one, other keeps running
        drop(ctx_a);
        assert!(!QUERY_REGISTRY.contains_key(&ctx_a_id));
        assert!(QUERY_REGISTRY.contains_key(&ctx_b_id));

        drop(res_a);
        drop(res_b);
        drop(ctx_b);
        assert!(!QUERY_REGISTRY.contains_key(&ctx_b_id));
    }

    // -----------------------------------------------------------------------
    // Query lifecycle tests (simulating stream completion and error paths)
    // -----------------------------------------------------------------------

    #[test]
    fn test_context_removes_on_normal_drop_with_stream() {
        // Simulates: query succeeds → stream is consumed → handle dropped
        let global = make_global_pool(1_000_000);
        let ctx_id = 50_010;

        let ctx = QueryTrackingContext::new(ctx_id, global);
        let qp = ctx.memory_pool().unwrap();
        let pool: Arc<dyn MemoryPool> = qp.clone();
        let mut reservation = make_reservation(&pool, "stream_data");

        // Simulate allocations during stream consumption
        reservation.try_grow(4000).unwrap();
        assert_eq!(qp.peak_bytes(), 4000);
        assert!(QUERY_REGISTRY.contains_key(&ctx_id));

        // Stream fully consumed — reservation and context dropped together
        drop(reservation);
        drop(ctx);

        // Removed from registry
        assert!(!QUERY_REGISTRY.contains_key(&ctx_id));
        // Pool stats still accessible via Arc
        assert_eq!(qp.peak_bytes(), 4000);
        assert_eq!(qp.current_bytes(), 0);
    }

    #[test]
    fn test_context_removes_on_error_drop() {
        // Simulates: query execution fails → context dropped without
        // explicit cleanup (the error path in executeQueryPhaseAsync)
        let global = make_global_pool(1_000_000);
        let ctx_id = 50_011;

        {
            let ctx = QueryTrackingContext::new(ctx_id, global);
            let _pool = ctx.memory_pool();
            assert!(QUERY_REGISTRY.contains_key(&ctx_id));
        } // ctx dropped here — removes from registry

        assert!(!QUERY_REGISTRY.contains_key(&ctx_id));
    }
}
