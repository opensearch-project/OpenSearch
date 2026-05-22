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
use std::time::{Duration, Instant};

use dashmap::DashMap;
use log::debug;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;

use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};

// ---------------------------------------------------------------------------
// Query type discriminator
// ---------------------------------------------------------------------------

/// Distinguishes shard-level queries from coordinator-level queries for stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    /// Data-node shard fragment execution (AnalyticsShardTask on the Java side).
    Shard,
    /// Coordinator-side local reduce execution (AnalyticsQueryTask on the Java side).
    Coordinator,
}

/// Default threshold for "long-running post-cancel" — matches the Java-side
/// `task_cancellation.duration_millis` default of 10 000 ms.
pub const DEFAULT_CANCEL_THRESHOLD: Duration = Duration::from_secs(10);

/// Runtime-configurable threshold. Initialized to DEFAULT_CANCEL_THRESHOLD (10_000 ms).
/// Set via `set_cancel_stats_threshold`.
static CANCEL_STATS_THRESHOLD_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(10_000);

/// Returns the currently configured cancellation stats threshold.
pub fn cancel_stats_threshold() -> Duration {
    Duration::from_millis(CANCEL_STATS_THRESHOLD_MS.load(Ordering::Relaxed))
}

/// Sets the cancellation stats threshold (in milliseconds).
pub fn set_cancel_stats_threshold(millis: u64) {
    CANCEL_STATS_THRESHOLD_MS.store(millis, Ordering::Relaxed);
}

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
    pub query_type: QueryType,
    pub memory_pool: Arc<QueryMemoryPool>,
    pub cancellation_token: CancellationToken,
    /// CPU task abort handle, set after the stream is created.
    pub abort_handle: OnceLock<AbortHandle>,
    /// Instant when cancellation was signalled, or None if not cancelled.
    pub cancelled_at: Mutex<Option<Instant>>,
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
        {
            let mut cancelled_at = tracker.cancelled_at.lock();
            if cancelled_at.is_none() {
                *cancelled_at = Some(Instant::now());
            }
        }
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

/// Counts queries currently running past the cancellation threshold, by type.
/// Returns (shard_current, coordinator_current).
///
/// Uses `try_lock` on the per-entry mutex to avoid holding a lock during
/// DashMap iteration — if contended (cancel_query running concurrently),
/// the entry is skipped (conservative undercount for that instant).
pub fn count_cancelled_running(threshold: Duration) -> (i64, i64) {
    let mut shard_count: i64 = 0;
    let mut coordinator_count: i64 = 0;
    for entry in QUERY_REGISTRY.iter() {
        let tracker = entry.value();
        if let Some(guard) = tracker.cancelled_at.try_lock() {
            if let Some(cancelled_at) = *guard {
                if cancelled_at.elapsed() >= threshold {
                    match tracker.query_type {
                        QueryType::Shard => shard_count += 1,
                        QueryType::Coordinator => coordinator_count += 1,
                    }
                }
            }
        }
    }
    (shard_count, coordinator_count)
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
    phantom_reservation: Option<MemoryReservation>,
    phantom_corrector: Option<Arc<crate::phantom_corrector::PhantomCorrector>>,
}

impl QueryTrackingContext {
    /// Create a new query context. If `context_id` is 0, tracking is
    /// disabled and `memory_pool()` returns `None`.
    pub fn new(context_id: i64, global_pool: Arc<dyn MemoryPool>, query_type: QueryType) -> Self {
        if context_id == 0 {
            return Self { tracker: None, phantom_reservation: None, phantom_corrector: None };
        }
        let query_pool = Arc::new(QueryMemoryPool::new(global_pool));
        let tracker = Arc::new(QueryTracker {
            start_time: Instant::now(),
            context_id,
            query_type,
            memory_pool: query_pool,
            cancellation_token: CancellationToken::new(),
            abort_handle: OnceLock::new(),
            cancelled_at: Mutex::new(None),
            completed: AtomicBool::new(false),
            wall_nanos: std::sync::atomic::AtomicU64::new(0),
        });
        QUERY_REGISTRY.insert(context_id, Arc::clone(&tracker));
        Self {
            tracker: Some(tracker),
            phantom_reservation: None,
            phantom_corrector: None,
        }
    }

    /// Attach a phantom reservation for bounded memory accounting.
    pub fn set_phantom_reservation(&mut self, reservation: MemoryReservation) {
        self.phantom_reservation = Some(reservation);
    }

    /// Attach a phantom corrector and return an Arc clone for CrossRtStream.
    pub fn set_phantom_corrector(
        &mut self,
        corrector: Arc<crate::phantom_corrector::PhantomCorrector>,
    ) -> Arc<crate::phantom_corrector::PhantomCorrector> {
        let clone = Arc::clone(&corrector);
        self.phantom_corrector = Some(corrector);
        clone
    }

    /// Apply pending phantom correction from the self-correcting budget.
    pub fn apply_pending_phantom_correction(&mut self) {
        let (corrector, reservation) = match (&self.phantom_corrector, &mut self.phantom_reservation) {
            (Some(c), Some(r)) => (c, r),
            _ => return,
        };
        let delta = corrector.take_pending_delta();
        if delta == 0 {
            return;
        }
        if delta > 0 {
            let _ = reservation.try_grow(delta as usize);
        } else {
            let shrink = (-delta) as usize;
            let current = reservation.size();
            let floor = current / 10;
            let actual_shrink = shrink.min(current.saturating_sub(floor));
            if actual_shrink > 0 {
                reservation.shrink(actual_shrink);
            }
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

            // Remove from registry BEFORE incrementing total. This ensures a
            // query is never simultaneously visible in both the registry scan
            // (current) and the total counter — preventing double-counting in
            // snapshot_cancellation_stats.
            QUERY_REGISTRY.remove(&tracker.context_id);

            // If this query was cancelled and ran past the threshold, bump the total counter.
            if let Some(cancelled_at) = *tracker.cancelled_at.lock() {
                if cancelled_at.elapsed() >= cancel_stats_threshold() {
                    match tracker.query_type {
                        QueryType::Shard => {
                            crate::native_node_stats::inc_native_search_shard_task_total();
                        }
                        QueryType::Coordinator => {
                            crate::native_node_stats::inc_native_search_task_total();
                        }
                    }
                }
            }
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
        let ctx = QueryTrackingContext::new(0, global, QueryType::Shard);
        assert!(ctx.memory_pool().is_none());
    }

    #[test]
    fn test_context_registers_and_removes_on_drop() {
        let global = make_global_pool(10_000);
        let ctx_id = 50_000;
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);
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
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);

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
        let _ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);

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
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);
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

        let ctx_a = QueryTrackingContext::new(ctx_a_id, Arc::clone(&global), QueryType::Shard);
        let ctx_b = QueryTrackingContext::new(ctx_b_id, Arc::clone(&global), QueryType::Shard);

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

        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);
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
            let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);
            let _pool = ctx.memory_pool();
            assert!(QUERY_REGISTRY.contains_key(&ctx_id));
        } // ctx dropped here — removes from registry

        assert!(!QUERY_REGISTRY.contains_key(&ctx_id));
    }

    // -----------------------------------------------------------------------
    // Cancellation stats tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cancel_query_sets_cancelled_at() {
        let global = make_global_pool(10_000);
        let ctx_id = 60_001;
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);

        // Not cancelled yet
        let tracker = QUERY_REGISTRY.get(&ctx_id).unwrap();
        assert!(tracker.cancelled_at.lock().is_none());

        // Cancel
        cancel_query(ctx_id);

        // cancelled_at should be set
        assert!(tracker.cancelled_at.lock().is_some());
        drop(tracker);
        drop(ctx);
    }

    #[test]
    fn test_cancel_query_idempotent() {
        let global = make_global_pool(10_000);
        let ctx_id = 60_002;
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);

        cancel_query(ctx_id);
        let first = *QUERY_REGISTRY.get(&ctx_id).unwrap().cancelled_at.lock();

        thread::sleep(Duration::from_millis(10));
        cancel_query(ctx_id);
        let second = *QUERY_REGISTRY.get(&ctx_id).unwrap().cancelled_at.lock();

        // Second cancel should not overwrite the first timestamp
        assert_eq!(first, second);
        drop(ctx);
    }

    #[test]
    fn test_count_cancelled_running_with_zero_threshold() {
        let global = make_global_pool(10_000);
        let ctx_id = 60_003;
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);

        // Not cancelled — should not be counted
        let (shard, coord) = count_cancelled_running(Duration::ZERO);
        // (Other tests may have live entries, so just check this specific one)
        assert!(!is_counted(ctx_id, Duration::ZERO));

        // Cancel it
        cancel_query(ctx_id);

        // Now it should be counted with zero threshold
        assert!(is_counted(ctx_id, Duration::ZERO));

        drop(ctx);

        // After drop, not in registry
        assert!(!is_counted(ctx_id, Duration::ZERO));
    }

    #[test]
    fn test_count_cancelled_running_respects_threshold() {
        let global = make_global_pool(10_000);
        let ctx_id = 60_004;
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);

        cancel_query(ctx_id);

        // With a very large threshold, should NOT be counted (just cancelled)
        assert!(!is_counted(ctx_id, Duration::from_secs(9999)));

        // With zero threshold, should be counted
        assert!(is_counted(ctx_id, Duration::ZERO));

        drop(ctx);
    }

    #[test]
    fn test_count_cancelled_running_distinguishes_query_types() {
        let global = make_global_pool(10_000);
        let shard_id = 60_005;
        let coord_id = 60_006;

        let shard_ctx = QueryTrackingContext::new(shard_id, Arc::clone(&global), QueryType::Shard);
        let coord_ctx = QueryTrackingContext::new(coord_id, Arc::clone(&global), QueryType::Coordinator);

        cancel_query(shard_id);
        cancel_query(coord_id);

        let (shard_count, coord_count) = count_cancelled_running(Duration::ZERO);
        assert!(shard_count >= 1, "shard count should be >= 1, got {}", shard_count);
        assert!(coord_count >= 1, "coord count should be >= 1, got {}", coord_count);

        drop(shard_ctx);
        drop(coord_ctx);
    }

    #[test]
    fn test_drop_increments_total_when_past_threshold() {
        // Set threshold to 0 so any cancellation counts
        set_cancel_stats_threshold(0);

        let global = make_global_pool(10_000);
        let ctx_id = 60_007;

        // Read baseline total
        let baseline = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);

        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);
        cancel_query(ctx_id);

        // Drop should increment total (threshold is 0, elapsed > 0)
        drop(ctx);

        let after = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);
        assert_eq!(after, baseline + 1, "total should increment by 1");

        // Restore default
        set_cancel_stats_threshold(10_000);
    }

    #[test]
    fn test_drop_does_not_increment_total_when_under_threshold() {
        // Set threshold to a large value
        set_cancel_stats_threshold(999_999);

        let global = make_global_pool(10_000);
        let ctx_id = 60_008;

        let baseline = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);

        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);
        cancel_query(ctx_id);
        // Drop immediately — elapsed is well under 999s
        drop(ctx);

        let after = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);
        assert_eq!(after, baseline, "total should not increment when under threshold");

        // Restore default
        set_cancel_stats_threshold(10_000);
    }

    #[test]
    fn test_drop_does_not_increment_total_for_uncancelled_query() {
        // Use a unique high threshold so only our query's drop logic is tested
        // (other parallel tests with threshold=0 don't affect our assertion).
        set_cancel_stats_threshold(999_999);

        let global = make_global_pool(10_000);
        let ctx_id = 60_009;

        // Read baseline immediately before drop to minimize parallel interference
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);
        let baseline = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);
        // Don't cancel — just drop
        drop(ctx);

        let after = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);
        assert_eq!(after, baseline, "total should not increment for uncancelled query");

        set_cancel_stats_threshold(10_000);
    }

    #[test]
    fn test_timing_based_total_increment_with_real_delay() {
        // 50ms threshold — cancel, sleep 100ms, drop → should increment
        set_cancel_stats_threshold(50);

        let global = make_global_pool(10_000);
        let ctx_id = 60_020;

        let baseline = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);

        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);
        cancel_query(ctx_id);

        // Verify current count while still in registry
        assert!(is_counted(ctx_id, Duration::ZERO));
        // Not yet past 50ms threshold
        assert!(!is_counted(ctx_id, Duration::from_millis(50)));

        // Sleep past threshold
        thread::sleep(Duration::from_millis(100));

        // Now past threshold — should be counted
        assert!(is_counted(ctx_id, Duration::from_millis(50)));

        // Drop — should increment total
        drop(ctx);

        let after = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);
        assert_eq!(after, baseline + 1);

        set_cancel_stats_threshold(10_000);
    }

    #[test]
    fn test_current_count_live_while_query_registered() {
        set_cancel_stats_threshold(0);

        let global = make_global_pool(10_000);
        let ctx_id = 60_021;

        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);
        cancel_query(ctx_id);

        // While registered: current should include this query
        let (shard, _) = count_cancelled_running(Duration::ZERO);
        assert!(shard >= 1, "current should be >= 1 while registered, got {}", shard);

        drop(ctx);

        // After drop: this query should no longer be counted
        assert!(!QUERY_REGISTRY.contains_key(&ctx_id));

        set_cancel_stats_threshold(10_000);
    }

    #[test]
    fn test_drop_increments_correct_total_by_query_type() {
        set_cancel_stats_threshold(0);

        let global = make_global_pool(10_000);

        // Baseline for both counters
        let shard_baseline = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);
        let coord_baseline = crate::native_node_stats::NATIVE_SEARCH_TASK_TOTAL
            .load(Ordering::Relaxed);

        // Cancel and drop a shard query
        let shard_ctx = QueryTrackingContext::new(60_030, Arc::clone(&global), QueryType::Shard);
        cancel_query(60_030);
        drop(shard_ctx);

        // Shard total should increment, coordinator should not
        let shard_after = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);
        let coord_after = crate::native_node_stats::NATIVE_SEARCH_TASK_TOTAL
            .load(Ordering::Relaxed);
        assert_eq!(shard_after, shard_baseline + 1, "shard total should increment");
        assert_eq!(coord_after, coord_baseline, "coordinator total should not increment for shard query");

        // Cancel and drop a coordinator query
        let coord_ctx = QueryTrackingContext::new(60_031, Arc::clone(&global), QueryType::Coordinator);
        cancel_query(60_031);
        drop(coord_ctx);

        // Now coordinator should increment, shard should stay the same
        let shard_final = crate::native_node_stats::NATIVE_SEARCH_SHARD_TASK_TOTAL
            .load(Ordering::Relaxed);
        let coord_final = crate::native_node_stats::NATIVE_SEARCH_TASK_TOTAL
            .load(Ordering::Relaxed);
        assert_eq!(shard_final, shard_baseline + 1, "shard total should not change for coordinator query");
        assert_eq!(coord_final, coord_baseline + 1, "coordinator total should increment");

        set_cancel_stats_threshold(10_000);
    }

    /// Helper: checks if a specific context_id is counted in the registry scan.
    fn is_counted(ctx_id: i64, threshold: Duration) -> bool {
        QUERY_REGISTRY.get(&ctx_id).map_or(false, |tracker| {
            tracker.cancelled_at.lock().map_or(false, |t| t.elapsed() >= threshold)
        })
    }
}
