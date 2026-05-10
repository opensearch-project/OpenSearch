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
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use log::{debug, info};
use once_cell::sync::Lazy;
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

    /// Wall-clock duration in nanoseconds, as an `i64` for FFM transport.
    /// Returns the frozen snapshot if completed, otherwise live elapsed time.
    /// Elapsed nanos is `u128` internally; saturates at `i64::MAX` (~292 years)
    /// so it can always be represented as an `i64`.
    pub fn elapsed_nanos(&self) -> i64 {
        let frozen = self.wall_nanos.load(Ordering::Acquire);
        if frozen > 0 {
            // `AtomicU64` → `i64`: `frozen` was produced from `elapsed().as_nanos() as u64`
            // so its high bit is effectively clear. Still clamp defensively.
            frozen.min(i64::MAX as u64) as i64
        } else {
            self.start_time.elapsed().as_nanos().min(i64::MAX as u128) as i64
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

/// Remove a completed tracker from the registry and return it.
/// Called from JNI after Java has consumed the metrics.
pub fn drain_completed_query(context_id: i64) -> Option<Arc<QueryTracker>> {
    QUERY_REGISTRY
        .remove_if(&context_id, |_, t| t.is_completed())
        .map(|(_, t)| t)
}

// ---------------------------------------------------------------------------
// Registry snapshot — two-phase FFM export
// ---------------------------------------------------------------------------

/// Wire representation of a single query's tracker, used by the two-phase
/// snapshot API. Fields are `i64` so the struct crosses the FFM boundary with
/// a stable, alignment-safe layout (8-byte aligned on every target we support).
///
/// | Field         | Meaning                                                   |
/// |---------------|-----------------------------------------------------------|
/// | context_id    | `QueryTracker::context_id`                                |
/// | current_bytes | `QueryMemoryPool::current_bytes`, clamped to `i64::MAX`   |
/// | peak_bytes    | `QueryMemoryPool::peak_bytes`, clamped to `i64::MAX`      |
/// | wall_nanos    | live elapsed or frozen wall time (see `elapsed_nanos()`)  |
/// | completed     | 1 if the tracker has been marked completed, else 0        |
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct WireQueryMetric {
    pub context_id: i64,
    pub current_bytes: i64,
    pub peak_bytes: i64,
    pub wall_nanos: i64,
    pub completed: i64,
}

const _: () = assert!(std::mem::size_of::<WireQueryMetric>() == 5 * 8);

fn usize_to_i64_saturating(value: usize) -> i64 {
    if value > i64::MAX as usize {
        i64::MAX
    } else {
        value as i64
    }
}

impl WireQueryMetric {
    fn from_tracker(tracker: &QueryTracker) -> Self {
        Self {
            context_id: tracker.context_id,
            current_bytes: usize_to_i64_saturating(tracker.memory_pool.current_bytes()),
            peak_bytes: usize_to_i64_saturating(tracker.memory_pool.peak_bytes()),
            wall_nanos: tracker.elapsed_nanos(),
            completed: if tracker.is_completed() { 1 } else { 0 },
        }
    }
}

/// Phase 1 of the snapshot API: return the current number of entries in the
/// registry. Java uses this to allocate a buffer for phase 2.
///
/// The value is racy by design — queries may register or be drained between
/// phase 1 and phase 2. Phase 2 handles both cases: if there are more entries
/// than the buffer holds, the extra entries are truncated; if there are
/// fewer, the unused tail of the buffer is left untouched and the actual
/// count is returned.
pub fn query_registry_len() -> usize {
    let n = QUERY_REGISTRY.len();
    info!("[nativemem-bp] rust.query_registry_len = {}", n);
    n
}

/// Phase 2 of the snapshot API: copy up to `cap_entries` entries from the
/// registry into `out`. Returns the number of entries actually written.
///
/// `out` may be a raw slice pointing at a Java-owned buffer; it must be
/// valid for writes of `cap_entries * size_of::<WireQueryMetric>()` bytes
/// and properly aligned for `WireQueryMetric` (8-byte aligned). Entries are
/// collected in the order DashMap's iterator returns them, which is not
/// otherwise specified.
pub fn snapshot_query_registry(out: &mut [WireQueryMetric]) -> usize {
    let mut written = 0usize;
    for entry in QUERY_REGISTRY.iter() {
        if written >= out.len() {
            break;
        }
        let metric = WireQueryMetric::from_tracker(entry.value());
        info!(
            "[nativemem-bp] rust.snapshot entry[{}]: ctx={}, current={}B, peak={}B, wall_ns={}, completed={}",
            written, metric.context_id, metric.current_bytes, metric.peak_bytes, metric.wall_nanos, metric.completed
        );
        out[written] = metric;
        written += 1;
    }
    info!(
        "[nativemem-bp] rust.snapshot_query_registry: wrote {} entries (buffer cap {})",
        written,
        out.len()
    );
    written
}

/// Fire the cancellation token for the given context_id.
/// No-op for unknown or already-completed queries.
pub fn cancel_query(context_id: i64) {
    if let Some(tracker) = QUERY_REGISTRY.get(&context_id) {
        tracker.cancellation_token.cancel();
    }
}

/// Clone the cancellation token for the given context_id, if registered.
pub fn get_cancellation_token(context_id: i64) -> Option<CancellationToken> {
    QUERY_REGISTRY.get(&context_id).map(|t| t.cancellation_token.clone())
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
            completed: AtomicBool::new(false),
            wall_nanos: std::sync::atomic::AtomicU64::new(0),
        });
        QUERY_REGISTRY.insert(context_id, Arc::clone(&tracker));
        info!(
            "[nativemem-bp] rust.QueryTrackingContext::new: registered ctx={} (registry_size={})",
            context_id,
            QUERY_REGISTRY.len()
        );
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
            info!(
                "[nativemem-bp] rust.QueryTrackingContext::drop: ctx={} completed (wall={:.3}s, mem_current={}B, mem_peak={}B)",
                tracker.context_id,
                tracker.wall_secs(),
                tracker.memory_pool.current_bytes(),
                tracker.memory_pool.peak_bytes(),
            );
            // Keep the debug line for operators who already tail the Rust debug log.
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

    // -----------------------------------------------------------------------
    // Two-phase snapshot tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_snapshot_captures_live_and_completed() {
        let global = make_global_pool(1_000_000);
        let live_id = 60_000;
        let done_id = 60_001;

        let live_ctx = QueryTrackingContext::new(live_id, Arc::clone(&global));
        let live_pool = live_ctx.memory_pool().unwrap();
        let pool: Arc<dyn MemoryPool> = live_pool.clone();
        let mut reservation = make_reservation(&pool, "live");
        reservation.try_grow(4096).unwrap();

        let done_ctx = QueryTrackingContext::new(done_id, Arc::clone(&global));
        let done_pool = done_ctx.memory_pool().unwrap();
        let done_pool_dyn: Arc<dyn MemoryPool> = done_pool.clone();
        let mut done_reservation = make_reservation(&done_pool_dyn, "done");
        done_reservation.try_grow(2048).unwrap();
        drop(done_reservation);
        drop(done_ctx);

        // Phase 1
        let len = query_registry_len();
        assert!(len >= 2, "expected at least 2 entries, got {len}");

        // Phase 2 with exact capacity
        let mut buf = vec![
            WireQueryMetric {
                context_id: 0,
                current_bytes: 0,
                peak_bytes: 0,
                wall_nanos: 0,
                completed: 0,
            };
            len
        ];
        let written = snapshot_query_registry(&mut buf);
        assert_eq!(written, len);

        let live = buf.iter().find(|m| m.context_id == live_id).expect("live not captured");
        assert_eq!(live.current_bytes, 4096);
        assert_eq!(live.peak_bytes, 4096);
        assert_eq!(live.completed, 0);
        assert!(live.wall_nanos >= 0);

        let done = buf.iter().find(|m| m.context_id == done_id).expect("done not captured");
        assert_eq!(done.current_bytes, 0);
        assert_eq!(done.peak_bytes, 2048);
        assert_eq!(done.completed, 1);
        assert!(done.wall_nanos > 0);

        drop(reservation);
        drop(live_ctx);
        QUERY_REGISTRY.remove(&live_id);
        QUERY_REGISTRY.remove(&done_id);
    }

    #[test]
    fn test_snapshot_truncates_when_buffer_is_smaller_than_registry() {
        let global = make_global_pool(1_000_000);
        let ids: Vec<i64> = (60_100..60_105).collect();
        let contexts: Vec<QueryTrackingContext> = ids
            .iter()
            .map(|id| QueryTrackingContext::new(*id, Arc::clone(&global)))
            .collect();

        let mut buf = vec![
            WireQueryMetric {
                context_id: 0,
                current_bytes: 0,
                peak_bytes: 0,
                wall_nanos: 0,
                completed: 0,
            };
            2
        ];
        let written = snapshot_query_registry(&mut buf);
        assert_eq!(written, 2, "buffer should cap the write count");
        // QUERY_REGISTRY is process-wide. Other parallel tests may contribute entries to
        // this 2-slot buffer, so the only invariant we can assert here is "no more than
        // two entries were written". The more specific "every entry belongs to this
        // test's id range" check is inherently racy; don't re-introduce it.

        drop(contexts);
        for id in &ids {
            QUERY_REGISTRY.remove(id);
        }
    }

    #[test]
    fn test_snapshot_into_oversized_buffer_leaves_tail_untouched() {
        let global = make_global_pool(1_000_000);
        let id = 60_200;
        let ctx = QueryTrackingContext::new(id, global);

        let sentinel = WireQueryMetric {
            context_id: -1,
            current_bytes: -1,
            peak_bytes: -1,
            wall_nanos: -1,
            completed: -1,
        };
        let mut buf = vec![sentinel; query_registry_len() + 4];
        let written = snapshot_query_registry(&mut buf);
        assert!(written >= 1);
        assert!(written < buf.len(), "should not fill the oversized buffer");
        // Trailing slots keep the sentinel — verifies snapshot did not touch them.
        for entry in &buf[written..] {
            assert_eq!(entry.context_id, -1);
        }

        drop(ctx);
        QUERY_REGISTRY.remove(&id);
    }
}
