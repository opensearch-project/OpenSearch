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

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use log::{debug, warn};
use once_cell::sync::Lazy;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;

/// Process-wide epoch for cancelled_at timestamps. Using nanos since this
/// instant avoids storing full Instant values (which aren't atomically sized).
static PROCESS_START: Lazy<Instant> = Lazy::new(Instant::now);

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

impl std::fmt::Display for QueryMemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryMemoryPool(inner={}, current={}, peak={})",
            self.inner.name(),
            self.current_bytes.load(Ordering::Relaxed),
            self.peak_bytes.load(Ordering::Relaxed)
        )
    }
}

impl MemoryPool for QueryMemoryPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

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
    /// Handle to the DedicatedExecutor's tokio runtime. Used by `cancel_query`
    /// to flush pending deferred drops (pull_from_input tasks holding GroupValues
    /// buffers) after aborting the outer CrossRtStream task.
    pub cpu_runtime_handle: OnceLock<tokio::runtime::Handle>,
    /// Nanos since PROCESS_START when cancellation was signalled, or 0 if not cancelled.
    /// Set atomically via CAS in cancel_query — no lock needed.
    pub cancelled_at_nanos: AtomicU64,
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

// ---------------------------------------------------------------------------
// Registry snapshot — top-N FFM export
// ---------------------------------------------------------------------------

/// Wire representation of a single query's tracker, used by the top-N
/// snapshot API. Fields are `i64` so the struct crosses the FFM boundary with
/// a stable, alignment-safe layout (8-byte aligned on every target we support).
///
/// | Field         | Meaning                                                   |
/// |---------------|-----------------------------------------------------------|
/// | context_id    | `QueryTracker::context_id`                                |
/// | current_bytes | `QueryMemoryPool::current_bytes`, clamped to `i64::MAX`   |
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct WireQueryMetric {
    pub context_id: i64,
    pub current_bytes: i64,
}

const _: () = assert!(std::mem::size_of::<WireQueryMetric>() == 2 * 8);

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
        }
    }
}

/// Top-N snapshot: copy at most `out.len()` of the heaviest live queries
/// (ranked by `current_bytes` descending) into `out`. Returns the number of
/// entries actually written.
///
/// Selection is done with a bounded min-heap of capacity `out.len()`, so a
/// single pass over the registry runs in `O(M log N)` where `M` is the
/// registry size and `N = out.len()`. The heap holds the *top N candidates*
/// seen so far; the smallest of those sits at the root, and any new entry
/// heavier than that root replaces it.
///
/// Order of entries written into `out` is unspecified — heap order, not
/// sorted. Callers that need a sorted view sort the prefix client-side.
///
/// Filters applied on the Rust side, in this order:
/// - completed trackers are skipped (a completed query's `current_bytes`
///   drops to zero on `Drop` anyway, so this is mostly a fast path)
/// - `current_bytes == 0` is skipped (registered but un-allocated trackers
///   are not "heavy" candidates)
///
/// `out` may be a raw slice pointing at a Java-owned buffer; it must be valid
/// for writes of `out.len() * size_of::<WireQueryMetric>()` bytes and properly
/// aligned for `WireQueryMetric` (8-byte aligned).
pub fn snapshot_top_n_by_current(out: &mut [WireQueryMetric]) -> usize {
    use std::cmp::{Ordering, Reverse};
    use std::collections::BinaryHeap;

    /// Heap entry — orders purely by `bytes`. The tracker rides along but is
    /// not part of the comparison, so `QueryTracker` doesn't need `Ord`.
    struct HeapEntry {
        bytes: i64,
        tracker: Arc<QueryTracker>,
    }

    impl Eq for HeapEntry {}
    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.bytes == other.bytes
        }
    }
    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            self.bytes.cmp(&other.bytes)
        }
    }
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let n = out.len();
    if n == 0 {
        return 0;
    }

    // BinaryHeap is a max-heap; Reverse flips it so the smallest current_bytes
    // sits at the root. That root is the weakest member of the current top-N
    // set — the one a heavier candidate displaces.
    let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::with_capacity(n);

    let mut sample_logged = 0usize;

    for entry in QUERY_REGISTRY.iter() {
        let tracker = entry.value();
        let bytes_raw = tracker.memory_pool.current_bytes();
        let completed = tracker.is_completed();
        if sample_logged < 5 {
            sample_logged += 1;
        }
        if completed {
            continue;
        }
        let bytes = usize_to_i64_saturating(bytes_raw);
        if bytes == 0 {
            continue;
        }
        if heap.len() < n {
            heap.push(Reverse(HeapEntry {
                bytes,
                tracker: Arc::clone(tracker),
            }));
        } else if let Some(Reverse(min)) = heap.peek() {
            // Strictly greater: equal-byte ties keep the first-seen entry.
            if bytes > min.bytes {
                heap.pop();
                heap.push(Reverse(HeapEntry {
                    bytes,
                    tracker: Arc::clone(tracker),
                }));
            }
        }
    }

    let mut written = 0usize;
    for Reverse(entry) in heap.into_iter() {
        // Re-read the tracker fields here so the wire entry reflects the live
        // values at materialization time, consistent with from_tracker. Ranking
        // and final values are independent point-in-time samples.
        out[written] = WireQueryMetric::from_tracker(&entry.tracker);
        written += 1;
    }
    written
}

/// Maximum time the flush will block waiting for the CPU runtime to process
/// deferred drops (pull_from_input tasks holding GroupValues buffers).
const CANCEL_FLUSH_TIMEOUT: Duration = Duration::from_millis(500);

/// Yields per flush worker. The abort cascade has 3 scheduling levels:
///   Level 1: CrossRtStream CPU task abort → drops CoalescePartitions receiver
///   Level 2: CoalescePartitions' run_input tasks see closed channel → exit →
///            drop PerPartitionStream → Arc<SpawnedTask> refcount hits 0
///   Level 3: SpawnedTask::drop aborts pull_from_input → drops GroupValues
/// Each level needs at least one scheduling round per task. With
/// target_partitions = N, levels 2 and 3 each have N tasks.
/// 32 yields per worker covers up to 32 partitions per level on a single worker.
const FLUSH_YIELDS_PER_WORKER: usize = 32;

/// Number of flush tasks to spawn. Spawning across multiple workers ensures
/// the woken tasks (which may land on different workers' queues) are processed
/// in parallel rather than serialized through one worker's yields.
const FLUSH_WORKER_COUNT: usize = 4;

/// Fire the cancellation token for the given context_id and abort the CPU task.
/// No-op for unknown or already-completed queries.
///
/// Note: this does NOT flush the runtime — the abort cascade only completes
/// after the `QueryStreamHandle` is dropped (via `stream_close`). Call
/// [`flush_cpu_runtime`] after `stream_close` to ensure deferred drops are
/// processed and GroupValues buffers are freed.
pub fn cancel_query(context_id: i64) {
    if let Some(tracker) = QUERY_REGISTRY.get(&context_id) {
        tracker.cancellation_token.cancel();
        if let Some(handle) = tracker.abort_handle.get() {
            handle.abort();
        }
        let nanos = PROCESS_START.elapsed().as_nanos() as u64;
        tracker
            .cancelled_at_nanos
            .compare_exchange(0, nanos, Ordering::Release, Ordering::Relaxed)
            .ok();
    }
}

/// Flush the CPU runtime for the given context_id, giving tokio workers
/// scheduling opportunities to process deferred drops from the abort cascade.
///
/// Call this AFTER `stream_close` has dropped the `QueryStreamHandle` (which
/// drops the JoinSet, releasing aborted task futures for collection). The flush
/// spawns lightweight tasks that yield repeatedly, ensuring workers wake up and
/// process the pending drops (pull_from_input futures → GroupValues buffers).
///
/// No-op if no runtime handle is registered or the query is unknown.
pub fn flush_cpu_runtime(context_id: i64) {
    let rt_handle = QUERY_REGISTRY
        .get(&context_id)
        .and_then(|tracker| tracker.cpu_runtime_handle.get().cloned());

    if let Some(handle) = rt_handle {
        flush_cpu_runtime_with_handle(&handle, context_id);
    }
}

/// Flush variant that takes the runtime handle directly — used by `stream_close`
/// which must extract the handle before dropping the tracker (drop removes it
/// from the registry).
pub fn flush_cpu_runtime_with_handle(handle: &tokio::runtime::Handle, context_id: i64) {
    let (tx, rx) = std::sync::mpsc::sync_channel(FLUSH_WORKER_COUNT);
    for _ in 0..FLUSH_WORKER_COUNT {
        let tx = tx.clone();
        handle.spawn(async move {
            for _ in 0..FLUSH_YIELDS_PER_WORKER {
                tokio::task::yield_now().await;
            }
            let _ = tx.send(());
        });
    }
    drop(tx);
    for _ in 0..FLUSH_WORKER_COUNT {
        if rx.recv_timeout(CANCEL_FLUSH_TIMEOUT).is_err() {
            warn!(
                "flush_cpu_runtime({}): timed out after {}ms",
                context_id,
                CANCEL_FLUSH_TIMEOUT.as_millis()
            );
            break;
        }
    }
}

/// Extract the CPU runtime handle from the tracker, removing it. Used by
/// `stream_close` to grab the handle before the tracker is dropped (which
/// removes it from the registry).
pub fn take_cpu_runtime_handle(context_id: i64) -> Option<tokio::runtime::Handle> {
    QUERY_REGISTRY
        .get(&context_id)
        .and_then(|tracker| tracker.cpu_runtime_handle.get().cloned())
}

/// Clone the cancellation token for the given context_id, if registered.
pub fn get_cancellation_token(context_id: i64) -> Option<CancellationToken> {
    QUERY_REGISTRY
        .get(&context_id)
        .map(|t| t.cancellation_token.clone())
}

/// Store the CPU task's AbortHandle for the given context_id.
pub fn set_abort_handle(context_id: i64, handle: AbortHandle) {
    if let Some(tracker) = QUERY_REGISTRY.get(&context_id) {
        tracker.abort_handle.set(handle).ok();
    }
}

/// Store the CPU runtime handle for the given context_id so that
/// `cancel_query` can flush deferred drops on that runtime.
pub fn set_cpu_runtime_handle(context_id: i64, handle: tokio::runtime::Handle) {
    if let Some(tracker) = QUERY_REGISTRY.get(&context_id) {
        tracker.cpu_runtime_handle.set(handle).ok();
    }
}

/// Counts queries currently running past the cancellation threshold, by type.
/// Returns (shard_current, coordinator_current).
///
/// Lock-free: reads each entry's `cancelled_at_nanos` atomically.
pub fn count_cancelled_running(threshold: Duration) -> (i64, i64) {
    let mut shard_count: i64 = 0;
    let mut coordinator_count: i64 = 0;
    let threshold_nanos = threshold.as_nanos() as u64;
    let now_nanos = PROCESS_START.elapsed().as_nanos() as u64;
    for entry in QUERY_REGISTRY.iter() {
        let tracker = entry.value();
        let cancelled_nanos = tracker.cancelled_at_nanos.load(Ordering::Acquire);
        if cancelled_nanos > 0 && (now_nanos - cancelled_nanos) >= threshold_nanos {
            match tracker.query_type {
                QueryType::Shard => shard_count += 1,
                QueryType::Coordinator => coordinator_count += 1,
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
            return Self {
                tracker: None,
                phantom_reservation: None,
                phantom_corrector: None,
            };
        }
        let query_pool = Arc::new(QueryMemoryPool::new(global_pool));
        let tracker = Arc::new(QueryTracker {
            start_time: Instant::now(),
            context_id,
            query_type,
            memory_pool: query_pool,
            cancellation_token: CancellationToken::new(),
            abort_handle: OnceLock::new(),
            cpu_runtime_handle: OnceLock::new(),
            cancelled_at_nanos: AtomicU64::new(0),
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
        let (corrector, reservation) =
            match (&self.phantom_corrector, &mut self.phantom_reservation) {
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
            let cancelled_nanos = tracker.cancelled_at_nanos.load(Ordering::Acquire);
            if cancelled_nanos > 0 {
                let elapsed_since_cancel =
                    PROCESS_START.elapsed().as_nanos() as u64 - cancelled_nanos;
                if elapsed_since_cancel >= cancel_stats_threshold().as_nanos() as u64 {
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
    use std::time::{Duration, Instant};

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
        assert!(tracker.cancelled_at_nanos.load(Ordering::Relaxed) == 0);

        // Cancel
        cancel_query(ctx_id);

        // cancelled_at should be set
        assert!(tracker.cancelled_at_nanos.load(Ordering::Relaxed) > 0);
        drop(tracker);
        drop(ctx);
    }

    #[test]
    fn test_cancel_query_idempotent() {
        let global = make_global_pool(10_000);
        let ctx_id = 60_002;
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);

        cancel_query(ctx_id);
        let first = QUERY_REGISTRY
            .get(&ctx_id)
            .unwrap()
            .cancelled_at_nanos
            .load(Ordering::Relaxed);

        thread::sleep(Duration::from_millis(10));
        cancel_query(ctx_id);
        let second = QUERY_REGISTRY
            .get(&ctx_id)
            .unwrap()
            .cancelled_at_nanos
            .load(Ordering::Relaxed);

        // Second cancel should not overwrite the first timestamp
        assert_eq!(first, second);
        drop(ctx);
    }

    #[test]
    fn test_count_cancelled_running_with_zero_threshold() {
        let global = make_global_pool(10_000);
        let ctx_id = 60_003;
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);

        // Not cancelled — cancelled_at_nanos should be 0
        let tracker = QUERY_REGISTRY.get(&ctx_id).unwrap();
        assert_eq!(tracker.cancelled_at_nanos.load(Ordering::Relaxed), 0);
        drop(tracker);

        // Cancel it
        cancel_query(ctx_id);

        // Now cancelled_at_nanos should be > 0
        let tracker = QUERY_REGISTRY.get(&ctx_id).unwrap();
        assert!(tracker.cancelled_at_nanos.load(Ordering::Relaxed) > 0);
        drop(tracker);

        drop(ctx);

        // After drop, not in registry
        assert!(QUERY_REGISTRY.get(&ctx_id).is_none());
    }

    #[test]
    fn test_count_cancelled_running_distinguishes_query_types() {
        let global = make_global_pool(10_000);
        let shard_id = 60_005;
        let coord_id = 60_006;

        let shard_ctx = QueryTrackingContext::new(shard_id, Arc::clone(&global), QueryType::Shard);
        let coord_ctx =
            QueryTrackingContext::new(coord_id, Arc::clone(&global), QueryType::Coordinator);

        cancel_query(shard_id);
        cancel_query(coord_id);

        // Verify each query is registered, cancelled, and has correct type
        let shard_tracker = QUERY_REGISTRY.get(&shard_id).unwrap();
        assert!(shard_tracker.cancelled_at_nanos.load(Ordering::Relaxed) > 0);
        assert_eq!(shard_tracker.query_type, QueryType::Shard);
        drop(shard_tracker);

        let coord_tracker = QUERY_REGISTRY.get(&coord_id).unwrap();
        assert!(coord_tracker.cancelled_at_nanos.load(Ordering::Relaxed) > 0);
        assert_eq!(coord_tracker.query_type, QueryType::Coordinator);
        drop(coord_tracker);

        drop(shard_ctx);
        drop(coord_ctx);
    }

    // -----------------------------------------------------------------------
    // Flush-on-cancel tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cancel_query_flushes_deferred_drops() {
        use std::sync::atomic::AtomicBool;

        // Tracks whether the captured state inside the spawned future was dropped.
        struct DropSentinel(Arc<AtomicBool>);
        impl Drop for DropSentinel {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let global = make_global_pool(10_000);
        // Unique id: the QUERY_REGISTRY is process-wide and tests run in parallel, so this must not
        // collide with any other test's id (70_001 collides with test_top_n_picks_highest_current_bytes).
        let ctx_id = 80_001;
        let ctx = QueryTrackingContext::new(ctx_id, global, QueryType::Shard);

        // Build a dedicated executor with its own tokio runtime.
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(2).enable_all();
        let exec = crate::executor::DedicatedExecutor::new("test-flush", builder, 2);

        // Store the runtime handle in the tracker (mirrors production path).
        if let Some(rt) = exec.handle() {
            set_cpu_runtime_handle(ctx_id, rt);
        }

        // Spawn a task that holds the sentinel and blocks forever at an await.
        let dropped = Arc::new(AtomicBool::new(false));
        let sentinel = DropSentinel(Arc::clone(&dropped));

        let (abort_handle, _join_fut) = exec.spawn_with_abort_handle(async move {
            let _hold = sentinel; // captured in the future's state
                                  // Block forever — only abort can end this.
            futures::future::pending::<()>().await;
        });

        if let Some(h) = abort_handle {
            set_abort_handle(ctx_id, h);
        }

        // Small delay to let the task actually park at the pending().await
        thread::sleep(Duration::from_millis(10));

        // Verify not yet dropped
        assert!(
            !dropped.load(Ordering::Acquire),
            "sentinel should be alive before cancel"
        );

        // cancel_query aborts the task (marks it cancelled in the runtime)
        cancel_query(ctx_id);

        // The abort is async — the task future may not be dropped yet.
        // flush_cpu_runtime gives the runtime scheduling opportunities to
        // process the abort and drop the future (freeing the sentinel). A single
        // flush is best-effort: it spawns a fixed number of yield tasks and
        // returns once they finish, which under heavy parallel test load (a
        // CPU-starved CI box running 1000+ tests against a 2-worker runtime) can
        // race the runtime actually polling and dropping the aborted task. Poll
        // the flush until the deferred drop is observed, bounded by a timeout so
        // a genuine regression (flush never processes the drop) still fails.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !dropped.load(Ordering::Acquire) && Instant::now() < deadline {
            flush_cpu_runtime(ctx_id);
            if dropped.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }

        // After flushing, the sentinel must have been dropped.
        assert!(
            dropped.load(Ordering::Acquire),
            "sentinel must be dropped after flush — deferred drop was not processed"
        );

        drop(ctx);
        exec.join_blocking();
    }

    // -----------------------------------------------------------------------
    // Top-N snapshot tests
    // -----------------------------------------------------------------------

    /// Helper: drain all live tracker contexts so each top-N test starts from
    /// a clean registry. Tests in this module are tagged with unique id ranges
    /// but the registry is process-wide, so we only assert on contexts we own.
    fn fresh_buf(n: usize) -> Vec<WireQueryMetric> {
        vec![
            WireQueryMetric {
                context_id: 0,
                current_bytes: 0,
            };
            n
        ]
    }

    #[test]
    fn test_top_n_zero_capacity_returns_zero() {
        let mut buf: Vec<WireQueryMetric> = Vec::new();
        let written = snapshot_top_n_by_current(&mut buf);
        assert_eq!(written, 0);
    }

    #[test]
    fn test_top_n_picks_highest_current_bytes() {
        let global = make_global_pool(10_000_000);
        let id_lo = 70_000;
        let id_md = 70_001;
        let id_hi = 70_002;

        let ctx_lo = QueryTrackingContext::new(id_lo, Arc::clone(&global), QueryType::Shard);
        let ctx_md = QueryTrackingContext::new(id_md, Arc::clone(&global), QueryType::Shard);
        let ctx_hi = QueryTrackingContext::new(id_hi, Arc::clone(&global), QueryType::Shard);

        let pool_lo: Arc<dyn MemoryPool> = ctx_lo.memory_pool().unwrap();
        let pool_md: Arc<dyn MemoryPool> = ctx_md.memory_pool().unwrap();
        let pool_hi: Arc<dyn MemoryPool> = ctx_hi.memory_pool().unwrap();

        let mut r_lo = make_reservation(&pool_lo, "lo");
        let mut r_md = make_reservation(&pool_md, "md");
        let mut r_hi = make_reservation(&pool_hi, "hi");
        r_lo.try_grow(1_000).unwrap();
        r_md.try_grow(5_000).unwrap();
        r_hi.try_grow(9_000).unwrap();

        // Top-2 must contain ids hi and md but not lo. Other parallel tests
        // may push entries into the registry, so we filter to our id range.
        let mut buf = fresh_buf(2);
        let written = snapshot_top_n_by_current(&mut buf);
        assert!(written >= 1 && written <= 2);

        let our: Vec<&WireQueryMetric> = buf[..written]
            .iter()
            .filter(|m| (id_lo..=id_hi).contains(&m.context_id))
            .collect();
        // At minimum, id_hi (the heaviest) must be in our slice if any of
        // ours made it in.
        if !our.is_empty() {
            assert!(our.iter().any(|m| m.context_id == id_hi));
            assert!(!our.iter().any(|m| m.context_id == id_lo));
        }

        drop(r_lo);
        drop(r_md);
        drop(r_hi);
        drop(ctx_lo);
        drop(ctx_md);
        drop(ctx_hi);
        QUERY_REGISTRY.remove(&id_lo);
        QUERY_REGISTRY.remove(&id_md);
        QUERY_REGISTRY.remove(&id_hi);
    }

    #[test]
    fn test_top_n_skips_completed_and_zero_byte_trackers() {
        let global = make_global_pool(1_000_000);
        let live_id = 70_100;
        let zero_id = 70_101;
        let done_id = 70_102;

        let live_ctx = QueryTrackingContext::new(live_id, Arc::clone(&global), QueryType::Shard);
        let live_pool: Arc<dyn MemoryPool> = live_ctx.memory_pool().unwrap();
        let mut r_live = make_reservation(&live_pool, "live");
        r_live.try_grow(4_096).unwrap();

        // Registered but never reserved — current_bytes stays 0.
        let _zero_ctx = QueryTrackingContext::new(zero_id, Arc::clone(&global), QueryType::Shard);

        // Completed before snapshot. Drop reservation first so QueryMemoryPool
        // is settled, then drop the context to flip the completed flag.
        let done_ctx = QueryTrackingContext::new(done_id, Arc::clone(&global), QueryType::Shard);
        let done_pool: Arc<dyn MemoryPool> = done_ctx.memory_pool().unwrap();
        let mut r_done = make_reservation(&done_pool, "done");
        r_done.try_grow(8_192).unwrap();
        drop(r_done);
        drop(done_ctx);

        let mut buf = fresh_buf(8);
        let written = snapshot_top_n_by_current(&mut buf);

        let our: Vec<&WireQueryMetric> = buf[..written]
            .iter()
            .filter(|m| [live_id, zero_id, done_id].contains(&m.context_id))
            .collect();
        assert!(our.iter().any(|m| m.context_id == live_id));
        assert!(!our.iter().any(|m| m.context_id == zero_id));
        assert!(!our.iter().any(|m| m.context_id == done_id));

        drop(r_live);
        drop(live_ctx);
        QUERY_REGISTRY.remove(&live_id);
        QUERY_REGISTRY.remove(&zero_id);
        QUERY_REGISTRY.remove(&done_id);
    }

    #[test]
    fn test_top_n_with_buffer_larger_than_live_set() {
        let global = make_global_pool(1_000_000);
        let id = 70_200;
        let ctx = QueryTrackingContext::new(id, global, QueryType::Shard);
        let pool: Arc<dyn MemoryPool> = ctx.memory_pool().unwrap();
        let mut r = make_reservation(&pool, "only");
        r.try_grow(2_048).unwrap();

        let sentinel = WireQueryMetric {
            context_id: -1,
            current_bytes: -1,
        };
        let mut buf = vec![sentinel; 16];
        let written = snapshot_top_n_by_current(&mut buf);
        assert!(written >= 1);
        assert!(written < buf.len());
        // Tail past `written` keeps the sentinel — proves the snapshot did not
        // touch it.
        for slot in &buf[written..] {
            assert_eq!(slot.context_id, -1);
        }

        drop(r);
        drop(ctx);
        QUERY_REGISTRY.remove(&id);
    }

    #[test]
    fn test_top_n_caps_at_buffer_capacity() {
        let global = make_global_pool(10_000_000);
        let ids: Vec<i64> = (70_300..70_310).collect();
        let mut contexts = Vec::with_capacity(ids.len());
        let mut reservations = Vec::with_capacity(ids.len());
        for (i, id) in ids.iter().enumerate() {
            let ctx = QueryTrackingContext::new(*id, Arc::clone(&global), QueryType::Shard);
            let pool: Arc<dyn MemoryPool> = ctx.memory_pool().unwrap();
            let mut r = make_reservation(&pool, "cap");
            r.try_grow((i as usize + 1) * 1_000).unwrap();
            reservations.push(r);
            contexts.push(ctx);
        }

        let mut buf = fresh_buf(3);
        let written = snapshot_top_n_by_current(&mut buf);
        assert!(written <= 3, "must not exceed buffer capacity");

        for r in reservations.drain(..) {
            drop(r);
        }
        for ctx in contexts.drain(..) {
            drop(ctx);
        }
        for id in &ids {
            QUERY_REGISTRY.remove(id);
        }
    }
}
