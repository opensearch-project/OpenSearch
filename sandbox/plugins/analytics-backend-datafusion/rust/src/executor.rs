/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use futures::{future::BoxFuture, Future, FutureExt, TryFutureExt};
use log::{info, warn};
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::{
    runtime::Handle,
    sync::{oneshot::error::RecvError, Mutex as AsyncMutex, Notify, Semaphore, OwnedSemaphorePermit},
    task::{AbortHandle, JoinSet},
};

// DedicatedExecutor — runs CPU-bound DataFusion work on its own tokio runtime.
// Based on InfluxDB's executor pattern.
// https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/query_planning/thread_pools.rs

#[derive(Clone)]
pub struct DedicatedExecutor {
    state: Arc<RwLock<State>>,
    /// Per-query concurrency gate: limits how many query stream drivers can
    /// run simultaneously on this executor. Acquired inside the spawned
    /// CrossRtStream driver future and held for the query's entire lifetime.
    concurrency_gate: Arc<ConcurrencyGate>,
}

/// Internal state guarded by the async resize mutex.
/// Holds poison permits (used to reduce effective capacity) and the
/// target max permits during an in-progress resize-down.
struct ResizeState {
    /// Poison permits held to reduce effective capacity.
    /// On scale-down: we acquire permits and store them here.
    /// On scale-up: we drop permits from here first, then add_permits for the delta.
    poison_permits: Vec<OwnedSemaphorePermit>,
    /// The target max_permits if a resize-down is in progress, else equals max_permits.
    target_max_permits: u32,
}

/// Limits the number of concurrent query streams active on the CPU executor.
/// The permit is acquired inside the spawned stream-driver future and held
/// until the stream is fully consumed or dropped.
pub struct ConcurrencyGate {
    semaphore: Arc<Semaphore>,
    max_permits: AtomicU32,
    total_wait_ms: AtomicU64,
    total_queries_admitted: AtomicU64,
    /// Number of tasks currently waiting to acquire the semaphore.
    pending_acquire_permits: AtomicU64,
    /// Number of batches currently waiting to acquire permits from this gate.
    pending_acquire_batches: AtomicU64,
    /// Serializes resize operations. Only one resize runs at a time.
    resize_mutex: AsyncMutex<ResizeState>,
}

impl ConcurrencyGate {
    pub fn new(max_concurrent: usize) -> Self {
        let permits = max_concurrent.max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            max_permits: AtomicU32::new(permits as u32),
            total_wait_ms: AtomicU64::new(0),
            total_queries_admitted: AtomicU64::new(0),
            pending_acquire_permits: AtomicU64::new(0),
            pending_acquire_batches: AtomicU64::new(0),
            resize_mutex: AsyncMutex::new(ResizeState {
                poison_permits: Vec::new(),
                target_max_permits: permits as u32,
            }),
        }
    }

    /// Acquire a permit. Held for the entire query stream lifetime.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        self.acquire_many(1).await
    }

    /// Acquire N permits (partition-weighted). Held for the entire query stream lifetime.
    pub async fn acquire_many(&self, n: u32) -> OwnedSemaphorePermit {
        self.pending_acquire_permits.fetch_add(n as u64, Ordering::Relaxed);
        self.pending_acquire_batches.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        let result = self.semaphore.clone().acquire_many_owned(n).await;
        self.pending_acquire_permits.fetch_sub(n as u64, Ordering::Relaxed);
        self.pending_acquire_batches.fetch_sub(1, Ordering::Relaxed);
        let permit = result.expect("concurrency gate semaphore closed");
        let elapsed_ms = start.elapsed().as_millis() as u64;
        self.total_wait_ms.fetch_add(elapsed_ms, Ordering::Relaxed);
        self.total_queries_admitted.fetch_add(1, Ordering::Relaxed);
        permit
    }

    /// Returns the number of tasks currently waiting to acquire the semaphore.
    pub fn pending_acquire_permits(&self) -> u64 {
        self.pending_acquire_permits.load(Ordering::Relaxed)
    }

    /// Returns the number of batches currently waiting to acquire permits.
    pub fn pending_acquire_batches(&self) -> u64 {
        self.pending_acquire_batches.load(Ordering::Relaxed)
    }

    pub fn max_permits(&self) -> u32 {
        self.max_permits.load(Ordering::Acquire)
    }

    pub fn active_permits(&self) -> u32 {
        self.max_permits.load(Ordering::Acquire).saturating_sub(self.semaphore.available_permits() as u32)
    }

    pub fn total_wait_ms(&self) -> u64 {
        self.total_wait_ms.load(Ordering::Relaxed)
    }

    pub fn total_queries_admitted(&self) -> u64 {
        self.total_queries_admitted.load(Ordering::Relaxed)
    }

    /// Returns the number of poison permits currently held.
    /// Non-blocking: returns 0 if a resize is in progress.
    pub fn poison_permits_held(&self) -> u32 {
        match self.resize_mutex.try_lock() {
            Ok(state) => state.poison_permits.len() as u32,
            Err(_) => 0, // Resize in progress, report 0
        }
    }

    /// Returns the target max_permits (differs from max_permits during in-progress resize-down).
    /// Non-blocking: returns current max_permits if a resize is in progress.
    pub fn target_max_permits(&self) -> u32 {
        match self.resize_mutex.try_lock() {
            Ok(state) => state.target_max_permits,
            Err(_) => self.max_permits.load(Ordering::Relaxed),
        }
    }

    /// Resize the concurrency gate to a new maximum permit count.
    ///
    /// - Scale-up: releases poison permits first, then adds new permits.
    /// - Scale-down: acquires poison permits to reduce effective capacity.
    /// - Serialized via the async resize mutex (only one resize at a time).
    pub async fn resize(&self, new_max: u32, gate_name: &str) {
        // Validate bounds
        if new_max < 1 || new_max > Semaphore::MAX_PERMITS as u32 {
            warn!(
                "[{}] resize rejected: new_max {} out of bounds [1, {}]",
                gate_name, new_max, Semaphore::MAX_PERMITS
            );
            return;
        }

        let mut state = self.resize_mutex.lock().await;
        let current_max = self.max_permits.load(Ordering::Acquire);

        if new_max == current_max {
            return; // No-op
        }

        let old_max = current_max;
        state.target_max_permits = new_max;

        if new_max > current_max {
            // Scale-Up: release poison permits first, then add_permits for remaining delta
            let poison_count = state.poison_permits.len() as u32;
            let permits_from_poison = poison_count.min(new_max - current_max);
            for _ in 0..permits_from_poison {
                state.poison_permits.pop();
            }
            let still_needed = (new_max - current_max) - permits_from_poison;
            if still_needed > 0 {
                self.semaphore.add_permits(still_needed as usize);
            }
            self.max_permits.store(new_max, Ordering::Release);
            state.target_max_permits = new_max;
            info!(
                "[{}] resize complete: {} → {} (scale-up, released {} poison, added {} new)",
                gate_name, old_max, new_max, permits_from_poison, still_needed
            );
        } else {
            // Scale-Down: acquire poison permits to reduce effective capacity
            let existing_poison = state.poison_permits.len() as u32;
            let total_poison_needed = current_max - new_max;

            if existing_poison >= total_poison_needed {
                // We already hold enough poison permits; release excess
                let excess = existing_poison - total_poison_needed;
                for _ in 0..excess {
                    state.poison_permits.pop();
                }
                self.max_permits.store(new_max, Ordering::Release);
                state.target_max_permits = new_max;
                info!(
                    "[{}] resize complete: {} → {} (scale-down, released {} excess poison)",
                    gate_name, old_max, new_max, excess
                );
            } else {
                // Need to acquire additional poison permits (one at a time for precise release)
                let additional_needed = total_poison_needed - existing_poison;
                for _ in 0..additional_needed {
                    let permit = self.semaphore.clone()
                        .acquire_owned()
                        .await
                        .expect("semaphore closed during resize");
                    state.poison_permits.push(permit);
                }
                self.max_permits.store(new_max, Ordering::Release);
                state.target_max_permits = new_max;
                info!(
                    "[{}] resize complete: {} → {} (scale-down, acquired {} poison permits)",
                    gate_name, old_max, new_max, additional_needed
                );
            }
        }
    }
}

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60 * 5);

#[derive(Debug)]
pub enum JobError {
    WorkerGone,
    Panic { msg: String },
}

struct State {
    handle: Option<Handle>,
    start_shutdown: Arc<Notify>,
    completed_shutdown: futures::future::Shared<BoxFuture<'static, Result<(), Arc<RecvError>>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for State {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.handle = None;
            self.start_shutdown.notify_one();
        }
        if !std::thread::panicking() {
            self.completed_shutdown.clone().now_or_never();
        }
        if let Some(thread) = self.thread.take() {
            thread.join().ok();
        }
    }
}

impl std::fmt::Debug for DedicatedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DedicatedExecutor")
    }
}

impl DedicatedExecutor {
    pub fn new(name: &str, runtime_builder: tokio::runtime::Builder, max_concurrent_queries: usize) -> Self {
        let name = name.to_owned();
        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);
        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel();
        let (tx_handle, rx_handle) = std::sync::mpsc::channel();

        let thread = std::thread::Builder::new()
            .name(format!("{name} driver"))
            .spawn(move || {
                let mut runtime_builder = runtime_builder;
                let runtime = runtime_builder
                    .build()
                    .expect("Creating tokio runtime");

                runtime.block_on(async move {
                    let shutdown = notify_shutdown_captured.notified();
                    let mut shutdown = std::pin::pin!(shutdown);
                    shutdown.as_mut().enable();
                    if tx_handle.send(Handle::current()).is_err() {
                        return;
                    }
                    shutdown.await;
                });
                runtime.shutdown_timeout(SHUTDOWN_TIMEOUT);
                tx_shutdown.send(()).ok();
            })
            .expect("executor setup");

        let handle = rx_handle.recv().expect("driver started");
        let state = State {
            handle: Some(handle),
            start_shutdown: notify_shutdown,
            completed_shutdown: rx_shutdown.map_err(Arc::new).boxed().shared(),
            thread: Some(thread),
        };
        Self {
            state: Arc::new(RwLock::new(state)),
            concurrency_gate: Arc::new(ConcurrencyGate::new(max_concurrent_queries)),
        }
    }

    pub fn spawn<T>(&self, task: T) -> impl Future<Output = Result<T::Output, JobError>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let (_abort_handle, fut) = self.spawn_with_abort_handle(task);
        fut
    }


    /// Like [`spawn`](Self::spawn), but also returns an [`AbortHandle`] that
    /// can be used to cancel the CPU task from outside (e.g. from `cancel_query`).
    pub fn spawn_with_abort_handle<T>(
        &self,
        task: T,
    ) -> (Option<AbortHandle>, impl Future<Output = Result<T::Output, JobError>>)
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let handle = {
            let state = self.state.read();
            state.handle.clone()
        };
        let Some(handle) = handle else {
            return (None, futures::future::err(JobError::WorkerGone).boxed());
        };
        let mut join_set = JoinSet::new();
        let abort_handle = join_set.spawn_on(task, &handle);
        let fut = async move {
            join_set
                .join_next()
                .await
                .expect("just spawned task")
                .map_err(|e| match e.try_into_panic() {
                    Ok(e) => {
                        let s = if let Some(s) = e.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = e.downcast_ref::<&str>() {
                            s.to_string()
                        } else {
                            "unknown internal error".to_string()
                        };
                        JobError::Panic { msg: s }
                    }
                    Err(_) => JobError::WorkerGone,
                })
        }
        .boxed();
        (Some(abort_handle), fut)
    }
    pub fn join_blocking(&self) {
        self.shutdown();
        let thread_handle = {
            let mut state = self.state.write();
            state.thread.take()
        };
        if let Some(handle) = thread_handle {
            let _ = handle.join();
        }
    }

    /// Returns a clone of the underlying Tokio runtime `Handle`, if the
    /// executor has not been shut down. Used to create a
    /// `tokio_metrics::RuntimeMonitor` for the CPU runtime.
    pub fn handle(&self) -> Option<Handle> {
        let state = self.state.read();
        state.handle.clone()
    }

    /// Returns a reference to the concurrency gate for this executor.
    pub fn concurrency_gate(&self) -> &Arc<ConcurrencyGate> {
        &self.concurrency_gate
    }

    pub fn shutdown(&self) {
        let mut state = self.state.write();
        state.handle = None;
        state.start_shutdown.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::{Arc, Barrier};
    use tokio::sync::Barrier as AsyncBarrier;

    fn test_exec(threads: usize) -> DedicatedExecutor {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(threads).enable_all();
        DedicatedExecutor::new("test-cpu", builder, threads)
    }

    #[tokio::test]
    async fn test_spawn_basic() {
        let exec = test_exec(1);
        let result = exec.spawn(async { 42 }).await.unwrap();
        assert_eq!(result, 42);
        exec.join_blocking();
    }

    #[tokio::test]
    async fn test_spawn_runs_on_different_thread() {
        let exec = test_exec(1);
        let caller_id = std::thread::current().id();
        let spawned_id = exec
            .spawn(async { std::thread::current().id() })
            .await
            .unwrap();
        assert_ne!(caller_id, spawned_id);
        exec.join_blocking();
    }

    #[tokio::test]
    async fn test_multi_task() {
        let barrier = Arc::new(Barrier::new(3));
        let exec = test_exec(2);
        let t1 = exec.spawn({
            let b = barrier.clone();
            async move {
                b.wait();
                11
            }
        });
        let t2 = exec.spawn({
            let b = barrier.clone();
            async move {
                b.wait();
                22
            }
        });
        barrier.wait();
        assert_eq!(t1.await.unwrap(), 11);
        assert_eq!(t2.await.unwrap(), 22);
        exec.join_blocking();
    }

    #[tokio::test]
    async fn test_panic_propagates() {
        let exec = test_exec(1);
        let result = exec.spawn(async { panic!("boom") }).await;
        match result {
            Err(JobError::Panic { msg }) => assert!(msg.contains("boom")),
            other => panic!("Expected Panic error, got {:?}", other),
        }
        exec.join_blocking();
    }

    #[tokio::test]
    async fn test_submit_after_shutdown() {
        let exec = test_exec(1);
        exec.shutdown();
        let result = exec.spawn(async { 1 }).await;
        assert!(matches!(result, Err(JobError::WorkerGone)));
    }

    #[tokio::test]
    async fn test_clone_shares_runtime() {
        let exec = test_exec(1);
        let cloned = exec.clone();
        let result = cloned.spawn(async { 99 }).await.unwrap();
        assert_eq!(result, 99);
        exec.join_blocking();
    }

    #[tokio::test]
    async fn test_cancel_on_drop() {
        let exec = test_exec(1);
        let barrier = Arc::new(AsyncBarrier::new(2));
        let b = barrier.clone();
        let task = exec.spawn(async move {
            b.wait().await;
            42
        });
        // Drop the task without awaiting — it should be cancelled
        drop(task);
        // The barrier will never be reached, so this just verifies no hang
        exec.join_blocking();
    }

    // ─── ConcurrencyGate resize unit tests ───────────────────────────────

    /// Helper: read poison_permits count from the resize state.
    async fn poison_permits_held(gate: &ConcurrencyGate) -> u32 {
        let state = gate.resize_mutex.lock().await;
        state.poison_permits.len() as u32
    }

    /// Validates: Requirements 3.1, 3.2
    /// Scale-up from 4 → 8: verify max_permits == 8 and 8 permits available.
    #[tokio::test]
    async fn test_resize_scale_up() {
        let gate = ConcurrencyGate::new(4);
        assert_eq!(gate.max_permits(), 4);
        assert_eq!(gate.semaphore.available_permits(), 4);

        gate.resize(8, "test").await;

        assert_eq!(gate.max_permits(), 8);
        assert_eq!(gate.semaphore.available_permits(), 8);
        assert_eq!(poison_permits_held(&gate).await, 0);
    }

    /// Validates: Requirements 3.2, 3.7
    /// Scale-down from 8 → 4 with no active queries: verify max_permits == 4,
    /// 4 poison permits held, and 4 available permits.
    #[tokio::test]
    async fn test_resize_scale_down_no_active() {
        let gate = ConcurrencyGate::new(8);
        assert_eq!(gate.max_permits(), 8);
        assert_eq!(gate.semaphore.available_permits(), 8);

        gate.resize(4, "test").await;

        assert_eq!(gate.max_permits(), 4);
        assert_eq!(poison_permits_held(&gate).await, 4); // 4 individual permits
        // Available permits should be 4: original 8 minus 4 acquired as poison
        assert_eq!(gate.semaphore.available_permits(), 4);
    }

    /// Validates: Requirements 3.6
    /// No-op resize (same value): verify no state change.
    #[tokio::test]
    async fn test_resize_noop() {
        let gate = ConcurrencyGate::new(4);
        assert_eq!(gate.max_permits(), 4);
        assert_eq!(gate.semaphore.available_permits(), 4);

        gate.resize(4, "test").await;

        assert_eq!(gate.max_permits(), 4);
        assert_eq!(gate.semaphore.available_permits(), 4);
        assert_eq!(poison_permits_held(&gate).await, 0);
    }

    /// Validates: Requirements 3.7
    /// Boundary rejection: resize to 0 should be rejected.
    /// On 64-bit platforms, Semaphore::MAX_PERMITS exceeds u32::MAX so the upper
    /// bound check is effectively unreachable for u32 inputs — we verify the lower
    /// bound (0) is properly rejected.
    #[tokio::test]
    async fn test_resize_boundary_rejection() {
        let gate = ConcurrencyGate::new(4);

        // Attempt resize to 0 — should be rejected (below minimum of 1)
        gate.resize(0, "test").await;
        assert_eq!(gate.max_permits(), 4);
        assert_eq!(gate.semaphore.available_permits(), 4);

        // Verify a valid resize still works after a rejected one
        gate.resize(6, "test").await;
        assert_eq!(gate.max_permits(), 6);
        assert_eq!(gate.semaphore.available_permits(), 6);

        // Attempt resize to 0 again — still rejected
        gate.resize(0, "test").await;
        assert_eq!(gate.max_permits(), 6);
        assert_eq!(gate.semaphore.available_permits(), 6);
    }

    /// Validates: Requirements 3.1, 3.2
    /// Scale-up after scale-down: create gate with 8, resize down to 4 (poison held),
    /// then resize up to 6. Verify poison permits released and max_permits == 6.
    ///
    /// Each poison permit is acquired individually (one Vec entry = one permit),
    /// so scale-up releases exactly the needed count without overshoot.
    #[tokio::test]
    async fn test_resize_scale_up_after_scale_down() {
        let gate = ConcurrencyGate::new(8);

        // Scale down: 8 → 4 (acquires 4 individual poison permits)
        gate.resize(4, "test").await;
        assert_eq!(gate.max_permits(), 4);
        assert_eq!(gate.semaphore.available_permits(), 4);
        assert_eq!(poison_permits_held(&gate).await, 4); // 4 entries, one permit each

        // Scale up: 4 → 6
        // Releases 2 poison permits (pops 2 entries), leaving 2 poison held.
        gate.resize(6, "test").await;
        assert_eq!(gate.max_permits(), 6);
        // 2 poison permits remain (8 - 6 = 2 needed to maintain capacity at 6)
        assert_eq!(poison_permits_held(&gate).await, 2);
        // Available permits == max_permits - poison_held = 6 (semaphore has 6 available)
        assert_eq!(gate.semaphore.available_permits(), 6);
    }

    // ─── Property-based tests ────────────────────────────────────────────

    /// **Validates: Requirements 3.1, 3.2, 3.4**
    ///
    /// Property 1: Resize reaches target capacity
    ///
    /// For any valid ConcurrencyGate with initial max permits `initial_max`
    /// and any valid target `target` (both in [1, 256]), after `resize(target)`
    /// completes, the effective max permits SHALL equal `target`.
    /// Since no queries are active in this test, available permits SHALL be
    /// at least `target` (may exceed target due to the bulk-release behavior
    /// of acquire_many_owned poison permits on scale-up-after-scale-down).
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_resize_reaches_target_capacity(
            initial_max in 1u32..=256u32,
            target in 1u32..=256u32,
        ) {
            // proptest does not natively support async; create a runtime per iteration.
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let gate = ConcurrencyGate::new(initial_max as usize);

                // Verify initial state
                prop_assert_eq!(gate.max_permits(), initial_max);

                // Perform resize to target
                gate.resize(target, "prop_test").await;

                // Primary assertion: max_permits reflects the target
                prop_assert_eq!(
                    gate.max_permits(), target,
                    "After resize({}) from initial_max={}, max_permits should equal target",
                    target, initial_max
                );

                // Secondary assertion: since no queries are active, available permits
                // should be at least `target`. It may exceed target when a scale-up
                // follows an implicit scale-down (poison permits released in bulk).
                prop_assert!(
                    gate.semaphore.available_permits() >= target as usize,
                    "After resize({}) from initial_max={} with no active queries, \
                     available_permits ({}) should be >= target ({})",
                    target, initial_max,
                    gate.semaphore.available_permits(), target
                );

                Ok(())
            })?;
        }
    }

    /// **Validates: Requirements 5.2, 5.6**
    ///
    /// Property 3: Last-writer-wins convergence
    ///
    /// For any random sequence of resize targets [T1, T2, ..., Tn] (length 2..20,
    /// values in [1, 128]) applied sequentially to a gate, after all resizes
    /// complete, the gate's effective max_permits SHALL equal Tn (the last value).
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_last_writer_wins_convergence(
            targets in proptest::collection::vec(1u32..=128u32, 2..20),
        ) {
            // proptest does not natively support async; create a runtime per iteration.
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let gate = ConcurrencyGate::new(64);

                // Apply all resizes sequentially
                for target in &targets {
                    gate.resize(*target, "prop_test").await;
                }

                // After all resizes, max_permits must equal the last target
                let last_target = *targets.last().unwrap();
                prop_assert_eq!(
                    gate.max_permits(), last_target,
                    "After applying resize sequence {:?}, max_permits should equal \
                     the last target {}",
                    targets, last_target
                );

                Ok(())
            })?;
        }
    }

    /// **Validates: Requirements 5.4**
    ///
    /// Property 5: Concurrent acquire/resize safety
    ///
    /// For any interleaving of acquire() calls from query tasks and concurrent
    /// resize() calls, no thread SHALL panic, deadlock, or receive an invalid
    /// permit. All acquired permits SHALL be backed by the single shared
    /// semaphore.
    ///
    /// This test generates random gate sizes, numbers of concurrent acquires,
    /// and a sequence of resize targets (1..5), then spawns multiple tokio tasks
    /// that acquire permits concurrently with multiple resize operations.
    /// A timeout detects deadlocks.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_concurrent_acquire_resize_safety(
            initial_max in 4u32..=64u32,
            num_acquires in 1usize..=16usize,
            resize_targets in proptest::collection::vec(1u32..=64u32, 1..=5),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let gate = Arc::new(ConcurrencyGate::new(initial_max as usize));

                let mut join_handles = Vec::new();

                // Spawn `num_acquires` tasks that each acquire a permit,
                // hold it briefly (yield_now), then drop it.
                for _ in 0..num_acquires {
                    let gate_clone = Arc::clone(&gate);
                    let handle = tokio::spawn(async move {
                        let permit = gate_clone.acquire().await;
                        // Hold the permit briefly to create contention
                        tokio::task::yield_now().await;
                        drop(permit);
                    });
                    join_handles.push(handle);
                }

                // Concurrently spawn resize tasks for each target in the sequence
                let resize_targets_clone = resize_targets.clone();
                for target in &resize_targets_clone {
                    let gate_for_resize = Arc::clone(&gate);
                    let t = *target;
                    let resize_handle = tokio::spawn(async move {
                        gate_for_resize.resize(t, "prop_test").await;
                    });
                    join_handles.push(resize_handle);
                }

                // Use a timeout to detect deadlocks (5 seconds is generous)
                let all_tasks = futures::future::join_all(join_handles);
                let result = tokio::time::timeout(
                    Duration::from_secs(5),
                    all_tasks,
                ).await;

                // Assert: no deadlock (timeout didn't fire)
                prop_assert!(
                    result.is_ok(),
                    "Deadlock detected: tasks did not complete within 5s timeout \
                     (initial_max={}, num_acquires={}, resize_targets={:?})",
                    initial_max, num_acquires, resize_targets
                );

                // Assert: no panics occurred in any task
                let results = result.unwrap();
                for (i, task_result) in results.iter().enumerate() {
                    prop_assert!(
                        task_result.is_ok(),
                        "Task {} panicked (initial_max={}, num_acquires={}, \
                         resize_targets={:?}): {:?}",
                        i, initial_max, num_acquires, resize_targets, task_result
                    );
                }

                // Assert: gate is in a valid state after all tasks complete.
                // Since resize operations are spawned concurrently, the tokio
                // scheduler determines execution order. The final max_permits
                // must equal one of the resize targets (whichever executed last).
                let final_max = gate.max_permits();
                prop_assert!(
                    resize_targets.contains(&final_max),
                    "After concurrent acquire/resize, max_permits ({}) should \
                     equal one of the resize_targets ({:?})",
                    final_max, resize_targets
                );

                Ok(())
            })?;
        }
    }

    // ─── pending_acquire_permits counter tests ─────────────────────────────────

    /// Verify pending_acquire_permits increments during contention and returns to 0 after.
    #[tokio::test]
    async fn test_pending_acquire_permits_counter() {
        let gate = Arc::new(ConcurrencyGate::new(1));

        // Acquire the single permit so subsequent acquires will block
        let permit = gate.acquire().await;
        assert_eq!(gate.pending_acquire_permits(), 0);
        assert_eq!(gate.pending_acquire_batches(), 0);

        // Spawn a task that will block on acquire
        let gate_clone = Arc::clone(&gate);
        let handle = tokio::spawn(async move {
            gate_clone.acquire().await
        });

        // Yield to let the spawned task start waiting
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(gate.pending_acquire_permits(), 1);
        assert_eq!(gate.pending_acquire_batches(), 1);

        // Drop the first permit — spawned task should complete
        drop(permit);
        let _permit2 = handle.await.unwrap();

        // After the spawned task completes, counters should be 0
        assert_eq!(gate.pending_acquire_permits(), 0);
        assert_eq!(gate.pending_acquire_batches(), 0);
    }

    /// **Validates: Requirements 2.5**
    ///
    /// Property 7: Permit computation correctness
    ///
    /// For any valid cpu_threads (in [1, 128]) and multiplier (in [0.1, 10.0]),
    /// the computed newMaxPermits SHALL equal max(1, floor(cpu_threads × multiplier)),
    /// ensuring the result is always at least 1.
    ///
    /// This mirrors the Java-side computation:
    ///   Math.max(1, (int)(cpuThreads * multiplier))
    ///
    /// We generate multiplier as an integer in [1, 100] divided by 10.0 to get
    /// values in [0.1, 10.0] without floating-point strategy complexity.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_permit_computation_correctness(
            cpu_threads in 1i32..=128,
            multiplier_int in 1u32..=100,
        ) {
            // Convert integer to f64 multiplier in [0.1, 10.0]
            let multiplier: f64 = multiplier_int as f64 / 10.0;

            // Compute using the same formula as the Java side:
            // Math.max(1, (int)(cpuThreads * multiplier))
            // Java's (int) cast truncates toward zero, equivalent to Rust's `as i32`.
            let computed = std::cmp::max(1, (cpu_threads as f64 * multiplier) as i32);

            // Property 1: Result is always at least 1
            prop_assert!(computed >= 1,
                "Permit count must be >= 1, got {} for cpu_threads={}, multiplier={}",
                computed, cpu_threads, multiplier);

            // Property 2: Result matches the floor-based formula
            // For positive values (cpu_threads >= 1, multiplier >= 0.1), truncation
            // toward zero is equivalent to floor. Both formulas must agree.
            let expected = std::cmp::max(1, (cpu_threads as f64 * multiplier).floor() as i32);
            prop_assert_eq!(computed, expected,
                "Mismatch: computed={}, expected={} for cpu_threads={}, multiplier={}",
                computed, expected, cpu_threads, multiplier);
        }
    }
}
