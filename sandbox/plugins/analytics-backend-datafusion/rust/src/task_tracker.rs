/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Lightweight task lifecycle tracker using tokio's unstable runtime hooks.
//!
//! Tracks spawn/terminate/poll events for all tasks on instrumented runtimes.
//! Exposes a `zombie_tasks()` query that returns tasks whose last poll started
//! more than a threshold ago without completing — the diagnostic signal for
//! tasks stuck in synchronous `poll_next` loops after cancellation.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;
use native_bridge_common::log_error;
use tokio::task::Id as TaskId;

/// Process-wide start time for relative timestamps.
static PROCESS_START: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

/// Global registry of live tasks.
static LIVE_TASKS: std::sync::LazyLock<DashMap<TaskId, TaskInfo>> =
    std::sync::LazyLock::new(DashMap::new);

/// Whether tracking is enabled. Disabled by default; enabled via `enable()`.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Per-task metadata.
pub struct TaskInfo {
    pub spawn_nanos: u64,
    pub last_poll_start_nanos: AtomicU64,
    pub poll_count: AtomicU64,
    pub context_id: AtomicI64,
    pub runtime_name: &'static str,
}

/// A task that has been polling for longer than the threshold.
pub struct ZombieTask {
    pub task_id: TaskId,
    pub context_id: i64,
    pub spawn_age_secs: f64,
    pub poll_duration_secs: f64,
    pub poll_count: u64,
    pub runtime_name: &'static str,
}

/// Enable task tracking globally. Starts a background task that periodically
/// logs zombie tasks (stuck for > 10s) when any are detected.
pub fn enable() {
    let _ = *PROCESS_START;
    let was_enabled = ENABLED.swap(true, Ordering::AcqRel);
    if !was_enabled {
        // Spawn a periodic zombie check on the IO runtime (if available).
        if let Some(handle) = crate::io::IO_RUNTIME.with_borrow(|h| h.clone()) {
            handle.spawn(async {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    if !ENABLED.load(Ordering::Relaxed) {
                        return;
                    }
                    log_zombies(10.0);
                }
            });
        }
    }
}

/// Disable task tracking. The background periodic check will exit on its next tick.
pub fn disable() {
    ENABLED.store(false, Ordering::Release);
}

/// Returns true if tracking is active.
pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Acquire)
}

/// Returns tasks whose current poll has been running for longer than `threshold_secs`.
/// These are likely stuck in a synchronous loop without yielding.
pub fn zombie_tasks(threshold_secs: f64) -> Vec<ZombieTask> {
    if !is_enabled() {
        return Vec::new();
    }
    let now_nanos = PROCESS_START.elapsed().as_nanos() as u64;
    let threshold_nanos = (threshold_secs * 1_000_000_000.0) as u64;

    let mut zombies = Vec::new();
    for entry in LIVE_TASKS.iter() {
        let info = entry.value();
        let last_poll = info.last_poll_start_nanos.load(Ordering::Acquire);
        if last_poll == 0 {
            continue;
        }
        let poll_duration = now_nanos.saturating_sub(last_poll);
        if poll_duration >= threshold_nanos {
            zombies.push(ZombieTask {
                task_id: entry.key().clone(),
                context_id: info.context_id.load(Ordering::Relaxed),
                spawn_age_secs: (now_nanos - info.spawn_nanos) as f64 / 1_000_000_000.0,
                poll_duration_secs: poll_duration as f64 / 1_000_000_000.0,
                poll_count: info.poll_count.load(Ordering::Relaxed),
                runtime_name: info.runtime_name,
            });
        }
    }
    zombies
}

/// Number of currently tracked live tasks.
pub fn live_task_count() -> usize {
    LIVE_TASKS.len()
}

/// Associates the current task with a query context_id. Call this from inside
/// the spawned future (where `tokio::task::id()` returns the correct task ID).
pub fn associate_query(context_id: i64) {
    if !is_enabled() {
        return;
    }
    if let Some(task_id) = tokio::task::try_id() {
        if let Some(entry) = LIVE_TASKS.get(&task_id) {
            entry.context_id.store(context_id, Ordering::Release);
        }
    }
}

/// Install task-tracking hooks on a tokio runtime builder.
/// Call this before `.build()`.
pub fn instrument_builder(
    builder: &mut tokio::runtime::Builder,
    runtime_name: &'static str,
) {
    builder
        .on_task_spawn(move |info| {
            if !ENABLED.load(Ordering::Relaxed) {
                return;
            }
            let now = PROCESS_START.elapsed().as_nanos() as u64;
            LIVE_TASKS.insert(info.id(), TaskInfo {
                spawn_nanos: now,
                last_poll_start_nanos: AtomicU64::new(0),
                poll_count: AtomicU64::new(0),
                context_id: AtomicI64::new(0),
                runtime_name,
            });
        })
        .on_task_terminate(move |info| {
            LIVE_TASKS.remove(&info.id());
        })
        .on_before_task_poll(move |info| {
            if !ENABLED.load(Ordering::Relaxed) {
                return;
            }
            if let Some(entry) = LIVE_TASKS.get(&info.id()) {
                let now = PROCESS_START.elapsed().as_nanos() as u64;
                entry.last_poll_start_nanos.store(now, Ordering::Release);
                entry.poll_count.fetch_add(1, Ordering::Relaxed);
            }
        })
        .on_after_task_poll(move |info| {
            if let Some(entry) = LIVE_TASKS.get(&info.id()) {
                entry.last_poll_start_nanos.store(0, Ordering::Release);
            }
        });
}

/// Log any zombie tasks that have been polling for longer than the threshold.
/// Intended to be called periodically (e.g., from a metrics collection loop).
pub fn log_zombies(threshold_secs: f64) {
    let zombies = zombie_tasks(threshold_secs);
    for z in &zombies {
        log_error!(
            "ZOMBIE TASK: id={:?} context_id={} runtime={} poll_duration={:.1}s spawn_age={:.1}s polls={}",
            z.task_id, z.context_id, z.runtime_name, z.poll_duration_secs, z.spawn_age_secs, z.poll_count
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    fn test_runtime(name: &'static str) -> tokio::runtime::Runtime {
        // Reset global state for test isolation
        enable();
        LIVE_TASKS.clear();

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(2).enable_all();
        instrument_builder(&mut builder, name);
        builder.build().unwrap()
    }

    /// A future that blocks the thread synchronously on first poll,
    /// simulating a CPU-bound DataFusion operator.
    struct BlockingFuture {
        duration: std::time::Duration,
        done: bool,
    }

    impl Future for BlockingFuture {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            if self.done {
                return Poll::Ready(());
            }
            std::thread::sleep(self.duration);
            self.done = true;
            Poll::Ready(())
        }
    }

    #[tokio::test]
    async fn test_no_zombies_when_disabled() {
        LIVE_TASKS.clear();
        disable();
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(2).enable_all();
        instrument_builder(&mut builder, "test-disabled");
        let rt = builder.build().unwrap();
        rt.spawn(async { tokio::time::sleep(std::time::Duration::from_secs(60)).await }).abort();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(zombie_tasks(0.0).is_empty());
        rt.shutdown_background();
    }

    #[tokio::test]
    async fn test_healthy_task_not_zombie() {
        enable();
        let rt = test_runtime("test-healthy");

        // A task that yields properly is never a zombie
        let handle = rt.spawn(async {
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
        });
        handle.await.unwrap();

        assert!(zombie_tasks(0.01).is_empty());
        rt.shutdown_background();
    }

    #[tokio::test]
    async fn test_blocking_task_detected_as_zombie() {
        enable();
        let rt = test_runtime("test-zombie");

        // Spawn a task that blocks for 3 seconds in a single poll
        let _handle = rt.spawn(BlockingFuture {
            duration: std::time::Duration::from_secs(3),
            done: false,
        });

        // Wait for the task to enter its blocking poll
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // With threshold 0.05s, the task should be detected as zombie
        let zombies = zombie_tasks(0.05);
        assert_eq!(zombies.len(), 1, "Expected 1 zombie task");
        assert!(zombies[0].poll_duration_secs >= 0.05);
        assert_eq!(zombies[0].poll_count, 1);
        assert_eq!(zombies[0].runtime_name, "test-zombie");

        rt.shutdown_background();
    }

    #[tokio::test]
    async fn test_zombie_disappears_after_poll_completes() {
        enable();
        let rt = test_runtime("test-disappear");

        // Spawn a task that blocks for 500ms then completes
        let handle = rt.spawn(BlockingFuture {
            duration: std::time::Duration::from_millis(500),
            done: false,
        });

        // While blocking: should be zombie
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!zombie_tasks(0.05).is_empty(), "Should be zombie while blocking");

        // After completion: should not be zombie
        handle.await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let zombies: Vec<_> = zombie_tasks(0.05)
            .into_iter()
            .filter(|z| z.runtime_name == "test-disappear")
            .collect();
        assert!(zombies.is_empty(), "Should not be zombie after completion");

        rt.shutdown_background();
    }

    #[tokio::test]
    async fn test_associate_query_links_context_id() {
        enable();
        let rt = test_runtime("test-assoc");

        let handle = rt.spawn(async {
            associate_query(12345);
            // Block to stay visible as a zombie
            std::thread::sleep(std::time::Duration::from_secs(2));
        });

        // Wait for task to enter blocking section
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let zombies: Vec<_> = zombie_tasks(0.05)
            .into_iter()
            .filter(|z| z.context_id == 12345)
            .collect();
        assert_eq!(zombies.len(), 1, "Should find zombie with context_id=12345");
        assert_eq!(zombies[0].context_id, 12345);

        rt.shutdown_background();
        drop(handle);
    }

    #[tokio::test]
    async fn test_live_task_count_tracks_spawn_and_terminate() {
        let rt = test_runtime("test-count");

        let before = live_task_count();

        // Spawn a task that stays alive long enough to be counted
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = rt.spawn(async move {
            rx.await.ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let during = live_task_count();
        assert!(during > before, "Live count should increase after spawn");

        // Signal task to complete
        tx.send(()).ok();
        handle.await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let after = live_task_count();
        assert!(after < during, "Live count should decrease after terminate");

        rt.shutdown_background();
    }
}
