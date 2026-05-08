/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use crate::executor::DedicatedExecutor;
use crate::io::register_io_runtime;
use log::info;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::JoinHandle;
use tokio_metrics::RuntimeMonitor;

/// Exponential moving average stored as fixed-point (×1000) in an AtomicU64.
/// Smooths instantaneous signals to avoid reacting to transient spikes.
///
/// EMA formula: `ema = alpha × sample + (1 - alpha) × prev_ema`
/// where alpha = 0.3 (responds within ~3-5 samples, dampens single-sample spikes).
pub struct EmaSignal {
    value_x1000: AtomicU64,
}

/// Alpha = 0.3 as fixed-point ×1000
const EMA_ALPHA_X1000: u64 = 300;

impl EmaSignal {
    pub fn new(initial: f64) -> Self {
        Self {
            value_x1000: AtomicU64::new((initial * 1000.0) as u64),
        }
    }

    /// Update with a new sample and return the smoothed value.
    pub fn update(&self, sample: f64) -> f64 {
        let sample_x1000 = (sample * 1000.0) as u64;
        let prev = self.value_x1000.load(Ordering::Relaxed);
        let new_val = (EMA_ALPHA_X1000 * sample_x1000 + (1000 - EMA_ALPHA_X1000) * prev) / 1000;
        self.value_x1000.store(new_val, Ordering::Relaxed);
        new_val as f64 / 1000.0
    }

    /// Read the current smoothed value without updating.
    pub fn get(&self) -> f64 {
        self.value_x1000.load(Ordering::Relaxed) as f64 / 1000.0
    }
}

/// Snapshot of CPU executor contention. All fields are instantaneous — they
/// represent the state at the moment of the call, not averages.
#[derive(Debug, Clone, Default)]
pub struct CpuContention {
    /// Number of worker threads in the CPU executor.
    pub num_workers: usize,
    /// Number of tasks currently alive (spawned but not yet completed).
    pub alive_tasks: usize,
    /// Number of tasks waiting in queues (global + all local worker queues).
    pub queued_tasks: usize,
}

impl CpuContention {
    /// Ratio of alive tasks to workers. > 1.0 means tasks are competing for
    /// CPU time. > 2.0 means significant queuing.
    pub fn task_per_worker_ratio(&self) -> f64 {
        if self.num_workers == 0 {
            return 0.0;
        }
        self.alive_tasks as f64 / self.num_workers as f64
    }

    /// True if the executor is under significant contention (more queued tasks
    /// than workers available to run them).
    pub fn is_contended(&self) -> bool {
        self.queued_tasks > self.num_workers
    }
}

/// Shared state updated by the background metrics ticker.
/// The hot path (`cpu_contention()`) only reads these atomics — no sampling,
/// no iteration over worker queues, no jemalloc epoch advance.
struct MetricsTicker {
    ema_alive_tasks: EmaSignal,
    ema_queued_tasks: EmaSignal,
    ema_jemalloc_allocated: EmaSignal,
    num_workers: AtomicU64,
}

/// How often the background ticker samples runtime metrics.
/// 50ms balances responsiveness (20 samples/sec) with negligible CPU cost.
const TICKER_INTERVAL: Duration = Duration::from_millis(50);

// RuntimeManager — owns IO runtime + CPU DedicatedExecutor.
pub struct RuntimeManager {
    pub io_runtime: Arc<Runtime>,
    pub cpu_executor: DedicatedExecutor,
    pub io_monitor: RuntimeMonitor,
    pub cpu_monitor: Option<RuntimeMonitor>,
    metrics: Arc<MetricsTicker>,
    _ticker_handle: Option<JoinHandle<()>>,
}

impl RuntimeManager {
    pub fn new(cpu_threads: usize) -> Self {
        let io_threads = cpu_threads * 2;

        let io_runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(io_threads)
                .thread_name("datafusion-io")
                .enable_all()
                .build()
                .expect("Failed to create IO runtime"),
        );

        register_io_runtime(Some(io_runtime.handle().clone()));

        let io_monitor = RuntimeMonitor::new(&io_runtime.handle());

        let io_handle = io_runtime.handle().clone();
        let mut cpu_runtime_builder = Builder::new_multi_thread();
        cpu_runtime_builder
            .worker_threads(cpu_threads)
            .thread_name("datafusion-cpu")
            .enable_all()
            .on_thread_start(move || {
                register_io_runtime(Some(io_handle.clone()));
            });

        let cpu_executor = DedicatedExecutor::new("datafusion-cpu", cpu_runtime_builder);

        let cpu_monitor = cpu_executor
            .handle()
            .map(|h| RuntimeMonitor::new(&h));

        let metrics = Arc::new(MetricsTicker {
            ema_alive_tasks: EmaSignal::new(0.0),
            ema_queued_tasks: EmaSignal::new(0.0),
            ema_jemalloc_allocated: EmaSignal::new(0.0),
            num_workers: AtomicU64::new(cpu_threads as u64),
        });

        // Spawn background ticker on IO runtime — samples CPU metrics + jemalloc
        // every TICKER_INTERVAL and feeds into EMA. The hot path only reads atomics.
        let ticker_handle = if let Some(cpu_handle) = cpu_executor.handle() {
            let metrics_clone = Arc::clone(&metrics);
            let handle = io_runtime.spawn(async move {
                let mut interval = tokio::time::interval(TICKER_INTERVAL);
                loop {
                    interval.tick().await;
                    let rt_metrics = cpu_handle.metrics();
                    let num_workers = rt_metrics.num_workers();
                    let alive_tasks = rt_metrics.num_alive_tasks();
                    let global_queue = rt_metrics.global_queue_depth();
                    let local_queue_total: usize = (0..num_workers)
                        .map(|w| rt_metrics.worker_local_queue_depth(w))
                        .sum();
                    let queued_tasks = global_queue + local_queue_total;

                    metrics_clone.ema_alive_tasks.update(alive_tasks as f64);
                    metrics_clone.ema_queued_tasks.update(queued_tasks as f64);
                    metrics_clone.num_workers.store(num_workers as u64, Ordering::Relaxed);

                    // Sample jemalloc and publish smoothed value for query_budget
                    let allocated = native_bridge_common::allocator::allocated_bytes();
                    if allocated > 0 {
                        let smoothed = metrics_clone.ema_jemalloc_allocated.update(allocated as f64);
                        crate::query_budget::publish_smoothed_jemalloc(smoothed as usize);
                    }
                }
            });
            Some(handle)
        } else {
            None
        };

        Self {
            io_runtime,
            cpu_executor,
            io_monitor,
            cpu_monitor,
            metrics,
            _ticker_handle: ticker_handle,
        }
    }

    pub fn cpu_executor(&self) -> DedicatedExecutor {
        self.cpu_executor.clone()
    }

    /// Returns the number of CPU worker threads configured for this runtime.
    pub fn cpu_thread_count(&self) -> usize {
        self.cpu_executor
            .runtime_metrics()
            .map(|m| m.num_workers())
            .unwrap_or(0)
    }

    /// Smoothed CPU executor contention state (EMA, alpha=0.3).
    ///
    /// Reads pre-computed values from the background ticker — zero sampling,
    /// zero worker-queue iteration on the hot path. Just 3 atomic loads (~5ns).
    ///
    /// The ticker runs every 50ms on the IO runtime, feeding instantaneous
    /// metrics into the EMA. This means the values returned here are at most
    /// 50ms stale — acceptable for partition decisions that affect queries
    /// lasting seconds.
    pub fn cpu_contention(&self) -> CpuContention {
        CpuContention {
            num_workers: self.metrics.num_workers.load(Ordering::Relaxed) as usize,
            alive_tasks: self.metrics.ema_alive_tasks.get() as usize,
            queued_tasks: self.metrics.ema_queued_tasks.get() as usize,
        }
    }

    /// Smoothed jemalloc allocated bytes. Updated by the background ticker.
    /// Used by `query_budget::should_override_pool_rejection` instead of
    /// calling jemalloc directly on the hot path.
    pub fn smoothed_jemalloc_allocated(&self) -> usize {
        self.metrics.ema_jemalloc_allocated.get() as usize
    }

    pub fn shutdown(&self) {
        info!("Shutting down RuntimeManager");
        self.cpu_executor.join_blocking();
    }
}

impl Drop for RuntimeManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_mgr() -> RuntimeManager {
        RuntimeManager::new(1)
    }

    #[tokio::test]
    async fn test_runtime_manager_creates_and_shuts_down() {
        let mgr = test_mgr();
        let result = mgr.io_runtime.spawn(async { 42 }).await.unwrap();
        assert_eq!(result, 42);
        let result = mgr.cpu_executor().spawn(async { 99 }).await.unwrap();
        assert_eq!(result, 99);
        mgr.cpu_executor.shutdown();
        // Forget to avoid Drop which can't run in async context
        std::mem::forget(mgr);
    }

    #[tokio::test]
    async fn test_cpu_executor_runs_on_different_thread() {
        let mgr = test_mgr();
        let io_id = std::thread::current().id();
        let cpu_id = mgr
            .cpu_executor()
            .spawn(async { std::thread::current().id() })
            .await
            .unwrap();
        assert_ne!(io_id, cpu_id);
        mgr.cpu_executor.shutdown();
        std::mem::forget(mgr);
    }

    #[tokio::test]
    async fn test_io_runtime_registered_on_cpu_threads() {
        let mgr = test_mgr();
        let has_io = mgr
            .cpu_executor()
            .spawn(async { crate::io::IO_RUNTIME.with_borrow(|h| h.is_some()) })
            .await
            .unwrap();
        assert!(has_io);
        mgr.cpu_executor.shutdown();
        std::mem::forget(mgr);
    }
}
