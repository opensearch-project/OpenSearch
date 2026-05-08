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
use tokio::runtime::{Builder, Runtime};
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

// RuntimeManager — owns IO runtime + CPU DedicatedExecutor.
pub struct RuntimeManager {
    pub io_runtime: Arc<Runtime>,
    pub cpu_executor: DedicatedExecutor,
    pub io_monitor: RuntimeMonitor,
    pub cpu_monitor: Option<RuntimeMonitor>,
    /// Smoothed CPU contention signals (EMA, alpha=0.3).
    ema_alive_tasks: EmaSignal,
    ema_queued_tasks: EmaSignal,
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

        Self {
            io_runtime,
            cpu_executor,
            io_monitor,
            cpu_monitor,
            ema_alive_tasks: EmaSignal::new(0.0),
            ema_queued_tasks: EmaSignal::new(0.0),
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
    /// Each call samples the instantaneous metrics, feeds them into the EMA,
    /// and returns the smoothed values. This prevents a single transient spike
    /// (query burst completing, GC pause releasing memory) from causing
    /// unnecessary partition reduction for the next query.
    ///
    /// Convergence: ~3-5 calls to reflect a sustained change. A single spike
    /// contributes 30% to the EMA — not enough to cross the contention
    /// threshold alone.
    pub fn cpu_contention(&self) -> CpuContention {
        let Some(metrics) = self.cpu_executor.runtime_metrics() else {
            return CpuContention::default();
        };
        let num_workers = metrics.num_workers();
        let alive_tasks = metrics.num_alive_tasks();
        let global_queue = metrics.global_queue_depth();
        let local_queue_total: usize = (0..num_workers)
            .map(|w| metrics.worker_local_queue_depth(w))
            .sum();
        let queued_tasks = global_queue + local_queue_total;

        // Feed instantaneous samples into EMA
        let smoothed_alive = self.ema_alive_tasks.update(alive_tasks as f64);
        let smoothed_queued = self.ema_queued_tasks.update(queued_tasks as f64);

        CpuContention {
            num_workers,
            alive_tasks: smoothed_alive as usize,
            queued_tasks: smoothed_queued as usize,
        }
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
