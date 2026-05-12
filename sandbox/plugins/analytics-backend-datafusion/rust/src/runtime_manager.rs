/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use crate::executor::{ConcurrencyGate, DedicatedExecutor};
use crate::io::register_io_runtime;
use log::info;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio_metrics::RuntimeMonitor;

// RuntimeManager — owns IO runtime + CPU DedicatedExecutor.
pub struct RuntimeManager {
    pub io_runtime: Arc<Runtime>,
    pub cpu_executor: DedicatedExecutor,
    pub io_monitor: RuntimeMonitor,
    pub cpu_monitor: Option<RuntimeMonitor>,
    /// Separate concurrency gate for coordinator-reduce execution.
    /// Independent from the datanode gate (on DedicatedExecutor) to avoid
    /// deadlock when shard streams and coordinator reduce run concurrently.
    coordinator_gate: Arc<ConcurrencyGate>,
}

impl RuntimeManager {
    pub fn new(cpu_threads: usize, datanode_multiplier: f64, coordinator_multiplier: f64) -> Self {
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

        // Datanode concurrency gate: limits concurrent partition tasks from shard scans.
        let datanode_max_concurrent = (cpu_threads as f64 * datanode_multiplier).max(1.0) as usize;
        let cpu_executor = DedicatedExecutor::new("datafusion-cpu", cpu_runtime_builder, datanode_max_concurrent);

        let cpu_monitor = cpu_executor
            .handle()
            .map(|h| RuntimeMonitor::new(&h));

        // Coordinator concurrency gate: limits concurrent partition tasks from reduce execution.
        // Separate from datanode gate to avoid deadlock (shard streams hold datanode permits
        // while coordinator reduce runs concurrently on single-node clusters).
        let coordinator_max_concurrent = (cpu_threads as f64 * coordinator_multiplier).max(1.0) as usize;
        let coordinator_gate = Arc::new(ConcurrencyGate::new(coordinator_max_concurrent));

        Self {
            io_runtime,
            cpu_executor,
            io_monitor,
            cpu_monitor,
            coordinator_gate,
        }
    }

    pub fn cpu_executor(&self) -> DedicatedExecutor {
        self.cpu_executor.clone()
    }

    pub fn coordinator_gate(&self) -> &Arc<ConcurrencyGate> {
        &self.coordinator_gate
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
        RuntimeManager::new(1, 1.0, 1.0)
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
