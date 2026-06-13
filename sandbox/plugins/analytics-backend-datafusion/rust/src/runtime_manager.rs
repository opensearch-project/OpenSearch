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
    /// Independent from the fragment executor gate (on DedicatedExecutor) to avoid
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
        // Install the process-global IO handle so SpawnIoStore can dispatch reads
        // onto the IO runtime from any thread (including bare Java/FFM threads).
        crate::io::set_global_io_handle(io_runtime.handle().clone());

        let io_monitor = RuntimeMonitor::new(&io_runtime.handle());

        let io_handle = io_runtime.handle().clone();
        let mut cpu_runtime_builder = Builder::new_multi_thread();
        cpu_runtime_builder
            .worker_threads(cpu_threads)
            .thread_name("datafusion-cpu")
            .enable_all();
        // NOTE: do NOT set `on_thread_start` here to register the IO runtime.
        // `DedicatedExecutor::new` sets its own `on_thread_start`, and tokio's
        // builder *replaces* (not chains) the callback, so any registration set
        // here would be silently clobbered. Pass the handle explicitly instead.

        // Fragment executor concurrency gate: limits concurrent partition tasks from shard scans.
        let datanode_max_concurrent = (cpu_threads as f64 * datanode_multiplier).max(1.0) as usize;
        let cpu_executor = DedicatedExecutor::new_with_io_handle(
            "datafusion-cpu",
            cpu_runtime_builder,
            datanode_max_concurrent,
            Some(io_handle),
        );

        let cpu_monitor = cpu_executor
            .handle()
            .map(|h| RuntimeMonitor::new(&h));

        // Reduce concurrency gate: limits concurrent partition tasks from reduce execution.
        // Separate from fragment executor gate to avoid deadlock (shard streams hold fragment executor permits
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
        // Drop the process-global IO handle first so SpawnIoStore stops
        // dispatching onto the runtime we are about to tear down. Otherwise a
        // late read spawns onto a dead runtime and joins as cancelled, failing
        // the query with "object-store read was cancelled".
        crate::io::clear_global_io_handle();
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
        RuntimeManager::new(1, 1.5, 1.5)
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

    // Regression test for the IO-runtime wiring: a `spawn_io` call issued from a
    // CPU worker thread MUST execute on the dedicated `datafusion-io` runtime,
    // never inline on the CPU runtime.
    //
    // This is a *plain* (non-tokio) test on purpose: it mimics the FFM entrypoint
    // (`df_init_runtime_manager`), which runs on a bare Java thread with no
    // ambient tokio runtime. Under that condition `Handle::try_current()` is
    // `None`, so the previous wiring (which captured the ambient handle inside
    // `DedicatedExecutor::new` and let tokio's builder *replace* RuntimeManager's
    // own `on_thread_start`) registered `IO_RUNTIME = None` on every CPU worker —
    // making `spawn_io` panic. We now pass the IO handle explicitly; this asserts
    // the read actually lands on a `datafusion-io` worker.
    #[test]
    fn test_spawn_io_dispatches_to_io_runtime_from_cpu_worker() {
        let mgr = RuntimeManager::new(2, 1.5, 1.5);

        // Issue spawn_io from a CPU worker and capture the thread it ran on.
        let task = mgr.cpu_executor().spawn(async {
            crate::io::spawn_io(async {
                std::thread::current().name().map(|s| s.to_owned())
            })
            .await
        });
        // We're off any runtime here; drive the task to completion on the IO runtime.
        let thread_name = mgr.io_runtime.block_on(task).unwrap();

        assert_eq!(
            thread_name.as_deref().map(|n| n.starts_with("datafusion-io")),
            Some(true),
            "spawn_io executed on thread {:?}, expected a `datafusion-io` worker",
            thread_name,
        );

        mgr.cpu_executor.shutdown();
        std::mem::forget(mgr);
    }
}
