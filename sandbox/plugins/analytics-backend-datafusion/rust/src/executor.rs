/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use futures::{future::BoxFuture, Future, FutureExt, TryFutureExt};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    runtime::Handle,
    sync::{oneshot::error::RecvError, Notify},
    task::JoinSet,
};

use crate::io::register_io_runtime;
// DedicatedExecutor — runs CPU-bound DataFusion work on its own tokio runtime.
// Based on InfluxDB's executor pattern.
// https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/query_planning/thread_pools.rs

#[derive(Clone)]
pub struct DedicatedExecutor {
    state: Arc<RwLock<State>>,
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
    pub fn new(name: &str, runtime_builder: tokio::runtime::Builder) -> Self {
        let name = name.to_owned();
        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);
        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel();
        let (tx_handle, rx_handle) = std::sync::mpsc::channel();
        let io_handle = tokio::runtime::Handle::try_current().ok();

        let thread = std::thread::Builder::new()
            .name(format!("{name} driver"))
            .spawn(move || {
                register_io_runtime(io_handle.clone());
                let mut runtime_builder = runtime_builder;
                let runtime = runtime_builder
                    .on_thread_start(move || register_io_runtime(io_handle.clone()))
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
            completed_shutdown: rx_shutdown
                .map_err(Arc::new)
                .boxed()
                .shared(),
            thread: Some(thread),
        };
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub fn spawn<T>(&self, task: T) -> impl Future<Output = Result<T::Output, JobError>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let handle = {
            let state = self.state.read();
            state.handle.clone()
        };
        let Some(handle) = handle else {
            return futures::future::err(JobError::WorkerGone).boxed();
        };
        let mut join_set = JoinSet::new();
        join_set.spawn_on(task, &handle);
        async move {
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
        .boxed()
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

    pub fn shutdown(&self) {
        let mut state = self.state.write();
        state.handle = None;
        state.start_shutdown.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use tokio::sync::Barrier as AsyncBarrier;

    fn test_exec(threads: usize) -> DedicatedExecutor {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(threads).enable_all();
        DedicatedExecutor::new("test-cpu", builder)
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
        let spawned_id = exec.spawn(async { std::thread::current().id() }).await.unwrap();
        assert_ne!(caller_id, spawned_id);
        exec.join_blocking();
    }

    #[tokio::test]
    async fn test_multi_task() {
        let barrier = Arc::new(Barrier::new(3));
        let exec = test_exec(2);
        let t1 = exec.spawn({
            let b = barrier.clone();
            async move { b.wait(); 11 }
        });
        let t2 = exec.spawn({
            let b = barrier.clone();
            async move { b.wait(); 22 }
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
}
