#![warn(missing_docs)]

// Have used the same code as influxdb

use parking_lot::RwLock;
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::{Notify, oneshot::error::RecvError},
    task::JoinSet,
};

use futures::{
    Future, FutureExt, TryFutureExt,
    future::{BoxFuture, Shared},
};

use tracing::warn;
use crate::io::register_io_runtime;

// copy of https://github.com/influxdata/influxdb3_core/blob/main/executor/src/lib.rs

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60 * 5);

/// Errors occurring when polling [`DedicatedExecutor::spawn`].
#[derive(Debug)]
#[expect(missing_docs)]
pub enum JobError {
    WorkerGone,
    Panic { msg: String },
}

/// Manages a separate tokio runtime (thread pool) for executing tasks.
///
/// A `DedicatedExecutor` runs futures (and any `tasks` that are
/// `tokio::task::spawned` by them) on a separate tokio Executor
///
/// # Background
///
/// Tokio has the notion of the "current" runtime, which runs the current future
/// and any tasks spawned by it. Typically, this is the runtime created by
/// `tokio::main` and is used for the main application logic and I/O handling
///
/// For CPU bound work, such as DataFusion plan execution, it is important to
/// run on a separate thread pool to avoid blocking the I/O handling for extended
/// periods of time in order to avoid long poll latencies (which decreases the
/// throughput of small requests under concurrent load).
///
/// # IO Scheduling
///
/// I/O, such as network calls, should not be performed on the runtime managed
/// by [`DedicatedExecutor`]. As tokio is a cooperative scheduler, long-running
/// CPU tasks will not be preempted and can therefore starve servicing of other
/// tasks. This manifests in long poll-latencies, where a task is ready to run
/// but isn't being scheduled to run. For CPU-bound work this isn't a problem as
/// there is no external party waiting on a response, however, for I/O tasks,
/// long poll latencies can prevent timely servicing of IO, which can have a
/// significant detrimental effect.
///
/// # Details
///
/// The worker thread priority is set to low so that such tasks do
/// not starve other more important tasks (such as answering health checks)
///
/// Follows the example from to stack overflow and spawns a new
/// thread to install a Tokio runtime "context"
/// <https://stackoverflow.com/questions/62536566>
///
/// # Trouble Shooting:
///
/// ## "No IO runtime registered. Call `register_io_runtime`/`register_current_runtime_for_io` in current thread!
///
/// This means that IO was attempted on a tokio runtime that was not registered
/// for IO. One solution is to run the task using [DedicatedExecutor::spawn].
///
/// ## "Cannot drop a runtime in a context where blocking is not allowed"`
///
/// If you try to use this structure from an async context you see something like
/// thread 'plan::stringset::tests::test_builder_plan' panicked at 'Cannot
/// drop a runtime in a context where blocking is not allowed. This
/// happens when a runtime is dropped from within an asynchronous
/// context.', .../tokio-1.4.0/src/runtime/blocking/shutdown.rs:51:21
///
#[derive(Clone)]
pub struct DedicatedExecutor {
    state: Arc<RwLock<State>>,
}

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio Executor.
///
/// The state is only used by the "outer" API, not by the newly created runtime. The new runtime waits for
/// [`start_shutdown`](Self::start_shutdown) and signals the completion via
/// [`completed_shutdown`](Self::completed_shutdown) (for which is owns the sender side).
struct State {
    /// Runtime handle.
    ///
    /// This is `None` when the executor is shutting down.
    handle: Option<Handle>,

    /// If notified, the executor tokio runtime will begin to shutdown.
    ///
    /// We could implement this by checking `handle.is_none()` in regular intervals but requires regular wake-ups and
    /// locking of the state. Just using a proper async signal is nicer.
    start_shutdown: Arc<Notify>,

    /// Receiver side indicating that shutdown is complete.
    completed_shutdown: Shared<BoxFuture<'static, Result<(), Arc<RecvError>>>>,

    /// The inner thread that can be used to join during drop.
    thread: Option<std::thread::JoinHandle<()>>,
}

// IMPORTANT: Implement `Drop` for `State`, NOT for `DedicatedExecutor`, because the executor can be cloned and clones
// share their inner state.
impl Drop for State {
    fn drop(&mut self) {
        if self.handle.is_some() {
            warn!("DedicatedExecutor dropped without calling shutdown()");
            self.handle = None;
            self.start_shutdown.notify_one();
        }

        // do NOT poll the shared future if we are panicking due to https://github.com/rust-lang/futures-rs/issues/2575
        if !std::thread::panicking() && self.completed_shutdown.clone().now_or_never().is_none() {
            warn!("DedicatedExecutor dropped without waiting for worker termination",);
        }

        // join thread but don't care about the results
        self.thread.take().expect("not dropped yet").join().ok();
    }
}

impl std::fmt::Debug for DedicatedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Avoid taking the mutex in debug formatting
        write!(f, "DedicatedExecutor")
    }
}

/// [`DedicatedExecutor`] for testing purposes.
static TESTING_EXECUTOR: OnceLock<DedicatedExecutor> = OnceLock::new();

impl DedicatedExecutor {
    /// Creates a new `DedicatedExecutor` with a dedicated tokio
    /// executor that is separate from the threadpool created via
    /// `[tokio::main]` or similar.
    ///
    /// See the documentation on [`DedicatedExecutor`] for more details.
    ///
    /// If [`DedicatedExecutor::new`] is called from an existing tokio runtime,
    /// it will assume that the existing runtime should be used for I/O, and is
    /// thus set, via [`register_io_runtime`] by all threads spawned by the
    /// executor. This will allow scheduling IO outside the context of
    /// [`DedicatedExecutor`] using [`spawn_io`].
    pub fn new(
        name: &str,
        runtime_builder: tokio::runtime::Builder,
    ) -> Self {
        Self::new_inner(name, runtime_builder, false)
    }

    fn new_inner(
        name: &str,
        runtime_builder: tokio::runtime::Builder,
        testing: bool,
    ) -> Self {
        let name = name.to_owned();

        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel();
        let (tx_handle, rx_handle) = std::sync::mpsc::channel();

        let io_handle = tokio::runtime::Handle::try_current().ok();
        let thread = std::thread::Builder::new()
            .name(format!("{name} driver"))
            .spawn(move || {
                // also register the IO runtime for the current thread, since it might be used as well (esp. for the
                // current thread RT)
                register_io_runtime(io_handle.clone());

                let mut runtime_builder = runtime_builder;
                let runtime = runtime_builder
                    .on_thread_start(move || register_io_runtime(io_handle.clone()))
                    .build()
                    .expect("Creating tokio runtime");


                runtime.block_on(async move {
                    // Enable the "notified" receiver BEFORE sending the runtime handle back to the constructor thread
                    // (i.e .the one that runs `new`) to avoid the potential (but unlikely) race that the shutdown is
                    // started right after the constructor finishes and the new runtime calls
                    // `notify_shutdown_captured.notified().await`.
                    //
                    // Tokio provides an API for that by calling `enable` on the `notified` future (this requires
                    // pinning though).
                    let shutdown = notify_shutdown_captured.notified();
                    let mut shutdown = std::pin::pin!(shutdown);
                    shutdown.as_mut().enable();

                    if tx_handle.send(Handle::current()).is_err() {
                        return;
                    }
                    shutdown.await;
                });

                runtime.shutdown_timeout(SHUTDOWN_TIMEOUT);

                // send shutdown "done" signal
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
        }
    }

    /// Runs the specified [`Future`] (and any tasks it spawns) on the thread
    /// pool managed by this `DedicatedExecutor`.
    ///
    /// # Notes
    ///
    /// UNLIKE [`tokio::task::spawn`], the returned future is **cancelled** when
    /// it is dropped. Thus, you need ensure the returned future lives until it
    /// completes (call `await`) or you wish to cancel it.
    ///
    /// Currently all tasks are added to the tokio executor immediately and
    /// compete for the threadpool's resources.
    pub fn spawn<T>(&self, task: T) -> impl Future<Output = Result<T::Output, JobError>> + use<T>
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

        // use JoinSet implement "cancel on drop"
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

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all clones of this
    /// `DedicatedExecutor` as well.
    ///
    /// Only the first call to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
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

    /// signals shutdown of this executor and any Clones
    pub fn shutdown(&self) {
        // hang up the channel which will cause the dedicated thread
        // to quit
        let mut state = self.state.write();
        state.handle = None;
        state.start_shutdown.notify_one();
    }
}
