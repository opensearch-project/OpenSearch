use futures::FutureExt;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

thread_local! {
    /// Tokio runtime `Handle` for doing network (I/O) operations, see [`spawn_io`]
    pub static IO_RUNTIME: RefCell<Option<Handle>> = const { RefCell::new(None) };
}

/// Registers `handle` as the IO runtime for this thread
///
/// See [`spawn_io`]
pub fn register_io_runtime(handle: Option<Handle>) {
    IO_RUNTIME.set(handle)
}

/// [Registers](register_io_runtime) current runtime as IO runtime.
///
/// This is mostly a convenience function for testing.
pub fn register_current_runtime_for_io() {
    register_io_runtime(Some(Handle::current()));
}

/// Runs `fut` on the runtime registered by [`register_io_runtime`] if any,
/// otherwise awaits on the current thread
///
/// # Panic
/// Needs a IO runtime [registered](register_io_runtime).
pub async fn spawn_io<Fut>(fut: Fut) -> Fut::Output
where
    Fut: Future + Send + 'static,
    Fut::Output: Send,
{
    let h = IO_RUNTIME.with_borrow(|h| h.clone()).expect(
        "No IO runtime registered. If you hit this panic, it likely \
            means a DataFusion plan or other CPU bound work is running on the \
            a tokio threadpool used for IO. Try spawning the work using \
            `DedicatedExecutor::spawn` or for tests `register_current_runtime_for_io`",
    );
    DropGuard(h.spawn(fut)).await
}

struct DropGuard<T>(JoinHandle<T>);

impl<T> Drop for DropGuard<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}

impl<T> Future for DropGuard<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(match std::task::ready!(self.0.poll_unpin(cx)) {
            Ok(v) => v,
            Err(e) if e.is_cancelled() => panic!("IO runtime was shut down"),
            Err(e) => std::panic::resume_unwind(e.into_panic()),
        })
    }
}