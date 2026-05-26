/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use futures::FutureExt;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

// IO runtime thread-local registration.
// Ensures CPU-bound DataFusion threads can dispatch IO to the IO runtime.

thread_local! {
    pub static IO_RUNTIME: RefCell<Option<Handle>> = const { RefCell::new(None) };
}

pub fn register_io_runtime(handle: Option<Handle>) {
    IO_RUNTIME.set(handle)
}

/// Runs `fut` on the IO runtime registered for this thread.
pub async fn spawn_io<Fut>(fut: Fut) -> Fut::Output
where
    Fut: Future + Send + 'static,
    Fut::Output: Send,
{
    let h = IO_RUNTIME
        .with_borrow(|h| h.clone())
        .expect("No IO runtime registered");
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
