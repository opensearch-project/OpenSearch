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
use std::sync::RwLock;
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

/// Process-global IO runtime handle.
///
/// Unlike [`IO_RUNTIME`] (thread-local, only set on runtime worker threads),
/// this is reachable from ANY thread — including the bare Java/FFM threads that
/// register object stores and plan queries. [`SpawnIoStore`](crate::spawn_io_store)
/// uses it to dispatch every object-store read onto the IO runtime regardless of
/// which thread polls the store.
///
/// This MUST track the *current* `RuntimeManager`'s IO runtime, not the first
/// one ever installed: `DataFusionService.doStop()` shuts the IO runtime down
/// and `doStart()` builds a fresh one (node restarts in tests, service
/// recycling). A first-wins `OnceLock` would leave `SpawnIoStore` dispatching
/// onto a dead runtime, so every spawned read joins as *cancelled* and fails
/// the query ("object-store read was cancelled"). A swappable slot keeps the
/// handle live across restarts.
static GLOBAL_IO_HANDLE: RwLock<Option<Handle>> = RwLock::new(None);

/// Install (or replace) the process-global IO runtime handle. The most recent
/// writer wins, so the handle always points at the live IO runtime — see the
/// note on [`GLOBAL_IO_HANDLE`].
pub fn set_global_io_handle(handle: Handle) {
     *GLOBAL_IO_HANDLE.write().unwrap() = Some(handle);
}

/// Clear the process-global IO runtime handle (called when the owning
/// `RuntimeManager` is shut down) so a stale, dead-runtime handle is never
/// handed out. `SpawnIoStore::wrap` then falls back to the unwrapped store.
pub fn clear_global_io_handle() {
    *GLOBAL_IO_HANDLE.write().unwrap() = None;
}

/// Returns the process-global IO runtime handle, if one is currently installed.
pub fn global_io_handle() -> Option<Handle> {
    GLOBAL_IO_HANDLE.read().unwrap().clone()
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
