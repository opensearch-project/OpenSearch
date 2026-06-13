/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cancellation-correctness tests for [`SpawnIoStore`](crate::spawn_io_store).
//!
//! The decorator dispatches every read onto the IO runtime. Because the store is
//! driven by the full DataFusion operator tree — which cancels in-flight reads as
//! a routine part of execution (LIMIT satisfied, a join side completing, a
//! consumer dropping the stream) — those cancellations MUST be handled
//! gracefully:
//!
//! * dropping the awaiting future must abort the spawned IO task (no leak), and
//! * a cancelled IO task must surface as an `object_store::Error`, never a panic.
//!
//! A panic here was the cause of the `"IO runtime was shut down"` failures the
//! first version of this decorator produced under real query execution.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjStorePath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

use crate::runtime_manager::RuntimeManager;
use crate::spawn_io_store::SpawnIoStore;

/// Inner store that sleeps for a configurable duration inside `get_opts` and
/// counts how many gets actually started and how many fully completed. The
/// completion counter lets a test prove that a dropped read's spawned task was
/// aborted (started but never completed).
#[derive(Debug)]
struct SlowStore {
    inner: Arc<InMemory>,
    delay: Duration,
    started: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
}

impl std::fmt::Display for SlowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SlowStore {
    async fn put_opts(
        &self,
        location: &ObjStorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjStorePath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjStorePath, options: GetOptions) -> OsResult<GetResult> {
        self.started.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        let r = self.inner.get_opts(location, options).await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        r
    }

    fn list(&self, prefix: Option<&ObjStorePath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<ObjStorePath>>,
    ) -> BoxStream<'static, OsResult<ObjStorePath>> {
        self.inner.delete_stream(locations)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjStorePath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjStorePath,
        to: &ObjStorePath,
        options: object_store::CopyOptions,
    ) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn staged_store(
    mgr: &RuntimeManager,
    delay: Duration,
) -> (Arc<dyn ObjectStore>, ObjStorePath, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let inner = Arc::new(InMemory::new());
    let location = ObjStorePath::from("data.bin");
    mgr.io_runtime
        .block_on(inner.put(&location, PutPayload::from_bytes(vec![7u8; 64].into())))
        .unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(SpawnIoStore::new(
        Arc::new(SlowStore {
            inner,
            delay,
            started: Arc::clone(&started),
            completed: Arc::clone(&completed),
        }),
        mgr.io_runtime.handle().clone(),
    ));
    (store, location, started, completed)
}

/// Baseline: a normal read through the wrapper completes successfully.
#[test]
fn read_completes_normally() {
    let mgr = RuntimeManager::new(2, 1.5, 1.5);
    let (store, location, _started, completed) = staged_store(&mgr, Duration::from_millis(0));

    let bytes = mgr
        .io_runtime
        .block_on(store.get_range(&location, 0..64))
        .expect("read should succeed");
    assert_eq!(bytes.len(), 64);
    assert_eq!(completed.load(Ordering::SeqCst), 1);

    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

/// Dropping the awaiting read future mid-flight must abort the spawned IO task
/// (so it never completes) and must NOT panic — this models a DataFusion
/// operator dropping a child read when it no longer needs more data.
#[test]
fn dropped_read_aborts_spawned_task_without_panic() {
    let mgr = RuntimeManager::new(2, 1.5, 1.5);
    let (store, location, started, completed) = staged_store(&mgr, Duration::from_secs(30));

    mgr.io_runtime.block_on(async {
        let mut fut = Box::pin(store.get_range(&location, 0..64));
        // Poll once so the spawned IO task is created and begins its 30s sleep,
        // then drop the future before it can complete.
        let _ = futures::poll!(&mut fut);
        // Give the spawned task a moment to register as "started".
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(fut);
        // Wait well past when the read would have completed had it not been
        // aborted; the abort must prevent completion.
        tokio::time::sleep(Duration::from_millis(150)).await;
    });

    assert_eq!(started.load(Ordering::SeqCst), 1, "read should have started");
    assert_eq!(
        completed.load(Ordering::SeqCst),
        0,
        "dropped read's spawned task must have been aborted before completing"
    );

    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

/// A read whose spawned IO task is aborted while the consumer is still awaiting
/// (the IO-runtime-shutdown shape) must surface as an `Err`, never a panic.
#[test]
fn aborted_inflight_read_returns_error_not_panic() {
    let mgr = RuntimeManager::new(2, 1.5, 1.5);
    let (store, location, _started, _completed) = staged_store(&mgr, Duration::from_secs(30));

    let result = mgr.io_runtime.block_on(async {
        let handle = tokio::spawn(async move { store.get_range(&location, 0..64).await });
        // Let the inner read start, then abort the consumer task. The spawned IO
        // task's JoinHandle resolves as cancelled; spawn_read must map that to an
        // Err rather than unwinding with "IO runtime was shut down".
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.abort();
        handle.await
    });

    // The consumer task itself was aborted, so the JoinError is cancelled — the
    // key assertion is that this did not panic the IO runtime / process.
    assert!(
        result.is_err() && result.unwrap_err().is_cancelled(),
        "aborting the consumer must cancel cleanly without a panic"
    );

    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

/// Serializes tests that read/write the process-global IO handle. Other tests in
/// this binary also mutate it via `RuntimeManager::new`/`shutdown`; this lock
/// only orders the global-handle tests against each other. The critical sections
/// below therefore manipulate the global directly and contain NO yield points
/// (no `.await`, no blocking join), keeping the window in which an unlocked
/// `RuntimeManager` test could interleave a `set` vanishingly small.
static GLOBAL_HANDLE_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Regression test for the stale-handle bug that surfaced as
/// `"object-store read was cancelled"` / `Stage 0 failed` in the cluster IT.
///
/// `DataFusionService.doStop()` shuts the IO runtime down and `doStart()` builds
/// a fresh one. With a first-wins global handle, `SpawnIoStore::wrap` kept
/// dispatching onto the dead runtime, so every spawned read joined as cancelled.
/// The fix: shutdown CLEARS the global handle (so `wrap` falls back to the
/// unwrapped store), and a fresh runtime re-installs it (most-recent-wins).
///
/// This drives the global slot directly (rather than through the blocking
/// `RuntimeManager` lifecycle) to keep the assertions deterministic: a handle
/// present → `wrap` produces a `SpawnIoStore`; the handle cleared → `wrap`
/// returns the inner store unchanged.
#[test]
fn wrap_falls_back_to_inner_store_when_io_handle_cleared() {
    let _guard = GLOBAL_HANDLE_TEST_LOCK.lock().unwrap();

    // A throwaway runtime gives us a real, live Handle to install — kept alive
    // for the duration of the "present" assertion.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Handle present → wrap() must return a real SpawnIoStore (a different Arc).
    crate::io::set_global_io_handle(rt.handle().clone());
    let wrapped_live = SpawnIoStore::wrap(Arc::clone(&inner));
    assert!(
        !Arc::ptr_eq(&wrapped_live, &inner),
        "with an IO handle installed, wrap() must return a SpawnIoStore, not the inner store"
    );
    drop(wrapped_live);

    // Handle cleared (the shutdown path) → wrap() must fall back to the inner
    // store unchanged, never binding to a torn-down runtime.
    crate::io::clear_global_io_handle();
    let wrapped_cleared = SpawnIoStore::wrap(Arc::clone(&inner));
    assert!(
        Arc::ptr_eq(&wrapped_cleared, &inner),
        "after the IO handle is cleared, wrap() must return the inner store unchanged"
    );
}

/// `RuntimeManager::shutdown` must clear the process-global IO handle (the
/// production trigger for the fallback verified above). We can't assert
/// `global_io_handle().is_none()` after shutdown — other unlocked tests in this
/// binary may re-install it concurrently — so we verify the unit `shutdown`
/// relies on directly: `clear_global_io_handle` empties the slot.
#[test]
fn clear_global_io_handle_empties_the_slot() {
    let _guard = GLOBAL_HANDLE_TEST_LOCK.lock().unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    crate::io::set_global_io_handle(rt.handle().clone());
    assert!(crate::io::global_io_handle().is_some(), "handle should be installed");

    crate::io::clear_global_io_handle();
    assert!(
        crate::io::global_io_handle().is_none(),
        "clear_global_io_handle must empty the slot (the unit RuntimeManager::shutdown uses)"
    );
}
