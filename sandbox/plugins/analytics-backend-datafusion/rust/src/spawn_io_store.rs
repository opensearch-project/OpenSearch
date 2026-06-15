/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`SpawnIoStore`] — an [`ObjectStore`] decorator that dispatches every read
//! onto the dedicated IO runtime.
//!
//! # Why
//!
//! DataFusion drives query execution (including the parquet `AsyncFileReader`'s
//! object-store fetches) on the CPU `DedicatedExecutor`. Without intervention,
//! those `store.get_*().await` calls are polled inline on CPU worker threads, so
//! network/disk IO — and its completion work (TLS decrypt, body assembly,
//! decompression) — competes with decode for CPU threads. The dedicated IO
//! runtime sits idle.
//!
//! This is the same problem, and the same fix, as DataFusion's own
//! `SpawnedReqwestConnector` example: move the IO onto a separate runtime by
//! wrapping the store, not by editing each reader. Wrapping the *store* (rather
//! than individual call sites like `CachedMetadataReader`) means EVERY reader —
//! the indexed reader, DataFusion's stock `ParquetObjectReader` used by the
//! `ListingTable` path, statistics/metadata probes — gets IO-runtime dispatch
//! for free, because they all go through the registered `ObjectStore`.
//!
//! # How
//!
//! The wrapper captures the IO runtime [`Handle`] by value, so it works even on
//! the bare Java/FFM threads that register stores (which are not tokio runtime
//! workers). Every read method spawns its work onto that handle via the local
//! `spawn_read` helper and awaits the join handle — so the CPU thread only parks
//! on a cheap handle while the actual fetch runs on an IO worker. A cancelled
//! read (a normal part of DataFusion execution) is mapped to an
//! `object_store::Error`, never a panic.
//!
//! `get_opts` is special: its [`GetResult`] payload may be a lazy `Stream`, so
//! the bytes are collected *inside* the spawned task and returned as an
//! in-memory payload. Otherwise the stream would be drained on the caller's
//! runtime, defeating the purpose. `get_range`/`get_ranges` already return
//! materialized `Bytes`, so they are spawned directly.
//!
//! Writes, listing, and deletes delegate to the inner store unchanged — they are
//! not on the latency-bound scan read path (and some run during planning).

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    Error as OsError, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as OsResult,
};
use prost::bytes::Bytes;
use tokio::runtime::Handle;

/// Spawn a fallible object-store read `fut` onto `handle` (the IO runtime) and
/// await its result, aborting the spawned task if this future is dropped.
///
/// A cancelled spawned task is mapped to an `object_store::Error` rather than a
/// panic. This matters because the store is
/// driven by the full DataFusion operator tree, which cancels in-flight reads as
/// a normal part of execution (early completion, LIMIT, a join side finishing);
/// turning those routine cancellations into panics would crash the query.
async fn spawn_read<T>(
    handle: &Handle,
    fut: impl std::future::Future<Output = OsResult<T>> + Send + 'static,
) -> OsResult<T>
where
    T: Send + 'static,
{
    /// Aborts the spawned task if the awaiting future is dropped, so a cancelled
    /// read does not leak work on the IO runtime.
    struct AbortOnDrop<R>(tokio::task::JoinHandle<R>);
    impl<R> Drop for AbortOnDrop<R> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    let mut guard = AbortOnDrop(handle.spawn(fut));
    match (&mut guard.0).await {
        Ok(result) => result,
        Err(join_err) if join_err.is_cancelled() => Err(OsError::Generic {
            store: "SpawnIoStore",
            source: "object-store read was cancelled".into(),
        }),
        Err(join_err) => std::panic::resume_unwind(join_err.into_panic()),
    }
}

/// Wraps an inner [`ObjectStore`] so that every read executes on the IO runtime
/// identified by the captured [`Handle`].
#[derive(Debug)]
pub struct SpawnIoStore {
    inner: Arc<dyn ObjectStore>,
    io_handle: Handle,
}

impl SpawnIoStore {
    /// Wrap `inner`, dispatching reads onto `io_handle`.
    pub fn new(inner: Arc<dyn ObjectStore>, io_handle: Handle) -> Self {
        Self { inner, io_handle }
    }

    /// Wrap `inner` using the process-global IO handle. If no IO runtime has
    /// been installed (e.g. a unit test that never built a `RuntimeManager`),
    /// returns `inner` unwrapped so behaviour is unchanged.
    pub fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        match crate::io::global_io_handle() {
            Some(h) => Arc::new(SpawnIoStore::new(inner, h)),
            None => inner,
        }
    }
}

impl fmt::Display for SpawnIoStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpawnIoStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SpawnIoStore {
    // ── Reads: dispatched to the IO runtime ──────────────────────────────────

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let inner = Arc::clone(&self.inner);
        let location = location.clone();
        spawn_read(&self.io_handle, async move {
            let result = inner.get_opts(&location, options).await?;
            // Collect the (possibly lazy `Stream`) payload here, on the IO
            // runtime, so no byte fetching leaks back onto the caller's runtime.
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let bytes = result.bytes().await?;
            Ok(GetResult {
                payload: GetResultPayload::Stream(
                    futures::stream::once(async move { Ok(bytes) }).boxed(),
                ),
                meta,
                range,
                attributes,
            })
        })
        .await
    }

    // NOTE: `get_range` is NOT a trait method — it is a provided method on
    // `ObjectStoreExt` whose default implementation calls `get_opts`. Since we
    // override `get_opts` (above) to dispatch onto the IO runtime, `get_range`
    // automatically routes through it; there is nothing to override here.

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        let inner = Arc::clone(&self.inner);
        let location = location.clone();
        let ranges = ranges.to_vec();
        spawn_read(&self.io_handle, async move {
            inner.get_ranges(&location, &ranges).await
        })
        .await
    }

    // ── Everything else: delegate unchanged ──────────────────────────────────

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
