/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! End-to-end verification that object-store data reads execute on the
//! dedicated IO runtime, not the CPU runtime.
//!
//! This drives a full indexed scan through the real path
//! (`IndexedTableProvider` → `execute_stream` → parquet `AsyncFileReader`)
//! exactly as production does: the DataFusion stream is driven on the
//! `RuntimeManager`'s CPU `DedicatedExecutor` via [`CrossRtStream`], and results
//! are consumed on the IO runtime. The scan's object-store reads must be
//! dispatched onto the IO runtime by the [`SpawnIoStore`](crate::spawn_io_store)
//! wrapper around the registered store.
//!
//! Two independent signals confirm this, both of which would fail if the store
//! were registered without the `SpawnIoStore` wrapper:
//!
//! 1. **Thread attribution** — a latency-injecting store records the name of the
//!    thread each `get_opts` (and therefore `get_range`/`get_ranges`) runs on;
//!    every read must land on a `datafusion-io` worker, never `datafusion-cpu`.
//! 2. **Node-stats counter** — `pack_runtime_metrics(io).spawned_tasks_count`
//!    (the *same* function `df_stats` exports to OpenSearch node stats) must
//!    increase across the query, since SpawnIoStore spawns IO-runtime tasks.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use datafusion::prelude::SessionContext;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjStorePath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult,
};

use crate::cross_rt_stream::CrossRtStream;
use crate::runtime_manager::RuntimeManager;
use crate::spawn_io_store::SpawnIoStore;
use crate::stats::pack_runtime_metrics;

use super::super::table_provider::SegmentFileInfo;
use super::{build_fixture_segment_and_factory, build_indexed_provider, index_leaf};

/// Wraps a real `LocalFileSystem` and records, for every `get_opts` call (the
/// method `get_range`/`get_ranges` funnel through), the name of the thread the
/// fetch ran on. A small async sleep makes the read genuine in-runtime async
/// work — unlike `LocalFileSystem`'s blocking read, which would be offloaded to
/// tokio's blocking pool and thus invisible to runtime worker attribution.
#[derive(Debug)]
struct LatencyRecordingStore {
    inner: Arc<LocalFileSystem>,
    get_threads: Arc<Mutex<Vec<Option<String>>>>,
}

impl std::fmt::Display for LatencyRecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LatencyRecordingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for LatencyRecordingStore {
    async fn put_opts(
        &self,
        location: &ObjStorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjStorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // Record the runtime worker thread this fetch is polled on.
        self.get_threads
            .lock()
            .unwrap()
            .push(std::thread::current().name().map(|s| s.to_owned()));
        // Real awaitable work so the read shows up as in-runtime activity
        // (not a blocking-pool offload).
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.inner.get_opts(location, options).await
    }

    fn list(
        &self,
        prefix: Option<&ObjStorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjStorePath>>,
    ) -> BoxStream<'static, object_store::Result<ObjStorePath>> {
        self.inner.delete_stream(locations)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjStorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjStorePath,
        to: &ObjStorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[test]
fn data_reads_execute_on_io_runtime_not_cpu_runtime() {
    let mgr = RuntimeManager::new(2, 1.5, 1.5);

    // Build the standard e2e fixture (single segment, one collector leaf) but
    // substitute the latency-recording store for the object store.
    let tree = index_leaf(0); // brand == "amazon"
    let (schema, segment, factory, _tmp): (_, SegmentFileInfo, _, _) =
        build_fixture_segment_and_factory(tree);

    let get_threads = Arc::new(Mutex::new(Vec::new()));
    let recording: Arc<dyn ObjectStore> = Arc::new(LatencyRecordingStore {
        inner: Arc::new(LocalFileSystem::new()),
        get_threads: Arc::clone(&get_threads),
    });
    // Wrap exactly as production does at register_object_store, but bind to THIS
    // manager's IO handle explicitly. (`SpawnIoStore::wrap` uses the process-global
    // handle set by the first RuntimeManager; with multiple managers in one test
    // binary that global may point at a sibling test's runtime, so we pass the
    // handle directly to keep dispatch and the sampled metric on the same runtime.)
    let store: Arc<dyn ObjectStore> = Arc::new(SpawnIoStore::new(
        recording,
        mgr.io_runtime.handle().clone(),
    ));
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();

    let provider = build_indexed_provider(schema, segment, factory, store, store_url);

    // Snapshot the IO runtime's spawned-task counter before the query. This is
    // the exact metric exported to OpenSearch node stats via `df_stats`.
    let io_spawned_before =
        pack_runtime_metrics(&mgr.io_monitor, mgr.io_runtime.handle()).spawned_tasks_count;

    // Plan and execute exactly as production does: build the DataFusion stream,
    // then drive it on the CPU DedicatedExecutor via CrossRtStream while
    // consuming results on the IO runtime.
    let cpu_executor = mgr.cpu_executor();
    let row_count = Arc::new(AtomicUsize::new(0));
    let row_count_c = Arc::clone(&row_count);

    mgr.io_runtime.block_on(async move {
        let ctx = SessionContext::new();
        ctx.register_table("t", provider).unwrap();
        let df = ctx
            .sql("SELECT brand, price, status, category FROM t")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let df_stream =
            datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();

        // CrossRtStream drives df_stream (incl. all get_byte_ranges) on the CPU
        // executor; we consume batches here on the IO runtime.
        let mut cross = CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
        while let Some(batch) = cross.next().await {
            row_count_c.fetch_add(batch.unwrap().num_rows(), Ordering::Relaxed);
        }
    });

    // The scan must have produced rows (amazon rows in the fixture).
    assert!(
        row_count.load(Ordering::Relaxed) > 0,
        "scan produced no rows — fixture/query wiring broken, test would be vacuous"
    );

    // ── Signal 1: thread attribution ─────────────────────────────────────────
    let threads = get_threads.lock().unwrap();
    assert!(
        !threads.is_empty(),
        "object store was never read — cannot attribute IO runtime"
    );
    for t in threads.iter() {
        let on_io = t.as_deref().map(|n| n.starts_with("datafusion-io")) == Some(true);
        assert!(
            on_io,
            "data read ran on thread {:?}, expected a `datafusion-io` worker \
             (spawn_io dispatch missing or routed to the wrong runtime)",
            t
        );
    }
    let read_count = threads.len();
    drop(threads);

    // ── Signal 2: node-stats spawned-task counter ────────────────────────────
    // This is the exact counter exported to OpenSearch node stats via `df_stats`.
    // SpawnIoStore spawns at least one IO-runtime task per dispatched read, so the
    // counter must advance across the scan. (We assert a strict increase rather
    // than an exact delta because get_ranges coalescing means one spawned task can
    // service several `get_opts` records, so the spawn count need not equal
    // read_count.)
    let io_spawned_after =
        pack_runtime_metrics(&mgr.io_monitor, mgr.io_runtime.handle()).spawned_tasks_count;
    assert!(
        io_spawned_after > io_spawned_before,
        "io_runtime.spawned_tasks_count did not advance across the scan \
         (before={}, after={}) despite {} object-store reads — IO is not being \
         dispatched to the IO runtime",
        io_spawned_before,
        io_spawned_after,
        read_count
    );

    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}
