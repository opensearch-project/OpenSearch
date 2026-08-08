/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared helpers for driving the shard-scoped `ObjectStore` from the otherwise-synchronous
//! Parquet write/merge paths.
//!
//! The Parquet writer, the sorting chunked writer, and the k-way merge are all synchronous and
//! serialized per-writer by Java (or run on rayon/finalize threads). None of those threads are
//! Tokio runtime workers, so it is safe to drive the async `ObjectStore` operations to completion
//! with `block_on` on the dedicated runtime here. `object_store` I/O (multipart PUT parts, ranged
//! GETs) runs on this runtime's worker threads while the caller blocks.

use bytes::Bytes;
use object_store::buffered::BufWriter as ObjectBufWriter;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::{Arc, OnceLock};
use tokio_util::io::SyncIoBridge;

static OS_STORE_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// Dedicated multi-thread runtime for all shard `ObjectStore` I/O on the write path
/// (streaming Parquet/IPC uploads, chunk reads during the sort-finalize merge, deletes/renames).
pub fn os_store_runtime() -> &'static tokio::runtime::Runtime {
    OS_STORE_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("parquet-os-store")
            .enable_all()
            .build()
            .expect("Failed to build parquet ObjectStore runtime")
    })
}

/// A synchronous [`std::io::Write`] that streams into an `ObjectStore` multipart upload.
///
/// Backed by `object_store::buffered::BufWriter` (which chunks writes into multipart parts) bridged
/// to sync I/O via [`SyncIoBridge`]. The upload is **only finalized** when [`SyncIoBridge::shutdown`]
/// is called — callers MUST call `shutdown()` after finishing their writes (drop alone will NOT
/// complete the multipart upload).
pub type StoreSyncWriter = SyncIoBridge<ObjectBufWriter>;

/// Build a sync streaming writer targeting `path` in `store`.
pub fn store_sync_writer(store: Arc<dyn ObjectStore>, path: ObjectPath) -> StoreSyncWriter {
    let buf = ObjectBufWriter::new(store, path);
    SyncIoBridge::new_with_handle(buf, os_store_runtime().handle().clone())
}

/// Fully read an object into memory (used to read back an IPC staging object for sort/merge, which
/// the local path also loads fully into memory at flush time — so this adds no extra peak).
pub fn read_object(store: &Arc<dyn ObjectStore>, path: &ObjectPath) -> object_store::Result<Bytes> {
    os_store_runtime().block_on(async move { store.get(path).await?.bytes().await })
}

/// Delete an object (best-effort semantics are the caller's; this surfaces the error).
pub fn delete_object(store: &Arc<dyn ObjectStore>, path: &ObjectPath) -> object_store::Result<()> {
    os_store_runtime().block_on(async move { store.delete(path).await })
}

/// Rename `from` to `to` within the store (metadata-only for `LocalFileSystem`).
pub fn rename_object(
    store: &Arc<dyn ObjectStore>,
    from: &ObjectPath,
    to: &ObjectPath,
) -> object_store::Result<()> {
    os_store_runtime().block_on(async move { store.rename(from, to).await })
}
