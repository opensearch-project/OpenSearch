/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Disk page cache backed by [Foyer](https://github.com/foyer-rs/foyer).
//!
//! Caches arbitrary byte ranges keyed by `(path, start, end)`. Intended for
//! warm reads of large columnar or segment files where repeated range reads
//! to the same byte region are common.
//!
//! ## Design
//!
//! - **Storage**: disk-only HybridCache (Foyer). No in-process heap used for
//!   cached data — the disk store acts as a fast local NVMe cache tier.
//! - **Key index**: a `DashMap<file_path → [cache_keys]>` allows efficient
//!   per-file eviction without Foyer prefix-scan support.
//! - **Tokio runtime**: kept alive as an `Arc` field because Foyer's background
//!   I/O tasks run on it. Dropping the runtime before the cache causes a panic.
//!
//! ## Integration
//!
//! The crate is compiled as an `rlib` into the parent shared library. Callers
//! in the same binary use the public Rust API ([`FoyerCache::get`],
//! [`FoyerCache::put`], [`FoyerCache::evict_file`]) directly.
//!
//! Two `extern "C"` symbols (`foyer_create_cache`, `foyer_destroy_cache`) are
//! exported for lifecycle management from Java via the Foreign Function &
//! Memory (FFM) API. All other operations stay in Rust.

use std::path::PathBuf;
use std::sync::Arc;
use bytes::Bytes;
use dashmap::DashMap;
use foyer::{HybridCache, HybridCacheBuilder, DirectFsDeviceOptionsBuilder, LruConfig};
use serde::{Deserialize, Serialize};

// ── Value wrapper ─────────────────────────────────────────────────────────────

/// Newtype wrapper around a byte slice that satisfies Foyer's `StorageValue`
/// bound (`serde::Serialize + Deserialize`). Not part of the public API.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CachedBytes(#[serde(with = "serde_bytes")] Vec<u8>);
impl CachedBytes {
    fn from_slice(b: &[u8]) -> Self { Self(b.to_vec()) }
    fn as_slice(&self) -> &[u8] { &self.0 }
}

// ── FoyerCache ────────────────────────────────────────────────────────────────

/// Disk page cache with per-file eviction support.
///
/// Wraps a Foyer [`HybridCache`] configured as a disk-only store, together with
/// a concurrent key index that maps each file path to its cached entry keys.
/// The key index allows removing all cached ranges for a specific file in O(n)
/// without requiring Foyer to support prefix-scan semantics.
///
/// Thread-safe: both [`HybridCache`] and [`DashMap`] are `Send + Sync`.
pub struct FoyerCache {
    inner: HybridCache<String, CachedBytes>,
    /// Maps each file path to the list of Foyer keys stored for that file.
    key_index: DashMap<String, Vec<String>>,
    /// Keeps the Tokio runtime alive for the lifetime of the cache.
    /// Foyer spawns background I/O tasks on this runtime during initialisation.
    _runtime: Arc<tokio::runtime::Runtime>,
}

impl FoyerCache {
    /// Initialise the cache synchronously.
    ///
    /// Builds a Foyer `HybridCache` configured with a disk-only store of
    /// `disk_bytes` capacity stored in `disk_dir`. Blocks the calling thread
    /// until Foyer has completed asynchronous setup.
    ///
    /// # Panics
    /// Panics if the Tokio runtime cannot be created or if Foyer fails to
    /// build the cache (e.g. insufficient disk space or invalid path).
    pub fn new(disk_bytes: usize, disk_dir: impl Into<PathBuf>) -> Self {
        let disk_dir = disk_dir.into();
        let rt = tokio::runtime::Runtime::new()
            .expect("[foyer-page-cache] failed to create Tokio runtime");
        let dir_clone = disk_dir.clone();
        let inner = rt.block_on(async move {
            HybridCacheBuilder::new()
                .with_name("foyer-page-cache")
                .memory(1) // disk-only; Foyer requires memory >= 1
                .with_eviction_config(LruConfig { high_priority_pool_ratio: 0.0 })
                .storage()
                .with_device_config(
                    DirectFsDeviceOptionsBuilder::new(dir_clone)
                        .with_capacity(disk_bytes)
                        .build()
                )
                .build()
                .await
                .expect("[foyer-page-cache] HybridCache build failed")
        });
        log::info!("[foyer-page-cache] ready: disk={}B, dir={}", disk_bytes, disk_dir.display());
        Self { inner, key_index: DashMap::new(), _runtime: Arc::new(rt) }
    }

    /// Construct the string cache key for a byte range.
    fn make_key(path: &str, start: u64, end: u64) -> String {
        format!("{}:{}-{}", path, start, end)
    }

    /// Look up a cached byte range by file path and byte offsets.
    ///
    /// Returns `Some(Bytes)` on a cache hit and `None` on a miss.
    /// Blocks the calling thread while Foyer performs the disk lookup.
    pub fn get(&self, path: &str, start: u64, end: u64) -> Option<Bytes> {
        let k = Self::make_key(path, start, end);
        self._runtime.block_on(async {
            match self.inner.get(&k).await {
                Ok(Some(e)) => Some(Bytes::copy_from_slice(e.value().as_slice())),
                _           => None,
            }
        })
    }

    /// Insert a byte range into the cache.
    ///
    /// Foyer writes the entry to disk asynchronously in the background.
    /// The entry is also recorded in the key index to support per-file eviction.
    pub fn put(&self, path: &str, start: u64, end: u64, data: Bytes) {
        let k = Self::make_key(path, start, end);
        self.inner.insert(k.clone(), CachedBytes::from_slice(&data));
        self.key_index.entry(path.to_string()).or_default().push(k);
    }

    /// Remove all cached ranges for a given file path.
    ///
    /// Uses the key index to locate and remove each entry individually.
    /// A no-op if no entries exist for the path.
    pub fn evict_file(&self, path: &str) {
        if let Some((_, keys)) = self.key_index.remove(path) {
            for k in keys { self.inner.remove(&k); }
        }
    }

    /// Remove all entries from the cache.
    ///
    /// Clears the key index and blocks until Foyer has completed clearing
    /// the disk store.
    pub fn clear(&self) {
        self.key_index.clear();
        self._runtime.block_on(async { let _ = self.inner.clear().await; });
    }
}

// ── Lifecycle extern "C" API ──────────────────────────────────────────────────
//
// These two symbols are the only entry points exported to Java.
// All cache access operations are called directly from Rust.

/// Allocate a [`FoyerCache`] and return an opaque handle.
///
/// The returned value is a `Box<FoyerCache>` cast to `i64`. It must be treated
/// as an opaque token by the caller — do not dereference or free it directly.
/// Pass it back to [`foyer_destroy_cache`] when the cache is no longer needed.
///
/// Returns `-1` if `dir_ptr` is not valid UTF-8; returns a positive pointer
/// on success.
///
/// # Safety
/// `dir_ptr` must point to `dir_len` consecutive valid UTF-8 bytes.
#[no_mangle]
pub unsafe extern "C" fn foyer_create_cache(
    disk_bytes: u64,
    dir_ptr: *const u8,
    dir_len: u64,
) -> i64 {
    match std::str::from_utf8(std::slice::from_raw_parts(dir_ptr, dir_len as usize)) {
        Ok(dir) => Box::into_raw(Box::new(FoyerCache::new(disk_bytes as usize, dir))) as i64,
        Err(_)  => -1,
    }
}

/// Free a [`FoyerCache`] previously created by [`foyer_create_cache`].
///
/// After this call the handle is invalid and must not be used again.
///
/// # Safety
/// `ptr` must be a value returned by [`foyer_create_cache`] that has not
/// already been passed to this function.
#[no_mangle]
pub unsafe extern "C" fn foyer_destroy_cache(ptr: i64) {
    if ptr > 0 {
        drop(Box::from_raw(ptr as *mut FoyerCache));
    }
}
