/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`FoyerCache`] — a [`PageCache`] implementation backed by Foyer.

use std::path::PathBuf;
use std::sync::Arc;
use bytes::Bytes;
use dashmap::DashMap;
use foyer::{BlockEngineConfig, DeviceBuilder, Event, EventListener, FsDeviceBuilder,
            HybridCache, HybridCacheBuilder, PsyncIoEngineConfig};

use crate::traits::PageCache;

// ── Key index eviction listener ───────────────────────────────────────────────

/// Foyer event listener that removes evicted keys from the key index.
///
/// Shared between [`FoyerCache`] and Foyer via `Arc`. When Foyer evicts,
/// replaces, or removes an entry, `on_leave` is called, which removes the key
/// from the path-to-keys index. This prevents `key_index` from growing
/// unbounded as Foyer's LRU evicts entries from disk.
struct KeyIndexListener {
    key_index: Arc<DashMap<String, Vec<String>>>,
}

impl EventListener for KeyIndexListener {
    type Key   = String;
    type Value = Vec<u8>;

    fn on_leave(&self, reason: Event, key: &String, _value: &Vec<u8>) {
        match reason {
            Event::Evict | Event::Replace | Event::Remove => {
                if let Some(colon_pos) = key.rfind(':') {
                    let path = &key[..colon_pos];
                    if let Some(mut keys) = self.key_index.get_mut(path) {
                        keys.retain(|k| k != key);
                        if keys.is_empty() {
                            drop(keys);
                            self.key_index.remove(path);
                        }
                    }
                }
            }
            Event::Clear => {}
        }
    }
}

// ── FoyerCache ────────────────────────────────────────────────────────────────

/// Disk page cache with per-file eviction support backed by Foyer.
///
/// Wraps a Foyer [`HybridCache`] configured as a disk-only store, together
/// with a concurrent key index that maps each file path to its cached entry
/// keys. The key index allows removing all cached ranges for a specific file
/// in O(n) without requiring Foyer to support prefix-scan semantics.
///
/// The key index is kept in sync with Foyer's internal state via an
/// [`EventListener`] — stale keys are removed automatically when Foyer evicts
/// entries via LRU.
///
/// Thread-safe: both [`HybridCache`] and [`DashMap`] are `Send + Sync`.
pub struct FoyerCache {
    inner: HybridCache<String, Vec<u8>>,
    /// Maps each file path to the list of Foyer keys stored for that file.
    /// Shared with [`KeyIndexListener`] for automatic stale-key removal.
    pub(crate) key_index: Arc<DashMap<String, Vec<String>>>,
    /// Keeps the Tokio runtime alive for the lifetime of the cache.
    _runtime: Arc<tokio::runtime::Runtime>,
}

impl FoyerCache {
    /// Initialise the cache synchronously.
    ///
    /// Blocks the calling thread until Foyer has completed asynchronous setup.
    ///
    /// # Panics
    /// Panics if the Tokio runtime cannot be created or if Foyer fails to
    /// build the cache (e.g. insufficient disk space or invalid path).
    pub fn new(disk_bytes: usize, disk_dir: impl Into<PathBuf>) -> Self {
        let disk_dir = disk_dir.into();
        let key_index: Arc<DashMap<String, Vec<String>>> = Arc::new(DashMap::new());
        let listener = Arc::new(KeyIndexListener { key_index: Arc::clone(&key_index) });

        let rt = tokio::runtime::Runtime::new()
            .expect("[page-cache] failed to create Tokio runtime");
        let dir_clone = disk_dir.clone();
        let inner = rt.block_on(async move {
            HybridCacheBuilder::<String, Vec<u8>>::new()
                .with_name("page-cache")
                .with_event_listener(listener)
                .memory(1)
                .storage()
                .with_io_engine_config(PsyncIoEngineConfig::new())
                .with_engine_config(BlockEngineConfig::new(
                    FsDeviceBuilder::new(dir_clone)
                        .with_capacity(disk_bytes)
                        .build()
                        .expect("[page-cache] FsDevice build failed")
                ))
                .build()
                .await
                .expect("[page-cache] HybridCache build failed")
        });
        log::info!("[page-cache] ready: disk={}B, dir={}", disk_bytes, disk_dir.display());
        Self { inner, key_index, _runtime: Arc::new(rt) }
    }

    fn make_key(path: &str, start: u64, end: u64) -> String {
        format!("{}:{}-{}", path, start, end)
    }
}

impl PageCache for FoyerCache {
    async fn get(&self, path: &str, start: u64, end: u64) -> Option<Bytes> {
        let k = Self::make_key(path, start, end);
        match self.inner.get(&k).await {
            Ok(Some(e)) => Some(Bytes::copy_from_slice(e.value())),
            _           => None,
        }
    }

    fn put(&self, path: &str, start: u64, end: u64, data: Bytes) {
        let k = Self::make_key(path, start, end);
        self.inner.insert(k.clone(), data.to_vec());
        self.key_index.entry(path.to_string()).or_default().push(k);
    }

    fn evict_file(&self, path: &str) {
        if let Some((_, keys)) = self.key_index.remove(path) {
            for k in keys { self.inner.remove(&k); }
        }
    }

    async fn clear(&self) {
        self.key_index.clear();
        let _ = self.inner.clear().await;
    }
}
