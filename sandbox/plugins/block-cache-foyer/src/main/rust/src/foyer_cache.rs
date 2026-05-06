/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`FoyerCache`] — a [`BlockCache`] implementation backed by Foyer.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use bytes::Bytes;
use dashmap::DashMap;
use foyer::{BlockEngineConfig, DeviceBuilder, Event, EventListener, FsDeviceBuilder,
            HybridCache, HybridCacheBuilder, IoEngineConfig, PsyncIoEngineConfig};
#[cfg(target_os = "linux")]
use foyer::UringIoEngineConfig;

use crate::range_cache::{CacheKey, SEPARATOR};
use crate::stats::FoyerStatsCounter;
use crate::traits::BlockCache;

// ── I/O engine selection ──────────────────────────────────────────────────────

/// Return `true` if the running Linux kernel is >= `(major, minor)`.
///
/// Reads `/proc/sys/kernel/osrelease` (e.g. `"5.15.0-91-generic"`) and
/// compares the major/minor version numbers. Returns `false` on any parse
/// error so the caller can fall back safely.
#[cfg(target_os = "linux")]
fn kernel_version_at_least(required_major: u32, required_minor: u32) -> bool {
    let release = match std::fs::read_to_string("/proc/sys/kernel/osrelease") {
        Ok(s) => s,
        Err(_) => return false,
    };
    let mut parts = release.trim().split('.');
    let major: u32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor: u32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    major > required_major || (major == required_major && minor >= required_minor)
}

/// Select the I/O engine based on the operator-configured `choice`.
///
/// | `choice`   | Behaviour |
/// |------------|-----------|
/// | `"auto"`   | Detect at runtime: io_uring on Linux ≥ 5.1, psync otherwise (default). |
/// | `"io_uring"` | Force io_uring. Fails at node startup if io_uring is unavailable (e.g. blocked by seccomp/AppArmor in locked-down container environments). |
/// | `"psync"`  | Force synchronous pread/pwrite. Use when io_uring is restricted or when predictable syscall-level profiling is needed. |
///
/// Invalid values are treated as `"auto"` with a warning.
fn build_io_engine_config(choice: &str) -> Box<dyn IoEngineConfig> {
    match choice {
        "io_uring" => {
            log::info!("[block-cache] io_engine=io_uring forced by config");
            #[cfg(target_os = "linux")]
            return UringIoEngineConfig::new().boxed();
            #[cfg(not(target_os = "linux"))]
            panic!("[block-cache] io_engine=io_uring requested but io_uring is not supported on non-Linux platforms");
        }
        "psync" => {
            log::info!("[block-cache] io_engine=psync forced by config");
            return PsyncIoEngineConfig::new().boxed();
        }
        other => {
            if other != "auto" {
                log::warn!("[block-cache] unknown io_engine='{}'; falling back to auto-detect", other);
            }
            // "auto" — detect by kernel version (existing logic)
            #[cfg(target_os = "linux")]
            {
                let release = std::fs::read_to_string("/proc/sys/kernel/osrelease")
                    .unwrap_or_else(|_| "unknown".to_string());
                let release = release.trim();
                if kernel_version_at_least(5, 1) {
                    log::info!(
                        "[block-cache] kernel {} — io_uring available, using UringIoEngineConfig",
                        release
                    );
                    return UringIoEngineConfig::new().boxed();
                } else {
                    log::warn!(
                        "[block-cache] kernel {} — io_uring unavailable (requires >= 5.1), \
                         falling back to PsyncIoEngineConfig",
                        release
                    );
                }
            }
            PsyncIoEngineConfig::new().boxed()
        }
    }
}

// ── Key index eviction listener ───────────────────────────────────────────────

/// Foyer event listener that removes evicted keys from the key index
/// and updates the shared [`FoyerStatsCounter`] counters.
///
/// Shared between [`FoyerCache`] and Foyer via `Arc`. When Foyer evicts,
/// replaces, or removes an entry, `on_leave` is called, which:
/// 1. Removes the key from the prefix-to-keys index (prevents unbounded growth).
/// 2. Updates the appropriate stats counters.
///
/// # Key index prefix extraction
///
/// The index key is derived by splitting each cache key on [`SEPARATOR`].
/// Keys that contain `SEPARATOR` (range entries) use everything before it as
/// the index key.
struct KeyIndexListener {
    key_index: Arc<DashMap<String, Vec<String>>>,
    /// Shared stats counters — updated here for eviction/remove/clear events.
    stats: Arc<FoyerStatsCounter>,
}

impl EventListener for KeyIndexListener {
    type Key   = String;
    type Value = Vec<u8>;

    fn on_leave(&self, reason: Event, key: &String, value: &Vec<u8>) {
        let size = value.len() as i64;

        match reason {
            Event::Evict => {
                // Remove from key index
                let index_key = if let Some(sep_pos) = key.find(SEPARATOR) {
                    &key[..sep_pos]
                } else {
                    key.as_str()
                };
                if let Some(mut keys) = self.key_index.get_mut(index_key) {
                    keys.retain(|k| k != key);
                    if keys.is_empty() {
                        drop(keys);
                        self.key_index.remove(index_key);
                    }
                }
                // Update eviction stats
                self.stats.eviction_count.fetch_add(1, Ordering::Relaxed);
                self.stats.eviction_bytes.fetch_add(size, Ordering::Relaxed);
                self.stats.used_bytes.fetch_add(-size, Ordering::Relaxed);
            }
            Event::Replace | Event::Remove => {
                // Remove from key index
                let index_key = if let Some(sep_pos) = key.find(SEPARATOR) {
                    &key[..sep_pos]
                } else {
                    key.as_str()
                };
                if let Some(mut keys) = self.key_index.get_mut(index_key) {
                    keys.retain(|k| k != key);
                    if keys.is_empty() {
                        drop(keys);
                        self.key_index.remove(index_key);
                    }
                }
                // Remove: entry is being explicitly deleted — subtract from used_bytes.
                // Replace: old entry leaves, new entry will be added via put() with new size.
                self.stats.used_bytes.fetch_add(-size, Ordering::Relaxed);
            }
            Event::Clear => {
                // key_index and used_bytes are reset in FoyerCache::clear()
            }
        }
    }
}

// ── FoyerCache ────────────────────────────────────────────────────────────────

/// Disk block cache with prefix-based eviction support backed by Foyer.
///
/// Wraps a Foyer [`HybridCache`] configured as a disk-only store, together
/// with a concurrent key index that maps each index prefix to its cached entry
/// keys, and a set of [`FoyerStatsCounter`] atomic counters.
///
/// The key index allows removing all cached entries sharing a common prefix
/// in O(n) without requiring Foyer to support prefix-scan semantics.
///
/// Stats are updated on every hot-path operation:
/// - `get()` → hit_count / miss_count
/// - `put()` → used_bytes
/// - `KeyIndexListener::on_leave()` → eviction_count, eviction_bytes, used_bytes
///
/// Stats are exposed via [`FoyerCache::stats`] and read by the
/// `foyer_snapshot_stats` FFM function at most once per `_nodes/stats` request.
///
/// Thread-safe: both [`HybridCache`] and [`DashMap`] are `Send + Sync`;
/// all stats fields are [`AtomicI64`].
pub struct FoyerCache {
    inner: HybridCache<String, Vec<u8>>,
    /// Maps each index prefix to the list of Foyer keys stored under that prefix.
    /// Shared with [`KeyIndexListener`] for automatic stale-key removal.
    pub(crate) key_index: Arc<DashMap<String, Vec<String>>>,
    /// Keeps the Tokio runtime alive for the lifetime of the cache.
    _runtime: Arc<tokio::runtime::Runtime>,
    /// Atomic stats counters. Shared with [`KeyIndexListener`].
    /// Exposed for FFM read via `foyer_snapshot_stats`.
    pub(crate) stats: Arc<FoyerStatsCounter>,
}

impl FoyerCache {
    /// Initialise the cache synchronously.
    ///
    /// # Parameters
    /// - `disk_bytes` — total disk capacity for this cache.
    /// - `disk_dir` — directory on the local SSD where Foyer stores its data files.
    /// - `block_size_bytes` — Foyer disk block size. Must be ≥ the largest entry ever
    ///   put into the cache. Configurable via `format_cache.block_size`.
    /// - `io_engine` — I/O engine selection: `"auto"`, `"io_uring"`, or `"psync"`.
    ///   Configurable via `format_cache.io_engine`.
    ///
    /// # Panics
    /// Panics if the Tokio runtime cannot be created or if Foyer fails to
    /// build the cache (e.g. insufficient disk space or invalid path).
    pub fn new(
        disk_bytes: usize,
        disk_dir: impl Into<PathBuf>,
        block_size_bytes: usize,
        io_engine: &str,
    ) -> Self {
        let disk_dir = disk_dir.into();
        let key_index: Arc<DashMap<String, Vec<String>>> = Arc::new(DashMap::new());
        let stats = FoyerStatsCounter::new();
        let listener = Arc::new(KeyIndexListener {
            key_index: Arc::clone(&key_index),
            stats: Arc::clone(&stats),
        });

        let rt = tokio::runtime::Runtime::new()
            .expect("[block-cache] failed to create Tokio runtime");
        let dir_clone = disk_dir.clone();
        let io_engine = io_engine.to_string();
        let io_engine_for_log = io_engine.clone();  // clone for use in log after the closure
        let inner = rt.block_on(async move {
            HybridCacheBuilder::<String, Vec<u8>>::new()
                .with_name("block-cache")
                .with_event_listener(listener)
                .memory(1)
                    // Disable the in-memory tier — this cache is disk-only.
                    // Foyer is a hybrid (DRAM + disk) cache; setting the memory capacity
                    // to 1 byte opts out of DRAM caching. All entries go directly to the
                    // disk tier (FsDevice) below.
                .storage()
                .with_io_engine_config(build_io_engine_config(&io_engine))
                .with_engine_config(
                    // block_size must be >= the largest entry ever put into the cache.
                    // DataFusion reads Parquet row groups of up to 64 MB; Lucene blocks are
                    // also 64 MB. A block_size smaller than the entry causes a silent drop
                    // (put succeeds but entry is not stored, resulting in a cache miss).
                    // Configurable via format_cache.block_size (default: 64 MB).
                    BlockEngineConfig::new(
                        FsDeviceBuilder::new(dir_clone)
                            .with_capacity(disk_bytes)
                            .build()
                            .expect("[block-cache] FsDevice build failed")
                    )
                    .with_block_size(block_size_bytes)
                )
                .build()
                .await
                .expect("[block-cache] HybridCache build failed")
        });
        log::info!(
            "[block-cache] ready: disk={}B, block_size={}B, io_engine={}, dir={}",
            disk_bytes, block_size_bytes, io_engine_for_log, disk_dir.display()
        );
        Self { inner, key_index, _runtime: Arc::new(rt), stats }
    }

    /// Derive the index key from a cache key: everything before the first [`SEPARATOR`].
    /// For keys without [`SEPARATOR`] (e.g. Lucene block paths), the full key is its
    /// own index entry.
    fn index_key(key: &str) -> &str {
        if let Some(pos) = key.find(SEPARATOR) { &key[..pos] } else { key }
    }
}

impl BlockCache for FoyerCache {
    async fn get(&self, key: &CacheKey) -> Option<Bytes> {
        match self.inner.get(&key.as_str().to_string()).await {
            Ok(Some(e)) => {
                let size = e.value().len() as i64;
                self.stats.hit_count.fetch_add(1, Ordering::Relaxed);
                // Track bytes served from cache. For variable-size entries this is
                // more informative than hit_count alone — see FoyerStatsCounter docs.
                self.stats.hit_bytes.fetch_add(size, Ordering::Relaxed);
                Some(Bytes::copy_from_slice(e.value()))
            }
            _ => {
                self.stats.miss_count.fetch_add(1, Ordering::Relaxed);
                self.stats.miss_bytes.fetch_add(key.range_len() as i64, Ordering::Relaxed);
                None
            }
        }
    }

    fn put(&self, key: &CacheKey, data: Bytes) {
        let size = data.len() as i64;
        let raw = key.as_str();
        let k = raw.to_string();
        self.inner.insert(k.clone(), data.to_vec());
        // Track bytes added. If this entry replaces an existing one, on_leave()
        // will subtract the old size via Event::Replace.
        self.stats.used_bytes.fetch_add(size, Ordering::Relaxed);
        let idx = Self::index_key(raw).to_string();
        self.key_index.entry(idx).or_default().push(k);
    }

    fn evict_prefix(&self, prefix: &str) {
        // Collect all index entries whose key starts with `prefix`
        let matching: Vec<String> = self.key_index
            .iter()
            .filter(|e| e.key().starts_with(prefix))
            .map(|e| e.key().clone())
            .collect();

        for idx_key in matching {
            if let Some((_, keys)) = self.key_index.remove(&idx_key) {
                for k in keys { self.inner.remove(&k); }
            }
        }
    }

    async fn clear(&self) {
        self.key_index.clear();
        self.stats.used_bytes.store(0, Ordering::Relaxed);
        let _ = self.inner.clear().await;
    }
}
