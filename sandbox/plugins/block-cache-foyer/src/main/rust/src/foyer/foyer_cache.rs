/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`FoyerCache`] — a [`BlockCache`] implementation backed by Foyer.

use bytes::Bytes;
use dashmap::DashMap;
#[cfg(target_os = "linux")]
use foyer::UringIoEngineConfig;
use foyer::{
    AdmitAll, BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    IoEngineConfig, PsyncIoEngineConfig, RecoverMode, StorageFilter,
};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::key_index_store;
use crate::range_cache::{key_byte_size, CacheKey, SEPARATOR};
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
            native_bridge_common::log_info!("[block-cache] io_engine=io_uring forced by config");
            #[cfg(target_os = "linux")]
            return UringIoEngineConfig::new().boxed();
            #[cfg(not(target_os = "linux"))]
            panic!("[block-cache] io_engine=io_uring requested but io_uring is not supported on non-Linux platforms");
        }
        "psync" => {
            native_bridge_common::log_info!("[block-cache] io_engine=psync forced by config");
            return PsyncIoEngineConfig::new().boxed();
        }
        other => {
            if other != "auto" {
                native_bridge_common::log_info!(
                    "[block-cache] unknown io_engine='{}'; falling back to auto-detect",
                    other
                );
            }
            // "auto" — detect by kernel version (existing logic)
            #[cfg(target_os = "linux")]
            {
                let release = std::fs::read_to_string("/proc/sys/kernel/osrelease")
                    .unwrap_or_else(|_| "unknown".to_string());
                let release = release.trim();
                if kernel_version_at_least(5, 1) {
                    native_bridge_common::log_info!(
                        "[block-cache] kernel {} — io_uring available, using UringIoEngineConfig",
                        release
                    );
                    return UringIoEngineConfig::new().boxed();
                } else {
                    native_bridge_common::log_info!(
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

// ── ActiveBytesGuard ─────────────────────────────────────────────────────────

/// Increments `active_in_bytes` when created and decrements it when dropped.
///
/// Ensures the counter is always restored even when the owning `get()` future
/// is cancelled mid-flight (e.g. a `tokio::select!` timeout fires before the
/// disk read completes). Without this guard, a cancelled future would leave
/// `active_in_bytes` permanently elevated.
pub(crate) struct ActiveBytesGuard<'a> {
    counter: &'a std::sync::atomic::AtomicI64,
    value: i64,
}

impl<'a> ActiveBytesGuard<'a> {
    /// Construct the guard and immediately increment the counter.
    pub(crate) fn new(counter: &'a std::sync::atomic::AtomicI64, value: i64) -> Self {
        counter.fetch_add(value, Ordering::Relaxed);
        Self { counter, value }
    }
}

impl Drop for ActiveBytesGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.value, Ordering::Relaxed);
    }
}

// ── FoyerCache ────────────────────────────────────────────────────────────────

/// Disk block cache with prefix-based eviction support backed by Foyer.
///
/// Wraps a Foyer [`HybridCache`] configured as a disk-only store (memory tier = 1 byte),
/// a concurrent key index that maps each index prefix to its cached entry keys,
/// and a set of [`FoyerStatsCounter`] atomic counters.
///
/// The key index allows removing all cached entries sharing a common prefix
/// in O(n) without requiring Foyer to support prefix-scan semantics.
///
/// ## Recovery
/// On startup the key_index is bulk-loaded from `key_index.json` inside `disk_dir`
/// (written on graceful shutdown and periodically by the persist task). The Foyer
/// disk data is recovered via `RecoverMode::Quiet`. Stale key_index entries (keys
/// Foyer did not recover) are cleaned up by the background sweep task.
///
/// ## Persistence
/// The key_index is written:
/// 1. By the independent periodic persist task every `persist_interval_secs` seconds
///    (when `used_bytes` has changed since the last write — zero-write when idle).
/// 2. Unconditionally in `Drop` (graceful shutdown).
/// On crash (`Drop` not called), only the Foyer disk data is recovered;
/// key_index starts empty (same as before this feature was added).
///
/// Shutdown: the background sweep task and persist task are cancelled immediately via
/// [`CancellationToken`] when the cache is dropped — both tasks wake from
/// `tokio::select!` without waiting for the current sleep interval to expire.
///
/// Stats: `get()` → hit/miss counts; `put()` → `used_bytes`; `evict_prefix()` → `removed_count`;
/// background sweeper → `eviction_count` for disk-reclaimer evictions. Thread-safe.
pub struct FoyerCache {
    inner: HybridCache<String, Vec<u8>>,
    /// Maps each index prefix to the **set** of Foyer cache keys stored under that prefix.
    ///
    /// `HashSet` is used (not `Vec`) so that inserting the same key twice — which can happen
    /// during a cache stampede when two threads both miss and call `put()` for the same range —
    /// is a no-op. This prevents double-counting in `used_bytes`, `eviction_bytes`,
    /// `removed_bytes`, and related stats.
    ///
    /// Entries added by `put()`. Entries removed by `evict_prefix()` and `clear()`.
    /// Disk-evicted keys become stale until the sweeper prunes them via `inner.contains(key)`
    /// (in-RAM index lookup, no disk I/O; false positives on hash collision are harmless).
    pub(crate) key_index: Arc<DashMap<String, HashSet<String>>>,
    /// Keeps the Tokio runtime alive for the lifetime of the cache.
    _runtime: Arc<tokio::runtime::Runtime>,
    /// Atomic stats counters. Exposed for FFM read via `foyer_snapshot_stats`.
    pub(crate) stats: Arc<FoyerStatsCounter>,
    /// Signals the background sweep task and persist task to stop immediately when
    /// `FoyerCache` is dropped.
    ///
    /// Uses [`CancellationToken`] rather than `AtomicBool` so that both loops can use
    /// `tokio::select!` and wake instantly on cancellation instead of waiting for the
    /// current sleep interval to expire before checking the flag.
    ///
    /// Exposed as `pub(crate)` so tests can clone the token and inspect `is_cancelled()`
    /// after drop to verify the shutdown signal was sent.
    pub(crate) shutdown: CancellationToken,
    /// Tracks which DashMap shard to sweep next.
    ///
    /// `key_index` is internally sharded by DashMap. Each sweep call processes exactly
    /// one shard and advances this cursor by 1. Over `shard_count` sweep cycles the
    /// entire key_index is covered, distributing lock pressure across multiple intervals
    /// rather than blocking all shards in a single call.
    ///
    /// In production builds this field is not read directly — the background task holds
    /// its own `Arc` clone. In test builds `sweep_once()` reads it via `&self.sweep_cursor`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) sweep_cursor: Arc<AtomicUsize>,
    /// Configured disk capacity in bytes.
    ///
    /// Used in the background sweep task to compute `used_bytes / disk_bytes` and compare
    /// against `sweep_threshold_ratio`. Only accessed inside the async task closure (which
    /// captures it by value) and in test builds via `should_skip_sweep()`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) disk_bytes: usize,
    /// Minimum `used_bytes / disk_bytes` ratio required to run the key_index sweep.
    ///
    /// On each interval tick, the sweep loop checks whether the current usage ratio is
    /// strictly below this threshold. If so, the sweep is skipped entirely (no-op) — no
    /// DashMap locks are acquired and no shard is iterated. This avoids wasting CPU cycles
    /// when the cache is lightly loaded and Foyer's disk reclaimer is unlikely to have
    /// evicted anything.
    ///
    /// `0.0` = disabled (always sweep, preserving pre-threshold behaviour).
    /// Example: `0.75` → only sweep when `used_bytes >= 75 %` of `disk_bytes`.
    ///
    /// Only accessed inside the async task closure (captured by value) and in test builds
    /// via `should_skip_sweep()`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) sweep_threshold_ratio: Arc<AtomicU64>,
    /// Live-updatable sweep interval in seconds. `0` = disabled (task uses this only to sleep;
    /// if changed to 0 while running, the task sleeps 0 s and immediately checks cancellation).
    pub(crate) sweep_interval_secs: Arc<AtomicU64>,
    /// Live-updatable persist interval in seconds. `0` = disabled.
    pub(crate) persist_interval_secs: Arc<AtomicU64>,
    /// Absolute path to the Foyer cache directory.
    /// Used to write/read `key_index.json` for persistence and recovery.
    pub(crate) cache_dir: PathBuf,
}

impl Drop for FoyerCache {
    fn drop(&mut self) {
        // Flush partial blocks to SSD before shutdown with a timeout.
        // Without close(), entries smaller than block_size are lost on restart.
        // Timeout prevents indefinite blocking if the flusher is stuck.
        let close_result = self._runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(30), self.inner.close()).await
        });
        match close_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                native_bridge_common::log_info!(
                    "[block-cache] HybridCache close FAILED on shutdown: {}",
                    e
                );
            }
            Err(_) => {
                native_bridge_common::log_info!(
                    "[block-cache] HybridCache close TIMED OUT (30s) on shutdown — partial blocks may be lost"
                );
            }
        }

        // Unconditional final persist — graceful shutdown path.
        // Writes the complete current key_index as the authoritative final snapshot.
        if let Err(e) = key_index_store::save(&self.cache_dir, &self.key_index) {
            native_bridge_common::log_info!(
                "[block-cache] key_index final persist FAILED on shutdown: {}",
                e
            );
        } else {
            native_bridge_common::log_info!(
                "[block-cache] key_index persisted on shutdown: {} prefix buckets, dir={}",
                self.key_index.len(),
                self.cache_dir.display()
            );
        }
        // Cancel the background sweep task and persist task so they wake immediately
        // from tokio::select! without waiting for the current sleep to expire.
        self.shutdown.cancel();
    }
}

impl FoyerCache {
    /// Initialise the cache synchronously.
    ///
    /// # Parameters
    /// - `disk_bytes` — total disk capacity for this cache.
    /// - `disk_dir` — directory on the local SSD where Foyer stores its data files.
    /// - `block_size_bytes` — Foyer disk block size. Must be ≥ the largest entry ever
    ///   put into the cache. Configurable via `block_cache.foyer.block_size`.
    /// - `io_engine` — I/O engine selection: `"auto"`, `"io_uring"`, or `"psync"`.
    ///   Configurable via `block_cache.foyer.io_engine`.
    /// - `sweep_interval_secs` — how often (in seconds) the background sweeper prunes
    ///   stale key_index entries left by the disk reclaimer. `0` = disabled (no sweep task
    ///   is spawned). Configurable via `block_cache.foyer.key_index_sweep_interval_seconds`.
    /// - `sweep_threshold_ratio` — minimum usage ratio (`used_bytes / disk_bytes`) required
    ///   to run the sweep. If the ratio is below this threshold the sweep tick is skipped
    ///   (no-op). `0.0` = always sweep (threshold disabled). Range: `[0.0, 1.0]`.
    ///   Configurable via `block_cache.foyer.key_index_sweep_threshold`.
    /// - `persist_interval_secs` — how often (in seconds) the independent persist task
    ///   flushes the key_index to disk. `0` = disabled (only `Drop` persists).
    ///   Configurable via `block_cache.foyer.key_index_persist_interval_seconds`.
    ///
    /// # Panics
    /// Panics if the Tokio runtime cannot be created or if Foyer fails to
    /// build the cache (e.g. insufficient disk space or invalid path).
    pub fn new(
        disk_bytes: usize,
        disk_dir: impl Into<PathBuf>,
        block_size_bytes: usize,
        buffer_pool_size_bytes: usize,
        submit_queue_size_threshold_bytes: usize,
        io_engine: &str,
        sweep_interval_secs: u64,
        sweep_threshold_ratio: f64,
        persist_interval_secs: u64,
        reinsertion_admit_all: bool,
    ) -> Self {
        let disk_dir: PathBuf = disk_dir.into();

        let key_index: Arc<DashMap<String, HashSet<String>>> = Arc::new(DashMap::new());
        let stats = FoyerStatsCounter::new();

        let rt =
            tokio::runtime::Runtime::new().expect("[block-cache] failed to create Tokio runtime");
        // Clone disk_dir only for the async closure; the original is moved into cache_dir
        // after block_on returns.
        let dir_clone = disk_dir.clone();
        let io_engine = io_engine.to_string();
        let io_engine_for_log = io_engine.clone(); // clone for use in log after the closure
        let inner = rt.block_on(async move {
            let mut engine_config = BlockEngineConfig::new(
                FsDeviceBuilder::new(dir_clone)
                    .with_capacity(disk_bytes)
                    .build()
                    .expect("[block-cache] FsDevice build failed"),
            )
            .with_block_size(block_size_bytes)
            .with_buffer_pool_size(buffer_pool_size_bytes)
            .with_submit_queue_size_threshold(submit_queue_size_threshold_bytes);

            if reinsertion_admit_all {
                engine_config = engine_config
                    .with_reinsertion_filter(StorageFilter::new().with_condition(AdmitAll));
            }

            HybridCacheBuilder::<String, Vec<u8>>::new()
                .with_name("block-cache")
                .memory(1)
                // Disable the in-memory tier — this cache is disk-only.
                // Foyer is a hybrid (DRAM + disk) cache; setting the memory capacity
                // to 1 byte opts out of DRAM caching. All entries go directly to the
                // disk tier (FsDevice) below.
                .storage()
                // RecoverMode::Quiet recovers existing disk entries into Foyer's in-RAM
                // index without raising an error on corrupted pages. Together with
                // recover_key_index() this ensures disk data and key_index are consistent
                // after a graceful restart.
                // On a fresh directory (clean startup) Quiet behaves identically to None.
                .with_recover_mode(RecoverMode::Quiet)
                .with_io_engine_config(build_io_engine_config(&io_engine))
                .with_engine_config(engine_config)
                .build()
                .await
                .expect("[block-cache] HybridCache build failed")
        });
        native_bridge_common::log_info!(
            "[block-cache] ready: disk={}B, block_size={}B, io_engine={}, sweep_threshold={:.0}%, \
             persist_interval={}s, reinsertion={}, dir={}",
            disk_bytes,
            block_size_bytes,
            io_engine_for_log,
            sweep_threshold_ratio * 100.0,
            if persist_interval_secs == 0 {
                "disabled".to_string()
            } else {
                persist_interval_secs.to_string()
            },
            if reinsertion_admit_all {
                "AdmitAll"
            } else {
                "RejectAll"
            },
            disk_dir.display()
        );
        // CancellationToken is Clone and Send — cheap to share with background tasks.
        let shutdown = CancellationToken::new();

        // Sweep cursor starts at shard 0 and advances by one per sweep call,
        // rotating through all shards over successive intervals.
        let sweep_cursor = Arc::new(AtomicUsize::new(0));

        // ── Construct instance ────────────────────────────────────────────────
        let sweep_threshold_atomic = Arc::new(AtomicU64::new(sweep_threshold_ratio.to_bits()));
        let sweep_interval_atomic = Arc::new(AtomicU64::new(sweep_interval_secs));
        let persist_interval_atomic = Arc::new(AtomicU64::new(persist_interval_secs));

        let mut instance = Self {
            inner,
            key_index,
            _runtime: Arc::new(rt),
            stats,
            shutdown,
            sweep_cursor,
            disk_bytes,
            sweep_threshold_ratio: sweep_threshold_atomic,
            sweep_interval_secs: sweep_interval_atomic,
            persist_interval_secs: persist_interval_atomic,
            cache_dir: disk_dir, // move: dir_clone was consumed by block_on, disk_dir is still owned
        };

        // Bulk-load key_index from disk.
        // On clean startup or crash: load_or_empty() returns empty silently.
        instance.recover_key_index();

        // ── Spawn background sweep task ───────────────────────────────────────
        // Spawn the background key_index sweeper: removes entries silently evicted by Foyer's
        // disk reclaimer. inner.contains() is an in-RAM index lookup (no disk I/O).
        // sweep_interval_secs > 0 required to spawn; 0 means sweep is disabled.
        if sweep_interval_secs > 0 {
            let sweep_token = instance.shutdown.clone();
            let sweep_inner = instance.inner.clone();
            let sweep_key_index = Arc::clone(&instance.key_index);
            let sweep_stats = Arc::clone(&instance.stats);
            let sweep_cursor_clone = Arc::clone(&instance.sweep_cursor);
            let sweep_interval_atomic_clone = Arc::clone(&instance.sweep_interval_secs);
            let sweep_threshold_atomic_clone = Arc::clone(&instance.sweep_threshold_ratio);
            let sweep_disk_bytes = disk_bytes;

            instance._runtime.spawn(async move {
                native_bridge_common::log_info!(
                    "[block-cache] sweep task started: interval={}s", sweep_interval_secs
                );
                let mut restart_count = 0u32;
                loop {
                    'inner: loop {
                        // Read interval on each tick — allows live update without restart.
                        let current_secs = sweep_interval_atomic_clone.load(Ordering::Relaxed);
                        let interval = if current_secs == 0 {
                            Duration::from_secs(3600) // effectively disabled: sleep 1 hour
                        } else {
                            Duration::from_secs(current_secs)
                        };
                        tokio::select! {
                            _ = tokio::time::sleep(interval) => {
                                if current_secs == 0 { continue 'inner; } // disabled
                                let threshold = f64::from_bits(sweep_threshold_atomic_clone.load(Ordering::Relaxed));
                                if threshold > 0.0 {
                                    let used = sweep_stats.used_bytes.load(Ordering::Relaxed).max(0) as usize;
                                    let ratio = used as f64 / sweep_disk_bytes as f64;
                                    if ratio < threshold {
                                        continue 'inner;
                                    }
                                    native_bridge_common::log_debug!(
                                        "[block-cache] sweep running: usage={:.2}% >= threshold={:.2}%",
                                        ratio * 100.0, threshold * 100.0
                                    );
                                }
                                // Panic safety: a panic in reconcile_key_index must not kill the task.
                                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    Self::reconcile_key_index(
                                        &sweep_key_index, &sweep_inner, &sweep_stats, &sweep_cursor_clone,
                                    )
                                }));
                                if let Err(_panic_payload) = result {
                                    native_bridge_common::log_error!(
                                        "[block-cache] sweep task: reconcile_key_index panicked — sweep continuing"
                                    );
                                }
                            }
                            _ = sweep_token.cancelled() => {
                                native_bridge_common::log_info!("[block-cache] sweep task stopped");
                                return;
                            }
                        }
                    }
                    restart_count += 1;
                    native_bridge_common::log_error!(
                        "[block-cache] sweep task exited unexpectedly (restart #{})", restart_count
                    );
                    let backoff_secs = std::cmp::min(1u64 << restart_count.min(6), 60);
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    if sweep_token.is_cancelled() { return; }
                }
            });
        }

        // ── Spawn independent persist task ────────────────────────────────────
        // Persists the key_index whenever used_bytes changes, on a schedule independent
        // of the sweep task. This allows different intervals for sweeping (expensive DashMap
        // scan) vs persisting (cheap file write).
        // persist_interval_secs > 0 required to spawn; 0 means disabled (Drop-only persist).
        if persist_interval_secs > 0 {
            let persist_token = instance.shutdown.clone();
            let persist_key_index = Arc::clone(&instance.key_index);
            let persist_stats = Arc::clone(&instance.stats);
            let persist_dir = instance.cache_dir.clone();
            let persist_interval = Duration::from_secs(persist_interval_secs);

            instance._runtime.spawn(async move {
                native_bridge_common::log_info!(
                    "[block-cache] persist task started: interval={}s",
                    persist_interval_secs
                );
                let mut restart_count = 0u32;
                loop {
                    // Reset sentinel on each loop entry so the first tick always persists.
                    let mut last_persisted: i64 = i64::MIN;
                    'inner: loop {
                        tokio::select! {
                            _ = tokio::time::sleep(persist_interval) => {
                                let current = persist_stats.used_bytes.load(Ordering::Relaxed);
                                if current != last_persisted {
                                    // Panic safety: a panic in save must not kill the task.
                                    let dir_clone = persist_dir.clone();
                                    let ki_clone = Arc::clone(&persist_key_index);
                                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        key_index_store::save(&dir_clone, &ki_clone)
                                    }));
                                    match result {
                                        Ok(Ok(())) => {
                                            last_persisted = current;
                                        }
                                        Ok(Err(e)) => {
                                            // I/O error — do not update last_persisted, retry next tick.
                                            native_bridge_common::log_info!(
                                                "[block-cache] persist task FAILED: {}", e
                                            );
                                        }
                                        Err(_panic_payload) => {
                                            // Panic inside save — log error, continue loop.
                                            native_bridge_common::log_error!(
                                                "[block-cache] persist task: key_index_store::save panicked — persist continuing"
                                            );
                                        }
                                    }
                                }
                            }
                            _ = persist_token.cancelled() => {
                                native_bridge_common::log_info!("[block-cache] persist task stopped");
                                return;
                            }
                        }
                    }
                    restart_count += 1;
                    native_bridge_common::log_error!(
                        "[block-cache] persist task exited unexpectedly (restart #{})", restart_count
                    );
                    let backoff_secs = std::cmp::min(1u64 << restart_count.min(6), 60);
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    if persist_token.is_cancelled() { return; }
                }
            });
        }

        instance
    }

    /// Bulk-load the persisted key_index snapshot into the in-memory DashMap.
    ///
    /// # Design: no per-key validation (no `inner.contains()`)
    /// All keys in the snapshot are inserted directly into `key_index` and
    /// `used_bytes` is initialised to the sum of all their byte sizes.
    ///
    /// Rationale:
    /// - When below `sweep_threshold_ratio`, Foyer LRU is not actively evicting →
    ///   the snapshot is accurate. Any minor inaccuracy from entries LRU-evicted
    ///   between the last periodic persist and shutdown is corrected by the sweep
    ///   task within `shard_count × sweep_interval_secs` of startup.
    /// - Skipping `contains()` avoids O(N) hash lookups at startup.
    /// - The sweep task (already built and tested) handles stale key cleanup for
    ///   both post-recovery and live-session stale entries uniformly.
    ///
    /// # `used_bytes` initialisation
    /// Initialised to `sum(key_byte_size(k))` for ALL keys in the snapshot. May be
    /// temporarily over-counted by keys Foyer LRU-evicted between the last periodic
    /// persist and shutdown. Corrected by sweep.
    ///
    /// # Failure modes (all non-fatal)
    /// - Missing file (clean startup or crash): silently treated as empty.
    /// - Corrupt JSON or version mismatch: logged as WARN, treated as empty.
    /// - I/O error: logged as WARN, treated as empty.
    fn recover_key_index(&mut self) {
        let t0 = Instant::now();
        let snapshot = key_index_store::load_or_empty(&self.cache_dir);

        if snapshot.is_empty() {
            native_bridge_common::log_info!(
                "[block-cache] key_index recovery: no snapshot (clean startup or crash), dir={}",
                self.cache_dir.display()
            );
            return;
        }

        // Compute stats before moving snapshot.index into the DashMap.
        let total_loaded: usize = snapshot.index.values().map(|s| s.len()).sum();
        let recovered_bytes: i64 = snapshot
            .index
            .values()
            .flat_map(|keys| keys.iter())
            .map(|k| key_byte_size(k))
            .sum();

        // Bulk-insert each bucket: DashMap::insert only needs &self (interior mutability).
        // No per-key inner.contains() — the sweep task corrects stale entries post-startup.
        for (prefix, keys) in snapshot.index {
            self.key_index.insert(prefix, keys);
        }

        // Initialise used_bytes. No put() calls have happened yet so it is 0.
        self.stats
            .used_bytes
            .fetch_add(recovered_bytes, Ordering::Relaxed);

        let elapsed_ms = t0.elapsed().as_millis() as u64;
        native_bridge_common::log_info!(
            "[block-cache] key_index recovered: total_keys={} recovered_bytes={} \
             elapsed_ms={} prefix_buckets={}",
            total_loaded,
            recovered_bytes,
            elapsed_ms,
            self.key_index.len()
        );
    }

    /// Sweep one DashMap shard per call and advance the cursor.
    ///
    /// Each call acquires one shard's write lock, filters out keys no longer present in
    /// Foyer, and releases the lock. The cursor advances by 1 modulo the shard count so
    /// successive calls rotate fairly through all shards.
    ///
    /// Spreading work across calls bounds the per-call lock-hold time to one shard's
    /// entry count, keeping `put()` latency impact minimal — only entries in the swept
    /// shard are blocked, and only for the duration of that shard's `retain()` pass.
    fn reconcile_key_index(
        key_index: &Arc<DashMap<String, HashSet<String>>>,
        cache: &HybridCache<String, Vec<u8>>,
        stats: &Arc<FoyerStatsCounter>,
        cursor: &Arc<AtomicUsize>,
    ) -> usize {
        let shards = key_index.shards();
        let shard_count = shards.len();
        let shard_idx = cursor.fetch_add(1, Ordering::Relaxed) % shard_count;

        let mut shard = shards[shard_idx].write(); // write lock on ONE shard
        let mut stale_removed = 0usize;
        let mut freed_bytes = 0i64;

        for value in shard.values_mut() {
            let keys: &mut HashSet<String> = value.get_mut();
            let before = keys.len();
            keys.retain(|k| {
                if cache.contains(k) {
                    true
                } else {
                    freed_bytes += key_byte_size(k);
                    false
                }
            });
            stale_removed += before - keys.len();
        }
        // Remove empty prefix buckets from this shard.
        shard.retain(|_, v| !v.get().is_empty());

        // Explicitly release the write lock BEFORE calling key_index.len().
        //
        // DashMap::len() acquires read locks on ALL shards (including this one).
        // Calling len() while still holding the write lock on shard_idx causes a
        // self-deadlock: len() tries to read-lock the shard we already write-lock.
        drop(shard);

        if stale_removed > 0 {
            stats
                .eviction_count
                .fetch_add(stale_removed as i64, Ordering::Relaxed);
            stats
                .eviction_bytes
                .fetch_add(freed_bytes, Ordering::Relaxed);
            stats.used_bytes.fetch_add(-freed_bytes, Ordering::Relaxed);
            native_bridge_common::log_info!(
                "[block-cache] key_index_sweep: shard={} stale_removed={} freed_bytes={} key_index_size={}",
                shard_idx, stale_removed, freed_bytes, key_index.len()
            );
        } else {
            native_bridge_common::log_debug!(
                "[block-cache] key_index_sweep: shard={} no stale entries, key_index_size={}",
                shard_idx,
                key_index.len()
            );
        }
        stale_removed
    }

    /// Trigger one shard sweep synchronously, advancing the cursor.
    ///
    /// Test-only helper — calls [`Self::reconcile_key_index`] directly so tests can
    /// drive the sweep without waiting for the background timer. Does **not** apply
    /// the `sweep_threshold_ratio` guard (that lives in the async task loop).
    #[cfg(test)]
    pub(crate) fn sweep_once(&self) -> usize {
        Self::reconcile_key_index(
            &self.key_index,
            &self.inner,
            &self.stats,
            &self.sweep_cursor,
        )
    }

    /// Returns `true` if the current usage ratio is below the configured threshold,
    /// meaning the background sweep task would skip this tick.
    ///
    /// Test-only helper — lets tests verify the threshold guard logic without needing
    /// a running async task or sleeping.
    ///
    /// # Returns
    /// - `false` when `sweep_threshold_ratio == 0.0` (threshold disabled → never skip).
    /// - `true`  when `sweep_threshold_ratio > 0.0` AND `used_bytes / disk_bytes < threshold`.
    /// - `false` when `sweep_threshold_ratio > 0.0` AND `used_bytes / disk_bytes >= threshold`.
    #[cfg(test)]
    pub(crate) fn should_skip_sweep(&self) -> bool {
        let threshold = f64::from_bits(self.sweep_threshold_ratio.load(Ordering::Relaxed));
        if threshold <= 0.0 {
            return false; // disabled: always sweep
        }
        let used = self.stats.used_bytes.load(Ordering::Relaxed).max(0) as usize;
        (used as f64 / self.disk_bytes as f64) < threshold
    }

    /// Update the sweep threshold ratio atomically. Takes effect on next tick.
    pub(crate) fn update_sweep_threshold(&self, new_ratio: f64) {
        self.sweep_threshold_ratio
            .store(new_ratio.to_bits(), Ordering::Relaxed);
        native_bridge_common::log_info!(
            "[block-cache] sweep threshold updated live: {:.0}%",
            new_ratio * 100.0
        );
    }

    /// Update the sweep interval live. `0` = disable sweep. Takes effect on next sleep.
    pub(crate) fn update_sweep_interval(&self, new_secs: u64) {
        self.sweep_interval_secs.store(new_secs, Ordering::Relaxed);
        native_bridge_common::log_info!("[block-cache] sweep interval updated live: {}s", new_secs);
    }

    /// Update the persist interval live. `0` = disable periodic persist. Takes effect on next sleep.
    pub(crate) fn update_persist_interval(&self, new_secs: u64) {
        self.persist_interval_secs
            .store(new_secs, Ordering::Relaxed);
        native_bridge_common::log_info!(
            "[block-cache] persist interval updated live: {}s",
            new_secs
        );
    }

    /// Clear all entries synchronously. Called from the FFM layer.
    pub(crate) fn clear_sync(&self) {
        self._runtime.block_on(self.clear());
    }

    /// Wait for the storage flusher to drain its write queue.
    /// After this returns, all previously put() entries are on SSD and findable via get().
    pub async fn wait_for_flush(&self) {
        self.inner.storage().wait().await;
    }

    /// Derive the normalized index key from a cache key.
    ///
    /// Extracts everything before the first [`SEPARATOR`] (the path prefix),
    /// then strips any leading `/` so that keys stored by DataFusion's
    /// `object_store::Path` (no leading slash) and keys from tests or
    /// direct path strings (with leading slash) both map to the same bucket.
    fn index_key(key: &str) -> &str {
        let raw = if let Some(pos) = key.find(SEPARATOR) {
            &key[..pos]
        } else {
            key
        };
        raw.trim_start_matches('/')
    }
}

impl BlockCache for FoyerCache {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn get<'a>(
        &'a self,
        key: &'a CacheKey,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Bytes>> + Send + 'a>> {
        Box::pin(async {
            let range_len = key.range_len() as i64;
            // ActiveBytesGuard increments active_in_bytes on construction and decrements it in Drop.
            // This ensures the counter is restored even if this future is dropped mid-execution
            // (e.g. the caller uses tokio::select! with a timeout that fires before the disk read
            // completes). Without the guard, a cancelled future would leave active_in_bytes elevated.
            let _active_guard = ActiveBytesGuard::new(&self.stats.active_in_bytes, range_len);

            match self.inner.get(&key.as_str().to_string()).await {
                Ok(Some(e)) => {
                    let size = e.value().len() as i64;
                    self.stats.hit_count.fetch_add(1, Ordering::Relaxed);
                    self.stats.hit_bytes.fetch_add(size, Ordering::Relaxed);
                    Some(Bytes::copy_from_slice(e.value()))
                }
                _ => {
                    self.stats.miss_count.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .miss_bytes
                        .fetch_add(range_len, Ordering::Relaxed);
                    None
                }
            }
            // _active_guard dropped here — fetch_sub runs regardless of hit/miss/cancellation
        })
    }

    fn put(&self, key: &CacheKey, data: Bytes) {
        let size = data.len() as i64;
        let raw = key.as_str();
        let k = raw.to_string();
        self.inner.insert(k.clone(), data.to_vec());
        // index_key() normalizes by stripping leading '/' — evict_prefix uses the same normalization.
        let idx = Self::index_key(raw).to_string();
        // HashSet::insert returns true only when the key is new.
        // If it returns false the key was already present (concurrent put / re-put after eviction)
        // and we must NOT increment used_bytes again — the caller already paid for it on the first put.
        let is_new = self.key_index.entry(idx).or_default().insert(k);
        if is_new {
            self.stats.used_bytes.fetch_add(size, Ordering::Relaxed);
        }
    }

    /// Intended for use during index or shard deletion, after new reads for the prefix have
    /// already been stopped. If a `put()` happens to race with this call on the same prefix,
    /// that new entry will also be evicted — which is expected, since the prefix is going away.
    fn evict_prefix(&self, prefix: &str) {
        // Normalize prefix: object_store::Path strips leading '/' when building keys,
        let normalized = prefix.trim_start_matches('/');
        let matching: Vec<String> = self
            .key_index
            .iter()
            .filter(|e| e.key().starts_with(normalized))
            .map(|e| e.key().clone())
            .collect();

        let mut total_evicted = 0usize;
        let mut removed_bytes = 0i64;
        for idx_key in &matching {
            if let Some((_, keys)) = self.key_index.remove(idx_key) {
                total_evicted += keys.len();
                for k in keys {
                    // Derive byte size from key before removing: "path\x1Fstart-end" → end-start.
                    removed_bytes += key_byte_size(&k);
                    self.inner.remove(&k);
                }
            }
        }

        // Update stats here — NOT via Event::Remove — because with memory(1) entries are already
        // on disk when evict_prefix() is called, so memory.remove() returns None and Event::Remove
        // never fires.
        if total_evicted > 0 {
            self.stats
                .removed_count
                .fetch_add(total_evicted as i64, Ordering::Relaxed);
            self.stats
                .removed_bytes
                .fetch_add(removed_bytes, Ordering::Relaxed);
            self.stats
                .used_bytes
                .fetch_add(-removed_bytes, Ordering::Relaxed);
        }

        native_bridge_common::log_info!(
            "[block-cache] EVICT_PREFIX prefix='{}' matched_index_keys={} evicted_entries={} key_index_len={}",
            prefix, matching.len(), total_evicted, self.key_index.len()
        );
    }

    fn clear(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            // Accumulate removed stats from key_index before wiping.
            // Slightly inaccurate: stale entries (disk-reclaimer-evicted but not yet swept)
            // are counted as removed here rather than as evictions. Acceptable — mirrors how
            // FileCache.clear() uses recordRemoval() per entry.
            let mut total_removed = 0i64;
            let mut total_removed_bytes = 0i64;
            for entry in self.key_index.iter() {
                for k in entry.value() {
                    total_removed += 1;
                    total_removed_bytes += key_byte_size(k);
                }
            }
            self.key_index.clear();
            self.stats.used_bytes.store(0, Ordering::Relaxed);
            if total_removed > 0 {
                self.stats
                    .removed_count
                    .fetch_add(total_removed, Ordering::Relaxed);
                self.stats
                    .removed_bytes
                    .fetch_add(total_removed_bytes, Ordering::Relaxed);
            }
            let _ = self.inner.clear().await;

            // Delete the persisted key_index files so the next startup does not bulk-load
            // stale keys into a freshly-cleared cache.
            if let Err(e) = key_index_store::delete(&self.cache_dir) {
                native_bridge_common::log_info!(
                    "[block-cache] key_index clear: failed to delete snapshot files: {}",
                    e
                );
            }
        })
    }
}
