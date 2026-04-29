/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Atomic stats counters for the block cache.
//!
//! [`BlockCacheStatsCounter`] is the Rust equivalent of Java's `DefaultStatsCounter` —
//! live atomic counters updated on every hot-path operation.
//!
//! - **Writes** happen inside [`FoyerCache`][crate::foyer::foyer_cache::FoyerCache]
//!   on every `get()`, `put()`, and `KeyIndexListener::on_leave()` call — all on
//!   hot paths, so all updates use [`Ordering::Relaxed`].
//! - **Reads** happen at most once per `_nodes/stats` request, via
//!   [`BlockCacheStatsCounter::snapshot()`] called from the `foyer_snapshot_stats` FFM
//!   function in [`crate::foyer::ffm`].
//!
//! The snapshot produces a `[i64; 7]` array whose layout is fixed and
//! documented on [`BlockCacheStatsCounter::snapshot`]. Java reads this array via
//! `FoyerBridge.snapshotStats()` and constructs a `BlockCacheStats` Java object
//! that implements `IBlockCacheStats`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Aggregate snapshot of block cache statistics, mirroring the structure of
/// Java's [`AggregateRefCountedCacheStats`] for uniformity.
///
/// Today Foyer is a single-tier disk cache (no in-memory tier), so
/// `overall` and `block_level` always carry identical values. The struct
/// exists so that callers on the Java side can treat block-cache stats with
/// the same shape they use for FileCache stats, and so that future Foyer
/// configurations (e.g. a real in-memory tier alongside the disk tier) can
/// populate the two fields independently without a breaking change.
///
/// # FFM layout
/// [`foyer_snapshot_stats`][crate::foyer::ffm] writes `14` consecutive
/// `i64` values: the 7-field snapshot of `overall` followed by the 7-field
/// snapshot of `block_level`.
///
/// Each 7-value section layout:
/// `[hit_count, hit_bytes, miss_count, miss_bytes, eviction_count, eviction_bytes, used_bytes]`
#[derive(Default, Debug)]
pub struct AggregateBlockCacheStats {
    /// Cross-tier rollup (today: same as `block_level` — Foyer has one tier).
    pub overall: [i64; 7],
    /// Block-level (disk tier) stats.
    pub block_level: [i64; 7],
}

impl AggregateBlockCacheStats {
    /// Flatten to a `[i64; 14]` array for FFM transfer.
    /// Layout: `overall[0..7]` then `block_level[0..7]`.
    pub fn to_flat(&self) -> [i64; 14] {
        let mut out = [0i64; 14];
        out[..7].copy_from_slice(&self.overall);
        out[7..].copy_from_slice(&self.block_level);
        out
    }
}

/// Atomic stats counters shared between the cache and its event listener.
///
/// # Thread safety
/// All fields are [`AtomicI64`] — safe to update concurrently from Tokio
/// worker threads (Foyer's async I/O) and the caller thread.
///
/// # Counter semantics
///
/// Fixed-size block caches (e.g. Lucene ~8 MB blocks) can use count-based hit rate
/// as a proxy for byte-based effectiveness because all entries are roughly the same
/// size. The block cache here stores variable-size Parquet column chunks (1 KB –
/// 64 MB), so **count-based hit rate alone is misleading** — a 99% count hit rate
/// on tiny metadata columns while missing 64 MB row groups represents near-zero
/// actual I/O savings. `hit_bytes` and `miss_bytes` give the true picture.
///
/// | Field              | Updated by                   | Notes                                                |
/// |--------------------|------------------------------|------------------------------------------------------|
/// | `hit_count`        | `FoyerCache::get()` hit      | +1 per cache hit                                     |
/// | `hit_bytes`        | `FoyerCache::get()` hit      | +entry.len() — bytes served from cache               |
/// | `miss_count`       | `FoyerCache::get()` miss     | +1 per cache miss                                    |
/// | `miss_bytes`       | `FoyerCache::get()` miss     | +key.range_len() — bytes that must be fetched remotely|
/// | `eviction_count`   | `KeyIndexListener::on_leave` | +1 per LRU eviction (`Event::Evict`)                 |
/// | `eviction_bytes`   | `KeyIndexListener::on_leave` | +len per LRU eviction                                |
/// | `used_bytes`       | `put()` / `on_leave`         | +len on insert, -len on eviction/remove/clear        |
#[derive(Default)]
pub struct BlockCacheStatsCounter {
    /// Number of `get()` calls that returned a cached value.
    pub hit_count: AtomicI64,
    /// Bytes served from cache across all hits.
    /// More meaningful than `hit_count` for variable-size entries.
    pub hit_bytes: AtomicI64,
    /// Number of `get()` calls that returned no value.
    pub miss_count: AtomicI64,
    /// Bytes that had to be fetched from the remote store due to cache misses.
    /// Derived from `key.range_len()` — the requested range size is known at
    /// miss time from the cache key, without waiting for the remote fetch.
    pub miss_bytes: AtomicI64,
    /// Number of entries removed by LRU pressure.
    pub eviction_count: AtomicI64,
    /// Total bytes removed by LRU pressure.
    pub eviction_bytes: AtomicI64,
    /// Current bytes resident in the cache on disk.
    pub used_bytes: AtomicI64,
}

impl BlockCacheStatsCounter {
    /// Allocate a new zeroed stats instance wrapped in an [`Arc`].
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Snapshot all counters atomically (best-effort — each field is read once).
    ///
    /// Returns `[hit_count, hit_bytes, miss_count, miss_bytes,
    ///           eviction_count, eviction_bytes, used_bytes]` (7 values).
    ///
    /// Called by the `foyer_snapshot_stats` FFM export at most once per
    /// `_nodes/stats` request. Relaxed ordering is sufficient — stale-by-one
    /// stats are acceptable for monitoring purposes.
    pub fn snapshot(&self) -> [i64; 7] {
        [
            self.hit_count.load(Ordering::Relaxed),
            self.hit_bytes.load(Ordering::Relaxed),
            self.miss_count.load(Ordering::Relaxed),
            self.miss_bytes.load(Ordering::Relaxed),
            self.eviction_count.load(Ordering::Relaxed),
            self.eviction_bytes.load(Ordering::Relaxed),
            self.used_bytes.load(Ordering::Relaxed),
        ]
    }
}
