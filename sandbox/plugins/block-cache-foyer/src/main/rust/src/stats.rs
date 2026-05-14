/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Atomic stats counters for the block cache.
//!
//! [`FoyerStatsCounter`] holds the live atomic counters updated on every hot-path
//! operation.
//!
//! - **Writes** happen inside [`FoyerCache`][crate::foyer::foyer_cache::FoyerCache]
//!   on every `get()`, `put()`, and `KeyIndexListener::on_leave()` call — all on
//!   hot paths, so all updates use [`Ordering::Relaxed`].
//! - **Reads** happen at most once per `_nodes/stats` request, via
//!   [`FoyerStatsCounter::snapshot()`] called from the `foyer_snapshot_stats` FFM
//!   function in [`crate::foyer::ffm`].
//!
//! The snapshot produces a `[i64; 9]` array whose layout is fixed and
//! documented on [`FoyerStatsCounter::snapshot`]. Java reads this array via
//! `FoyerBridge.snapshotStats()` and constructs a `FoyerAggregatedStats` snapshot
//! (two sections: overall and block-level), which is then projected to a
//! `BlockCacheStats` record for core consumption.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

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
/// | `removed_count`    | `KeyIndexListener::on_leave` | +1 per explicit removal (`Event::Remove`)            |
/// | `removed_bytes`    | `KeyIndexListener::on_leave` | +len per explicit removal                            |
#[derive(Default)]
pub struct FoyerStatsCounter {
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
    /// Number of entries explicitly removed (e.g. via `evict_prefix` on shard/index deletion).
    /// Distinct from LRU evictions (`eviction_count`) — these are application-driven removals.
    pub removed_count: AtomicI64,
    /// Total bytes explicitly removed.
    pub removed_bytes: AtomicI64,
}

impl FoyerStatsCounter {
    /// Allocate a new zeroed stats instance wrapped in an [`Arc`].
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Snapshot all counters atomically (best-effort — each field is read once).
    ///
    /// Returns `[hit_count, hit_bytes, miss_count, miss_bytes,
    ///           eviction_count, eviction_bytes, used_bytes,
    ///           removed_count, removed_bytes]` (9 values).
    ///
    /// The field order is fixed and must match `FoyerAggregatedStats.Field` ordinals in Java.
    ///
    /// Called by the `foyer_snapshot_stats` FFM export at most once per
    /// `_nodes/stats` request. Relaxed ordering is sufficient — stale-by-one
    /// stats are acceptable for monitoring purposes.
    pub fn snapshot(&self) -> [i64; 9] {
        [
            self.hit_count.load(Ordering::Relaxed),
            self.hit_bytes.load(Ordering::Relaxed),
            self.miss_count.load(Ordering::Relaxed),
            self.miss_bytes.load(Ordering::Relaxed),
            self.eviction_count.load(Ordering::Relaxed),
            self.eviction_bytes.load(Ordering::Relaxed),
            self.used_bytes.load(Ordering::Relaxed),
            self.removed_count.load(Ordering::Relaxed),
            self.removed_bytes.load(Ordering::Relaxed),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn new_returns_all_zeros() {
        let c = FoyerStatsCounter::new();
        let snap = c.snapshot();
        assert_eq!(snap, [0i64; 9]);
    }

    #[test]
    fn snapshot_returns_9_values() {
        let snap = FoyerStatsCounter::new().snapshot();
        assert_eq!(snap.len(), 9);
    }

    // Verify exact field-to-index mapping: each field maps to a distinct index.
    // A bug in the `snapshot()` array literal order would break Java parsing.

    #[test]
    fn hit_count_is_index_0() {
        let c = FoyerStatsCounter::new();
        c.hit_count.store(42, Ordering::Relaxed);
        assert_eq!(c.snapshot()[0], 42);
    }

    #[test]
    fn hit_bytes_is_index_1() {
        let c = FoyerStatsCounter::new();
        c.hit_bytes.store(1024, Ordering::Relaxed);
        assert_eq!(c.snapshot()[1], 1024);
    }

    #[test]
    fn miss_count_is_index_2() {
        let c = FoyerStatsCounter::new();
        c.miss_count.store(7, Ordering::Relaxed);
        assert_eq!(c.snapshot()[2], 7);
    }

    #[test]
    fn miss_bytes_is_index_3() {
        let c = FoyerStatsCounter::new();
        c.miss_bytes.store(512, Ordering::Relaxed);
        assert_eq!(c.snapshot()[3], 512);
    }

    #[test]
    fn eviction_count_is_index_4() {
        let c = FoyerStatsCounter::new();
        c.eviction_count.store(3, Ordering::Relaxed);
        assert_eq!(c.snapshot()[4], 3);
    }

    #[test]
    fn eviction_bytes_is_index_5() {
        let c = FoyerStatsCounter::new();
        c.eviction_bytes.store(2048, Ordering::Relaxed);
        assert_eq!(c.snapshot()[5], 2048);
    }

    #[test]
    fn used_bytes_is_index_6() {
        let c = FoyerStatsCounter::new();
        c.used_bytes.store(999, Ordering::Relaxed);
        assert_eq!(c.snapshot()[6], 999);
    }

    #[test]
    fn removed_count_is_index_7() {
        let c = FoyerStatsCounter::new();
        c.removed_count.store(11, Ordering::Relaxed);
        assert_eq!(c.snapshot()[7], 11);
    }

    #[test]
    fn removed_bytes_is_index_8() {
        let c = FoyerStatsCounter::new();
        c.removed_bytes.store(4096, Ordering::Relaxed);
        assert_eq!(c.snapshot()[8], 4096);
    }

    #[test]
    fn all_fields_independent() {
        let c = FoyerStatsCounter::new();
        c.hit_count.store(1, Ordering::Relaxed);
        c.hit_bytes.store(2, Ordering::Relaxed);
        c.miss_count.store(3, Ordering::Relaxed);
        c.miss_bytes.store(4, Ordering::Relaxed);
        c.eviction_count.store(5, Ordering::Relaxed);
        c.eviction_bytes.store(6, Ordering::Relaxed);
        c.used_bytes.store(7, Ordering::Relaxed);
        c.removed_count.store(8, Ordering::Relaxed);
        c.removed_bytes.store(9, Ordering::Relaxed);
        assert_eq!(c.snapshot(), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}
