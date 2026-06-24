/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Byte-bounded concurrent cache for the scoped page-index caches.
//!
//! `BoundedCache<K, V>` is a thin shim over [`FoyerBackedCache`], kept so the page-index call
//! sites (`mod.rs`, `page_index_io.rs`) don't change. It preserves the prior public surface:
//!
//! - `with_named_policy(limit, CacheEvictionPolicy)` / `with_policy(limit, ())` / `new(limit)`
//! - `get(&K) -> Option<V>`, `insert(K, V, size)`, `insert_batch(IntoIterator<(K,V,size)>)`
//! - `set_limit`, `clear_keep_limit`, `evict_by_prefix(&str)` (matches keys by `Display`)
//! - `stats() -> ScopedCacheStats`
//!
//! # Why a shim over foyer instead of the hand-written `DashMap + Mutex + FIFO queue`
//! foyer gives byte-bounded, sharded, lock-free eviction with production-grade S3-FIFO (and
//! LRU/LFU/FIFO/Sieve) out of the box, so we no longer hand-maintain a `ScopedEvictionPolicy`.
//! The page-index caller passes an externally-computed `size`; we carry it in the stored value
//! (`(V, usize)`) so foyer's weighter reads the exact same byte cost the old cache used.

use std::fmt::Display;
use std::hash::Hash;

use crate::cache::eviction_policy::CacheEvictionPolicy;
use crate::cache::foyer_backed_cache::FoyerBackedCache;

/// Fallback byte budget used only in tests that bypass the Java startup path.
/// In production the Java settings consumer always calls
/// `set_column_index_cache_limit` / `set_offset_index_cache_limit` during
/// plugin initialization before any query runs, so this value is never used
/// outside tests.
pub(crate) const DEFAULT_SCOPED_CACHE_LIMIT: usize = 150 * 1024 * 1024;

/// Snapshot of one scoped cache's counters plus occupancy.
/// Surfaced on node-stats and used by tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScopedCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub used_bytes: usize,
    pub limit_bytes: usize,
}

/// Byte-bounded concurrent cache. `K` must implement `Display` so `evict_by_prefix` can match
/// keys by their string representation (e.g. `"path:col:rg"` → prefix `"path"`).
///
/// Backed by a [`FoyerBackedCache`] whose value is `(V, size)`; the stored `size` is the
/// caller-supplied byte cost, which foyer's weighter returns verbatim.
pub(super) struct BoundedCache<K, V>
where
    K: Eq + Hash + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    inner: FoyerBackedCache<K, (V, usize)>,
}

impl<K, V> BoundedCache<K, V>
where
    K: Eq + Hash + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a `BoundedCache` with the given eviction policy. All `CacheEvictionPolicy` variants
    /// are accepted; the page-index caches construct this with `CacheEvictionPolicy::default()`
    /// (S3-FIFO, scan-resistant).
    pub(crate) fn with_named_policy(limit: usize, policy: CacheEvictionPolicy) -> Self {
        // Weight by the externally-supplied byte cost carried in the value tuple.
        Self {
            inner: FoyerBackedCache::with_key_tracking(limit, policy, |_k, (_v, size): &(V, usize)| *size),
        }
    }

    /// Test-only shorthand: a cache with the default eviction policy (S3-FIFO).
    #[cfg(test)]
    pub(super) fn new(limit: usize) -> Self {
        Self::with_named_policy(limit, CacheEvictionPolicy::default())
    }

    /// Lock-free read. Returns a clone of the stored value (drops the byte-cost tuple element).
    pub(super) fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key).map(|(v, _size)| v)
    }

    /// Insert a key-value pair with its byte cost.
    ///
    /// If `size > limit` the entry is silently dropped (parity with the prior cache; foyer
    /// would otherwise immediately evict it). The hot path uses [`insert_batch`](Self::insert_batch);
    /// this single-entry form is exercised by tests.
    #[cfg(test)]
    pub(super) fn insert(&self, key: K, value: V, size: usize) {
        if size > self.inner.stats().limit_bytes {
            return;
        }
        self.inner.insert(key, (value, size));
    }

    /// Insert multiple entries. foyer is internally sharded, so there is no single write lock to
    /// amortise; this simply forwards each entry (still one eviction pass per insert internally).
    pub(super) fn insert_batch(&self, entries: impl IntoIterator<Item = (K, V, usize)>) {
        let limit = self.inner.stats().limit_bytes;
        for (key, value, size) in entries {
            if size > limit {
                continue;
            }
            self.inner.insert(key, (value, size));
        }
    }

    /// Update the byte cap and immediately evict if over budget.
    pub(super) fn set_limit(&self, limit: usize) {
        self.inner.set_limit(limit);
    }

    /// Drop all entries and reset counters, keeping the current limit.
    pub(super) fn clear_keep_limit(&self) {
        self.inner.clear_keep_limit();
    }

    /// Evict all entries whose `Display` representation starts with `prefix`.
    /// Used when a parquet file is deleted or replaced. Cold path only.
    pub(super) fn evict_by_prefix(&self, prefix: &str) {
        self.inner.evict_by_prefix(prefix);
    }

    /// Lock-free stats snapshot.
    pub(super) fn stats(&self) -> ScopedCacheStats {
        let s = self.inner.stats();
        ScopedCacheStats {
            hits: s.hits,
            misses: s.misses,
            evictions: s.evictions,
            entries: s.entries,
            used_bytes: s.used_bytes,
            limit_bytes: s.limit_bytes,
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Clone, PartialEq, Eq, Hash)]
    struct Key(String);
    impl std::fmt::Display for Key {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
    }
    impl Key {
        fn new(s: impl Into<String>) -> Self { Key(s.into()) }
    }

    fn make_cache(limit: usize) -> BoundedCache<Key, Vec<u8>> {
        BoundedCache::new(limit)
    }

    // ── basic correctness ─────────────────────────────────────────────────────

    #[test]
    fn insert_and_get_roundtrip() {
        let c = make_cache(1024);
        c.insert(Key::new("a"), vec![1, 2, 3], 3);
        assert_eq!(c.get(&Key::new("a")), Some(vec![1, 2, 3]));
        assert_eq!(c.get(&Key::new("missing")), None);
    }

    #[test]
    fn used_bytes_tracks_inserts_and_evictions() {
        let c = make_cache(10);
        c.insert(Key::new("a"), vec![], 4);
        c.insert(Key::new("b"), vec![], 4);
        assert_eq!(c.stats().used_bytes, 8);
        c.insert(Key::new("c"), vec![], 4);
        let s = c.stats();
        assert!(s.used_bytes <= 10, "used_bytes={} must be <= limit=10", s.used_bytes);
        assert!(s.evictions >= 1);
    }

    #[test]
    fn overwrite_does_not_leak_bytes() {
        let c = make_cache(1024);
        c.insert(Key::new("a"), vec![1], 10);
        c.insert(Key::new("a"), vec![2], 5);
        assert_eq!(c.stats().used_bytes, 5);
        assert_eq!(c.get(&Key::new("a")), Some(vec![2]));
    }

    #[test]
    fn entry_too_large_is_dropped() {
        let c = make_cache(10);
        c.insert(Key::new("huge"), vec![], 11);
        assert_eq!(c.get(&Key::new("huge")), None);
        assert_eq!(c.stats().used_bytes, 0);
    }

    #[test]
    fn evict_by_prefix_removes_matching_entries() {
        let c = make_cache(1024);
        c.insert(Key::new("file1:col0"), vec![], 10);
        c.insert(Key::new("file1:col1"), vec![], 10);
        c.insert(Key::new("file2:col0"), vec![], 10);
        c.evict_by_prefix("file1");
        assert_eq!(c.get(&Key::new("file1:col0")), None);
        assert_eq!(c.get(&Key::new("file1:col1")), None);
        assert_eq!(c.get(&Key::new("file2:col0")), Some(vec![]));
        assert_eq!(c.stats().used_bytes, 10);
    }

    #[test]
    fn clear_resets_all_state() {
        let c = make_cache(1024);
        c.insert(Key::new("a"), vec![], 10);
        c.insert(Key::new("b"), vec![], 20);
        c.clear_keep_limit();
        let s = c.stats();
        assert_eq!(s.used_bytes, 0);
        assert_eq!(s.entries, 0);
        assert_eq!(s.hits, 0);
        assert_eq!(s.misses, 0);
        assert_eq!(s.evictions, 0);
        assert_eq!(s.limit_bytes, 1024);
    }

    #[test]
    fn insert_batch_same_result_as_individual() {
        let c1 = make_cache(1024);
        let c2 = make_cache(1024);
        let entries = vec![
            (Key::new("a"), vec![1u8], 10),
            (Key::new("b"), vec![2u8], 20),
            (Key::new("c"), vec![3u8], 30),
        ];
        for (k, v, s) in entries.clone() {
            c1.insert(k, v, s);
        }
        c2.insert_batch(entries.into_iter());
        assert_eq!(c1.get(&Key::new("a")), c2.get(&Key::new("a")));
        assert_eq!(c1.get(&Key::new("b")), c2.get(&Key::new("b")));
        assert_eq!(c1.get(&Key::new("c")), c2.get(&Key::new("c")));
        assert_eq!(c1.stats().used_bytes, c2.stats().used_bytes);
    }

    // ── concurrency ───────────────────────────────────────────────────────────

    #[test]
    fn concurrent_inserts_stay_within_limit() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 200;
        const ENTRY_SIZE: usize = 10;
        const LIMIT: usize = THREADS * PER_THREAD * ENTRY_SIZE / 4;

        let cache = Arc::new(make_cache(LIMIT));
        let mut handles = vec![];

        for t in 0..THREADS {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    c.insert(Key::new(format!("t{t}:k{i}")), vec![t as u8], ENTRY_SIZE);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }

        let s = cache.stats();
        assert!(s.used_bytes <= LIMIT, "used_bytes={} > limit={}", s.used_bytes, LIMIT);
    }

    #[test]
    fn concurrent_reads_and_writes_no_panic() {
        let cache = Arc::new(make_cache(200));
        let mut handles = vec![];
        for t in 0..4usize {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..500 {
                    c.insert(Key::new(format!("k{}", i % 20)), vec![t as u8], 10);
                }
            }));
        }
        for _ in 0..4 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..500 {
                    let _ = c.get(&Key::new(format!("k{}", i % 20)));
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        let s = cache.stats();
        assert!(s.used_bytes <= 200);
    }

    #[test]
    fn concurrent_insert_and_evict_by_prefix_consistent() {
        let cache = Arc::new(make_cache(4096));
        let mut handles = vec![];
        for t in 0..3usize {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..300 {
                    c.insert(Key::new(format!("hot/t{t}:k{i}")), vec![], 10);
                }
            }));
        }
        for t in 0..3usize {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..300 {
                    c.insert(Key::new(format!("cold/t{t}:k{i}")), vec![], 10);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        cache.evict_by_prefix("hot/");
        // No "hot/" key should survive.
        assert!(cache.get(&Key::new("hot/t0:k0")).is_none());
    }
}
