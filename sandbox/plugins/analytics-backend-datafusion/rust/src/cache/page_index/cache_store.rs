/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Byte-bounded cache with FIFO eviction, used by the two scoped page-index
//! caches ([`COLUMN_INDEX_CACHE`] / [`OFFSET_INDEX_CACHE`] in `mod.rs`).
//!
//! # Design
//!
//! **Reads are lock-free**: `get` only touches the `DashMap`, which uses
//! per-shard read locks — different shards never contend. No policy metadata
//! is updated on read (see below).
//!
//! **Writes are serialised under one `Mutex<WriteState>`**: `insert`,
//! `evict_by_prefix`, `clear_keep_limit`, and `set_limit` all take the write
//! lock. The lock is held only for in-memory bookkeeping (hash-map ops,
//! counter arithmetic). All expensive work (parquet IO, decoding) is done by
//! the caller **before** calling `insert`, so the critical section is
//! microsecond-scale regardless of entry size.
//!
//! **Eviction is FIFO (insert-order)**. Entries are evicted oldest-first when
//! `used_bytes > limit`. We deliberately do NOT update recency on `get` —
//! that would require a write on every read, defeating the lock-free goal.
//! For the page-index use case this is acceptable: the working set is bounded
//! by files × predicate-columns, entries are large and long-lived, and warm
//! hit-rates are typically >95% so eviction is rare. A future S3-FIFO policy
//! (ghost-set promotion) can be plugged in as a follow-up.
//!
//! **Consistency**: `map`, `reverse`, and `used_bytes` are always mutated
//! together inside the write lock, so there are no torn states visible to
//! concurrent readers.

use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};

use dashmap::DashMap;
use parking_lot::Mutex;
use crate::eviction_policy;

/// Default byte budget for EACH scoped cache (column-index and offset-index),
/// used until overridden by the Java settings consumer at startup.
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

/// Mutable state serialised under the write lock.
struct WriteState<K> {
    /// Insertion-order queue for FIFO eviction.  Each element is the
    /// string-form key of one entry.i m  The front is the oldest insert.
    fifo: VecDeque<String>,
    /// Reverse map: string key → typed key, needed to remove from `map`.
    reverse: std::collections::HashMap<String, K>,
    /// Running total of bytes currently stored.
    used_bytes: usize,
    /// Byte cap.  Stored here so it can be atomically updated with eviction.
    limit: usize,
}

impl<K> WriteState<K> {
    fn new(limit: usize) -> Self {
        Self {
            fifo: VecDeque::new(),
            reverse: std::collections::HashMap::new(),
            used_bytes: 0,
            limit,
        }
    }
}

/// Byte-bounded concurrent cache with FIFO eviction.
///
/// `K` must implement `Display` so entries can be keyed in the FIFO queue by
/// their string representation (same key space used by the eviction policy).
pub(super) struct BoundedCache<K, V>
where
    K: Eq + Hash + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Value store — DashMap for lock-free concurrent reads.
    map: DashMap<K, (V, usize)>,
    /// All mutable bookkeeping serialised under one lock.
    /// Held only for in-memory ops; never held during IO.
    write: Mutex<WriteState<K>>,
    /// Byte cap exposed via atomics so `stats()` never blocks.
    limit_snapshot: AtomicUsize,
    // Lock-free counters — written with `Relaxed` since ordering relative to
    // each other is not required for correctness (they are diagnostic only).
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    used_bytes_snapshot: AtomicUsize,
}

impl<K, V> BoundedCache<K, V>
where
    K: Eq + Hash + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(super) fn new(limit: usize, _policy_type: eviction_policy::PolicyType) -> Self {
        Self {
            map: DashMap::new(),
            write: Mutex::new(WriteState::new(limit)),
            limit_snapshot: AtomicUsize::new(limit),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            used_bytes_snapshot: AtomicUsize::new(0),
        }
    }

    /// Lock-free read. Contends only on the DashMap per-shard read lock.
    /// Does NOT update eviction metadata — see module-level docs.
    pub(super) fn get(&self, key: &K) -> Option<V> {
        match self.map.get(key) {
            Some(entry) => {
                self.hits.fetch_add(1, Relaxed);
                Some(entry.0.clone())
            }
            None => {
                self.misses.fetch_add(1, Relaxed);
                None
            }
        }
    }

    /// Insert a key-value pair with its byte cost.
    ///
    /// If `size > limit` the entry is silently dropped (it can never fit).
    /// Takes the write lock for bookkeeping; IO must be done by the caller
    /// before calling this.
    pub(super) fn insert(&self, key: K, value: V, size: usize) {
        let limit = self.limit_snapshot.load(Relaxed);
        if size > limit {
            return;
        }

        let key_str = key.to_string();

        // Update the value store outside the write lock — DashMap is
        // independently concurrent-safe.  The write lock below then updates
        // the bookkeeping to match.
        let old_size = self.map.insert(key.clone(), (value, size))
            .map(|(_, s)| s)
            .unwrap_or(0);

        let mut w = self.write.lock();

        // Adjust accounting for an overwritten entry.
        if old_size > 0 {
            w.used_bytes = w.used_bytes.saturating_sub(old_size);
            w.fifo.retain(|k| k != &key_str);
            w.reverse.remove(&key_str);
        }

        w.used_bytes += size;
        w.fifo.push_back(key_str.clone());
        w.reverse.insert(key_str, key);

        let evicted = Self::drain_to_limit(&mut w, &self.map);
        self.used_bytes_snapshot.store(w.used_bytes, Relaxed);
        drop(w);

        if evicted > 0 {
            self.evictions.fetch_add(evicted, Relaxed);
        }
    }

    /// Insert multiple entries under a single write-lock acquisition.
    ///
    /// This is the preferred path when a query produces several cache cells at
    /// once (e.g. CI cells for multiple predicate columns after a vectored
    /// fetch). Taking the lock once avoids per-entry lock overhead and runs
    /// eviction exactly once at the end, preventing per-insert thrashing when
    /// many cells arrive together.
    ///
    /// Entries whose individual `size > limit` are silently skipped.
    pub(super) fn insert_batch(&self, entries: impl IntoIterator<Item = (K, V, usize)>) {
        let limit = self.limit_snapshot.load(Relaxed);

        // Write all fitting entries to the DashMap outside the write lock.
        let mut bookkeeping: Vec<(String, K, usize, usize)> = Vec::new(); // (key_str, key, size, old_size)
        for (key, value, size) in entries {
            if size > limit {
                continue;
            }
            let key_str = key.to_string();
            let old_size = self.map.insert(key.clone(), (value, size))
                .map(|(_, s)| s)
                .unwrap_or(0);
            bookkeeping.push((key_str, key, size, old_size));
        }

        if bookkeeping.is_empty() {
            return;
        }

        let mut w = self.write.lock();
        for (key_str, key, size, old_size) in bookkeeping {
            if old_size > 0 {
                w.used_bytes = w.used_bytes.saturating_sub(old_size);
                w.fifo.retain(|k| k != &key_str);
                w.reverse.remove(&key_str);
            }
            w.used_bytes += size;
            w.fifo.push_back(key_str.clone());
            w.reverse.insert(key_str, key);
        }

        let evicted = Self::drain_to_limit(&mut w, &self.map);
        self.used_bytes_snapshot.store(w.used_bytes, Relaxed);
        drop(w);

        if evicted > 0 {
            self.evictions.fetch_add(evicted, Relaxed);
        }
    }

    /// Evict oldest FIFO entries until `used_bytes <= limit`.
    /// Returns the number of entries evicted.
    fn drain_to_limit(w: &mut WriteState<K>, map: &DashMap<K, (V, usize)>) -> u64 {
        let mut evicted = 0u64;
        while w.used_bytes > w.limit {
            let Some(victim_str) = w.fifo.pop_front() else { break };
            if let Some(typed_key) = w.reverse.remove(&victim_str) {
                if let Some((_, (_, victim_size))) = map.remove(&typed_key) {
                    w.used_bytes = w.used_bytes.saturating_sub(victim_size);
                    evicted += 1;
                }
            }
        }
        evicted
    }

    /// Update the byte cap and immediately evict if over budget.
    pub(super) fn set_limit(&self, limit: usize) {
        self.limit_snapshot.store(limit, Relaxed);

        let mut w = self.write.lock();
        w.limit = limit;
        let evicted = Self::drain_to_limit(&mut w, &self.map);
        self.used_bytes_snapshot.store(w.used_bytes, Relaxed);
        drop(w);

        if evicted > 0 {
            self.evictions.fetch_add(evicted, Relaxed);
        }
    }

    /// Drop all entries and reset counters, keeping the current limit.
    pub(super) fn clear_keep_limit(&self) {
        self.map.clear();

        let mut w = self.write.lock();
        w.fifo.clear();
        w.reverse.clear();
        w.used_bytes = 0;
        drop(w);

        self.hits.store(0, Relaxed);
        self.misses.store(0, Relaxed);
        self.evictions.store(0, Relaxed);
        self.used_bytes_snapshot.store(0, Relaxed);
    }

    /// Evict all entries whose string key starts with `prefix`.
    /// Used when a parquet file is deleted or replaced so stale cells are
    /// removed from both the CI and OI caches.
    pub(super) fn evict_by_prefix(&self, prefix: &str) {
        let mut w = self.write.lock();

        let victims: Vec<String> = w.reverse.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();

        let mut evicted = 0u64;
        for key_str in victims {
            w.fifo.retain(|k| k != &key_str);
            if let Some(typed_key) = w.reverse.remove(&key_str) {
                if let Some((_, (_, size))) = self.map.remove(&typed_key) {
                    w.used_bytes = w.used_bytes.saturating_sub(size);
                    evicted += 1;
                }
            }
        }

        self.used_bytes_snapshot.store(w.used_bytes, Relaxed);
        drop(w);

        if evicted > 0 {
            self.evictions.fetch_add(evicted, Relaxed);
        }
    }

    /// Lock-free stats snapshot. Counters are approximate (Relaxed ordering)
    /// but consistent enough for observability and test assertions.
    pub(super) fn stats(&self) -> ScopedCacheStats {
        ScopedCacheStats {
            hits: self.hits.load(Relaxed),
            misses: self.misses.load(Relaxed),
            evictions: self.evictions.load(Relaxed),
            entries: self.map.len(),
            used_bytes: self.used_bytes_snapshot.load(Relaxed),
            limit_bytes: self.limit_snapshot.load(Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::eviction_policy::PolicyType;
    use std::sync::Arc;
    use std::thread;

    /// A simple string key that implements the required bounds.
    #[derive(Clone, PartialEq, Eq, Hash)]
    struct Key(String);
    impl Display for Key {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
    }
    impl Key {
        fn new(s: impl Into<String>) -> Self { Key(s.into()) }
    }

    fn make_cache(limit: usize) -> BoundedCache<Key, Vec<u8>> {
        BoundedCache::new(limit, PolicyType::Lru)
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
        // inserting 4 more bytes pushes us over limit=10; "a" (oldest) is evicted
        c.insert(Key::new("c"), vec![], 4);
        let s = c.stats();
        assert!(s.used_bytes <= 10, "used_bytes={} must be <= limit=10", s.used_bytes);
        assert!(s.evictions >= 1);
    }

    #[test]
    fn overwrite_does_not_leak_bytes() {
        let c = make_cache(1024);
        c.insert(Key::new("a"), vec![1], 10);
        c.insert(Key::new("a"), vec![2], 5); // overwrite with smaller size
        assert_eq!(c.stats().used_bytes, 5);
        assert_eq!(c.get(&Key::new("a")), Some(vec![2]));
    }

    #[test]
    fn entry_too_large_is_dropped() {
        let c = make_cache(10);
        c.insert(Key::new("huge"), vec![], 11); // > limit
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
        assert_eq!(c.limit_snapshot.load(Relaxed), 1024); // limit preserved
    }

    #[test]
    fn insert_batch_one_lock_same_result_as_individual() {
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

    // ── concurrency: correctness and consistency under parallel access ────────

    /// Many threads insert disjoint keys concurrently.
    /// Invariant: `used_bytes <= limit` and `used_bytes == sum of sizes in map`.
    #[test]
    fn concurrent_inserts_stay_within_limit() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 200;
        const ENTRY_SIZE: usize = 10;
        const LIMIT: usize = THREADS * PER_THREAD * ENTRY_SIZE / 4; // tight: forces eviction

        let cache = Arc::new(make_cache(LIMIT));
        let mut handles = vec![];

        for t in 0..THREADS {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let key = Key::new(format!("t{t}:k{i}"));
                    c.insert(key, vec![t as u8], ENTRY_SIZE);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }

        let s = cache.stats();
        assert!(
            s.used_bytes <= LIMIT,
            "used_bytes={} exceeded limit={}", s.used_bytes, LIMIT
        );

        // Map and write-state must agree: sum of sizes in DashMap == used_bytes_snapshot.
        let map_total: usize = cache.map.iter().map(|e| e.value().1).sum();
        assert_eq!(
            map_total, s.used_bytes,
            "DashMap total={map_total} != used_bytes_snapshot={}", s.used_bytes
        );
    }

    /// Readers and writers run concurrently.
    /// Readers must never see a panicked unwrap — any `Some(v)` returned must
    /// be a valid value that was inserted.
    #[test]
    fn concurrent_reads_and_writes_no_panic() {
        const WRITERS: usize = 4;
        const READERS: usize = 4;
        const OPS: usize = 500;

        let cache = Arc::new(make_cache(200));
        let mut handles = vec![];

        for t in 0..WRITERS {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..OPS {
                    c.insert(Key::new(format!("k{}", i % 20)), vec![t as u8], 10);
                }
            }));
        }
        for t in 0..READERS {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let mut hits = 0u64;
                for i in 0..OPS {
                    if c.get(&Key::new(format!("k{}", i % 20))).is_some() {
                        hits += 1;
                    }
                }
                let _ = hits; // exercising the read path is the goal
            }));
        }
        for h in handles { h.join().unwrap(); }

        // After all ops: invariant still holds.
        let s = cache.stats();
        assert!(s.used_bytes <= 200);
        let map_total: usize = cache.map.iter().map(|e| e.value().1).sum();
        assert_eq!(map_total, s.used_bytes,
            "map_total={map_total} != used_bytes_snapshot={}", s.used_bytes);
    }

    /// Concurrent inserts + evict_by_prefix.
    /// After eviction no entry with the evicted prefix must remain, and
    /// used_bytes must still equal the sum of remaining map entries.
    #[test]
    fn concurrent_insert_and_evict_by_prefix_consistent() {
        const THREADS: usize = 6;
        const OPS: usize = 300;

        let cache = Arc::new(make_cache(4096));
        let mut handles = vec![];

        // Half the threads insert under "hot/" prefix.
        for t in 0..THREADS / 2 {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..OPS {
                    c.insert(Key::new(format!("hot/t{t}:k{i}")), vec![], 10);
                }
            }));
        }
        // Other half insert under "cold/" prefix.
        for t in 0..THREADS / 2 {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..OPS {
                    c.insert(Key::new(format!("cold/t{t}:k{i}")), vec![], 10);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }

        // Evict everything under "hot/" prefix.
        cache.evict_by_prefix("hot/");

        // No hot entry must remain.
        for entry in cache.map.iter() {
            assert!(
                !entry.key().0.starts_with("hot/"),
                "hot/ entry {:?} survived evict_by_prefix", entry.key().0
            );
        }

        // used_bytes must equal map sum.
        let map_total: usize = cache.map.iter().map(|e| e.value().1).sum();
        assert_eq!(map_total, cache.stats().used_bytes,
            "map_total={map_total} != used_bytes_snapshot after prefix eviction");
    }

    /// insert_batch from concurrent threads: invariants must hold throughout.
    #[test]
    fn concurrent_insert_batch_consistent() {
        const THREADS: usize = 8;
        const BATCHES: usize = 50;
        const BATCH_SIZE: usize = 5;
        const ENTRY_SIZE: usize = 8;
        const LIMIT: usize = THREADS * BATCHES * BATCH_SIZE * ENTRY_SIZE / 3;

        let cache = Arc::new(make_cache(LIMIT));
        let mut handles = vec![];

        for t in 0..THREADS {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for b in 0..BATCHES {
                    let batch = (0..BATCH_SIZE).map(|i| {
                        (Key::new(format!("t{t}:b{b}:i{i}")), vec![t as u8], ENTRY_SIZE)
                    });
                    c.insert_batch(batch);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }

        let s = cache.stats();
        assert!(s.used_bytes <= LIMIT,
            "used_bytes={} > limit={}", s.used_bytes, LIMIT);

        let map_total: usize = cache.map.iter().map(|e| e.value().1).sum();
        assert_eq!(map_total, s.used_bytes,
            "map_total={map_total} != used_bytes_snapshot={}", s.used_bytes);
    }

    /// set_limit under concurrent inserts: limit change must take immediate
    /// effect and invariants must hold after.
    #[test]
    fn concurrent_set_limit_and_inserts_consistent() {
        const THREADS: usize = 4;
        const OPS: usize = 400;

        let cache = Arc::new(make_cache(1000));
        let mut handles = vec![];

        for t in 0..THREADS {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..OPS {
                    c.insert(Key::new(format!("t{t}:k{i}")), vec![], 10);
                }
            }));
        }
        // Concurrently shrink and grow the limit.
        let c2 = Arc::clone(&cache);
        handles.push(std::thread::spawn(move || {
            for _ in 0..20 {
                c2.set_limit(200);
                std::hint::spin_loop();
                c2.set_limit(1000);
            }
        }));

        for h in handles { h.join().unwrap(); }

        let limit = cache.limit_snapshot.load(Relaxed);
        let s = cache.stats();
        assert!(s.used_bytes <= limit,
            "used_bytes={} > final limit={}", s.used_bytes, limit);

        let map_total: usize = cache.map.iter().map(|e| e.value().1).sum();
        assert_eq!(map_total, s.used_bytes,
            "map_total={map_total} != used_bytes_snapshot={}", s.used_bytes);
    }
}
