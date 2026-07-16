/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Byte-bounded concurrent cache with a pluggable eviction policy.
//!
//! # Architecture
//!
//! ```text
//!  BoundedCache<K, V, P: ScopedEvictionPolicy<K>>
//!  ├── DashMap<K, (V, usize)>        ← lock-free concurrent reads
//!  ├── Mutex<{ used_bytes, limit }>  ← write-path accounting only
//!  └── P (eviction policy)           ← owns the eviction queue, uses interior mutability
//! ```
//!
//! **Reads are lock-free** — `get` only touches the `DashMap` shard lock.
//!
//! **Writes take one `parking_lot::Mutex`** for `used_bytes` + limit accounting.
//! The eviction policy is called inside the lock for consistency.
//!
//! **Pluggable eviction**: [`ScopedEvictionPolicy`] is the trait. Today
//! [`FifoPolicy`] is the only implementation. S3-FIFO (ghost-set promotion)
//! can be added later by implementing the same trait.
//!
//! **Lazy deletion on overwrite**: re-inserting an existing key leaves a stale
//! entry in the eviction queue. [`BoundedCache::drain_to_limit`] skips stale
//! victims with a single `DashMap::remove` miss — O(1) overwrite, no O(n)
//! retain scan.
//!
//! **`evict_by_prefix`** is the only O(n) operation: it scans the eviction
//! queue to remove all cells for a deleted/replaced file. This is a rare
//! cold-path operation (file deletion or merge), not on the query hot path.

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};

use crate::eviction_policy::CacheEvictionPolicy;
use dashmap::DashMap;
use parking_lot::Mutex;

/// Fallback byte budget used only in tests that bypass the Java startup path.
/// In production the Java settings consumer always calls
/// `set_column_index_cache_limit` / `set_offset_index_cache_limit` during
/// plugin initialization before any query runs, so this value is never used
/// outside tests.
pub(crate) const DEFAULT_SCOPED_CACHE_LIMIT: usize = 150 * 1024 * 1024;

// ── Eviction policy trait ────────────────────────────────────────────────────

/// Pluggable eviction policy for [`BoundedCache`].
///
/// Implementations must use interior mutability (`Mutex`, atomics, etc.) so
/// they can be called through a shared reference. All methods are called from
/// inside the `BoundedCache` write lock, so implementations do not need their
/// own locks — but must still be `Send + Sync` for the `Lazy<BoundedCache>`
/// statics.
///
/// # Implementing a new policy
///
/// 1. Implement this trait with interior mutability.
/// 2. Wire it in `mod.rs` by passing it to `BoundedCache::new`.
/// 3. Expose a `PolicyType` variant in the Java API and route it from
///    `df_create_cache` in `ffm.rs`.
pub(super) trait ScopedEvictionPolicy<K>: Send + Sync {
    /// Called when a new entry is inserted (or an existing key is overwritten).
    /// The policy should record `key` as the most-recently inserted entry.
    fn on_insert(&self, key: K);

    /// Called when an entry is accessed on the hot read path.
    /// FIFO ignores this; frequency-aware policies (LFU, S3-FIFO) update
    /// access counts here.
    fn on_access(&self, key: &K);

    /// Pop and return the next eviction candidate.
    ///
    /// Called repeatedly by [`BoundedCache::drain_to_limit`] until
    /// `used_bytes <= limit`. The caller performs lazy deletion: if the
    /// returned key is no longer in the `DashMap` (stale entry from an
    /// overwrite), `drain_to_limit` skips it and calls `next_victim` again.
    fn next_victim(&self) -> Option<K>;

    /// Called when an entry is explicitly removed (prefix eviction, clear).
    /// Allows the policy to remove the entry from its internal bookkeeping.
    fn on_remove(&self, key: &K);

    /// Reset all internal state (called on `clear_keep_limit`).
    fn clear(&self);
}

// ── FIFO policy ──────────────────────────────────────────────────────────────

/// Insert-order (FIFO) eviction policy.
///
/// Oldest-inserted entry is evicted first. Access order is not tracked — reads
/// are fully lock-free. Stale entries (from overwrites) are left in the queue
/// and skipped by the `BoundedCache` eviction loop (lazy deletion).
///
/// # No inner lock
///
/// All `ScopedEvictionPolicy` methods are called exclusively from inside
/// `BoundedCache`'s outer `Mutex<WriteState>` lock, which already serializes
/// all mutations. An additional inner lock on the queue would be a
/// no-contention lock/unlock cycle with zero benefit. We use `UnsafeCell`
/// instead, with the outer lock as the synchronization invariant.
///
/// # Safety invariant
///
/// Every method that calls `queue_mut()` or `queue_ref()` must be called while
/// the `BoundedCache` write lock is held. `on_access` is a no-op for FIFO and
/// is called from the lock-free `get` path — it does not touch the queue.
///
/// S3-FIFO (planned successor) wraps two FIFO queues with a ghost set. If its
/// `on_access` needs to update state from the read path, it would need its own
/// interior mutability (e.g. atomics for frequency counters).
pub(super) struct FifoPolicy<K> {
    /// Guarded by the outer `BoundedCache` write lock — no inner lock needed.
    queue: UnsafeCell<VecDeque<K>>,
}

// SAFETY: `FifoPolicy` is only mutated under `BoundedCache`'s `Mutex<WriteState>`.
unsafe impl<K: Send> Send for FifoPolicy<K> {}
unsafe impl<K: Send> Sync for FifoPolicy<K> {}

impl<K> FifoPolicy<K> {
    pub(super) fn new() -> Self {
        Self {
            queue: UnsafeCell::new(VecDeque::new()),
        }
    }

    /// Borrow the queue mutably. Caller must hold the outer write lock.
    #[inline]
    fn queue_mut(&self) -> &mut VecDeque<K> {
        // SAFETY: caller holds the `BoundedCache` write lock.
        unsafe { &mut *self.queue.get() }
    }
}

impl<K: Clone + PartialEq + Send + Sync> ScopedEvictionPolicy<K> for FifoPolicy<K> {
    fn on_insert(&self, key: K) {
        self.queue_mut().push_back(key);
    }

    fn on_access(&self, _key: &K) {
        // FIFO does not track access order — no queue mutation, reads stay lock-free.
    }

    fn next_victim(&self) -> Option<K> {
        self.queue_mut().pop_front()
    }

    fn on_remove(&self, key: &K) {
        // O(n) scan — only called from evict_by_prefix (cold path).
        self.queue_mut().retain(|k| k != key);
    }

    fn clear(&self) {
        self.queue_mut().clear();
    }
}

// ── Write-lock state ─────────────────────────────────────────────────────────

/// Mutable accounting serialised under the `BoundedCache` write lock.
/// The eviction queue lives in the policy; this struct tracks only bytes.
struct WriteState {
    used_bytes: usize,
    limit: usize,
}

impl WriteState {
    fn new(limit: usize) -> Self {
        Self {
            used_bytes: 0,
            limit,
        }
    }
}

// ── BoundedCache ─────────────────────────────────────────────────────────────

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

/// Byte-bounded concurrent cache parameterised over an eviction policy `P`.
///
/// `K` must implement `Display` so `evict_by_prefix` can match keys by their
/// string representation (e.g. `"path:col:rg"` → prefix `"path"`).
pub(super) struct BoundedCache<K, V, P = FifoPolicy<K>>
where
    K: Eq + Hash + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    P: ScopedEvictionPolicy<K>,
{
    /// Value store — DashMap for lock-free concurrent reads.
    map: DashMap<K, (V, usize)>,
    /// Byte accounting under one lock. The eviction queue lives in `policy`.
    write: Mutex<WriteState>,
    /// Eviction policy — uses interior mutability, called inside `write` lock.
    policy: P,
    /// Byte cap snapshot for lock-free `stats()`.
    limit_snapshot: AtomicUsize,
    // Lock-free diagnostic counters.
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    used_bytes_snapshot: AtomicUsize,
}

impl<K, V> BoundedCache<K, V, FifoPolicy<K>>
where
    K: Eq + Hash + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a `BoundedCache` with the named eviction policy.
    ///
    /// Only `Fifo` is accepted — `Lru`/`Lfu` belong to `CustomStatisticsCache`.
    /// The match is exhaustive over `CacheEvictionPolicy` so adding a new
    /// variant (e.g. `S3Fifo`) will be a compile error here until it's wired.
    pub(crate) fn with_named_policy(limit: usize, policy: CacheEvictionPolicy) -> Self {
        use crate::cache::eviction_policy::CacheEvictionPolicy;
        let CacheEvictionPolicy::Fifo = policy else {
            unreachable!("BoundedCache only supports Fifo; use CustomStatisticsCache for Lru/Lfu");
        };
        Self::with_policy(limit, FifoPolicy::new())
    }

    /// Create a new cache with FIFO eviction — shorthand for tests.
    pub(super) fn new(limit: usize) -> Self {
        Self::with_named_policy(limit, CacheEvictionPolicy::Fifo)
    }
}

impl<K, V, P> BoundedCache<K, V, P>
where
    K: Eq + Hash + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    P: ScopedEvictionPolicy<K>,
{
    /// Create a cache with an explicit policy. Use this when plugging in a
    /// non-default policy (e.g. S3-FIFO in a future PR).
    pub(super) fn with_policy(limit: usize, policy: P) -> Self {
        Self {
            map: DashMap::new(),
            write: Mutex::new(WriteState::new(limit)),
            policy,
            limit_snapshot: AtomicUsize::new(limit),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            used_bytes_snapshot: AtomicUsize::new(0),
        }
    }

    /// Lock-free read. Contends only on the DashMap per-shard read lock.
    pub(super) fn get(&self, key: &K) -> Option<V> {
        match self.map.get(key) {
            Some(entry) => {
                self.policy.on_access(key);
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
    /// If `size > limit` the entry is silently dropped. Takes the write lock
    /// for accounting; IO must be done by the caller before calling this.
    pub(super) fn insert(&self, key: K, value: V, size: usize) {
        let limit = self.limit_snapshot.load(Relaxed);
        if size > limit {
            return;
        }

        let old_size = self
            .map
            .insert(key.clone(), (value, size))
            .map(|(_, s)| s)
            .unwrap_or(0);

        let mut w = self.write.lock();

        if old_size > 0 {
            // Overwrite: update byte accounting. Stale FIFO entry is left in
            // place for lazy deletion by drain_to_limit.
            w.used_bytes = w.used_bytes.saturating_sub(old_size);
        }

        w.used_bytes += size;
        self.policy.on_insert(key);

        let evicted = self.drain_to_limit(&mut w);
        self.used_bytes_snapshot.store(w.used_bytes, Relaxed);
        drop(w);

        if evicted > 0 {
            self.evictions.fetch_add(evicted, Relaxed);
        }
    }

    /// Insert multiple entries under a single write-lock acquisition.
    ///
    /// Preferred when a query produces several cells at once (e.g. all CI
    /// cells for a vectored multi-column fetch). One lock + one eviction pass
    /// instead of N separate calls.
    pub(super) fn insert_batch(&self, entries: impl IntoIterator<Item = (K, V, usize)>) {
        let limit = self.limit_snapshot.load(Relaxed);

        // DashMap updates are outside the write lock — independently concurrent.
        let mut to_account: Vec<(K, usize, usize)> = Vec::new();
        for (key, value, size) in entries {
            if size > limit {
                continue;
            }
            let old_size = self
                .map
                .insert(key.clone(), (value, size))
                .map(|(_, s)| s)
                .unwrap_or(0);
            to_account.push((key, size, old_size));
        }

        if to_account.is_empty() {
            return;
        }

        let mut w = self.write.lock();
        for (key, size, old_size) in to_account {
            if old_size > 0 {
                w.used_bytes = w.used_bytes.saturating_sub(old_size);
            }
            w.used_bytes += size;
            self.policy.on_insert(key);
        }

        let evicted = self.drain_to_limit(&mut w);
        self.used_bytes_snapshot.store(w.used_bytes, Relaxed);
        drop(w);

        if evicted > 0 {
            self.evictions.fetch_add(evicted, Relaxed);
        }
    }

    /// Evict policy victims until `used_bytes <= limit`.
    /// Stale victims (overwritten entries) are skipped via lazy deletion.
    /// Must be called inside the write lock.
    fn drain_to_limit(&self, w: &mut WriteState) -> u64 {
        let mut evicted = 0u64;
        while w.used_bytes > w.limit {
            let Some(victim) = self.policy.next_victim() else {
                break;
            };
            if let Some((_, (_, size))) = self.map.remove(&victim) {
                w.used_bytes = w.used_bytes.saturating_sub(size);
                evicted += 1;
            }
            // If map.remove returned None: stale entry, used_bytes already
            // adjusted at overwrite time — skip without double-decrementing.
        }
        evicted
    }

    /// Update the byte cap and immediately evict if over budget.
    pub(super) fn set_limit(&self, limit: usize) {
        self.limit_snapshot.store(limit, Relaxed);
        let mut w = self.write.lock();
        w.limit = limit;
        let evicted = self.drain_to_limit(&mut w);
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
        w.used_bytes = 0;
        self.policy.clear();
        drop(w);
        self.hits.store(0, Relaxed);
        self.misses.store(0, Relaxed);
        self.evictions.store(0, Relaxed);
        self.used_bytes_snapshot.store(0, Relaxed);
    }

    /// Evict all entries whose `Display` representation starts with `prefix`.
    /// Used when a parquet file is deleted or replaced.
    ///
    /// O(n) scan of the eviction queue — cold path only (file deletion/merge).
    pub(super) fn evict_by_prefix(&self, prefix: &str) {
        // Collect victims by iterating the DashMap (avoids holding the write
        // lock while doing string comparisons on every FIFO entry).
        let victims: Vec<K> = self
            .map
            .iter()
            .filter(|e| e.key().to_string().starts_with(prefix))
            .map(|e| e.key().clone())
            .collect();

        if victims.is_empty() {
            return;
        }

        let mut w = self.write.lock();
        let mut evicted = 0u64;
        for victim in &victims {
            if let Some((_, (_, size))) = self.map.remove(victim) {
                w.used_bytes = w.used_bytes.saturating_sub(size);
                evicted += 1;
            }
            self.policy.on_remove(victim);
        }
        self.used_bytes_snapshot.store(w.used_bytes, Relaxed);
        drop(w);

        if evicted > 0 {
            self.evictions.fetch_add(evicted, Relaxed);
        }
    }

    /// Lock-free stats snapshot.
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

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Clone, PartialEq, Eq, Hash)]
    struct Key(String);
    impl std::fmt::Display for Key {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }
    impl Key {
        fn new(s: impl Into<String>) -> Self {
            Key(s.into())
        }
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
        assert!(
            s.used_bytes <= 10,
            "used_bytes={} must be <= limit=10",
            s.used_bytes
        );
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
        assert_eq!(c.limit_snapshot.load(Relaxed), 1024);
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

    // ── pluggable policy ──────────────────────────────────────────────────────

    /// Verify that `with_policy` compiles and works with a custom policy,
    /// proving the extensibility point works without modifying `BoundedCache`.
    #[test]
    fn custom_policy_compiles_and_works() {
        // A trivial "always evict the first key ever inserted" policy (same as FIFO).
        let cache: BoundedCache<Key, Vec<u8>, FifoPolicy<Key>> =
            BoundedCache::with_policy(20, FifoPolicy::new());
        cache.insert(Key::new("x"), vec![], 10);
        cache.insert(Key::new("y"), vec![], 10);
        cache.insert(Key::new("z"), vec![], 10); // triggers eviction of "x"
        assert_eq!(cache.get(&Key::new("x")), None, "x must have been evicted");
        assert!(cache.stats().used_bytes <= 20);
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
        for h in handles {
            h.join().unwrap();
        }

        let s = cache.stats();
        assert!(
            s.used_bytes <= LIMIT,
            "used_bytes={} > limit={}",
            s.used_bytes,
            LIMIT
        );
        let map_total: usize = cache.map.iter().map(|e| e.value().1).sum();
        assert_eq!(map_total, s.used_bytes);
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
        for h in handles {
            h.join().unwrap();
        }
        let s = cache.stats();
        assert!(s.used_bytes <= 200);
        let map_total: usize = cache.map.iter().map(|e| e.value().1).sum();
        assert_eq!(map_total, s.used_bytes);
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
        for h in handles {
            h.join().unwrap();
        }
        cache.evict_by_prefix("hot/");
        for entry in cache.map.iter() {
            assert!(!entry.key().0.starts_with("hot/"));
        }
        let map_total: usize = cache.map.iter().map(|e| e.value().1).sum();
        assert_eq!(map_total, cache.stats().used_bytes);
    }

    #[test]
    fn concurrent_insert_batch_consistent() {
        const THREADS: usize = 8;
        const LIMIT: usize = THREADS * 50 * 5 * 8 / 3;
        let cache = Arc::new(make_cache(LIMIT));
        let mut handles = vec![];
        for t in 0..THREADS {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for b in 0..50usize {
                    let batch = (0..5usize)
                        .map(|i| (Key::new(format!("t{t}:b{b}:i{i}")), vec![t as u8], 8));
                    c.insert_batch(batch);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let s = cache.stats();
        assert!(s.used_bytes <= LIMIT);
        let map_total: usize = cache.map.iter().map(|e| e.value().1).sum();
        assert_eq!(map_total, s.used_bytes);
    }

    #[test]
    fn concurrent_set_limit_and_inserts_consistent() {
        let cache = Arc::new(make_cache(1000));
        let mut handles = vec![];
        for t in 0..4usize {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..400 {
                    c.insert(Key::new(format!("t{t}:k{i}")), vec![], 10);
                }
            }));
        }
        let c2 = Arc::clone(&cache);
        handles.push(std::thread::spawn(move || {
            for _ in 0..20 {
                c2.set_limit(200);
                std::hint::spin_loop();
                c2.set_limit(1000);
            }
        }));
        for h in handles {
            h.join().unwrap();
        }
        let limit = cache.limit_snapshot.load(Relaxed);
        let s = cache.stats();
        assert!(s.used_bytes <= limit);
        let map_total: usize = cache.map.iter().map(|e| e.value().1).sum();
        assert_eq!(map_total, s.used_bytes);
    }
}
