/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `FoyerBackedCache` — a byte-bounded in-memory cache backed by [`foyer::Cache`].
//!
//! Replaces the bespoke eviction machinery (hand-written `CachePolicy`, `BoundedCache`'s
//! `DashMap + Mutex<WriteState> + ScopedEvictionPolicy`) with foyer's sharded, lock-free
//! in-memory cache. foyer provides byte-bounded capacity (via a weighter), all the eviction
//! algorithms we care about (LRU / LFU / FIFO / S3-FIFO / Sieve), dynamic resize, and
//! per-shard concurrency — so the metadata, statistics, column-index and offset-index caches
//! can all share one well-tested implementation instead of four hand-rolled ones.
//!
//! ## Why this exists
//! - **No hand-maintained eviction.** foyer's S3-FIFO is production-grade and tunable to our
//!   empirically-best config (`small_queue_capacity_ratio = 0.25`, `ghost_queue_capacity_ratio
//!   = 0.0`); see `project_s3fifo_tuning_findings`.
//! - **No global policy lock.** foyer shards internally, removing the `Mutex<policy>` read-path
//!   contention the metadata-cache micro-bench A/B flagged.
//!
//! ## Byte accounting
//! Capacity and per-entry cost are **bytes** (foyer weighter), matching the existing size-limit
//! model. The caller supplies the weighter; the cache stays ≤ the configured byte cap.
//!
//! ## Prefix eviction
//! Page-index caches must drop every cell belonging to a replaced/deleted segment. foyer keys
//! aren't prefix-scannable, so we keep a `prefix → {keys}` side index. It is kept leak-free by
//! a foyer [`EventListener`]: whenever an entry leaves the cache for ANY reason (eviction,
//! removal, clear) foyer calls `on_leave`, and we drop that key from the side index. Without the
//! listener the side index would accumulate keys foyer had already evicted.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};

use dashmap::DashMap;
use foyer::{Cache, CacheBuilder, Event, EventListener, FifoConfig, LfuConfig, LruConfig, S3FifoConfig};

use crate::cache::eviction_policy::CacheEvictionPolicy;

/// Extracts the eviction-prefix (e.g. a segment base path) from a key, or `None` for caches
/// that don't use prefix eviction (metadata / statistics). Held behind `Arc` so the cache and
/// its event-listener share the same closure.
pub type PrefixFn<K> = Arc<dyn Fn(&K) -> Option<String> + Send + Sync>;

/// Per-entry byte cost. Shared (`Arc`) between foyer's own weighter, the cache's `insert`
/// (adds the new entry's bytes) and the event-listener (subtracts a leaving entry's bytes),
/// so our `used_bytes` accounting is exact and independent of foyer's `usage()`.
pub type WeighterFn<K, V> = Arc<dyn Fn(&K, &V) -> usize + Send + Sync>;

/// Lightweight stats snapshot, shape-compatible with the page-index `ScopedCacheStats`
/// (`hits, misses, entries, used_bytes, limit_bytes`) the Java stats endpoint reads.
#[derive(Debug, Clone, Default)]
pub struct FoyerCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub used_bytes: usize,
    pub limit_bytes: usize,
}

/// Map our policy enum → a foyer eviction config. S3-FIFO is tuned to the values that beat
/// LRU/paper-default on our metadata cache (ghost-off, small-queue 0.25).
fn eviction_config(policy: CacheEvictionPolicy) -> foyer::EvictionConfig {
    match policy {
        CacheEvictionPolicy::Lru => LruConfig::default().into(),
        CacheEvictionPolicy::Lfu => LfuConfig::default().into(),
        CacheEvictionPolicy::Fifo => FifoConfig::default().into(),
        CacheEvictionPolicy::S3Fifo => S3FifoConfig {
            small_queue_capacity_ratio: 0.25,
            ghost_queue_capacity_ratio: 0.0,
            ..Default::default()
        }
        .into(),
    }
}

/// Number of shards for the foyer cache. Sharding is what removes global-lock contention;
/// scale modestly with cores but cap so tiny caches aren't over-sharded.
fn default_shards() -> usize {
    num_cpus::get().clamp(1, 16)
}

/// State shared between the cache and its event-listener: the prefix side index, a live entry
/// count, and our own byte counter. `K` only — the listener carries `V`.
///
/// **Why we track `used_bytes` ourselves instead of calling `foyer::Cache::usage()`:** foyer
/// 0.22.3's `clear()` resets per-shard entry counts but does NOT decrement the per-shard `usage`
/// accumulator, so `usage()` reports stale bytes after a clear. We therefore maintain an exact
/// `AtomicUsize` updated from `insert` (+weight) and `on_leave` (−weight) — the same lock-free
/// `memory_used` accounting approach as PR #22146.
struct CacheState<K>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
{
    /// prefix (segment base path) → keys cached under it. Empty for non-prefix caches.
    by_prefix: DashMap<String, HashSet<K>>,
    /// Live entry count (advisory — like `memory_used`, exactness isn't required).
    entries: AtomicUsize,
    /// Exact byte usage (see struct doc for why this is not foyer's `usage()`).
    used_bytes: AtomicUsize,
    /// Derives a key's prefix; returns `None` to opt a cache out of prefix tracking.
    prefix_of: PrefixFn<K>,
}

impl<K> CacheState<K>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
{
    /// Decrement entry count without underflowing past zero.
    fn dec_entries(&self) {
        let _ = self
            .entries
            .fetch_update(Relaxed, Relaxed, |n| if n == 0 { None } else { Some(n - 1) });
    }

    /// Subtract a leaving entry's bytes without underflowing past zero.
    fn sub_bytes(&self, weight: usize) {
        let _ = self
            .used_bytes
            .fetch_update(Relaxed, Relaxed, |n| Some(n.saturating_sub(weight)));
    }

    /// Drop a key from the prefix side index when it leaves the cache.
    fn forget_key(&self, key: &K) {
        if let Some(prefix) = (self.prefix_of)(key) {
            if let Some(mut set) = self.by_prefix.get_mut(&prefix) {
                set.remove(key);
            }
        }
    }
}

/// foyer event-listener that keeps [`CacheState`] in sync as entries leave the cache. Holds the
/// shared weighter so it can subtract the exact bytes of the departing value.
struct StateListener<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Send + Sync + 'static,
{
    state: Arc<CacheState<K>>,
    weighter: WeighterFn<K, V>,
}

impl<K, V> EventListener for StateListener<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Send + Sync + 'static,
{
    type Key = K;
    type Value = V;

    fn on_leave(&self, reason: Event, key: &K, value: &V) {
        let weight = (self.weighter)(key, value).max(1);
        match reason {
            // Entry genuinely left the cache → drop all bookkeeping for it.
            Event::Evict | Event::Remove | Event::Clear => {
                self.state.dec_entries();
                self.state.sub_bytes(weight);
                self.state.forget_key(key);
            }
            // Replace evicts the OLD value (same key) before the new one is accounted in
            // `insert`. Subtract the old bytes; entry count and prefix tracking are unchanged.
            Event::Replace => {
                self.state.sub_bytes(weight);
            }
        }
    }
}

/// A byte-bounded, foyer-backed cache. `K`/`V` must be `Clone + Send + Sync + 'static`
/// (foyer stores values behind `Arc`; `get` returns a cloned `V` to match the prior
/// `BoundedCache`/`DashMap` `Option<V>` semantics).
pub struct FoyerBackedCache<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    inner: Cache<K, V>,
    state: Arc<CacheState<K>>,
    /// Shared byte-cost fn; used by `insert` to add the new entry's bytes (the listener and
    /// foyer hold their own clones).
    weighter: WeighterFn<K, V>,
    /// Hit/miss are not exposed by foyer in the shape we report, so we track them here.
    hits: AtomicU64,
    misses: AtomicU64,
    /// Configured byte cap (mirrors foyer's capacity for lock-free `stats()` reads).
    limit_bytes: AtomicUsize,
}

impl<K, V> FoyerBackedCache<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    /// Build a cache that does not use prefix eviction (metadata / statistics caches).
    pub fn new(
        limit_bytes: usize,
        policy: CacheEvictionPolicy,
        weighter: impl Fn(&K, &V) -> usize + Send + Sync + 'static,
    ) -> Self {
        Self::with_prefix_fn(limit_bytes, policy, weighter, Arc::new(|_| None))
    }

    /// Build a cache with `limit_bytes` capacity, the given eviction policy, a byte `weighter`,
    /// and a `prefix_of` extractor enabling [`evict_by_prefix`](Self::evict_by_prefix)
    /// (page-index caches).
    pub fn with_prefix_fn(
        limit_bytes: usize,
        policy: CacheEvictionPolicy,
        weighter: impl Fn(&K, &V) -> usize + Send + Sync + 'static,
        prefix_of: PrefixFn<K>,
    ) -> Self {
        // One shared weighter used by foyer (for capacity), the listener (subtract on leave),
        // and `insert` (add on insert) — so all three agree on each entry's byte cost.
        let weighter: WeighterFn<K, V> = Arc::new(weighter);
        let state = Arc::new(CacheState {
            by_prefix: DashMap::new(),
            entries: AtomicUsize::new(0),
            used_bytes: AtomicUsize::new(0),
            prefix_of,
        });
        let listener: Arc<dyn EventListener<Key = K, Value = V>> = Arc::new(StateListener {
            state: Arc::clone(&state),
            weighter: Arc::clone(&weighter),
        });
        let foyer_weighter = Arc::clone(&weighter);
        let inner: Cache<K, V> = CacheBuilder::new(limit_bytes)
            .with_shards(default_shards())
            .with_eviction_config(eviction_config(policy))
            .with_weighter(move |k: &K, v: &V| foyer_weighter(k, v).max(1))
            .with_event_listener(listener)
            .build();
        Self {
            inner,
            state,
            weighter,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            limit_bytes: AtomicUsize::new(limit_bytes),
        }
    }

    /// Per-shard read. Returns a clone of the value on hit. Records hit/miss.
    pub fn get(&self, key: &K) -> Option<V> {
        match self.inner.get(key) {
            Some(entry) => {
                self.hits.fetch_add(1, Relaxed);
                Some(entry.value().clone())
            }
            None => {
                self.misses.fetch_add(1, Relaxed);
                None
            }
        }
    }

    /// Insert `(key, value)`; foyer evicts as needed to stay ≤ capacity. The byte cost comes
    /// from the weighter set at build time, so no explicit size argument is needed.
    pub fn insert(&self, key: K, value: V) {
        // Account the new entry's bytes up front. On a replace, foyer fires `on_leave(Replace)`
        // for the old value (subtracting its bytes) during `inner.insert`, so net usage is
        // correct either way. On eviction triggered by this insert, `on_leave(Evict)` subtracts
        // the victim(s). Add before insert so concurrent readers never observe negative drift.
        let weight = (self.weighter)(&key, &value).max(1);
        self.state.used_bytes.fetch_add(weight, Relaxed);

        // Fresh vs. replace decides whether the entry count grows and whether we track the
        // key in the prefix index. `contains` is a cheap per-shard check.
        let fresh = self.inner.contains(&key) == false;
        if fresh {
            self.state.entries.fetch_add(1, Relaxed);
            if let Some(prefix) = (self.state.prefix_of)(&key) {
                // Scope the DashMap guard so it is dropped BEFORE `inner.insert` — that insert
                // may evict, firing `on_leave` which touches `by_prefix`; holding the guard
                // across it could deadlock on the same shard.
                self.state.by_prefix.entry(prefix).or_default().insert(key.clone());
            }
        }
        self.inner.insert(key, value);
    }

    /// Insert many entries (call-site parity with `BoundedCache::insert_batch`).
    pub fn insert_batch(&self, entries: impl IntoIterator<Item = (K, V)>) {
        for (k, v) in entries {
            self.insert(k, v);
        }
    }

    /// Membership check.
    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains(key)
    }

    /// Remove an entry if present (fires `on_leave(Remove)` → bookkeeping cleaned up there).
    pub fn remove(&self, key: &K) {
        self.inner.remove(key);
    }

    /// Evict every key recorded under `prefix` (a replaced/deleted segment). The side index
    /// entry is taken first, so the `on_leave(Remove)` callbacks triggered by `inner.remove`
    /// find nothing left to clean and cannot re-enter the prefix we are draining.
    pub fn evict_by_prefix(&self, prefix: &str) {
        if let Some((_, keys)) = self.state.by_prefix.remove(prefix) {
            for key in keys {
                self.inner.remove(&key);
            }
        }
    }

    /// Resize the byte cap at runtime (dynamic `datafusion.*.cache.size.limit`). foyer evicts
    /// down to the new capacity as needed.
    pub fn set_limit(&self, limit_bytes: usize) {
        if self.inner.resize(limit_bytes).is_ok() {
            self.limit_bytes.store(limit_bytes, Relaxed);
        }
    }

    /// Drop all entries; keeps the configured capacity. Resetting hit/miss is left to the caller.
    pub fn clear(&self) {
        self.inner.clear();
        // `on_leave(Clear)` fires per entry and decrements our counters, but foyer 0.22.3 does
        // not reliably zero its own per-shard usage on clear; force our counters to zero so a
        // post-clear `used_bytes`/`len` reports 0 regardless of foyer's internal drift.
        self.state.entries.store(0, Relaxed);
        self.state.used_bytes.store(0, Relaxed);
        self.state.by_prefix.clear();
    }

    /// Current byte usage (our own exact counter — NOT foyer's `usage()`, which is stale after
    /// `clear()` in 0.22.3; see [`CacheState`]).
    pub fn used_bytes(&self) -> usize {
        self.state.used_bytes.load(Relaxed)
    }

    /// Live entry count (advisory).
    pub fn len(&self) -> usize {
        self.state.entries.load(Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stats snapshot for the Java stats endpoint.
    pub fn stats(&self) -> FoyerCacheStats {
        FoyerCacheStats {
            hits: self.hits.load(Relaxed),
            misses: self.misses.load(Relaxed),
            entries: self.state.entries.load(Relaxed),
            used_bytes: self.state.used_bytes.load(Relaxed),
            limit_bytes: self.limit_bytes.load(Relaxed),
        }
    }

    /// Reset hit/miss counters (stats baselining for benchmarks).
    pub fn reset_stats(&self) {
        self.hits.store(0, Relaxed);
        self.misses.store(0, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn byte_cache(limit: usize, policy: CacheEvictionPolicy) -> FoyerBackedCache<String, Vec<u8>> {
        FoyerBackedCache::new(limit, policy, |_k, v: &Vec<u8>| v.len())
    }

    #[test]
    fn get_insert_hit_miss() {
        let c = byte_cache(1024, CacheEvictionPolicy::S3Fifo);
        assert!(c.get(&"a".to_string()).is_none());
        c.insert("a".to_string(), vec![1, 2, 3]);
        assert_eq!(c.get(&"a".to_string()), Some(vec![1, 2, 3]));
        let s = c.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.entries, 1);
        assert_eq!(s.used_bytes, 3);
    }

    #[test]
    fn replace_does_not_grow_entries() {
        let c = byte_cache(1024, CacheEvictionPolicy::Lru);
        c.insert("k".to_string(), vec![0; 10]);
        c.insert("k".to_string(), vec![0; 20]);
        assert_eq!(c.len(), 1);
        assert_eq!(c.get(&"k".to_string()), Some(vec![0; 20]));
    }

    #[test]
    fn byte_bound_evicts() {
        // Cap of 100 bytes; insert 30 entries of 10 bytes each → must evict down to ≤ cap.
        let c = byte_cache(100, CacheEvictionPolicy::S3Fifo);
        for i in 0..30 {
            c.insert(format!("k{i}"), vec![0u8; 10]);
        }
        assert!(c.used_bytes() <= 100, "used {} > cap", c.used_bytes());
    }

    #[test]
    fn evict_by_prefix_drops_segment_cells() {
        // prefix = everything before the last ':'
        let prefix_of: PrefixFn<String> =
            Arc::new(|k: &String| k.rsplit_once(':').map(|(p, _)| p.to_string()));
        let c: FoyerBackedCache<String, Vec<u8>> =
            FoyerBackedCache::with_prefix_fn(10_000, CacheEvictionPolicy::Fifo, |_k, v: &Vec<u8>| v.len(), prefix_of);
        c.insert("seg1:col0".to_string(), vec![0; 4]);
        c.insert("seg1:col1".to_string(), vec![0; 4]);
        c.insert("seg2:col0".to_string(), vec![0; 4]);
        assert_eq!(c.len(), 3);

        c.evict_by_prefix("seg1");
        assert!(c.get(&"seg1:col0".to_string()).is_none());
        assert!(c.get(&"seg1:col1".to_string()).is_none());
        assert_eq!(c.get(&"seg2:col0".to_string()), Some(vec![0; 4]));
    }

    #[test]
    fn clear_resets_entries() {
        let c = byte_cache(1024, CacheEvictionPolicy::Lfu);
        c.insert("a".to_string(), vec![1]);
        c.insert("b".to_string(), vec![2]);
        c.clear();
        assert_eq!(c.len(), 0);
        assert_eq!(c.used_bytes(), 0);
        assert!(c.get(&"a".to_string()).is_none());
    }

    #[test]
    fn set_limit_shrinks() {
        let c = byte_cache(1000, CacheEvictionPolicy::S3Fifo);
        for i in 0..50 {
            c.insert(format!("k{i}"), vec![0u8; 10]);
        }
        c.set_limit(100);
        assert!(c.used_bytes() <= 100);
        assert_eq!(c.stats().limit_bytes, 100);
    }
}
