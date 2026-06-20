/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Byte-bounded cache with a pluggable eviction policy, used by the two
//! scoped page-index caches ([`COLUMN_INDEX_CACHE`] / [`OFFSET_INDEX_CACHE`]
//! defined in `mod.rs`).

use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;

use dashmap::DashMap;

use crate::cache::eviction_policy::{CachePolicy, PolicyType, create_policy};

/// Default byte budget for EACH scoped cache, used until the caller sets one from
/// the runtime's configured limit (see [`set_column_index_cache_limit`] /
/// [`set_offset_index_cache_limit`]). The two caches are budgeted independently:
/// the ColumnIndex (per-page string min/max) is the heavy one and the OffsetIndex
/// (fixed-width page offsets) is tiny, so they get separate, separately-tunable
/// limits rather than sharing one number.
///
/// TODO : configure via settings
pub(crate) const DEFAULT_SCOPED_CACHE_LIMIT: usize = 150 * 1024 * 1024;

/// Snapshot of one scoped cache's counters plus occupancy. Surfaced on
/// node-stats and used by tests to assert hits/misses without `Arc::ptr_eq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScopedCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub used_bytes: usize,
    pub limit_bytes: usize,
}

/// Byte-bounded cache with a pluggable eviction policy.
///
/// `DashMap` shards the value store so concurrent `get` calls on different keys
/// never contend. The eviction policy (ordering metadata + victim selection) sits
/// behind a `Mutex` — it is only touched on `insert` and `set_limit`, not on the
/// read path. Counters are atomics so `stats()` is always lock-free.
pub(super) struct BoundedCache<K, V>
where
    K: Eq + Hash + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Primary value store — concurrent reads with no global lock.
    map: DashMap<K, (V, usize)>,
    /// Reverse map: policy string key → typed cache key, needed to resolve
    /// eviction candidates (the policy works with `String` keys).
    reverse: DashMap<String, K>,
    /// Eviction policy — behind a Mutex since mutation (on_insert/select_for_eviction)
    /// is not concurrent-safe. Not held during reads.
    policy: Mutex<Box<dyn CachePolicy>>,
    limit: AtomicUsize,
    // lock-free counters
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    used_bytes: AtomicUsize,
}

impl<K, V> BoundedCache<K, V>
where
    K: Eq + Hash + Clone + Display + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(super) fn new(limit: usize, policy_type: PolicyType) -> Self {
        Self {
            map: DashMap::new(),
            reverse: DashMap::new(),
            policy: Mutex::new(create_policy(policy_type)),
            limit: AtomicUsize::new(limit),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            used_bytes: AtomicUsize::new(0),
        }
    }

    pub(super) fn get(&self, key: &K) -> Option<V> {
        match self.map.get(key) {
            Some(entry) => {
                let size = entry.1;
                if let Ok(mut p) = self.policy.lock() {
                    p.on_access(&key.to_string(), size);
                }
                self.hits.fetch_add(1, Relaxed);
                Some(entry.0.clone())
            }
            None => {
                self.misses.fetch_add(1, Relaxed);
                None
            }
        }
    }

    pub(super) fn insert(&self, key: K, value: V, size: usize) {
        let limit = self.limit.load(Relaxed);
        if size > limit {
            return;
        }
        let key_str = key.to_string();
        if let Some(old) = self.map.insert(key.clone(), (value, size)) {
            self.used_bytes.fetch_sub(old.1, Relaxed);
        }
        self.reverse.insert(key_str.clone(), key);
        self.used_bytes.fetch_add(size, Relaxed);
        if let Ok(mut p) = self.policy.lock() {
            p.on_insert(&key_str, size);
        }
        self.evict();
    }

    fn evict(&self) {
        let limit = self.limit.load(Relaxed);
        let used = self.used_bytes.load(Relaxed);
        if used <= limit {
            return;
        }
        let candidates = if let Ok(p) = self.policy.lock() {
            p.select_for_eviction(used - limit)
        } else {
            return;
        };
        for key_str in candidates {
            if let Some((_, typed_key)) = self.reverse.remove(&key_str) {
                if let Some((_, (_, size))) = self.map.remove(&typed_key) {
                    self.used_bytes.fetch_sub(size, Relaxed);
                    self.evictions.fetch_add(1, Relaxed);
                    if let Ok(mut p) = self.policy.lock() {
                        p.on_remove(&key_str);
                    }
                }
            }
        }
    }

    pub(super) fn set_limit(&self, limit: usize) {
        self.limit.store(limit, Relaxed);
        self.evict();
    }

    pub(super) fn clear_keep_limit(&self) {
        self.map.clear();
        self.reverse.clear();
        if let Ok(mut p) = self.policy.lock() {
            p.clear();
        }
        self.hits.store(0, Relaxed);
        self.misses.store(0, Relaxed);
        self.evictions.store(0, Relaxed);
        self.used_bytes.store(0, Relaxed);
    }

    /// Remove all entries whose string key starts with `prefix` (used to evict
    /// all cells for a given file path when the file is deleted/replaced).
    pub(super) fn evict_by_prefix(&self, prefix: &str) {
        let victims: Vec<String> = self.reverse
            .iter()
            .filter(|e| e.key().starts_with(prefix))
            .map(|e| e.key().clone())
            .collect();
        for key_str in victims {
            if let Some((_, typed_key)) = self.reverse.remove(&key_str) {
                if let Some((_, (_, size))) = self.map.remove(&typed_key) {
                    self.used_bytes.fetch_sub(size, Relaxed);
                    self.evictions.fetch_add(1, Relaxed);
                    if let Ok(mut p) = self.policy.lock() {
                        p.on_remove(&key_str);
                    }
                }
            }
        }
    }

    pub(super) fn stats(&self) -> ScopedCacheStats {
        ScopedCacheStats {
            hits: self.hits.load(Relaxed),
            misses: self.misses.load(Relaxed),
            evictions: self.evictions.load(Relaxed),
            entries: self.map.len(),
            used_bytes: self.used_bytes.load(Relaxed),
            limit_bytes: self.limit.load(Relaxed),
        }
    }
}
