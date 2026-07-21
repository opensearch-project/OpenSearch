/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! # Cache Policy Module
//!
//! One pluggable eviction-policy trait shared by every cache in this crate.
//!
//! [`EvictionPolicy<K>`] is generic over the cache's key type, so the same
//! trait serves the `String`-keyed statistics cache and the typed-key
//! (`CiCellKey` / `OiCellKey`) page-index caches. Adding a new policy
//! (e.g. S3-FIFO) means implementing this one trait and adding a
//! [`CacheEvictionPolicy`] variant — no cache implementation changes.

use datafusion::common::instant;
use instant::Instant;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::hash::Hash;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use thiserror::Error;

/// Error types for cache operations
#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Policy lock error: {reason}")]
    PolicyLockError { reason: String },
}

/// Result type for cache operations
pub type CacheResult<T> = Result<T, CacheError>;

/// Unified eviction policy selector for all caches in this crate.
///
/// One enum for all caches means `df_create_cache` parses the Java-supplied
/// eviction string once and routes uniformly. Which policies are valid for
/// which cache is enforced in one place: [`crate::cache::CacheKind::validate_policy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheEvictionPolicy {
    Lru,
    Lfu,
    Fifo,
}

impl CacheEvictionPolicy {
    /// Parse the Java-supplied eviction string ("LRU"/"LFU"/"FIFO", any case).
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_uppercase().as_str() {
            "LRU" => Ok(CacheEvictionPolicy::Lru),
            "LFU" => Ok(CacheEvictionPolicy::Lfu),
            "FIFO" => Ok(CacheEvictionPolicy::Fifo),
            _ => Err(format!("unsupported eviction type: {}", s)),
        }
    }
}

/// Backward-compatible alias — will be removed once all call sites are migrated.
pub type PolicyType = CacheEvictionPolicy;

/// Core trait for cache eviction policies, generic over the cache key type.
///
/// All methods take `&self` — implementations use interior mutability
/// (`DashMap`, `Mutex`, atomics) so the trait object can be shared as
/// `Arc<dyn EvictionPolicy<K>>` without an outer lock.
///
/// # Eviction protocol
///
/// The owning cache drives eviction by calling [`Self::next_victim`] in a loop
/// until it has freed enough bytes. `next_victim` **pops** the candidate from
/// the policy's bookkeeping; the cache then removes it from its own store and
/// must NOT call `on_remove` for that key (the policy already forgot it).
/// Caches that tolerate stale policy entries (lazy deletion on overwrite, see
/// `BoundedCache`) simply skip victims that are no longer present and call
/// `next_victim` again.
///
/// # Implementing a new policy (e.g. S3-FIFO)
///
/// 1. Implement this trait with interior mutability.
/// 2. Add a [`CacheEvictionPolicy`] variant and wire it in [`create_policy`]
///    (the exhaustive match makes the compiler flag the missing arm).
/// 3. Allow it for the target caches in `CacheKind::validate_policy`.
pub trait EvictionPolicy<K>: Send + Sync {
    /// Called when a cache entry is accessed (updates recency/frequency
    /// metadata). Policies that don't track access (FIFO) treat this as a
    /// no-op so the cache read path stays lock-free.
    fn on_access(&self, key: &K, size: usize);

    /// Called when a cache entry is inserted (or an existing key overwritten).
    fn on_insert(&self, key: &K, size: usize);

    /// Called when an entry is explicitly removed (file eviction, clear).
    /// NOT called for victims returned by [`Self::next_victim`].
    fn on_remove(&self, key: &K);

    /// Pop and return the next eviction candidate (lowest-priority entry),
    /// removing it from the policy's bookkeeping. Returns `None` when the
    /// policy has no more candidates.
    fn next_victim(&self) -> Option<K>;

    /// Reset all policy state (called on cache clear).
    fn clear(&self);

    /// Name of this policy, for logging/stats.
    fn policy_name(&self) -> &'static str;
}

/// Metadata tracked per cached entry for eviction ordering.
/// Stored inside the policy's `DashMap`, mutated via `get_mut`.
#[derive(Debug, Clone)]
pub struct CacheEntryMetadata {
    pub size: usize,
    pub last_accessed: Instant,
    pub access_count: usize,
}

impl CacheEntryMetadata {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            last_accessed: Instant::now(),
            access_count: 1,
        }
    }

    /// Update recency and frequency. Called via `DashMap::get_mut` which holds
    /// only the per-shard write lock — not a global lock.
    pub fn on_access(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }
}

/// LRU (Least Recently Used) policy
pub struct LruPolicy<K: Eq + Hash> {
    entries: dashmap::DashMap<K, CacheEntryMetadata>,
    total_size: std::sync::atomic::AtomicUsize,
}

impl<K: Eq + Hash> LruPolicy<K> {
    pub fn new() -> Self {
        Self {
            entries: dashmap::DashMap::new(),
            total_size: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl<K: Eq + Hash> Default for LruPolicy<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone + Send + Sync> EvictionPolicy<K> for LruPolicy<K> {
    fn on_access(&self, key: &K, size: usize) {
        match self.entries.get_mut(key) {
            Some(mut entry) => {
                entry.on_access();
            }
            None => {
                // Entry missing from policy metadata (can happen transiently) — insert it.
                self.entries
                    .insert(key.clone(), CacheEntryMetadata::new(size));
                self.total_size.fetch_add(size, Ordering::Relaxed);
            }
        }
    }

    fn on_insert(&self, key: &K, size: usize) {
        if let Some(old_entry) = self
            .entries
            .insert(key.clone(), CacheEntryMetadata::new(size))
        {
            self.total_size.fetch_sub(old_entry.size, Ordering::Relaxed);
        }
        self.total_size.fetch_add(size, Ordering::Relaxed);
    }

    fn on_remove(&self, key: &K) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
        }
    }

    fn next_victim(&self) -> Option<K> {
        // Least recently accessed entry. O(n) scan per victim — same asymptotic
        // work as the previous batch-sort selection, spread across calls.
        let victim = self
            .entries
            .iter()
            .min_by_key(|entry| entry.value().last_accessed)
            .map(|entry| entry.key().clone())?;
        if let Some((_, entry)) = self.entries.remove(&victim) {
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
        }
        Some(victim)
    }

    fn clear(&self) {
        self.entries.clear();
        self.total_size.store(0, Ordering::Relaxed);
    }

    fn policy_name(&self) -> &'static str {
        "lru"
    }
}

/// LFU (Least Frequently Used) policy
pub struct LfuPolicy<K: Eq + Hash> {
    entries: dashmap::DashMap<K, CacheEntryMetadata>,
    total_size: std::sync::atomic::AtomicUsize,
}

impl<K: Eq + Hash> LfuPolicy<K> {
    pub fn new() -> Self {
        Self {
            entries: dashmap::DashMap::new(),
            total_size: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl<K: Eq + Hash> Default for LfuPolicy<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone + Send + Sync> EvictionPolicy<K> for LfuPolicy<K> {
    fn on_access(&self, key: &K, size: usize) {
        match self.entries.get_mut(key) {
            Some(mut entry) => {
                entry.on_access();
            }
            None => {
                self.entries
                    .insert(key.clone(), CacheEntryMetadata::new(size));
                self.total_size.fetch_add(size, Ordering::Relaxed);
            }
        }
    }

    fn on_insert(&self, key: &K, size: usize) {
        if let Some(old_entry) = self
            .entries
            .insert(key.clone(), CacheEntryMetadata::new(size))
        {
            self.total_size.fetch_sub(old_entry.size, Ordering::Relaxed);
        }
        self.total_size.fetch_add(size, Ordering::Relaxed);
    }

    fn on_remove(&self, key: &K) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
        }
    }

    fn next_victim(&self) -> Option<K> {
        // Least frequently accessed entry; recency breaks ties (same ordering
        // as the previous batch-sort selection).
        let victim = self
            .entries
            .iter()
            .min_by(|a, b| {
                let (av, bv) = (a.value(), b.value());
                av.access_count
                    .cmp(&bv.access_count)
                    .then(av.last_accessed.cmp(&bv.last_accessed))
            })
            .map(|entry| entry.key().clone())?;
        if let Some((_, entry)) = self.entries.remove(&victim) {
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);
        }
        Some(victim)
    }

    fn clear(&self) {
        self.entries.clear();
        self.total_size.store(0, Ordering::Relaxed);
    }

    fn policy_name(&self) -> &'static str {
        "lfu"
    }
}

/// Insert-order (FIFO) eviction policy.
///
/// Oldest-inserted entry is evicted first. Access order is not tracked —
/// `on_access` is a no-op, so cache reads never touch the queue. Stale entries
/// (from overwrites) are left in the queue and skipped by the owning cache's
/// eviction loop (lazy deletion; see `BoundedCache::drain_to_limit`).
pub struct FifoPolicy<K> {
    /// Uncontended in practice: all mutating policy calls happen inside the
    /// owning cache's write path.
    queue: Mutex<VecDeque<K>>,
}

impl<K> FifoPolicy<K> {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
        }
    }
}

impl<K> Default for FifoPolicy<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Clone + PartialEq + Send + Sync> EvictionPolicy<K> for FifoPolicy<K> {
    fn on_access(&self, _key: &K, _size: usize) {
        // FIFO does not track access order — reads stay lock-free.
    }

    fn on_insert(&self, key: &K, _size: usize) {
        self.queue.lock().push_back(key.clone());
    }

    fn on_remove(&self, key: &K) {
        // O(n) scan — only called from evict_by_prefix (cold path).
        self.queue.lock().retain(|k| k != key);
    }

    fn next_victim(&self) -> Option<K> {
        self.queue.lock().pop_front()
    }

    fn clear(&self) {
        self.queue.lock().clear();
    }

    fn policy_name(&self) -> &'static str {
        "fifo"
    }
}

/// Create an [`EvictionPolicy`] from a [`CacheEvictionPolicy`] selector.
///
/// Total over the enum: every cache accepts every policy object; which
/// policies are *allowed* per cache type is validated separately in
/// `CacheKind::validate_policy` (so the rule lives in one place).
///
/// When a new variant is added to `CacheEvictionPolicy`, the compiler will
/// flag the missing arm here.
pub fn create_policy<K>(policy_type: CacheEvictionPolicy) -> Arc<dyn EvictionPolicy<K>>
where
    K: Eq + Hash + Clone + PartialEq + Send + Sync + 'static,
{
    match policy_type {
        CacheEvictionPolicy::Lru => Arc::new(LruPolicy::new()),
        CacheEvictionPolicy::Lfu => Arc::new(LfuPolicy::new()),
        CacheEvictionPolicy::Fifo => Arc::new(FifoPolicy::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_cache_entry_metadata() {
        let mut metadata = CacheEntryMetadata::new(1024);
        assert_eq!(metadata.size, 1024);
        assert_eq!(metadata.access_count, 1);

        let initial_access_time = metadata.last_accessed;
        thread::sleep(Duration::from_millis(1));

        metadata.on_access();
        assert_eq!(metadata.access_count, 2);
        assert!(metadata.last_accessed > initial_access_time);
    }

    #[test]
    fn test_create_policy() {
        let lru_policy = create_policy::<String>(PolicyType::Lru);
        assert_eq!(lru_policy.policy_name(), "lru");

        let lfu_policy = create_policy::<String>(PolicyType::Lfu);
        assert_eq!(lfu_policy.policy_name(), "lfu");

        let fifo_policy = create_policy::<String>(PolicyType::Fifo);
        assert_eq!(fifo_policy.policy_name(), "fifo");
    }

    #[test]
    fn test_parse_eviction_policy() {
        assert_eq!(
            CacheEvictionPolicy::parse("lru").unwrap(),
            CacheEvictionPolicy::Lru
        );
        assert_eq!(
            CacheEvictionPolicy::parse("LFU").unwrap(),
            CacheEvictionPolicy::Lfu
        );
        assert_eq!(
            CacheEvictionPolicy::parse("Fifo").unwrap(),
            CacheEvictionPolicy::Fifo
        );
        assert!(CacheEvictionPolicy::parse("clock").is_err());
    }

    #[test]
    fn test_lru_policy_basic_operations() {
        let policy: LruPolicy<String> = LruPolicy::new();
        assert_eq!(EvictionPolicy::policy_name(&policy), "lru");

        policy.on_insert(&"key1".to_string(), 100);
        policy.on_insert(&"key2".to_string(), 200);
        policy.on_access(&"key1".to_string(), 100);
        policy.on_remove(&"key1".to_string());
        EvictionPolicy::clear(&policy);
    }

    /// Drain victims until at least `target_size` bytes would be freed —
    /// mirrors how the owning caches drive `next_victim`.
    fn drain_for<K: Clone>(
        policy: &dyn EvictionPolicy<K>,
        sizes: &dyn Fn(&K) -> usize,
        target_size: usize,
    ) -> Vec<K> {
        let mut freed = 0;
        let mut victims = Vec::new();
        while freed < target_size {
            let Some(v) = policy.next_victim() else { break };
            freed += sizes(&v);
            victims.push(v);
        }
        victims
    }

    #[test]
    fn test_lru_policy_victim_selection() {
        let policy: LruPolicy<String> = LruPolicy::new();

        policy.on_insert(&"oldest".to_string(), 100);
        thread::sleep(Duration::from_millis(1));

        policy.on_insert(&"middle".to_string(), 100);
        thread::sleep(Duration::from_millis(1));

        policy.on_insert(&"newest".to_string(), 100);
        thread::sleep(Duration::from_millis(1));

        // Access middle entry to make it more recent
        policy.on_access(&"middle".to_string(), 100);

        let candidates = drain_for(&policy, &|_| 100, 150);
        assert_eq!(candidates.len(), 2);
        assert!(candidates.contains(&"oldest".to_string()));
        assert!(!candidates.contains(&"middle".to_string()));
    }

    #[test]
    fn test_lfu_policy_victim_selection() {
        let policy: LfuPolicy<String> = LfuPolicy::new();

        policy.on_insert(&"rarely_used".to_string(), 100);
        policy.on_insert(&"sometimes_used".to_string(), 100);
        policy.on_insert(&"frequently_used".to_string(), 100);

        // Create frequency patterns
        policy.on_access(&"sometimes_used".to_string(), 100);

        for _ in 0..3 {
            policy.on_access(&"frequently_used".to_string(), 100);
        }

        let candidates = drain_for(&policy, &|_| 100, 150);
        assert_eq!(candidates.len(), 2);
        assert!(candidates.contains(&"rarely_used".to_string()));
        assert!(candidates.contains(&"sometimes_used".to_string()));
        assert!(!candidates.contains(&"frequently_used".to_string()));
    }

    #[test]
    fn test_fifo_policy_victim_order() {
        let policy: FifoPolicy<String> = FifoPolicy::new();
        policy.on_insert(&"first".to_string(), 10);
        policy.on_insert(&"second".to_string(), 10);
        policy.on_access(&"first".to_string(), 10); // no-op for FIFO
        assert_eq!(policy.next_victim(), Some("first".to_string()));
        assert_eq!(policy.next_victim(), Some("second".to_string()));
        assert_eq!(policy.next_victim(), None);
    }
}
