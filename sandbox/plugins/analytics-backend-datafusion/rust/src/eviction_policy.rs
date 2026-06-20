/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! # Cache Policy Module
//!
//! Simple pluggable cache eviction policies for statistics cache.

use datafusion::common::instant;
use instant::Instant;
use thiserror::Error;

/// Error types for cache operations
#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Policy lock error: {reason}")]
    PolicyLockError { reason: String },
}

/// Result type for cache operations
pub type CacheResult<T> = Result<T, CacheError>;

/// Core trait for cache eviction policies
pub trait CachePolicy: Send + Sync {
    /// Called when a cache entry is accessed
    fn on_access(&mut self, key: &str, size: usize);

    /// Called when a cache entry is inserted
    fn on_insert(&mut self, key: &str, size: usize);
    /// Called when a cache entry is removed
    fn on_remove(&mut self, key: &str);

    /// Select entries for eviction to reach target size
    /// Returns keys to evict, ordered by eviction priority
    fn select_for_eviction(&self, target_size: usize) -> Vec<String>;

    /// Reset policy state
    fn clear(&mut self);

    /// Get the name of this policy
    fn policy_name(&self) -> &'static str;
}

/// Policy types
#[derive(Debug, Clone)]
pub enum PolicyType {
    Lru,
    Lfu,
    /// S3-FIFO (Yang et al., SOSP'23): scan-resistant, FIFO-based. New keys enter a
    /// small probationary queue; only keys re-accessed there are promoted to the main
    /// queue, so a one-off scan of cold keys is evicted out of the small queue without
    /// displacing the hot working set in main.
    S3Fifo,
}

/// Simple cache entry metadata
#[derive(Debug, Clone)]
pub struct CacheEntryMetadata {
    pub size: usize,
    pub last_accessed: Instant,
    pub access_count: usize,
}

impl CacheEntryMetadata {
    pub fn new(_key: String, size: usize) -> Self {
        Self {
            size,
            last_accessed: Instant::now(),
            access_count: 1,
        }
    }

    pub fn on_access(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }
}

/// LRU (Least Recently Used) policy
pub struct LruPolicy {
    entries: dashmap::DashMap<String, CacheEntryMetadata>,
    total_size: std::sync::atomic::AtomicUsize,
}

impl LruPolicy {
    pub fn new() -> Self {
        Self {
            entries: dashmap::DashMap::new(),
            total_size: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Default for LruPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for LruPolicy {
    fn on_access(&mut self, key: &str, size: usize) {
        match self.entries.get_mut(key) {
            Some(mut entry) => {
                entry.on_access();
            }
            None => {
                let metadata = CacheEntryMetadata::new(key.to_string(), size);
                self.entries.insert(key.to_string(), metadata);
                self.total_size
                    .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    fn on_insert(&mut self, key: &str, size: usize) {
        let metadata = CacheEntryMetadata::new(key.to_string(), size);

        if let Some(old_entry) = self.entries.insert(key.to_string(), metadata) {
            let old_size = old_entry.size;
            self.total_size
                .fetch_sub(old_size, std::sync::atomic::Ordering::Relaxed);
        }

        self.total_size
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
    }

    fn on_remove(&mut self, key: &str) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_size
                .fetch_sub(entry.size, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn select_for_eviction(&self, target_size: usize) -> Vec<String> {
        if target_size == 0 {
            return Vec::new();
        }

        // Collect entries with access times
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let last_accessed = entry.value().last_accessed;
                (key, last_accessed)
            })
            .collect();

        // Sort by access time (oldest first)
        entries.sort_by_key(|(_, last_accessed)| *last_accessed);

        // Select entries for eviction until target size is reached
        let mut candidates = Vec::new();
        let mut freed_size = 0;

        for (key, _) in entries {
            if freed_size >= target_size {
                break;
            }
            if let Some(entry) = self.entries.get(&key) {
                freed_size += entry.size;
                candidates.push(key);
            }
        }

        candidates
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.total_size
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    fn policy_name(&self) -> &'static str {
        "lru"
    }
}

/// LFU (Least Frequently Used) policy
pub struct LfuPolicy {
    entries: dashmap::DashMap<String, CacheEntryMetadata>,
    total_size: std::sync::atomic::AtomicUsize,
}

impl LfuPolicy {
    pub fn new() -> Self {
        Self {
            entries: dashmap::DashMap::new(),
            total_size: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Default for LfuPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for LfuPolicy {
    fn on_access(&mut self, key: &str, size: usize) {
        match self.entries.get_mut(key) {
            Some(mut entry) => {
                entry.on_access();
            }
            None => {
                let metadata = CacheEntryMetadata::new(key.to_string(), size);
                self.entries.insert(key.to_string(), metadata);
                self.total_size
                    .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    fn on_insert(&mut self, key: &str, size: usize) {
        let metadata = CacheEntryMetadata::new(key.to_string(), size);

        if let Some(old_entry) = self.entries.insert(key.to_string(), metadata) {
            let old_size = old_entry.size;
            self.total_size
                .fetch_sub(old_size, std::sync::atomic::Ordering::Relaxed);
        }

        self.total_size
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
    }

    fn on_remove(&mut self, key: &str) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_size
                .fetch_sub(entry.size, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn select_for_eviction(&self, target_size: usize) -> Vec<String> {
        if target_size == 0 {
            return Vec::new();
        }

        // Collect entries with access counts
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let access_count = entry.value().access_count;
                let last_accessed = entry.value().last_accessed;
                (key, access_count, last_accessed)
            })
            .collect();

        // Sort by access count (least frequent first), then by time for tie-breaking
        entries.sort_by(|(_, count_a, time_a), (_, count_b, time_b)| {
            count_a.cmp(count_b).then(time_a.cmp(time_b))
        });

        // Select entries for eviction until target size is reached
        let mut candidates = Vec::new();
        let mut freed_size = 0;

        for (key, _, _) in entries {
            if freed_size >= target_size {
                break;
            }
            if let Some(entry) = self.entries.get(&key) {
                freed_size += entry.size;
                candidates.push(key);
            }
        }

        candidates
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.total_size
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    fn policy_name(&self) -> &'static str {
        "lfu"
    }
}

/// S3-FIFO (Yang et al., SOSP'23) — scan-resistant eviction.
///
/// Three structures (here keyed by cache key string, sizes tracked in bytes):
/// - **small**  : probationary FIFO. Every new key lands here.
/// - **main**   : the hot set FIFO (proven reuse).
/// - **ghost**  : keys-only history of recently small-evicted keys (no sizes); a
///                re-insert whose key is in ghost goes straight to main.
///
/// Each live entry carries a 2-bit-style frequency (0..=3) bumped on access.
///
/// Scan resistance: a one-off scan touches each cold key once, so `freq` stays 0 in
/// `small`; on small-overflow such a key is evicted (to ghost) rather than promoted,
/// so it never enters `main` and cannot displace the hot working set. This mirrors
/// ClickHouse's SLRU intent but with FIFO queues (no per-access list reordering).
///
/// `select_for_eviction` is the trait's `&self` victim-picker; S3-FIFO mutates its
/// queues while choosing victims (demotion, ghost bookkeeping), so all mutable state
/// lives behind a `Mutex` for interior mutability. Lock poisoning is recovered rather
/// than propagated (`into_inner`): a panic in another thread can at worst leave the
/// byte accounting slightly stale, never unsafe, and a cache must degrade rather than
/// abort the process.
pub struct S3FifoPolicy {
    state: std::sync::Mutex<S3FifoState>,
    /// Fraction of total capacity assigned to the small/probationary queue. Defaults to
    /// 0.50 (SLRU-like 50/50 split); see `S3FifoPolicy::new`.
    small_ratio: f64,
}

struct S3FifoEntry {
    size: usize,
    freq: u8,
}

struct S3FifoState {
    /// key -> (size, freq) for keys resident in `small`.
    small: std::collections::HashMap<String, S3FifoEntry>,
    /// FIFO order of keys in `small`.
    small_order: std::collections::VecDeque<String>,
    small_bytes: usize,
    /// key -> (size, freq) for keys resident in `main`.
    main: std::collections::HashMap<String, S3FifoEntry>,
    /// FIFO order of keys in `main`.
    main_order: std::collections::VecDeque<String>,
    main_bytes: usize,
    /// Keys-only ghost history (recently evicted from small), FIFO-bounded.
    ghost: std::collections::HashSet<String>,
    ghost_order: std::collections::VecDeque<String>,
    /// Running total = small_bytes + main_bytes. Tracked to derive capacity targets
    /// without a configured limit (the cache enforces the byte cap; this policy only
    /// decides ordering/victims relative to the small/main split of current usage).
    total_bytes: usize,
}

/// Frequency saturates at 3 (2-bit counter, as in the S3-FIFO paper).
const S3FIFO_MAX_FREQ: u8 = 3;

impl S3FifoPolicy {
    /// Default small/probationary share. Set to 0.50 (rather than the S3-FIFO paper's
    /// ~10%) so the probationary vs. protected split mirrors a 50/50 SLRU — matching
    /// ClickHouse's default `*_cache_size_ratio` for its scan-resistant caches. A larger
    /// probationary segment tolerates more one-off churn before promoting to the
    /// protected `main` set.
    pub fn new() -> Self {
        Self::with_small_ratio(0.50)
    }

    pub fn with_small_ratio(small_ratio: f64) -> Self {
        Self {
            state: std::sync::Mutex::new(S3FifoState {
                small: std::collections::HashMap::new(),
                small_order: std::collections::VecDeque::new(),
                small_bytes: 0,
                main: std::collections::HashMap::new(),
                main_order: std::collections::VecDeque::new(),
                main_bytes: 0,
                ghost: std::collections::HashSet::new(),
                ghost_order: std::collections::VecDeque::new(),
                total_bytes: 0,
            }),
            small_ratio: small_ratio.clamp(0.01, 0.99),
        }
    }
}

impl Default for S3FifoPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl S3FifoState {
    /// Capacity budget for the small queue, derived as a fraction of current total
    /// usage. The cache owns the absolute byte limit and calls `select_for_eviction`
    /// with the number of bytes it needs freed; `small`'s share just controls how much
    /// probationary churn we tolerate before promoting/evicting.
    fn small_budget(&self, small_ratio: f64) -> usize {
        ((self.total_bytes as f64) * small_ratio) as usize
    }

    fn ghost_capacity(&self) -> usize {
        // Bound ghost to the number of keys main can hold, approximated by current
        // resident key count (paper bounds ghost to ~main's entry count).
        (self.small.len() + self.main.len()).max(16)
    }

    fn remember_ghost(&mut self, key: String) {
        if self.ghost.insert(key.clone()) {
            self.ghost_order.push_back(key);
            while self.ghost_order.len() > self.ghost_capacity() {
                if let Some(old) = self.ghost_order.pop_front() {
                    self.ghost.remove(&old);
                }
            }
        }
    }

    fn forget_key(&mut self, key: &str) {
        if let Some(e) = self.small.remove(key) {
            self.small_bytes -= e.size;
            self.total_bytes -= e.size;
            if let Some(pos) = self.small_order.iter().position(|k| k == key) {
                self.small_order.remove(pos);
            }
        } else if let Some(e) = self.main.remove(key) {
            self.main_bytes -= e.size;
            self.total_bytes -= e.size;
            if let Some(pos) = self.main_order.iter().position(|k| k == key) {
                self.main_order.remove(pos);
            }
        }
    }
}

impl CachePolicy for S3FifoPolicy {
    fn on_access(&mut self, key: &str, size: usize) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = s.small.get_mut(key) {
            e.freq = (e.freq + 1).min(S3FIFO_MAX_FREQ);
        } else if let Some(e) = s.main.get_mut(key) {
            e.freq = (e.freq + 1).min(S3FIFO_MAX_FREQ);
        } else {
            // Not resident yet — treat as an insert so the policy and cache stay in
            // sync even if the cache reports access before insert.
            drop(s);
            self.on_insert(key, size);
        }
    }

    fn on_insert(&mut self, key: &str, size: usize) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Re-insert of a still-resident key: refresh size, bump freq, done.
        if let Some(e) = s.small.get_mut(key) {
            let old = e.size;
            e.size = size;
            e.freq = (e.freq + 1).min(S3FIFO_MAX_FREQ);
            s.small_bytes = s.small_bytes - old + size;
            s.total_bytes = s.total_bytes - old + size;
            return;
        }
        if let Some(e) = s.main.get_mut(key) {
            let old = e.size;
            e.size = size;
            e.freq = (e.freq + 1).min(S3FIFO_MAX_FREQ);
            s.main_bytes = s.main_bytes - old + size;
            s.total_bytes = s.total_bytes - old + size;
            return;
        }
        // A key seen recently (in ghost) was popular enough to come back → main.
        if s.ghost.remove(key) {
            if let Some(pos) = s.ghost_order.iter().position(|k| k == key) {
                s.ghost_order.remove(pos);
            }
            s.main.insert(key.to_string(), S3FifoEntry { size, freq: 0 });
            s.main_order.push_back(key.to_string());
            s.main_bytes += size;
            s.total_bytes += size;
        } else {
            // Fresh key → small (probation).
            s.small.insert(key.to_string(), S3FifoEntry { size, freq: 0 });
            s.small_order.push_back(key.to_string());
            s.small_bytes += size;
            s.total_bytes += size;
        }
    }

    fn on_remove(&mut self, key: &str) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.forget_key(key);
    }

    fn select_for_eviction(&self, target_size: usize) -> Vec<String> {
        if target_size == 0 {
            return Vec::new();
        }
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut victims = Vec::new();
        let mut freed = 0usize;

        // Each outer round produces (at most) one evicted victim. Per S3-FIFO, the
        // small-vs-main choice is made once per round, then that queue is *fully
        // resolved*: promotions (small, freq>0) and CLOCK reinsertions (main, freq>0)
        // do not count as the round's eviction — we keep popping that queue until a
        // freq-0 entry is actually evicted. Re-deciding small-vs-main after a promotion
        // (the earlier bug) could evict a key we just promoted into main.
        //
        // Bounded so a round that only promotes/reinserts (no freq-0 victim yet) cannot
        // spin forever. A main entry may need up to S3FIFO_MAX_FREQ decrement passes
        // before it becomes evictable, so allow that many laps over the resident set.
        let max_rounds =
            (s.small_order.len() + s.main_order.len()) * (S3FIFO_MAX_FREQ as usize + 1) + 1;
        for _ in 0..max_rounds {
            if freed >= target_size {
                break;
            }
            if s.small_order.is_empty() && s.main_order.is_empty() {
                break;
            }
            // Drain `small` (probation) while it is over its byte budget — that is where
            // one-hit-wonders from a scan accumulate, so draining it protects the hot
            // `main` set. Otherwise take from `main`; fall back to whichever is non-empty.
            let small_over_budget = s.small_bytes > s.small_budget(self.small_ratio);
            let use_small = if s.small_order.is_empty() {
                false
            } else if s.main_order.is_empty() {
                true
            } else {
                small_over_budget
            };

            if use_small {
                // Pop small until we evict one freq-0 entry; promote freq>0 entries to
                // main. Bound the scan to the current queue length so a round of
                // all-freq>0 entries (all promoted, none evicted) cannot spin.
                let mut steps = s.small_order.len();
                while steps > 0 {
                    steps -= 1;
                    let Some(key) = s.small_order.pop_front() else { break };
                    let Some(entry) = s.small.remove(&key) else { continue };
                    if entry.freq > 0 {
                        s.small_bytes -= entry.size;
                        s.main_bytes += entry.size;
                        s.main.insert(key.clone(), S3FifoEntry { size: entry.size, freq: 0 });
                        s.main_order.push_back(key);
                    } else {
                        s.small_bytes -= entry.size;
                        s.total_bytes -= entry.size;
                        freed += entry.size;
                        s.remember_ghost(key.clone());
                        victims.push(key);
                        break;
                    }
                }
            } else {
                // Pop main until we evict one freq-0 entry; reinsert freq>0 (CLOCK lap,
                // freq decremented). Bound to the current queue length so a round where
                // every entry still has freq>0 makes one decrement pass and then stops
                // (the next round will find lower freqs), rather than spinning.
                let mut steps = s.main_order.len();
                while steps > 0 {
                    steps -= 1;
                    let Some(key) = s.main_order.pop_front() else { break };
                    let Some(entry) = s.main.remove(&key) else { continue };
                    if entry.freq > 0 {
                        s.main.insert(
                            key.clone(),
                            S3FifoEntry { size: entry.size, freq: entry.freq - 1 },
                        );
                        s.main_order.push_back(key);
                    } else {
                        s.main_bytes -= entry.size;
                        s.total_bytes -= entry.size;
                        freed += entry.size;
                        victims.push(key);
                        break;
                    }
                }
            }
        }
        victims
    }

    fn clear(&mut self) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.small.clear();
        s.small_order.clear();
        s.small_bytes = 0;
        s.main.clear();
        s.main_order.clear();
        s.main_bytes = 0;
        s.ghost.clear();
        s.ghost_order.clear();
        s.total_bytes = 0;
    }

    fn policy_name(&self) -> &'static str {
        "s3fifo"
    }
}

/// Create a cache policy instance
pub fn create_policy(policy_type: PolicyType) -> Box<dyn CachePolicy> {
    match policy_type {
        PolicyType::Lru => Box::new(LruPolicy::new()),
        PolicyType::Lfu => Box::new(LfuPolicy::new()),
        PolicyType::S3Fifo => Box::new(S3FifoPolicy::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_cache_entry_metadata() {
        let mut metadata = CacheEntryMetadata::new("test_key".to_string(), 1024);
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
        let lru_policy = create_policy(PolicyType::Lru);
        assert_eq!(lru_policy.policy_name(), "lru");

        let lfu_policy = create_policy(PolicyType::Lfu);
        assert_eq!(lfu_policy.policy_name(), "lfu");
    }

    #[test]
    fn test_lru_policy_basic_operations() {
        let mut policy = LruPolicy::new();
        assert_eq!(policy.policy_name(), "lru");

        policy.on_insert("key1", 100);
        policy.on_insert("key2", 200);
        policy.on_access("key1", 100);
        policy.on_remove("key1");
        policy.clear();
    }

    #[test]
    fn test_lru_policy_victim_selection() {
        let mut policy = LruPolicy::new();

        policy.on_insert("oldest", 100);
        thread::sleep(Duration::from_millis(1));

        policy.on_insert("middle", 100);
        thread::sleep(Duration::from_millis(1));

        policy.on_insert("newest", 100);
        thread::sleep(Duration::from_millis(1));

        // Access middle entry to make it more recent
        policy.on_access("middle", 100);

        let candidates = policy.select_for_eviction(150);
        assert_eq!(candidates.len(), 2);
        assert!(candidates.contains(&"oldest".to_string()));
        assert!(!candidates.contains(&"middle".to_string()));
    }

    #[test]
    fn test_s3fifo_policy_name_and_create() {
        let p = create_policy(PolicyType::S3Fifo);
        assert_eq!(p.policy_name(), "s3fifo");
    }

    #[test]
    fn test_s3fifo_scan_does_not_evict_hot_set() {
        // The core property: a one-off scan of cold keys must not evict a hot key.
        let mut p = S3FifoPolicy::new();

        // "hot" is inserted and re-accessed several times → earns frequency in small.
        p.on_insert("hot", 100);
        for _ in 0..3 {
            p.on_access("hot", 100);
        }

        // A scan streams many cold keys, each touched once.
        for i in 0..50 {
            p.on_insert(&format!("scan{i}"), 100);
        }

        // Evict ~half the bytes. Victims must be scan keys, never "hot".
        let victims = p.select_for_eviction(2500);
        assert!(!victims.is_empty(), "should evict something");
        assert!(
            !victims.contains(&"hot".to_string()),
            "hot key must survive a scan; victims={:?}",
            victims
        );
        assert!(
            victims.iter().all(|k| k.starts_with("scan")),
            "only scan one-hit-wonders should be evicted; victims={:?}",
            victims
        );
    }

    #[test]
    fn test_s3fifo_reaccessed_key_promoted_not_evicted() {
        // A key re-accessed while on probation (freq>0) is promoted to main on
        // eviction sweep rather than discarded.
        let mut p = S3FifoPolicy::new();
        p.on_insert("kept", 100);
        p.on_access("kept", 100); // freq -> 1
        p.on_insert("cold", 100); // freq 0

        let victims = p.select_for_eviction(100);
        assert_eq!(victims, vec!["cold".to_string()], "cold (freq 0) evicted, kept promoted");
    }

    #[test]
    fn test_s3fifo_ghost_reinsert_goes_to_main() {
        // A key evicted from small is remembered in ghost; a re-insert of that key is
        // admitted straight to main (it proved worth keeping) rather than re-entering
        // probation. Verify via placement, independent of the small/main budget split.
        let mut p = S3FifoPolicy::new();
        p.on_insert("k", 100);
        // Evict it (freq 0) → lands in ghost.
        let victims = p.select_for_eviction(100);
        assert_eq!(victims, vec!["k".to_string()]);

        // Re-insert the ghosted key and a brand-new key.
        p.on_insert("k", 100); // ghost hit → main
        p.on_insert("fresh", 100); // brand new → small

        // `k` is in main, `fresh` is in small. Confirm via the internal state: a fresh
        // key sits in probation, a ghost-readmitted key sits in the protected main set.
        {
            let s = p.state.lock().unwrap();
            assert!(s.main.contains_key("k"), "ghost-readmitted key must be in main");
            assert!(s.small.contains_key("fresh"), "brand-new key must be in small (probation)");
        }
    }

    #[test]
    fn test_s3fifo_clear_and_remove() {
        let mut p = S3FifoPolicy::new();
        p.on_insert("a", 100);
        p.on_insert("b", 200);
        p.on_remove("a");
        // After removing a, only b remains; evicting should yield b.
        let victims = p.select_for_eviction(200);
        assert_eq!(victims, vec!["b".to_string()]);
        p.on_insert("c", 100);
        p.clear();
        assert!(p.select_for_eviction(100).is_empty(), "cleared policy has no victims");
    }

    #[test]
    fn test_lfu_policy_victim_selection() {
        let mut policy = LfuPolicy::new();

        policy.on_insert("rarely_used", 100);
        policy.on_insert("sometimes_used", 100);
        policy.on_insert("frequently_used", 100);

        // Create frequency patterns
        policy.on_access("sometimes_used", 100);

        for _ in 0..3 {
            policy.on_access("frequently_used", 100);
        }

        let candidates = policy.select_for_eviction(150);
        assert_eq!(candidates.len(), 2);
        assert!(candidates.contains(&"rarely_used".to_string()));
        assert!(candidates.contains(&"sometimes_used".to_string()));
        assert!(!candidates.contains(&"frequently_used".to_string()));
    }
}
