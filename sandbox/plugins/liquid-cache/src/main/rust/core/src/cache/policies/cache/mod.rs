//! Cache policies for liquid cache.

use crate::cache::cached_batch::CachedBatchType;
use crate::cache::utils::EntryID;

mod doubly_linked_list;
mod lru;
mod three_queue;

pub use lru::LruPolicy;
pub use three_queue::LiquidPolicy;

/// The cache policy that guides the replacement of LiquidCache
pub trait CachePolicy: std::fmt::Debug + Send + Sync {
    /// Give cnt amount of entries to evict when cache is full.
    fn find_memory_victim(&self, cnt: usize) -> Vec<EntryID>;

    /// Notify the cache policy that an entry was inserted.
    fn notify_insert(&self, _entry_id: &EntryID, _batch_type: CachedBatchType) {}

    /// Notify the cache policy that an entry was accessed.
    fn notify_access(&self, _entry_id: &EntryID, _batch_type: CachedBatchType) {}

    /// Notify the cache policy that an entry was removed.
    fn notify_remove(&self, _entry_id: &EntryID) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::utils::EntryID;
    use crate::sync::{Arc, Mutex, thread};

    fn entry(id: usize) -> EntryID {
        id.into()
    }

    fn concurrent_invariant_advice_once(policy: Arc<dyn CachePolicy>) {
        let num_threads = 4;

        for i in 0..100 {
            policy.notify_insert(&entry(i), CachedBatchType::MemoryArrow);
        }

        let advised_entries = Arc::new(Mutex::new(Vec::new()));

        let mut handles = Vec::new();
        for _ in 0..num_threads {
            let policy_clone = policy.clone();
            let advised_entries_clone = advised_entries.clone();

            let handle = thread::spawn(move || {
                let advice = policy_clone.find_memory_victim(1);
                if let Some(entry_id) = advice.first() {
                    let mut entries = advised_entries_clone.lock().unwrap();
                    entries.push(*entry_id);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let entries = advised_entries.lock().unwrap();
        let mut unique_entries = entries.clone();
        unique_entries.sort();
        unique_entries.dedup();

        assert_eq!(
            entries.len(),
            unique_entries.len(),
            "Some entries were advised for eviction multiple times: {entries:?}"
        );
    }

    fn run_concurrent_invariant_tests() {
        concurrent_invariant_advice_once(Arc::new(LiquidPolicy::new()));
        concurrent_invariant_advice_once(Arc::new(super::LruPolicy::new()));
    }

    #[test]
    fn test_concurrent_invariant_advice_once() {
        run_concurrent_invariant_tests();
    }

    #[test]
    fn lru_evicts_least_recently_used() {
        let policy = LruPolicy::new();

        // Insert entries 0, 1, 2, 3 in order
        for i in 0..4 {
            policy.notify_insert(&entry(i), CachedBatchType::MemoryArrow);
        }

        // Evict 1 → should be entry 0 (oldest)
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(0)]);

        // Evict 1 more → should be entry 1
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(1)]);
    }

    #[test]
    fn lru_access_moves_to_back() {
        let policy = LruPolicy::new();

        // Insert 0, 1, 2
        for i in 0..3 {
            policy.notify_insert(&entry(i), CachedBatchType::MemoryArrow);
        }

        // Access entry 0 → moves it to back
        policy.notify_access(&entry(0), CachedBatchType::MemoryArrow);

        // Evict → should be entry 1 now (0 was moved to back)
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(1)]);

        // Evict → entry 2
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(2)]);

        // Evict → entry 0 (was moved to back by access)
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(0)]);
    }

    #[test]
    fn lru_remove_removes_entry() {
        let policy = LruPolicy::new();

        for i in 0..3 {
            policy.notify_insert(&entry(i), CachedBatchType::MemoryArrow);
        }

        // Remove entry 1
        policy.notify_remove(&entry(1));

        // Evict → entry 0, then entry 2 (entry 1 is gone)
        let victims = policy.find_memory_victim(2);
        assert_eq!(victims, vec![entry(0), entry(2)]);
    }

    #[test]
    fn lru_evict_more_than_available() {
        let policy = LruPolicy::new();

        policy.notify_insert(&entry(0), CachedBatchType::MemoryArrow);
        policy.notify_insert(&entry(1), CachedBatchType::MemoryArrow);

        // Ask for 5 but only 2 exist
        let victims = policy.find_memory_victim(5);
        assert_eq!(victims.len(), 2);
        assert_eq!(victims, vec![entry(0), entry(1)]);

        // Empty now
        let victims = policy.find_memory_victim(1);
        assert!(victims.is_empty());
    }

    #[test]
    fn lru_reinsert_updates_position() {
        let policy = LruPolicy::new();

        for i in 0..3 {
            policy.notify_insert(&entry(i), CachedBatchType::MemoryArrow);
        }

        // Re-insert entry 0 → should move to back
        policy.notify_insert(&entry(0), CachedBatchType::MemoryLiquid);

        // Evict → entry 1 (oldest now)
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(1)]);

        // Evict → entry 2
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(2)]);

        // Evict → entry 0 (re-inserted last)
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(0)]);
    }
}
