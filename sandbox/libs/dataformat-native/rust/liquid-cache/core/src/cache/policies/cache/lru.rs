//! LRU (Least Recently Used) cache eviction policy.
//!
//! Uses 2 queues by entry type (Arrow, Liquid), same as LiquidPolicy.
//! Within each queue, entries are ordered by recency (moved to back on access).
//! Eviction priority: Arrow (largest) first, then Liquid.
//! Within each queue, evicts the LRU entry (front of the queue).

use crate::cache::cached_batch::CachedBatchType;
use crate::cache::utils::EntryID;
use crate::sync::Mutex;
use ahash::AHashMap;
use std::ptr::NonNull;

use super::CachePolicy;
use super::doubly_linked_list::{DoublyLinkedList, DoublyLinkedNode, drop_boxed_node};

/// Which queue an entry belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LruQueueKind {
    Arrow,
    Liquid,
}

/// LRU cache eviction policy with type-aware queues.
///
/// On insert: entry goes to the back of its type queue (most recently used).
/// On access: entry moves to the back of its type queue.
/// On eviction: picks the LRU entry from Arrow queue first, then Liquid.
#[derive(Debug)]
pub struct LruPolicy {
    inner: Mutex<LruInner>,
}

#[derive(Debug)]
struct LruInner {
    arrow: DoublyLinkedList<EntryID>,
    liquid: DoublyLinkedList<EntryID>,
    /// Maps entry_id → (node_ptr, which queue it's in)
    map: AHashMap<EntryID, (NonNull<DoublyLinkedNode<EntryID>>, LruQueueKind)>,
}

// Safety: We control access via Mutex and never expose raw pointers outside.
unsafe impl Send for LruInner {}
unsafe impl Sync for LruInner {}

impl LruInner {
    fn queue_mut(&mut self, kind: LruQueueKind) -> &mut DoublyLinkedList<EntryID> {
        match kind {
            LruQueueKind::Arrow => &mut self.arrow,
            LruQueueKind::Liquid => &mut self.liquid,
        }
    }

    fn queue_kind_for(batch_type: CachedBatchType) -> LruQueueKind {
        match batch_type {
            CachedBatchType::MemoryArrow => LruQueueKind::Arrow,
            CachedBatchType::MemoryLiquid => LruQueueKind::Liquid,
        }
    }

    /// Pop the LRU entry (front) from a specific queue.
    fn pop_lru(&mut self, kind: LruQueueKind) -> Option<EntryID> {
        let list = self.queue_mut(kind);
        let node_ptr = list.head()?;
        let entry_id = unsafe { node_ptr.as_ref().data };
        unsafe { list.unlink(node_ptr) };
        self.map.remove(&entry_id);
        unsafe { drop_boxed_node(node_ptr) };
        Some(entry_id)
    }
}

impl LruPolicy {
    /// Create a new LRU policy.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(LruInner {
                arrow: DoublyLinkedList::new(),
                liquid: DoublyLinkedList::new(),
                map: AHashMap::new(),
            }),
        }
    }
}

impl Default for LruPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for LruPolicy {
    fn find_memory_victim(&self, cnt: usize) -> Vec<EntryID> {
        let mut inner = self.inner.lock().unwrap();
        let mut victims = Vec::with_capacity(cnt);

        while victims.len() < cnt {
            // Priority: evict Arrow (largest) first, then Liquid
            if let Some(entry) = inner.pop_lru(LruQueueKind::Arrow) {
                victims.push(entry);
                continue;
            }
            if let Some(entry) = inner.pop_lru(LruQueueKind::Liquid) {
                victims.push(entry);
                continue;
            }
            break;
        }

        victims
    }

    fn notify_insert(&self, entry_id: &EntryID, batch_type: CachedBatchType) {
        let mut inner = self.inner.lock().unwrap();
        let target = LruInner::queue_kind_for(batch_type);

        // If already present, remove from old position/queue
        if let Some((node_ptr, old_kind)) = inner.map.remove(entry_id) {
            let old_list = inner.queue_mut(old_kind);
            unsafe { old_list.unlink(node_ptr) };
            unsafe { drop_boxed_node(node_ptr) };
        }

        // Insert at the back of the target queue (most recently used)
        let node = DoublyLinkedNode::new(*entry_id);
        let node_ptr = NonNull::new(Box::into_raw(node)).unwrap();
        let list = inner.queue_mut(target);
        unsafe { list.push_back(node_ptr) };
        inner.map.insert(*entry_id, (node_ptr, target));
    }

    fn notify_access(&self, entry_id: &EntryID, _batch_type: CachedBatchType) {
        let mut inner = self.inner.lock().unwrap();

        let Some(&(node_ptr, kind)) = inner.map.get(entry_id) else {
            return;
        };

        // Move to back of its current queue (most recently used)
        let list = inner.queue_mut(kind);
        unsafe {
            list.unlink(node_ptr);
            list.push_back(node_ptr);
        }
    }

    fn notify_remove(&self, entry_id: &EntryID) {
        let mut inner = self.inner.lock().unwrap();

        if let Some((node_ptr, kind)) = inner.map.remove(entry_id) {
            let list = inner.queue_mut(kind);
            unsafe { list.unlink(node_ptr) };
            unsafe { drop_boxed_node(node_ptr) };
        }
    }
}

impl Drop for LruPolicy {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        unsafe {
            inner.arrow.drop_all();
            inner.liquid.drop_all();
        }
        inner.map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: usize) -> EntryID {
        EntryID::from(id)
    }

    #[test]
    fn test_lru_evicts_arrow_first() {
        let policy = LruPolicy::new();

        // Insert entries: some Arrow, some Liquid
        policy.notify_insert(&entry(1), CachedBatchType::MemoryArrow);
        policy.notify_insert(&entry(2), CachedBatchType::MemoryLiquid);
        policy.notify_insert(&entry(3), CachedBatchType::MemoryArrow);
        policy.notify_insert(&entry(4), CachedBatchType::MemoryLiquid);

        // Evict 1 — should pick from Arrow queue first (LRU = entry 1)
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(1)]);

        // Evict 1 more — next Arrow LRU = entry 3
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(3)]);

        // Evict 1 more — Arrow empty, now Liquid LRU = entry 2
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(2)]);
    }

    #[test]
    fn test_lru_access_moves_to_back() {
        let policy = LruPolicy::new();

        policy.notify_insert(&entry(1), CachedBatchType::MemoryArrow);
        policy.notify_insert(&entry(2), CachedBatchType::MemoryArrow);
        policy.notify_insert(&entry(3), CachedBatchType::MemoryArrow);

        // Access entry 1 — moves it to back
        policy.notify_access(&entry(1), CachedBatchType::MemoryArrow);

        // Evict 1 — LRU is now entry 2 (entry 1 was moved to back)
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(2)]);
    }

    #[test]
    fn test_lru_insert_moves_between_queues() {
        let policy = LruPolicy::new();

        // Insert as Arrow
        policy.notify_insert(&entry(1), CachedBatchType::MemoryArrow);

        // Re-insert same entry as Liquid (simulates transcode)
        policy.notify_insert(&entry(1), CachedBatchType::MemoryLiquid);

        // Arrow queue should be empty now
        let victims = policy.find_memory_victim(1);
        // Should come from Liquid queue
        assert_eq!(victims, vec![entry(1)]);
    }

    #[test]
    fn test_lru_remove() {
        let policy = LruPolicy::new();

        policy.notify_insert(&entry(1), CachedBatchType::MemoryArrow);
        policy.notify_insert(&entry(2), CachedBatchType::MemoryArrow);

        // Remove entry 1
        policy.notify_remove(&entry(1));

        // Evict — should get entry 2 (entry 1 was removed)
        let victims = policy.find_memory_victim(1);
        assert_eq!(victims, vec![entry(2)]);
    }
}
