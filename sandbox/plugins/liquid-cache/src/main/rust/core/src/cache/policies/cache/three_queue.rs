use std::{collections::HashMap, ptr::NonNull};

use crate::{
    cache::{CachePolicy, EntryID, cached_batch::CachedBatchType},
    sync::Mutex,
};

use super::doubly_linked_list::{DoublyLinkedList, DoublyLinkedNode, drop_boxed_node};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueueKind {
    Arrow,
    Liquid,
}

#[derive(Debug)]
struct QueueNode {
    entry_id: EntryID,
    queue: QueueKind,
}

type NodePtr = NonNull<DoublyLinkedNode<QueueNode>>;

#[derive(Default, Debug)]
struct LiquidQueueInternalState {
    map: HashMap<EntryID, NodePtr>,
    arrow: DoublyLinkedList<QueueNode>,
    liquid: DoublyLinkedList<QueueNode>,
}

impl LiquidQueueInternalState {
    unsafe fn list_mut(&mut self, queue: QueueKind) -> &mut DoublyLinkedList<QueueNode> {
        match queue {
            QueueKind::Arrow => &mut self.arrow,
            QueueKind::Liquid => &mut self.liquid,
        }
    }

    unsafe fn push_back(&mut self, queue: QueueKind, mut node_ptr: NodePtr) {
        unsafe {
            node_ptr.as_mut().data.queue = queue;
            self.list_mut(queue).push_back(node_ptr);
        }
    }

    unsafe fn detach(&mut self, node_ptr: NodePtr) {
        unsafe {
            let queue = node_ptr.as_ref().data.queue;
            self.list_mut(queue).unlink(node_ptr);
        }
    }

    fn upsert_into_queue(&mut self, entry_id: EntryID, target: QueueKind) {
        if let Some(node_ptr) = self.map.get(&entry_id).copied() {
            unsafe {
                self.detach(node_ptr);
                self.push_back(target, node_ptr);
            }
            return;
        }

        let node = DoublyLinkedNode::new(QueueNode {
            entry_id,
            queue: target,
        });
        let node_ptr = NonNull::from(Box::leak(node));

        self.map.insert(entry_id, node_ptr);
        unsafe {
            self.push_back(target, node_ptr);
        }
    }

    fn pop_front(&mut self, queue: QueueKind) -> Option<EntryID> {
        let list = match queue {
            QueueKind::Arrow => &mut self.arrow,
            QueueKind::Liquid => &mut self.liquid,
        };

        let head_ptr = list.head()?;
        let entry_id = unsafe { head_ptr.as_ref().data.entry_id };
        let node_ptr = self
            .map
            .remove(&entry_id)
            .expect("list head must exist in map");
        unsafe {
            list.unlink(node_ptr);
            drop_boxed_node(node_ptr);
        }
        Some(entry_id)
    }

    fn remove(&mut self, entry_id: &EntryID) -> Option<EntryID> {
        let node_ptr = self.map.remove(entry_id)?;
        let removed = unsafe { node_ptr.as_ref().data.entry_id };
        unsafe {
            self.detach(node_ptr);
            drop_boxed_node(node_ptr);
        }
        Some(removed)
    }
}

impl Drop for LiquidQueueInternalState {
    fn drop(&mut self) {
        let nodes: Vec<_> = self.map.drain().map(|(_, ptr)| ptr).collect();
        for node_ptr in nodes {
            unsafe {
                match node_ptr.as_ref().data.queue {
                    QueueKind::Arrow => self.arrow.unlink(node_ptr),
                    QueueKind::Liquid => self.liquid.unlink(node_ptr),
                }
                drop_boxed_node(node_ptr);
            }
        }

        unsafe {
            self.arrow.drop_all();
            self.liquid.drop_all();
        }
    }
}

/// Cache policy that keeps independent FIFO queues per batch type.
#[derive(Debug, Default)]
pub struct LiquidPolicy {
    inner: Mutex<LiquidQueueInternalState>,
}

impl LiquidPolicy {
    /// Create a new [`LiquidPolicy`].
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(LiquidQueueInternalState::default()),
        }
    }
}

// SAFETY: Access to raw pointers is protected by the internal `Mutex`.
unsafe impl Send for LiquidPolicy {}
unsafe impl Sync for LiquidPolicy {}

impl CachePolicy for LiquidPolicy {
    fn notify_insert(&self, entry_id: &EntryID, batch_type: CachedBatchType) {
        let mut inner = self.inner.lock().unwrap();
        let target = match batch_type {
            CachedBatchType::MemoryArrow => QueueKind::Arrow,
            CachedBatchType::MemoryLiquid => QueueKind::Liquid,
        };

        inner.upsert_into_queue(*entry_id, target);
    }

    fn find_memory_victim(&self, cnt: usize) -> Vec<EntryID> {
        if cnt == 0 {
            return vec![];
        }

        let mut inner = self.inner.lock().unwrap();
        let mut victims = Vec::with_capacity(cnt);

        while victims.len() < cnt {
            if let Some(entry) = inner.pop_front(QueueKind::Arrow) {
                victims.push(entry);
                continue;
            }

            if let Some(entry) = inner.pop_front(QueueKind::Liquid) {
                victims.push(entry);
                continue;
            }

            break;
        }

        victims
    }

    fn notify_access(&self, _entry_id: &EntryID, _batch_type: CachedBatchType) {}

    fn notify_remove(&self, entry_id: &EntryID) {
        let mut inner = self.inner.lock().unwrap();
        inner.remove(entry_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::utils::EntryID;

    fn entry(id: usize) -> EntryID {
        id.into()
    }

    #[test]
    fn test_fifo_within_each_queue() {
        let policy = LiquidPolicy::new();

        let arrow_a = entry(1);
        let arrow_b = entry(2);
        let liquid_a = entry(3);
        let liquid_b = entry(4);

        policy.notify_insert(&arrow_a, CachedBatchType::MemoryArrow);
        policy.notify_insert(&arrow_b, CachedBatchType::MemoryArrow);
        policy.notify_insert(&liquid_a, CachedBatchType::MemoryLiquid);
        policy.notify_insert(&liquid_b, CachedBatchType::MemoryLiquid);

        assert_eq!(policy.find_memory_victim(1), vec![arrow_a]);
        assert_eq!(policy.find_memory_victim(2), vec![arrow_b, liquid_a]);
        assert_eq!(policy.find_memory_victim(1), vec![liquid_b]);
    }

    #[test]
    fn test_queue_priority_order() {
        let policy = LiquidPolicy::new();

        let arrow_entry = entry(1);
        let liquid_entry = entry(2);

        policy.notify_insert(&liquid_entry, CachedBatchType::MemoryLiquid);
        policy.notify_insert(&arrow_entry, CachedBatchType::MemoryArrow);

        // Request more victims than available to ensure we only get what exists.
        let victims = policy.find_memory_victim(5);
        assert_eq!(victims, vec![arrow_entry, liquid_entry]);
    }

    #[test]
    fn test_zero_victim_request_returns_empty() {
        let policy = LiquidPolicy::new();

        policy.notify_insert(&entry(1), CachedBatchType::MemoryArrow);
        assert!(policy.find_memory_victim(0).is_empty());
    }

    #[test]
    fn test_reinsert_moves_entry_to_back_of_queue() {
        let policy = LiquidPolicy::new();

        let first = entry(1);
        let second = entry(2);

        policy.notify_insert(&first, CachedBatchType::MemoryArrow);
        policy.notify_insert(&second, CachedBatchType::MemoryArrow);

        // Reinserting should refresh the entry as the newest arrow batch.
        policy.notify_insert(&first, CachedBatchType::MemoryArrow);

        assert_eq!(policy.find_memory_victim(1), vec![second]);
        assert_eq!(policy.find_memory_victim(1), vec![first]);
    }

    #[test]
    fn test_reinsert_handles_cross_queue_move() {
        let policy = LiquidPolicy::new();

        let entry_id = entry(42);

        policy.notify_insert(&entry_id, CachedBatchType::MemoryArrow);
        policy.notify_insert(&entry_id, CachedBatchType::MemoryLiquid);

        let victims = policy.find_memory_victim(2);
        assert_eq!(victims, vec![entry_id]);
    }
}
