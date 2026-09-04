use congee::CongeeArc;
use std::{
    fmt::{Debug, Formatter},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::cache::{cached_batch::CacheEntry, utils::EntryID};

pub(crate) struct ArtIndex {
    art: CongeeArc<EntryID, CacheEntry>,
    entry_count: AtomicUsize,
}

impl Debug for ArtIndex {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl ArtIndex {
    pub(crate) fn new() -> Self {
        let art: CongeeArc<EntryID, CacheEntry> = CongeeArc::new();
        Self {
            art,
            entry_count: AtomicUsize::new(0),
        }
    }

    pub(crate) fn get(&self, entry_id: &EntryID) -> Option<Arc<CacheEntry>> {
        let guard = self.art.pin();
        let batch = self.art.get(*entry_id, &guard)?;
        Some(batch)
    }

    pub(crate) fn is_cached(&self, entry_id: &EntryID) -> bool {
        let guard = self.art.pin();
        self.art.get(*entry_id, &guard).is_some()
    }

    pub(crate) fn insert(&self, entry_id: &EntryID, batch: CacheEntry) {
        let guard = self.art.pin();
        let existing = self
            .art
            .insert(*entry_id, Arc::new(batch), &guard)
            .expect("Insertion failed");
        if existing.is_none() {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn remove(&self, entry_id: &EntryID) -> Option<Arc<CacheEntry>> {
        let guard = self.art.pin();
        let removed = self.art.remove(*entry_id, &guard);
        if removed.is_some() {
            self.entry_count.fetch_sub(1, Ordering::Relaxed);
        }
        removed
    }

    pub(crate) fn reset(&self) {
        let guard = self.art.pin();
        self.art.keys().into_iter().for_each(|k| {
            _ = self.art.remove(k, &guard).unwrap();
        });
        self.entry_count.store(0, Ordering::Relaxed);
    }

    pub(crate) fn for_each(&self, mut f: impl FnMut(&EntryID, &CacheEntry)) {
        let guard = self.art.pin();
        for id in self.art.keys().into_iter() {
            f(
                &id,
                &self
                    .art
                    .get(id, &guard)
                    .expect("Failed to get value from ART"),
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn keys(&self) -> Vec<EntryID> {
        self.art.keys()
    }

    pub(crate) fn entry_count(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::cached_batch::CacheEntry;
    use crate::cache::utils::create_test_array;

    use super::*;

    #[test]
    fn test_get_and_is_cached() {
        let store = ArtIndex::new();
        let entry_id1: EntryID = EntryID::from(1);
        let entry_id2: EntryID = EntryID::from(2);
        let array1 = create_test_array(100);

        // Initially, entries should not be cached
        assert!(!store.is_cached(&entry_id1));
        assert!(!store.is_cached(&entry_id2));
        assert!(store.get(&entry_id1).is_none());

        // Insert an entry and verify it's cached
        {
            store.insert(&entry_id1, array1.clone());
        }

        assert!(store.is_cached(&entry_id1));
        assert!(!store.is_cached(&entry_id2));

        // Get should return the cached value
        match store.get(&entry_id1) {
            Some(batch) => match batch.as_ref() {
                CacheEntry::MemoryArrow(arr) => assert_eq!(arr.len(), 100),
                _ => panic!("Expected ArrowMemory batch"),
            },
            None => panic!("Expected ArrowMemory batch"),
        }
    }

    #[test]
    fn test_reset() {
        let store = ArtIndex::new();
        let entry_id: EntryID = EntryID::from(1);
        let array = create_test_array(100);

        store.insert(&entry_id, array.clone());

        let entry_id: EntryID = EntryID::from(1);
        assert!(store.is_cached(&entry_id));

        store.reset();
        let entry_id: EntryID = EntryID::from(1);
        assert!(!store.is_cached(&entry_id));
    }
}
