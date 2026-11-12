use arrow::array::cast::AsArray;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::buffer::BooleanBuffer;
use arrow::record_batch::RecordBatch;
use arrow_schema::{Field, Schema};

use super::{
    budget::BudgetAccounting,
    builders::{EvaluatePredicate, Get, Insert},
    cached_batch::{CacheEntry, CachedBatchType},
    observer::Observer,
    policies::CachePolicy,
    utils::CacheConfig,
};
use crate::cache::CacheStats;
use crate::cache::policies::{SqueezeOutcome, SqueezePolicy};
use crate::cache::{LiquidExpr, index::ArtIndex, utils::EntryID};
use crate::sync::Arc;

/// The cache could not free enough memory to admit an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheFull;

/// Cache storage for liquid cache.
///
/// Example:
/// ```rust
/// use liquid_cache::cache::{LiquidCacheBuilder, EntryID};
/// use arrow::array::UInt64Array;
/// use std::sync::Arc;
///
/// let storage = LiquidCacheBuilder::new()
///     .with_max_memory_bytes(1024 * 1024)
///     .build();
///
/// let entry_id = EntryID::from(0);
/// let arrow_array = Arc::new(UInt64Array::from_iter_values(0..32));
/// storage.insert(entry_id, arrow_array.clone()).execute().unwrap();
///
/// // Get the arrow array back
/// let retrieved = storage.get(&entry_id).read().unwrap();
/// assert_eq!(retrieved.as_ref(), arrow_array.as_ref());
/// ```
#[derive(Debug)]
pub struct LiquidCache {
    index: ArtIndex,
    config: CacheConfig,
    budget: BudgetAccounting,
    cache_policy: Box<dyn CachePolicy>,
    squeeze_policy: Box<dyn SqueezePolicy>,
    observer: Arc<Observer>,
}

impl LiquidCache {
    /// Return current cache statistics: counts and resource usage.
    pub fn stats(&self) -> CacheStats {
        // Count entries by format
        let total_entries = self.index.entry_count();

        let mut memory_arrow_entries = 0usize;
        let mut memory_liquid_entries = 0usize;

        let mut memory_arrow_bytes = 0usize;
        let mut memory_liquid_bytes = 0usize;

        self.index.for_each(|_, batch| match batch {
            CacheEntry::MemoryArrow(array) => {
                memory_arrow_entries += 1;
                memory_arrow_bytes += array.get_array_memory_size();
            }
            CacheEntry::MemoryLiquid(array) => {
                memory_liquid_entries += 1;
                memory_liquid_bytes += array.get_array_memory_size();
            }
        });

        let memory_usage_bytes = self.budget.memory_usage_bytes();
        let runtime = self.observer.runtime_snapshot();

        CacheStats {
            total_entries,
            memory_arrow_entries,
            memory_liquid_entries,
            memory_arrow_bytes,
            memory_liquid_bytes,
            memory_usage_bytes,
            max_memory_bytes: self.budget.max_memory_bytes(),
            runtime,
        }
    }

    /// Insert a batch into the cache.
    pub fn insert<'a>(&'a self, entry_id: EntryID, batch_to_cache: ArrayRef) -> Insert<'a> {
        Insert::new(self, entry_id, batch_to_cache)
    }

    /// Create a [`Get`] builder for the provided entry.
    pub fn get<'a>(&'a self, entry_id: &'a EntryID) -> Get<'a> {
        Get::new(self, entry_id)
    }

    /// Create an [`EvaluatePredicate`] builder for evaluating predicates on cached data.
    pub fn eval_predicate<'a>(
        &'a self,
        entry_id: &'a EntryID,
        predicate: &'a LiquidExpr,
    ) -> EvaluatePredicate<'a> {
        EvaluatePredicate::new(self, entry_id, predicate)
    }

    /// Try to read a liquid array from the cache.
    /// Returns None if the cached data is not in liquid format.
    pub fn try_read_liquid(
        &self,
        entry_id: &EntryID,
    ) -> Option<crate::liquid_array::LiquidArrayRef> {
        self.observer.on_try_read_liquid();
        let batch = self.index.get(entry_id)?;
        self.cache_policy
            .notify_access(entry_id, CachedBatchType::from(batch.as_ref()));

        match batch.as_ref() {
            CacheEntry::MemoryLiquid(array) => Some(array.clone()),
            CacheEntry::MemoryArrow(_) => None,
        }
    }

    /// Iterate over all entries in the cache.
    /// No guarantees are made about the order of the entries.
    /// Isolation level: read-committed
    pub fn for_each_entry(&self, mut f: impl FnMut(&EntryID, &CacheEntry)) {
        self.index.for_each(&mut f);
    }

    /// Reset the cache.
    pub fn reset(&self) {
        self.index.reset();
        self.budget.reset_usage();
    }

    /// Check if a batch is cached.
    pub fn is_cached(&self, entry_id: &EntryID) -> bool {
        self.index.is_cached(entry_id)
    }

    /// Get the config of the cache.
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Get the budget of the cache.
    pub fn budget(&self) -> &BudgetAccounting {
        &self.budget
    }

    /// Access the cache observer (runtime stats).
    pub fn observer(&self) -> &Observer {
        &self.observer
    }
}

impl LiquidCache {
    /// Insert a batch into the cache, it will run cache replacement policy until the batch is inserted.
    pub(crate) fn insert_inner(
        &self,
        entry_id: EntryID,
        mut batch_to_cache: CacheEntry,
    ) -> Result<(), CacheFull> {
        loop {
            let Err(not_inserted) = self.try_insert(entry_id, batch_to_cache) else {
                return Ok(());
            };

            let victims = self.cache_policy.find_memory_victim(8);
            if victims.is_empty() {
                // No advice, because the cache is already empty:
                // the entry to be inserted does not fit in memory at all.
                return Err(CacheFull);
            }
            self.squeeze_victims(victims);

            batch_to_cache = not_inserted;
        }
    }

    /// Create a new instance of CacheStorage.
    pub(crate) fn new(
        batch_size: usize,
        max_memory_bytes: usize,
        squeeze_policy: Box<dyn SqueezePolicy>,
        cache_policy: Box<dyn CachePolicy>,
    ) -> Self {
        let config = CacheConfig::new(batch_size, max_memory_bytes);
        let observer = Arc::new(Observer::new());
        Self {
            index: ArtIndex::new(),
            budget: BudgetAccounting::new(config.max_memory_bytes()),
            config,
            cache_policy,
            squeeze_policy,
            observer,
        }
    }

    fn try_insert(&self, entry_id: EntryID, to_insert: CacheEntry) -> Result<(), CacheEntry> {
        let new_memory_size = to_insert.memory_usage_bytes();
        let cached_batch_type = if let Some(entry) = self.index.get(&entry_id) {
            let old_memory_size = entry.memory_usage_bytes();
            if self
                .budget
                .try_update_memory_usage(old_memory_size, new_memory_size)
                .is_err()
            {
                return Err(to_insert);
            }
            let batch_type = CachedBatchType::from(&to_insert);
            self.index.insert(&entry_id, to_insert);
            batch_type
        } else {
            if self.budget.try_reserve_memory(new_memory_size).is_err() {
                return Err(to_insert);
            }
            let batch_type = CachedBatchType::from(&to_insert);
            self.index.insert(&entry_id, to_insert);
            batch_type
        };

        self.cache_policy
            .notify_insert(&entry_id, cached_batch_type);

        Ok(())
    }

    fn remove_memory_entry(&self, entry_id: EntryID) {
        let Some(removed) = self.index.remove(&entry_id) else {
            return;
        };
        self.budget
            .try_update_memory_usage(removed.memory_usage_bytes(), 0)
            .expect("memory release cannot fail");
        self.cache_policy.notify_remove(&entry_id);
        self.observer.on_memory_eviction();
    }

    /// Get the index of the cache.
    #[cfg(test)]
    pub(crate) fn index(&self) -> &ArtIndex {
        &self.index
    }

    fn squeeze_victims(&self, victims: Vec<EntryID>) {
        for victim in victims {
            self.squeeze_victim_inner(victim);
        }
    }

    fn squeeze_victim_inner(&self, to_squeeze: EntryID) {
        let Some(mut to_squeeze_batch) = self.index.get(&to_squeeze) else {
            return;
        };

        loop {
            let outcome = self.squeeze_policy.squeeze(to_squeeze_batch.as_ref());

            match outcome {
                SqueezeOutcome::Replace(new_batch) => {
                    match self.try_insert(to_squeeze, new_batch) {
                        Ok(()) => {
                            self.observer.on_transcode();
                            break;
                        }
                        Err(batch) => {
                            // The replacement did not fit either; squeeze it further.
                            // The squeeze policies guarantee this converges to `Remove`.
                            to_squeeze_batch = Arc::new(batch);
                        }
                    }
                }
                SqueezeOutcome::Remove => {
                    self.remove_memory_entry(to_squeeze);
                    break;
                }
            }
        }
    }

    pub(crate) fn read_arrow_array(
        &self,
        entry_id: &EntryID,
        selection: Option<&BooleanBuffer>,
    ) -> Option<ArrayRef> {
        use arrow::array::BooleanArray;

        let batch = self.index.get(entry_id)?;
        self.cache_policy
            .notify_access(entry_id, CachedBatchType::from(batch.as_ref()));

        match batch.as_ref() {
            CacheEntry::MemoryArrow(array) => match selection {
                Some(selection) => {
                    let selection_array = BooleanArray::new(selection.clone(), None);
                    arrow::compute::filter(array, &selection_array).ok()
                }
                None => Some(array.clone()),
            },
            CacheEntry::MemoryLiquid(array) => match selection {
                Some(selection) => Some(array.filter(selection)),
                None => Some(array.to_arrow_array()),
            },
        }
    }

    pub(crate) fn eval_predicate_internal(
        &self,
        entry_id: &EntryID,
        selection_opt: Option<&BooleanBuffer>,
        predicate: &LiquidExpr,
    ) -> Option<BooleanArray> {
        use arrow::array::BooleanArray;

        self.observer.on_eval_predicate();
        let batch = self.index.get(entry_id)?;
        self.cache_policy
            .notify_access(entry_id, CachedBatchType::from(batch.as_ref()));

        match batch.as_ref() {
            CacheEntry::MemoryArrow(array) => {
                let mut owned = None;
                let selection = selection_opt.unwrap_or_else(|| {
                    owned = Some(BooleanBuffer::new_set(array.len()));
                    owned.as_ref().unwrap()
                });
                let selection_array = BooleanArray::new(selection.clone(), None);
                let filtered = arrow::compute::filter(array, &selection_array)
                    .expect("selection must match array length");
                Some(self.eval_predicate_on_array(filtered, predicate))
            }
            CacheEntry::MemoryLiquid(array) => {
                let mut owned = None;
                let selection = selection_opt.unwrap_or_else(|| {
                    owned = Some(BooleanBuffer::new_set(array.len()));
                    owned.as_ref().unwrap()
                });
                Some(array.try_eval_predicate(predicate, selection))
            }
        }
    }

    fn eval_predicate_on_array(&self, array: ArrayRef, predicate: &LiquidExpr) -> BooleanArray {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "liquid_predicate_col",
            array.data_type().clone(),
            true,
        )]));
        let record_batch =
            RecordBatch::try_new(schema, vec![array]).expect("single-column predicate batch");
        let result = predicate
            .physical_expr()
            .evaluate(&record_batch)
            .expect("validated LiquidExpr must evaluate");
        let boolean_array = result
            .into_array(record_batch.num_rows())
            .expect("predicate output must be an array");
        boolean_array.as_boolean().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{
        CacheEntry, CachePolicy, LiquidCacheBuilder, LiquidPolicy, transcode_liquid_inner,
        utils::{create_cache_store, create_test_array, create_test_arrow_array},
    };
    use crate::sync::thread;
    use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array};
    use datafusion_common::ScalarValue;
    use datafusion_expr_common::operator::Operator as DFOperator;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Unified advice type for more concise testing
    #[derive(Debug)]
    struct TestPolicy {
        target_id: Option<EntryID>,
        advice_count: AtomicUsize,
    }

    impl TestPolicy {
        fn new(target_id: Option<EntryID>) -> Self {
            Self {
                target_id,
                advice_count: AtomicUsize::new(0),
            }
        }
    }

    impl CachePolicy for TestPolicy {
        fn find_memory_victim(&self, _cnt: usize) -> Vec<EntryID> {
            self.advice_count.fetch_add(1, Ordering::SeqCst);
            let id_to_use = self.target_id.unwrap();
            vec![id_to_use]
        }
    }

    #[test]
    fn test_basic_cache_operations() {
        // Test basic insert, get, and size tracking in one test
        let budget_size = 10 * 1024;
        let store = create_cache_store(budget_size, Box::new(LiquidPolicy::new()));

        // 1. Initial budget should be empty
        assert_eq!(store.budget.memory_usage_bytes(), 0);

        // 2. Insert and verify first entry
        let entry_id1: EntryID = EntryID::from(1);
        let array1 = create_test_array(100);
        let size1 = array1.memory_usage_bytes();
        store.insert_inner(entry_id1, array1).unwrap();

        // Verify budget usage and data correctness
        assert_eq!(store.budget.memory_usage_bytes(), size1);
        let retrieved1 = store.index().get(&entry_id1).unwrap();
        match retrieved1.as_ref() {
            CacheEntry::MemoryArrow(arr) => assert_eq!(arr.len(), 100),
            _ => panic!("Expected ArrowMemory"),
        }

        let entry_id2: EntryID = EntryID::from(2);
        let array2 = create_test_array(200);
        let size2 = array2.memory_usage_bytes();
        store.insert_inner(entry_id2, array2).unwrap();

        assert_eq!(store.budget.memory_usage_bytes(), size1 + size2);

        let array3 = create_test_array(150);
        let size3 = array3.memory_usage_bytes();
        store.insert_inner(entry_id1, array3).unwrap();

        assert_eq!(store.budget.memory_usage_bytes(), size3 + size2);
        assert!(store.index().get(&EntryID::from(999)).is_none());
    }

    #[test]
    fn test_cache_advice_strategies() {
        // Under memory pressure, the advised victim is transcoded to liquid.
        let entry_id1 = EntryID::from(1);
        let entry_id2 = EntryID::from(2);

        let advisor = TestPolicy::new(Some(entry_id1));
        let store = create_cache_store(8000, Box::new(advisor)); // Small budget to force advice

        store
            .insert_inner(entry_id1, create_test_array(800))
            .unwrap();
        match store.index().get(&entry_id1).unwrap().as_ref() {
            CacheEntry::MemoryArrow(_) => {}
            other => panic!("Expected ArrowMemory, got {other:?}"),
        }

        store
            .insert_inner(entry_id2, create_test_array(800))
            .unwrap();
        match store.index().get(&entry_id1).unwrap().as_ref() {
            CacheEntry::MemoryLiquid(_) => {}
            other => panic!("Expected LiquidMemory after eviction, got {other:?}"),
        }
    }

    #[test]
    fn test_concurrent_cache_operations() {
        concurrent_cache_operations();
    }

    fn concurrent_cache_operations() {
        let num_threads = 3;
        let ops_per_thread = 50;

        // Large enough that no entry is evicted: with no disk tier, evicted
        // entries are gone, so the retrievability invariant below requires
        // every entry to fit in memory.
        let budget_size =
            num_threads * ops_per_thread * create_test_arrow_array(100).get_array_memory_size() * 2;
        let store = create_cache_store(budget_size, Box::new(LiquidPolicy::new()));

        let mut handles = vec![];
        for thread_id in 0..num_threads {
            let store = store.clone();
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let unique_id = thread_id * ops_per_thread + i;
                    let entry_id: EntryID = EntryID::from(unique_id);
                    let array = create_test_arrow_array(100);
                    store.insert(entry_id, array).execute().unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        // Invariant 1: Every previously inserted entry can be retrieved
        for thread_id in 0..num_threads {
            for i in 0..ops_per_thread {
                let unique_id = thread_id * ops_per_thread + i;
                let entry_id: EntryID = EntryID::from(unique_id);
                assert!(store.index().get(&entry_id).is_some());
            }
        }

        // Invariant 2: Number of entries matches number of insertions
        assert_eq!(store.index().keys().len(), num_threads * ops_per_thread);
    }

    #[test]
    fn test_cache_stats_memory_usage() {
        let storage = LiquidCacheBuilder::new()
            .with_max_memory_bytes(10 * 1024 * 1024)
            .build();

        // Insert two small batches
        let arr1: ArrayRef = Arc::new(Int32Array::from_iter_values(0..64));
        let arr2: ArrayRef = Arc::new(Int32Array::from_iter_values(0..128));
        storage
            .insert(EntryID::from(1usize), arr1)
            .execute()
            .unwrap();
        storage
            .insert(EntryID::from(2usize), arr2)
            .execute()
            .unwrap();

        // Stats after insert: 2 entries, memory usage > 0
        let s = storage.stats();
        assert_eq!(s.total_entries, 2);
        assert_eq!(s.memory_arrow_entries, 2);
        assert_eq!(s.memory_liquid_entries, 0);
        assert!(s.memory_usage_bytes > 0);
        assert_eq!(s.max_memory_bytes, 10 * 1024 * 1024);
    }

    /// `LiquidCache::stats()` must be non-destructive — reading stats for
    /// observability must not reset the runtime counters, so repeated reads
    /// return the same cumulative values.
    #[test]
    fn stats_runtime_counters_are_non_destructive() {
        let cache = LiquidCacheBuilder::new()
            .with_max_memory_bytes(10 * 1024 * 1024)
            .build();
        let entry = EntryID::from(1usize);
        let array: ArrayRef = Arc::new(Int32Array::from_iter_values(0..16));
        cache.insert(entry, array).execute().unwrap();

        // Bump a runtime counter deterministically via the public API.
        for _ in 0..5 {
            let _ = cache.try_read_liquid(&entry);
        }

        let first = cache.stats().runtime.try_read_liquid_calls;
        let second = cache.stats().runtime.try_read_liquid_calls;
        assert_eq!(first, 5, "counter must reflect the 5 try_read_liquid calls");
        assert_eq!(
            first, second,
            "stats() must be non-destructive: repeated reads must be stable"
        );
    }

    #[test]
    fn insert_returns_cache_full_when_memory_is_saturated() {
        let cache = LiquidCacheBuilder::new().with_max_memory_bytes(0).build();
        let array: ArrayRef = Arc::new(Int32Array::from_iter_values(0..16));

        let err = cache.insert(EntryID::from(900usize), array).execute();

        assert_eq!(err, Err(CacheFull));
        assert!(!cache.is_cached(&EntryID::from(900usize)));
    }

    #[test]
    fn eviction_under_memory_pressure_keeps_newest_entries() {
        // Budget fits roughly one arrow entry; older entries must be transcoded or evicted.
        let first_array: ArrayRef = Arc::new(Int32Array::from_iter_values(0..1024));
        let entry_size = first_array.get_array_memory_size();
        let cache = LiquidCacheBuilder::new()
            .with_max_memory_bytes(entry_size + entry_size / 2)
            .with_cache_policy(Box::new(LiquidPolicy::new()))
            .build();

        let first = EntryID::from(910usize);
        let second = EntryID::from(911usize);
        cache.insert(first, first_array).execute().unwrap();
        assert!(cache.is_cached(&first));

        let second_array: ArrayRef = Arc::new(Int32Array::from_iter_values(1024..2048));
        cache.insert(second, second_array).execute().unwrap();
        assert!(cache.is_cached(&second));
        assert!(cache.budget().memory_usage_bytes() <= cache.budget().max_memory_bytes());
    }

    #[test]
    fn eviction_releases_budget() {
        let cache = LiquidCacheBuilder::new()
            .with_max_memory_bytes(1 << 20)
            .build();
        let entry = EntryID::from(914usize);
        let array: ArrayRef = Arc::new(Int32Array::from_iter_values(0..16));
        cache.insert(entry, array).execute().unwrap();
        let before = cache.stats().memory_usage_bytes;
        assert!(before > 0);

        cache.remove_memory_entry(entry);

        assert_eq!(cache.stats().memory_usage_bytes, 0);
        assert!(!cache.is_cached(&entry));
    }

    #[test]
    fn get_with_selection_filters_rows() {
        let cache = LiquidCacheBuilder::new()
            .with_max_memory_bytes(1 << 20)
            .build();
        let entry = EntryID::from(915usize);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        cache.insert(entry, array).execute().unwrap();

        let selection = arrow::buffer::BooleanBuffer::from(vec![true, false, true, false]);
        let result = cache
            .get(&entry)
            .with_selection(&selection)
            .read()
            .expect("present");
        let expected = Int32Array::from(vec![1, 3]);
        assert_eq!(result.as_ref(), &expected as &dyn Array);
    }

    #[test]
    fn eval_predicate_on_arrow_and_liquid_entries() {
        let cache = LiquidCacheBuilder::new()
            .with_max_memory_bytes(1 << 20)
            .build();
        let arrow_entry = EntryID::from(916usize);
        let liquid_entry = EntryID::from(917usize);

        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        cache.insert(arrow_entry, array.clone()).execute().unwrap();

        let liquid = transcode_liquid_inner(&array).unwrap();
        cache
            .insert_inner(liquid_entry, CacheEntry::memory_liquid(liquid))
            .unwrap();

        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("liquid_predicate_col", 0)),
            DFOperator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        ));
        let predicate = LiquidExpr::new_unchecked(expr);
        let expected = BooleanArray::from(vec![false, false, true, true]);

        for entry in [arrow_entry, liquid_entry] {
            let got = cache
                .eval_predicate(&entry, &predicate)
                .read()
                .expect("entry present");
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn try_read_liquid_returns_only_liquid_entries() {
        let cache = LiquidCacheBuilder::new()
            .with_max_memory_bytes(1 << 20)
            .build();
        let arrow_entry = EntryID::from(918usize);
        let liquid_entry = EntryID::from(919usize);

        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        cache.insert(arrow_entry, array.clone()).execute().unwrap();
        let liquid = transcode_liquid_inner(&array).unwrap();
        cache
            .insert_inner(liquid_entry, CacheEntry::memory_liquid(liquid))
            .unwrap();

        assert!(cache.try_read_liquid(&arrow_entry).is_none());
        let read = cache.try_read_liquid(&liquid_entry).expect("liquid entry");
        assert_eq!(read.to_arrow_array().as_ref(), array.as_ref());
    }

    #[test]
    fn reset_clears_entries_and_budget() {
        let cache = LiquidCacheBuilder::new()
            .with_max_memory_bytes(1 << 20)
            .build();
        let entry = EntryID::from(920usize);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        cache.insert(entry, array).execute().unwrap();
        assert!(cache.is_cached(&entry));

        cache.reset();

        assert!(!cache.is_cached(&entry));
        assert_eq!(cache.budget().memory_usage_bytes(), 0);
        assert_eq!(cache.stats().total_entries, 0);
    }
}
