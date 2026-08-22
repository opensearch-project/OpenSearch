#[cfg(test)]
use crate::cache::cached_batch::CacheEntry;
#[cfg(test)]
use crate::sync::Arc;
#[cfg(test)]
use arrow::array::ArrayRef;

#[derive(Debug)]
pub struct CacheConfig {
    batch_size: usize,
    max_memory_bytes: usize,
}

impl CacheConfig {
    pub(super) fn new(batch_size: usize, max_memory_bytes: usize) -> Self {
        Self {
            batch_size,
            max_memory_bytes,
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn max_memory_bytes(&self) -> usize {
        self.max_memory_bytes
    }
}

// Helper methods
#[cfg(test)]
pub(crate) fn create_test_array(size: usize) -> CacheEntry {
    use arrow::array::Int64Array;
    use std::sync::Arc;

    CacheEntry::memory_arrow(Arc::new(Int64Array::from_iter_values(0..size as i64)))
}

// Helper methods
#[cfg(test)]
pub(crate) fn create_test_arrow_array(size: usize) -> ArrayRef {
    use arrow::array::Int64Array;
    Arc::new(Int64Array::from_iter_values(0..size as i64))
}

#[cfg(test)]
pub(crate) fn create_cache_store(
    max_memory_bytes: usize,
    policy: Box<dyn super::policies::CachePolicy>,
) -> Arc<super::core::LiquidCache> {
    use crate::cache::{LiquidCacheBuilder, TranscodeEvict};

    let batch_size = 128;

    let builder = LiquidCacheBuilder::new()
        .with_batch_size(batch_size)
        .with_max_memory_bytes(max_memory_bytes)
        .with_squeeze_policy(Box::new(TranscodeEvict))
        .with_cache_policy(policy);
    builder.build()
}

/// EntryID is a unique identifier for a batch of rows, i.e., the cache key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, serde::Serialize)]
pub struct EntryID {
    val: usize,
}

impl From<usize> for EntryID {
    fn from(val: usize) -> Self {
        Self { val }
    }
}

impl From<EntryID> for usize {
    fn from(val: EntryID) -> Self {
        val.val
    }
}
