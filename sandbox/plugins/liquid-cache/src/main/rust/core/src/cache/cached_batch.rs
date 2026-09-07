//! Cached batch types.

use std::{fmt::Display, sync::Arc};

use arrow::array::ArrayRef;

use crate::liquid_array::LiquidArrayRef;

/// A cached entry storing data in various formats.
#[derive(Debug, Clone)]
pub enum CacheEntry {
    /// Cached batch in memory as Arrow array.
    MemoryArrow(ArrayRef),
    /// Cached batch in memory as liquid array.
    MemoryLiquid(LiquidArrayRef),
}

impl CacheEntry {
    /// Construct a cached batch stored as an in-memory Arrow array.
    pub fn memory_arrow(array: ArrayRef) -> Self {
        Self::MemoryArrow(array)
    }

    /// Construct a cached batch stored as an in-memory Liquid array.
    pub fn memory_liquid(array: LiquidArrayRef) -> Self {
        Self::MemoryLiquid(array)
    }

    /// Memory usage reported by the underlying representation.
    pub fn memory_usage_bytes(&self) -> usize {
        match self {
            Self::MemoryArrow(array) => array.get_array_memory_size(),
            Self::MemoryLiquid(array) => array.get_array_memory_size(),
        }
    }

    /// Reference count (if any) of the backing storage.
    pub fn reference_count(&self) -> usize {
        match self {
            Self::MemoryArrow(array) => Arc::strong_count(array),
            Self::MemoryLiquid(array) => Arc::strong_count(array),
        }
    }
}

impl Display for CacheEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryArrow(_) => write!(f, "MemoryArrow"),
            Self::MemoryLiquid(_) => write!(f, "MemoryLiquid"),
        }
    }
}

/// The type of the cached batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum CachedBatchType {
    /// Cached batch in memory as Arrow array.
    MemoryArrow,
    /// Cached batch in memory as liquid array.
    MemoryLiquid,
}

impl From<&CacheEntry> for CachedBatchType {
    fn from(batch: &CacheEntry) -> Self {
        match batch {
            CacheEntry::MemoryArrow(_) => Self::MemoryArrow,
            CacheEntry::MemoryLiquid(_) => Self::MemoryLiquid,
        }
    }
}
