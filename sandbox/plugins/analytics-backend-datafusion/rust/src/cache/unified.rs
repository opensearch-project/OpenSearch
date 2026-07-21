/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unified management interface over all four analytics caches.
//!
//! Every cache in this crate (footer metadata, statistics, column-index,
//! offset-index) is managed through the same two abstractions:
//!
//! - [`CacheKind`] — the closed set of cache types. Parses the Java-supplied
//!   type string once at the FFI boundary and carries per-kind rules
//!   ([`CacheKind::validate_policy`]) so they live in exactly one place.
//! - [`ManagedCache`] — the lifecycle/observability surface each cache
//!   implements: byte-limit updates, clearing, stats snapshots, per-file
//!   removal. `CustomCacheManager` holds these as `Arc<dyn ManagedCache>` and
//!   dispatches by kind instead of per-cache match arms.
//!
//! Query-path access (get/put) intentionally stays on each cache's own typed
//! API — the key/value shapes differ per cache and erasing them would cost
//! hot-path performance for no management benefit. This trait unifies the
//! *management* plane only, which is also where a future eviction-policy
//! swap plugs in: one [`crate::cache::eviction_policy::EvictionPolicy`]
//! implementation + one [`CacheEvictionPolicy`] variant covers every cache.

use crate::cache::eviction_policy::CacheEvictionPolicy;

// Cache type constants — the exact strings the Java side passes over FFI.
pub const CACHE_TYPE_METADATA: &str = "METADATA";
pub const CACHE_TYPE_STATS: &str = "STATISTICS";
pub const CACHE_TYPE_COLUMN_INDEX: &str = "COLUMN_INDEX";
pub const CACHE_TYPE_OFFSET_INDEX: &str = "OFFSET_INDEX";

/// The four analytics caches, as a closed enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheKind {
    Metadata,
    Statistics,
    ColumnIndex,
    OffsetIndex,
}

impl CacheKind {
    pub const ALL: [CacheKind; 4] = [
        CacheKind::Metadata,
        CacheKind::Statistics,
        CacheKind::ColumnIndex,
        CacheKind::OffsetIndex,
    ];

    /// Parse the Java-supplied cache type string.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            CACHE_TYPE_METADATA => Ok(CacheKind::Metadata),
            CACHE_TYPE_STATS => Ok(CacheKind::Statistics),
            CACHE_TYPE_COLUMN_INDEX => Ok(CacheKind::ColumnIndex),
            CACHE_TYPE_OFFSET_INDEX => Ok(CacheKind::OffsetIndex),
            _ => Err(format!("invalid cache type: {}", s)),
        }
    }

    /// The FFI string for this kind (inverse of [`Self::parse`]).
    pub fn as_str(&self) -> &'static str {
        match self {
            CacheKind::Metadata => CACHE_TYPE_METADATA,
            CacheKind::Statistics => CACHE_TYPE_STATS,
            CacheKind::ColumnIndex => CACHE_TYPE_COLUMN_INDEX,
            CacheKind::OffsetIndex => CACHE_TYPE_OFFSET_INDEX,
        }
    }

    /// Which eviction policies each cache currently supports. This encodes the
    /// SAME rules the per-cache match arms in `df_create_cache` used to
    /// enforce — centralized so a future policy (e.g. S3-FIFO) is enabled by
    /// editing one function.
    ///
    /// - `Metadata` accepts any value: `DefaultFilesMetadataCache` has its own
    ///   built-in LRU, the setting is accepted but not forwarded (unchanged).
    /// - `Statistics` runs the pluggable policy: LRU or LFU.
    /// - `ColumnIndex` / `OffsetIndex` are FIFO-only today (lock-free read
    ///   path relies on `on_access` being a no-op).
    pub fn validate_policy(&self, policy: CacheEvictionPolicy) -> Result<(), String> {
        match self {
            CacheKind::Metadata => Ok(()),
            CacheKind::Statistics => match policy {
                CacheEvictionPolicy::Lru | CacheEvictionPolicy::Lfu => Ok(()),
                CacheEvictionPolicy::Fifo => {
                    Err("STATISTICS cache does not support FIFO eviction".to_string())
                }
            },
            CacheKind::ColumnIndex | CacheKind::OffsetIndex => match policy {
                CacheEvictionPolicy::Fifo => Ok(()),
                _ => Err(format!(
                    "{} cache only supports FIFO eviction",
                    self.as_str()
                )),
            },
        }
    }
}

impl std::fmt::Display for CacheKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Uniform stats snapshot for any managed cache. Surfaced on node-stats.
///
/// Caches that don't track a counter report 0 (e.g. the metadata and
/// statistics caches don't count evictions today).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub used_bytes: usize,
    pub limit_bytes: usize,
}

/// Management-plane interface implemented by all four caches.
///
/// `CustomCacheManager` keeps one `Arc<dyn ManagedCache>` per registered
/// [`CacheKind`] and dispatches every per-type operation through this trait.
pub trait ManagedCache: Send + Sync {
    /// Which cache this is.
    fn kind(&self) -> CacheKind;

    /// Lock-free-ish counters + occupancy snapshot.
    fn stats(&self) -> CacheStats;

    /// Update the byte cap; implementations evict immediately if over budget.
    fn set_limit(&self, limit: usize);

    /// Drop all entries (counters reset where the cache supports it), keeping
    /// the current limit.
    fn clear(&self);

    /// Reset hit/miss diagnostic counters without touching entries.
    fn reset_counters(&self);

    /// Remove all entries belonging to `file_path` (exact key for file-keyed
    /// caches, key prefix for cell-keyed page-index caches). Returns whether
    /// anything was removed.
    fn remove_file(&self, file_path: &str) -> bool;

    /// Whether this cache currently holds entries for `file_path`.
    ///
    /// Diagnostics only. The page-index caches approximate (entries > 0) —
    /// a per-file scan of the cell keys is not worth it for a boolean probe.
    fn contains_file(&self, file_path: &str) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_roundtrips_all_kinds() {
        for kind in CacheKind::ALL {
            assert_eq!(CacheKind::parse(kind.as_str()).unwrap(), kind);
        }
        assert!(CacheKind::parse("BOGUS").is_err());
    }

    #[test]
    fn validate_policy_preserves_per_cache_rules() {
        use CacheEvictionPolicy::*;
        // Metadata accepts everything (setting not forwarded).
        for p in [Lru, Lfu, Fifo] {
            assert!(CacheKind::Metadata.validate_policy(p).is_ok());
        }
        // Statistics: LRU/LFU only.
        assert!(CacheKind::Statistics.validate_policy(Lru).is_ok());
        assert!(CacheKind::Statistics.validate_policy(Lfu).is_ok());
        assert!(CacheKind::Statistics.validate_policy(Fifo).is_err());
        // CI/OI: FIFO only.
        for kind in [CacheKind::ColumnIndex, CacheKind::OffsetIndex] {
            assert!(kind.validate_policy(Fifo).is_ok());
            assert!(kind.validate_policy(Lru).is_err());
            assert!(kind.validate_policy(Lfu).is_err());
        }
    }
}
