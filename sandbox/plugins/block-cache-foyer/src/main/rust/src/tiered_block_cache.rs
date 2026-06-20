/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`TieredBlockCache`] — routes entries between a data cache and a metadata cache.
//!
//! The data cache is a large SSD-backed [`FoyerCache`] with default eviction (RejectAll
//! reinsertion). The metadata cache is a smaller SSD-backed [`FoyerCache`] configured
//! with AdmitAll reinsertion so that metadata entries are never evicted by the disk
//! reclaimer — they are always reinserted.
//!
//! ## Routing
//!
//! - `get()`: tries metadata cache first, then data cache. No marking needed.
//! - `put()`: always goes to data cache (normal query-time caching).
//! - `put_metadata()`: explicit warmup call — goes to metadata cache only.
//!
//! ## Restart
//!
//! No persistence needed for routing state. Foyer recovers metadata SSD blocks
//! via `RecoverMode::Quiet`. After restart, `get()` probes metadata cache first
//! and finds the recovered entries — zero S3 calls for metadata.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use crate::foyer::foyer_cache::FoyerCache;
use crate::range_cache::CacheKey;
use crate::traits::BlockCache;

/// Default max metadata entry size: 10MB.
/// Entries larger than this are routed to data_cache (evictable) instead of
/// metadata_cache (never-evict) to prevent one pathological file from filling
/// the metadata SSD.
const DEFAULT_MAX_METADATA_ENTRY_SIZE: usize = 10 * 1024 * 1024;

/// A two-tier block cache: metadata cache (never evicts) + data cache (LRU eviction).
///
/// ## Lookup order
///
/// `get()` always probes metadata cache first (small, fast), then data cache.
/// This ensures metadata is found on warm restart without any routing state.
///
/// ## Write routing
///
/// - `put()` → data cache (called by TieredObjectStore::populate_cache after S3 fetch)
/// - `put_metadata()` → metadata cache if entry ≤ max_metadata_entry_size,
///   otherwise falls back to data cache (evictable).
///
/// This guarantees data blocks never pollute the metadata cache, and metadata
/// never competes with data for LRU eviction.
pub struct TieredBlockCache {
    data_cache: Arc<FoyerCache>,
    metadata_cache: Arc<FoyerCache>,
    /// Max entry size for metadata cache. Entries larger than this are routed
    /// to data_cache instead. Dynamically updatable via `update_max_metadata_entry_size`.
    max_metadata_entry_size: AtomicUsize,
}

impl TieredBlockCache {
    pub fn new(data_cache: Arc<FoyerCache>, metadata_cache: Arc<FoyerCache>) -> Self {
        native_bridge_common::log_info!(
            "[tiered-block-cache] created: data_disk={}B, metadata_disk={}B, max_metadata_entry={}B",
            data_cache.disk_bytes,
            metadata_cache.disk_bytes,
            DEFAULT_MAX_METADATA_ENTRY_SIZE
        );
        Self {
            data_cache,
            metadata_cache,
            max_metadata_entry_size: AtomicUsize::new(DEFAULT_MAX_METADATA_ENTRY_SIZE),
        }
    }

    /// Put bytes into the metadata cache. Called during shard warmup only.
    ///
    /// Entries in the metadata cache are never evicted (AdmitAll reinsertion)
    /// and survive node restarts (Foyer disk recovery).
    ///
    /// If the entry exceeds `max_metadata_entry_size`, it's routed to the data
    /// cache instead (evictable by LRU). This prevents pathological wide-schema
    /// files (1000+ columns, 50MB+ page indexes) from filling the metadata SSD.
    pub fn put_metadata(&self, key: &CacheKey, data: Bytes) {
        let limit = self.max_metadata_entry_size.load(Ordering::Relaxed);
        if data.len() > limit {
            native_bridge_common::log_info!(
                "[tiered-block-cache] metadata entry {}B exceeds limit {}B — routing to data cache",
                data.len(), limit
            );
            self.data_cache.put(key, data);
        } else {
            self.metadata_cache.put(key, data);
        }
    }

    /// Update the max metadata entry size dynamically. Takes effect immediately.
    /// Called from the FFM layer when the Java setting is changed via cluster settings API.
    pub fn update_max_metadata_entry_size(&self, new_limit: usize) {
        self.max_metadata_entry_size.store(new_limit, Ordering::Relaxed);
        native_bridge_common::log_info!(
            "[tiered-block-cache] max_metadata_entry_size updated to {}B", new_limit
        );
    }

    /// Current max metadata entry size.
    pub fn max_metadata_entry_size(&self) -> usize {
        self.max_metadata_entry_size.load(Ordering::Relaxed)
    }

    /// Access the underlying data cache (e.g. for stats).
    pub fn data_cache(&self) -> &FoyerCache {
        &self.data_cache
    }

    /// Access the underlying metadata cache (e.g. for stats).
    pub fn metadata_cache(&self) -> &FoyerCache {
        &self.metadata_cache
    }

    /// Clear all entries synchronously.
    pub(crate) fn clear_sync(&self) {
        self.data_cache.clear_sync();
        self.metadata_cache.clear_sync();
        native_bridge_common::log_info!("[tiered-block-cache] clear_sync completed");
    }
}

impl BlockCache for TieredBlockCache {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get<'a>(&'a self, key: &'a CacheKey)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Bytes>> + Send + 'a>>
    {
        Box::pin(async move {
            // Metadata cache first — small SSD, fast probe, never evicts.
            // On warm restart, Foyer recovers these from disk — instant hit.
            if let Some(bytes) = self.metadata_cache.get(key).await {
                return Some(bytes);
            }
            // Fall through to data cache.
            self.data_cache.get(key).await
        })
    }

    fn put(&self, key: &CacheKey, data: Bytes) {
        // Normal put (called by TieredObjectStore::populate_cache after S3 fetch)
        // always goes to data cache. Metadata is populated only via put_metadata().
        self.data_cache.put(key, data);
    }

    fn put_metadata(&self, key: &CacheKey, data: Bytes) {
        // Delegate to the inherent method which applies the size bound.
        TieredBlockCache::put_metadata(self, key, data);
    }

    fn evict_prefix(&self, prefix: &str) {
        self.data_cache.evict_prefix(prefix);
        self.metadata_cache.evict_prefix(prefix);
    }

    fn clear(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.data_cache.clear_sync();
            self.metadata_cache.clear_sync();
            native_bridge_common::log_info!("[tiered-block-cache] cleared");
        })
    }
}
