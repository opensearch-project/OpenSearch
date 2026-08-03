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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use crate::foyer::foyer_cache::FoyerCache;
use crate::range_cache::CacheKey;
use crate::traits::BlockCache;

/// Default max metadata entry size: 8MB.
/// Covers up to ~1000-column schemas with page indexes.
const DEFAULT_MAX_METADATA_ENTRY_SIZE: u64 = 8 * 1024 * 1024;

/// Default max data entry size: 32MB.
/// Covers individual column chunks for most schemas. Skips full-RG fetches.
const DEFAULT_MAX_DATA_ENTRY_SIZE: u64 = 32 * 1024 * 1024;

/// A two-tier block cache: metadata cache + data cache on separate SSDs.
///
/// ## Lookup order
///
/// `get()` always probes metadata cache first (small, fast), then data cache.
///
/// ## Write routing
///
/// - `put()` → data cache if entry ≤ max_data_entry_size, else skip.
/// - `put_metadata()` → metadata cache if entry ≤ max_metadata_entry_size, else skip.
///
/// Entries exceeding their respective limits are not cached (returned to caller
/// without caching). This bounds memory usage for large entries.
pub struct TieredBlockCache {
    data_cache: Arc<FoyerCache>,
    metadata_cache: Arc<FoyerCache>,
    /// Max entry size for metadata cache. Entries larger than this are not cached.
    max_metadata_entry_size: AtomicU64,
    /// Max entry size for data cache. Entries larger than this are not cached.
    max_data_entry_size: AtomicU64,
}

impl TieredBlockCache {
    pub fn new(data_cache: Arc<FoyerCache>, metadata_cache: Arc<FoyerCache>) -> Self {
        native_bridge_common::log_info!(
            "[tiered-block-cache] created: data_disk={}B, metadata_disk={}B, \
             max_metadata_entry={}B, max_data_entry={}B",
            data_cache.disk_bytes,
            metadata_cache.disk_bytes,
            DEFAULT_MAX_METADATA_ENTRY_SIZE,
            DEFAULT_MAX_DATA_ENTRY_SIZE
        );
        Self {
            data_cache,
            metadata_cache,
            max_metadata_entry_size: AtomicU64::new(DEFAULT_MAX_METADATA_ENTRY_SIZE),
            max_data_entry_size: AtomicU64::new(DEFAULT_MAX_DATA_ENTRY_SIZE),
        }
    }

    /// Put bytes into the metadata cache. Called during shard warmup only.
    ///
    /// Entries exceeding max_metadata_entry_size are skipped (not cached).
    /// Metadata cache uses LRU eviction on a separate SSD.
    pub fn put_metadata(&self, key: &CacheKey, data: Bytes) {
        let limit = self.max_metadata_entry_size.load(Ordering::Relaxed);
        if data.len() as u64 > limit {
            return;
        }
        self.metadata_cache.put(key, data);
    }

    /// Update max metadata entry size dynamically. Takes effect immediately.
    pub fn update_max_metadata_entry_size(&self, size: u64) {
        self.max_metadata_entry_size.store(size, Ordering::Relaxed);
        native_bridge_common::log_info!(
            "[tiered-block-cache] max_metadata_entry_size updated to {}B",
            size
        );
    }

    /// Update max data entry size dynamically. Takes effect immediately.
    pub fn update_max_data_entry_size(&self, size: u64) {
        self.max_data_entry_size.store(size, Ordering::Relaxed);
        native_bridge_common::log_info!(
            "[tiered-block-cache] max_data_entry_size updated to {}B",
            size
        );
    }

    /// Current max data entry size (used by TieredObjectStore for get_opts threshold).
    pub fn max_data_entry_size(&self) -> u64 {
        self.max_data_entry_size.load(Ordering::Relaxed)
    }

    /// Wait for both caches' flushers to drain. After this, all entries are on
    /// SSD and findable via get(). Used in tests and warmup to ensure durability.
    pub async fn wait_for_flush(&self) {
        self.metadata_cache.wait_for_flush().await;
        self.data_cache.wait_for_flush().await;
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

    fn get<'a>(
        &'a self,
        key: &'a CacheKey,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Bytes>> + Send + 'a>> {
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
        // Data cache put — entries exceeding max_data_entry_size are skipped.
        let limit = self.max_data_entry_size.load(Ordering::Relaxed);
        if data.len() as u64 > limit {
            return;
        }
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
