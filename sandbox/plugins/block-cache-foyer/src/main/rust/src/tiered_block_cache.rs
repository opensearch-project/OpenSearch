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
use crate::traits::{BlockCache, PutOutcome};

/// Default max metadata entry size: 8MB.
/// Covers up to ~1000-column schemas with page indexes.
const DEFAULT_MAX_METADATA_ENTRY_SIZE: u64 = 8 * 1024 * 1024;

/// Default max data entry size: 32MB.
/// Covers individual column chunks for most schemas. Skips full-RG fetches.
const DEFAULT_MAX_DATA_ENTRY_SIZE: u64 = 32 * 1024 * 1024;

/// Bytes reserved per Foyer block for everything that is not the entry payload.
///
/// Foyer's block engine can only store an entry when its serialized form fits in
/// `block_size - blob_index_size`, and it rounds each entry up to a 4 KiB page:
/// `max_entry_size = block_size - blob_index_size` in `foyer-storage`'s
/// `engine::block::flusher`, checked as `aligned > max_entry_size` in
/// `engine::block::buffer`'s `Buffer::push`. `blob_index_size` defaults to 4 KiB,
/// page alignment can round up by nearly another 4 KiB, and the entry header plus
/// the key (a file path with a byte-range suffix) take a few hundred bytes more.
///
/// This margin matters because an entry that overshoots is **silently discarded** by
/// Foyer — the piece is dropped, only an internal counter moves, and nothing is
/// logged or returned. Keeping our own limit below the real ceiling converts that
/// invisible loss into a [`PutOutcome::Rejected`] the caller can see and report.
const FOYER_BLOCK_OVERHEAD_BYTES: u64 = 16 * 1024;

/// Largest entry the given Foyer block size can actually hold, with margin.
///
/// Saturates to 0 for a block size at or below the overhead margin, which makes
/// every entry rejected rather than silently dropped — the safe direction.
fn foyer_entry_ceiling(block_size: usize) -> u64 {
    (block_size as u64).saturating_sub(FOYER_BLOCK_OVERHEAD_BYTES)
}

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
/// Entries exceeding their respective limits are not cached; the caller is told so
/// via [`PutOutcome::Rejected`]. This bounds memory usage for large entries.
///
/// ## Entry-size limits are clamped to what Foyer can store
///
/// Each limit is the smaller of the configured value and
/// [`foyer_entry_ceiling`] of that tier's block size. Without the clamp an entry
/// between the two values is accepted here, counted in `used_bytes` and the
/// `key_index`, and then silently discarded inside Foyer's flusher — so the node
/// reports the bytes as cached while every read of them misses. Clamping keeps the
/// rejection visible and the accounting honest.
pub struct TieredBlockCache {
    data_cache: Arc<FoyerCache>,
    metadata_cache: Arc<FoyerCache>,
    /// Max entry size for metadata cache. Entries larger than this are not cached.
    /// Never exceeds `metadata_entry_ceiling`.
    max_metadata_entry_size: AtomicU64,
    /// Max entry size for data cache. Entries larger than this are not cached.
    /// Never exceeds `data_entry_ceiling`.
    max_data_entry_size: AtomicU64,
    /// Largest entry the metadata tier's Foyer block size can physically store.
    /// Caps `max_metadata_entry_size`, including on live updates.
    metadata_entry_ceiling: u64,
    /// Largest entry the data tier's Foyer block size can physically store.
    /// Caps `max_data_entry_size`, including on live updates.
    data_entry_ceiling: u64,
}

impl TieredBlockCache {
    pub fn new(data_cache: Arc<FoyerCache>, metadata_cache: Arc<FoyerCache>) -> Self {
        let metadata_entry_ceiling = foyer_entry_ceiling(metadata_cache.block_size);
        let data_entry_ceiling = foyer_entry_ceiling(data_cache.block_size);
        let max_metadata_entry_size = DEFAULT_MAX_METADATA_ENTRY_SIZE.min(metadata_entry_ceiling);
        let max_data_entry_size = DEFAULT_MAX_DATA_ENTRY_SIZE.min(data_entry_ceiling);

        native_bridge_common::log_info!(
            "[tiered-block-cache] created: data_disk={}B, metadata_disk={}B, \
             max_metadata_entry={}B (ceiling={}B from block_size={}B), \
             max_data_entry={}B (ceiling={}B from block_size={}B)",
            data_cache.disk_bytes,
            metadata_cache.disk_bytes,
            max_metadata_entry_size,
            metadata_entry_ceiling,
            metadata_cache.block_size,
            max_data_entry_size,
            data_entry_ceiling,
            data_cache.block_size
        );
        // A block size small enough to clamp the configured limit is worth calling out:
        // entries between the two sizes are now refused, where previously they were
        // accepted and then silently lost inside Foyer.
        if metadata_entry_ceiling < DEFAULT_MAX_METADATA_ENTRY_SIZE {
            native_bridge_common::log_info!(
                "[tiered-block-cache] metadata entry limit clamped {}B -> {}B because \
                 metadata block_size={}B cannot store larger entries; raise \
                 block_cache.foyer.metadata_block_size to cache bigger metadata ranges",
                DEFAULT_MAX_METADATA_ENTRY_SIZE,
                max_metadata_entry_size,
                metadata_cache.block_size
            );
        }
        if data_entry_ceiling < DEFAULT_MAX_DATA_ENTRY_SIZE {
            native_bridge_common::log_info!(
                "[tiered-block-cache] data entry limit clamped {}B -> {}B because \
                 data block_size={}B cannot store larger entries; raise \
                 block_cache.foyer.block_size to cache bigger ranges",
                DEFAULT_MAX_DATA_ENTRY_SIZE,
                max_data_entry_size,
                data_cache.block_size
            );
        }

        Self {
            data_cache,
            metadata_cache,
            max_metadata_entry_size: AtomicU64::new(max_metadata_entry_size),
            max_data_entry_size: AtomicU64::new(max_data_entry_size),
            metadata_entry_ceiling,
            data_entry_ceiling,
        }
    }

    /// Put bytes into the metadata cache. Called during shard warmup only.
    ///
    /// Entries exceeding max_metadata_entry_size are not cached and reported as
    /// [`PutOutcome::Rejected`]. Warmup must surface that instead of counting the
    /// range as promoted — a rejected range is served from the remote store on every
    /// subsequent read.
    ///
    /// Metadata cache uses LRU eviction on a separate SSD.
    pub fn put_metadata(&self, key: &CacheKey, data: Bytes) -> PutOutcome {
        let limit = self.max_metadata_entry_size.load(Ordering::Relaxed);
        let len = data.len();
        if len as u64 > limit {
            // Info, not debug: metadata promotion happens once per file at warmup, so this
            // is low volume, and a rejected metadata range costs a remote fetch on every
            // query that touches the file. The per-file summary in the warmup path raises
            // this to error level when any range was refused.
            native_bridge_common::log_info!(
                "[tiered-block-cache] metadata entry NOT cached: key='{}' len={}B \
                 exceeds max_metadata_entry={}B (metadata block_size={}B). Reads of this \
                 range will fall back to the remote store.",
                key.as_str(),
                len,
                limit,
                self.metadata_cache.block_size
            );
            return PutOutcome::Rejected { len, limit };
        }
        self.metadata_cache.put(key, data)
    }

    /// Update max metadata entry size dynamically. Takes effect immediately.
    ///
    /// Clamped to what the metadata tier's Foyer block size can store; a larger
    /// request cannot be honoured because Foyer would discard such entries silently.
    pub fn update_max_metadata_entry_size(&self, size: u64) {
        let clamped = size.min(self.metadata_entry_ceiling);
        self.max_metadata_entry_size
            .store(clamped, Ordering::Relaxed);
        if clamped == size {
            native_bridge_common::log_info!(
                "[tiered-block-cache] max_metadata_entry_size updated to {}B",
                clamped
            );
        } else {
            native_bridge_common::log_info!(
                "[tiered-block-cache] max_metadata_entry_size requested {}B, clamped to \
                 {}B (metadata block_size={}B cannot store larger entries)",
                size,
                clamped,
                self.metadata_cache.block_size
            );
        }
    }

    /// Update max data entry size dynamically. Takes effect immediately.
    ///
    /// Clamped to what the data tier's Foyer block size can store; see
    /// [`Self::update_max_metadata_entry_size`].
    pub fn update_max_data_entry_size(&self, size: u64) {
        let clamped = size.min(self.data_entry_ceiling);
        self.max_data_entry_size.store(clamped, Ordering::Relaxed);
        if clamped == size {
            native_bridge_common::log_info!(
                "[tiered-block-cache] max_data_entry_size updated to {}B",
                clamped
            );
        } else {
            native_bridge_common::log_info!(
                "[tiered-block-cache] max_data_entry_size requested {}B, clamped to {}B \
                 (data block_size={}B cannot store larger entries)",
                size,
                clamped,
                self.data_cache.block_size
            );
        }
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

    fn put(&self, key: &CacheKey, data: Bytes) -> PutOutcome {
        // Data cache put — entries exceeding max_data_entry_size are skipped.
        let limit = self.max_data_entry_size.load(Ordering::Relaxed);
        let len = data.len();
        if len as u64 > limit {
            // Debug, not warn: this is the query hot path and a rejected data range is
            // merely uncached, not lost — the reader already holds the bytes. Volume here
            // can be high, so keep it out of the default log level.
            native_bridge_common::log_debug!(
                "[tiered-block-cache] data entry NOT cached: key='{}' len={}B exceeds \
                 max_data_entry={}B (data block_size={}B)",
                key.as_str(),
                len,
                limit,
                self.data_cache.block_size
            );
            return PutOutcome::Rejected { len, limit };
        }
        self.data_cache.put(key, data)
    }

    fn put_metadata(&self, key: &CacheKey, data: Bytes) -> PutOutcome {
        // Delegate to the inherent method which applies the size bound.
        TieredBlockCache::put_metadata(self, key, data)
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
