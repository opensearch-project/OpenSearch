/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`BlockCache`] trait — the abstraction for disk caching with typed keys.

use crate::range_cache::CacheKey;
use bytes::Bytes;

/// Outcome of a cache write.
///
/// Each tier bounds the size of a single entry, so a `put` is not guaranteed to be
/// accepted. Callers that report progress upwards — shard warmup in particular —
/// must distinguish the two outcomes rather than assuming the write landed, or they
/// end up reporting bytes as cached that were never stored.
///
/// `Accepted` means the cache took ownership of the bytes, **not** that they are
/// durable yet: the write may still be in flight to disk. Use
/// `TieredBlockCache::wait_for_flush` when durability matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    /// Handed to the cache. May still be in flight to disk.
    Accepted,
    /// Not cached at all: the entry is larger than `limit` bytes.
    Rejected {
        /// Size of the entry that was refused.
        len: usize,
        /// The tier's maximum entry size at the time of the call.
        limit: u64,
    },
}

impl PutOutcome {
    /// `true` when the entry was refused outright and is not in the cache.
    pub fn is_rejected(&self) -> bool {
        matches!(self, PutOutcome::Rejected { .. })
    }
}

/// A disk block cache.
///
/// Keys are [`CacheKey`] values — opaque newtypes that can only be constructed
/// via the helpers in [`crate::range_cache`]. This enforces the `\x1F` separator
/// convention at compile time and prevents accidental use of raw strings.
///
/// ## Eviction
///
/// `evict_prefix` still accepts `&str` because the eviction prefix is the bare
/// file path (no separator) — there is nothing to encode, and any valid path
/// string is a correct eviction prefix.
///
/// Implementations must be `Send + Sync` so they can be shared across async
/// tasks and threads.
pub trait BlockCache: Send + Sync + std::any::Any {
    /// Look up a cached entry. Returns `Some(Bytes)` on hit, `None` on miss.
    fn as_any(&self) -> &dyn std::any::Any;
    fn get<'a>(
        &'a self,
        key: &'a CacheKey,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Bytes>> + Send + 'a>>;

    /// Insert bytes under the given key (data cache — evictable by LRU).
    ///
    /// Returns [`PutOutcome::Rejected`] when the entry exceeds the tier's maximum
    /// entry size, in which case nothing is cached.
    fn put(&self, key: &CacheKey, data: Bytes) -> PutOutcome;

    /// Insert bytes into the metadata cache (never evicted by LRU).
    ///
    /// For caches without a separate metadata tier (e.g., `FoyerCache` used standalone),
    /// this falls back to `put()` — which is correct since the single-tier cache does
    /// not distinguish metadata from data. `TieredBlockCache` overrides this to route
    /// metadata to its dedicated non-evictable metadata cache.
    ///
    /// Called by the warmup path to ensure metadata bytes are stored in the durable
    /// (non-evictable) tier, surviving LRU pressure from data scan workloads. Warmup
    /// must honour [`PutOutcome::Rejected`] instead of reporting the range as promoted.
    fn put_metadata(&self, key: &CacheKey, data: Bytes) -> PutOutcome {
        self.put(key, data)
    }

    /// Evict all entries whose key starts with `prefix`. A no-op if nothing matches.
    ///
    /// For range entries: pass the file path — evicts all byte-range keys for that file.
    /// For block entries: pass the segment base path — evicts all block keys for that segment.
    fn evict_prefix(&self, prefix: &str);

    /// Remove all entries from the cache.
    fn clear(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;
}
