/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`BlockCache`] trait — the abstraction for disk caching with typed keys.

use bytes::Bytes;
use crate::range_cache::CacheKey;

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
    fn get<'a>(&'a self, key: &'a CacheKey)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Bytes>> + Send + 'a>>;

    /// Insert bytes under the given key.
    fn put(&self, key: &CacheKey, data: Bytes);

    /// Evict all entries whose key starts with `prefix`. A no-op if nothing matches.
    ///
    /// For range entries: pass the file path — evicts all byte-range keys for that file.
    /// For block entries: pass the segment base path — evicts all block keys for that segment.
    fn evict_prefix(&self, prefix: &str);

    /// Remove all entries from the cache.
    fn clear(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;
}
