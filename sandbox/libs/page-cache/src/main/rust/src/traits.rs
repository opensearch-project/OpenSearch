/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`PageCache`] trait — the abstraction for byte-range disk caching.

use bytes::Bytes;

/// A disk page cache that stores arbitrary byte ranges keyed by `(path, start, end)`.
///
/// Implementations are expected to be `Send + Sync` so they can be shared
/// across async tasks and threads.
pub trait PageCache: Send + Sync {
    /// Look up a cached byte range. Returns `Some(Bytes)` on hit, `None` on miss.
    fn get(&self, path: &str, start: u64, end: u64)
        -> impl std::future::Future<Output = Option<Bytes>> + Send;

    /// Insert a byte range into the cache.
    fn put(&self, path: &str, start: u64, end: u64, data: Bytes);

    /// Evict all cached ranges for a given file path. A no-op if the path has no entries.
    fn evict_file(&self, path: &str);

    /// Remove all entries from the cache.
    fn clear(&self) -> impl std::future::Future<Output = ()> + Send;
}
