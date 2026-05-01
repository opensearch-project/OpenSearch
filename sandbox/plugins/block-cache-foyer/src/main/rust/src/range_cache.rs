/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cache key helpers for [`PageCache`] consumers.
//!
//! ## Enforced key construction
//!
//! [`PageCache::get`] and [`PageCache::put`] accept [`CacheKey`], not `&str`.
//! [`CacheKey`] has no public constructor and no `From<&str>` impl — raw strings
//! are rejected at compile time. Callers must use the helpers in this module.
//!
//! ## Key conventions
//!
//! - **Range entries** (byte-range reads): key = `"path\x1Fstart-end"`.
//!   Use [`range_cache_key`] to build the key; pass `path` directly to
//!   [`PageCache::evict_prefix`] to evict all ranges for a file.
//!
//! - **Block entries** (fixed-size block reads, e.g. Lucene): key = full block
//!   path (already unique, no separator needed). Pass the block path directly to
//!   `put`/`get`, and the segment base path to [`PageCache::evict_prefix`] to
//!   evict all blocks for a segment. A `block_cache_key()` helper will be added
//!   when the Lucene cache consumer is integrated.
//!
//! Add new key-format helpers here as additional cache consumers are integrated.
//!
//! [`PageCache`]: crate::traits::PageCache
//! [`PageCache::get`]: crate::traits::PageCache::get
//! [`PageCache::put`]: crate::traits::PageCache::put
//! [`PageCache::evict_prefix`]: crate::traits::PageCache::evict_prefix

/// The separator between a file path and its byte-range suffix in range keys.
///
/// `\x1F` (ASCII Unit Separator, decimal 31) cannot appear in any filesystem
/// path or object-store URL — S3/GCS/Azure percent-encode it as `%1F`.
///
/// Used by [`range_cache_key`] when building keys, and by [`FoyerCache`]
/// internally when parsing keys to derive the index prefix.
///
/// [`FoyerCache`]: crate::foyer::foyer_cache::FoyerCache
pub(crate) const SEPARATOR: char = '\x1f';

// ── CacheKey newtype ──────────────────────────────────────────────────────────

/// Opaque cache key.
///
/// Cannot be constructed from a raw string — use the helpers in this module
/// (e.g. [`range_cache_key`]). This enforces the [`SEPARATOR`] convention at
/// compile time: any caller that tries to pass a `&str` directly to
/// [`PageCache::get`] or [`PageCache::put`] will get a compile error.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey(String);

impl CacheKey {
    /// Return the inner string representation of this key.
    ///
    /// Use this in doc-tests and when passing the key to Foyer internals.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// ── Range entry helpers ───────────────────────────────────────────────────────

/// Build a cache key for a byte-range read.
///
/// Key format: `"path\x1Fstart-end"`.
///
/// # Example
/// ```
/// use opensearch_page_cache::range_cache::range_cache_key;
/// let key = range_cache_key("data/nodes/0/_0.parquet", 0, 4096);
/// assert_eq!(key.as_str(), "data/nodes/0/_0.parquet\x1f0-4096");
/// ```
pub fn range_cache_key(path: &str, start: u64, end: u64) -> CacheKey {
    CacheKey(format!("{}{}{}-{}", path, SEPARATOR, start, end))
}

// ── Future key-format helpers ─────────────────────────────────────────────────
// Add new helpers here when additional cache consumers are integrated.
// For example, block_cache_key() for Lucene IndexInput block caching.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_key_contains_separator() {
        let key = range_cache_key("/data/file.parquet", 0, 4096);
        assert_eq!(key.as_str(), "/data/file.parquet\x1f0-4096");
    }

    #[test]
    fn range_keys_for_same_path_share_index_prefix() {
        let k0 = range_cache_key("/data/file.parquet", 0,    4096);
        let k1 = range_cache_key("/data/file.parquet", 4096, 8192);
        assert!(k0.as_str().starts_with("/data/file.parquet"));
        assert!(k1.as_str().starts_with("/data/file.parquet"));
    }

    #[test]
    fn range_keys_for_different_paths_do_not_share_prefix() {
        let k = range_cache_key("/data/other.parquet", 0, 4096);
        assert!(!k.as_str().starts_with("/data/file.parquet"));
    }

}
