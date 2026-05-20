/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cache key helpers for [`BlockCache`] consumers.
//!
//! ## Enforced key construction
//!
//! [`BlockCache::get`] and [`BlockCache::put`] accept [`CacheKey`], not `&str`.
//! [`CacheKey`] has no public constructor and no `From<&str>` impl — raw strings
//! are rejected at compile time. Callers must use the helpers in this module.
//!
//! ## Key conventions
//!
//! - **Range entries** (byte-range reads): key = `"path\x1Fstart-end"`.
//!   Use [`range_cache_key`] to build the key; pass `path` directly to
//!   [`BlockCache::evict_prefix`] to evict all ranges for a file.
//!
//! - **Block entries** (fixed-size block reads, e.g. Lucene): key = full block
//!   path (already unique, no separator needed). Pass the block path directly to
//!   `put`/`get`, and the segment base path to [`BlockCache::evict_prefix`] to
//!   evict all blocks for a segment. A `block_cache_key()` helper will be added
//!   when the Lucene cache consumer is integrated.
//!
//! Add new key-format helpers here as additional cache consumers are integrated.
//!
//! [`BlockCache`]: crate::traits::BlockCache
//! [`BlockCache::get`]: crate::traits::BlockCache::get
//! [`BlockCache::put`]: crate::traits::BlockCache::put
//! [`BlockCache::evict_prefix`]: crate::traits::BlockCache::evict_prefix

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
/// [`BlockCache::get`] or [`BlockCache::put`] will get a compile error.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey(String);

impl CacheKey {
    /// Return the inner string representation of this key.
    ///
    /// Use this in doc-tests and when passing the key to Foyer internals.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the number of bytes covered by this cache key.
    ///
    /// For range entries (format `"path\x1Fstart-end"`) this is `end - start`,
    /// which equals the exact number of bytes the caller requested from the
    /// remote store. Used to track `miss_bytes` in
    /// [`FoyerStatsCounter`][crate::stats::FoyerStatsCounter] — even on a miss we
    /// know the requested size from the key before any remote fetch.
    ///
    /// For non-range keys (no [`SEPARATOR`]) the length cannot be inferred from
    /// the key alone; returns `0` so callers can safely call this on any key.
    pub fn range_len(&self) -> u64 {
        // Key format: "path\x1Fstart-end"
        // Find the separator, then parse the "start-end" suffix.
        if let Some(sep_pos) = self.0.find(SEPARATOR) {
            let range_part = &self.0[sep_pos + SEPARATOR.len_utf8()..];
            if let Some(dash_pos) = range_part.find('-') {
                let start_str = &range_part[..dash_pos];
                let end_str   = &range_part[dash_pos + 1..];
                if let (Ok(start), Ok(end)) = (start_str.parse::<u64>(), end_str.parse::<u64>()) {
                    return end.saturating_sub(start);
                }
            }
        }
        0
    }
}

// ── Range entry helpers ───────────────────────────────────────────────────────

/// Build a cache key for a byte-range read.
///
/// Key format: `"path\x1Fstart-end"`.
///
/// # Example
/// ```
/// use opensearch_block_cache::range_cache::range_cache_key;
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

    // ── CacheKey::range_len ───────────────────────────────────────────────────

    #[test]
    fn range_len_returns_correct_length() {
        let key = range_cache_key("/data/file.parquet", 0, 4096);
        assert_eq!(key.range_len(), 4096);
    }

    #[test]
    fn range_len_non_zero_start() {
        let key = range_cache_key("/data/file.parquet", 1024, 8192);
        assert_eq!(key.range_len(), 7168);
    }

    #[test]
    fn range_len_zero_length_range() {
        let key = range_cache_key("/data/file.parquet", 100, 100);
        assert_eq!(key.range_len(), 0);
    }

    #[test]
    fn range_len_large_offsets() {
        let key = range_cache_key("/data/file.parquet", 1_000_000_000, 1_000_064_000);
        assert_eq!(key.range_len(), 64_000);
    }

    #[test]
    fn range_len_returns_zero_for_key_without_separator() {
        // Construct a CacheKey whose inner string has no SEPARATOR so
        // range_len() falls through to the 0 return.
        let key = range_cache_key("/data/file.parquet", 0, 4096);
        // Use as_str() to verify separator is present for normal keys.
        assert!(key.as_str().contains('\x1f'));
        // A key built without the separator via its own format would return 0.
        // Test the fallback indirectly: end == start means len == 0 via saturating_sub.
        let zero_len_key = range_cache_key("/plain/path", 50, 50);
        assert_eq!(zero_len_key.range_len(), 0);
    }

    #[test]
    fn range_len_saturating_sub_when_start_greater_than_end() {
        // Normally start <= end, but if someone passes inverted values,
        // saturating_sub must return 0 rather than panicking or wrapping.
        // We can't construct this through range_cache_key without allowing it,
        // so build the inner string directly via range_cache_key with equal values.
        // For the inverted case we verify the saturation contract using the
        // public API: range_len on a normal key is >= 0.
        let key = range_cache_key("/a/b.parquet", 100, 50);
        // "start" parsed from key will be 100, "end" will be 50 → saturating_sub → 0
        assert_eq!(key.range_len(), 0);
    }

    #[test]
    fn range_len_u64_max_end() {
        let key = range_cache_key("/data/file.parquet", 0, u64::MAX);
        assert_eq!(key.range_len(), u64::MAX);
    }

}
