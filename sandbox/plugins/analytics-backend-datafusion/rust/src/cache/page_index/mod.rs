/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Scoped parquet page-index caches — TWO caches, by consumer.
//!
//! # Why this exists
//!
//! Parquet metadata loading pulls the **entire page index** — `ColumnIndex`
//! (per-page min/max; the per-page *string* min/max is the heap hog) plus
//! `OffsetIndex` (per-page byte offsets), for every column of every row group.
//! On wide schemas this is very memory expensive.
//! The level-1 metadata cache is kept footer-only (see
//! [`crate::cache`]); this module rebuilds a *scoped* page index per query and
//! caches it, shared by both scan paths (the DataFusion `ListingTable` path and
//! the custom indexed-table executor).
//!
//! # Two caches, because the two indexes have different drivers
//!
//! The `ColumnIndex` and `OffsetIndex` are consumed by different parts of
//! DataFusion / parquet, with **different natural cache keys**. Forcing
//! them into one key makes the projection-driven OffsetIndex poison the
//! predicate-driven ColumnIndex's broad cross-path sharing (the failure mode of
//! the prior iteration). So they are split:
//!
//! - **ColumnIndex — predicate-driven.** Read only at *prune* time, and only for
//!   the predicate column being evaluated
//!   (`page_filter::PagesPruningStatistics`, `offset_index[rg][predicate_col]`).
//!   Key: `(file, predicate_cols, surviving_rgs)`. Deterministic in the
//!   *predicate* (independent of what you `SELECT`), so the same filter shares
//!   its entry across scan paths **and** across queries with different
//!   projections. This is the heavy index (string min/max) and the big heap win.
//!   Scoped to predicate columns (`NONE` placeholders elsewhere) and, optionally,
//!   to the row groups that pass footer-stats pruning ([`surviving_row_groups`]).
//!
//! - **OffsetIndex — projection-driven.** Read at *scan* time for **projected**
//!   columns (`InMemoryRowGroup::fetch_ranges`, `projection.leaf_included(idx)`),
//!   and at prune time for the predicate column, and at column 0 for the
//!   page-skip metric. Key: `(file, projection_cols)` where
//!   `projection_cols = predicate ∪ projection ∪ {0}`. This is the cheap, fixed-width
//!   index (no per-page string stats). Built for **all row groups** (an empty
//!   OffsetIndex on a row group DataFusion scans panics / breaks reads, and
//!   DataFusion chooses the scanned set itself, after our load).
//!
//! Each cache stores only its decoded vector (`ParquetColumnIndex` /
//! `ParquetOffsetIndex`) — never a full `ParquetMetaData` (no footer
//! duplication). On lookup the two are **grafted** onto the caller's
//! already-resident footer via [`ParquetMetaData::into_builder`] →
//! `set_column_index`/`set_offset_index`.
//!
//! **Consequence for tests:** a lookup returns a *fresh* `Arc`, so `Arc::ptr_eq`
//! is the wrong signal for "served from cache" — assert via the per-cache hit
//! counters ([`column_index_cache_stats`] / [`offset_index_cache_stats`]).
//!
//! ## Correctness / fallback
//!
//! Any failure (file has no page index, a column lacks an index range, a
//! decode/IO error) makes the load return `None`. The caller keeps its
//! footer-only metadata and the pruner conservatively no-ops (scans the whole
//! row group) — never a wrong result.
//!
//! ## Upstream note
//!
//! arrow-rs is moving toward first-class selective metadata decoding
//! (apache/arrow-rs#8643 open; the `ParquetStatisticsPolicy::skip_except` pattern
//! merged in #8797 / #8714 for encoding stats). None yet expose a page-index
//! column/row-group projection, so we hand-roll it with the deprecated
//! [`read_columns_indexes`]/[`read_offset_indexes`] (the only public subset
//! decoders). Migrate to `ParquetMetaDataOptions` when it grows a page-index knob.

pub mod cache_keys;
pub mod cache_store;
pub mod column_schema_resolver;
pub mod page_index_io;

use crate::cache::eviction_policy::CacheEvictionPolicy;
use cache_keys::{CiCellKey, OiCellKey, OiColumn};
use cache_store::{BoundedCache, DEFAULT_SCOPED_CACHE_LIMIT};

use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicBool, Ordering};

/// Process-global kill-switch for the scoped page-index feature.
/// When false, `metadata_cache::put` retains the full page index (fallback mode),
/// and the scoped optimizer/augmentation loops are no-ops.
/// Toggled via `datafusion.scoped_page_index.enabled` dynamic cluster setting.
pub(crate) static SCOPED_PAGE_INDEX_ENABLED: AtomicBool = AtomicBool::new(true);

/// Returns true when the scoped page-index feature is enabled (default).
pub fn is_scoped_page_index_enabled() -> bool {
    SCOPED_PAGE_INDEX_ENABLED.load(Ordering::Relaxed)
}

/// Enable or disable the scoped page-index feature. Called from the Java settings consumer.
pub fn set_scoped_page_index_enabled(enabled: bool) {
    SCOPED_PAGE_INDEX_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Whether the page-index loader fetches each file's WHOLE index region in one
/// `get_ranges` (decode stays scoped) instead of per-column scoped fetches.
///
/// Only worthwhile on a remote/warm object store: the warm tier is keyed by the
/// exact byte-range string, and eager shard-init populates the whole-region range,
/// so a narrower per-column fetch computes a different key and misses. Fetching the
/// whole region makes the query's key match what warming wrote → a warm hit. For
/// local stores there is no warm tier and fewer bytes is better, so it stays off.
/// Set from `create_reader` when a Java-supplied (remote) store is wired in.
pub(crate) static WHOLE_REGION_FETCH_ENABLED: AtomicBool = AtomicBool::new(false);

/// Returns true when whole-region page-index fetch is enabled (remote stores).
pub fn is_whole_region_fetch_enabled() -> bool {
    WHOLE_REGION_FETCH_ENABLED.load(Ordering::Relaxed)
}

/// Enable/disable whole-region page-index fetch. Called from `create_reader` based
/// on whether a remote object store is in use (`store_ptr > 0`).
pub fn set_whole_region_fetch_enabled(enabled: bool) {
    WHOLE_REGION_FETCH_ENABLED.store(enabled, Ordering::Relaxed);
}

pub use cache_store::ScopedCacheStats;
pub use column_schema_resolver::{
    resolve_predicate_parquet_columns, resolve_predicate_parquet_columns_pair,
};
pub use page_index_io::load_scoped_page_index_cols;

// Process-global caches

pub(crate) static COLUMN_INDEX_CACHE: Lazy<BoundedCache<CiCellKey, ColumnIndexMetaData>> =
    Lazy::new(|| {
        BoundedCache::with_named_policy(DEFAULT_SCOPED_CACHE_LIMIT, CacheEvictionPolicy::Fifo)
    });

pub(crate) static OFFSET_INDEX_CACHE: Lazy<BoundedCache<OiCellKey, OiColumn>> = Lazy::new(|| {
    BoundedCache::with_named_policy(DEFAULT_SCOPED_CACHE_LIMIT, CacheEvictionPolicy::Fifo)
});

/// Set the ColumnIndex cache's byte budget. Called from startup wiring with the
/// configured limit. Idempotent; shrinking evicts immediately. Zero ignored.
pub fn set_column_index_cache_limit(limit: usize) {
    if limit > 0 {
        COLUMN_INDEX_CACHE.set_limit(limit);
    }
}

/// Set the OffsetIndex cache's byte budget. Called from startup wiring with the
/// configured limit. Idempotent; shrinking evicts immediately. Zero ignored.
pub fn set_offset_index_cache_limit(limit: usize) {
    if limit > 0 {
        OFFSET_INDEX_CACHE.set_limit(limit);
    }
}

/// Counters + occupancy of the ColumnIndex (predicate-driven) cache. Lock-free.
pub fn column_index_cache_stats() -> ScopedCacheStats {
    COLUMN_INDEX_CACHE.stats()
}

/// Counters + occupancy of the OffsetIndex (projection-driven) cache. Lock-free.
pub fn offset_index_cache_stats() -> ScopedCacheStats {
    OFFSET_INDEX_CACHE.stats()
}

/// Drop all entries and reset counters in BOTH caches, keeping the budgets. For
/// operational testing — reset and re-measure without a cluster restart.
pub fn clear_scoped_cache() {
    COLUMN_INDEX_CACHE.clear_keep_limit();
    OFFSET_INDEX_CACHE.clear_keep_limit();
}

/// Evict all page-index cells for a specific file from both caches.
///
/// Called when a segment file is deleted or replaced so stale cells don't survive
/// in the cache under the same `(path, col, rg)` key. The page-index caches have
/// no freshness check (unlike the metadata cache's `is_valid_for`), so stale cells
/// from a re-written file would otherwise be served as hits — wrong data.
pub fn evict_file_from_scoped_cache(file_path: &str) {
    COLUMN_INDEX_CACHE.evict_by_prefix(file_path);
    OFFSET_INDEX_CACHE.evict_by_prefix(file_path);
}

/// Crate-wide guard so every test that touches the process-global caches mutually
/// excludes (distinct fixtures alone aren't enough — the `InMemory` path is always
/// "data.parquet"). Shared (not per-module) so all cache users serialize.
#[cfg(test)]
pub(crate) static SCOPED_CACHE_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Clear both caches AND restore the default limit on each.
#[cfg(test)]
pub(crate) fn clear_scoped_cache_for_test() {
    COLUMN_INDEX_CACHE.clear_keep_limit();
    COLUMN_INDEX_CACHE.set_limit(DEFAULT_SCOPED_CACHE_LIMIT);
    OFFSET_INDEX_CACHE.clear_keep_limit();
    OFFSET_INDEX_CACHE.set_limit(DEFAULT_SCOPED_CACHE_LIMIT);
}

#[cfg(test)]
pub(crate) fn set_column_index_cache_limit_for_test(limit: usize) {
    COLUMN_INDEX_CACHE.set_limit(limit);
}

/// Combined view (sum of both caches) — test-only convenience for assertions that
/// only need "is the scoped machinery doing anything". Production code reads the
/// two caches separately ([`column_index_cache_stats`] / [`offset_index_cache_stats`]).
#[cfg(test)]
pub(crate) fn scoped_cache_stats() -> ScopedCacheStats {
    let a = column_index_cache_stats();
    let b = offset_index_cache_stats();
    ScopedCacheStats {
        hits: a.hits + b.hits,
        misses: a.misses + b.misses,
        evictions: a.evictions + b.evictions,
        entries: a.entries + b.entries,
        used_bytes: a.used_bytes + b.used_bytes,
        limit_bytes: a.limit_bytes.max(b.limit_bytes),
    }
}
