/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cache infrastructure for the analytics backend.
//!
//! # Structure
//!
//! - [`unified`] — the crate-wide cache management interface: [`unified::CacheKind`]
//!   (the closed set of cache types, with per-kind policy rules) and
//!   [`unified::ManagedCache`] (limits, clearing, stats, per-file removal).
//!   Every cache below implements `ManagedCache`; `CustomCacheManager`
//!   dispatches management operations through it uniformly.
//! - [`eviction_policy`] — ONE pluggable eviction policy trait
//!   (`EvictionPolicy<K>`) shared by all caches, with built-in `LruPolicy`,
//!   `LfuPolicy`, and `FifoPolicy`. Add new policies here (e.g. S3-FIFO)
//!   without touching the cache implementations.
//! - [`metadata_cache`] — `MutexFileMetadataCache`: wraps DataFusion's
//!   `DefaultFilesMetadataCache` with hit/miss counters and enforces the
//!   footer-only invariant via `strip_page_index` at every `put`.
//! - [`statistics_cache`] — `CustomStatisticsCache`: byte-bounded LRU cache
//!   for per-file `Statistics` (row-group min/max/null-count).
//! - [`custom_cache_manager`] — `CustomCacheManager`: registry of
//!   `Arc<dyn ManagedCache>` plus the domain flows (warming, file add/remove).
//! - [`page_index`] — scoped parquet page-index caches (ColumnIndex +
//!   OffsetIndex), cell-granular and backed by `BoundedCache` over the same
//!   `EvictionPolicy` trait.

pub mod custom_cache_manager;
pub mod eviction_policy;
pub mod metadata_cache;
pub mod page_index;
pub mod statistics_cache;
pub mod unified;

// Flat re-exports so existing call sites keep working without path changes.
pub use custom_cache_manager::CustomCacheManager;
pub use eviction_policy::{create_policy, CacheResult, EvictionPolicy, PolicyType};
pub use metadata_cache::MutexFileMetadataCache;
pub use statistics_cache::{compute_parquet_statistics, CustomStatisticsCache};
pub use unified::{
    CacheKind, CacheStats, ManagedCache, CACHE_TYPE_COLUMN_INDEX, CACHE_TYPE_METADATA,
    CACHE_TYPE_OFFSET_INDEX, CACHE_TYPE_STATS,
};
