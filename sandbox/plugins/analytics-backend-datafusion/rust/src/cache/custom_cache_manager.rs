/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use crate::cache::metadata_cache::MutexFileMetadataCache;
use crate::cache::statistics_cache::CustomStatisticsCache;
use crate::cache::statistics_cache::{
    compute_parquet_statistics, compute_parquet_statistics_from_metadata,
};
use crate::cache::{metadata_cache, page_index};
use crate::indexed_table::parquet_bridge;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::execution::cache::cache_manager::{
    CacheManagerConfig, FileMetadataCache, FileStatisticsCache,
};
use datafusion::execution::cache::file_statistics_cache::DefaultFileStatisticsCache;
use datafusion::execution::cache::CacheAccessor;
use log::{debug, error};
use native_bridge_common::log_debug;
use object_store::path::Path;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use opensearch_tiered_storage::tiered_object_store::MetadataCachingStore;
use std::sync::Arc;

/// Compute the page-index regions to warm, as SEPARATE column-index (CI) and
/// offset-index (OI) whole regions — matching exactly how the query-time
/// whole-region fetch (`page_index/page_index_io.rs::whole_index_region`) computes
/// and probes them.
///
/// Each region folds ALL columns across ALL row groups for that one index kind:
/// - CI region = `min(start)..max(end)` over every column×RG `column_index` extent
/// - OI region = `min(start)..max(end)` over every column×RG `offset_index` extent
///
/// Returns up to two ranges (CI first, then OI), skipping whichever kind the file
/// lacks. Returns an empty Vec if the file has no page index at all.
///
/// IMPORTANT: warmup must write these as TWO keys (CI, OI) — NOT one merged
/// CI∪OI range. The query probes the CI whole region and the OI whole region
/// under separate keys, so a single merged key would never match (warm miss).
pub(crate) fn compute_page_index_range(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Vec<std::ops::Range<u64>> {
    // Column-index whole region across all columns × all row groups.
    let ci_region = metadata
        .row_groups()
        .iter()
        .flat_map(|rg| rg.columns().iter())
        .fold(None::<std::ops::Range<u64>>, |acc, col| {
            if let (Some(offset), Some(length)) =
                (col.column_index_offset(), col.column_index_length())
            {
                let start = offset as u64;
                let end = start + length as u64;
                match acc {
                    Some(a) => Some(a.start.min(start)..a.end.max(end)),
                    None => Some(start..end),
                }
            } else {
                acc
            }
        });

    // Offset-index whole region across all columns × all row groups.
    let oi_region = metadata
        .row_groups()
        .iter()
        .flat_map(|rg| rg.columns().iter())
        .fold(None::<std::ops::Range<u64>>, |acc, col| {
            if let (Some(offset), Some(length)) =
                (col.offset_index_offset(), col.offset_index_length())
            {
                let start = offset as u64;
                let end = start + length as u64;
                match acc {
                    Some(a) => Some(a.start.min(start)..a.end.max(end)),
                    None => Some(start..end),
                }
            } else {
                acc
            }
        });

    let mut ranges = Vec::with_capacity(2);
    if let Some(r) = ci_region {
        ranges.push(r);
    }
    if let Some(r) = oi_region {
        ranges.push(r);
    }
    ranges
}

/// Create ObjectMeta from a local file path.
fn create_object_meta_from_file(
    file_path: &str,
) -> Result<Vec<ObjectMeta>, datafusion::common::DataFusionError> {
    use chrono::{DateTime, Utc};
    use datafusion::common::DataFusionError;

    let metadata = std::fs::metadata(file_path).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to get file metadata for {}: {}",
            file_path, e
        ))
    })?;

    let file_size = metadata.len();

    let modified = metadata
        .modified()
        .map(|t| DateTime::<Utc>::from(t))
        .unwrap_or_else(|_| Utc::now());

    let object_meta = ObjectMeta {
        location: Path::from(file_path),
        last_modified: modified,
        size: file_size,
        e_tag: None,
        version: None,
    };

    Ok(vec![object_meta])
}

/// Custom CacheManager that holds cache references directly
pub struct CustomCacheManager {
    /// Direct reference to the file metadata cache
    file_metadata_cache: Option<Arc<MutexFileMetadataCache>>,
    /// Direct reference to the statistics cache
    statistics_cache: Option<Arc<CustomStatisticsCache>>,
    column_index_registered: bool,
    offset_index_registered: bool,
}

impl CustomCacheManager {
    /// Create a new CustomCacheManager
    pub fn new() -> Self {
        Self {
            file_metadata_cache: None,
            statistics_cache: None,
            column_index_registered: false,
            offset_index_registered: false,
        }
    }

    /// Set the file metadata cache
    pub fn set_file_metadata_cache(&mut self, cache: Arc<MutexFileMetadataCache>) {
        self.file_metadata_cache = Some(cache);
        log_debug!("[CACHE INFO] File metadata cache set in CustomCacheManager");
    }

    /// Set the statistics cache
    pub fn set_statistics_cache(&mut self, cache: Arc<CustomStatisticsCache>) {
        self.statistics_cache = Some(cache);
        log_debug!("[CACHE INFO] Statistics cache set in CustomCacheManager");
    }

    /// Register the column index cache with the given size limit.
    /// Sets the limit on the process-global `COLUMN_INDEX_CACHE` singleton.
    pub fn set_column_index_cache(&mut self, size_limit: usize) {
        crate::cache::page_index::set_column_index_cache_limit(size_limit);
        self.column_index_registered = true;
        log_debug!(
            "[CACHE INFO] Column index cache registered (limit={} bytes)",
            size_limit
        );
    }

    /// Register the offset index cache with the given size limit.
    /// Sets the limit on the process-global `OFFSET_INDEX_CACHE` singleton.
    pub fn set_offset_index_cache(&mut self, size_limit: usize) {
        crate::cache::page_index::set_offset_index_cache_limit(size_limit);
        self.offset_index_registered = true;
        log_debug!(
            "[CACHE INFO] Offset index cache registered (limit={} bytes)",
            size_limit
        );
    }

    /// Get the statistics cache
    pub fn get_statistics_cache(&self) -> Option<Arc<CustomStatisticsCache>> {
        self.statistics_cache.clone()
    }

    /// Get the file metadata cache as Arc<dyn FileMetadataCache> for DataFusion
    pub fn get_file_metadata_cache_for_datafusion(&self) -> Option<Arc<dyn FileMetadataCache>> {
        self.file_metadata_cache
            .as_ref()
            .map(|cache| cache.clone() as Arc<dyn FileMetadataCache>)
    }

    /// Build a CacheManagerConfig from the caches stored in this CustomCacheManager
    pub fn build_cache_manager_config(&self) -> CacheManagerConfig {
        let mut config = CacheManagerConfig::default();

        // Add file metadata cache if available
        if let Some(cache) = self.get_file_metadata_cache_for_datafusion() {
            config = config
                .with_file_metadata_cache(Some(cache.clone()))
                .with_metadata_cache_limit(cache.cache_limit());
        }

        // Add statistics cache if available - use CustomStatisticsCache directly
        if let Some(stats_cache) = &self.statistics_cache {
            config = config.with_file_statistics_cache(Some(
                stats_cache.clone() as Arc<dyn FileStatisticsCache>
            ));
        } else {
            // Default statistics cache if none set
            let default_stats = Arc::new(DefaultFileStatisticsCache::default());
            config = config.with_file_statistics_cache(Some(default_stats));
        }

        config
    }

    /// Add multiple files to all applicable caches.
    ///
    /// Footer metadata and statistics are derived from a **single** object-store
    /// read per file: `load_parquet_metadata` fetches the footer, caches it, and
    /// returns `(schema, ParquetMetaData)`. Statistics are then computed from that
    /// already-decoded metadata — avoiding a second file read.
    pub fn add_files(
        &self,
        file_paths: &[String],
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<Vec<(String, bool)>, String> {
        let mut results = Vec::new();

        for file_path in file_paths {
            let mut any_success = false;
            let mut errors = Vec::new();

            // Single footer fetch — warms metadata cache and returns the decoded metadata
            // so statistics can be derived without a second IO round-trip.
            match self.metadata_cache_put_returning_meta(file_path, rt_handle) {
                Ok(Some((schema, pq_meta))) => {
                    any_success = true;

                    // Derive statistics from the already-loaded metadata — no second read.
                    if let Some(stats_cache) = &self.statistics_cache {
                        let path = Path::from(file_path.as_str());
                        if !stats_cache.contains_key(&path) {
                            match compute_parquet_statistics_from_metadata(&pq_meta, &schema) {
                                Ok(stats) => {
                                    let meta = ObjectMeta {
                                        location: path.clone(),
                                        last_modified: chrono::Utc::now(),
                                        size: std::fs::metadata(file_path)
                                            .map(|m| m.len())
                                            .unwrap_or(0),
                                        e_tag: None,
                                        version: None,
                                    };
                                    stats_cache.put_statistics(&path, Arc::new(stats), &meta);
                                }
                                Err(e) => {
                                    errors.push(format!("Statistics cache: {}", e));
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    log_debug!(
                        "[CACHE INFO] File not added for metadata cache: {}",
                        file_path
                    );
                }
                Err(e) => {
                    errors.push(format!("Metadata cache: {}", e));
                }
            }

            let success = if !errors.is_empty() && !any_success {
                false
            } else {
                any_success
            };
            results.push((file_path.clone(), success));
        }

        Ok(results)
    }

    /// Remove multiple files from all caches
    pub fn remove_files(&self, file_paths: &[String]) -> Result<Vec<(String, bool)>, String> {
        let mut results = Vec::new();

        for file_path in file_paths {
            let mut any_removed = false;
            let mut errors = Vec::new();

            // Remove from metadata cache
            {
                let path = Path::from(file_path.clone());
                if let Some(cache) = &self.file_metadata_cache {
                    match cache.inner.lock() {
                        Ok(cache_guard) => {
                            if cache_guard.remove(&path).is_some() {
                                any_removed = true;
                            } else {
                                log_debug!(
                                    "[CACHE INFO] File not found in metadata cache: {}",
                                    file_path
                                );
                            }
                        }
                        Err(e) => {
                            errors.push(format!("Metadata cache: Cache remove failed: {}", e));
                        }
                    }
                } else {
                    errors.push("No metadata cache configured".to_string());
                }
            }

            // Remove from statistics cache
            if let Some(cache) = &self.statistics_cache {
                let path = Path::from(file_path.clone());
                if cache.remove(&path).is_some() {
                    any_removed = true;
                }
            }

            // Evict from scoped page-index caches (CI + OI) by file prefix
            if self.column_index_registered || self.offset_index_registered {
                page_index::evict_file_from_scoped_cache(file_path);
                any_removed = true;
            }

            let removed = if !errors.is_empty() && !any_removed {
                false
            } else {
                any_removed
            };

            results.push((file_path.clone(), removed));
        }

        Ok(results)
    }

    /// Check if a file exists in any cache
    pub fn contains_file(&self, file_path: &str) -> bool {
        let mut found = false;

        // Check metadata cache
        {
            let path = Path::from(file_path);
            if let Some(cache) = &self.file_metadata_cache {
                if cache.get(&path).is_some() {
                    found = true;
                }
            }
        }

        // Check statistics cache
        if let Some(cache) = &self.statistics_cache {
            let path = Path::from(file_path);
            if cache.contains_key(&path) {
                found = true;
            }
        }

        found
    }

    /// Check if a file exists in a specific cache type
    pub fn contains_file_by_type(&self, file_path: &str, cache_type: &str) -> bool {
        match cache_type {
            crate::cache::metadata_cache::CACHE_TYPE_METADATA => {
                let path = Path::from(file_path);
                self.file_metadata_cache
                    .as_ref()
                    .and_then(|cache| cache.get(&path))
                    .is_some()
            }
            crate::cache::metadata_cache::CACHE_TYPE_STATS => self
                .statistics_cache
                .as_ref()
                .map_or(false, |cache| cache.contains_key(&Path::from(file_path))),
            metadata_cache::CACHE_TYPE_COLUMN_INDEX => {
                if !self.column_index_registered {
                    return false;
                }
                let stats = page_index::column_index_cache_stats();
                // CI is keyed by (file, col) — a file is "present" if entries > 0 and
                // we match by prefix; check via evict-probe is heavy so we approximate
                // with entries > 0. A per-file lookup requires iterating DashMap — not
                // worth it for a boolean check; callers use this for diagnostics only.
                stats.entries > 0
            }
            metadata_cache::CACHE_TYPE_OFFSET_INDEX => {
                if !self.offset_index_registered {
                    return false;
                }
                let stats = page_index::offset_index_cache_stats();
                stats.entries > 0
            }
            _ => false,
        }
    }

    /// Update the file metadata cache size limit
    pub fn update_metadata_cache_limit(&self, new_limit: usize) {
        if let Some(cache) = &self.file_metadata_cache {
            cache.update_cache_limit(new_limit);
        }
    }

    /// Update the statistics cache size limit
    pub fn update_statistics_cache_limit(&self, new_limit: usize) -> Result<(), String> {
        if let Some(cache) = &self.statistics_cache {
            cache
                .update_size_limit(new_limit)
                .map_err(|e| format!("Failed to update statistics cache limit: {:?}", e))
        } else {
            Err("No statistics cache configured".to_string())
        }
    }

    /// Get total memory consumed by all caches
    pub fn get_total_memory_consumed(&self) -> usize {
        let mut total = 0;

        // Add metadata cache memory
        if let Some(cache) = &self.file_metadata_cache {
            if let Ok(cache_guard) = cache.inner.lock() {
                total += cache_guard.memory_used();
            }
        }

        // Add statistics cache memory
        if let Some(cache) = &self.statistics_cache {
            total += cache.memory_consumed();
        }

        total
    }

    /// Clear all caches
    pub fn clear_all(&self) {
        if let Some(cache) = &self.file_metadata_cache {
            cache.clear();
        }
        if let Some(cache) = &self.statistics_cache {
            cache.clear();
        }
        if self.column_index_registered || self.offset_index_registered {
            page_index::clear_scoped_cache();
        }
    }

    /// Clear specific cache type
    pub fn clear_cache_type(&self, cache_type: &str) -> Result<(), String> {
        match cache_type {
            metadata_cache::CACHE_TYPE_METADATA => {
                if let Some(cache) = &self.file_metadata_cache {
                    cache.clear();
                    Ok(())
                } else {
                    Err("No metadata cache configured".to_string())
                }
            }
            metadata_cache::CACHE_TYPE_STATS => {
                if let Some(cache) = &self.statistics_cache {
                    cache.clear();
                    Ok(())
                } else {
                    Err("No statistics cache configured".to_string())
                }
            }
            metadata_cache::CACHE_TYPE_COLUMN_INDEX | metadata_cache::CACHE_TYPE_OFFSET_INDEX => {
                page_index::clear_scoped_cache();
                Ok(())
            }
            _ => Err(format!("Unknown cache type: {}", cache_type)),
        }
    }

    /// Get memory consumed by specific cache type
    pub fn get_memory_consumed_by_type(&self, cache_type: &str) -> Result<usize, String> {
        match cache_type {
            metadata_cache::CACHE_TYPE_METADATA => {
                if let Some(cache) = &self.file_metadata_cache {
                    if let Ok(cache_guard) = cache.inner.lock() {
                        Ok(cache_guard.memory_used())
                    } else {
                        Err("Failed to lock metadata cache".to_string())
                    }
                } else {
                    Err("No metadata cache configured".to_string())
                }
            }
            metadata_cache::CACHE_TYPE_STATS => {
                if let Some(cache) = &self.statistics_cache {
                    Ok(cache.memory_consumed())
                } else {
                    Err("No statistics cache configured".to_string())
                }
            }
            metadata_cache::CACHE_TYPE_COLUMN_INDEX => {
                Ok(page_index::column_index_cache_stats().used_bytes)
            }
            metadata_cache::CACHE_TYPE_OFFSET_INDEX => {
                Ok(page_index::offset_index_cache_stats().used_bytes)
            }
            _ => Err(format!("Unknown cache type: {}", cache_type)),
        }
    }

    /// Fetch a parquet file's footer, warm the metadata cache, and return the
    /// decoded `(SchemaRef, Arc<ParquetMetaData>)` so the caller can derive
    /// statistics without a second IO round-trip.
    ///
    /// Returns `Ok(None)` for non-parquet files.
    fn metadata_cache_put_returning_meta(
        &self,
        file_path: &str,
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<
        Option<(
            datafusion::arrow::datatypes::SchemaRef,
            Arc<datafusion::parquet::file::metadata::ParquetMetaData>,
        )>,
        String,
    > {
        if !file_path.to_lowercase().ends_with(".parquet") {
            return Ok(None);
        }

        let object_metas = create_object_meta_from_file(file_path)
            .map_err(|e| format!("Failed to get object metadata: {}", e))?;

        let object_meta = object_metas
            .first()
            .ok_or_else(|| "No object metadata returned".to_string())?;

        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());

        let metadata_cache = self
            .file_metadata_cache
            .as_ref()
            .ok_or_else(|| "No file metadata cache configured".to_string())?
            .clone() as Arc<dyn FileMetadataCache>;

        let location = object_meta.location.clone();
        let meta = object_meta.clone();
        let (schema, _size, pq_meta) = rt_handle.block_on(async {
            parquet_bridge::load_parquet_metadata_with_meta(store, &location, meta, metadata_cache)
                .await
        })?;

        Ok(Some((schema, pq_meta)))
    }

    /// Warmup: load footer (lightweight) into heap, fetch page/offset index bytes
    /// through the store (for Foyer caching), and promote those ranges to the
    /// metadata Foyer tier via `MetadataCachingStore::put_metadata`.
    ///
    /// Key design:
    /// - Footer (schema + RG stats) → heap file_metadata_cache (parsed `ParquetMetaData`, fast path)
    /// - Footer raw bytes (last 64 KB) + page-index bytes → **promoted to metadata Foyer**
    ///   (never-evict SSD tier) via `store.put_metadata(...)`. The parsed footer in heap is
    ///   the fast path; the metadata-Foyer footer is the heap-eviction safety net (served from
    ///   SSD instead of S3 on cold-path re-parse).
    /// - Page indexes NOT stored in heap (avoids 1GB+ memory bloat on wide schemas).
    pub fn add_files_with_store(
        &self,
        file_paths: &[String],
        store: Arc<dyn MetadataCachingStore>,
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<Vec<(String, bool)>, String> {
        let mut results = Vec::with_capacity(file_paths.len());
        for file_path in file_paths {
            match self.warmup_file_with_store(file_path, &store, rt_handle) {
                Ok(success) => {
                    results.push((file_path.clone(), success));
                }
                Err(e) => {
                    error!(
                        "[CACHE ERROR] add_files_with_store failed for {}: {}",
                        file_path, e
                    );
                    results.push((file_path.clone(), false));
                }
            }
        }
        Ok(results)
    }

    /// Warmup a single file
    /// 1. Fetch footer only (PageIndexPolicy::Skip) → heap cache (lightweight parsed metadata)
    /// 2. Derive statistics from the same parsed metadata → statistics cache
    /// 3. Compute page/offset index byte ranges from the parsed footer; append the footer
    ///    range so it's also covered by the metadata Foyer promotion below
    /// 4. Fetch those ranges through the store (populates **data Foyer** as a side effect of get_ranges)
    /// 5. Promote those ranges to **metadata Foyer** (never-evict tier) via `store.put_metadata`
    fn warmup_file_with_store(
        &self,
        file_path: &str,
        store: &Arc<dyn MetadataCachingStore>,
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<bool, String> {
        if !file_path.to_lowercase().ends_with(".parquet") {
            return Ok(false);
        }

        // Step 1: Fetch footer only → heap cache (parsed ParquetMetaData)
        let (parquet_metadata, object_meta) =
            self.fetch_footer_to_heap(file_path, store, rt_handle)?;

        // Step 2: Derive statistics from the same parsed metadata → statistics cache.
        // Reuses `parquet_metadata` so we don't re-fetch through the store. Failures
        // here are non-fatal — they only mean the first query will recompute statistics.
        if let Err(e) =
            self.statistics_cache_put_from_parsed(file_path, &parquet_metadata, &object_meta)
        {
            error!(
                "[warmup::warmup_file_with_store] statistics_cache_put failed for {}: {} (non-fatal)",
                file_path, e
            );
        }

        // Step 3: Compute page/offset index byte ranges from the parsed footer.
        // NOTE: this now returns SEPARATE CI and OI whole regions (up to 2 ranges),
        // matching the query-time whole-region probe keys exactly. A single merged
        // CI∪OI range would never match the query's per-kind probes (warm miss).
        let mut index_ranges = compute_page_index_range(&parquet_metadata);

        // Also promote the footer (last 64 KB) to metadata Foyer — heap-eviction safety net.
        // Heap holds the parsed `ParquetMetaData` for fast lookups; the metadata Foyer entry
        // is the SSD-served re-parse path when the heap evicts under memory pressure.
        let footer_prefetch = 64 * 1024u64; // matches DataFusion's DEFAULT_FOOTER_READ_SIZE
        let footer_start = object_meta.size.saturating_sub(footer_prefetch);
        index_ranges.push(footer_start..object_meta.size);

        // Also promote the exact last-8-byte postscript [size-8, size] to metadata Foyer.
        // DataFusion's FIRST footer read is an 8-byte read of the postscript (4-byte
        // footer-metadata length + "PAR1" magic) to learn the footer length. On an
        // exact-key warm tier that 8-byte read probes key [size-8, size], which the
        // 64 KB footer range above does NOT match — so without this the very first
        // metadata read always misses the warm tier. Promoting the exact 8 bytes under
        // their own key makes that probe a warm hit.
        let postscript_start = object_meta.size.saturating_sub(8);
        index_ranges.push(postscript_start..object_meta.size);

        // Step 4: Fetch the ranges through the store (populates data Foyer on the way).
        let fetched_bytes =
            Self::fetch_ranges_via_store(store, file_path, &index_ranges, rt_handle)?;

        // Step 5: Promote bytes to metadata Foyer (never-evict tier).
        // No-op when `store` is not a TieredObjectStore (default trait impl is no-op).
        store.put_metadata(file_path, &index_ranges, &fetched_bytes);

        native_bridge_common::log_debug!(
            "[warm-tier] warmed file='{}' size={} promoted {} metadata ranges ({} bytes)",
            file_path,
            object_meta.size,
            index_ranges.len(),
            index_ranges.iter().map(|r| r.end - r.start).sum::<u64>()
        );

        Ok(true)
    }

    /// Fetch footer only (no page indexes) from the store and put into heap cache.
    ///
    /// Returns the parsed metadata and object meta (for file size).
    ///
    /// # Panics
    /// Panics if called from within a tokio async context (uses `block_on`).
    /// Must be called from FFM entry points (Java → Rust, non-async) or from
    /// a dedicated synchronous thread. Integration tests use a separate OnceLock runtime.
    fn fetch_footer_to_heap(
        &self,
        file_path: &str,
        store: &Arc<dyn MetadataCachingStore>,
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<(Arc<parquet::file::metadata::ParquetMetaData>, ObjectMeta), String> {
        let path = Path::from(file_path.to_string());

        // Head call to get file size (TieredObjectStore serves from registry)
        let object_meta = rt_handle.block_on(async {
            use object_store::ObjectStoreExt;
            store
                .head(&path)
                .await
                .map_err(|e| format!("Failed to head {}: {}", file_path, e))
        })?;

        let cache_ref = self
            .file_metadata_cache
            .as_ref()
            .ok_or_else(|| "No file metadata cache configured".to_string())?;
        let metadata_cache = cache_ref.clone() as Arc<dyn FileMetadataCache>;

        // Do NOT pass file_metadata_cache here — that triggers PageIndexPolicy::Optional
        // which loads page indexes into the heap struct. Instead, load footer only
        // and manually put into the heap cache afterward.
        let parquet_metadata: Arc<parquet::file::metadata::ParquetMetaData> =
            rt_handle.block_on(async {
                let df_metadata = DFParquetMetadata::new(store.as_ref(), &object_meta);
                df_metadata
                    .fetch_metadata()
                    .await
                    .map_err(|e| format!("Failed to fetch footer: {}", e))
            })?;

        // Put lightweight footer-only metadata into heap cache
        use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
        use datafusion::execution::cache::cache_manager::CachedFileMetadataEntry;
        use datafusion::execution::cache::CacheAccessor;
        let cached_entry = CachedFileMetadataEntry::new(
            object_meta.clone(),
            Arc::new(CachedParquetMetaData::new(Arc::clone(&parquet_metadata))),
        );
        metadata_cache.put(&path, cached_entry);

        Ok((parquet_metadata, object_meta))
    }

    /// Fetch byte ranges from the store. Returns the fetched bytes in order.
    fn fetch_ranges_via_store(
        store: &Arc<dyn MetadataCachingStore>,
        file_path: &str,
        ranges: &[std::ops::Range<u64>],
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<Vec<bytes::Bytes>, String> {
        if ranges.is_empty() {
            return Ok(vec![]);
        }
        let path = Path::from(file_path.to_string());
        rt_handle.block_on(async {
            store
                .get_ranges(&path, ranges)
                .await
                .map_err(|e| format!("Failed to fetch ranges for {}: {}", file_path, e))
        })
    }

    /// Compute and put statistics into cache (hot path — reads the local file)
    ///
    /// Used by [`Self::add_files`] (no-store flow) when the parquet lives on local fs
    /// For warm shards (data on the per-shard remote store) the equivalent helper is
    /// [`Self::statistics_cache_put_from_parsed`], which reuses already-parsed
    /// `ParquetMetaData` from the warmup pass instead of opening the file locally
    pub fn statistics_cache_compute_and_put(&self, file_path: &str) -> Result<bool, String> {
        let cache = self
            .statistics_cache
            .as_ref()
            .ok_or_else(|| "No statistics cache configured".to_string())?;

        let path = Path::from(file_path.to_string());

        // Check if already cached
        if cache.contains_key(&path) {
            return Ok(true);
        }

        // Compute statistics
        match compute_parquet_statistics(file_path) {
            Ok(stats) => {
                let meta = ObjectMeta {
                    location: path.clone(),
                    last_modified: chrono::Utc::now(),
                    size: std::fs::metadata(file_path).map(|m| m.len()).unwrap_or(0),
                    e_tag: None,
                    version: None,
                };

                cache.put_statistics(&path, Arc::new(stats), &meta);
                Ok(true)
            }
            Err(e) => Err(format!(
                "Failed to compute statistics for {}: {}",
                file_path, e
            )),
        }
    }

    /// Warm-tier statistics warmer: derive statistics from already-parsed
    /// [`parquet::file::metadata::ParquetMetaData`] and put them into the statistics
    /// cache, reusing the same [`ObjectMeta`] the heap metadata cache used so
    /// subsequent query lookups validate cleanly via `is_valid_for`.
    ///
    /// Called from [`Self::warmup_file_with_store`] right after the heap metadata
    /// cache is warmed, so we avoid re-fetching the footer through the store
    /// (which is a remote-IO call on warm shards). Returns `Ok(true)` on insert,
    /// `Ok(false)` if the statistics cache is not configured or the file is
    /// already cached, `Err(_)` on schema/derivation failure.
    fn statistics_cache_put_from_parsed(
        &self,
        file_path: &str,
        parquet_metadata: &Arc<parquet::file::metadata::ParquetMetaData>,
        object_meta: &ObjectMeta,
    ) -> Result<bool, String> {
        use datafusion::parquet::arrow::parquet_to_arrow_schema;

        let cache = match self.statistics_cache.as_ref() {
            Some(c) => c,
            None => return Ok(false),
        };

        let path = Path::from(file_path.to_string());
        if cache.contains_key(&path) {
            return Ok(false);
        }

        let file_metadata = parquet_metadata.file_metadata();
        let schema = Arc::new(
            parquet_to_arrow_schema(
                file_metadata.schema_descr(),
                file_metadata.key_value_metadata(),
            )
            .map_err(|e| format!("failed to derive arrow schema for {}: {}", file_path, e))?,
        );

        let stats = DFParquetMetadata::statistics_from_parquet_metadata(parquet_metadata, &schema)
            .map_err(|e| format!("failed to compute statistics for {}: {}", file_path, e))?;

        cache.put_statistics(&path, Arc::new(stats), object_meta);
        Ok(true)
    }

    /// Batch compute and cache statistics for multiple files
    pub fn statistics_cache_batch_compute_and_put(
        &self,
        file_paths: &[String],
    ) -> Result<usize, String> {
        let cache = self
            .statistics_cache
            .as_ref()
            .ok_or_else(|| "No statistics cache configured".to_string())?;

        let mut success_count = 0;
        let mut failed_files = Vec::new();

        for file_path in file_paths {
            let path = Path::from(file_path.clone());

            if cache.contains_key(&path) {
                success_count += 1;
                continue;
            }

            match compute_parquet_statistics(file_path) {
                Ok(stats) => {
                    let meta = ObjectMeta {
                        location: path.clone(),
                        last_modified: chrono::Utc::now(),
                        size: std::fs::metadata(file_path).map(|m| m.len()).unwrap_or(0),
                        e_tag: None,
                        version: None,
                    };

                    cache.put_statistics(&path, Arc::new(stats), &meta);
                    success_count += 1;
                }
                Err(e) => {
                    native_bridge_common::log_debug!(
                        "[STATS CACHE ERROR] Failed to compute statistics for {}: {}",
                        file_path,
                        e
                    );
                    failed_files.push(file_path.clone());
                }
            }
        }

        if !failed_files.is_empty() {
            native_bridge_common::log_debug!(
                "[STATS CACHE WARNING] Failed to compute statistics for {} files: {:?}",
                failed_files.len(),
                failed_files
            );
        }

        Ok(success_count)
    }

    /// Get or compute statistics
    pub fn statistics_cache_get_or_compute(&self, file_path: &str) -> Result<bool, String> {
        let cache = self
            .statistics_cache
            .as_ref()
            .ok_or_else(|| "No statistics cache configured".to_string())?;

        let path = Path::from(file_path.to_string());

        if cache.get(&path).is_some() {
            return Ok(true);
        }

        self.statistics_cache_compute_and_put(file_path)
    }

    /// Get statistics cache hit count
    pub fn statistics_cache_hit_count(&self) -> usize {
        self.statistics_cache
            .as_ref()
            .map(|cache| cache.hit_count())
            .unwrap_or(0)
    }

    /// Get statistics cache miss count
    pub fn statistics_cache_miss_count(&self) -> usize {
        self.statistics_cache
            .as_ref()
            .map(|cache| cache.miss_count())
            .unwrap_or(0)
    }

    /// Get statistics cache hit rate
    pub fn statistics_cache_hit_rate(&self) -> f64 {
        self.statistics_cache
            .as_ref()
            .map(|cache| cache.hit_rate())
            .unwrap_or(0.0)
    }

    /// Get statistics cache entry count
    pub fn statistics_cache_entry_count(&self) -> usize {
        self.statistics_cache
            .as_ref()
            .map(|cache| <CustomStatisticsCache as CacheAccessor<_, _>>::len(cache))
            .unwrap_or(0)
    }

    /// Get statistics cache size limit in bytes
    pub fn statistics_cache_size_limit(&self) -> usize {
        self.statistics_cache
            .as_ref()
            .map(|cache| cache.current_size_limit())
            .unwrap_or(0)
    }

    /// Reset statistics cache stats
    pub fn statistics_cache_reset_stats(&self) {
        if let Some(cache) = &self.statistics_cache {
            cache.reset_stats();
        }
    }

    /// Get metadata cache hit count
    pub fn metadata_cache_hit_count(&self) -> usize {
        self.file_metadata_cache
            .as_ref()
            .map(|cache| cache.hit_count())
            .unwrap_or(0)
    }

    /// Get metadata cache miss count
    pub fn metadata_cache_miss_count(&self) -> usize {
        self.file_metadata_cache
            .as_ref()
            .map(|cache| cache.miss_count())
            .unwrap_or(0)
    }

    /// Get metadata cache entry count
    pub fn metadata_cache_entry_count(&self) -> usize {
        self.file_metadata_cache
            .as_ref()
            .map(|cache| <MutexFileMetadataCache as CacheAccessor<_, _>>::len(cache))
            .unwrap_or(0)
    }

    /// Get metadata cache size limit in bytes
    pub fn metadata_cache_size_limit(&self) -> usize {
        self.file_metadata_cache
            .as_ref()
            .map(|cache| cache.get_cache_limit())
            .unwrap_or(0)
    }

    /// Reset metadata cache stats
    pub fn metadata_cache_reset_stats(&self) {
        if let Some(cache) = &self.file_metadata_cache {
            cache.reset_stats();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::eviction_policy::PolicyType;
    use crate::cache::page_index::{
        clear_scoped_cache_for_test, column_index_cache_stats, offset_index_cache_stats,
        SCOPED_CACHE_TEST_GUARD,
    };
    use crate::cache::{CACHE_TYPE_COLUMN_INDEX, CACHE_TYPE_OFFSET_INDEX};

    #[test]
    fn set_column_index_cache_registers_and_sets_limit() {
        let _g = SCOPED_CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let mut mgr = CustomCacheManager::new();
        assert!(!mgr.column_index_registered);

        let limit = 16 * 1024 * 1024; // 16 MB
        mgr.set_column_index_cache(limit);

        assert!(mgr.column_index_registered);
        assert_eq!(column_index_cache_stats().limit_bytes, limit);

        clear_scoped_cache_for_test();
    }

    #[test]
    fn set_offset_index_cache_registers_and_sets_limit() {
        let _g = SCOPED_CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let mut mgr = CustomCacheManager::new();
        assert!(!mgr.offset_index_registered);

        let limit = 32 * 1024 * 1024; // 32 MB
        mgr.set_offset_index_cache(limit);

        assert!(mgr.offset_index_registered);
        assert_eq!(offset_index_cache_stats().limit_bytes, limit);

        clear_scoped_cache_for_test();
    }

    #[test]
    fn clear_cache_type_column_index_clears_scoped_cache() {
        let _g = SCOPED_CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let mut mgr = CustomCacheManager::new();
        mgr.set_column_index_cache(16 * 1024 * 1024);

        // clear_cache_type must succeed for COLUMN_INDEX
        assert!(mgr.clear_cache_type(CACHE_TYPE_COLUMN_INDEX).is_ok());
        // and for OFFSET_INDEX too (both route to clear_scoped_cache)
        assert!(mgr.clear_cache_type(CACHE_TYPE_OFFSET_INDEX).is_ok());

        clear_scoped_cache_for_test();
    }

    #[test]
    fn get_memory_consumed_by_type_returns_scoped_stats() {
        let _g = SCOPED_CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let mut mgr = CustomCacheManager::new();
        mgr.set_column_index_cache(16 * 1024 * 1024);
        mgr.set_offset_index_cache(32 * 1024 * 1024);

        // On empty cache both return 0 bytes (no entries yet).
        assert_eq!(
            mgr.get_memory_consumed_by_type(CACHE_TYPE_COLUMN_INDEX)
                .unwrap(),
            0
        );
        assert_eq!(
            mgr.get_memory_consumed_by_type(CACHE_TYPE_OFFSET_INDEX)
                .unwrap(),
            0
        );

        clear_scoped_cache_for_test();
    }

    #[test]
    fn remove_files_evicts_scoped_cache_when_registered() {
        let _g = SCOPED_CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let mut mgr = CustomCacheManager::new();
        mgr.set_column_index_cache(16 * 1024 * 1024);

        // Calling remove_files on a non-existent file must not panic.
        let result = mgr.remove_files(&["/nonexistent/file.parquet".to_string()]);
        assert!(result.is_ok());

        clear_scoped_cache_for_test();
    }

    #[test]
    fn clear_all_clears_scoped_cache_when_registered() {
        let _g = SCOPED_CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();

        let mut mgr = CustomCacheManager::new();
        mgr.set_column_index_cache(16 * 1024 * 1024);
        mgr.set_offset_index_cache(32 * 1024 * 1024);

        // Must not panic even with no metadata/stats caches set.
        mgr.clear_all();

        clear_scoped_cache_for_test();
    }

    // ── compute_page_index_range ──────────────────────────────────────────────

    /// Write an in-memory parquet file and return its raw bytes. `stats` controls
    /// whether page indexes (column-index + offset-index) are emitted into the footer.
    fn parquet_bytes(
        num_cols: usize,
        num_rg: usize,
        stats: parquet::file::properties::EnabledStatistics,
    ) -> bytes::Bytes {
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let fields: Vec<Field> = (0..num_cols)
            .map(|c| Field::new(format!("col_{}", c), DataType::Int64, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(stats)
            .build();

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
            for rg in 0..num_rg {
                let base = (rg * 100) as i64;
                let cols: Vec<Arc<dyn arrow::array::Array>> = (0..num_cols)
                    .map(|c| {
                        let vals: Vec<i64> =
                            (base..base + 40).map(|v| v + (c as i64 * 1000)).collect();
                        Arc::new(Int64Array::from(vals)) as Arc<dyn arrow::array::Array>
                    })
                    .collect();
                let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
                w.write(&batch).unwrap();
            }
            w.close().unwrap();
        }
        bytes::Bytes::from(buf)
    }

    /// A page-indexed file yields exactly TWO regions — the column-index (CI) whole
    /// region first, then the offset-index (OI) whole region — and each region is the
    /// tight min..max fold over the corresponding per-column extents in the footer.
    #[test]
    fn compute_page_index_range_returns_tight_ci_and_oi_regions() {
        use parquet::file::properties::EnabledStatistics;
        use parquet::file::reader::FileReader;
        use parquet::file::serialized_reader::SerializedFileReader;

        let buf = parquet_bytes(3, 2, EnabledStatistics::Page);
        let reader = SerializedFileReader::new(buf).unwrap();
        let metadata = reader.metadata();

        let ranges = compute_page_index_range(metadata);
        assert_eq!(
            ranges.len(),
            2,
            "page-indexed file must yield separate CI and OI regions"
        );
        let (ci, oi) = (ranges[0].clone(), ranges[1].clone());
        assert!(ci.end > ci.start, "CI region must be non-empty");
        assert!(oi.end > oi.start, "OI region must be non-empty");
        assert_ne!(
            ci, oi,
            "CI and OI must be distinct warm-tier keys (not merged)"
        );

        // Compute the expected tight folds independently and compare.
        let mut exp_ci: Option<std::ops::Range<u64>> = None;
        let mut exp_oi: Option<std::ops::Range<u64>> = None;
        for rg in metadata.row_groups() {
            for col in rg.columns() {
                if let (Some(off), Some(len)) =
                    (col.column_index_offset(), col.column_index_length())
                {
                    let (s, e) = (off as u64, off as u64 + len as u64);
                    exp_ci = Some(match exp_ci {
                        Some(a) => a.start.min(s)..a.end.max(e),
                        None => s..e,
                    });
                }
                if let (Some(off), Some(len)) =
                    (col.offset_index_offset(), col.offset_index_length())
                {
                    let (s, e) = (off as u64, off as u64 + len as u64);
                    exp_oi = Some(match exp_oi {
                        Some(a) => a.start.min(s)..a.end.max(e),
                        None => s..e,
                    });
                }
            }
        }
        assert_eq!(
            Some(ci),
            exp_ci,
            "CI region must equal the tight min..max fold of all column_index extents"
        );
        assert_eq!(
            Some(oi),
            exp_oi,
            "OI region must equal the tight min..max fold of all offset_index extents"
        );
    }

    /// With page statistics disabled the column index is absent (it requires
    /// page-level stats), though the writer may still emit an offset index. The
    /// function must equal the independent `[CI?, OI?]` fold — skipping absent
    /// kinds — which exercises the "one kind present" branch.
    #[test]
    fn compute_page_index_range_matches_independent_fold_no_page_stats() {
        use parquet::file::properties::EnabledStatistics;
        use parquet::file::reader::FileReader;
        use parquet::file::serialized_reader::SerializedFileReader;

        let buf = parquet_bytes(2, 1, EnabledStatistics::None);
        let reader = SerializedFileReader::new(buf).unwrap();
        let metadata = reader.metadata();

        let mut exp_ci: Option<std::ops::Range<u64>> = None;
        let mut exp_oi: Option<std::ops::Range<u64>> = None;
        for rg in metadata.row_groups() {
            for col in rg.columns() {
                if let (Some(off), Some(len)) =
                    (col.column_index_offset(), col.column_index_length())
                {
                    let (s, e) = (off as u64, off as u64 + len as u64);
                    exp_ci = Some(match exp_ci {
                        Some(a) => a.start.min(s)..a.end.max(e),
                        None => s..e,
                    });
                }
                if let (Some(off), Some(len)) =
                    (col.offset_index_offset(), col.offset_index_length())
                {
                    let (s, e) = (off as u64, off as u64 + len as u64);
                    exp_oi = Some(match exp_oi {
                        Some(a) => a.start.min(s)..a.end.max(e),
                        None => s..e,
                    });
                }
            }
        }
        let mut expected: Vec<std::ops::Range<u64>> = Vec::new();
        if let Some(r) = exp_ci.clone() {
            expected.push(r);
        }
        if let Some(r) = exp_oi {
            expected.push(r);
        }

        assert_eq!(
            compute_page_index_range(metadata),
            expected,
            "result must equal the independent [CI?, OI?] fold, skipping absent index kinds"
        );
        // Column index requires page-level statistics; with stats disabled it must be absent.
        assert!(
            exp_ci.is_none(),
            "column index must be absent when statistics are disabled"
        );
    }
}
