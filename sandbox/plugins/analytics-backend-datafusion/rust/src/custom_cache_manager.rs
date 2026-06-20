/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;
use datafusion::execution::cache::cache_manager::{FileMetadataCache, FileStatisticsCache, CacheManagerConfig};
use datafusion::execution::cache::file_statistics_cache::DefaultFileStatisticsCache;
use datafusion::execution::cache::CacheAccessor;
use crate::statistics_cache::compute_parquet_statistics;
use crate::cache::MutexFileMetadataCache;
use crate::statistics_cache::CustomStatisticsCache;
use object_store::path::Path;
use object_store::ObjectMeta;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use log::{debug, error};

/// Compute ONE global merged range for all page indexes — matching exactly
/// what parquet crate's `range_for_page_index()` produces.
///
/// Folds ALL columns across ALL row groups into a single contiguous range
/// encompassing all column_index and offset_index data. Returns an empty Vec
/// if the file has no page index metadata, or a single-element Vec with the
/// merged range.
pub(crate) fn compute_page_index_range(metadata: &parquet::file::metadata::ParquetMetaData) -> Vec<std::ops::Range<u64>> {
    let page_index_range = metadata.row_groups().iter()
        .flat_map(|rg| rg.columns().iter())
        .fold(None::<std::ops::Range<u64>>, |acc, col| {
            let acc = if let (Some(offset), Some(length)) = (col.column_index_offset(), col.column_index_length()) {
                let start = offset as u64;
                let end = start + length as u64;
                match acc {
                    Some(a) => Some(a.start.min(start)..a.end.max(end)),
                    None => Some(start..end),
                }
            } else {
                acc
            };
            if let (Some(offset), Some(length)) = (col.offset_index_offset(), col.offset_index_length()) {
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
    match page_index_range {
        Some(r) => vec![r],
        None => vec![],
    }
}

/// Create ObjectMeta from a local file path.
fn create_object_meta_from_file(file_path: &str) -> Result<Vec<ObjectMeta>, datafusion::common::DataFusionError> {
    use chrono::{DateTime, Utc};
    use datafusion::common::DataFusionError;

    let metadata = std::fs::metadata(file_path)
        .map_err(|e| DataFusionError::Execution(format!("Failed to get file metadata for {}: {}", file_path, e)))?;

    let file_size = metadata.len();

    let modified = metadata.modified()
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
    statistics_cache: Option<Arc<CustomStatisticsCache>>
}

impl CustomCacheManager {
    /// Create a new CustomCacheManager
    pub fn new() -> Self {
        Self {
            file_metadata_cache: None,
            statistics_cache: None
        }
    }

    /// Set the file metadata cache
    pub fn set_file_metadata_cache(&mut self, cache: Arc<MutexFileMetadataCache>) {
        self.file_metadata_cache = Some(cache);
        debug!("[CACHE INFO] File metadata cache set in CustomCacheManager");
    }

    /// Set the statistics cache
    pub fn set_statistics_cache(&mut self, cache: Arc<CustomStatisticsCache>) {
        self.statistics_cache = Some(cache);
        debug!("[CACHE INFO] Statistics cache set in CustomCacheManager");
    }

    /// Get the statistics cache
    pub fn get_statistics_cache(&self) -> Option<Arc<CustomStatisticsCache>> {
        self.statistics_cache.clone()
    }

    /// Get the file metadata cache as Arc<dyn FileMetadataCache> for DataFusion
    pub fn get_file_metadata_cache_for_datafusion(&self) -> Option<Arc<dyn FileMetadataCache>> {
        self.file_metadata_cache.as_ref().map(|cache| cache.clone() as Arc<dyn FileMetadataCache>)
    }

    /// Build a CacheManagerConfig from the caches stored in this CustomCacheManager
    pub fn build_cache_manager_config(&self) -> CacheManagerConfig {
        let mut config = CacheManagerConfig::default();

        // Add file metadata cache if available
        if let Some(cache) = self.get_file_metadata_cache_for_datafusion() {
            config = config.with_file_metadata_cache(Some(cache.clone()))
                .with_metadata_cache_limit(cache.cache_limit());
        }

        // Add statistics cache if available - use CustomStatisticsCache directly
        if let Some(stats_cache) = &self.statistics_cache {
            config = config.with_file_statistics_cache(Some(stats_cache.clone() as Arc<dyn FileStatisticsCache>));
        } else {
            // Default statistics cache if none set
            let default_stats = Arc::new(DefaultFileStatisticsCache::default());
            config = config.with_file_statistics_cache(Some(default_stats));
        }

        config
    }

    /// Add multiple files to all applicable caches
    pub fn add_files(&self, file_paths: &[String], rt_handle: &tokio::runtime::Handle) -> Result<Vec<(String, bool)>, String> {
        let mut results = Vec::new();

        for file_path in file_paths {
            let mut any_success = false;
            let mut errors = Vec::new();

            // Add to metadata cache
            match self.metadata_cache_put(file_path, rt_handle) {
                Ok(true) => {
                    any_success = true;
                }
                Ok(false) => {
                    debug!("[CACHE INFO] File not added for metadata cache: {}", file_path);
                }
                Err(e) => {
                    errors.push(format!("Metadata cache: {}", e));
                }
            }

            // Add to statistics cache
            if let Some(_) = &self.statistics_cache {
                match self.statistics_cache_compute_and_put(file_path) {
                    Ok(true) => {
                        any_success = true;
                    }
                    Ok(false) => {
                        debug!("[CACHE INFO] File not added for statistics cache: {}", file_path);
                    }
                    Err(e) => {
                        errors.push(format!("Statistics cache: {}", e));
                    }
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
                                debug!("[CACHE INFO] File not found in metadata cache: {}", file_path);
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
            crate::cache::CACHE_TYPE_METADATA => {
                let path = Path::from(file_path);
                self.file_metadata_cache
                    .as_ref()
                    .and_then(|cache| cache.get(&path))
                    .is_some()
            }
            crate::cache::CACHE_TYPE_STATS => {
                self.statistics_cache
                    .as_ref()
                    .map_or(false, |cache| cache.contains_key(&Path::from(file_path)))
            }
            _ => false
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
            cache.update_size_limit(new_limit)
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
    }

    /// Clear specific cache type
    pub fn clear_cache_type(&self, cache_type: &str) -> Result<(), String> {
        match cache_type {
            crate::cache::CACHE_TYPE_METADATA => {
                if let Some(cache) = &self.file_metadata_cache {
                    cache.clear();
                    Ok(())
                } else {
                    Err("No metadata cache configured".to_string())
                }
            }
            crate::cache::CACHE_TYPE_STATS => {
                if let Some(cache) = &self.statistics_cache {
                    cache.clear();
                    Ok(())
                } else {
                    Err("No statistics cache configured".to_string())
                }
            }
            _ => Err(format!("Unknown cache type: {}", cache_type))
        }
    }

    /// Get memory consumed by specific cache type
    pub fn get_memory_consumed_by_type(&self, cache_type: &str) -> Result<usize, String> {
        match cache_type {
            crate::cache::CACHE_TYPE_METADATA => {
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
            crate::cache::CACHE_TYPE_STATS => {
                if let Some(cache) = &self.statistics_cache {
                    Ok(cache.memory_consumed())
                } else {
                    Err("No statistics cache configured".to_string())
                }
            }
            _ => Err(format!("Unknown cache type: {}", cache_type))
        }
    }

    /// Internal method to put metadata into cache
    fn metadata_cache_put(&self, file_path: &str, rt_handle: &tokio::runtime::Handle) -> Result<bool, String> {
        if !file_path.to_lowercase().ends_with(".parquet") {
            return Ok(false); // Skip unsupported formats
        }

        let object_metas = create_object_meta_from_file(file_path)
            .map_err(|e| format!("Failed to get object metadata: {}", e))?;

        let object_meta = object_metas.first()
            .ok_or_else(|| "No object metadata returned".to_string())?;

        let store = Arc::new(object_store::local::LocalFileSystem::new());

        // Get cache reference for DataFusion metadata loading
        let cache_ref = self.file_metadata_cache.as_ref()
            .ok_or_else(|| "No file metadata cache configured".to_string())?;

        let metadata_cache = cache_ref.clone() as Arc<dyn FileMetadataCache>;

        // Use DataFusion's metadata loading by passing reference to file_metadata_cache to get complete metadata
        // IMPORTANT: When a cache is provided to DFParquetMetadata, fetch_metadata() will:
        // 1. Enable page index loading (with_page_indexes(true))
        // 2. Load the complete metadata including column and offset indexes
        // 3. Automatically put the metadata into the cache (lines 155-160 in datafusion's metadata.rs)
        // This ensures we cache exactly what DataFusion would cache during query execution
        let _parquet_metadata = rt_handle.block_on(async {
            let df_metadata = DFParquetMetadata::new(store.as_ref(), object_meta)
                .with_file_metadata_cache(Some(metadata_cache));

            // fetch_metadata() performs the cache put operation internally
            df_metadata.fetch_metadata().await
                .map_err(|e| format!("Failed to fetch metadata: {}", e))
        })?;

        // Verify the metadata was cached properly
        match cache_ref.inner.lock() {
            Ok(cache_guard) => {
                let path = Path::from(file_path.to_string());
                if cache_guard.contains_key(&path) {
                    Ok(true)
                } else {
                    debug!("[CACHE ERROR] Failed to cache metadata for: {}", file_path);
                    Ok(false)
                }
            }
            Err(e) => Err(format!("Failed to verify cache: {}", e))
        }
    }

    /// Warmup: load footer (lightweight) into heap, fetch page/offset index bytes
    /// through the store (for Foyer caching), and return the index byte ranges
    /// so the caller can promote them to metadata Foyer via `ts_put_metadata`.
    ///
    /// Key design:
    /// - Footer (schema + RG stats) → heap file_metadata_cache (lightweight, kept forever)
    /// - Page/offset index bytes → fetched via store (populates data Foyer as side effect)
    ///   → caller promotes to metadata Foyer via ts_put_metadata
    /// - Page indexes NOT stored in heap (avoids 1GB+ memory bloat on wide schemas)
    ///
    /// Returns per-file: (success, Vec<(start, end, bytes)>) for the caller to pass
    /// to `ts_put_metadata`.
    pub fn add_files_with_store(
        &self,
        file_paths: &[String],
        store: Arc<dyn object_store::ObjectStore>,
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<Vec<(String, bool, Vec<(u64, u64, bytes::Bytes)>)>, String> {
        let mut results = Vec::new();
        for file_path in file_paths {
            match self.warmup_file_with_store(file_path, &store, rt_handle) {
                Ok((success, index_ranges)) => results.push((file_path.clone(), success, index_ranges)),
                Err(e) => {
                    error!("[CACHE ERROR] add_files_with_store failed for {}: {}", file_path, e);
                    results.push((file_path.clone(), false, vec![]));
                }
            }
        }
        Ok(results)
    }

    /// Warmup a single file:
    /// 1. Fetch footer only (PageIndexPolicy::Skip) → heap cache (lightweight)
    /// 2. Compute page/offset index ranges from the parsed footer
    /// 3. Fetch those ranges through the store (populates data Foyer)
    /// 4. Return the ranges + bytes for the caller to promote to metadata Foyer
    fn warmup_file_with_store(
        &self,
        file_path: &str,
        store: &Arc<dyn object_store::ObjectStore>,
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<(bool, Vec<(u64, u64, bytes::Bytes)>), String> {
        if !file_path.to_lowercase().ends_with(".parquet") {
            return Ok((false, vec![]));
        }

        // Step 1: Fetch footer only → heap cache
        let (parquet_metadata, object_meta) = self.fetch_footer_to_heap(file_path, store, rt_handle)?;

        // Step 2: Compute page/offset index byte ranges from the parsed footer
        let mut index_ranges = compute_page_index_range(&parquet_metadata);

        // Also include the footer range itself for metadata Foyer
        let footer_prefetch = 64 * 1024u64; // same as DataFusion's typical prefetch
        let footer_start = object_meta.size.saturating_sub(footer_prefetch);
        index_ranges.push(footer_start..object_meta.size);

        // Step 3: Fetch index byte ranges through the store (populates data Foyer on the way)
        let fetched_bytes = Self::fetch_ranges_via_store(store, file_path, &index_ranges, rt_handle)?;

        // Step 4: Return ranges + bytes for caller to put into metadata Foyer
        let metadata_entries: Vec<(u64, u64, bytes::Bytes)> = index_ranges.iter()
            .zip(fetched_bytes.into_iter())
            .map(|(r, b)| (r.start, r.end, b))
            .collect();

        Ok((true, metadata_entries))
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
        store: &Arc<dyn object_store::ObjectStore>,
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<(Arc<parquet::file::metadata::ParquetMetaData>, ObjectMeta), String> {
        let path = Path::from(file_path.to_string());

        // Head call to get file size (TieredObjectStore serves from registry)
        let object_meta = rt_handle.block_on(async {
            use object_store::ObjectStoreExt;
            store.head(&path).await
                .map_err(|e| format!("Failed to head {}: {}", file_path, e))
        })?;

        let cache_ref = self.file_metadata_cache.as_ref()
            .ok_or_else(|| "No file metadata cache configured".to_string())?;
        let metadata_cache = cache_ref.clone() as Arc<dyn FileMetadataCache>;

        // Do NOT pass file_metadata_cache here — that triggers PageIndexPolicy::Optional
        // which loads page indexes into the heap struct. Instead, load footer only
        // and manually put into the heap cache afterward.
        let parquet_metadata: Arc<parquet::file::metadata::ParquetMetaData> = rt_handle.block_on(async {
            let df_metadata = DFParquetMetadata::new(store.as_ref(), &object_meta);
            df_metadata.fetch_metadata().await
                .map_err(|e| format!("Failed to fetch footer: {}", e))
        })?;

        // Put lightweight footer-only metadata into heap cache
        use datafusion::execution::cache::cache_manager::CachedFileMetadataEntry;
        use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
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
        store: &Arc<dyn object_store::ObjectStore>,
        file_path: &str,
        ranges: &[std::ops::Range<u64>],
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<Vec<bytes::Bytes>, String> {
        if ranges.is_empty() {
            return Ok(vec![]);
        }
        let path = Path::from(file_path.to_string());
        rt_handle.block_on(async {
            store.get_ranges(&path, ranges).await
                .map_err(|e| format!("Failed to fetch ranges for {}: {}", file_path, e))
        })
    }

    /// Compute and put statistics into cache
    pub fn statistics_cache_compute_and_put(&self, file_path: &str) -> Result<bool, String> {
        let cache = self.statistics_cache.as_ref()
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
                    size: std::fs::metadata(file_path)
                        .map(|m| m.len())
                        .unwrap_or(0),
                    e_tag: None,
                    version: None,
                };

                cache.put_statistics(&path, Arc::new(stats), &meta);
                Ok(true)
            }
            Err(e) => {
                Err(format!("Failed to compute statistics for {}: {}", file_path, e))
            }
        }
    }

    /// Batch compute and cache statistics for multiple files
    pub fn statistics_cache_batch_compute_and_put(&self, file_paths: &[String]) -> Result<usize, String> {
        let cache = self.statistics_cache.as_ref()
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
                        size: std::fs::metadata(file_path)
                            .map(|m| m.len())
                            .unwrap_or(0),
                        e_tag: None,
                        version: None,
                    };

                    cache.put_statistics(&path, Arc::new(stats), &meta);
                    success_count += 1;
                }
                Err(e) => {
                    debug!("[STATS CACHE ERROR] Failed to compute statistics for {}: {}", file_path, e);
                    failed_files.push(file_path.clone());
                }
            }
        }

        if !failed_files.is_empty() {
            debug!("[STATS CACHE WARNING] Failed to compute statistics for {} files: {:?}",
                      failed_files.len(), failed_files);
        }

        Ok(success_count)
    }

    /// Get or compute statistics
    pub fn statistics_cache_get_or_compute(&self, file_path: &str) -> Result<bool, String> {
        let cache = self.statistics_cache.as_ref()
            .ok_or_else(|| "No statistics cache configured".to_string())?;

        let path = Path::from(file_path.to_string());

        if cache.get(&path).is_some() {
            return Ok(true);
        }

        self.statistics_cache_compute_and_put(file_path)
    }

    /// Get statistics cache hit count
    pub fn statistics_cache_hit_count(&self) -> usize {
        self.statistics_cache.as_ref()
            .map(|cache| cache.hit_count())
            .unwrap_or(0)
    }

    /// Get statistics cache miss count
    pub fn statistics_cache_miss_count(&self) -> usize {
        self.statistics_cache.as_ref()
            .map(|cache| cache.miss_count())
            .unwrap_or(0)
    }

    /// Get statistics cache hit rate
    pub fn statistics_cache_hit_rate(&self) -> f64 {
        self.statistics_cache.as_ref()
            .map(|cache| cache.hit_rate())
            .unwrap_or(0.0)
    }

    /// Get statistics cache entry count
    pub fn statistics_cache_entry_count(&self) -> usize {
        self.statistics_cache.as_ref()
            .map(|cache| <CustomStatisticsCache as CacheAccessor<_, _>>::len(cache))
            .unwrap_or(0)
    }

    /// Get statistics cache size limit in bytes
    pub fn statistics_cache_size_limit(&self) -> usize {
        self.statistics_cache.as_ref()
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
        self.file_metadata_cache.as_ref()
            .map(|cache| cache.hit_count())
            .unwrap_or(0)
    }

    /// Get metadata cache miss count
    pub fn metadata_cache_miss_count(&self) -> usize {
        self.file_metadata_cache.as_ref()
            .map(|cache| cache.miss_count())
            .unwrap_or(0)
    }

    /// Get metadata cache entry count
    pub fn metadata_cache_entry_count(&self) -> usize {
        self.file_metadata_cache.as_ref()
            .map(|cache| <MutexFileMetadataCache as CacheAccessor<_, _>>::len(cache))
            .unwrap_or(0)
    }

    /// Get metadata cache size limit in bytes
    pub fn metadata_cache_size_limit(&self) -> usize {
        self.file_metadata_cache.as_ref()
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
