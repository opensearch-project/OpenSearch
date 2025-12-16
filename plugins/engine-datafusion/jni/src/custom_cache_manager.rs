use std::sync::{Arc};
use datafusion::execution::cache::cache_manager::{FileMetadataCache, FileStatisticsCache, CacheManagerConfig};
use datafusion::execution::cache::cache_unit::{DefaultFileStatisticsCache};
use datafusion::execution::cache::CacheAccessor;
use crate::statistics_cache::compute_parquet_statistics;
use tokio::runtime::Runtime;
use crate::cache::MutexFileMetadataCache;
use crate::statistics_cache::CustomStatisticsCache;
use crate::util::{create_object_meta_from_file};
use object_store::path::Path;
use object_store::ObjectMeta;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use opensearch_vectorized_spi::{rust_log_debug, rust_log_error};

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
        rust_log_debug!("[CACHE INFO] File metadata cache set in CustomCacheManager");
    }

    /// Set the statistics cache
    pub fn set_statistics_cache(&mut self, cache: Arc<CustomStatisticsCache>) {
        self.statistics_cache = Some(cache);
        rust_log_debug!("[CACHE INFO] Statistics cache set in CustomCacheManager");
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
            config = config.with_files_statistics_cache(Some(stats_cache.clone() as FileStatisticsCache));
        } else {
            // Default statistics cache if none set
            let default_stats = Arc::new(DefaultFileStatisticsCache::default());
            config = config.with_files_statistics_cache(Some(default_stats));
        }

        config
    }

    /// Add multiple files to all applicable caches
    pub fn add_files(&self, file_paths: &[String]) -> Result<Vec<(String, bool)>, String> {
        let mut results = Vec::new();

        for file_path in file_paths {
            let mut any_success = false;
            let mut errors = Vec::new();

            // Add to metadata cache
            match self.metadata_cache_put(file_path) {
                Ok(true) => {
                    any_success = true;
                }
                Ok(false) => {
                    rust_log_debug!("[CACHE INFO] File not added for metadata cache: {}", file_path);
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
                        rust_log_debug!("[CACHE INFO] File not added for statistics cache: {}", file_path);
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
            match create_object_meta_from_file(file_path) {
                Ok(object_metas) => {
                    // Get the cache directly from our stored reference
                    if let Some(cache) = &self.file_metadata_cache {
                        match cache.inner.lock() {
                            Ok(mut cache_guard) => {
                                // Remove the first ObjectMeta from the vector
                                if let Some(object_meta) = object_metas.first() {
                                    if cache_guard.remove(object_meta).is_some() {
                                        any_removed = true;
                                    } else {
                                        rust_log_debug!("[CACHE INFO] File not found in metadata cache: {}", file_path);
                                    }
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
                Err(e) => {
                    errors.push(format!("Failed to get object metadata: {}", e));
                }
            }

            // Remove from statistics cache
            if let Some(cache) = &self.statistics_cache {
                let path = Path::from(file_path.clone());
                // Use contains_key to check if the entry exists before attempting removal
                if cache.contains_key(&path) {
                    // Since we can't call remove directly on Arc<CustomStatisticsCache>,
                    // we need to use the thread-safe DashMap operations
                    if cache.inner().remove(&path).is_some() {
                        any_removed = true;
                    }
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
        match create_object_meta_from_file(file_path) {
            Ok(object_metas) => {
                if let Some(cache) = &self.file_metadata_cache {
                    if let Some(object_meta) = object_metas.first() {
                        match cache.get(object_meta) {
                            Some(metadata) => {
                                found = true;
                            },
                            None => {
                                rust_log_debug!("No metadata found for: {}", file_path);
                            },
                        }
                    }
                }
            }
            Err(e) => {
                rust_log_error!("Failed to get object metadata for {}: {}", file_path, e);
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

    /// Update the file metadata cache size limit
    pub fn update_metadata_cache_limit(&self, new_limit: usize) {
        if let Some(cache) = &self.file_metadata_cache {
            cache.update_cache_limit(new_limit);
        }
    }

    /// Update the statistics cache size limit
    pub fn update_statistics_cache_limit(&self, new_limit: usize) -> Result<(), String> {
        if let Some(cache) = &self.statistics_cache {
            // Need mutable reference for update_size_limit
            let cache_mut = unsafe { &mut *(Arc::as_ptr(cache) as *mut CustomStatisticsCache) };
            cache_mut.update_size_limit(new_limit)
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
    fn metadata_cache_put(&self, file_path: &str) -> Result<bool, String> {
        let data_format = if file_path.to_lowercase().ends_with(".parquet") {
            "parquet"
        } else {
            return Ok(false); // Skip unsupported formats
        };

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
        let _parquet_metadata = Runtime::new()
            .map_err(|e| format!("Failed to create Tokio Runtime: {}", e))?
            .block_on(async {
                let df_metadata = DFParquetMetadata::new(store.as_ref(), object_meta)
                    .with_file_metadata_cache(Some(metadata_cache));

                // fetch_metadata() performs the cache put operation internally
                df_metadata.fetch_metadata().await
                    .map_err(|e| format!("Failed to fetch metadata: {}", e))
            })?;

        // Verify the metadata was cached properly
        match cache_ref.inner.lock() {
            Ok(cache_guard) => {
                if cache_guard.contains_key(object_meta) {
                    Ok(true)
                } else {
                    rust_log_debug!("[CACHE ERROR] Failed to cache metadata for: {}", file_path);
                    Ok(false)
                }
            }
            Err(e) => Err(format!("Failed to verify cache: {}", e))
        }
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

                cache.put_with_extra(&path, Arc::new(stats), &meta);
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

            // Skip if already cached
            if cache.contains_key(&path) {
                success_count += 1;
                continue;
            }

            // Compute and cache statistics
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

                    cache.put_with_extra(&path, Arc::new(stats), &meta);
                    success_count += 1;
                }
                Err(e) => {
                    rust_log_debug!("[STATS CACHE ERROR] Failed to compute statistics for {}: {}", file_path, e);
                    failed_files.push(file_path.clone());
                }
            }
        }

        if !failed_files.is_empty() {
            rust_log_debug!("[STATS CACHE WARNING] Failed to compute statistics for {} files: {:?}",
                      failed_files.len(), failed_files);
        }

        Ok(success_count)
    }

    /// Get or compute statistics
    pub fn statistics_cache_get_or_compute(&self, file_path: &str) -> Result<bool, String> {
        let cache = self.statistics_cache.as_ref()
            .ok_or_else(|| "No statistics cache configured".to_string())?;

        let path = Path::from(file_path.to_string());

        // Check if already cached
        if cache.get(&path).is_some() {
            return Ok(true);
        }

        // Not in cache, compute and add
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

    /// Reset statistics cache stats
    pub fn statistics_cache_reset_stats(&self) {
        if let Some(cache) = &self.statistics_cache {
            cache.reset_stats();
        }
    }
}
