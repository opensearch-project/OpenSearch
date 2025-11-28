use std::sync::{Arc, Mutex};
use datafusion::execution::cache::cache_manager::{FileMetadataCache, CacheManagerConfig};
use datafusion::execution::cache::cache_unit::{DefaultFileStatisticsCache, DefaultFilesMetadataCache, DefaultListFilesCache};
use datafusion::execution::cache::CacheAccessor;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use tokio::runtime::Runtime;
use crate::cache::MutexFileMetadataCache;
use crate::util::{create_object_meta_from_file, construct_file_metadata};

/// Custom CacheManager that holds cache references directly
pub struct CustomCacheManager {
    /// Direct reference to the file metadata cache
    file_metadata_cache: Option<Arc<MutexFileMetadataCache>>,
    // Future: Statistics cache when implemented
    // stats_cache: Option<Arc<dyn StatsCache>>,
}

impl CustomCacheManager {
    /// Create a new CustomCacheManager
    pub fn new() -> Self {
        Self {
            file_metadata_cache: None,
        }
    }

    /// Set the file metadata cache
    pub fn set_file_metadata_cache(&mut self, cache: Arc<MutexFileMetadataCache>) {
        self.file_metadata_cache = Some(cache);
        println!("[CACHE INFO] File metadata cache set in CustomCacheManager");
    }

    /// Get the file metadata cache as Arc<dyn FileMetadataCache> for DataFusion
    pub fn get_file_metadata_cache_for_datafusion(&self) -> Option<Arc<dyn FileMetadataCache>> {
        self.file_metadata_cache.as_ref().map(|cache| cache.clone() as Arc<dyn FileMetadataCache>)
    }

    /// Build a CacheManagerConfig from the caches stored in this CustomCacheManager
    pub fn build_cache_manager_config(&self) -> CacheManagerConfig {
        let mut config = CacheManagerConfig::default();
        let file_static_cache = Arc::new(DefaultFileStatisticsCache::default());
        // Add file metadata cache if available
        if let Some(cache) = self.get_file_metadata_cache_for_datafusion() {
            config = config.with_file_metadata_cache(Some(cache.clone()))
                .with_metadata_cache_limit(cache.cache_limit());
        }
        config = config.with_files_statistics_cache(Some(file_static_cache.clone()));
        // Future: Add stats cache when implemented
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
                    println!("[CACHE INFO] File not added for metadata cache: {}", file_path);
                }
                Err(e) => {
                    errors.push(format!("Metadata cache: {}", e));
                }
            }

            // Future: Add to stats cache when implemented

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
                                        println!("Cache removed for: {}", file_path);
                                        any_removed = true;
                                        println!("[CACHE INFO] Removed file from metadata cache: {}", file_path);
                                    } else {
                                        println!("Item not found in cache: {}", file_path);
                                        println!("[CACHE INFO] File not found in metadata cache: {}", file_path);
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

            // Future: Remove from stats cache when implemented

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
        // Check metadata cache
        match create_object_meta_from_file(file_path) {
            Ok(object_metas) => {
                if let Some(cache) = &self.file_metadata_cache {
                    if let Some(object_meta) = object_metas.first() {
                        match cache.get(object_meta) {
                            Some(metadata) => {
                                println!("Retrieved metadata for: {} - size: {:?}", file_path, metadata.memory_size());
                                true
                            },
                            None => {
                                println!("No metadata found for: {}", file_path);
                                false
                            },
                        }
                    } else {
                        println!("No object metadata returned for: {}", file_path);
                        false
                    }
                } else {
                    println!("No metadata cache configured");
                    false
                }
            }
            Err(e) => {
                println!("Failed to get object metadata for {}: {}", file_path, e);
                false
            }
        }
    }

    /// Update the file metadata cache size limit
    pub fn update_metadata_cache_limit(&self, new_limit: usize) {
        if let Some(cache) = &self.file_metadata_cache {
            cache.update_cache_limit(new_limit);
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

        // Future: Add stats cache memory when implemented

        total
    }

    /// Clear all caches
    pub fn clear_all(&self) {
        if let Some(cache) = &self.file_metadata_cache {
            cache.clear();
        }
        // Future: Clear stats cache when implemented
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
                // Future: Clear stats cache when implemented
                Err("Stats cache not yet implemented".to_string())
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
                // Future: Get stats cache memory when implemented
                Err("Stats cache not yet implemented".to_string())
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
                    println!("[CACHE ERROR] Failed to cache metadata for: {}", file_path);
                    Ok(false)
                }
            }
            Err(e) => Err(format!("Failed to verify cache: {}", e))
        }
    }
}
