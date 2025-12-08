use std::sync::{Arc, Mutex};
use datafusion::execution::cache::cache_manager::{FileMetadataCache, FileStatisticsCache, CacheManagerConfig};
use datafusion::execution::cache::cache_unit::{DefaultFileStatisticsCache, DefaultFilesMetadataCache, DefaultListFilesCache};
use datafusion::execution::cache::CacheAccessor;
use datafusion::physical_plan::Statistics;
use datafusion::common::stats::Precision;
use tokio::runtime::Runtime;
use crate::cache::MutexFileMetadataCache;
use crate::statistics_cache::CustomStatisticsCache;
use crate::cache_policy::{CacheConfig, PolicyType};
use crate::util::{create_object_meta_from_file, construct_file_metadata};
use object_store::path::Path;
use object_store::ObjectMeta;

/// Custom CacheManager that holds cache references directly
pub struct CustomCacheManager {
    /// Direct reference to the file metadata cache
    file_metadata_cache: Option<Arc<MutexFileMetadataCache>>,
    /// Direct reference to the statistics cache
    statistics_cache: Option<Arc<CustomStatisticsCache>>,
    /// DataFusion's statistics cache for the CacheManagerConfig
    datafusion_stats_cache: Option<Arc<DefaultFileStatisticsCache>>,
}

impl CustomCacheManager {
    /// Create a new CustomCacheManager
    pub fn new() -> Self {
        Self {
            file_metadata_cache: None,
            statistics_cache: None,
            datafusion_stats_cache: None,
        }
    }

    /// Set the file metadata cache
    pub fn set_file_metadata_cache(&mut self, cache: Arc<MutexFileMetadataCache>) {
        self.file_metadata_cache = Some(cache);
        println!("[CACHE INFO] File metadata cache set in CustomCacheManager");
    }

    /// Set the statistics cache
    pub fn set_statistics_cache(&mut self, cache: Arc<CustomStatisticsCache>, datafusion_cache: Arc<DefaultFileStatisticsCache>) {
        self.statistics_cache = Some(cache);
        self.datafusion_stats_cache = Some(datafusion_cache);
        println!("[CACHE INFO] Statistics cache set in CustomCacheManager");
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
            config = config.with_file_metadata_cache(Some(cache));
        }
        
        // Add statistics cache if available
        if let Some(stats_cache) = &self.datafusion_stats_cache {
            config = config.with_files_statistics_cache(Some(stats_cache.clone()));
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
                    println!("[CACHE INFO] File not added for metadata cache: {}", file_path);
                }
                Err(e) => {
                    errors.push(format!("Metadata cache: {}", e));
                }
            }

            // Add to statistics cache
            match self.statistics_cache_compute_and_put(file_path) {
                Ok(true) => {
                    any_success = true;
                }
                Ok(false) => {
                    println!("[CACHE INFO] File not added for statistics cache: {}", file_path);
                }
                Err(e) => {
                    errors.push(format!("Statistics cache: {}", e));
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

            // Remove from statistics cache
            if let Some(cache) = &self.statistics_cache {
                let path = Path::from(file_path.clone());
                // Use contains_key to check if the entry exists before attempting removal
                if cache.contains_key(&path) {
                    // Since we can't call remove directly on Arc<CustomStatisticsCache>,
                    // we need to use the thread-safe DashMap operations
                    if cache.inner().remove(&path).is_some() {
                        any_removed = true;
                        println!("[CACHE INFO] Removed file from statistics cache: {}", file_path);
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
                                println!("Retrieved metadata for: {} - size: {:?}", file_path, metadata.memory_size());
                                found = true;
                            },
                            None => {
                                println!("No metadata found for: {}", file_path);
                            },
                        }
                    }
                }
            }
            Err(e) => {
                println!("Failed to get object metadata for {}: {}", file_path, e);
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

        //TODO: Use TokioRuntimePtr to block on the async operation
        let metadata = Runtime::new()
            .map_err(|e| format!("Failed to create Tokio Runtime: {}", e))?
            .block_on(async {
                construct_file_metadata(store.as_ref(), object_meta, data_format)
                    .await
                    .map_err(|e| format!("Failed to construct file metadata: {}", e))
            })?;

        // Get the cache directly from our stored reference
        let cache = self.file_metadata_cache.as_ref()
            .ok_or_else(|| "No file metadata cache configured".to_string())?;

        match cache.inner.lock() {
            Ok(mut cache_guard) => {
                cache_guard.put(object_meta, metadata);

                if cache_guard.contains_key(object_meta) {
                    Ok(true)
                } else {
                    println!("Failed to cache metadata for: {}", file_path);
                    Ok(false)
                }
            }
            Err(e) => Err(format!("Cache put failed: {}", e))
        }
    }

    /// Compute and put statistics into cache
    pub fn statistics_cache_compute_and_put(&self, file_path: &str) -> Result<bool, String> {
        let cache = self.statistics_cache.as_ref()
            .ok_or_else(|| "No statistics cache configured".to_string())?;

        let path = Path::from(file_path.to_string());
        
        // Check if already cached
        if cache.contains_key(&path) {
            println!("[STATS CACHE INFO] Statistics already cached for: {}", file_path);
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
                println!("[STATS CACHE INFO] Successfully computed and cached statistics for: {}", file_path);
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
                    eprintln!("[STATS CACHE ERROR] Failed to compute statistics for {}: {}", file_path, e);
                    failed_files.push(file_path.clone());
                }
            }
        }

        if !failed_files.is_empty() {
            eprintln!("[STATS CACHE WARNING] Failed to compute statistics for {} files: {:?}", 
                      failed_files.len(), failed_files);
        }

        println!("[STATS CACHE INFO] Successfully computed and cached statistics for {} files", success_count);
        Ok(success_count)
    }

    /// Get or compute statistics
    pub fn statistics_cache_get_or_compute(&self, file_path: &str) -> Result<bool, String> {
        let cache = self.statistics_cache.as_ref()
            .ok_or_else(|| "No statistics cache configured".to_string())?;

        let path = Path::from(file_path.to_string());
        
        // Check if already cached
        if cache.get(&path).is_some() {
            println!("[STATS CACHE INFO] Statistics found in cache for: {}", file_path);
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

/// Compute statistics from a parquet file using DataFusion's built-in functionality
fn compute_parquet_statistics(file_path: &str) -> Result<Statistics, Box<dyn std::error::Error>> {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::common::stats::Precision;
    use std::fs::File;
    
    // Open the parquet file
    let file = File::open(file_path)?;
    
    // Build parquet reader to get metadata
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();
    let schema = builder.schema();
    
    // Get file-level statistics
    let file_metadata = metadata.file_metadata();
    let num_rows = file_metadata.num_rows() as usize;
    
    // Calculate total byte size from row groups
    let mut total_byte_size = 0;
    for row_group in metadata.row_groups() {
        total_byte_size += row_group.total_byte_size() as usize;
    }
    
    // Extract column statistics from parquet metadata
    let mut column_statistics = Vec::new();
    
    // Get the number of columns from schema
    let num_columns = schema.fields().len();
    
    // Initialize column statistics for each field
    for field_idx in 0..num_columns {
        let mut null_count = 0;
        let mut has_statistics = false;
        
        // Aggregate statistics across all row groups for this column
        for row_group in metadata.row_groups() {
            if let Some(col_metadata) = row_group.columns().get(field_idx) {
                if let Some(stats) = col_metadata.statistics() {
                    has_statistics = true;
                    null_count += stats.null_count_opt().unwrap_or(0);
                }
            }
        }
        
        // Create column statistics
        let col_stats = datafusion::common::stats::ColumnStatistics {
            null_count: if has_statistics {
                Precision::Exact(null_count as usize)
            } else {
                Precision::Absent
            },
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            distinct_count: Precision::Absent,
            sum_value: Precision::Absent,
        };
        
        column_statistics.push(col_stats);
    }
    
    // Create the Statistics object
    let statistics = Statistics {
        num_rows: Precision::Exact(num_rows),
        total_byte_size: Precision::Exact(total_byte_size),
        column_statistics,
    };
    
    Ok(statistics)
}
