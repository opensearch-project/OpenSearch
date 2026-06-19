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
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use log::{debug, error};

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
    pub fn add_files(&self, file_paths: &[String], rt_handle: &tokio::runtime::Handle, store_ptr: i64) -> Result<Vec<(String, bool)>, String> {
        let mut results = Vec::new();

        for file_path in file_paths {
            let mut any_success = false;
            let mut errors = Vec::new();

            // Add to metadata cache
            match self.metadata_cache_put(file_path, rt_handle, store_ptr) {
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

            // Add to statistics cache. Statistics warming is std::fs-based and out of scope for
            // remote (warm) shards; only warm it for the local hot path (store_ptr == 0).
            if store_ptr == 0 {
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
    fn metadata_cache_put(&self, file_path: &str, rt_handle: &tokio::runtime::Handle, store_ptr: i64) -> Result<bool, String> {
        if !file_path.to_lowercase().ends_with(".parquet") {
            return Ok(false); // Skip unsupported formats
        }

        // Resolve the object store + ObjectMeta exactly as the query/reader path does
        // (api::create_reader). Warm shard (store_ptr > 0): decode the SAME boxed-fat
        // Box<Arc<dyn ObjectStore>> pointer the reader uses and derive ObjectMeta via
        // store.head() so size/last_modified match the query path (else is_valid_for fails
        // and every query re-reads). Hot (store_ptr == 0): keep the cheap local std::fs path.
        let (store, object_meta): (Arc<dyn ObjectStore>, ObjectMeta) = if store_ptr > 0 {
            // Safety: store_ptr is a Box<Arc<dyn ObjectStore>> pointer from
            // ts_get_object_store_box_ptr (same form df_create_reader decodes). Passing the
            // raw thin *const TieredObjectStore form would read garbage as the vtable -> SIGSEGV.
            let boxed = unsafe { &*(store_ptr as *const Arc<dyn ObjectStore>) };
            let store = Arc::clone(boxed);
            let location = Path::from(file_path);
            let meta = rt_handle
                .block_on(async { store.head(&location).await })
                .map_err(|e| format!("Failed to head {} on remote store: {}", file_path, e))?;
            (store, meta)
        } else {
            let object_metas = create_object_meta_from_file(file_path)
                .map_err(|e| format!("Failed to get object metadata: {}", e))?;
            let object_meta = object_metas
                .into_iter()
                .next()
                .ok_or_else(|| "No object metadata returned".to_string())?;
            let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
            (store, object_meta)
        };

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
            let df_metadata = DFParquetMetadata::new(store.as_ref(), &object_meta)
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

#[cfg(test)]
mod store_aware_warm_tests {
    use super::*;
    use crate::cache::MutexFileMetadataCache;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::cache::CacheAccessor;
    use datafusion::execution::cache::DefaultFilesMetadataCache;
    use datafusion::parquet::arrow::ArrowWriter;
    use object_store::local::LocalFileSystem;

    fn write_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))]).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn manager_with_metadata_cache() -> (CustomCacheManager, Arc<MutexFileMetadataCache>) {
        let metadata_cache = Arc::new(MutexFileMetadataCache::new(DefaultFilesMetadataCache::new(50 * 1024 * 1024)));
        let mut mgr = CustomCacheManager::new();
        mgr.set_file_metadata_cache(metadata_cache.clone());
        (mgr, metadata_cache)
    }

    // store_ptr > 0 (warm): resolve the boxed object store, read ObjectMeta via store.head, cache
    // the footer, and verify a query-path fetch reuses it (is_valid_for passes => no re-read).
    #[test]
    fn warm_store_ptr_caches_and_validates() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("seg_0.parquet");
        write_parquet(&file_path);
        let abs = file_path.to_str().unwrap().to_string();

        let (mgr, metadata_cache) = manager_with_metadata_cache();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle();

        // Simulate the per-shard remote store handle: a Box<Arc<dyn ObjectStore>> raw pointer,
        // the same boxed-fat form df_create_reader / df_cache_manager_add_files decode.
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let store_ptr = Box::into_raw(Box::new(Arc::clone(&store))) as i64;

        let results = mgr.add_files(&[abs.clone()], handle, store_ptr).unwrap();
        assert_eq!(results, vec![(abs.clone(), true)], "warm add_files should cache the footer");

        let key = Path::from(abs.as_str());
        assert!(metadata_cache.contains_key(&key), "entry must be present under the file path key");
        assert_eq!(metadata_cache.len(), 1, "exactly one entry cached");

        // A query-path fetch with a freshly headed ObjectMeta must HIT the warmed entry
        // (is_valid_for passes); otherwise a stale/fabricated meta would force a re-read.
        let head = handle.block_on(async { store.head(&key).await }).unwrap();
        let hits_before = metadata_cache.hit_count();
        handle
            .block_on(async {
                DFParquetMetadata::new(store.as_ref(), &head)
                    .with_file_metadata_cache(Some(metadata_cache.clone() as Arc<dyn FileMetadataCache>))
                    .fetch_metadata()
                    .await
            })
            .unwrap();
        assert!(
            metadata_cache.hit_count() > hits_before,
            "warm-cached entry should be reused by a query-path fetch"
        );
        assert_eq!(metadata_cache.len(), 1, "is_valid_for passed => no duplicate entry");

        // Reclaim the boxed store pointer.
        unsafe {
            drop(Box::from_raw(store_ptr as *mut Arc<dyn ObjectStore>));
        }
    }

    // store_ptr == 0 (hot): the existing local std::fs / LocalFileSystem path still caches.
    #[test]
    fn hot_store_ptr_zero_caches_locally() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("seg_0.parquet");
        write_parquet(&file_path);
        let abs = file_path.to_str().unwrap().to_string();

        let (mgr, metadata_cache) = manager_with_metadata_cache();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle();

        let results = mgr.add_files(&[abs.clone()], handle, 0).unwrap();
        assert_eq!(results, vec![(abs.clone(), true)], "hot add_files should cache the footer");
        assert!(metadata_cache.contains_key(&Path::from(abs.as_str())));
        assert_eq!(metadata_cache.len(), 1);
    }
}
