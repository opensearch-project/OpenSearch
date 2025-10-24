
use std::sync::{Arc, Mutex};


use datafusion::execution::cache::cache_manager::{FileMetadataCache};
use datafusion::execution::cache::cache_unit::{DefaultFilesMetadataCache};
use datafusion::execution::cache::CacheAccessor;
use object_store::ObjectMeta;

// Wrapper to make Mutex<DefaultFilesMetadataCache> implement FileMetadataCache
pub struct MutexFileMetadataCache {
    pub inner: Mutex<DefaultFilesMetadataCache>,
}

impl MutexFileMetadataCache {
    pub fn new(cache: DefaultFilesMetadataCache) -> Self {
        Self {
            inner: Mutex::new(cache),
        }
    }
}

// Implement CacheAccessor which is required by FileMetadataCache
impl CacheAccessor<ObjectMeta, Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> for MutexFileMetadataCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &ObjectMeta) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        self.inner.lock().unwrap().get(k)
    }

    fn get_with_extra(&self, k: &ObjectMeta, extra: &Self::Extra) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        self.inner.lock().unwrap().get_with_extra(k, extra)
    }

    fn put(&self, k: &ObjectMeta, v: Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        self.inner.lock().unwrap().put(k, v)
    }

    fn put_with_extra(&self, k: &ObjectMeta, v: Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>, e: &Self::Extra) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        self.inner.lock().unwrap().put_with_extra(k, v, e)
    }

    fn remove(&mut self, k: &ObjectMeta) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        self.inner.lock().unwrap().remove(k)
    }

    fn contains_key(&self, k: &ObjectMeta) -> bool {
        self.inner.lock().unwrap().contains_key(k)
    }

    fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    fn clear(&self) {
        self.inner.lock().unwrap().clear()
    }

    fn name(&self) -> String {
        self.inner.lock().unwrap().name()
    }
}

impl FileMetadataCache for MutexFileMetadataCache {
    fn cache_limit(&self) -> usize {
        self.inner.lock().unwrap().cache_limit()
    }

    fn update_cache_limit(&self, limit: usize) {
        self.inner.lock().unwrap().update_cache_limit(limit)
    }

    fn list_entries(&self) -> std::collections::HashMap<object_store::path::Path, datafusion::execution::cache::cache_manager::FileMetadataCacheEntry> {
        self.inner.lock().unwrap().list_entries()
    }
}
