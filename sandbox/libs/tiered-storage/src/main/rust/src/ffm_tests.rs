use super::*;

#[test]
fn test_destroy_null_returns_error() {
    assert!(ts_destroy_tiered_object_store(0) < 0);
}

#[test]
fn test_create_and_destroy_no_leak() {
    let store_ptr = ts_create_tiered_object_store(0, 0, 0);
    assert!(store_ptr > 0);
    assert_eq!(ts_destroy_tiered_object_store(store_ptr), 0);
}

#[test]
fn test_register_files_null_store_returns_error() {
    let entries = b"test.parquet\nremote/test.parquet";
    let result = ts_register_files(0, entries.as_ptr(), entries.len() as i64, 1, 1);
    assert!(result < 0);
}

#[test]
fn test_remove_file_null_store_returns_error() {
    let result = ts_remove_file(0, b"test.parquet".as_ptr(), 12);
    assert!(result < 0);
}

#[test]
fn test_register_files_and_remove_round_trip() {
    let store_ptr = ts_create_tiered_object_store(0, 0, 0);
    assert!(store_ptr > 0);

    // Batch register: two files as Remote (triplets: path\nremotePath\nsize\n...)
    let entries = b"data/seg_0.parquet\nremote/seg_0.parquet\n1024\ndata/local.parquet\n\n0";
    let result = ts_register_files(store_ptr, entries.as_ptr(), entries.len() as i64, 2, 1);
    assert_eq!(result, 0);

    // Remove one
    let result = ts_remove_file(store_ptr, b"data/seg_0.parquet".as_ptr(), 18);
    assert_eq!(result, 0);

    assert_eq!(ts_destroy_tiered_object_store(store_ptr), 0);
}

#[test]
fn test_register_files_invalid_location_returns_error() {
    let store_ptr = ts_create_tiered_object_store(0, 0, 0);
    assert!(store_ptr > 0);

    let entries = b"test.parquet\nremote/test.parquet\n2048";
    let result = ts_register_files(store_ptr, entries.as_ptr(), entries.len() as i64, 1, 99);
    assert!(result < 0);

    assert_eq!(ts_destroy_tiered_object_store(store_ptr), 0);
}

#[test]
fn test_get_object_store_box_ptr_null_returns_error() {
    assert!(ts_get_object_store_box_ptr(0) < 0);
}

#[test]
fn test_destroy_object_store_box_ptr_null_returns_error() {
    assert!(ts_destroy_object_store_box_ptr(0) < 0);
}

#[test]
fn test_get_and_destroy_object_store_box_ptr_round_trip() {
    let store_ptr = ts_create_tiered_object_store(0, 0, 0);
    assert!(store_ptr > 0);

    // Get a boxed pointer — this increments the Arc refcount
    let box_ptr = ts_get_object_store_box_ptr(store_ptr);
    assert!(box_ptr > 0);
    assert_ne!(box_ptr, store_ptr); // different pointer (Box wrapping Arc)

    // Destroy the box — decrements Arc refcount
    assert_eq!(ts_destroy_object_store_box_ptr(box_ptr), 0);

    // Original store still alive — destroy it
    assert_eq!(ts_destroy_tiered_object_store(store_ptr), 0);
}

#[test]
fn test_get_object_store_box_ptr_multiple_calls() {
    let store_ptr = ts_create_tiered_object_store(0, 0, 0);
    assert!(store_ptr > 0);

    // Multiple box pointers can coexist (simulates multiple reader managers)
    let box1 = ts_get_object_store_box_ptr(store_ptr);
    let box2 = ts_get_object_store_box_ptr(store_ptr);
    assert!(box1 > 0);
    assert!(box2 > 0);
    assert_ne!(box1, box2); // each call creates a new Box

    // Destroy both boxes
    assert_eq!(ts_destroy_object_store_box_ptr(box1), 0);
    assert_eq!(ts_destroy_object_store_box_ptr(box2), 0);

    // Original store still alive
    assert_eq!(ts_destroy_tiered_object_store(store_ptr), 0);
}

#[test]
fn test_create_with_remote_does_not_consume_pointer() {
    // Simulate node-level remote store: create a Box<Arc<dyn ObjectStore>>
    let remote: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
    let remote_box = Box::new(remote);
    let remote_ptr = Box::into_raw(remote_box) as i64;

    // Create two TieredObjectStores sharing the same remote pointer
    let store1 = ts_create_tiered_object_store(0, remote_ptr, 0);
    let store2 = ts_create_tiered_object_store(0, remote_ptr, 0);
    assert!(store1 > 0);
    assert!(store2 > 0);

    // Both stores work — remote pointer not consumed
    assert_eq!(ts_destroy_tiered_object_store(store1), 0);
    assert_eq!(ts_destroy_tiered_object_store(store2), 0);

    // Clean up the remote Box (simulates repository.doClose())
    let _remote_box = unsafe { Box::from_raw(remote_ptr as *mut Arc<dyn ObjectStore>) };
}

// -- cache_box_ptr tests ------------------------------------------------

/// ts_create_tiered_object_store with 0 cache_box_ptr creates a store without
/// a cache. The store must still be functional for all registry and I/O operations.
#[test]
fn test_create_with_zero_cache_ptr_creates_uncached_store() {
    let store_ptr = ts_create_tiered_object_store(0, 0, 0);
    assert!(store_ptr > 0);

    // Store is functional — register a file without error.
    let entries = b"a.parquet\nremote/a.parquet\n512";
    let result = ts_register_files(store_ptr, entries.as_ptr(), entries.len() as i64, 1, 1);
    assert_eq!(result, 0);

    assert_eq!(ts_destroy_tiered_object_store(store_ptr), 0);
}

/// ts_create_tiered_object_store with a valid cache_box_ptr wires the cache
/// without consuming the Box — two stores can share the same cache pointer.
#[test]
fn test_create_with_cache_does_not_consume_pointer() {
    use opensearch_block_cache::range_cache::CacheKey;
    use opensearch_block_cache::traits::BlockCache;
    use bytes::Bytes;

    // Minimal no-op cache used only to construct a valid Box<Arc<dyn BlockCache>> pointer.
    struct NoopCache;
    impl BlockCache for NoopCache {
        fn get(&self, _key: &CacheKey) -> impl std::future::Future<Output = Option<Bytes>> + Send {
            std::future::ready(None)
        }
        fn put(&self, _key: &CacheKey, _data: Bytes) {}
        fn evict_prefix(&self, _prefix: &str) {}
        fn clear(&self) -> impl std::future::Future<Output = ()> + Send {
            std::future::ready(())
        }
    }

    let cache: Arc<dyn BlockCache> = Arc::new(NoopCache);
    let cache_box = Box::new(Arc::clone(&cache));
    let cache_ptr = Box::into_raw(cache_box) as i64;

    // Create two stores sharing the same cache pointer.
    let store1 = ts_create_tiered_object_store(0, 0, cache_ptr);
    let store2 = ts_create_tiered_object_store(0, 0, cache_ptr);
    assert!(store1 > 0);
    assert!(store2 > 0);

    // Both stores created — cache pointer not consumed (Arc strong count still valid).
    assert_eq!(ts_destroy_tiered_object_store(store1), 0);
    assert_eq!(ts_destroy_tiered_object_store(store2), 0);

    // Clean up the cache Box (simulates FoyerBlockCache.close()).
    let _cache_box = unsafe { Box::from_raw(cache_ptr as *mut Arc<dyn BlockCache>) };
}
