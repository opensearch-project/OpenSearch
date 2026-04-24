/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! ObjectStore handle management for FFM.
//!
//! Handles are `Arc<dyn ObjectStore>` leaked as raw pointers (`i64`).
//! Java holds the pointer and passes it back for scoping or destruction.

use object_store::local::LocalFileSystem;
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use std::sync::Arc;

/// Reconstruct an `Arc<dyn ObjectStore>` from a raw pointer without taking ownership.
/// Clones the Arc so the original handle remains valid.
///
/// # Safety
/// `handle` must be a valid pointer previously returned by `arc_into_raw`.
pub unsafe fn arc_clone_from_raw(handle: i64) -> Arc<dyn ObjectStore> {
    let ptr = handle as *const Arc<dyn ObjectStore>;
    (*ptr).clone()
}

/// Leak an `Arc<dyn ObjectStore>` as a raw pointer (`i64`).
/// The caller is responsible for eventually calling `drop_store` to free it.
pub fn arc_into_raw(store: Arc<dyn ObjectStore>) -> i64 {
    Box::into_raw(Box::new(store)) as i64
}

/// Create a local filesystem ObjectStore rooted at the given path.
/// Returns a new handle.
pub fn create_local_store(root_path: &str) -> Result<i64, String> {
    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(root_path)
            .map_err(|e| format!("Failed to create local store at {}: {}", root_path, e))?,
    );
    Ok(arc_into_raw(store))
}

/// Create a `PrefixStore` that scopes a parent ObjectStore to a path prefix.
/// Returns a new handle (the parent handle remains valid).
///
/// # Safety
/// `parent_handle` must be a valid ObjectStore pointer.
pub unsafe fn create_scoped_store(parent_handle: i64, prefix: &str) -> Result<i64, String> {
    if parent_handle <= 0 {
        return Err("invalid parent store handle".to_string());
    }
    let parent = arc_clone_from_raw(parent_handle);
    let scoped: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(parent, prefix));
    Ok(arc_into_raw(scoped))
}

/// Drop an `Arc<dyn ObjectStore>` from a raw pointer, decrementing the refcount.
///
/// # Safety
/// `handle` must be a valid ObjectStore pointer. Must not be called twice on the same handle.
pub unsafe fn drop_store(handle: i64) {
    if handle > 0 {
        let _ = Box::from_raw(handle as *mut Arc<dyn ObjectStore>);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::PutPayload;

    #[tokio::test]
    async fn test_scoped_store_write_reads_from_parent() {
        let parent: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_handle = arc_into_raw(parent.clone());

        let scoped_handle =
            unsafe { create_scoped_store(parent_handle, "shard/0/parquet/") }.unwrap();
        let scoped = unsafe { arc_clone_from_raw(scoped_handle) };

        // Write through scoped store
        scoped
            .put(
                &Path::from("data.parquet"),
                PutPayload::from_static(b"hello"),
            )
            .await
            .unwrap();

        // Read from parent at prefixed path
        let bytes = parent
            .get(&Path::from("shard/0/parquet/data.parquet"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"hello");

        unsafe {
            drop_store(scoped_handle);
            drop_store(parent_handle);
        }
    }

    #[test]
    fn test_invalid_handle_returns_error() {
        let result = unsafe { create_scoped_store(0, "prefix/") };
        assert!(result.is_err());
    }
}
