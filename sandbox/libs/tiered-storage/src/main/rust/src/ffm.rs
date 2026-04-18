/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for tiered storage.
//!
//! `TieredStorageRegistry` (file registry) and local `ObjectStore` are
//! created internally — no separate pointers exposed to Java.

use std::sync::Arc;

use native_bridge_common::ffm_safe;

use crate::registry::TieredStorageRegistry;
use crate::tiered_object_store::TieredObjectStore;

const NULL_PTR: i64 = 0;

// ---------------------------------------------------------------------------
// Public FFM exports
// ---------------------------------------------------------------------------

/// Create a [`TieredObjectStore`] with an internally-created file registry
/// and local filesystem store.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_create_tiered_object_store() -> i64 {
    let file_registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(object_store::local::LocalFileSystem::new());
    let store = Arc::new(TieredObjectStore::new(file_registry, local));
    let ptr = Arc::into_raw(store) as i64;
    native_bridge_common::log_info!("ffm: ts_create_tiered_object_store ptr={}", ptr);
    Ok(ptr)
}

/// Destroy a [`TieredObjectStore`].
///
/// Also drops the internally-owned `TieredStorageRegistry`.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_destroy_tiered_object_store(ptr: i64) -> i64 {
    if ptr == NULL_PTR {
        return Err("ts_destroy_tiered_object_store: null pointer (0)".to_string());
    }
    let _store = unsafe { Arc::from_raw(ptr as *const TieredObjectStore) };
    native_bridge_common::log_info!("ffm: ts_destroy_tiered_object_store ptr={}", ptr);
    Ok(0)
}

// TODO: File registry operations via TieredObjectStore pointer:
//       ts_register_file(store_ptr, ...), ts_remove_by_prefix(store_ptr, ...)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_destroy_null_returns_error() {
        assert!(ts_destroy_tiered_object_store(0) < 0);
    }

    #[test]
    fn test_create_and_destroy_no_leak() {
        let store_ptr = ts_create_tiered_object_store();
        assert!(store_ptr > 0);
        assert_eq!(ts_destroy_tiered_object_store(store_ptr), 0);
    }
}
