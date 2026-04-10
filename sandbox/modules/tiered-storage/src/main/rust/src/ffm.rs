/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for tiered storage.
//!
//! Each `extern "C"` function here is the direct entry point from Java via FFM.
//! String parameters become `(*const u8, i64)` pairs; optional strings use a
//! nullable pointer (null = `None`). Return convention:
//!
//! - `>= 0` → success (value or pointer)
//! - `< 0`  → error — negate to get a pointer to a heap-allocated error string.
//!
//! All functions are prefixed with `ts_` (tiered storage) to avoid symbol collisions.
//!
//! # Pointer Lifecycle
//!
//! ```text
//! Java                                Rust
//! ─────                               ────
//! ts_create_tiered_registry()   →     TieredStorageRegistry::new() → Arc::into_raw → i64
//! ts_*(registry_ptr, ...)       →     tiered_registry_from_ptr(ptr) → Arc (ref +1, then -1 on drop)
//! ts_destroy_tiered_registry(ptr) →   Arc::from_raw(ptr) → drop (ref -1 → dealloc if last)
//! ```

use std::slice;
use std::str;
use std::sync::Arc;

use native_bridge_common::ffm_safe;

use crate::registry::{FileRegistry, TieredStorageRegistry};
use crate::tiered_object_store::TieredObjectStore;
use crate::types::FileLocation;
use opensearch_remote_store::factory as store_factory;

/// Minimum valid pointer value. Null (0) is always rejected.
const NULL_PTR: i64 = 0;

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null string pointer".to_string());
    }
    if len < 0 {
        return Err(format!("negative string length: {}", len));
    }
    let bytes = slice::from_raw_parts(ptr, len as usize);
    str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))
}

unsafe fn optional_str_from_raw(ptr: *const u8, len: i64) -> Result<Option<String>, String> {
    if ptr.is_null() {
        return Ok(None);
    }
    Ok(Some(str_from_raw(ptr, len)?.to_string()))
}

/// Reconstruct an `Arc<TieredStorageRegistry>` from a raw pointer without
/// consuming ownership.
///
/// # Safety
///
/// The caller must ensure `ptr` was produced by `Arc::into_raw` on a
/// `TieredStorageRegistry` and has not been destroyed.
fn tiered_registry_from_ptr(ptr: i64) -> Result<Arc<TieredStorageRegistry>, String> {
    if ptr == NULL_PTR {
        return Err("tiered_registry_from_ptr: null pointer (0)".to_string());
    }
    let raw = ptr as *const TieredStorageRegistry;
    unsafe {
        Arc::increment_strong_count(raw);
        Ok(Arc::from_raw(raw))
    }
}

/// Reconstruct an `Arc<TieredObjectStore>` from a raw pointer without
/// consuming ownership.
///
/// # Safety
///
/// Same invariants as `tiered_registry_from_ptr`.
fn tiered_store_from_ptr(ptr: i64) -> Result<Arc<TieredObjectStore>, String> {
    if ptr == NULL_PTR {
        return Err("tiered_store_from_ptr: null pointer (0)".to_string());
    }
    let raw = ptr as *const TieredObjectStore;
    unsafe {
        Arc::increment_strong_count(raw);
        Ok(Arc::from_raw(raw))
    }
}

// ---------------------------------------------------------------------------
// Public FFM exports
// ---------------------------------------------------------------------------

/// Create a new [`TieredStorageRegistry`] and return its pointer as `i64`.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_create_tiered_registry() -> i64 {
    let tiered = Arc::new(TieredStorageRegistry::new());
    let ptr = Arc::into_raw(tiered) as i64;
    native_bridge_common::log_info!("ffm: ts_create_tiered_registry ptr={}", ptr);
    Ok(ptr)
}

/// Destroy a [`TieredStorageRegistry`].
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_destroy_tiered_registry(ptr: i64) -> i64 {
    if ptr == NULL_PTR {
        return Err("ts_destroy_tiered_registry: null pointer (0)".to_string());
    }
    let _registry = unsafe { Arc::from_raw(ptr as *const TieredStorageRegistry) };
    native_bridge_common::log_info!("ffm: ts_destroy_tiered_registry ptr={}", ptr);
    Ok(0)
}

/// Register a file via the TieredObjectStore (resolves cached_store).
///
/// `location`: 0 = Local, 1 = Remote, 2 = Both.
/// `remote_path` and `repo_key` are nullable (null pointer = `None`).
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn ts_register_file(
    store_ptr: i64,
    path_ptr: *const u8,
    path_len: i64,
    location: u8,
    remote_path_ptr: *const u8,
    remote_path_len: i64,
    repo_key_ptr: *const u8,
    repo_key_len: i64,
) -> i64 {
    let path = str_from_raw(path_ptr, path_len)?;
    let remote_path = optional_str_from_raw(remote_path_ptr, remote_path_len)?;
    let repo_key = optional_str_from_raw(repo_key_ptr, repo_key_len)?;
    let store = tiered_store_from_ptr(store_ptr)?;
    let loc = FileLocation::from_u8(location)
        .ok_or_else(|| format!("ts_register_file: invalid location value {}", location))?;
    store
        .register_file(path, loc, remote_path, repo_key)
        .map_err(|e| e.to_string())?;
    Ok(0)
}

// TODO: Remove ts_acquire_read and ts_release_read once Java side is updated.
// Acquire/release is now automatic via ReadGuard — these are no-ops for backward compat.

/// Acquire a read on a file (no-op — acquire is automatic via ReadGuard).
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn ts_acquire_read(
    _registry_ptr: i64,
    _path_ptr: *const u8,
    _path_len: i64,
) -> i64 {
    // No-op: acquire/release is now automatic via ReadGuard.
    Ok(0)
}

/// Release a read on a file (no-op — release is automatic via ReadGuard drop).
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn ts_release_read(
    _registry_ptr: i64,
    _path_ptr: *const u8,
    _path_len: i64,
) -> i64 {
    // No-op: acquire/release is now automatic via ReadGuard.
    Ok(0)
}

/// Mark a file for pending deletion (force-remove from registry).
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn ts_mark_pending_delete(
    store_ptr: i64,
    path_ptr: *const u8,
    path_len: i64,
) -> i64 {
    let path = str_from_raw(path_ptr, path_len)?;
    let store = tiered_store_from_ptr(store_ptr)?;
    store.registry().remove(path, true);
    Ok(0)
}

// TODO: Implement sweep when eviction lifecycle is added.
// Currently returns 0 (no-op).

/// Sweep all pending deletes. Returns 0 (no sweep in current design).
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_sweep_pending_deletes(registry_ptr: i64) -> i64 {
    let _tiered = tiered_registry_from_ptr(registry_ptr)?;
    Ok(0)
}

/// Add a remote object store to the TieredObjectStore.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn ts_add_remote_store(
    store_ptr: i64,
    repo_key_ptr: *const u8,
    repo_key_len: i64,
    store_type_ptr: *const u8,
    store_type_len: i64,
    config_json_ptr: *const u8,
    config_json_len: i64,
) -> i64 {
    let repo_key = str_from_raw(repo_key_ptr, repo_key_len)?;
    let store_type = str_from_raw(store_type_ptr, store_type_len)?;
    let config_json = str_from_raw(config_json_ptr, config_json_len)?;
    let tiered = tiered_store_from_ptr(store_ptr)?;
    let store =
        store_factory::create(store_type, config_json, repo_key).map_err(|e| e.to_string())?;
    tiered.add_store(repo_key.to_string(), store);
    Ok(0)
}

/// Create a [`TieredObjectStore`] wrapping the given registry.
///
/// Returns the store pointer on success, `< 0` on error.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_create_tiered_object_store(registry_ptr: i64) -> i64 {
    let tiered = tiered_registry_from_ptr(registry_ptr)?;
    let local = Arc::new(object_store::local::LocalFileSystem::new());
    let store = Arc::new(TieredObjectStore::new(tiered, local));
    let ptr = Arc::into_raw(store) as i64;
    native_bridge_common::log_info!("ffm: ts_create_tiered_object_store ptr={}", ptr);
    Ok(ptr)
}

/// Destroy a [`TieredObjectStore`].
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_pointer_rejection_tiered_registry_from_ptr() {
        let result = tiered_registry_from_ptr(0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("null pointer"));
    }

    #[test]
    fn test_null_pointer_rejection_tiered_store_from_ptr() {
        let result = tiered_store_from_ptr(0);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_and_destroy_tiered_registry_no_leak() {
        let ptr = ts_create_tiered_registry();
        assert_ne!(ptr, 0);
        let tiered = tiered_registry_from_ptr(ptr).unwrap();
        drop(tiered);
        let _registry = unsafe { Arc::from_raw(ptr as *const TieredStorageRegistry) };
    }

    #[test]
    fn test_destroy_null_registry_returns_error() {
        let result = ts_destroy_tiered_registry(0);
        assert!(result < 0);
    }

    #[test]
    fn test_create_and_destroy_tiered_object_store_no_leak() {
        let reg_ptr = ts_create_tiered_registry();
        let store_ptr = ts_create_tiered_object_store(reg_ptr);
        assert!(store_ptr > 0);

        let result = ts_destroy_tiered_object_store(store_ptr);
        assert_eq!(result, 0);

        let result = ts_destroy_tiered_registry(reg_ptr);
        assert_eq!(result, 0);
    }

    #[test]
    fn test_destroy_null_tiered_store_returns_error() {
        let result = ts_destroy_tiered_object_store(0);
        assert!(result < 0);
    }

    #[test]
    fn test_tiered_registry_from_ptr_arc_count_correct() {
        let tiered = Arc::new(TieredStorageRegistry::new());
        let ptr = Arc::into_raw(tiered) as i64;
        {
            let _arc = tiered_registry_from_ptr(ptr).unwrap();
        }
        let _ = unsafe { Arc::from_raw(ptr as *const TieredStorageRegistry) };
    }

    #[test]
    fn test_null_ptr_on_all_extern_functions() {
        assert!(ts_destroy_tiered_registry(0) < 0);
        assert!(ts_sweep_pending_deletes(0) < 0);
        assert!(ts_create_tiered_object_store(0) < 0);
        assert!(ts_destroy_tiered_object_store(0) < 0);
    }

    #[test]
    fn test_null_ptr_on_helper_functions() {
        assert!(tiered_registry_from_ptr(0).is_err());
        assert!(tiered_store_from_ptr(0).is_err());
    }
}
