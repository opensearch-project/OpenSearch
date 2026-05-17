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

use std::slice;
use std::str;
use std::sync::Arc;

use native_bridge_common::ffm_safe;
use object_store::ObjectStore;

use opensearch_block_cache::traits::BlockCache;

use crate::registry::TieredStorageRegistry;
use crate::registry::FileRegistry;
use crate::tiered_object_store::TieredObjectStore;
use crate::types::FileLocation;

const NULL_PTR: i64 = 0;

/// Decode a UTF-8 string from a raw pointer and length.
///
/// # Safety
/// The caller must ensure `ptr` points to `len` valid UTF-8 bytes.
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

/// Reconstruct an `Arc<TieredObjectStore>` from a raw pointer without
/// consuming ownership. Increments the strong count so the caller's
/// copy remains valid.
///
/// # Safety
/// `ptr` must have been produced by `Arc::into_raw` on a live
/// `Arc<TieredObjectStore>`.
unsafe fn arc_from_ptr(ptr: i64) -> Result<Arc<TieredObjectStore>, String> {
    if ptr == NULL_PTR {
        return Err("null store pointer (0)".to_string());
    }
    let raw = ptr as *const TieredObjectStore;
    Arc::increment_strong_count(raw);
    Ok(Arc::from_raw(raw))
}

// ---------------------------------------------------------------------------
// Public FFM exports
// ---------------------------------------------------------------------------

/// Create a [`TieredObjectStore`] with optional local and remote object stores.
///
/// Create a `TieredObjectStore` with optional local and remote stores.
///
/// `local_store_box_ptr=0` creates a default `LocalFileSystem`. For per-shard
/// stores, Java passes 0 and DataFusion uses absolute paths to resolve files.
///
/// - `local_store_box_ptr`: if non-zero, a `Box<Arc<dyn ObjectStore>>` pointer for local I/O.
///   If 0, creates a default `LocalFileSystem::new()`.
/// - `remote_store_box_ptr`: if non-zero, a `Box<Arc<dyn ObjectStore>>` pointer from a repository
///   plugin. The Arc is cloned (ownership is NOT taken — the pointer remains valid for other
///   shards). If 0, no remote store.
/// - `cache_box_ptr`: if non-zero, a `Box<Arc<dyn BlockCache>>` pointer from a block cache
///   plugin. The Arc is cloned (ownership is NOT taken — the pointer remains valid for the
///   node lifetime). If 0, no block cache is attached.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_create_tiered_object_store(
    local_store_box_ptr: i64,
    remote_store_box_ptr: i64,
    cache_box_ptr: i64,
) -> i64 {
    let file_registry = Arc::new(TieredStorageRegistry::new());

    let local: Arc<dyn ObjectStore> = if local_store_box_ptr != NULL_PTR {
        *unsafe { Box::from_raw(local_store_box_ptr as *mut Arc<dyn ObjectStore>) }
    } else {
        Arc::new(object_store::local::LocalFileSystem::new())
    };

    let mut store = TieredObjectStore::new(file_registry, local);

    if remote_store_box_ptr != NULL_PTR {
        // IMPORTANT: Do NOT consume the Box — the pointer is node-level and shared
        // across multiple shards. Clone the Arc out of the Box without taking ownership.
        let remote_box = unsafe { &*(remote_store_box_ptr as *const Arc<dyn ObjectStore>) };
        let remote_arc = Arc::clone(remote_box);
        store.set_remote(remote_arc);
    }

    if cache_box_ptr != NULL_PTR {
        let cache_box = unsafe { &*(cache_box_ptr as *const Arc<dyn BlockCache>) };
        store = store.with_cache(Arc::clone(cache_box));
        native_bridge_common::log_info!("ffm: ts_create_tiered_object_store: block cache wired");
    }

    let ptr = Arc::into_raw(Arc::new(store)) as i64;
    native_bridge_common::log_info!(
        "ffm: ts_create_tiered_object_store cache_wired={}",
        cache_box_ptr != NULL_PTR
    );
    Ok(ptr)
}

/// Returns a `Box<Arc<dyn ObjectStore>>` pointer from an existing TieredObjectStore Arc pointer.
/// This is the format that `df_create_reader` expects — a boxed fat pointer to the trait object.
/// Each call creates a new Box with its own Arc clone — caller must free with
/// `ts_destroy_object_store_box_ptr`.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_get_object_store_box_ptr(tiered_store_ptr: i64) -> i64 {
    if tiered_store_ptr == NULL_PTR {
        return Err("ts_get_object_store_box_ptr: null pointer".to_string());
    }
    // Increment strong count so we don't consume the original Arc
    unsafe { Arc::increment_strong_count(tiered_store_ptr as *const TieredObjectStore) };
    let arc: Arc<TieredObjectStore> = unsafe { Arc::from_raw(tiered_store_ptr as *const TieredObjectStore) };
    // Coerce to trait object and box it
    let boxed: Box<Arc<dyn ObjectStore>> = Box::new(arc as Arc<dyn ObjectStore>);
    let ptr = Box::into_raw(boxed) as i64;
    native_bridge_common::log_info!("ffm: ts_get_object_store_box_ptr: ok");
    Ok(ptr)
}

/// Destroy a `Box<Arc<dyn ObjectStore>>` pointer returned by `ts_get_object_store_box_ptr`.
/// Drops the Box and decrements the Arc strong count.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_destroy_object_store_box_ptr(ptr: i64) -> i64 {
    if ptr == NULL_PTR {
        return Err("ts_destroy_object_store_box_ptr: null pointer (0)".to_string());
    }
    let _boxed = unsafe { Box::from_raw(ptr as *mut Arc<dyn ObjectStore>) };
    native_bridge_common::log_info!("ffm: ts_destroy_object_store_box_ptr: ok");
    Ok(0)
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
    native_bridge_common::log_info!("ffm: ts_destroy_tiered_object_store: ok");
    Ok(0)
}

// ---------------------------------------------------------------------------
// File registry operations via TieredObjectStore pointer
// ---------------------------------------------------------------------------

/// Register a file in the TieredObjectStore's registry.
// TODO (writable warm): add ts_register_file for single-file registration (afterSyncToRemote).

/// Batch register files in the TieredObjectStore's registry.
///
/// `entries_ptr`/`entries_len`: UTF-8 string with newline-delimited pairs:
/// `"path1\nremotePath1\nsize1\npath2\nremotePath2\nsize2\n..."`.
/// Each triplet is (path, remotePath, size). For Local files, remotePath can be empty.
/// `count`: number of file triplets (entries_len contains 3*count lines).
/// `location`: 0=Local, 1=Remote — applied to all files in the batch.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_register_files(
    store_ptr: i64,
    entries_ptr: *const u8,
    entries_len: i64,
    count: i32,
    location: i32,
) -> i64 {
    let store = unsafe { arc_from_ptr(store_ptr) }?;
    let entries_str = unsafe { str_from_raw(entries_ptr, entries_len) }
        .map_err(|e| format!("ts_register_files entries: {}", e))?;

    let file_location = FileLocation::from_u8(location as u8)
        .ok_or_else(|| format!("ts_register_files: invalid location {}", location))?;

    let lines: Vec<&str> = entries_str.split('\n').collect();
    let expected = (count as usize) * 3;
    if lines.len() < expected {
        return Err(format!(
            "ts_register_files: expected {} lines ({}*3) but got {}",
            expected, count, lines.len()
        ));
    }

    let registry = store.registry();
    for i in 0..(count as usize) {
        let path = lines[i * 3];
        // Strip leading "/" — object_store::Path normalizes paths without leading slash
        let path = path.strip_prefix('/').unwrap_or(path);
        let remote_path_str = lines[i * 3 + 1];
        let size_str = lines[i * 3 + 2];
        let remote_arc: Option<Arc<str>> = if remote_path_str.is_empty() {
            None
        } else {
            Some(Arc::from(remote_path_str))
        };
        let size: u64 = size_str.parse().unwrap_or(0);
        let entry = crate::types::TieredFileEntry::with_size(file_location, remote_arc, size);
        registry.register(path, entry);
    }

    native_bridge_common::log_debug!("ffm: ts_register_files count={}, location={}", count, file_location);
    Ok(0)
}

/// Remove a file from the registry.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_remove_file(
    store_ptr: i64,
    path_ptr: *const u8,
    path_len: i64,
) -> i64 {
    let store = unsafe { arc_from_ptr(store_ptr) }?;
    let path = unsafe { str_from_raw(path_ptr, path_len) }
        .map_err(|e| format!("ts_remove_file path: {}", e))?;
    // Strip leading "/" — object_store::Path normalizes paths without leading slash
    let path = path.strip_prefix('/').unwrap_or(path);

    store.registry().remove(path, false);
    store.evict_path(path);

    native_bridge_common::log_debug!("ffm: ts_remove_file path='{}'", path);
    Ok(0)
}

// TODO (writable warm): add ts_get_file_location when LOCAL routing is needed.
// TODO (writable warm): add ts_add_remote_store_ptr for late-binding remote store.

#[cfg(test)]
#[path = "ffm_tests.rs"]
mod tests;
