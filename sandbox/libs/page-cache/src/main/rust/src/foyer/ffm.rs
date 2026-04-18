/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM lifecycle entry points exported to Java.

use std::sync::Arc;
use native_bridge_common::ffm_safe;
use crate::foyer::foyer_cache::FoyerCache;

/// Create a [`FoyerCache`] and return an opaque `Arc` handle as `i64`.
///
/// # Safety
/// `dir_ptr` must point to `dir_len` consecutive valid UTF-8 bytes.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn foyer_create_cache(
    disk_bytes: u64,
    dir_ptr: *const u8,
    dir_len: u64,
) -> i64 {
    if dir_ptr.is_null() {
        return Err("dir_ptr is null".to_string());
    }
    let dir = std::str::from_utf8(std::slice::from_raw_parts(dir_ptr, dir_len as usize))
        .map_err(|e| format!("invalid UTF-8 in dir path: {}", e))?;
    Ok(Arc::into_raw(Arc::new(FoyerCache::new(disk_bytes as usize, dir))) as i64)
}

/// Destroy a [`FoyerCache`] previously created by [`foyer_create_cache`].
///
/// Returns `0` on success, `< 0` (error pointer) if `ptr` is invalid.
///
/// # Safety
/// `ptr` must be a value returned by [`foyer_create_cache`] not yet destroyed.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn foyer_destroy_cache(ptr: i64) -> i64 {
    if ptr <= 0 {
        return Err(format!("foyer_destroy_cache: invalid ptr {}", ptr));
    }
    drop(Arc::from_raw(ptr as *const FoyerCache));
    Ok(0)
}
