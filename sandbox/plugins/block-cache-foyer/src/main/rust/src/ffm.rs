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
/// # Parameters
/// - `disk_bytes` — total disk capacity in bytes.
/// - `dir_ptr` / `dir_len` — UTF-8 path to the cache directory.
/// - `block_size_bytes` — Foyer disk block size in bytes. Must be ≥ the largest
///   entry ever put into the cache. Set via `format_cache.block_size` (default 64 MB).
/// - `io_engine_ptr` / `io_engine_len` — I/O engine selection: `"auto"`,
///   `"io_uring"`, or `"psync"`. Set via `format_cache.io_engine` (default `"auto"`).
///
/// # Safety
/// `dir_ptr` must point to `dir_len` consecutive valid UTF-8 bytes.
/// `io_engine_ptr` must point to `io_engine_len` consecutive valid UTF-8 bytes.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn foyer_create_cache(
    disk_bytes: u64,
    dir_ptr: *const u8,
    dir_len: u64,
    block_size_bytes: u64,
    io_engine_ptr: *const u8,
    io_engine_len: u64,
) -> i64 {
    if dir_ptr.is_null() {
        return Err("dir_ptr is null".to_string());
    }
    let dir = std::str::from_utf8(std::slice::from_raw_parts(dir_ptr, dir_len as usize))
        .map_err(|e| format!("invalid UTF-8 in dir path: {}", e))?;
    let io_engine = if io_engine_ptr.is_null() {
        "auto"
    } else {
        std::str::from_utf8(std::slice::from_raw_parts(io_engine_ptr, io_engine_len as usize))
            .unwrap_or("auto")
    };
    Ok(Arc::into_raw(Arc::new(FoyerCache::new(
        disk_bytes as usize,
        dir,
        block_size_bytes as usize,
        io_engine,
    ))) as i64)
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

/// Snapshots cache statistics into a caller-supplied `i64[14]` output buffer.
///
/// Two consecutive 7-value sections:
/// - Indices 0–6: cross-tier rollup (`overall`)
/// - Indices 7–13: disk-tier stats (`block_level`)
///
/// Field order within each section (must match the private `Field` enum in
/// `FoyerAggregatedStats` on the Java side):
///
/// | Offset | Field            |
/// |--------|------------------|
/// | +0     | `hit_count`      |
/// | +1     | `hit_bytes`      |
/// | +2     | `miss_count`     |
/// | +3     | `miss_bytes`     |
/// | +4     | `eviction_count` |
/// | +5     | `eviction_bytes` |
/// | +6     | `used_bytes`     |
///
/// Foyer is currently single-tier (disk only): `overall` and `block_level` are identical.
/// The two-section layout is preserved so a future in-memory tier can be added without
/// changing this buffer contract.
///
/// Called by `FoyerBridge.snapshotStats(ptr)` — not on the hot path.
///
/// # Returns
/// `0` on success; `< 0` if `ptr` is invalid or `out` is null.
///
/// # Safety
/// - `ptr` must be a valid handle from [`foyer_create_cache`], not yet destroyed.
/// - `out` must point to a writable buffer of at least **14** `i64` values.
#[no_mangle]
pub unsafe extern "C" fn foyer_snapshot_stats(ptr: i64, out: *mut i64) -> i64 {
    if ptr <= 0 || out.is_null() {
        return -1;
    }
    // Borrow the Arc without consuming it.
    let cache = Arc::from_raw(ptr as *const FoyerCache);
    let single = cache.stats.snapshot();
    // Prevent the Arc from being dropped — we don't own it.
    std::mem::forget(cache);

    // Foyer is currently single-tier (disk only): overall and block_level are identical.
    // When an in-memory tier is added, snapshot each tier separately and write independently.
    let mut flat = [0i64; 14];
    flat[..7].copy_from_slice(&single);  // section 0: overall
    flat[7..].copy_from_slice(&single);  // section 1: block_level
    for (i, &v) in flat.iter().enumerate() {
        *out.add(i) = v;
    }
    0
}
