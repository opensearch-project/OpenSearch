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

/// Create a [`FoyerCache`] and return an opaque `Box<Arc<dyn BlockCache>>` fat pointer as `i64`.
///
/// The returned pointer can be passed directly as `cache_box_ptr` to
/// `ts_create_tiered_object_store` without any further conversion.
///
/// # Parameters
/// - `disk_bytes` — total disk capacity in bytes.
/// - `dir_ptr` / `dir_len` — UTF-8 path to the cache directory.
/// - `block_size_bytes` — Foyer disk block size in bytes.
/// - `io_engine_ptr` / `io_engine_len` — I/O engine: `"auto"`, `"io_uring"`, or `"psync"`.
/// - `sweep_interval_secs` — background key_index sweep interval in seconds. `0` = default (30 s).
///   Maps to `block_cache.foyer.key_index_sweep_interval_seconds` on the Java side.
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
    sweep_interval_secs: u64,
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
    // Return a fat Arc<dyn BlockCache> from the start — no separate wrapping step needed.
    let cache: Arc<dyn crate::traits::BlockCache> = Arc::new(FoyerCache::new(
        disk_bytes as usize,
        dir,
        block_size_bytes as usize,
        io_engine,
        sweep_interval_secs,
    ));
    Ok(Box::into_raw(Box::new(cache)) as i64)
}

/// Destroy a cache previously created by [`foyer_create_cache`].
///
/// Returns `0` on success, `< 0` if `ptr` is invalid.
///
/// # Safety
/// `ptr` must be a value returned by [`foyer_create_cache`], not yet destroyed.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn foyer_destroy_cache(ptr: i64) -> i64 {
    if ptr <= 0 {
        return Err(format!("foyer_destroy_cache: invalid ptr {}", ptr));
    }
    drop(Box::from_raw(ptr as *mut Arc<dyn crate::traits::BlockCache>));
    Ok(0)
}

/// Snapshots cache statistics into a caller-supplied `i64[20]` output buffer.
///
/// Two consecutive 10-value sections:
/// - Indices 0–9:  cross-tier rollup (`overall`)
/// - Indices 10–19: disk-tier stats (`block_level`)
///
/// Field order within each section (must match `FoyerAggregatedStats.Field` on the Java side):
///
/// | Offset | Field              |
/// |--------|--------------------|
/// | +0     | `hit_count`        |
/// | +1     | `hit_bytes`        |
/// | +2     | `miss_count`       |
/// | +3     | `miss_bytes`       |
/// | +4     | `eviction_count`   |
/// | +5     | `eviction_bytes`   |
/// | +6     | `used_bytes`       |
/// | +7     | `removed_count`    |
/// | +8     | `removed_bytes`    |
/// | +9     | `active_in_bytes`  |
///
/// Foyer is currently single-tier (disk only): `overall` and `block_level` are identical.
///
/// # Returns
/// `0` on success; `< 0` if `ptr` is invalid or `out` is null.
///
/// # Safety
/// - `ptr` must be a valid handle from [`foyer_create_cache`], not yet destroyed.
/// - `out` must point to a writable buffer of at least **20** `i64` values.
#[no_mangle]
pub unsafe extern "C" fn foyer_snapshot_stats(ptr: i64, out: *mut i64) -> i64 {
    if ptr <= 0 || out.is_null() {
        return -1;
    }
    // Borrow the Box<Arc<dyn BlockCache>> without consuming it.
    let boxed = &*(ptr as *const Arc<dyn crate::traits::BlockCache>);
    // Downcast to FoyerCache to access Foyer-specific stats.
    let foyer = match boxed.as_any().downcast_ref::<FoyerCache>() {
        Some(f) => f,
        None => return -1,
    };
    let single = foyer.stats.snapshot();

    // Foyer is currently single-tier (disk only): overall and block_level are identical.
    // 10 fields × 2 sections = 20 longs total.
    let mut flat = [0i64; 20];
    flat[..10].copy_from_slice(&single);
    flat[10..].copy_from_slice(&single);
    for (i, &v) in flat.iter().enumerate() {
        *out.add(i) = v;
    }
    0
}

/// Evict all cache entries whose key starts with `prefix`.
///
/// Called by Java's `NodeCacheOrchestratorCleaner` on shard/index deletion.
///
/// # Safety
/// - `ptr` must be a valid handle from [`foyer_create_cache`], not yet destroyed.
/// - `prefix_ptr` must point to `prefix_len` consecutive valid UTF-8 bytes.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn foyer_evict_prefix(ptr: i64, prefix_ptr: *const u8, prefix_len: u64) -> i64 {
    if ptr <= 0 {
        return Err("foyer_evict_prefix: invalid ptr".to_string());
    }
    if prefix_ptr.is_null() {
        return Err("foyer_evict_prefix: null prefix_ptr".to_string());
    }
    let prefix = unsafe {
        let bytes = std::slice::from_raw_parts(prefix_ptr, prefix_len as usize);
        std::str::from_utf8(bytes).map_err(|e| format!("foyer_evict_prefix: invalid utf-8: {}", e))?
    };
    let boxed = unsafe { &*(ptr as *const Arc<dyn crate::traits::BlockCache>) };
    boxed.evict_prefix(prefix);
    native_bridge_common::log_debug!("ffm: foyer_evict_prefix prefix='{}'", prefix);
    Ok(0)
}
