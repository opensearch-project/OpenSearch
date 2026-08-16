/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use crate::runtime::{self, LiquidOnlyRuntime};

// `lc_*` control surface for the plugin's settings/REST layer. These symbols are
// linked into the analytics engine native library (feature `liquid_cache`) and
// bound by the Java plugin. `lc_init` builds the process-global cache at node
// startup; the rest drive the enable/memory/columns/stats/clear settings.

/// Initialize the process-global liquid cache. First call wins. `eviction_ptr`
/// is an optional UTF-8 policy name (`"lru"` or `"liquid"`); null/empty => lru.
/// Returns 0 on success, -1 on init failure.
///
/// # Safety
/// `eviction_ptr`/`eviction_len` must describe a valid UTF-8 range or be null/0.
#[no_mangle]
pub unsafe extern "C" fn lc_init(
    max_memory_bytes: i64,
    enabled: i64,
    eviction_ptr: *const u8,
    eviction_len: i64,
) -> i64 {
    let eviction = if eviction_ptr.is_null() || eviction_len <= 0 {
        "lru"
    } else {
        std::str::from_utf8(std::slice::from_raw_parts(eviction_ptr, eviction_len as usize))
            .unwrap_or("lru")
    };
    let size = if max_memory_bytes < 0 {
        0
    } else {
        max_memory_bytes as u64
    };
    match LiquidOnlyRuntime::init(size, eviction) {
        Ok(rt) => {
            rt.set_enabled(enabled != 0);
            0
        }
        Err(e) => {
            log::error!("lc_init: liquid cache init failed: {e}");
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn lc_set_enabled(enabled: i64) {
    LiquidOnlyRuntime::set_enabled_globally(enabled != 0);
}

#[no_mangle]
pub extern "C" fn lc_set_memory_limit(bytes: i64) {
    if bytes >= 0 {
        LiquidOnlyRuntime::set_max_memory_bytes_globally(bytes as usize);
    }
}

#[no_mangle]
pub extern "C" fn lc_set_indexed_max_columns(count: i64) {
    if count > 0 {
        runtime::set_lc_indexed_max_columns(count as usize);
    }
}

#[no_mangle]
pub extern "C" fn lc_set_listing_max_columns(count: i64) {
    if count > 0 {
        runtime::set_lc_listing_max_columns(count as usize);
    }
}

/// Clear all cache entries. Panic-guarded: a poisoned lock must not unwind
/// across the `extern "C"` boundary. No-op before init.
#[no_mangle]
pub extern "C" fn lc_reset_cache() {
    let result = std::panic::catch_unwind(|| {
        LiquidOnlyRuntime::reset_cache_if_initialized();
    });
    if result.is_err() {
        log::error!("lc_reset_cache: panic while clearing liquid cache (ignored)");
    }
}

/// Write 8 counters into `out_ptr` (zeros if uninitialized): `[cache_hit,
/// cache_miss, predicate_evals, memory_evictions, transcodes, total_entries,
/// memory_usage_bytes, max_memory_bytes]`.
///
/// # Safety
/// `out_ptr` must be null or point to space for at least 8 `i64` values.
#[no_mangle]
pub unsafe extern "C" fn lc_stats(out_ptr: *mut i64) {
    if out_ptr.is_null() {
        return;
    }
    let stats = LiquidOnlyRuntime::liquid_cache_stats_for_ffi().unwrap_or([0i64; 8]);
    std::ptr::copy_nonoverlapping(stats.as_ptr(), out_ptr, 8);
}
