/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use opensearch_query_spi::SessionOptimizerProviderRef;

use crate::runtime::{self, LiquidOnlyRuntime};
use crate::LiquidOptimizerProvider;

/// # Safety
/// `ptr` must be null or point to `len` valid bytes for the call's duration.
unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Option<&'a str> {
    if ptr.is_null() || len <= 0 {
        return None;
    }
    let bytes = std::slice::from_raw_parts(ptr, len as usize);
    std::str::from_utf8(bytes).ok()
}

/// Initialize the global runtime and return an opaque provider handle (`0` on
/// failure). Caller frees it once via [`lc_destroy_optimizer`].
///
/// # Safety
/// `policy_ptr`/`policy_len` must describe a valid byte range or be null/0.
#[no_mangle]
pub unsafe extern "C" fn lc_create_optimizer(
    size: i64,
    policy_ptr: *const u8,
    policy_len: i64,
    enabled: i64,
) -> i64 {
    let policy = str_from_raw(policy_ptr, policy_len).unwrap_or("lru");
    let size = if size < 0 { 0 } else { size as u64 };
    match LiquidOnlyRuntime::init(size, policy) {
        Ok(rt) => {
            rt.set_enabled(enabled != 0);
            let provider: SessionOptimizerProviderRef =
                Arc::new(LiquidOptimizerProvider::new(rt.optimizer()));
            opensearch_query_spi::into_handle(provider)
        }
        Err(e) => {
            log::error!("lc_create_optimizer: failed to init liquid cache: {}", e);
            0
        }
    }
}

/// Free a handle from [`lc_create_optimizer`]. `0` is ignored.
///
/// # Safety
/// `handle` must be `0` or a handle from [`lc_create_optimizer`] not yet freed.
#[no_mangle]
pub unsafe extern "C" fn lc_destroy_optimizer(handle: i64) {
    if handle != 0 {
        let _ = opensearch_query_spi::from_handle(handle);
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
