/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! jemalloc allocator interface: memory stats and runtime tuning.
//!
//! FFI convention (same as all other native bridge functions):
//!   - `>= 0` → success (the stat value in bytes, or 0 for setters)
//!   - `< 0`  → error pointer. Negate and pass to `native_error_message` / `native_error_free`.

use crate::error::{ffm_wrap, into_error_ptr};
use std::sync::OnceLock;
use tikv_jemalloc_ctl::{epoch, epoch_mib, stats, stats::allocated_mib, stats::resident_mib};

struct StatsMib {
    epoch: epoch_mib,
    allocated: allocated_mib,
    resident: resident_mib,
}

static MIB: OnceLock<StatsMib> = OnceLock::new();

fn mib() -> &'static StatsMib {
    MIB.get_or_init(|| StatsMib {
        epoch: epoch::mib().unwrap(),
        allocated: stats::allocated::mib().unwrap(),
        resident: stats::resident::mib().unwrap(),
    })
}

/// Advances the jemalloc epoch and reads both stats atomically.
fn refresh_stats() -> Result<(i64, i64), String> {
    let m = mib();
    m.epoch.advance().map_err(|e| format!("jemalloc epoch advance failed: {}", e))?;
    let alloc = m.allocated.read().map_err(|e| format!("jemalloc allocated read failed: {}", e))? as i64;
    let res = m.resident.read().map_err(|e| format!("jemalloc resident read failed: {}", e))? as i64;
    Ok((alloc, res))
}

/// Returns current jemalloc allocated bytes (live malloc'd objects).
/// Useful for application-level memory accounting and DataFusion memory pool budgeting.
/// On error: returns negative error pointer (use `native_error_message` to read).
///
/// TODO: integrate with node/stats
pub fn allocated_bytes() -> i64 {
    match refresh_stats() {
        Ok((alloc, _)) => alloc,
        Err(msg) => into_error_ptr(msg),
    }
}

/// Returns current jemalloc resident bytes (physical RAM used by native layer only).
/// Excludes JVM heap, metaspace, and other non-jemalloc allocations.
/// On error: returns negative error pointer (use `native_error_message` to read).
///
/// TODO: integrate with node/stats
pub fn resident_bytes() -> i64 {
    match refresh_stats() {
        Ok((_, res)) => res,
        Err(msg) => into_error_ptr(msg),
    }
}

/// FFI: Returns current jemalloc allocated bytes, or negative error pointer.
#[no_mangle]
pub extern "C" fn native_jemalloc_allocated_bytes() -> i64 {
    ffm_wrap("native_jemalloc_allocated_bytes", || refresh_stats().map(|(alloc, _)| alloc))
}

/// FFI: Returns current jemalloc resident bytes, or negative error pointer.
#[no_mangle]
pub extern "C" fn native_jemalloc_resident_bytes() -> i64 {
    ffm_wrap("native_jemalloc_resident_bytes", || refresh_stats().map(|(_, res)| res))
}

/// FFI: Sets dirty_decay_ms for all arenas at runtime. Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.dirty_decay_ms` changes.
#[no_mangle]
pub extern "C" fn native_jemalloc_set_dirty_decay_ms(ms: i64) -> i64 {
    ffm_wrap("native_jemalloc_set_dirty_decay_ms", || set_all_arenas(b"dirty_decay_ms\0", ms))
}

/// FFI: Sets muzzy_decay_ms for all arenas at runtime. Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.muzzy_decay_ms` changes.
#[no_mangle]
pub extern "C" fn native_jemalloc_set_muzzy_decay_ms(ms: i64) -> i64 {
    ffm_wrap("native_jemalloc_set_muzzy_decay_ms", || set_all_arenas(b"muzzy_decay_ms\0", ms))
}

/// Applies a setting to all existing jemalloc arenas.
/// Skips arenas that are not available (destroyed or internal).
fn set_all_arenas(suffix: &[u8], ms: i64) -> Result<i64, String> {
    let narenas: u32 = unsafe { tikv_jemalloc_ctl::raw::read(b"arenas.narenas\0") }
        .map_err(|e| format!("failed to read arenas.narenas: {}", e))?;
    let suffix_str = std::str::from_utf8(&suffix[..suffix.len() - 1]).unwrap();
    let mut any_success = false;
    for i in 0..narenas {
        let key = format!("arena.{}.{}\0", i, suffix_str);
        if unsafe { tikv_jemalloc_ctl::raw::write(key.as_bytes(), ms as isize) }.is_ok() {
            any_success = true;
        }
    }
    if any_success {
        Ok(0)
    } else {
        Err(format!("failed to set {} on any arena", suffix_str))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[global_allocator]
    static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

    #[test]
    fn allocated_bytes_is_positive() {
        assert!(allocated_bytes() > 0);
    }

    #[test]
    fn resident_bytes_is_positive() {
        assert!(resident_bytes() > 0);
    }

    #[test]
    fn allocated_increases_after_allocation() {
        let before = allocated_bytes();
        let _data: Vec<u8> = vec![42u8; 1024 * 1024];
        let after = allocated_bytes();
        assert!(after > before, "expected {after} > {before}");
    }

    #[test]
    fn set_dirty_decay_ms_applies_at_runtime() {
        let rc = native_jemalloc_set_dirty_decay_ms(5000);
        assert_eq!(rc, 0, "setter should succeed, got {}", rc);

        // Read back from arena 0 to verify it took effect
        let actual: isize =
            unsafe { tikv_jemalloc_ctl::raw::read(b"arena.0.dirty_decay_ms\0") }.unwrap();
        assert_eq!(actual, 5000);

        // Restore default
        native_jemalloc_set_dirty_decay_ms(30000);
    }

    #[test]
    fn set_muzzy_decay_ms_applies_at_runtime() {
        let rc = native_jemalloc_set_muzzy_decay_ms(10000);
        assert_eq!(rc, 0, "setter should succeed, got {}", rc);

        let actual: isize =
            unsafe { tikv_jemalloc_ctl::raw::read(b"arena.0.muzzy_decay_ms\0") }.unwrap();
        assert_eq!(actual, 10000);

        // Restore default
        native_jemalloc_set_muzzy_decay_ms(30000);
    }
}
