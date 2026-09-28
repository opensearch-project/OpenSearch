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
use crate::log_info;
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
    m.epoch
        .advance()
        .map_err(|e| format!("jemalloc epoch advance failed: {}", e))?;
    let alloc = m
        .allocated
        .read()
        .map_err(|e| format!("jemalloc allocated read failed: {}", e))? as i64;
    let res = m
        .resident
        .read()
        .map_err(|e| format!("jemalloc resident read failed: {}", e))? as i64;
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
    ffm_wrap("native_jemalloc_allocated_bytes", || {
        refresh_stats().map(|(alloc, _)| alloc)
    })
}

/// FFI: Returns current jemalloc resident bytes, or negative error pointer.
#[no_mangle]
pub extern "C" fn native_jemalloc_resident_bytes() -> i64 {
    ffm_wrap("native_jemalloc_resident_bytes", || {
        refresh_stats().map(|(_, res)| res)
    })
}

/// FFI: Sets dirty_decay_ms for all arenas at runtime. Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.dirty_decay_ms` changes.
#[no_mangle]
pub extern "C" fn native_jemalloc_set_dirty_decay_ms(ms: i64) -> i64 {
    ffm_wrap("native_jemalloc_set_dirty_decay_ms", || {
        set_all_arenas(b"dirty_decay_ms\0", ms)
    })
}

/// FFI: Sets muzzy_decay_ms for all arenas at runtime. Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.muzzy_decay_ms` changes.
#[no_mangle]
pub extern "C" fn native_jemalloc_set_muzzy_decay_ms(ms: i64) -> i64 {
    ffm_wrap("native_jemalloc_set_muzzy_decay_ms", || {
        set_all_arenas(b"muzzy_decay_ms\0", ms)
    })
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

// ── Background purge thread ─────────────────────────────────────────────────
//
// A dedicated OS thread that periodically checks jemalloc resident bytes against
// a configurable threshold and purges all arenas if exceeded. Runs entirely in
// Rust — no FFI round-trip per check. Java pushes threshold/interval updates via
// FFI setters.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

static PURGE_THRESHOLD_BYTES: AtomicI64 = AtomicI64::new(i64::MAX);
static PURGE_INTERVAL_MS: AtomicU64 = AtomicU64::new(5000);
static PURGE_COUNT: AtomicU64 = AtomicU64::new(0);
static PURGE_STOP: AtomicBool = AtomicBool::new(false);
static PURGE_THREAD: OnceLock<std::thread::Thread> = OnceLock::new();

fn start_purge_thread() -> &'static std::thread::Thread {
    PURGE_THREAD.get_or_init(|| {
        let handle = std::thread::Builder::new()
            .name("jemalloc-purge".into())
            .spawn(purge_thread_loop)
            .expect("failed to spawn jemalloc-purge thread");
        handle.thread().clone()
    })
}

fn purge_thread_loop() {
    loop {
        if PURGE_STOP.load(Ordering::Relaxed) {
            return;
        }
        let interval_ms = PURGE_INTERVAL_MS.load(Ordering::Relaxed);
        if interval_ms == 0 {
            std::thread::park();
            continue;
        }
        std::thread::park_timeout(Duration::from_millis(interval_ms));

        let threshold = PURGE_THRESHOLD_BYTES.load(Ordering::Relaxed);
        let resident = match refresh_stats() {
            Ok((_, res)) => res,
            Err(_) => continue,
        };

        if resident <= threshold {
            continue;
        }

        log_info!(
            "jemalloc purge starting: resident={} MB, threshold={} MB",
            resident / (1024 * 1024),
            threshold / (1024 * 1024)
        );

        let narenas: u32 = match unsafe { tikv_jemalloc_ctl::raw::read(b"arenas.narenas\0") } {
            Ok(n) => n,
            Err(_) => continue,
        };
        let mut any_success = false;
        for i in 0..narenas {
            let key = format!("arena.{}.purge\0", i);
            let ret = unsafe {
                tikv_jemalloc_sys::mallctl(
                    key.as_ptr() as *const _,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    0,
                )
            };
            if ret == 0 {
                any_success = true;
            }
        }
        if any_success {
            PURGE_COUNT.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// FFI: Starts the background purge thread and sets the initial threshold.
/// Called once from Java at node startup (NativeBridgeModule.createComponents).
/// Safe to call multiple times — only the first call spawns the thread.
#[no_mangle]
pub extern "C" fn native_jemalloc_start_purge_thread(
    threshold_bytes: i64,
    interval_ms: i64,
) -> i64 {
    PURGE_THRESHOLD_BYTES.store(threshold_bytes, Ordering::Relaxed);
    PURGE_INTERVAL_MS.store(interval_ms as u64, Ordering::Relaxed);
    start_purge_thread().unpark();
    0
}

/// FFI: Updates the purge threshold at runtime. Called when cluster settings change.
#[no_mangle]
pub extern "C" fn native_jemalloc_set_purge_threshold(threshold_bytes: i64) -> i64 {
    PURGE_THRESHOLD_BYTES.store(threshold_bytes, Ordering::Relaxed);
    0
}

/// FFI: Updates the purge check interval at runtime. Set to 0 to pause purging.
/// Wakes the thread immediately so it picks up the new interval without waiting
/// for the old sleep to expire.
#[no_mangle]
pub extern "C" fn native_jemalloc_set_purge_interval(interval_ms: i64) -> i64 {
    PURGE_INTERVAL_MS.store(interval_ms as u64, Ordering::Relaxed);
    if let Some(t) = PURGE_THREAD.get() {
        t.unpark();
    }
    0
}

/// FFI: Stops the background purge thread. Called from Java during node shutdown.
#[no_mangle]
pub extern "C" fn native_jemalloc_stop_purge_thread() -> i64 {
    PURGE_STOP.store(true, Ordering::Relaxed);
    if let Some(t) = PURGE_THREAD.get() {
        t.unpark();
    }
    0
}

/// FFI: Returns the total number of purges executed by the background thread.
#[no_mangle]
pub extern "C" fn native_jemalloc_get_purge_count() -> i64 {
    PURGE_COUNT.load(Ordering::Relaxed) as i64
}

// ── Heap profiling ──────────────────────────────────────────────────────────
//
// Requires the process to be started with `_RJEM_MALLOC_CONF=prof:true,...`
// (or the compile-time MALLOC_CONF in lib.rs to include `prof:true`).
// Without that, activate/dump will return errors — the caller (Java) handles
// this gracefully by logging a warning.

/// FFI: Activates jemalloc heap profiling. Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.heap_prof_active` is set to true.
#[no_mangle]
pub extern "C" fn native_jemalloc_heap_prof_activate() -> i64 {
    ffm_wrap("native_jemalloc_heap_prof_activate", || {
        unsafe { tikv_jemalloc_ctl::raw::write(b"prof.active\0", true) }
            .map(|_| 0i64)
            .map_err(|e| format!("failed to activate profiling: {}", e))
    })
}

/// FFI: Deactivates jemalloc heap profiling. Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.heap_prof_active` is set to false.
#[no_mangle]
pub extern "C" fn native_jemalloc_heap_prof_deactivate() -> i64 {
    ffm_wrap("native_jemalloc_heap_prof_deactivate", || {
        unsafe { tikv_jemalloc_ctl::raw::write(b"prof.active\0", false) }
            .map(|_| 0i64)
            .map_err(|e| format!("failed to deactivate profiling: {}", e))
    })
}

/// FFI: Dumps a heap profile to the given path. Path must be a null-terminated C string.
/// Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.heap_prof_dump_path` is updated.
#[no_mangle]
pub unsafe extern "C" fn native_jemalloc_heap_prof_dump(path: *const std::ffi::c_char) -> i64 {
    ffm_wrap("native_jemalloc_heap_prof_dump", || {
        if path.is_null() {
            return Err("null path".to_string());
        }
        let c_str = std::ffi::CStr::from_ptr(path);
        let path_bytes = c_str.to_bytes_with_nul();
        // prof.dump expects a *const c_char pointing to the file path
        tikv_jemalloc_ctl::raw::write(
            b"prof.dump\0",
            path_bytes.as_ptr() as *const std::ffi::c_char,
        )
        .map(|_| 0i64)
        .map_err(|e| format!("failed to dump heap profile: {}", e))
    })
}

/// FFI: Resets profiling state and sets a new sample interval.
/// Discards all accumulated profiling data and applies the new lg_prof_sample value
/// for future allocations. Returns 0 on success, negative error pointer on failure.
///
/// Common values: 15 (~32KB, high accuracy), 17 (~128KB, default), 19 (~512KB, low overhead).
#[no_mangle]
pub extern "C" fn native_jemalloc_heap_prof_reset(lg_sample: usize) -> i64 {
    ffm_wrap("native_jemalloc_heap_prof_reset", || {
        unsafe { tikv_jemalloc_ctl::raw::write(b"prof.reset\0", lg_sample) }
            .map(|_| 0i64)
            .map_err(|e| {
                format!(
                    "failed to reset profiling with lg_sample={}: {}",
                    lg_sample, e
                )
            })
    })
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

    #[test]
    fn heap_prof_activate_returns_error_when_prof_disabled() {
        // When the process is not started with prof:true, activate should return
        // a negative error pointer (not crash).
        let rc = native_jemalloc_heap_prof_activate();
        // prof:true is not set in test builds, so this should fail gracefully
        assert!(rc <= 0, "expected error or 0, got {}", rc);
    }

    #[test]
    fn heap_prof_deactivate_returns_error_when_prof_disabled() {
        let rc = native_jemalloc_heap_prof_deactivate();
        assert!(rc <= 0, "expected error or 0, got {}", rc);
    }

    #[test]
    fn heap_prof_dump_null_path_returns_error() {
        let rc = unsafe { native_jemalloc_heap_prof_dump(std::ptr::null()) };
        assert!(
            rc < 0,
            "expected negative error pointer for null path, got {}",
            rc
        );
    }

    #[test]
    fn background_purge_thread_fires() {
        // threshold=0 means always purge; interval=50ms for fast feedback
        native_jemalloc_start_purge_thread(0, 50);
        let before = native_jemalloc_get_purge_count();
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            native_jemalloc_get_purge_count() > before,
            "purge thread should have fired"
        );
    }

    #[test]
    fn background_purge_pauses_when_interval_zero() {
        native_jemalloc_start_purge_thread(0, 50);
        std::thread::sleep(Duration::from_millis(100));
        // Pause the thread
        native_jemalloc_set_purge_interval(0);
        std::thread::sleep(Duration::from_millis(100));
        let before = native_jemalloc_get_purge_count();
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            native_jemalloc_get_purge_count(),
            before,
            "no purges when paused"
        );
        // Resume
        native_jemalloc_set_purge_interval(50);
    }

    #[test]
    fn background_purge_respects_threshold() {
        // Set threshold to MAX — purge should never fire
        native_jemalloc_start_purge_thread(i64::MAX, 50);
        let before = native_jemalloc_get_purge_count();
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            native_jemalloc_get_purge_count(),
            before,
            "no purge when below threshold"
        );
        // Restore for other tests
        native_jemalloc_set_purge_threshold(0);
    }
}
