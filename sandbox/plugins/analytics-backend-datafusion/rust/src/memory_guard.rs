/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unified jemalloc-based memory guard for pool override decisions.
//!
//! All RSS checks go through [`cached_resident_bytes()`] — a single source of
//! truth refreshed at most once per 100ms. This avoids expensive jemalloc
//! `epoch.advance()` calls on the hot path while keeping the memory picture
//! consistent across all decision layers (hard guard, override, cancel, admission).
//!
//! Thresholds are configurable at runtime via `set_thresholds`.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

// --- Cached RSS ---

const RESIDENT_CACHE_INTERVAL_MS: u64 = 100;
static CACHED_RESIDENT: AtomicI64 = AtomicI64::new(0);
// Initialized to u64::MAX so the first call always refreshes (any now_ms - MAX wraps to > 100).
static LAST_CHECK_MS: AtomicU64 = AtomicU64::new(u64::MAX);
static EPOCH_BASE: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

/// Returns jemalloc resident bytes, cached for up to 100ms on the happy path.
///
/// When the cached value is above the spill threshold, bypasses the cache and
/// reads fresh — because a stale-high value can incorrectly block the override
/// (which allows spill sort buffers to allocate). The cost of a fresh read (~1-5µs)
/// is acceptable under pressure since it only fires when memory is elevated.
///
/// On the happy path (RSS below threshold), only one thread per 100ms interval pays
/// the epoch.advance() cost; all others get the cached value in <1ns.
pub fn cached_resident_bytes() -> i64 {
    let cached = CACHED_RESIDENT.load(Ordering::Relaxed);

    // If last known value was above spill threshold, bypass cache and read fresh.
    // A stale-high value would block the override and prevent spill from completing.
    if cached > 0 {
        let spill_x1000 = EXECUTION_SPILL_X1000.load(Ordering::Relaxed);
        let limit = pool_limit_for_guard();
        if limit > 0 {
            let threshold = (limit as u64 * spill_x1000 / 1000) as i64;
            if cached >= threshold {
                let fresh = native_bridge_common::allocator::resident_bytes();
                CACHED_RESIDENT.store(fresh, Ordering::Relaxed);
                return fresh;
            }
        }
    }

    // Happy path: return cached value, refresh if interval elapsed.
    let base = EPOCH_BASE.get_or_init(Instant::now);
    let now_ms = base.elapsed().as_millis() as u64;
    let last = LAST_CHECK_MS.load(Ordering::Relaxed);
    if now_ms.wrapping_sub(last) >= RESIDENT_CACHE_INTERVAL_MS {
        if LAST_CHECK_MS
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            let r = native_bridge_common::allocator::resident_bytes();
            CACHED_RESIDENT.store(r, Ordering::Relaxed);
            return r;
        }
    }
    CACHED_RESIDENT.load(Ordering::Relaxed)
}

// Pool limit stored for the guard's threshold check. Set once from create_global_runtime.
static POOL_LIMIT_FOR_GUARD: AtomicI64 = AtomicI64::new(0);

/// Set the pool limit used by the cached RSS pressure check.
/// Called once at runtime creation.
pub fn set_pool_limit_for_guard(limit: i64) {
    POOL_LIMIT_FOR_GUARD.store(limit, Ordering::Release);
}

fn pool_limit_for_guard() -> i64 {
    POOL_LIMIT_FOR_GUARD.load(Ordering::Relaxed)
}

// --- Thresholds ---

/// Minimum pool size (bytes) for jemalloc override to activate.
/// Below this, the pool is assumed to be a unit test / benchmark with
/// artificial limits — never override.
const MIN_POOL_FOR_OVERRIDE: usize = 16 * 1024 * 1024; // 16MB

// Configurable thresholds stored as fixed-point (×1000) in atomics.
static ADMISSION_THROTTLE_X1000: AtomicU64 = AtomicU64::new(750);
static ADMISSION_REJECT_X1000: AtomicU64 = AtomicU64::new(850);
static EXECUTION_SPILL_X1000: AtomicU64 = AtomicU64::new(850);
static EXECUTION_CRITICAL_X1000: AtomicU64 = AtomicU64::new(950);

// Total byte budget for the spill-gate exemption (see
// `DynamicLimitPool::try_grow`). Limits how much memory spillable consumers can
// be allowed through the 85% check at the same time, so several spills together
// stay below the 95% limit. Default 512MB.
static SPILL_EXEMPT_CAP_BYTES: AtomicU64 = AtomicU64::new(512 * 1024 * 1024);

/// Set the spill-gate exemption byte cap at runtime.
pub fn set_spill_exempt_cap_bytes(bytes: u64) {
    SPILL_EXEMPT_CAP_BYTES.store(bytes, Ordering::Release);
}

/// Current spill-gate exemption byte cap.
pub fn spill_exempt_cap_bytes() -> usize {
    SPILL_EXEMPT_CAP_BYTES.load(Ordering::Acquire) as usize
}

/// Which layer is asking for the override check.
#[derive(Debug, Clone, Copy)]
pub enum OverrideContext {
    /// Admission-time: deciding whether to reduce target_partitions.
    Admission,
    /// Execution try_grow: deciding whether to allow despite pool rejection.
    Execution,
}

/// Configurable memory thresholds.
///
/// Admission thresholds control who gets in.
/// Execution thresholds control what happens during the query.
#[derive(Debug, Clone, Copy)]
pub struct MemoryThresholds {
    /// RSS above this → reduce parallelism for new queries. Default: 0.75
    pub admission_throttle: f64,
    /// RSS above this → reject new queries (429). Default: 0.85
    pub admission_reject: f64,
    /// RSS above this → force spill + disable override in try_grow. Default: 0.85
    pub execution_spill: f64,
    /// RSS above this → hard guard rejects (pre-CAS) + cancel (post-CAS). Default: 0.95
    pub execution_critical: f64,
}

impl Default for MemoryThresholds {
    fn default() -> Self {
        Self {
            admission_throttle: 0.75,
            admission_reject: 0.85,
            execution_spill: 0.85,
            execution_critical: 0.95,
        }
    }
}

/// Set the memory thresholds at runtime. Called from Java when cluster
/// settings change. Thread-safe (atomic stores).
pub fn set_thresholds(thresholds: MemoryThresholds) {
    ADMISSION_THROTTLE_X1000.store(
        (thresholds.admission_throttle * 1000.0) as u64,
        Ordering::Release,
    );
    ADMISSION_REJECT_X1000.store(
        (thresholds.admission_reject * 1000.0) as u64,
        Ordering::Release,
    );
    EXECUTION_SPILL_X1000.store(
        (thresholds.execution_spill * 1000.0) as u64,
        Ordering::Release,
    );
    EXECUTION_CRITICAL_X1000.store(
        (thresholds.execution_critical * 1000.0) as u64,
        Ordering::Release,
    );
}

/// Read current thresholds.
pub fn get_thresholds() -> MemoryThresholds {
    MemoryThresholds {
        admission_throttle: ADMISSION_THROTTLE_X1000.load(Ordering::Acquire) as f64 / 1000.0,
        admission_reject: ADMISSION_REJECT_X1000.load(Ordering::Acquire) as f64 / 1000.0,
        execution_spill: EXECUTION_SPILL_X1000.load(Ordering::Acquire) as f64 / 1000.0,
        execution_critical: EXECUTION_CRITICAL_X1000.load(Ordering::Acquire) as f64 / 1000.0,
    }
}

/// Returns `true` if RSS exceeds the critical threshold — the query should be
/// cancelled. This is the last-resort path (post-CAS-fail, post-override-denied):
/// the pool rejected, jemalloc confirms pressure, and spill alone can't recover
/// fast enough. Cancel the query to protect the node.
///
/// The same critical threshold is used by the hard guard (pre-CAS) to force spill
/// earlier — that path is recoverable. This path fires only when spill was already
/// attempted or cannot help.
pub fn should_cancel_query(pool_limit_bytes: usize) -> bool {
    if pool_limit_bytes < MIN_POOL_FOR_OVERRIDE {
        return false;
    }
    let resident = cached_resident_bytes();
    if resident <= 0 {
        return false;
    }
    let critical_bytes = (pool_limit_bytes as u64)
        .saturating_mul(EXECUTION_CRITICAL_X1000.load(Ordering::Acquire))
        / 1000;
    resident >= critical_bytes as i64
}

/// Check whether jemalloc says physical memory has headroom, meaning the
/// pool's rejection is a false positive (stale accounting).
///
/// Uses `resident_bytes` (physical RSS) instead of `allocated_bytes` (live objects).
/// `allocated_bytes` undercounts true memory pressure because jemalloc retains
/// freed pages in thread caches and arenas (dirty/muzzy decay). Under concurrent
/// workloads, the gap between allocated and resident can be 10-20GB, causing the
/// override to fire when the system is actually near OOM.
///
/// Returns `true` if the override should fire (proceed despite pool rejection).
/// Returns `false` if pressure is real or stats are unavailable.
///
/// # Arguments
/// - `pool_limit_bytes`: the pool's configured limit
/// - `context`: which layer is asking (determines threshold)
pub fn should_override(pool_limit_bytes: usize, context: OverrideContext) -> bool {
    if pool_limit_bytes < MIN_POOL_FOR_OVERRIDE {
        return false;
    }

    let resident = cached_resident_bytes();
    if resident <= 0 {
        return false;
    }

    let threshold_x1000 = match context {
        OverrideContext::Admission => ADMISSION_REJECT_X1000.load(Ordering::Acquire),
        OverrideContext::Execution => EXECUTION_SPILL_X1000.load(Ordering::Acquire),
    };

    let threshold_bytes = (pool_limit_bytes as u64).saturating_mul(threshold_x1000) / 1000;
    resident < threshold_bytes as i64
}

/// Proactive admission check: returns `true` if jemalloc resident memory
/// already exceeds the admission threshold (70% of pool limit by default).
///
/// Called BEFORE query execution (at budget acquisition) to reject or reduce
/// concurrency early — before any hash table allocation occurs. This prevents
/// the "20 queries all pass admission simultaneously" burst that causes OOM.
///
/// Cost: one `epoch.advance` + stat read (~1-5µs). Called once per query at
/// admission, not per-batch.
pub fn is_memory_pressured(pool_limit_bytes: usize) -> bool {
    if pool_limit_bytes < MIN_POOL_FOR_OVERRIDE {
        return false;
    }

    let resident = cached_resident_bytes();
    if resident <= 0 {
        return false;
    }

    let threshold_x1000 = ADMISSION_THROTTLE_X1000.load(Ordering::Acquire);
    let threshold_bytes = (pool_limit_bytes as u64).saturating_mul(threshold_x1000) / 1000;
    resident >= threshold_bytes as i64
}

// ---------------------------------------------------------------------------
// Disk spill budget: proactive disk pressure check
// ---------------------------------------------------------------------------

/// Fraction of available disk to allow per query (×1000 for atomic integer storage).
static DISK_FRACTION_X1000: AtomicU64 = AtomicU64::new(100); // 10% = 100/1000

/// Stored spill directory path. Set once at runtime creation.
static SPILL_DIR: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Whether spill is enabled at runtime construction. Stays `false` when DataFusion
/// is built with `DiskManagerMode::Disabled` (i.e. `datafusion.spill_directory` unset).
/// Used by `per_query_spill_budget` to short-circuit before touching `SPILL_DIR` so
/// the disabled path doesn't masquerade as "disk dying" and clamp parallelism.
static SPILL_ENABLED: AtomicBool = AtomicBool::new(false);

/// Per-query spill state, returned by `per_query_spill_budget`.
///
/// Three states make the call site unambiguous:
/// * `Disabled`     — spill is off; parallelism MUST NOT be clamped (no spill = no risk).
/// * `Critical`     — spill is on but available disk is dangerously low; clamp to 1.
/// * `Available(n)` — spill is on and disk is healthy; full parallelism + per-query budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillBudget {
    Disabled,
    Critical,
    Available(u64),
}

/// Set the spill directory and mark spill enabled (called once from create_global_runtime
/// when DataFusion is built with `DiskManagerMode::Directories`).
pub fn set_spill_dir(path: &str) {
    let _ = SPILL_DIR.set(path.to_string());
    SPILL_ENABLED.store(true, Ordering::Release);
}

/// Mark spill explicitly disabled (called once from create_global_runtime when
/// DataFusion is built with `DiskManagerMode::Disabled`). This makes the disabled
/// state explicit so `per_query_spill_budget` returns `Disabled` instead of
/// returning a phantom "disk pressure" signal driven by an unset `SPILL_DIR`.
pub fn mark_spill_disabled() {
    SPILL_ENABLED.store(false, Ordering::Release);
}

/// Returns the per-query spill budget based on available disk space.
///
/// Formula: `10% of available_disk` for the `Available` case.
///
/// Returns:
/// * `Disabled`     when spill is off — no `statvfs` call, no clamp.
/// * `Critical`     when spill is on but the spill volume is dangerously low
///                  (< 64MB after the fraction, or `statvfs` failed). Caller clamps to 1.
/// * `Available(n)` when spill is on and disk is healthy.
///
/// Cost: one `statvfs` syscall (~1µs) only when spill is enabled. Called once per
/// query at admission.
pub fn per_query_spill_budget() -> SpillBudget {
    if !SPILL_ENABLED.load(Ordering::Acquire) {
        return SpillBudget::Disabled;
    }
    // SPILL_ENABLED is only set to true by `set_spill_dir`, which always populates
    // SPILL_DIR first — but a defensive `match` keeps this safe even if call ordering
    // ever changes.
    let spill_dir = match SPILL_DIR.get() {
        Some(d) => d,
        None => return SpillBudget::Critical,
    };
    let available = match available_disk_space(spill_dir) {
        Some(a) => a,
        None => return SpillBudget::Critical,
    };

    let fraction_x1000 = DISK_FRACTION_X1000.load(Ordering::Acquire);
    let budget = available * fraction_x1000 / 1000;

    if budget < 64 * 1024 * 1024 {
        // 64MB minimum viable spill
        log::warn!(
            "[disk-pressure] Spill budget too low: {} MB (available={} MB)",
            budget / (1024 * 1024),
            available / (1024 * 1024),
        );
        return SpillBudget::Critical;
    }
    SpillBudget::Available(budget)
}

/// Query available disk space for the given path.
#[cfg(unix)]
pub fn available_disk_space(path: &str) -> Option<u64> {
    use std::ffi::CString;
    use std::mem::MaybeUninit;

    let c_path = CString::new(path).ok()?;
    let mut stat = MaybeUninit::<libc::statvfs>::uninit();
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if ret != 0 {
        return None;
    }
    let stat = unsafe { stat.assume_init() };
    Some(stat.f_bavail as u64 * stat.f_frsize as u64)
}

#[cfg(not(unix))]
pub fn available_disk_space(_path: &str) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_thresholds() {
        let t = MemoryThresholds::default();
        assert!((t.admission_throttle - 0.75).abs() < 0.001);
        assert!((t.execution_spill - 0.85).abs() < 0.001);
        assert!((t.execution_critical - 0.95).abs() < 0.001);
    }

    #[test]
    fn set_and_get_thresholds() {
        set_thresholds(MemoryThresholds {
            admission_throttle: 0.60,
            admission_reject: 0.80,
            execution_spill: 0.90,
            execution_critical: 0.97,
        });
        let t = get_thresholds();
        assert!((t.admission_throttle - 0.60).abs() < 0.001);
        assert!((t.execution_spill - 0.90).abs() < 0.001);
        assert!((t.execution_critical - 0.97).abs() < 0.001);
        // Restore defaults
        set_thresholds(MemoryThresholds::default());
    }

    #[test]
    fn set_and_get_spill_exempt_cap() {
        // Default is 512MB.
        assert_eq!(spill_exempt_cap_bytes(), 512 * 1024 * 1024);
        // Round-trips an arbitrary value.
        set_spill_exempt_cap_bytes(64 * 1024 * 1024);
        assert_eq!(spill_exempt_cap_bytes(), 64 * 1024 * 1024);
        // Zero is valid (disables the exemption budget).
        set_spill_exempt_cap_bytes(0);
        assert_eq!(spill_exempt_cap_bytes(), 0);
        // Restore default.
        set_spill_exempt_cap_bytes(512 * 1024 * 1024);
    }

    #[test]
    fn ffi_spill_exempt_cap_clamps_negative_to_zero() {
        // The FFI export takes a signed i64 (Java long); negative inputs must clamp
        // to 0 rather than wrap to a huge u64.
        crate::ffm::df_set_spill_exempt_cap_bytes(-1);
        assert_eq!(spill_exempt_cap_bytes(), 0);
        // A positive value passes through unchanged.
        crate::ffm::df_set_spill_exempt_cap_bytes(256 * 1024 * 1024);
        assert_eq!(spill_exempt_cap_bytes(), 256 * 1024 * 1024);
        // Restore default.
        set_spill_exempt_cap_bytes(512 * 1024 * 1024);
    }

    #[test]
    fn skip_for_small_pools() {
        // Pool below 16MB → always returns false (no override)
        assert!(!should_override(1_000_000, OverrideContext::Admission));
        assert!(!should_override(1_000_000, OverrideContext::Execution));
    }

    #[test]
    fn should_override_uses_resident_not_allocated() {
        // With a large pool (1TB), resident will always be below threshold
        // so override should fire (resident < threshold = "headroom available")
        let large_pool = 1024 * 1024 * 1024 * 1024; // 1TB
        let resident = cached_resident_bytes();
        if resident <= 0 {
            return; // jemalloc not active in this test env (CI)
        }
        let result = should_override(large_pool, OverrideContext::Execution);
        assert!(
            result,
            "With 1TB pool limit, resident should be well below threshold — override should fire"
        );
    }

    #[test]
    fn is_memory_pressured_false_for_large_pool() {
        // With a 1TB pool, current process RSS is far below 70% → not pressured
        let large_pool = 1024 * 1024 * 1024 * 1024; // 1TB
        assert!(!is_memory_pressured(large_pool));
    }

    #[test]
    fn is_memory_pressured_true_when_rss_exceeds_limit() {
        // Set pool limit to something well below current process RSS.
        // A Rust test process typically uses 50-200MB RSS, so a 20MB limit
        // should always be exceeded.
        let small_pool = 20 * 1024 * 1024; // 20MB — above MIN_POOL_FOR_OVERRIDE
        let resident = native_bridge_common::allocator::resident_bytes();
        if resident <= 0 {
            return; // jemalloc not available
        }
        // Only assert if RSS is actually above 70% of 20MB = 14MB (which it will be)
        if resident as usize > small_pool * 70 / 100 {
            assert!(is_memory_pressured(small_pool));
        }
    }

    #[test]
    fn is_memory_pressured_skips_small_pools() {
        assert!(!is_memory_pressured(1_000_000)); // 1MB — below MIN_POOL_FOR_OVERRIDE
    }

    #[test]
    fn cached_resident_bytes_returns_non_negative() {
        // Returns > 0 when jemalloc is active, 0 when not (CI may not link jemalloc)
        let resident = cached_resident_bytes();
        assert!(
            resident >= 0,
            "cached_resident_bytes() should never return negative, got {}",
            resident
        );
    }

    #[test]
    fn cached_resident_bytes_is_stable_within_interval() {
        // Two calls within <100ms should return the same cached value
        // (only one thread per interval refreshes the cache).
        let first = cached_resident_bytes();
        let second = cached_resident_bytes();
        assert_eq!(
            first, second,
            "Two immediate calls should return the same cached value"
        );
    }

    #[test]
    fn should_cancel_query_false_for_small_pools() {
        // Pools below MIN_POOL_FOR_OVERRIDE (16MB) always return false
        assert!(!should_cancel_query(1_000_000)); // 1MB
        assert!(!should_cancel_query(8 * 1024 * 1024)); // 8MB
        assert!(!should_cancel_query(15 * 1024 * 1024)); // 15MB
    }

    #[test]
    fn should_cancel_query_true_when_rss_exceeds_limit() {
        // With a 20MB pool limit (above MIN_POOL_FOR_OVERRIDE), the current test
        // process RSS should exceed 95% of 20MB = 19MB. A Rust test process
        // typically uses 50-200MB RSS.
        let small_pool = 20 * 1024 * 1024; // 20MB
        let resident = native_bridge_common::allocator::resident_bytes();
        if resident <= 0 {
            return; // jemalloc not available in this test env
        }
        // Only assert if RSS actually exceeds the critical threshold
        let critical_bytes = (small_pool as f64 * 0.95) as i64;
        if resident >= critical_bytes {
            assert!(
                should_cancel_query(small_pool),
                "should_cancel_query should return true when RSS ({}) exceeds 95% of pool ({})",
                resident,
                small_pool
            );
        }
    }

    #[test]
    fn override_respects_spill_vs_admission_threshold() {
        // Operator threshold (85%) is more permissive than admission (75%).
        // For a pool where RSS is between 70% and 85%:
        // - Admission override should NOT fire (RSS >= 70% threshold)
        // - Operator override SHOULD fire (RSS < 85% threshold)
        //
        // We can't precisely control RSS in a unit test, but we can verify
        // that the thresholds are read correctly by setting them and checking
        // behavior with known pool sizes.
        let resident = native_bridge_common::allocator::resident_bytes();
        if resident <= 0 {
            return; // jemalloc not available in this test env
        }
        let resident = resident as usize;

        // Set pool limit so that resident is exactly between 70% and 85%
        // pool = resident / 0.77 (midpoint) → resident/pool ≈ 77%
        let pool_at_midpoint = (resident as f64 / 0.77) as usize;
        if pool_at_midpoint < MIN_POOL_FOR_OVERRIDE {
            return;
        }

        // At 77% utilization: admission (75%) should NOT override, operator (85%) SHOULD override
        let admission_result = should_override(pool_at_midpoint, OverrideContext::Admission);
        let spill_result = should_override(pool_at_midpoint, OverrideContext::Execution);

        // admission: resident (77%) >= threshold (70%) → NOT below → override = false
        assert!(
            !admission_result,
            "At 77% RSS, admission override should NOT fire (threshold 70%)"
        );
        // operator: resident (77%) < threshold (85%) → below → override = true
        assert!(
            spill_result,
            "At 77% RSS, spill override SHOULD fire (threshold 85%)"
        );
    }
}
