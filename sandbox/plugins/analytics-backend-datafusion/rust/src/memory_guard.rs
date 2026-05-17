/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unified jemalloc-based memory guard for pool override decisions.
//!
//! Provides a single entry point (`should_override`) that both the admission
//! layer (`query_budget.rs`) and the operator layer (`memory.rs`) call before
//! reducing partitions or triggering spill respectively.
//!
//! Thresholds are configurable at runtime via `set_thresholds`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Minimum pool size (bytes) for jemalloc override to activate.
/// Below this, the pool is assumed to be a unit test / benchmark with
/// artificial limits — never override.
const MIN_POOL_FOR_OVERRIDE: usize = 16 * 1024 * 1024; // 16MB

// Configurable thresholds stored as fixed-point (×1000) in atomics.
// Defaults: admission=70%, operator=85%.
static ADMISSION_THRESHOLD_X1000: AtomicU64 = AtomicU64::new(700);
static OPERATOR_THRESHOLD_X1000: AtomicU64 = AtomicU64::new(850);

/// Which layer is asking for the override check.
#[derive(Debug, Clone, Copy)]
pub enum OverrideContext {
    /// Admission-time: deciding whether to reduce target_partitions.
    /// More conservative (lower threshold) — committing to resource usage.
    Admission,
    /// Operator try_grow: deciding whether to trigger spill.
    /// More aggressive (higher threshold) — avoiding expensive disk I/O.
    Operator,
}

/// Configurable memory thresholds for jemalloc override decisions.
///
/// Both values are fractions (0.0–1.0) of the pool limit:
/// - If `jemalloc_allocated < threshold × pool_limit`, the pool's rejection
///   is considered a false positive and the operation proceeds.
#[derive(Debug, Clone, Copy)]
pub struct MemoryThresholds {
    /// Threshold for admission decisions (reduce partitions). Default: 0.70
    pub admission: f64,
    /// Threshold for operator decisions (trigger spill). Default: 0.85
    pub operator: f64,
}

impl Default for MemoryThresholds {
    fn default() -> Self {
        Self {
            admission: 0.70,
            operator: 0.85,
        }
    }
}

/// Set the memory thresholds at runtime. Called from Java when cluster
/// settings change. Thread-safe (atomic stores).
pub fn set_thresholds(thresholds: MemoryThresholds) {
    ADMISSION_THRESHOLD_X1000.store(
        (thresholds.admission * 1000.0) as u64,
        Ordering::Release,
    );
    OPERATOR_THRESHOLD_X1000.store(
        (thresholds.operator * 1000.0) as u64,
        Ordering::Release,
    );
}

/// Read current thresholds.
pub fn get_thresholds() -> MemoryThresholds {
    MemoryThresholds {
        admission: ADMISSION_THRESHOLD_X1000.load(Ordering::Acquire) as f64 / 1000.0,
        operator: OPERATOR_THRESHOLD_X1000.load(Ordering::Acquire) as f64 / 1000.0,
    }
}

/// Check whether jemalloc says physical memory has headroom, meaning the
/// pool's rejection is a false positive (stale accounting).
///
/// Returns `true` if the override should fire (proceed despite pool rejection).
/// Returns `false` if pressure is real or stats are unavailable.
///
/// # Arguments
/// - `pool_limit_bytes`: the pool's configured limit
/// - `context`: which layer is asking (determines threshold)
pub fn should_override(pool_limit_bytes: usize, context: OverrideContext) -> bool {
    // Skip for tiny pools (unit tests, benchmarks with artificial limits)
    if pool_limit_bytes < MIN_POOL_FOR_OVERRIDE {
        return false;
    }

    let allocated = native_bridge_common::allocator::allocated_bytes();
    if allocated <= 0 {
        return false;
    }

    let threshold_x1000 = match context {
        OverrideContext::Admission => ADMISSION_THRESHOLD_X1000.load(Ordering::Acquire),
        OverrideContext::Operator => OPERATOR_THRESHOLD_X1000.load(Ordering::Acquire),
    };

    let threshold_bytes = (pool_limit_bytes as u64 * threshold_x1000 / 1000) as i64;
    allocated < threshold_bytes
}

// ---------------------------------------------------------------------------
// Disk spill budget: min(10% of available, hard_cap) with global tracking
// ---------------------------------------------------------------------------

/// Configurable disk spill limits, stored in atomics for runtime changes.
static DISK_FRACTION_X1000: AtomicU64 = AtomicU64::new(100); // 10% = 100/1000
static DISK_HARD_CAP_BYTES: AtomicU64 = AtomicU64::new(10 * 1024 * 1024 * 1024); // 10GB

/// Tracks total spill bytes currently in use across all queries.
/// Incremented when a query starts spilling, decremented when it completes.
static TOTAL_SPILL_USED: AtomicU64 = AtomicU64::new(0);

/// Stored spill directory path. Set once at runtime creation.
static SPILL_DIR: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Configurable spill disk limits.
#[derive(Debug, Clone, Copy)]
pub struct DiskSpillLimits {
    /// Fraction of available disk space (0.0–1.0). Default: 0.10 (10%)
    pub fraction: f64,
    /// Absolute hard cap in bytes. Default: 10GB.
    /// Per-query budget = min(fraction × available, hard_cap)
    /// Total across all queries also capped at hard_cap.
    pub hard_cap_bytes: u64,
}

impl Default for DiskSpillLimits {
    fn default() -> Self {
        Self {
            fraction: 0.10,
            hard_cap_bytes: 10 * 1024 * 1024 * 1024, // 10GB
        }
    }
}

/// Set disk spill limits at runtime. Thread-safe.
pub fn set_disk_spill_limits(limits: DiskSpillLimits) {
    DISK_FRACTION_X1000.store((limits.fraction * 1000.0) as u64, Ordering::Release);
    DISK_HARD_CAP_BYTES.store(limits.hard_cap_bytes, Ordering::Release);
}

/// Read current disk spill limits.
pub fn get_disk_spill_limits() -> DiskSpillLimits {
    DiskSpillLimits {
        fraction: DISK_FRACTION_X1000.load(Ordering::Acquire) as f64 / 1000.0,
        hard_cap_bytes: DISK_HARD_CAP_BYTES.load(Ordering::Acquire),
    }
}

/// Set the spill directory (called once from create_global_runtime).
pub fn set_spill_dir(path: &str) {
    let _ = SPILL_DIR.set(path.to_string());
}

/// Returns the per-query spill budget.
///
/// Formula: `min(10% of available_disk, hard_cap - total_already_used)`
///
/// Ensures:
/// - No single query can use more than 10% of available disk
/// - Total spill across all concurrent queries never exceeds hard_cap (10GB)
/// - Returns None if no budget available (disk critically low or cap reached)
///
/// Cost: one `statvfs` syscall (~1µs). Called once per query at admission.
pub fn per_query_spill_budget() -> Option<u64> {
    let spill_dir = SPILL_DIR.get()?;
    let available = available_disk_space(spill_dir)?;

    let fraction_x1000 = DISK_FRACTION_X1000.load(Ordering::Acquire);
    let hard_cap = DISK_HARD_CAP_BYTES.load(Ordering::Acquire);
    let total_used = TOTAL_SPILL_USED.load(Ordering::Relaxed);

    // Per-query: min(fraction × available, remaining_under_cap)
    let fraction_budget = available * fraction_x1000 / 1000;
    let remaining_cap = hard_cap.saturating_sub(total_used);
    let budget = fraction_budget.min(remaining_cap);

    if budget < 64 * 1024 * 1024 { // 64MB minimum viable spill
        log::warn!(
            "[disk-pressure] Spill budget too low: {} MB (available={} MB, used={} MB, cap={} MB)",
            budget / (1024 * 1024),
            available / (1024 * 1024),
            total_used / (1024 * 1024),
            hard_cap / (1024 * 1024),
        );
        return None;
    }
    Some(budget)
}

/// Reserve spill bytes for a query. Called at query start.
/// Returns the reserved amount (may be less than requested if cap is close).
pub fn reserve_spill_budget(bytes: u64) -> u64 {
    let hard_cap = DISK_HARD_CAP_BYTES.load(Ordering::Acquire);
    loop {
        let current = TOTAL_SPILL_USED.load(Ordering::Relaxed);
        let remaining = hard_cap.saturating_sub(current);
        let actual = bytes.min(remaining);
        if actual == 0 {
            return 0;
        }
        if TOTAL_SPILL_USED
            .compare_exchange(current, current + actual, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return actual;
        }
    }
}

/// Release spill bytes when a query completes (spill files cleaned up).
pub fn release_spill_budget(bytes: u64) {
    TOTAL_SPILL_USED.fetch_sub(bytes, Ordering::Relaxed);
}

/// Current total spill usage across all queries.
pub fn total_spill_used() -> u64 {
    TOTAL_SPILL_USED.load(Ordering::Relaxed)
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
        assert!((t.admission - 0.70).abs() < 0.001);
        assert!((t.operator - 0.85).abs() < 0.001);
    }

    #[test]
    fn set_and_get_thresholds() {
        set_thresholds(MemoryThresholds {
            admission: 0.60,
            operator: 0.90,
        });
        let t = get_thresholds();
        assert!((t.admission - 0.60).abs() < 0.001);
        assert!((t.operator - 0.90).abs() < 0.001);
        // Restore defaults
        set_thresholds(MemoryThresholds::default());
    }

    #[test]
    fn skip_for_small_pools() {
        // Pool below 16MB → always returns false (no override)
        assert!(!should_override(1_000_000, OverrideContext::Admission));
        assert!(!should_override(1_000_000, OverrideContext::Operator));
    }
}
