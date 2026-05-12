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
