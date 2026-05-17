/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Native node stats counters for task cancellation metrics.
//!
//! These counters track native search tasks and native search shard tasks that
//! continue executing after cancellation. They are completely independent of
//! `DfStatsBuffer` and `NODE_STATS_CACHE` used for plugin stats.
//!
//! All operations use `Ordering::Relaxed` since cross-counter ordering is not
//! required — each counter is independently meaningful.

use std::sync::atomic::{AtomicI64, Ordering};

// ---------------------------------------------------------------------------
// 4 independent atomic counters — no struct, no lock
// ---------------------------------------------------------------------------

static NATIVE_SEARCH_TASK_CURRENT: AtomicI64 = AtomicI64::new(0);
static NATIVE_SEARCH_TASK_TOTAL: AtomicI64 = AtomicI64::new(0);
static NATIVE_SEARCH_SHARD_TASK_CURRENT: AtomicI64 = AtomicI64::new(0);
static NATIVE_SEARCH_SHARD_TASK_TOTAL: AtomicI64 = AtomicI64::new(0);

// ---------------------------------------------------------------------------
// Public increment/decrement functions for producers
// ---------------------------------------------------------------------------

/// Increments the current count of native search tasks executing post-cancellation.
pub fn inc_native_search_task_current() {
    NATIVE_SEARCH_TASK_CURRENT.fetch_add(1, Ordering::Relaxed);
}

/// Decrements the current count of native search tasks executing post-cancellation.
pub fn dec_native_search_task_current() {
    NATIVE_SEARCH_TASK_CURRENT.fetch_sub(1, Ordering::Relaxed);
}

/// Increments the total count of native search tasks that have executed post-cancellation.
pub fn inc_native_search_task_total() {
    NATIVE_SEARCH_TASK_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Increments the current count of native search shard tasks executing post-cancellation.
pub fn inc_native_search_shard_task_current() {
    NATIVE_SEARCH_SHARD_TASK_CURRENT.fetch_add(1, Ordering::Relaxed);
}

/// Decrements the current count of native search shard tasks executing post-cancellation.
pub fn dec_native_search_shard_task_current() {
    NATIVE_SEARCH_SHARD_TASK_CURRENT.fetch_sub(1, Ordering::Relaxed);
}

/// Increments the total count of native search shard tasks that have executed post-cancellation.
pub fn inc_native_search_shard_task_total() {
    NATIVE_SEARCH_SHARD_TASK_TOTAL.fetch_add(1, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// FFM entry point
// ---------------------------------------------------------------------------

/// Reads 4 AtomicI64 counters and writes them as `[i64; 4]` to the caller buffer.
/// Returns 0 on success, -1 if buffer too small.
///
/// Buffer layout (32 bytes):
///   [0..8)   native_search_task_current
///   [8..16)  native_search_task_total
///   [16..24) native_search_shard_task_current
///   [24..32) native_search_shard_task_total
///
/// # Safety
/// `out_ptr` must point to a valid buffer of at least `out_cap` bytes.
#[no_mangle]
pub unsafe extern "C" fn df_native_node_stats(out_ptr: *mut u8, out_cap: i64) -> i64 {
    if (out_cap as usize) < 32 {
        return -1;
    }
    let vals: [i64; 4] = [
        NATIVE_SEARCH_TASK_CURRENT.load(Ordering::Relaxed),
        NATIVE_SEARCH_TASK_TOTAL.load(Ordering::Relaxed),
        NATIVE_SEARCH_SHARD_TASK_CURRENT.load(Ordering::Relaxed),
        NATIVE_SEARCH_SHARD_TASK_TOTAL.load(Ordering::Relaxed),
    ];
    std::ptr::copy_nonoverlapping(vals.as_ptr() as *const u8, out_ptr, 32);
    0
}

// ---------------------------------------------------------------------------
// Test helpers (for resetting counters in tests)
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) fn reset_all_counters() {
    NATIVE_SEARCH_TASK_CURRENT.store(0, Ordering::Relaxed);
    NATIVE_SEARCH_TASK_TOTAL.store(0, Ordering::Relaxed);
    NATIVE_SEARCH_SHARD_TASK_CURRENT.store(0, Ordering::Relaxed);
    NATIVE_SEARCH_SHARD_TASK_TOTAL.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Reset counters before each test to avoid cross-test interference.
    fn setup() {
        reset_all_counters();
    }

    #[test]
    fn test_counters_initialized_to_zero() {
        setup();
        assert_eq!(NATIVE_SEARCH_TASK_CURRENT.load(Ordering::Relaxed), 0);
        assert_eq!(NATIVE_SEARCH_TASK_TOTAL.load(Ordering::Relaxed), 0);
        assert_eq!(NATIVE_SEARCH_SHARD_TASK_CURRENT.load(Ordering::Relaxed), 0);
        assert_eq!(NATIVE_SEARCH_SHARD_TASK_TOTAL.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_inc_dec_native_search_task_current() {
        setup();
        inc_native_search_task_current();
        inc_native_search_task_current();
        assert_eq!(NATIVE_SEARCH_TASK_CURRENT.load(Ordering::Relaxed), 2);
        dec_native_search_task_current();
        assert_eq!(NATIVE_SEARCH_TASK_CURRENT.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_inc_native_search_task_total() {
        setup();
        inc_native_search_task_total();
        inc_native_search_task_total();
        inc_native_search_task_total();
        assert_eq!(NATIVE_SEARCH_TASK_TOTAL.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_inc_dec_native_search_shard_task_current() {
        setup();
        inc_native_search_shard_task_current();
        inc_native_search_shard_task_current();
        inc_native_search_shard_task_current();
        assert_eq!(NATIVE_SEARCH_SHARD_TASK_CURRENT.load(Ordering::Relaxed), 3);
        dec_native_search_shard_task_current();
        dec_native_search_shard_task_current();
        assert_eq!(NATIVE_SEARCH_SHARD_TASK_CURRENT.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_inc_native_search_shard_task_total() {
        setup();
        inc_native_search_shard_task_total();
        assert_eq!(NATIVE_SEARCH_SHARD_TASK_TOTAL.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_df_native_node_stats_reads_correct_values() {
        setup();
        // Set up known state
        inc_native_search_task_current();
        inc_native_search_task_current();
        inc_native_search_task_total();
        inc_native_search_task_total();
        inc_native_search_task_total();
        inc_native_search_shard_task_current();
        inc_native_search_shard_task_total();
        inc_native_search_shard_task_total();

        let mut buf = [0u8; 32];
        let rc = unsafe { df_native_node_stats(buf.as_mut_ptr(), 32) };
        assert_eq!(rc, 0);

        // Decode the buffer as [i64; 4]
        let vals: [i64; 4] = unsafe { std::ptr::read(buf.as_ptr() as *const [i64; 4]) };
        assert_eq!(vals[0], 2); // native_search_task_current
        assert_eq!(vals[1], 3); // native_search_task_total
        assert_eq!(vals[2], 1); // native_search_shard_task_current
        assert_eq!(vals[3], 2); // native_search_shard_task_total
    }

    #[test]
    fn test_df_native_node_stats_rejects_small_buffer() {
        let mut buf = [0u8; 16];
        let rc = unsafe { df_native_node_stats(buf.as_mut_ptr(), 16) };
        assert_eq!(rc, -1);
    }

    #[test]
    fn test_df_native_node_stats_rejects_zero_capacity() {
        let mut buf = [0u8; 32];
        let rc = unsafe { df_native_node_stats(buf.as_mut_ptr(), 0) };
        assert_eq!(rc, -1);
    }

    #[test]
    fn test_df_native_node_stats_accepts_larger_buffer() {
        setup();
        let mut buf = [0u8; 64];
        let rc = unsafe { df_native_node_stats(buf.as_mut_ptr(), 64) };
        assert_eq!(rc, 0);
    }

    #[test]
    fn test_counters_are_independent() {
        setup();
        // Increment only one counter and verify others remain zero
        inc_native_search_task_current();
        assert_eq!(NATIVE_SEARCH_TASK_CURRENT.load(Ordering::Relaxed), 1);
        assert_eq!(NATIVE_SEARCH_TASK_TOTAL.load(Ordering::Relaxed), 0);
        assert_eq!(NATIVE_SEARCH_SHARD_TASK_CURRENT.load(Ordering::Relaxed), 0);
        assert_eq!(NATIVE_SEARCH_SHARD_TASK_TOTAL.load(Ordering::Relaxed), 0);
    }

    // -----------------------------------------------------------------------
    // Property-based test: AtomicI64 increment/decrement correctness
    // -----------------------------------------------------------------------

    /// **Validates: Requirements 1.1, 2.1–2.6**
    ///
    /// Property 1: AtomicI64 increment/decrement correctness
    ///
    /// For any random number of increments and decrements applied to each of
    /// the 4 counters, reading the counters via `df_native_node_stats` SHALL
    /// return values equal to the algebraic sum (increments - decrements) for
    /// each counter.
    proptest! {
        #[test]
        fn prop_atomic_inc_dec_correctness(
            search_task_current_incs in 0u32..100,
            search_task_current_decs in 0u32..100,
            search_task_total_incs in 0u32..100,
            shard_task_current_incs in 0u32..100,
            shard_task_current_decs in 0u32..100,
            shard_task_total_incs in 0u32..100,
        ) {
            // Reset counters before each iteration
            reset_all_counters();

            // Apply increments and decrements for NATIVE_SEARCH_TASK_CURRENT
            for _ in 0..search_task_current_incs {
                inc_native_search_task_current();
            }
            for _ in 0..search_task_current_decs {
                dec_native_search_task_current();
            }

            // Apply only increments for NATIVE_SEARCH_TASK_TOTAL (no dec function exists)
            for _ in 0..search_task_total_incs {
                inc_native_search_task_total();
            }

            // Apply increments and decrements for NATIVE_SEARCH_SHARD_TASK_CURRENT
            for _ in 0..shard_task_current_incs {
                inc_native_search_shard_task_current();
            }
            for _ in 0..shard_task_current_decs {
                dec_native_search_shard_task_current();
            }

            // Apply only increments for NATIVE_SEARCH_SHARD_TASK_TOTAL (no dec function exists)
            for _ in 0..shard_task_total_incs {
                inc_native_search_shard_task_total();
            }

            // Expected algebraic sums
            let expected_search_task_current =
                search_task_current_incs as i64 - search_task_current_decs as i64;
            let expected_search_task_total = search_task_total_incs as i64;
            let expected_shard_task_current =
                shard_task_current_incs as i64 - shard_task_current_decs as i64;
            let expected_shard_task_total = shard_task_total_incs as i64;

            // Read counters via df_native_node_stats
            let mut buf = [0u8; 32];
            let rc = unsafe { df_native_node_stats(buf.as_mut_ptr(), 32) };
            prop_assert_eq!(rc, 0);

            // Decode the buffer as [i64; 4]
            let vals: [i64; 4] = unsafe { std::ptr::read(buf.as_ptr() as *const [i64; 4]) };

            prop_assert_eq!(
                vals[0], expected_search_task_current,
                "native_search_task_current mismatch: incs={}, decs={}",
                search_task_current_incs, search_task_current_decs
            );
            prop_assert_eq!(
                vals[1], expected_search_task_total,
                "native_search_task_total mismatch: incs={}",
                search_task_total_incs
            );
            prop_assert_eq!(
                vals[2], expected_shard_task_current,
                "native_search_shard_task_current mismatch: incs={}, decs={}",
                shard_task_current_incs, shard_task_current_decs
            );
            prop_assert_eq!(
                vals[3], expected_shard_task_total,
                "native_search_shard_task_total mismatch: incs={}",
                shard_task_total_incs
            );
        }
    }
}
