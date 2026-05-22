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
// Total counters — incremented by QueryTrackingContext::drop() when a
// cancelled query ran past the threshold before completing.
// ---------------------------------------------------------------------------

pub(crate) static NATIVE_SEARCH_TASK_TOTAL: AtomicI64 = AtomicI64::new(0);
pub(crate) static NATIVE_SEARCH_SHARD_TASK_TOTAL: AtomicI64 = AtomicI64::new(0);

// ---------------------------------------------------------------------------
// Public increment functions for producers (called from query_tracker Drop)
// ---------------------------------------------------------------------------

/// Increments the total count of native search tasks that executed post-cancellation
/// beyond the threshold duration.
pub fn inc_native_search_task_total() {
    NATIVE_SEARCH_TASK_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Increments the total count of native search shard tasks that executed post-cancellation
/// beyond the threshold duration.
pub fn inc_native_search_shard_task_total() {
    NATIVE_SEARCH_SHARD_TASK_TOTAL.fetch_add(1, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// FFM entry point
// ---------------------------------------------------------------------------

/// Reads task cancellation stats and writes them as `[i64; 4]` to the caller buffer.
/// `current` counts are computed by scanning the live query registry.
/// `total` counts come from the atomic counters (incremented on query drop).
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
    use crate::query_tracker;

    if (out_cap as usize) < 32 {
        return -1;
    }
    // Read totals FIRST, then scan registry. This ordering prevents double-counting:
    // if a query drops between our total read and scan, it increments total (which
    // we already captured at the lower value) and leaves the registry (so the scan
    // won't find it). Worst case: transient undercount by 1, self-corrects next read.
    // The reverse order (scan first, then total) would allow a query to appear in
    // both the scan result AND the incremented total.
    let coordinator_total = NATIVE_SEARCH_TASK_TOTAL.load(Ordering::Relaxed);
    let shard_total = NATIVE_SEARCH_SHARD_TASK_TOTAL.load(Ordering::Relaxed);
    let (shard_current, coordinator_current) =
        query_tracker::count_cancelled_running(query_tracker::cancel_stats_threshold());
    let vals: [i64; 4] = [
        coordinator_current,
        coordinator_total + coordinator_current,
        shard_current,
        shard_total + shard_current,
    ];
    std::ptr::copy_nonoverlapping(vals.as_ptr() as *const u8, out_ptr, 32);
    0
}

// ---------------------------------------------------------------------------
// Test helpers (for resetting counters in tests)
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) fn reset_all_counters() {
    NATIVE_SEARCH_TASK_TOTAL.store(0, Ordering::Relaxed);
    NATIVE_SEARCH_SHARD_TASK_TOTAL.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() {
        reset_all_counters();
    }

    #[test]
    fn test_total_counters_initialized_to_zero() {
        setup();
        assert_eq!(NATIVE_SEARCH_TASK_TOTAL.load(Ordering::Relaxed), 0);
        assert_eq!(NATIVE_SEARCH_SHARD_TASK_TOTAL.load(Ordering::Relaxed), 0);
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
    fn test_inc_native_search_shard_task_total() {
        setup();
        inc_native_search_shard_task_total();
        inc_native_search_shard_task_total();
        assert_eq!(NATIVE_SEARCH_SHARD_TASK_TOTAL.load(Ordering::Relaxed), 2);
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
    fn test_df_native_node_stats_total_counters() {
        // Read baseline (other tests may run in parallel and share globals)
        let mut buf = [0u8; 32];
        let rc = unsafe { df_native_node_stats(buf.as_mut_ptr(), 32) };
        assert_eq!(rc, 0);
        let baseline: [i64; 4] = unsafe { std::ptr::read(buf.as_ptr() as *const [i64; 4]) };

        inc_native_search_task_total();
        inc_native_search_task_total();
        inc_native_search_shard_task_total();

        let rc = unsafe { df_native_node_stats(buf.as_mut_ptr(), 32) };
        assert_eq!(rc, 0);
        let vals: [i64; 4] = unsafe { std::ptr::read(buf.as_ptr() as *const [i64; 4]) };

        // vals[1] = coordinator total — should be baseline + 2
        assert_eq!(vals[1], baseline[1] + 2);
        // vals[3] = shard total — should be baseline + 1
        assert_eq!(vals[3], baseline[3] + 1);
    }
}
