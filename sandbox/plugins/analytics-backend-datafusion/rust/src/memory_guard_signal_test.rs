/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Tests for the signal [`crate::memory::DynamicLimitPool::try_grow`]'s pre-CAS guard
//! compares against its threshold.
//!
//! The guard exists because the pool's own accounting undercounts what its work really
//! cost, so it consults jemalloc instead. It used to read `stats.resident`, which counts
//! freed-but-retained dirty pages alongside live ones. jemalloc hands those pages straight
//! to the next allocation, so the guard refused requests on the strength of the very
//! memory that would have satisfied them.
//!
//! Measured on a single-node cluster with a 16 GiB pool: while idle, `stats.resident` read
//! 12.76 GB against 82 MB of live bytes — 99.4 % of the signal was reusable memory. An
//! identical `stats dc(WatchID) as c by URL | sort - c | head 5` then succeeded and the
//! immediately following execution was refused, 858 KB requested against a
//! 17,179,869,184-byte limit while the pool held nothing reserved.
//!
//! The guard now reads `stats.active` + `stats.metadata` — pages backing live allocations
//! plus jemalloc's own bookkeeping, excluding only the dirty pages. Metadata is included
//! because it is live and unreclaimable, so omitting it would under-report real pressure.
//! Same cluster and query after the change: six consecutive executions succeeded and the
//! trip counter never moved.

use crate::memory::DynamicLimitPool;
use crate::memory_guard::{self, test_support::ScopedGuardState};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use native_bridge_common::allocator::{allocated_bytes, resident_bytes, unreclaimable_bytes};
use std::sync::Arc;

/// The pool limit the defect was measured against: `datafusion.memory_pool_limit_bytes`
/// at 16 GiB.
const POOL_LIMIT_BYTES: usize = 17_179_869_184;

/// A request small enough that refusing it can only be a guard decision, never genuine
/// exhaustion. Taken from a recorded refusal.
const SMALL_REQUEST_BYTES: usize = 346_224;

/// Defaults for `datafusion.memory_guard.execution.{spill,critical}_threshold`.
const SPILL_THRESHOLD: f64 = 0.85;
const CRITICAL_THRESHOLD: f64 = 0.95;

fn fraction_of_pool_limit(fraction: f64) -> i64 {
    (POOL_LIMIT_BYTES as f64 * fraction) as i64
}

fn pool_with_limit(limit: usize) -> (Arc<dyn MemoryPool>, crate::memory::DynamicLimitHandle) {
    let (pool, handle) = DynamicLimitPool::new(limit);
    (Arc::new(pool), handle)
}

/// Every assertion in this file that reads a real jemalloc statistic depends on the test
/// binary allocating through jemalloc, which needs a `#[global_allocator]` in the harness.
/// Without one, jemalloc is linked but unused: a touched 64 MiB `Vec` moved
/// `allocated_bytes()` by zero and `resident_bytes()` reported 4.6 MB of arena metadata,
/// which silently disabled the pre-existing guard tests rather than failing them. This
/// keeps the property measured instead of assumed.
#[test]
fn jemalloc_is_installed_as_the_test_harness_allocator() {
    let before = allocated_bytes();

    // Touched so the pages are faulted in and the allocation cannot be elided.
    let mut ballast: Vec<u8> = vec![0u8; 64 * 1024 * 1024];
    for i in (0..ballast.len()).step_by(4096) {
        ballast[i] = 1;
    }
    std::hint::black_box(&ballast);

    let delta = allocated_bytes() - before;
    drop(ballast);

    // Floored at half the ballast: `allocated_bytes()` is process-wide, so a concurrently
    // finishing test freeing memory shaves the net delta. The question here is binary —
    // jemalloc either sees the allocation or reports zero — so a loose floor costs nothing.
    assert!(
        delta >= 32 * 1024 * 1024,
        "jemalloc did not observe a 64 MiB allocation (net delta {delta}); the harness is \
         not allocating through jemalloc, so every statistic read in this file is vacuous"
    );
}

/// The fix: the guard's signal excludes pages that were freed but not yet returned to the
/// OS, while still counting allocator metadata.
///
/// `resident` is `active` + dirty + `metadata`, so freeing a churn moves its pages from
/// `active` into dirty and leaves `resident` where it was. The signal is
/// `active + metadata`: it drops when the churn is freed, but never omits jemalloc's own
/// live bookkeeping.
///
/// This is the test that the scripted-signal tests below cannot be: they force a value
/// through the test seam and so never exercise which statistic is read. Here every number
/// comes from jemalloc after real allocation.
///
/// Ignored because it moves process-wide allocator state, which destabilises the other
/// tests that read real statistics. Run with
/// `cargo test --lib memory_guard_signal_test -- --ignored --test-threads=1`.
#[test]
#[ignore = "moves process-wide jemalloc state; run explicitly"]
fn guard_signal_excludes_freed_but_retained_pages() {
    let _guard = ScopedGuardState::acquire();

    // Many small growing vectors across size classes: the shape a high-cardinality
    // grouped aggregate produces, at unit-test scale.
    const GROUPS: usize = 200_000;
    let mut groups: Vec<Vec<u64>> = Vec::with_capacity(GROUPS);
    for group in 0..GROUPS {
        let distinct = group % 64 + 1;
        let mut registers: Vec<u64> = Vec::new();
        for value in 0..distinct {
            registers.push((group as u64) << 32 | value as u64);
        }
        groups.push(registers);
    }
    std::hint::black_box(&groups);
    drop(groups);

    let live = allocated_bytes();
    let signal = unreclaimable_bytes();
    let resident = resident_bytes();
    println!("after freeing every group: allocated={live} signal={signal} resident={resident}");
    println!("freed but still resident: {} bytes", resident - signal);

    assert!(
        resident - signal > 16 * 1024 * 1024,
        "expected a large pool of freed-but-retained pages after the churn, but resident \
         ({resident}) exceeded the signal ({signal}) by only {}. If this is ever near zero \
         the allocator is returning pages promptly and the guard's old signal was not the \
         problem.",
        resident - signal
    );
    assert!(
        signal < resident / 4,
        "the guard's signal ({signal}) must be far below what it used to read ({resident}), \
         otherwise the change has no effect"
    );
    assert!(
        signal >= live,
        "the signal ({signal}) counts whole pages backing live bytes ({live}) plus jemalloc \
         metadata, so it cannot be smaller"
    );
}

/// A small request is refused once the signal passes the threshold, with nothing reserved.
///
/// Pins both halves of the error: the prefix `NativeErrorConverter` parses into the HTTP
/// 429, and the trailing clause naming the real cause. The old message reported
/// `0 available out of {pool limit} limit` and nothing else, which reads as pool
/// exhaustion even though `reserved()` is zero — that misattribution sent an
/// investigation after a memory leak.
#[test]
fn elevated_signal_refuses_a_small_request_with_nothing_reserved() {
    let guard = ScopedGuardState::acquire();
    guard.set_pool_limit(POOL_LIMIT_BYTES as i64);
    guard.force_resident(fraction_of_pool_limit(0.856));

    let (pool, handle) = pool_with_limit(POOL_LIMIT_BYTES);
    let mut reservation = MemoryConsumer::new("GroupedHashAggregateStream[1]").register(&pool);

    let message = reservation
        .try_grow(SMALL_REQUEST_BYTES)
        .expect_err("a 346224-byte request must be refused once the signal is elevated")
        .to_string();

    assert!(
        message.starts_with(
            "Resources exhausted: Failed to allocate 346224 bytes for \
             GroupedHashAggregateStream[1] (0 already reserved) — 0 available out of \
             17179869184 limit."
        ),
        "the prefix NativeErrorConverter parses must be preserved verbatim; got: {message}"
    );
    assert!(
        message.contains("Refused by RSS guard [spill-band]")
            && message.contains("pool used 0 of 17179869184")
            && message.contains("Not pool exhaustion"),
        "the message must name the guard, the branch and the pool's real used, so an RSS \
         trip is never again read as pool exhaustion; got: {message}"
    );

    assert_eq!(
        pool.reserved(),
        0,
        "the refusal happens before the pool's CAS, so nothing was charged"
    );
    assert_eq!(handle.tripped_count(), 1);
}

/// The same request succeeds, is refused, then succeeds again, with only the signal
/// changing.
///
/// This is the reported symptom's shape: the pool, consumer and byte count are constant
/// across all three attempts, which is why waiting between executions makes an identical
/// query pass.
#[test]
fn identical_request_succeeds_then_is_refused_then_succeeds_again() {
    let guard = ScopedGuardState::acquire();
    guard.set_pool_limit(POOL_LIMIT_BYTES as i64);

    let (pool, handle) = pool_with_limit(POOL_LIMIT_BYTES);
    let mut reservation = MemoryConsumer::new("GroupedHashAggregateStream[1]").register(&pool);

    guard.force_resident(fraction_of_pool_limit(0.10));
    assert!(
        reservation.try_grow(SMALL_REQUEST_BYTES).is_ok(),
        "the first attempt must succeed"
    );
    assert_eq!(pool.reserved(), SMALL_REQUEST_BYTES);
    assert_eq!(handle.tripped_count(), 0);
    reservation.free();

    guard.force_resident(fraction_of_pool_limit(
        (SPILL_THRESHOLD + CRITICAL_THRESHOLD) / 2.0,
    ));
    assert!(
        reservation.try_grow(SMALL_REQUEST_BYTES).is_err(),
        "the identical second attempt must be refused"
    );
    assert_eq!(pool.reserved(), 0);
    assert_eq!(handle.tripped_count(), 1);

    guard.force_resident(fraction_of_pool_limit(0.10));
    assert!(
        reservation.try_grow(SMALL_REQUEST_BYTES).is_ok(),
        "once the signal falls, the identical request must succeed again"
    );
    assert_eq!(
        handle.tripped_count(),
        1,
        "a success must not count as a trip"
    );
}

/// Past the critical threshold the requested size does not participate in the decision.
///
/// Scoped to the critical branch on purpose: inside the 85–95 % band the request size *is*
/// compared, against the remaining spill-exemption budget.
#[test]
fn refusal_ignores_requested_size_past_the_critical_threshold() {
    let guard = ScopedGuardState::acquire();
    guard.set_pool_limit(POOL_LIMIT_BYTES as i64);
    guard.force_resident(fraction_of_pool_limit(0.96));

    for bytes in [
        1024_usize,
        346_224,
        1_448_000,
        4_194_304,
        4 * 1024 * 1024 * 1024,
    ] {
        let (pool, handle) = pool_with_limit(POOL_LIMIT_BYTES);
        let mut reservation = MemoryConsumer::new("consumer").register(&pool);
        let message = reservation
            .try_grow(bytes)
            .expect_err("every size is refused on identical terms past the threshold")
            .to_string();
        assert!(
            message.contains(&format!("Failed to allocate {bytes} bytes")),
            "the size appears in the message but never in a comparison: {message}"
        );
        assert_eq!(pool.reserved(), 0);
        assert_eq!(handle.tripped_count(), 1);
    }
}

/// In the 85–95 % band spillability decides the outcome; past 95 % nothing does.
///
/// Both branches emit the same `0 available`, so the branch name added to the message is
/// the only thing that distinguishes them in a log.
#[test]
fn spill_band_exempts_spillable_consumers_but_critical_band_exempts_none() {
    let guard = ScopedGuardState::acquire();
    guard.set_pool_limit(POOL_LIMIT_BYTES as i64);

    guard.force_resident(fraction_of_pool_limit(
        (SPILL_THRESHOLD + CRITICAL_THRESHOLD) / 2.0,
    ));

    let (pool, _handle) = pool_with_limit(POOL_LIMIT_BYTES);
    let mut spillable = MemoryConsumer::new("SortExec")
        .with_can_spill(true)
        .register(&pool);
    assert!(
        spillable.try_grow(SMALL_REQUEST_BYTES).is_ok(),
        "a spillable consumer is exempted in the band so it can finish spilling"
    );

    let (pool, _handle) = pool_with_limit(POOL_LIMIT_BYTES);
    let mut non_spillable = MemoryConsumer::new("FinalHashAggregateStream[7]").register(&pool);
    let message = non_spillable
        .try_grow(SMALL_REQUEST_BYTES)
        .expect_err("a non-spillable consumer is refused in the band")
        .to_string();
    assert!(message.contains("[spill-band]"), "got: {message}");

    guard.force_resident(fraction_of_pool_limit(0.96));

    let (pool, _handle) = pool_with_limit(POOL_LIMIT_BYTES);
    let mut spillable = MemoryConsumer::new("SortExec")
        .with_can_spill(true)
        .register(&pool);
    let message = spillable
        .try_grow(SMALL_REQUEST_BYTES)
        .expect_err("the critical branch refuses spillable consumers too")
        .to_string();
    assert!(message.contains("[critical]"), "got: {message}");
}

/// The guard does not activate below a 16 MiB pool limit, which is why small-pool tests
/// never exercise it. Recorded so a future author does not read that silence as absence.
#[test]
fn guard_is_inert_for_pools_below_sixteen_mebibytes() {
    let guard = ScopedGuardState::acquire();
    let small_limit = 8 * 1024 * 1024;
    guard.set_pool_limit(small_limit as i64);
    guard.force_resident(64 * 1024 * 1024);

    let (pool, handle) = pool_with_limit(small_limit);
    let mut reservation = MemoryConsumer::new("consumer").register(&pool);
    assert!(
        reservation.try_grow(4096).is_ok(),
        "below a 16 MiB pool limit the guard does not activate"
    );
    assert_eq!(handle.tripped_count(), 0);
    let _ = memory_guard::get_thresholds();
}
