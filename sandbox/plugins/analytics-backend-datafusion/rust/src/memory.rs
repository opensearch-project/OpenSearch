/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Dynamic memory pool for DataFusion query execution.
//!
//! Replaces DataFusion's `GreedyMemoryPool` with a pool whose limit can be
//! changed at runtime via a shared `DynamicLimitHandle`. The pool and handle
//! share an `Arc<AtomicUsize>` for the limit, so `set_limit` is lock-free
//! and takes effect on the next `try_grow` call.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryPool, MemoryReservation};

/// Outcome of the 85%→95% spill-gate decision for one reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpillGateDecision {
    /// Allow the request through so the spill can finish writing.
    Exempt,
    /// Reject: not a spillable consumer, so reject it to make it spill.
    RejectNonSpillable,
    /// Reject: spillable, but the request is larger than the remaining budget.
    RejectCapped,
}

/// Decides what to do with a memory request whose RSS is in the 85%-to-95%
/// band (above the spill threshold, below the critical threshold).
///
/// A request from a spillable consumer is allowed through ("exempt"). When such
/// a consumer hits the spill threshold it spills: it frees its existing data
/// first, then needs a small temporary buffer to write the spilled data out.
/// That buffer must be allowed to allocate, otherwise the spill cannot finish
/// and the query gets stuck. The exemption is bounded by a fixed byte budget,
/// `cap - outstanding`, so that many spills happening at once cannot add up to
/// enough memory to cross the 95% critical threshold.
///
/// The decision deliberately does not look at how many pages the allocator has
/// freed-but-not-yet-returned. Right after a spill frees its data, the OS RSS
/// has not dropped yet (the allocator holds the pages), so that signal reads as
/// near-zero exactly when the spill needs its buffer, and an earlier version of
/// this code wrongly rejected the buffer ("Failed to reserve memory for sort
/// during spill"). The fixed byte budget avoids that. The caller still applies
/// the pool-limit check and the 95% critical check, so the node stays protected
/// from running out of memory.
///
/// Kept as a separate pure function so it can be unit-tested without needing
/// real process memory. The 95% critical check runs in the caller before this
/// is called, so this can never allow crossing 95%.
pub(crate) fn spill_gate_decision(
    can_spill: bool,
    additional: usize,
    cap: usize,
    outstanding: usize,
) -> SpillGateDecision {
    if !can_spill {
        return SpillGateDecision::RejectNonSpillable;
    }
    let cap_room = cap.saturating_sub(outstanding);
    if additional <= cap_room {
        SpillGateDecision::Exempt
    } else {
        SpillGateDecision::RejectCapped
    }
}

/// A `MemoryPool` whose limit can be changed at runtime.
///
/// Behaviour matches `GreedyMemoryPool` exactly, except the limit is stored
/// in an `AtomicUsize` shared with a [`DynamicLimitHandle`].
///
/// - Increasing the limit takes effect immediately for new allocations.
/// - Decreasing the limit takes effect for new allocations only.
///   Existing reservations that exceed the new limit are NOT reclaimed.
#[derive(Debug)]
pub struct DynamicLimitPool {
    used: AtomicUsize,
    dynamic_limit: Arc<AtomicUsize>,
    tripped_count: Arc<AtomicUsize>,
    /// Total bytes currently allowed through the 85% gate by the spill
    /// exemption that have not yet been freed. This is the `outstanding` value
    /// the exemption budget is measured against, so concurrent spills together
    /// cannot exceed the cap. Increased when a request is exempted, decreased in
    /// `shrink` as memory is freed (saturating, so it never goes below zero).
    exempt_outstanding: AtomicUsize,
}

/// Handle to change the pool limit at runtime.
///
/// Can be stored separately from the pool (which is consumed by
/// `TrackConsumersPool::new`). Both the pool and handle point to the
/// same `Arc<AtomicUsize>`.
#[derive(Debug, Clone)]
pub struct DynamicLimitHandle {
    limit: Arc<AtomicUsize>,
    tripped: Arc<AtomicUsize>,
}

impl DynamicLimitHandle {
    /// Atomically set a new limit. Takes effect on the next `try_grow`.
    pub fn set_limit(&self, new_limit: usize) {
        self.limit.store(new_limit, Ordering::Release);
    }

    /// Read the current limit.
    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Acquire)
    }

    /// Number of times try_grow was rejected.
    pub fn tripped_count(&self) -> usize {
        self.tripped.load(Ordering::Relaxed)
    }
}

impl DynamicLimitPool {
    /// Create a new pool with the given initial limit.
    /// Returns the pool and a handle to change the limit later.
    pub fn new(initial_limit: usize) -> (Self, DynamicLimitHandle) {
        let limit = Arc::new(AtomicUsize::new(initial_limit));
        let tripped = Arc::new(AtomicUsize::new(0));
        let handle = DynamicLimitHandle {
            limit: limit.clone(),
            tripped: tripped.clone(),
        };
        let pool = Self {
            used: AtomicUsize::new(0),
            dynamic_limit: limit,
            tripped_count: tripped,
            exempt_outstanding: AtomicUsize::new(0),
        };
        (pool, handle)
    }

    /// Read the current limit.
    pub fn limit(&self) -> usize {
        self.dynamic_limit.load(Ordering::Acquire)
    }

    /// Number of times try_grow was rejected (after jemalloc confirmation).
    pub fn tripped_count(&self) -> usize {
        self.tripped_count.load(Ordering::Relaxed)
    }
}

impl std::fmt::Display for DynamicLimitPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DynamicLimitPool(limit={}, used={})",
            self.dynamic_limit.load(Ordering::Acquire),
            self.used.load(Ordering::Relaxed)
        )
    }
}

impl MemoryPool for DynamicLimitPool {
    fn name(&self) -> &str {
        "DynamicLimitPool"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        // `grow` is an infallible accounting call; the caller is responsible
        // for pairing it with a successful `try_grow`, so under well-behaved
        // callers `used + additional` cannot overflow `usize`. Use a saturating
        // CAS loop so that a buggy caller (or a malicious `additional == usize::MAX`)
        // cannot wrap the counter.
        let _ = self
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                Some(used.saturating_add(additional))
            });
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
        // Give back exemption budget as memory is freed (a spill writing out its
        // data calls `shrink`). Subtract without going below zero. A normal,
        // non-exempt shrink may give budget back a little early, which is safe:
        // it only makes the exemption stricter, never looser.
        let _ = self
            .exempt_outstanding
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |out| {
                Some(out.saturating_sub(shrink))
            });
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        // Two memory checks based on process RSS:
        // - At 95% (critical): reject every request. This is the hard limit that
        //   protects the node from running out of memory.
        // - At 85% (spill): reject requests so consumers spill to disk, except
        //   spillable consumers, which are allowed through so they can finish
        //   spilling (see `spill_gate_decision`). The exemption is bounded by a
        //   byte budget, and the pool-limit and 95% checks still apply, so this
        //   cannot push the node over the limit.
        //
        // Set if a spillable consumer is allowed through the 85% check; its
        // budget is only counted after the allocation actually succeeds below.
        let mut exempted = false;
        let limit = self.dynamic_limit.load(Ordering::Acquire);
        let resident = crate::memory_guard::cached_resident_bytes();
        if resident > 0 && limit >= 16 * 1024 * 1024 {
            let thresholds = crate::memory_guard::get_thresholds();
            let critical_bytes = (limit as f64 * thresholds.execution_critical) as usize;
            let spill_bytes = (limit as f64 * thresholds.execution_spill) as usize;
            let resident_usize = resident as usize;

            // Critical (95%): hard reject — OOM imminent, protect the node.
            // Absolute and pre-CAS for every consumer, spillable or not.
            if resident_usize > critical_bytes {
                self.tripped_count.fetch_add(1, Ordering::Relaxed);
                return Err(crate::native_error::pool_limit_error(
                    additional,
                    reservation.consumer().name(),
                    reservation.size(),
                    0,
                    limit,
                ));
            }

            // RSS is between 85% and 95%. Allow a spillable consumer through so
            // it can finish spilling; reject everything else so it spills.
            if resident_usize > spill_bytes {
                let can_spill = reservation.consumer().can_spill();
                let outstanding = self.exempt_outstanding.load(Ordering::Relaxed);
                let cap = crate::memory_guard::spill_exempt_cap_bytes();

                match spill_gate_decision(can_spill, additional, cap, outstanding) {
                    // Continue to the allocation below. Only count this against
                    // the budget if the allocation actually succeeds (see
                    // `exempted`), so a failed one doesn't use up the budget.
                    SpillGateDecision::Exempt => exempted = true,
                    _ => {
                        self.tripped_count.fetch_add(1, Ordering::Relaxed);
                        return Err(crate::native_error::pool_limit_error(
                            additional,
                            reservation.consumer().name(),
                            reservation.size(),
                            0,
                            limit,
                        ));
                    }
                }
            }
        }

        let dynamic_limit = &self.dynamic_limit;

        // Fast path: try the normal CAS against the pool limit.
        let cas_result = self
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let limit = dynamic_limit.load(Ordering::Acquire);
                let new_used = used.checked_add(additional)?;
                (new_used <= limit).then_some(new_used)
            });

        if cas_result.is_ok() {
            // Charge the exemption budget only now that the grow succeeded;
            // released saturating in `shrink`.
            if exempted {
                self.exempt_outstanding
                    .fetch_add(additional, Ordering::Relaxed);
            }
            return Ok(());
        }

        // Pool accounting says "full". Before failing the operator (which
        // triggers spill), consult jemalloc as ground truth. If actual process
        // memory is below the override threshold, the pool's "full" state is
        // from stale phantoms or accounting drift — allow the grow.
        //
        // This gives already-executing operators a higher effective limit,
        // preventing unnecessary spills when phantoms from finished queries
        // haven't been released yet.
        let limit = dynamic_limit.load(Ordering::Acquire);
        let used = self.used.load(Ordering::Relaxed);
        // Only attempt override if the allocation is plausible (won't overflow).
        if used.checked_add(additional).is_some() {
            if crate::memory_guard::should_override(
                limit,
                crate::memory_guard::OverrideContext::Execution,
            ) {
                // jemalloc confirms headroom — allow the grow
                let _ = self
                    .used
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |u| {
                        u.checked_add(additional)
                    });
                if exempted {
                    self.exempt_outstanding
                        .fetch_add(additional, Ordering::Relaxed);
                }
                return Ok(());
            }
        }

        // Both pool and jemalloc confirm pressure. Check if RSS is critical —
        // if so, cancel the query rather than spilling (spill can't help at 95%+).
        if crate::memory_guard::should_cancel_query(limit) {
            native_bridge_common::log_info!(
                "Memory CANCEL: RSS exceeds critical threshold for consumer [{}]. Cancelling query to protect node.",
                reservation.consumer().name()
            );
            self.tripped_count.fetch_add(1, Ordering::Relaxed);
            return Err(crate::native_error::critical_pressure_error(
                additional,
                reservation.consumer().name(),
                reservation.size(),
                limit,
                (crate::memory_guard::get_thresholds().execution_critical * 100.0) as u32,
            ));
        }

        // RSS between operator (85%) and critical (95%) — reject (operator will spill)
        self.tripped_count.fetch_add(1, Ordering::Relaxed);
        let used = self.used.load(Ordering::Relaxed);
        Err(crate::native_error::pool_limit_error(
            additional,
            reservation.consumer().name(),
            reservation.size(),
            limit.saturating_sub(used),
            limit,
        ))
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;

    /// Build an `Arc<dyn MemoryPool>` + handle for tests.
    /// DataFusion 52+ `MemoryConsumer::register` takes `&Arc<dyn MemoryPool>`,
    /// so tests wrap the concrete pool once rather than repeating the cast
    /// at every call site.
    fn new_pool(limit: usize) -> (Arc<dyn MemoryPool>, DynamicLimitHandle) {
        let (pool, handle) = DynamicLimitPool::new(limit);
        (Arc::new(pool), handle)
    }

    #[test]
    fn test_initial_limit() {
        let (pool, handle) = new_pool(1024);
        assert_eq!(handle.limit(), 1024);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_set_limit() {
        let (_pool, handle) = new_pool(1024);
        handle.set_limit(2048);
        assert_eq!(handle.limit(), 2048);
    }

    #[test]
    fn test_try_grow_within_limit() {
        let (pool, _handle) = new_pool(1024);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);
        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 512);
    }

    #[test]
    fn test_try_grow_exceeds_limit() {
        let (pool, _handle) = new_pool(1024);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);
        assert!(reservation.try_grow(2048).is_err());
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_dynamic_limit_increase() {
        let (pool, handle) = new_pool(1024);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);

        // Fails at 1024 limit
        assert!(reservation.try_grow(2048).is_err());

        // Increase limit
        handle.set_limit(4096);

        // Now succeeds
        assert!(reservation.try_grow(2048).is_ok());
        assert_eq!(pool.reserved(), 2048);
    }

    #[test]
    fn test_dynamic_limit_decrease_existing_reservations_kept() {
        let (pool, handle) = new_pool(4096);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);

        // Reserve 2048
        assert!(reservation.try_grow(2048).is_ok());

        // Decrease limit below current usage
        handle.set_limit(1024);

        // Existing reservation is NOT reclaimed
        assert_eq!(pool.reserved(), 2048);

        // But new allocations fail
        let consumer2 = MemoryConsumer::new("test2");
        let mut reservation2 = consumer2.register(&pool);
        assert!(reservation2.try_grow(1).is_err());
    }

    #[test]
    fn test_try_grow_overflow_protection() {
        let (pool, _handle) = new_pool(usize::MAX);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);
        assert!(reservation.try_grow(1024).is_ok());
        // `additional == usize::MAX` would overflow `used + additional`.
        // checked_add inside fetch_update must reject it cleanly.
        assert!(reservation.try_grow(usize::MAX).is_err());
        assert_eq!(pool.reserved(), 1024);
    }

    #[test]
    fn test_grow_saturates_instead_of_wrapping() {
        let (pool, _handle) = new_pool(1024);
        // `grow` is infallible accounting — a buggy caller must not be able to
        // wrap `used` back to zero by passing `usize::MAX`. `saturating_add`
        // pins `used` at `usize::MAX` instead.
        let consumer = MemoryConsumer::new("test");
        let reservation = consumer.register(&pool);
        pool.grow(&reservation, usize::MAX);
        assert_eq!(pool.reserved(), usize::MAX);
        pool.grow(&reservation, 1);
        assert_eq!(pool.reserved(), usize::MAX);
    }

    #[test]
    fn test_concurrent_set_limit_observed_by_try_grow() {
        use std::sync::Barrier;
        use std::thread;

        // Repeat to give the race a chance to surface.
        for _ in 0..64 {
            let (pool, handle) = new_pool(1024);
            let barrier = Arc::new(Barrier::new(2));

            let raiser = {
                let handle = handle.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    handle.set_limit(1 << 30);
                })
            };

            let allocator = {
                let pool = pool.clone();
                let handle = handle.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let consumer = MemoryConsumer::new("race");
                    let mut reservation = consumer.register(&pool);
                    // Retry until either allocation succeeds OR the handle reports
                    // the new limit is visible. The previous fixed-iteration loop
                    // flaked on fast runners because the allocator could exhaust
                    // its retries before the raiser thread's store completed.
                    //
                    // The atomic invariant under test: once `handle.limit() >= 2048`
                    // is observable, the very next `try_grow(2048)` MUST succeed
                    // (that's the Release/Acquire happens-before contract). If that
                    // final try_grow fails, that IS a real pool bug.
                    loop {
                        if reservation.try_grow(2048).is_ok() {
                            break;
                        }
                        if handle.limit() >= 2048 {
                            reservation.try_grow(2048).expect(
                                "once handle.limit() reflects the raise, try_grow must succeed",
                            );
                            break;
                        }
                        std::hint::spin_loop();
                    }
                })
            };

            raiser.join().unwrap();
            allocator.join().unwrap();
        }
    }

    #[test]
    fn test_try_grow_rejection_increments_tripped_count() {
        // Pool with 1KB limit — below MIN_POOL_FOR_OVERRIDE (16MB) so
        // jemalloc override is skipped entirely. Every rejection goes
        // straight to tripped_count increment.
        let (pool, handle) = new_pool(1024);
        assert_eq!(handle.tripped_count(), 0);

        let consumer = MemoryConsumer::new("hash_agg");
        let mut reservation = consumer.register(&pool);

        // First grow succeeds
        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(handle.tripped_count(), 0);

        // Second grow exceeds limit → rejected, tripped increments
        assert!(reservation.try_grow(1024).is_err());
        assert_eq!(handle.tripped_count(), 1);

        // Fill to the limit
        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 1024);

        // Now even 1 byte exceeds → tripped again
        assert!(reservation.try_grow(1).is_err());
        assert_eq!(handle.tripped_count(), 2);
    }

    #[test]
    fn test_tripped_count_reflects_in_handle_after_multiple_consumers() {
        let (pool, handle) = new_pool(2048);
        assert_eq!(handle.tripped_count(), 0);

        // Consumer A takes 1500 bytes
        let consumer_a = MemoryConsumer::new("sort_buffer");
        let mut res_a = consumer_a.register(&pool);
        assert!(res_a.try_grow(1500).is_ok());

        // Consumer B tries 1000 bytes — only 548 available → rejected
        let consumer_b = MemoryConsumer::new("hash_agg");
        let mut res_b = consumer_b.register(&pool);
        assert!(res_b.try_grow(1000).is_err());
        assert_eq!(handle.tripped_count(), 1);

        // Consumer A releases, freeing space
        res_a.shrink(1500);

        // Consumer B retries — now succeeds, no additional trip
        assert!(res_b.try_grow(1000).is_ok());
        assert_eq!(handle.tripped_count(), 1);
    }

    #[test]
    fn test_tripped_count_zero_when_all_grows_succeed() {
        let (pool, handle) = new_pool(1_000_000);
        let consumer = MemoryConsumer::new("scan");
        let mut reservation = consumer.register(&pool);

        for _ in 0..100 {
            assert!(reservation.try_grow(1000).is_ok());
        }
        assert_eq!(handle.tripped_count(), 0);
        assert_eq!(pool.reserved(), 100_000);
    }

    #[test]
    fn test_hard_guard_rejects_when_rss_exceeds_critical() {
        // Create a pool with a limit smaller than the current process RSS.
        // A Rust test process typically uses 50-200MB RSS, so 20MB should
        // always trigger the hard guard (RSS > 95% of 20MB = 19MB).
        let (pool, handle) = new_pool(20 * 1024 * 1024); // 20MB

        let resident = crate::memory_guard::cached_resident_bytes();
        if resident <= 0 {
            return; // jemalloc not available in this test env
        }

        let critical_bytes = (20.0 * 1024.0 * 1024.0 * 0.95) as i64;
        if resident < critical_bytes {
            return; // RSS unexpectedly low — skip rather than false-fail
        }

        let consumer = MemoryConsumer::new("hard_guard_test");
        let mut reservation = consumer.register(&pool);

        // The hard guard fires before the CAS — even a tiny grow should be rejected
        let result = reservation.try_grow(1024);
        assert!(
            result.is_err(),
            "try_grow should fail when RSS ({}) exceeds critical threshold (95% of 20MB)",
            resident
        );
        assert!(
            handle.tripped_count() >= 1,
            "tripped_count should increment on hard guard rejection"
        );
    }

    #[test]
    fn test_hard_guard_passes_when_rss_below_critical() {
        // Create a pool with a huge limit (1TB). The test process RSS is well
        // below 95% of 1TB, so the hard guard should NOT fire.
        let limit = 1024 * 1024 * 1024 * 1024_usize; // 1TB
        let (pool, handle) = new_pool(limit);

        let consumer = MemoryConsumer::new("large_pool_test");
        let mut reservation = consumer.register(&pool);

        // A small allocation should succeed — RSS is far below 95% of 1TB
        let result = reservation.try_grow(4096);
        assert!(
            result.is_ok(),
            "try_grow should succeed when RSS is well below 95% of 1TB pool limit"
        );
        assert_eq!(
            handle.tripped_count(),
            0,
            "tripped_count should remain 0 when hard guard does not fire"
        );
        assert_eq!(pool.reserved(), 4096);
    }

    // ---- Spill-gate exemption rule (pure, deterministic) ----

    const MB: usize = 1024 * 1024;

    #[test]
    fn test_spill_gate_rejects_non_spillable() {
        // A non-spillable consumer in the 85% band is always rejected so it
        // triggers spill — regardless of the cap budget.
        let d = spill_gate_decision(false, 256 * 1024, 512 * MB, 0);
        assert_eq!(d, SpillGateDecision::RejectNonSpillable);
    }

    #[test]
    fn test_spill_gate_exempts_spillable_within_cap() {
        // Spillable, small request, full cap available → exempt.
        // Mirrors the observed 256 KiB spill-trigger allocation.
        let d = spill_gate_decision(true, 262_528, 512 * MB, 0);
        assert_eq!(d, SpillGateDecision::Exempt);
    }

    #[test]
    fn test_spill_gate_exempts_buffer_within_budget() {
        // A 193 MB spill buffer must be allowed through as long as it fits the
        // budget. An earlier version also looked at how much memory the allocator
        // had freed but not yet returned, which reads as near-zero right when a
        // spill needs its buffer, so it wrongly rejected this and the spill could
        // not finish. The decision now depends only on the budget.
        let d = spill_gate_decision(true, 193 * MB, 512 * MB, 0);
        assert_eq!(d, SpillGateDecision::Exempt);
        // A buffer larger than the whole budget is still rejected.
        let d2 = spill_gate_decision(true, 600 * MB, 512 * MB, 0);
        assert_eq!(d2, SpillGateDecision::RejectCapped);
    }

    #[test]
    fn test_spill_gate_caps_when_budget_exhausted() {
        // Most of the budget is already used (500MB of a 512MB cap, leaving
        // 12MB). A 64 MB request does not fit, so it is rejected.
        let d = spill_gate_decision(true, 64 * MB, 512 * MB, 500 * MB);
        assert_eq!(d, SpillGateDecision::RejectCapped);
        // A request that fits the remaining 12 MB is allowed through.
        let d2 = spill_gate_decision(true, 8 * MB, 512 * MB, 500 * MB);
        assert_eq!(d2, SpillGateDecision::Exempt);
    }

    #[test]
    fn test_spill_gate_bound_is_cap_minus_outstanding() {
        // The sole bound is the hard cap minus outstanding exemptions.
        // 40MB room (512 - 472), 60MB request → rejected.
        assert_eq!(
            spill_gate_decision(true, 60 * MB, 512 * MB, 472 * MB),
            SpillGateDecision::RejectCapped
        );
        // Same request fits when the cap is unused.
        assert_eq!(
            spill_gate_decision(true, 60 * MB, 512 * MB, 0),
            SpillGateDecision::Exempt
        );
    }
}
