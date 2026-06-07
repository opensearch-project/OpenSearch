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

impl MemoryPool for DynamicLimitPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        // `grow` is an infallible accounting call; the caller is responsible
        // for pairing it with a successful `try_grow`, so under well-behaved
        // callers `used + additional` cannot overflow `usize`. Use a saturating
        // CAS loop so that a buggy caller (or a malicious `additional == usize::MAX`)
        // cannot wrap the counter.
        let _ = self.used.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
            Some(used.saturating_add(additional))
        });
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        // Decision order: CAS → override (stale fill) → RSS-critical → pool-fill.
        let dynamic_limit = &self.dynamic_limit;

        // 1. Try the pool-limit CAS. Succeeds when accounting has room.
        let cas_result = self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let limit = dynamic_limit.load(Ordering::Acquire);
                let new_used = used.checked_add(additional)?;
                (new_used <= limit).then_some(new_used)
            });

        if cas_result.is_ok() {
            return Ok(());
        }

        let limit = dynamic_limit.load(Ordering::Acquire);
        let used = self.used.load(Ordering::Relaxed);

        // 2. Pool says full. Forgive if jemalloc reports physical headroom
        //    (pool view stale from accounting drift).
        if used.checked_add(additional).is_some() {
            if crate::memory_guard::should_override(limit, crate::memory_guard::OverrideContext::Execution) {
                let _ = self.used.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |u| {
                    u.checked_add(additional)
                });
                return Ok(());
            }
        }

        // 3. RSS-critical ceiling: physical memory is genuinely pressured.
        //    Cancel the allocation rather than spilling — recovery isn't possible.
        if crate::memory_guard::should_cancel_query(limit) {
            self.tripped_count.fetch_add(1, Ordering::Relaxed);
            let resident = crate::memory_guard::cached_resident_bytes();
            native_bridge_common::log_info!(
                "Memory RSS-CRITICAL: RSS={}B exceeds critical threshold for consumer [{}]. Cancelling allocation to protect node.",
                resident,
                reservation.consumer().name()
            );
            return Err(crate::native_error::rss_critical_error(
                additional,
                reservation.consumer().name(),
                reservation.size(),
                limit,
                used,
                resident,
            ));
        }

        // 4. Pool legitimately full but RSS is fine — return pool-fill error
        //    so DataFusion's spill-capable operators can flush state to disk.
        self.tripped_count.fetch_add(1, Ordering::Relaxed);
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
                            reservation
                                .try_grow(2048)
                                .expect("once handle.limit() reflects the raise, try_grow must succeed");
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
    fn test_rss_critical_rejects_after_cas_fail_when_rss_exceeds_critical() {
        // 20MB pool (above MIN_POOL_FOR_OVERRIDE), pre-filled so the next
        // try_grow CAS-fails. Test process RSS typically 50-200MB, so
        // RSS > 95% of 20MB and the rejection arrives via rss_critical_error.
        let (pool, handle) = new_pool(20 * 1024 * 1024); // 20MB

        let resident = crate::memory_guard::cached_resident_bytes();
        if resident <= 0 {
            return; // jemalloc not available in this test env
        }
        let critical_bytes = (20.0 * 1024.0 * 1024.0 * 0.95) as i64;
        if resident < critical_bytes {
            return; // RSS unexpectedly low — skip rather than false-fail
        }

        // Fill the pool so the CAS for the test allocation must fail.
        let filler = MemoryConsumer::new("filler");
        let mut filler_res = filler.register(&pool);
        assert!(filler_res.try_grow(20 * 1024 * 1024).is_ok());

        let consumer = MemoryConsumer::new("rss_critical_test");
        let mut reservation = consumer.register(&pool);

        let result = reservation.try_grow(1024);
        assert!(
            result.is_err(),
            "try_grow should fail when CAS fails and RSS ({}) exceeds critical (95% of 20MB)",
            resident
        );
        if let Err(DataFusionError::ResourcesExhausted(msg)) = result {
            assert!(
                msg.contains("Native RSS pressure"),
                "expected RSS-critical error, got: {}",
                msg
            );
        }
        assert!(
            handle.tripped_count() >= 1,
            "tripped_count should increment on RSS-critical rejection"
        );
    }

    #[test]
    fn test_rss_critical_passes_when_rss_below_critical() {
        // 1TB pool; test process RSS is far below 95% of 1TB so a small
        // allocation succeeds via CAS without engaging the RSS-critical path.
        let limit = 1024 * 1024 * 1024 * 1024_usize; // 1TB
        let (pool, handle) = new_pool(limit);

        let consumer = MemoryConsumer::new("large_pool_test");
        let mut reservation = consumer.register(&pool);

        let result = reservation.try_grow(4096);
        assert!(
            result.is_ok(),
            "try_grow should succeed via CAS on a 1TB pool"
        );
        assert_eq!(
            handle.tripped_count(),
            0,
            "tripped_count should remain 0 when CAS succeeds"
        );
        assert_eq!(pool.reserved(), 4096);
    }

    #[test]
    fn test_pool_fill_rejection_emits_pool_limit_error_not_rss_error() {
        // 1KB pool — below MIN_POOL_FOR_OVERRIDE so override and RSS-critical
        // skip. Asserts pool-fill rejections use pool_limit_error, not the
        // RSS variant — locks the error-message contract for the Java parser.
        let (pool, _handle) = new_pool(1024);
        let consumer = MemoryConsumer::new("pool_fill_test");
        let mut reservation = consumer.register(&pool);

        let result = reservation.try_grow(2048);
        assert!(result.is_err(), "exceeds pool limit, should fail");
        if let Err(DataFusionError::ResourcesExhausted(msg)) = result {
            assert!(
                msg.contains("Failed to allocate"),
                "expected pool_limit_error, got: {}",
                msg
            );
            assert!(
                msg.contains("Native RSS pressure") == false,
                "pool-fill rejection must NOT use rss_critical_error: {}",
                msg
            );
        } else {
            panic!("expected ResourcesExhausted variant");
        }
    }
}
