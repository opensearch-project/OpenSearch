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
}

/// Handle to change the pool limit at runtime.
///
/// Can be stored separately from the pool (which is consumed by
/// `TrackConsumersPool::new`). Both the pool and handle point to the
/// same `Arc<AtomicUsize>`.
#[derive(Debug, Clone)]
pub struct DynamicLimitHandle {
    limit: Arc<AtomicUsize>,
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
}

impl DynamicLimitPool {
    /// Create a new pool with the given initial limit.
    /// Returns the pool and a handle to change the limit later.
    pub fn new(initial_limit: usize) -> (Self, DynamicLimitHandle) {
        let limit = Arc::new(AtomicUsize::new(initial_limit));
        let handle = DynamicLimitHandle {
            limit: limit.clone(),
        };
        let pool = Self {
            used: AtomicUsize::new(0),
            dynamic_limit: limit,
        };
        (pool, handle)
    }

    /// Read the current limit.
    pub fn limit(&self) -> usize {
        self.dynamic_limit.load(Ordering::Acquire)
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
        // Load the limit inside the closure so every CAS retry sees the current
        // value. A concurrent `set_limit` that raises the limit while we are
        // spinning here should be honoured; loading once outside the closure
        // would miss that update.
        let dynamic_limit = &self.dynamic_limit;
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let limit = dynamic_limit.load(Ordering::Acquire);
                let new_used = used.checked_add(additional)?;
                (new_used <= limit).then_some(new_used)
            })
            .map_err(|used| {
                // Re-load the limit for the error message. This can be a slightly newer
                // value than the limit used in the decision above under a concurrent
                // `set_limit`, but we prefer "most-recent" for operator visibility. The
                // allocation decision itself was already made against a consistent
                // snapshot inside the closure.
                let limit = dynamic_limit.load(Ordering::Acquire);
                DataFusionError::ResourcesExhausted(format!(
                    "Failed to allocate {} bytes for {} ({} already reserved) \
                     — {} available out of {} (dynamic limit)",
                    additional,
                    reservation.consumer().name(),
                    reservation.size(),
                    limit.saturating_sub(used),
                    limit,
                ))
            })?;
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;

    #[test]
    fn test_initial_limit() {
        let (pool, handle) = DynamicLimitPool::new(1024);
        assert_eq!(pool.limit(), 1024);
        assert_eq!(handle.limit(), 1024);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_set_limit() {
        let (_pool, handle) = DynamicLimitPool::new(1024);
        handle.set_limit(2048);
        assert_eq!(handle.limit(), 2048);
    }

    #[test]
    fn test_try_grow_within_limit() {
        let (pool, _handle) = DynamicLimitPool::new(1024);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);
        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 512);
    }

    #[test]
    fn test_try_grow_exceeds_limit() {
        let (pool, _handle) = DynamicLimitPool::new(1024);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);
        assert!(reservation.try_grow(2048).is_err());
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_dynamic_limit_increase() {
        let (pool, handle) = DynamicLimitPool::new(1024);
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
        let (pool, handle) = DynamicLimitPool::new(4096);
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
        let (pool, _handle) = DynamicLimitPool::new(usize::MAX);
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
        let (pool, _handle) = DynamicLimitPool::new(1024);
        // `grow` is infallible accounting — a buggy caller must not be able to
        // wrap `used` back to zero by passing `usize::MAX`. `saturating_add`
        // pins `used` at `usize::MAX` instead.
        let consumer = MemoryConsumer::new("test");
        let reservation = consumer.register(&pool);
        MemoryPool::grow(&pool, &reservation, usize::MAX);
        assert_eq!(pool.reserved(), usize::MAX);
        MemoryPool::grow(&pool, &reservation, 1);
        assert_eq!(pool.reserved(), usize::MAX);
    }

    #[test]
    fn test_concurrent_set_limit_observed_by_try_grow() {
        use std::sync::Barrier;
        use std::thread;

        // Repeat to give the race a chance to surface.
        for _ in 0..64 {
            let (pool, handle) = DynamicLimitPool::new(1024);
            let pool = Arc::new(pool);
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
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let consumer = MemoryConsumer::new("race");
                    let mut reservation = consumer.register(&pool);
                    // Retry a bounded number of times — once `set_limit` lands,
                    // try_grow must observe it because the limit is loaded inside
                    // the fetch_update closure.
                    let mut last = reservation.try_grow(2048);
                    for _ in 0..1000 {
                        if last.is_ok() {
                            break;
                        }
                        last = reservation.try_grow(2048);
                    }
                    last.expect("set_limit raise should eventually be observed by try_grow");
                })
            };

            raiser.join().unwrap();
            allocator.join().unwrap();
        }
    }
}
