/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use datafusion::common::DataFusionError;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;


/// Wrapper around MonitoredMemoryPool providing access to memory monitoring capabilities.
#[derive(Debug)]
pub struct CustomMemoryPool {
    memory_pool: Arc<MonitoredMemoryPool>
}

impl CustomMemoryPool {
    pub fn new(memory_pool: Arc<MonitoredMemoryPool>) -> Self {
        Self { memory_pool }
    }

    pub fn get_monitor(&self) -> Arc<Monitor> {
        self.memory_pool.get_monitor()
    }

    pub fn get_memory_pool(&self) -> Arc<dyn MemoryPool> {
        self.memory_pool.clone()
    }
}

/// Tracks current and peak memory usage atomically.
#[derive(Debug, Default)]
pub(crate) struct Monitor {
    pub(crate) value: AtomicUsize,
    pub(crate) max: AtomicUsize,
}

impl Monitor {
    pub(crate) fn max(&self) -> usize {
        self.max.load(Ordering::Relaxed)
    }

    fn grow(&self, amount: usize) {
        let old = self.value.fetch_add(amount, Ordering::Relaxed);
        self.max.fetch_max(old + amount, Ordering::Relaxed);
    }

    fn shrink(&self, amount: usize) {
        self.value.fetch_sub(amount, Ordering::Relaxed);
    }

    pub(crate) fn get_current_val(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }
}

/// A MemoryPool that supports changing its limit at runtime.
///
/// Unlike DataFusion's GreedyMemoryPool (which stores pool_size as a plain usize
/// set once in the constructor with no setter), DynamicLimitPool uses an AtomicUsize
/// for the limit, allowing it to be changed at any time via the shared limit handle.
///
/// Behavior:
/// - Increasing the limit takes effect immediately for new allocations.
/// - Decreasing the limit takes effect for new allocations only.
///   Existing reservations that exceed the new limit are NOT reclaimed.
///   They remain until the consumer frees them (e.g., query completes).
#[derive(Debug)]
pub struct DynamicLimitPool {
    used: AtomicUsize,
    dynamic_limit: Arc<AtomicUsize>,
}

/// Handle to change the pool limit at runtime.
/// Can be stored separately from the pool itself.
#[derive(Debug, Clone)]
pub struct DynamicLimitHandle {
    limit: Arc<AtomicUsize>,
}

impl DynamicLimitHandle {
    pub fn set_limit(&self, new_limit: usize) {
        self.limit.store(new_limit, Ordering::SeqCst);
    }

    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::SeqCst)
    }
}

impl DynamicLimitPool {
    pub fn new(initial_limit: usize) -> (Self, DynamicLimitHandle) {
        let limit = Arc::new(AtomicUsize::new(initial_limit));
        let handle = DynamicLimitHandle { limit: limit.clone() };
        let pool = Self {
            used: AtomicUsize::new(0),
            dynamic_limit: limit,
        };
        (pool, handle)
    }

    pub fn limit(&self) -> usize {
        self.dynamic_limit.load(Ordering::SeqCst)
    }
}

impl MemoryPool for DynamicLimitPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let limit = self.dynamic_limit.load(Ordering::SeqCst);
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let new_used = used + additional;
                (new_used <= limit).then_some(new_used)
            })
            .map_err(|used| {
                DataFusionError::ResourcesExhausted(format!(
                    "Failed to allocate additional {} for {} with {} already allocated \
                     for this reservation - {} remain available for the total pool (dynamic limit: {})",
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

/// MemoryPool implementation that wraps another pool and tracks memory usage via Monitor.
#[derive(Debug)]
pub struct MonitoredMemoryPool {
    inner: Arc<dyn MemoryPool>,
    monitor: Arc<Monitor>,
}

impl MonitoredMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, monitor: Arc<Monitor>) -> Self {
        Self { inner, monitor }
    }

    pub fn get_monitor(&self) -> Arc<Monitor> {
        self.monitor.clone()
    }
}

impl MemoryPool for MonitoredMemoryPool {
    fn register(&self, _consumer: &MemoryConsumer) {
        self.inner.register(_consumer)
    }

    fn unregister(&self, _consumer: &MemoryConsumer) {
        self.inner.unregister(_consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.monitor.grow(additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.monitor.shrink(shrink);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.monitor.grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};

    #[test]
    fn test_dynamic_limit_pool_initial_limit() {
        let (pool, handle) = DynamicLimitPool::new(1024);
        assert_eq!(handle.limit(), 1024);
        assert_eq!(pool.limit(), 1024);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_dynamic_limit_pool_try_grow_within_limit() {
        let (pool, _handle) = DynamicLimitPool::new(1024);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);
        assert!(reservation.try_grow(512).is_ok());
        assert_eq!(pool.reserved(), 512);
    }

    #[test]
    fn test_dynamic_limit_pool_try_grow_exceeds_limit() {
        let (pool, _handle) = DynamicLimitPool::new(1024);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);
        assert!(reservation.try_grow(2048).is_err());
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_dynamic_limit_pool_shrink() {
        let (pool, _handle) = DynamicLimitPool::new(1024);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);
        reservation.try_grow(512).unwrap();
        assert_eq!(pool.reserved(), 512);
        reservation.shrink(256);
        assert_eq!(pool.reserved(), 256);
    }

    #[test]
    fn test_dynamic_limit_handle_set_limit_increase() {
        let (pool, handle) = DynamicLimitPool::new(1024);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);

        // Fill to near limit
        reservation.try_grow(900).unwrap();
        // Can't grow beyond original limit
        assert!(reservation.try_grow(200).is_err());

        // Increase limit
        handle.set_limit(2048);
        assert_eq!(handle.limit(), 2048);

        // Now the grow succeeds
        assert!(reservation.try_grow(200).is_ok());
        assert_eq!(pool.reserved(), 1100);
    }

    #[test]
    fn test_dynamic_limit_handle_set_limit_decrease() {
        let (pool, handle) = DynamicLimitPool::new(2048);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);

        // Allocate 1500 bytes
        reservation.try_grow(1500).unwrap();
        assert_eq!(pool.reserved(), 1500);

        // Decrease limit below current usage
        handle.set_limit(1024);
        assert_eq!(handle.limit(), 1024);

        // Existing reservation is NOT reclaimed
        assert_eq!(pool.reserved(), 1500);

        // But new allocations fail
        assert!(reservation.try_grow(100).is_err());

        // After releasing some memory, new allocations within new limit succeed
        reservation.shrink(600); // reserved = 900, limit = 1024
        assert_eq!(pool.reserved(), 900);
        assert!(reservation.try_grow(100).is_ok()); // 900 + 100 = 1000 <= 1024
    }

    #[test]
    fn test_dynamic_limit_pool_grow_bypasses_limit() {
        let (pool, _handle) = DynamicLimitPool::new(1024);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool);

        // grow() does NOT check the limit (same as GreedyMemoryPool)
        reservation.grow(2048);
        assert_eq!(pool.reserved(), 2048); // exceeds limit, but grow allows it
    }

    #[test]
    fn test_monitor_tracks_current_and_peak() {
        let monitor = Monitor::default();
        assert_eq!(monitor.get_current_val(), 0);
        assert_eq!(monitor.max(), 0);

        monitor.grow(500);
        assert_eq!(monitor.get_current_val(), 500);
        assert_eq!(monitor.max(), 500);

        monitor.grow(300);
        assert_eq!(monitor.get_current_val(), 800);
        assert_eq!(monitor.max(), 800);

        monitor.shrink(600);
        assert_eq!(monitor.get_current_val(), 200);
        assert_eq!(monitor.max(), 800); // peak unchanged

        monitor.grow(900);
        assert_eq!(monitor.get_current_val(), 1100);
        assert_eq!(monitor.max(), 1100); // new peak
    }

    #[test]
    fn test_monitored_memory_pool_delegates_and_tracks() {
        let (pool, handle) = DynamicLimitPool::new(4096);
        let monitor = Arc::new(Monitor::default());
        let monitored = Arc::new(MonitoredMemoryPool::new(
            Arc::new(pool),
            monitor.clone(),
        ));

        let pool_ref: Arc<dyn MemoryPool> = monitored;
        let consumer = MemoryConsumer::new("test");
        let mut reservation = consumer.register(&pool_ref);

        reservation.try_grow(1000).unwrap();
        assert_eq!(monitor.get_current_val(), 1000);
        assert_eq!(pool_ref.reserved(), 1000);

        reservation.shrink(400);
        assert_eq!(monitor.get_current_val(), 600);
        assert_eq!(pool_ref.reserved(), 600);

        // Change limit via handle and verify it takes effect
        handle.set_limit(700);
        assert!(reservation.try_grow(200).is_err()); // 600 + 200 > 700
        assert!(reservation.try_grow(50).is_ok());    // 600 + 50 = 650 <= 700
    }
}
