/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Memory pool for tracking native memory usage across write and merge operations.
//!
//! Provides an atomic counter with a configurable limit. Operations that allocate
//! significant memory call `try_grow` before allocating and `shrink` after freeing.
//! The pool rejects allocations that would exceed the configured limit.
//!
//! `MemoryReservation` is an RAII handle that automatically returns memory to the
//! pool on drop, preventing leaks even on error paths.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use std::fmt;

/// Default timeout for blocking wait (300 seconds).
pub const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(300);

/// Merge operations can wait longer (600 seconds).
pub const MERGE_WAIT_TIMEOUT: Duration = Duration::from_secs(600);

/// Controls whether an allocation blocks or rejects immediately.
#[derive(Debug, Clone)]
pub enum PoolBehavior {
    /// Block until memory is available, up to the given timeout.
    Wait(Duration),
    /// Fail immediately if pool is full.
    Reject,
}

/// Error returned when a pool cannot satisfy an allocation request.
#[derive(Debug, Clone)]
pub struct PoolExhausted {
    pub pool_name: &'static str,
    pub requested: usize,
    pub used: usize,
    pub limit: usize,
}

impl fmt::Display for PoolExhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] memory limit exceeded: requested {} bytes, used {}, limit {}",
            self.pool_name, self.requested, self.used, self.limit
        )
    }
}

impl std::error::Error for PoolExhausted {}

/// Error returned when wait_and_grow times out.
#[derive(Debug, Clone)]
pub struct PoolTimeout {
    pub pool_name: &'static str,
    pub requested: usize,
    pub used: usize,
    pub limit: usize,
    pub waited: Duration,
}

impl fmt::Display for PoolTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] timed out waiting for {} bytes after {:?} (used: {}, limit: {})",
            self.pool_name, self.requested, self.waited, self.used, self.limit
        )
    }
}

impl std::error::Error for PoolTimeout {}

/// A node-level memory pool backed by an atomic counter with blocking wait support.
pub struct MemoryPool {
    name: &'static str,
    used: AtomicUsize,
    limit: AtomicUsize,
    peak: AtomicUsize,
    notify: Condvar,
    notify_lock: Mutex<()>,
}

impl fmt::Debug for MemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryPool")
            .field("name", &self.name)
            .field("used", &self.used.load(Ordering::Relaxed))
            .field("limit", &self.limit.load(Ordering::Relaxed))
            .field("peak", &self.peak.load(Ordering::Relaxed))
            .finish()
    }
}

impl MemoryPool {
    /// Create a new pool. `limit = 0` means unlimited.
    pub fn new(name: &'static str, limit: usize) -> Self {
        Self {
            name,
            used: AtomicUsize::new(0),
            limit: AtomicUsize::new(limit),
            peak: AtomicUsize::new(0),
            notify: Condvar::new(),
            notify_lock: Mutex::new(()),
        }
    }

    /// Attempt to reserve `bytes`. Returns error if it would exceed the limit.
    pub fn try_grow(&self, bytes: usize) -> Result<(), PoolExhausted> {
        if bytes == 0 {
            return Ok(());
        }
        let limit = self.limit.load(Ordering::Relaxed);
        let result = self.used.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
            let new_used = used.checked_add(bytes)?;
            if limit > 0 && new_used > limit {
                None
            } else {
                Some(new_used)
            }
        });

        match result {
            Ok(old) => {
                self.peak.fetch_max(old + bytes, Ordering::Relaxed);
                Ok(())
            }
            Err(_) => Err(PoolExhausted {
                pool_name: self.name,
                requested: bytes,
                used: self.used.load(Ordering::Relaxed),
                limit,
            }),
        }
    }

    /// Blocks until `bytes` can be reserved, or timeout expires.
    pub fn wait_and_grow(&self, bytes: usize, timeout: Duration) -> Result<(), PoolTimeout> {
        if bytes == 0 {
            return Ok(());
        }
        if self.try_grow(bytes).is_ok() {
            return Ok(());
        }

        let start = std::time::Instant::now();
        loop {
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                let used = self.used.load(Ordering::Relaxed);
                let limit = self.limit.load(Ordering::Relaxed);
                return Err(PoolTimeout {
                    pool_name: self.name,
                    requested: bytes,
                    used,
                    limit,
                    waited: elapsed,
                });
            }

            let remaining = timeout - elapsed;
            let guard = self.notify_lock.lock().unwrap();
            let _ = self.notify.wait_timeout(guard, remaining.min(Duration::from_secs(1))).unwrap();

            if self.try_grow(bytes).is_ok() {
                return Ok(());
            }
        }
    }

    /// Infallible grow — use when the allocation has already happened.
    pub fn grow(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let new_used = self.used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(new_used, Ordering::Relaxed);
    }

    /// Release `bytes` back to the pool. Notifies any waiting threads.
    pub fn shrink(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            })
            .unwrap();
        self.notify.notify_all();
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Atomically update the limit. Called by the Java rebalancer.
    pub fn set_limit(&self, new_limit: usize) {
        self.limit.store(new_limit, Ordering::Release);
        // Wake waiters — new limit might allow blocked allocations
        self.notify.notify_all();
    }
}

/// RAII handle that tracks a portion of memory reserved from a [`MemoryPool`].
/// Automatically releases all held memory on drop.
pub struct MemoryReservation {
    pool: Arc<MemoryPool>,
    consumer: &'static str,
    size: usize,
    behavior: PoolBehavior,
}

impl MemoryReservation {
    pub fn new(pool: &Arc<MemoryPool>, consumer: &'static str, behavior: PoolBehavior) -> Self {
        Self {
            pool: Arc::clone(pool),
            consumer,
            size: 0,
            behavior,
        }
    }

    /// Grow based on the reservation's behavior: block (Wait) or reject (Reject).
    pub fn request(&mut self, bytes: usize) -> Result<(), Box<dyn std::error::Error>> {
        match &self.behavior {
            PoolBehavior::Reject => {
                self.pool.try_grow(bytes)?;
                self.size += bytes;
                Ok(())
            }
            PoolBehavior::Wait(timeout) => {
                self.pool.wait_and_grow(bytes, *timeout)?;
                self.size += bytes;
                Ok(())
            }
        }
    }

    /// Infallible grow.
    pub fn grow(&mut self, bytes: usize) {
        self.pool.grow(bytes);
        self.size += bytes;
    }

    /// Release `bytes` from this reservation.
    pub fn shrink(&mut self, bytes: usize) {
        let actual = bytes.min(self.size);
        self.pool.shrink(actual);
        self.size -= actual;
    }

    /// Release all memory back to the pool.
    pub fn free(&mut self) -> usize {
        let s = self.size;
        if s > 0 {
            self.pool.shrink(s);
            self.size = 0;
        }
        s
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn consumer(&self) -> &'static str {
        self.consumer
    }

    /// Create a sibling reservation from the same pool with a different consumer name.
    pub fn child(&self, consumer: &'static str) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
            consumer,
            size: 0,
            behavior: self.behavior.clone(),
        }
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if self.size > 0 {
            self.pool.shrink(self.size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_grow_within_limit() {
        let pool = Arc::new(MemoryPool::new("test", 1024));
        let mut res = MemoryReservation::new(&pool, "writer", PoolBehavior::Reject);
        assert!(res.request(512).is_ok());
        assert_eq!(res.size(), 512);
        assert_eq!(pool.used(), 512);
    }

    #[test]
    fn test_try_grow_exceeds_limit() {
        let pool = Arc::new(MemoryPool::new("test", 1024));
        let mut res = MemoryReservation::new(&pool, "writer", PoolBehavior::Reject);
        assert!(res.request(2048).is_err());
        assert_eq!(res.size(), 0);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_drop_releases_memory() {
        let pool = Arc::new(MemoryPool::new("test", 1024));
        {
            let mut res = MemoryReservation::new(&pool, "writer", PoolBehavior::Reject);
            res.request(500).unwrap();
            assert_eq!(pool.used(), 500);
        }
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_set_limit_allows_growth() {
        let pool = Arc::new(MemoryPool::new("test", 100));
        let mut res = MemoryReservation::new(&pool, "writer", PoolBehavior::Reject);
        assert!(res.request(200).is_err());
        pool.set_limit(500);
        assert!(res.request(200).is_ok());
    }

    #[test]
    fn test_peak_tracking() {
        let pool = Arc::new(MemoryPool::new("test", 1024));
        let mut res = MemoryReservation::new(&pool, "writer", PoolBehavior::Reject);
        res.request(800).unwrap();
        res.shrink(500);
        assert_eq!(pool.peak(), 800);
        assert_eq!(pool.used(), 300);
    }

    #[test]
    fn test_child_reservation() {
        let pool = Arc::new(MemoryPool::new("test", 1024));
        let res = MemoryReservation::new(&pool, "parent", PoolBehavior::Reject);
        let mut child = res.child("child");
        child.request(100).unwrap();
        assert_eq!(child.consumer(), "child");
        assert_eq!(pool.used(), 100);
    }
}
