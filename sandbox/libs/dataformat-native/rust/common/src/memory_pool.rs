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

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

/// Default timeout for blocking wait (300 seconds).
pub const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(300);

/// Merge operations can wait longer (600 seconds).
pub const MERGE_WAIT_TIMEOUT: Duration = Duration::from_secs(600);

/// Controls how a reservation reacts when the pool is full.
#[derive(Debug, Clone)]
pub enum PoolBehavior {
    /// Block until memory is available, up to the given timeout.
    Wait(Duration),
    /// Fail immediately if the pool is full, unless the registered over-commit decider (the Java-side
    /// allocator) grants an over-commit based on node-level native memory pressure. When granted, the
    /// reservation over-commits via infallible `grow` and the decider is told to release when the
    /// reservation is freed/dropped. Falls back to a plain reject when no decider is registered or the
    /// decider declines.
    Reject,
    /// Never block and never fail: account via infallible `grow`, allowing the pool to over-commit.
    IgnoreLimit,
}

// ─────────────────────────────────────────────────────────────────────────────
// Over-commit decider hook.
//
// The decision "may this reservation over-commit its pool right now?" is owned by the Java-side
// `ArrowNativeAllocator` (it knows node-level native memory pressure, permits, and the feature
// flag). Java registers two C-ABI callbacks via FFM upcall stubs. Because all native modules are
// linked into a single cdylib, these statics are a single shared instance and one registration
// covers every pool that uses `Reject`.
// ─────────────────────────────────────────────────────────────────────────────

/// Decider: returns a nonzero token id to grant an over-commit, or 0 to reject. The token is an
/// opaque handle minted by the Java allocator; Rust stores it and echoes it back on release.
pub type OverCommitDecider = extern "C" fn() -> i64;
/// Releaser: called with the token of a reservation that held an over-commit permit when it is
/// freed/dropped, so the Java allocator can release exactly that grant.
pub type OverCommitReleaser = extern "C" fn(i64);

static OVERCOMMIT_DECIDER: OnceLock<OverCommitDecider> = OnceLock::new();
static OVERCOMMIT_RELEASER: OnceLock<OverCommitReleaser> = OnceLock::new();

/// Registers the over-commit decision callbacks. Idempotent — first registration wins.
pub fn set_overcommit_callbacks(decider: OverCommitDecider, releaser: OverCommitReleaser) {
    let _ = OVERCOMMIT_DECIDER.set(decider);
    let _ = OVERCOMMIT_RELEASER.set(releaser);
}

/// Asks the registered decider whether an over-commit may proceed. Returns the granted token id
/// (nonzero), or 0 when no decider is registered (feature effectively off) or the decider declines.
fn overcommit_grant() -> i64 {
    match OVERCOMMIT_DECIDER.get() {
        Some(decide) => decide(),
        None => 0,
    }
}

/// Returns the over-commit permit identified by `token` back to the decider (Java allocator).
fn overcommit_release(token: i64) {
    if let Some(release) = OVERCOMMIT_RELEASER.get() {
        release(token);
    }
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
        let result = self
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
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
            let _ = self
                .notify
                .wait_timeout(guard, remaining.min(Duration::from_secs(1)))
                .unwrap();

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
    /// Nonzero token of the over-commit permit held by this reservation (0 = none). The permit is
    /// returned to the decider (with this token) when the reservation is freed/dropped (one grant ↔
    /// one release).
    overcommit_token: i64,
}

impl MemoryReservation {
    pub fn new(pool: &Arc<MemoryPool>, consumer: &'static str, behavior: PoolBehavior) -> Self {
        Self {
            pool: Arc::clone(pool),
            consumer,
            size: 0,
            behavior,
            overcommit_token: 0,
        }
    }

    /// Grow based on the reservation's behavior: block (Wait) or reject (Reject).
    pub fn request(&mut self, bytes: usize) -> Result<(), Box<dyn std::error::Error>> {
        match &self.behavior {
            PoolBehavior::Reject => {
                match self.pool.try_grow(bytes) {
                    Ok(()) => {
                        self.size += bytes;
                        Ok(())
                    }
                    Err(exhausted) => {
                        if self.overcommit_token != 0 {
                            // Already holding an over-commit permit for this reservation — continue
                            // over-committing without re-consulting the decider.
                            self.pool.grow(bytes);
                            self.size += bytes;
                            Ok(())
                        } else {
                            let token = overcommit_grant();
                            if token != 0 {
                                // Decider (Java allocator) granted based on node-level native pressure.
                                self.pool.grow(bytes); // infallible over-commit
                                self.size += bytes;
                                self.overcommit_token = token;
                                crate::log_debug!(
                                    "[RUST] over-commit granted: pool '{}' consumer '{}' token {} committing {} bytes beyond limit (reservation size now {})",
                                    self.pool.name(),
                                    self.consumer,
                                    token,
                                    bytes,
                                    self.size
                                );
                                Ok(())
                            } else {
                                crate::log_debug!(
                                    "[RUST] over-commit refused by decider: pool '{}' consumer '{}' needed {} bytes beyond limit",
                                    self.pool.name(),
                                    self.consumer,
                                    bytes
                                );
                                Err(Box::new(exhausted))
                            }
                        }
                    }
                }
            }
            PoolBehavior::Wait(timeout) => {
                self.pool.wait_and_grow(bytes, *timeout)?;
                self.size += bytes;
                Ok(())
            }
            PoolBehavior::IgnoreLimit => {
                self.pool.grow(bytes);
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

    /// Reserve an estimated amount. Returns the estimated amount for later use with `reconcile()`.
    pub fn reserve_estimated(
        &mut self,
        estimated: usize,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        self.request(estimated)?;
        Ok(estimated)
    }

    /// Reconcile a previous estimate with the actual measured size.
    /// If actual > estimated: infallible grow for the delta.
    /// If actual < estimated: shrink the excess.
    pub fn reconcile(&mut self, estimated: usize, actual: usize) {
        if actual > estimated {
            self.grow(actual - estimated);
        } else if actual < estimated {
            self.shrink(estimated - actual);
        }
    }

    /// Returns a reference to the underlying pool.
    pub fn pool(&self) -> &Arc<MemoryPool> {
        &self.pool
    }

    /// Release all memory back to the pool.
    pub fn free(&mut self) -> usize {
        let s = self.size;
        if s > 0 {
            self.pool.shrink(s);
            self.size = 0;
        }
        if self.overcommit_token != 0 {
            overcommit_release(self.overcommit_token);
            crate::log_debug!(
                "[RUST] over-commit permit released: pool '{}' consumer '{}' token {}",
                self.pool.name(),
                self.consumer,
                self.overcommit_token
            );
            self.overcommit_token = 0;
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
            overcommit_token: 0,
        }
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if self.size > 0 {
            self.pool.shrink(self.size);
        }
        if self.overcommit_token != 0 {
            overcommit_release(self.overcommit_token);
            crate::log_debug!(
                "[RUST] over-commit permit released on drop: pool '{}' consumer '{}' token {}",
                self.pool.name(),
                self.consumer,
                self.overcommit_token
            );
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

    #[test]
    fn test_ignore_limit_exceeds_limit() {
        let pool = Arc::new(MemoryPool::new("test", 100));
        let mut res = MemoryReservation::new(&pool, "writer", PoolBehavior::IgnoreLimit);
        // Request far beyond the limit — must succeed and over-commit.
        assert!(res.request(500).is_ok());
        assert_eq!(res.size(), 500);
        assert_eq!(pool.used(), 500);
        assert!(pool.used() > pool.limit());
    }

    #[test]
    fn test_ignore_limit_never_fails_when_already_over() {
        let pool = Arc::new(MemoryPool::new("test", 100));
        let mut res = MemoryReservation::new(&pool, "writer", PoolBehavior::IgnoreLimit);
        res.request(1000).unwrap();
        // Already over the limit; further requests still succeed immediately.
        assert!(res.request(1000).is_ok());
        assert_eq!(pool.used(), 2000);
    }

    #[test]
    fn test_ignore_limit_drop_releases() {
        let pool = Arc::new(MemoryPool::new("test", 100));
        {
            let mut res = MemoryReservation::new(&pool, "writer", PoolBehavior::IgnoreLimit);
            res.request(500).unwrap();
            assert_eq!(pool.used(), 500);
        }
        assert_eq!(pool.used(), 0);
    }
}
