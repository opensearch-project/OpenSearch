/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Tests for memory pool backpressure, timeout, and deadlock prevention.

use std::sync::Arc;
use std::time::{Duration, Instant};

use native_bridge_common::memory_pool::{MemoryPool, MemoryReservation, PoolBehavior};

/// Request times out cleanly when pool is full.
#[test]
fn test_request_rejected_when_pool_full() {
    let pool = Arc::new(MemoryPool::new("test", 1_000));
    let mut reservation = MemoryReservation::new(
        &pool,
        "requester",
        PoolBehavior::Wait(Duration::from_secs(1)),
    );

    // Pre-fill pool to near capacity
    reservation.grow(999);
    assert_eq!(pool.used(), 999);

    // Request more than available — should timeout
    let start = Instant::now();
    let result = reservation.request(500);
    let elapsed = start.elapsed();

    assert!(result.is_err(), "Expected timeout error");
    assert!(
        elapsed >= Duration::from_millis(900),
        "Should have waited ~1s, got {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(2_000),
        "Should not hang, got {:?}",
        elapsed
    );
    // Pool unchanged — failed request didn't partially allocate
    assert_eq!(pool.used(), 999);
}

/// Request blocks and succeeds when another thread frees memory.
#[test]
fn test_request_waits_and_succeeds() {
    let pool = Arc::new(MemoryPool::new("test", 10_000));

    // Blocker fills pool
    let mut blocker =
        MemoryReservation::new(&pool, "blocker", PoolBehavior::Wait(Duration::from_secs(1)));
    blocker.grow(9_500);
    assert_eq!(pool.used(), 9_500);

    // Thread frees after 500ms
    let pool_clone = Arc::clone(&pool);
    let handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(500));
        pool_clone.shrink(9_500);
    });

    // Writer requests — should block then succeed after space is freed
    let mut writer =
        MemoryReservation::new(&pool, "writer", PoolBehavior::Wait(Duration::from_secs(5)));
    let start = Instant::now();
    let result = writer.request(5_000);
    let elapsed = start.elapsed();

    handle.join().unwrap();

    assert!(result.is_ok(), "Expected success after blocker freed");
    assert!(
        elapsed >= Duration::from_millis(400),
        "Should have waited ~500ms, got {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(2_000),
        "Should not wait too long, got {:?}",
        elapsed
    );
    // Writer holds its allocation
    assert_eq!(writer.size(), 5_000);
}

/// Request blocks for the full timeout duration then fails.
#[test]
fn test_request_waits_and_times_out() {
    let pool = Arc::new(MemoryPool::new("test", 1_000));

    let mut blocker =
        MemoryReservation::new(&pool, "blocker", PoolBehavior::Wait(Duration::from_secs(1)));
    blocker.grow(999);

    let mut writer =
        MemoryReservation::new(&pool, "writer", PoolBehavior::Wait(Duration::from_secs(2)));

    let start = Instant::now();
    let result = writer.request(500);
    let elapsed = start.elapsed();

    assert!(result.is_err(), "Expected timeout");
    assert!(
        elapsed >= Duration::from_secs(2),
        "Should wait full timeout, got {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "Should not hang forever, got {:?}",
        elapsed
    );
    assert_eq!(pool.used(), 999); // blocker unchanged
}

/// Two concurrent requesters on the same pool — one waits for the other, no deadlock.
#[test]
fn test_no_deadlock_two_requesters_same_pool() {
    let pool = Arc::new(MemoryPool::new("test", 5_000));

    let start = Instant::now();

    std::thread::scope(|s| {
        let p1 = Arc::clone(&pool);
        let p2 = Arc::clone(&pool);

        // Thread 1: grabs 3000, holds for 500ms, releases
        s.spawn(move || {
            let mut r =
                MemoryReservation::new(&p1, "t1", PoolBehavior::Wait(Duration::from_secs(10)));
            r.request(3_000).expect("t1 request should succeed");
            std::thread::sleep(Duration::from_millis(500));
            r.shrink(3_000);
        });

        // Thread 2: waits 100ms (ensure t1 gets in first), then requests 3000
        s.spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            let mut r =
                MemoryReservation::new(&p2, "t2", PoolBehavior::Wait(Duration::from_secs(10)));
            // This blocks until t1 releases (pool: 3000/5000, needs 3000 more = 6000 > 5000)
            r.request(3_000)
                .expect("t2 request should succeed after t1 releases");
            r.shrink(3_000);
        });
    });

    let elapsed = start.elapsed();

    assert_eq!(pool.used(), 0, "Pool should be empty after both complete");
    assert!(
        elapsed < Duration::from_secs(5),
        "Should complete within 5s (no deadlock), got {:?}",
        elapsed
    );
}

/// Reservation that partially succeeded then failed on a subsequent request
/// frees all its memory when dropped.
#[test]
fn test_reservation_drop_frees_all_on_partial_failure() {
    let pool = Arc::new(MemoryPool::new("test", 10_000));

    {
        let mut reservation =
            MemoryReservation::new(&pool, "res", PoolBehavior::Wait(Duration::from_secs(1)));

        // First allocation succeeds
        let r1 = reservation.request(3_000);
        assert!(r1.is_ok());
        assert_eq!(pool.used(), 3_000);
        assert_eq!(reservation.size(), 3_000);

        // Infallible grow also succeeds
        reservation.grow(2_000);
        assert_eq!(pool.used(), 5_000);
        assert_eq!(reservation.size(), 5_000);

        // Second request fails — needs 8000 more, total would be 13000 > 10000
        let r2 = reservation.request(8_000);
        assert!(r2.is_err());
        assert_eq!(pool.used(), 5_000); // unchanged by failed request
        assert_eq!(reservation.size(), 5_000); // unchanged

        // reservation drops here
    }

    // After drop — all previously allocated memory is freed
    assert_eq!(pool.used(), 0, "Drop should free all tracked memory");
}
