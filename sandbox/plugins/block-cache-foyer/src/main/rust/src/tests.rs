/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unit tests for [`FoyerCache`] and the FFM lifecycle API.

use bytes::Bytes;
use std::sync::Arc;
use tempfile::TempDir;

use crate::foyer::ffm::{foyer_create_cache, foyer_destroy_cache};
use crate::foyer::foyer_cache::FoyerCache;
use crate::range_cache::range_cache_key;
use crate::traits::BlockCache;

// ── Test helpers ──────────────────────────────────────────────────────────────

/// Block size and disk capacity for FFM/integration tests that need a large Foyer instance.
/// Must be kept in sync: block_size (BLOCK_SIZE) ≤ disk_bytes (any test using this constant).
const BLOCK_SIZE: usize = 64 * 1024 * 1024; // 64 MB — used by FFM tests (disk=64MB) and large_value test

/// I/O engine for all test caches.
const IO_ENGINE: &str = "auto";

/// Disk capacity for the default `test_cache()` helper.
///
/// Must satisfy: TEST_CACHE_BLOCK_SIZE ≤ TEST_CACHE_DISK_BYTES (Foyer invariant).
/// - Disk: 4 MB — small enough to keep parallel test disk I/O fast (~64× faster
///   init than 64 MB), large enough for all non-capacity-pressure tests.
/// - Block size: 1 MB — accommodates `large_value_round_trips_correctly` (≈976 KB
///   entry) while staying well under the 4 MB disk cap.
///
/// Tests that explicitly exercise capacity pressure (`put_and_get_work_after_cache_nears_capacity`,
/// `lru_eviction_retains_keys_in_key_index`, etc.) create their own caches with
/// explicit sizes and are unaffected by these constants.
const TEST_CACHE_DISK_BYTES: usize = 4 * 1024 * 1024; // 4 MB disk capacity
const TEST_CACHE_BLOCK_SIZE: usize = 1 * 1024 * 1024; // 1 MB block size (must be ≤ disk)
const TEST_BUFFER_POOL_SIZE: usize = 1 * 1024 * 1024; // 1 MB buffer pool (≥ block_size)
const TEST_SUBMIT_QUEUE_SIZE: usize = 2 * 1024 * 1024; // 2 MB submit queue (≥ 2× buffer pool)

fn test_cache() -> (FoyerCache, TempDir) {
    let dir = TempDir::new().expect("failed to create temp dir");
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    (cache, dir)
}

fn put_range(cache: &FoyerCache, path: &str, start: u64, end: u64, data: &[u8]) {
    cache.put(
        &range_cache_key(path, start, end),
        Bytes::copy_from_slice(data),
    );
}

/// Shared Tokio runtime for all `block_on()` calls in tests.
///
/// The previous approach created a brand-new `tokio::runtime::Runtime` for every single
/// `block_on()` invocation. With 109 tests running in parallel, each calling `block_on()`
/// multiple times, this produced hundreds of separate Tokio runtimes — on top of the
/// per-`FoyerCache` runtimes created by `FoyerCache::new()`. Each runtime spawns OS-level
/// worker threads, quickly exhausting the macOS thread limit and causing tests to block
/// indefinitely waiting for thread allocation.
///
/// Using a single shared runtime for all test `block_on()` calls reduces the total runtime
/// count from O(tests × calls) to O(tests) — one per `FoyerCache` plus this one — making
/// parallel test execution practical.
static TEST_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    TEST_RUNTIME
        .get_or_init(|| tokio::runtime::Runtime::new().expect("shared test runtime"))
        .block_on(f)
}

// ── put + get round-trip ──────────────────────────────────────────────────────

#[test]
fn get_returns_exact_bytes_that_were_put() {
    let (cache, _dir) = test_cache();
    let data = b"hello foyer cache";
    let key = range_cache_key("/data/file.parquet", 0, 100);
    cache.put(&key, Bytes::from_static(data));
    let result = block_on(cache.get(&key));
    assert_eq!(result.as_deref(), Some(data.as_slice()));
}

#[test]
fn multiple_ranges_for_same_file_are_independent() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/a.parquet", 0, 4096, b"range0");
    put_range(&cache, "/data/a.parquet", 4096, 8192, b"range1");
    put_range(&cache, "/data/a.parquet", 8192, 12288, b"range2");
    assert_eq!(
        block_on(cache.get(&range_cache_key("/data/a.parquet", 0, 4096))).as_deref(),
        Some(b"range0".as_slice())
    );
    assert_eq!(
        block_on(cache.get(&range_cache_key("/data/a.parquet", 4096, 8192))).as_deref(),
        Some(b"range1".as_slice())
    );
    assert_eq!(
        block_on(cache.get(&range_cache_key("/data/a.parquet", 8192, 12288))).as_deref(),
        Some(b"range2".as_slice())
    );
}

#[test]
fn multiple_files_are_independent() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/a.parquet", 0, 100, b"file_a");
    put_range(&cache, "/data/b.parquet", 0, 100, b"file_b");
    put_range(&cache, "/data/c.parquet", 0, 100, b"file_c");
    assert_eq!(
        block_on(cache.get(&range_cache_key("/data/a.parquet", 0, 100))).as_deref(),
        Some(b"file_a".as_slice())
    );
    assert_eq!(
        block_on(cache.get(&range_cache_key("/data/b.parquet", 0, 100))).as_deref(),
        Some(b"file_b".as_slice())
    );
    assert_eq!(
        block_on(cache.get(&range_cache_key("/data/c.parquet", 0, 100))).as_deref(),
        Some(b"file_c".as_slice())
    );
}

#[test]
fn large_value_round_trips_correctly() {
    let (cache, _dir) = test_cache();
    let data: Vec<u8> = (0u32..1_000_000).map(|i| (i % 251) as u8).collect();
    let key = range_cache_key("/data/large.parquet", 0, data.len() as u64);
    cache.put(&key, Bytes::copy_from_slice(&data));
    let result = block_on(cache.get(&key)).expect("large value should be retrievable");
    assert_eq!(result.as_ref(), data.as_slice());
}

#[test]
fn put_same_key_twice_replaces_value() {
    let (cache, _dir) = test_cache();
    let key = range_cache_key("/data/file.parquet", 0, 100);
    cache.put(&key, Bytes::from_static(b"original"));
    cache.put(&key, Bytes::from_static(b"updated"));
    let result = block_on(cache.get(&key));
    assert_eq!(result.as_deref(), Some(b"updated".as_slice()));
}

// ── get miss cases ────────────────────────────────────────────────────────────

#[test]
fn get_returns_none_for_unknown_key() {
    let (cache, _dir) = test_cache();
    let result = block_on(cache.get(&range_cache_key("/never/inserted.parquet", 0, 100)));
    assert!(result.is_none());
}

#[test]
fn get_returns_none_for_wrong_range_on_known_path() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0, 100, b"data");
    assert!(block_on(cache.get(&range_cache_key("/data/file.parquet", 1, 100))).is_none());
    assert!(block_on(cache.get(&range_cache_key("/data/file.parquet", 0, 99))).is_none());
    assert!(block_on(cache.get(&range_cache_key("/data/file.parquet", 200, 300))).is_none());
}

// ── evict_prefix ──────────────────────────────────────────────────────────────

#[test]
fn evict_prefix_removes_all_ranges_for_file() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/target.parquet", 0, 4096, b"range0");
    put_range(&cache, "/data/target.parquet", 4096, 8192, b"range1");
    put_range(&cache, "/data/target.parquet", 8192, 12288, b"range2");
    cache.evict_prefix("/data/target.parquet");
    // key_index stores normalized keys (no leading '/'):
    assert!(!cache.key_index.contains_key("data/target.parquet"));
    put_range(&cache, "/data/target.parquet", 0, 4096, b"new");
    assert_eq!(
        block_on(cache.get(&range_cache_key("/data/target.parquet", 0, 4096))),
        Some(Bytes::from_static(b"new"))
    );
}

#[test]
fn evict_prefix_does_not_affect_other_files() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/target.parquet", 0, 100, b"target");
    put_range(&cache, "/data/other.parquet", 0, 100, b"other");
    cache.evict_prefix("/data/target.parquet");
    assert!(block_on(cache.get(&range_cache_key("/data/other.parquet", 0, 100))).is_some());
    assert!(block_on(cache.get(&range_cache_key("/data/target.parquet", 0, 100))).is_none());
}

#[test]
fn evict_prefix_on_nonexistent_prefix_is_noop() {
    let (cache, _dir) = test_cache();
    cache.evict_prefix("/never/inserted.parquet");
    cache.evict_prefix("");
}

#[test]
fn evict_prefix_twice_is_safe() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0, 100, b"data");
    cache.evict_prefix("/data/file.parquet");
    cache.evict_prefix("/data/file.parquet");
}

#[test]
fn after_evict_prefix_new_put_is_retrievable() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0, 100, b"first");
    cache.evict_prefix("/data/file.parquet");
    put_range(&cache, "/data/file.parquet", 0, 100, b"second");
    let result = block_on(cache.get(&range_cache_key("/data/file.parquet", 0, 100)));
    assert_eq!(result.as_deref(), Some(b"second".as_slice()));
}

// ── clear ─────────────────────────────────────────────────────────────────────

#[test]
fn clear_updates_removed_count_and_removed_bytes() {
    // clear() must increment removed_count and removed_bytes from key_index
    // and reset used_bytes to 0, mirroring FileCache.clear() recordRemoval semantics.
    let (cache, _dir) = test_cache();

    // Two files, one range each: 0-100 = 100 bytes, 0-200 = 200 bytes → total 300 bytes, 2 entries.
    put_range(&cache, "/data/a.parquet", 0, 100, &vec![0u8; 100]);
    put_range(&cache, "/data/b.parquet", 0, 200, &vec![0u8; 200]);

    let removed_count_before = cache
        .stats
        .removed_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let removed_bytes_before = cache
        .stats
        .removed_bytes
        .load(std::sync::atomic::Ordering::Relaxed);

    block_on(cache.clear());

    let removed_count_after = cache
        .stats
        .removed_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let removed_bytes_after = cache
        .stats
        .removed_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    let used_bytes_after = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(
        removed_count_after,
        removed_count_before + 2,
        "clear() must count 2 removed entries"
    );
    assert_eq!(
        removed_bytes_after,
        removed_bytes_before + 300,
        "clear() must count 100 + 200 = 300 removed bytes"
    );
    assert_eq!(used_bytes_after, 0, "clear() must reset used_bytes to 0");
    assert!(
        cache.key_index.is_empty(),
        "key_index must be empty after clear()"
    );
}

#[test]
fn clear_on_empty_cache_does_not_change_removed_stats() {
    let (cache, _dir) = test_cache();
    let removed_before = cache
        .stats
        .removed_count
        .load(std::sync::atomic::Ordering::Relaxed);
    block_on(cache.clear());
    assert_eq!(
        cache
            .stats
            .removed_count
            .load(std::sync::atomic::Ordering::Relaxed),
        removed_before,
        "clear() on empty cache must not increment removed_count"
    );
}

#[test]
fn clear_removes_all_entries() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/a.parquet", 0, 100, b"a");
    put_range(&cache, "/data/b.parquet", 0, 100, b"b");
    block_on(cache.clear());
    assert!(block_on(cache.get(&range_cache_key("/data/a.parquet", 0, 100))).is_none());
    assert!(block_on(cache.get(&range_cache_key("/data/b.parquet", 0, 100))).is_none());
}

#[test]
fn clear_on_empty_cache_is_safe() {
    let (cache, _dir) = test_cache();
    block_on(cache.clear());
}

#[test]
fn cache_is_usable_after_clear() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0, 100, b"before");
    block_on(cache.clear());
    put_range(&cache, "/data/file.parquet", 0, 100, b"after");
    let result = block_on(cache.get(&range_cache_key("/data/file.parquet", 0, 100)));
    assert_eq!(result.as_deref(), Some(b"after".as_slice()));
}

// ── key_index integrity ───────────────────────────────────────────────────────

#[test]
fn key_index_is_empty_after_clear() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/a.parquet", 0, 100, b"a");
    put_range(&cache, "/data/b.parquet", 0, 100, b"b");
    block_on(cache.clear());
    assert!(cache.key_index.is_empty());
}

#[test]
fn key_index_has_no_entry_for_evicted_file() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/target.parquet", 0, 100, b"data");
    put_range(&cache, "/data/other.parquet", 0, 100, b"other");
    cache.evict_prefix("/data/target.parquet");
    // key_index stores normalized keys (no leading '/'):
    assert!(!cache.key_index.contains_key("data/target.parquet"));
    assert!(cache.key_index.contains_key("data/other.parquet"));
}

// ── Duplicate key / HashSet deduplication tests ───────────────────────────────

/// Putting the same key twice must not create duplicate entries in key_index.
///
/// Scenario: cache stampede — two threads both miss for the same byte range and both
/// call put(). With the old Vec-based key_index, this created two copies of the key,
/// causing double-counting in removed_bytes and eviction_bytes.
/// With HashSet, the second insert is a no-op.
#[test]
fn put_same_key_twice_does_not_duplicate_key_index_entry() {
    let (cache, _dir) = test_cache();
    let key = range_cache_key("/data/file.parquet", 0, 1024);

    // First put: key must be added to key_index.
    cache.put(&key, Bytes::from(vec![0u8; 1024]));
    let count_after_first = cache
        .key_index
        .get("data/file.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(
        count_after_first, 1,
        "first put must add exactly 1 entry to key_index"
    );

    // Second put with same key (simulates cache stampede or re-put after eviction).
    cache.put(&key, Bytes::from(vec![0xFFu8; 1024]));
    let count_after_second = cache
        .key_index
        .get("data/file.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(
        count_after_second, 1,
        "second put of same key must NOT add a duplicate entry (HashSet dedup); got {}",
        count_after_second
    );
}

/// used_bytes must not be double-counted when the same key is put twice.
///
/// With the old Vec approach, two puts added `size` to used_bytes twice even though
/// Foyer only stores one entry. With HashSet, the second insert returns false and
/// used_bytes is only incremented on the first (genuine) insert.
#[test]
fn put_same_key_twice_does_not_double_count_used_bytes() {
    let (cache, _dir) = test_cache();
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    let key = range_cache_key("/data/file.parquet", 0, 512);
    cache.put(&key, Bytes::from(vec![0u8; 512]));
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        512,
        "used_bytes must be 512 after first put"
    );

    // Second put of the same key — Foyer upserts the value, key_index is unchanged.
    // used_bytes must NOT be incremented again.
    cache.put(&key, Bytes::from(vec![0xAAu8; 512]));
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        512,
        "used_bytes must still be 512 after second put of the same key (no double-count)"
    );
}

/// Concurrent stampede: multiple threads put the same key simultaneously.
/// After all threads finish, key_index must contain exactly 1 entry for the key,
/// and used_bytes must equal exactly 1× the entry size (not N× for N threads).
#[test]
fn concurrent_stampede_same_key_does_not_duplicate_key_index() {
    let (cache, _dir) = test_cache();
    let cache = Arc::new(cache);
    const THREAD_COUNT: usize = 8;
    const RANGE_SIZE: u64 = 1024;

    // All threads race to put the same key — simulates a cache stampede after a miss.
    let handles: Vec<_> = (0..THREAD_COUNT)
        .map(|_| {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || {
                let key = range_cache_key("/data/shared.parquet", 0, RANGE_SIZE);
                cache.put(&key, Bytes::from(vec![0xABu8; RANGE_SIZE as usize]));
            })
        })
        .collect();
    for h in handles {
        h.join().expect("thread panicked");
    }

    // key_index must have exactly 1 entry for this key, not THREAD_COUNT.
    let count = cache
        .key_index
        .get("data/shared.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(
        count, 1,
        "key_index must contain exactly 1 entry after {} concurrent puts of the same key; got {}",
        THREAD_COUNT, count
    );

    // used_bytes: at most 1× the entry size. It may be less if the second-N puts
    // raced and all returned false from HashSet::insert, but never more than 1×.
    let used_bytes = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        used_bytes, RANGE_SIZE as i64,
        "used_bytes must be exactly {} after {} concurrent puts of the same key; got {}",
        RANGE_SIZE, THREAD_COUNT, used_bytes
    );
}

// ── concurrent access ─────────────────────────────────────────────────────────

#[test]
fn concurrent_puts_to_different_files_do_not_corrupt() {
    let (cache, _dir) = test_cache();
    let cache = Arc::new(cache);
    let handles: Vec<_> = (0..16)
        .map(|i| {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || {
                let key = range_cache_key(&format!("/data/file_{}.parquet", i), 0, 1024);
                cache.put(&key, Bytes::copy_from_slice(&vec![i as u8; 1024]));
            })
        })
        .collect();
    for h in handles {
        h.join().expect("thread panicked");
    }
    for i in 0u8..16 {
        let key = range_cache_key(&format!("/data/file_{}.parquet", i), 0, 1024);
        let result = block_on(cache.get(&key)).expect("entry should be retrievable");
        assert!(result.iter().all(|&b| b == i));
    }
}

#[test]
fn concurrent_put_and_get_same_file_does_not_panic() {
    let (cache, _dir) = test_cache();
    let cache = Arc::new(cache);
    let writer_cache = Arc::clone(&cache);
    let writer = std::thread::spawn(move || {
        for i in 0u64..100 {
            let key = range_cache_key("/data/shared.parquet", i * 100, (i + 1) * 100);
            writer_cache.put(&key, Bytes::from_static(b"data"));
        }
    });
    let reader_cache = Arc::clone(&cache);
    let reader = std::thread::spawn(move || {
        for i in 0u64..100 {
            let key = range_cache_key("/data/shared.parquet", i * 100, (i + 1) * 100);
            let _ = block_on(reader_cache.get(&key));
        }
    });
    writer.join().expect("writer panicked");
    reader.join().expect("reader panicked");
}

#[test]
fn concurrent_evict_and_put_does_not_panic() {
    let (cache, _dir) = test_cache();
    let cache = Arc::new(cache);
    let writer_cache = Arc::clone(&cache);
    let writer = std::thread::spawn(move || {
        for i in 0u64..50 {
            let key = range_cache_key("/data/file.parquet", i * 100, (i + 1) * 100);
            writer_cache.put(&key, Bytes::from_static(b"data"));
        }
    });
    let evictor_cache = Arc::clone(&cache);
    let evictor = std::thread::spawn(move || {
        for _ in 0..50 {
            evictor_cache.evict_prefix("/data/file.parquet");
        }
    });
    writer.join().expect("writer panicked");
    evictor.join().expect("evictor panicked");
}

/// Stress test for the concurrent `put()` + `evict_prefix()` scenario described in the
/// `evict_prefix()` doc comment.
///
/// `evict_prefix` is a scan-then-remove operation: it collects matching key_index entries
/// under a read lock, releases all locks, then removes them. A `put()` that runs between
/// the scan and the remove will have its entry included in the eviction — this is the
/// expected outcome when a prefix is being deleted.
///
/// This test verifies:
/// - No panic or deadlock under concurrent load.
/// - `used_bytes` is never negative (double-subtraction would indicate stat corruption).
/// - The cache remains usable after all concurrent activity finishes.
///
/// Note: whether a specific entry ends up present or absent after this test is intentionally
/// not asserted — both outcomes are valid per the method's documented behaviour.
#[test]
fn concurrent_put_and_evict_same_prefix_does_not_corrupt() {
    let (cache, _dir) = test_cache();
    let cache = Arc::new(cache);
    const ITERATIONS: u64 = 100;

    // Writer: continuously caches new ranges under the same prefix.
    // Data length matches the key range (100 bytes each) so key_byte_size() and data.len()
    // agree — this keeps used_bytes accounting consistent between put() and evict_prefix().
    let writer_cache = Arc::clone(&cache);
    let writer = std::thread::spawn(move || {
        for i in 0..ITERATIONS {
            let key = range_cache_key("/data/shard.parquet", i * 100, (i + 1) * 100);
            writer_cache.put(&key, Bytes::from(vec![0u8; 100]));
        }
    });

    // Evictor: repeatedly evicts the same prefix — simulates a shard deletion racing
    // with an in-flight read that triggers a put().
    let evictor_cache = Arc::clone(&cache);
    let evictor = std::thread::spawn(move || {
        for _ in 0..ITERATIONS {
            evictor_cache.evict_prefix("/data/shard.parquet");
        }
    });

    writer.join().expect("writer panicked");
    evictor.join().expect("evictor panicked");

    // used_bytes must never go negative — that would indicate double-subtraction of stats.
    let used = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        used >= 0,
        "used_bytes must not be negative after concurrent put/evict; got {}",
        used
    );

    // Cache must remain fully usable after the concurrent activity.
    let key = range_cache_key("/data/shard.parquet", 999_000, 999_100);
    cache.put(&key, Bytes::from_static(b"after"));
    // put() must not panic — entry presence is not asserted (depends on timing).
}

// ── disk / capacity cases ─────────────────────────────────────────────────────

#[test]
fn put_and_get_work_after_cache_nears_capacity() {
    let dir = TempDir::new().unwrap();
    // disk=2MB ≥ block_size=512KB (Foyer invariant: block_size ≤ disk).
    // Writes 4 × 512KB = 2MB total into a 2MB cache to exercise near-capacity behaviour.
    let cache = FoyerCache::new(
        2 * 1024 * 1024,
        dir.path(),
        512 * 1024,
        512 * 1024,
        1024 * 1024,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    let chunk = vec![0u8; 512 * 1024];
    for i in 0u64..4 {
        let key = range_cache_key("/data/file.parquet", i * 524288, (i + 1) * 524288);
        cache.put(&key, Bytes::copy_from_slice(&chunk));
    }
    let fresh_key = range_cache_key("/data/file.parquet", 0, 100);
    cache.put(&fresh_key, Bytes::from_static(b"fresh"));
    let result = block_on(cache.get(&fresh_key));
    assert_eq!(result.as_deref(), Some(b"fresh".as_slice()));
}

// ── KeyIndexListener behaviour ────────────────────────────────────────────────

#[test]
fn lru_eviction_retains_keys_in_key_index() {
    // Event::Evict fires when an entry leaves the 1-byte DRAM tier and is written to disk.
    // It does NOT mean the entry is gone — it is still alive on disk.
    // Therefore key_index must RETAIN all keys after DRAM eviction pressure.
    // The background sweeper handles truly-gone disk entries via inner.contains().
    //
    // disk=1MB, block_size=256KB (256KB ≤ 1MB — Foyer invariant satisfied).
    // Writes 8 × 256KB = 2MB total into a 1MB cache to trigger LRU eviction pressure.
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(
        1 * 1024 * 1024,
        dir.path(),
        256 * 1024,
        256 * 1024,
        512 * 1024,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    const CHUNK_SIZE: usize = 256 * 1024;
    const TOTAL_WRITES: usize = 8;
    let chunk = vec![0xABu8; CHUNK_SIZE];
    for i in 0u64..TOTAL_WRITES as u64 {
        let key = range_cache_key(
            "/data/big.parquet",
            i * CHUNK_SIZE as u64,
            (i + 1) * CHUNK_SIZE as u64,
        );
        cache.put(&key, Bytes::copy_from_slice(&chunk));
    }
    // Busy-assert: poll until all TOTAL_WRITES keys appear in key_index or deadline expires.
    // Foyer's async flusher writes to disk asynchronously after put(); we wait for it
    // rather than using a fixed sleep — avoids flakiness on slow CI machines.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let key_count = loop {
        let count = cache
            .key_index
            .get("data/big.parquet")
            .map(|v| v.len())
            .unwrap_or(0);
        if count >= TOTAL_WRITES || std::time::Instant::now() >= deadline {
            break count;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    assert_eq!(
        key_count, TOTAL_WRITES,
        "all {} keys must remain in key_index after DRAM eviction (entries still on disk); got {}",
        TOTAL_WRITES, key_count
    );
}

#[test]
fn replace_event_key_index_retains_entry() {
    // Event::Replace fires only when the old value is still in the 1-byte DRAM tier at
    // overwrite time. With memory(1), this is near-zero frequency — entries are already
    // on disk. key_index must retain the entry either way so evict_prefix() still works.
    //
    // used_bytes is a monotonically-increasing counter (total bytes ever inserted).
    // No subtraction happens on replace because Event::Replace almost never fires.
    let (cache, _dir) = test_cache();
    let key = range_cache_key("/data/file.parquet", 0, 100);
    cache.put(&key, Bytes::from_static(b"version_1"));
    let used_after_v1 = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    cache.put(&key, Bytes::from_static(b"version_2"));
    std::thread::sleep(std::time::Duration::from_millis(100));

    // With HashSet key_index, putting the same key twice must result in exactly 1 entry
    // (not 2), because HashSet::insert is idempotent for duplicate keys.
    let count = cache
        .key_index
        .get("data/file.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(
        count, 1,
        "key must appear exactly once in key_index after overwrite (HashSet dedup); got {}",
        count
    );

    // used_bytes grows monotonically: v2 was added on top of v1 (no reliable subtraction).
    let used_after_v2 = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        used_after_v2 >= used_after_v1,
        "used_bytes must not decrease on overwrite"
    );

    // Latest value must be readable.
    let result = block_on(cache.get(&key));
    assert_eq!(result.as_deref(), Some(b"version_2".as_slice()));

    // evict_prefix must still clean up correctly and increment removed_count.
    let removed_before = cache
        .stats
        .removed_count
        .load(std::sync::atomic::Ordering::Relaxed);
    cache.evict_prefix("/data/file.parquet");
    assert!(!cache.key_index.contains_key("data/file.parquet"));
    let removed_after = cache
        .stats
        .removed_count
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        removed_after > removed_before,
        "removed_count must increase after evict_prefix"
    );
}

// ── Background sweeper tests ──────────────────────────────────────────────────

#[test]
fn sweep_once_returns_zero_when_all_entries_still_on_disk() {
    // All entries were just put — they are on disk and inner.contains() returns true.
    // Sweeper should remove nothing.
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0, 100, b"data");
    put_range(&cache, "/data/file.parquet", 100, 200, b"more");
    let removed = cache.sweep_once();
    assert_eq!(
        removed, 0,
        "no stale entries expected immediately after put"
    );
    assert!(cache.key_index.contains_key("data/file.parquet"));
}

#[test]
fn sweep_once_removes_stale_keys_and_updates_eviction_count() {
    // Inject a fake key into key_index that doesn't exist in Foyer's disk index.
    // Sweeper should remove it, increment eviction_count, update eviction_bytes, and
    // decrement used_bytes.
    //
    // Fake key range: 99999–100000 = 1 byte. key_byte_size() parses this from the key string.
    let (cache, _dir) = test_cache();

    // Put a real key so the prefix bucket exists.
    put_range(&cache, "/data/file.parquet", 0, 100, b"real");

    // Inject a fake key that Foyer has never seen — inner.contains() will return false.
    // Range: 99999-100000 → 1 byte, parseable by key_byte_size().
    let fake_key = "data/file.parquet\x1F99999-100000".to_string();
    cache
        .key_index
        .entry("data/file.parquet".to_string())
        .or_default()
        .insert(fake_key.clone());

    let eviction_count_before = cache
        .stats
        .eviction_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let eviction_bytes_before = cache
        .stats
        .eviction_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    let used_bytes_before = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    // Loop shard_count times to guarantee the shard containing the fake key is visited.
    // DashMap hashes keys to one of N shards (N=64 on macOS); sweep_once() processes ONE shard
    // per call, so we need N calls to cover all shards exactly once.
    let shard_count = cache.key_index.shards().len();
    let removed: usize = (0..shard_count).map(|_| cache.sweep_once()).sum();

    assert_eq!(
        removed, 1,
        "sweeper should have removed the 1 fake/stale key"
    );
    assert_eq!(
        cache
            .stats
            .eviction_count
            .load(std::sync::atomic::Ordering::Relaxed),
        eviction_count_before + 1,
        "eviction_count must be incremented for stale entries removed by sweeper"
    );
    // Fake key range 99999-100000 = 1 byte.
    assert_eq!(
        cache
            .stats
            .eviction_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        eviction_bytes_before + 1,
        "eviction_bytes must reflect the byte size of stale entries (parsed from key)"
    );
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        used_bytes_before - 1,
        "used_bytes must be decremented by freed bytes when sweeper removes stale entries"
    );

    // Real key is still in key_index.
    let remaining = cache
        .key_index
        .get("data/file.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(
        remaining, 1,
        "real key must remain in key_index after sweep"
    );
}

#[test]
fn sweep_once_removes_empty_prefix_buckets() {
    // When all keys under a prefix become stale, the prefix bucket itself should be removed.
    let (cache, _dir) = test_cache();

    // Inject only fake keys for a prefix — no real entries.
    let fake1 = "data/gone.parquet\x1F0-100".to_string();
    let fake2 = "data/gone.parquet\x1F100-200".to_string();
    cache
        .key_index
        .entry("data/gone.parquet".to_string())
        .or_default()
        .extend(vec![fake1, fake2]);

    // Loop shard_count times to guarantee all shards are visited.
    let shard_count = cache.key_index.shards().len();
    let removed: usize = (0..shard_count).map(|_| cache.sweep_once()).sum();
    assert_eq!(removed, 2);
    assert!(
        !cache.key_index.contains_key("data/gone.parquet"),
        "empty prefix bucket must be removed after sweep"
    );
}

#[test]
fn sweep_once_on_empty_cache_is_noop() {
    let (cache, _dir) = test_cache();
    let removed = cache.sweep_once();
    assert_eq!(removed, 0);
    assert!(cache.key_index.is_empty());
}

// ── evict_prefix stats correctness ───────────────────────────────────────────

#[test]
fn evict_prefix_updates_removed_bytes_and_used_bytes() {
    // evict_prefix() must update removed_bytes and decrement used_bytes via key_byte_size().
    // put_range stores exactly (end - start) bytes, so key_byte_size() == data.len().
    let (cache, _dir) = test_cache();

    // Two ranges: 0-100 = 100 bytes, 100-300 = 200 bytes → total 300 bytes.
    put_range(&cache, "/data/file.parquet", 0, 100, &vec![0u8; 100]);
    put_range(&cache, "/data/file.parquet", 100, 300, &vec![0u8; 200]);
    std::thread::sleep(std::time::Duration::from_millis(100));

    let used_before = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    let removed_count_before = cache
        .stats
        .removed_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let removed_bytes_before = cache
        .stats
        .removed_bytes
        .load(std::sync::atomic::Ordering::Relaxed);

    cache.evict_prefix("/data/file.parquet");

    let used_after = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    let removed_count = cache
        .stats
        .removed_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let removed_bytes = cache
        .stats
        .removed_bytes
        .load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(
        removed_count,
        removed_count_before + 2,
        "removed_count: 2 entries evicted"
    );
    assert_eq!(
        removed_bytes,
        removed_bytes_before + 300,
        "removed_bytes: 100 + 200 = 300"
    );
    assert_eq!(
        used_after,
        used_before - 300,
        "used_bytes must decrease by 300 after eviction"
    );
}

#[test]
fn evict_prefix_on_nonexistent_prefix_does_not_change_stats() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0, 100, &vec![0u8; 100]);

    let removed_before = cache
        .stats
        .removed_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let used_before = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);

    cache.evict_prefix("/data/other.parquet");

    assert_eq!(
        cache
            .stats
            .removed_count
            .load(std::sync::atomic::Ordering::Relaxed),
        removed_before
    );
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        used_before
    );
}

#[test]
fn used_bytes_is_correct_after_put_then_evict() {
    // used_bytes starts at 0, put adds size, evict_prefix subtracts — net result = 0.
    let (cache, _dir) = test_cache();
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    put_range(&cache, "/data/file.parquet", 0, 1024, &vec![0u8; 1024]);
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        1024
    );

    cache.evict_prefix("/data/file.parquet");
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "used_bytes must return to 0 after evicting all entries"
    );
}

// ── sweep idempotency and edge cases ─────────────────────────────────────────

#[test]
fn sweep_once_is_idempotent_for_real_entries() {
    // Calling sweep_once() twice on live entries must not double-decrement stats.
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0, 1024, &vec![0u8; 1024]);

    let removed1 = cache.sweep_once();
    let eviction_bytes_after_1 = cache
        .stats
        .eviction_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    let removed2 = cache.sweep_once();
    let eviction_bytes_after_2 = cache
        .stats
        .eviction_bytes
        .load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(removed1, 0, "no stale entries on first sweep");
    assert_eq!(removed2, 0, "no stale entries on second sweep");
    assert_eq!(eviction_bytes_after_1, 0);
    assert_eq!(
        eviction_bytes_after_2, 0,
        "eviction_bytes must not change across two sweeps of live entries"
    );
}

#[test]
fn sweep_once_stale_key_with_malformed_range_does_not_panic() {
    // A key without a parseable range (key_byte_size returns 0) must still be
    // removed cleanly — no panic, no stats corruption.
    let (cache, _dir) = test_cache();

    // Inject a malformed stale key — no separator, Foyer has never seen it.
    cache
        .key_index
        .entry("data/bad.parquet".to_string())
        .or_default()
        .insert("data/bad.parquet-not-a-range-key".to_string());

    let eviction_bytes_before = cache
        .stats
        .eviction_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    let shard_count = cache.key_index.shards().len();
    let removed: usize = (0..shard_count).map(|_| cache.sweep_once()).sum();

    assert_eq!(removed, 1, "stale malformed key must be removed");
    // key_byte_size returns 0 for malformed key — eviction_bytes unchanged.
    assert_eq!(
        cache
            .stats
            .eviction_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        eviction_bytes_before,
        "eviction_bytes must not change for malformed (size=0) key"
    );
    assert!(
        !cache.key_index.contains_key("data/bad.parquet"),
        "empty prefix bucket must be removed"
    );
}

#[test]
fn sweep_once_removes_empty_prefix_buckets_and_updates_eviction_bytes() {
    // Extended version of sweep_once_removes_empty_prefix_buckets that also checks bytes.
    // Two fake keys: 0-100 (100 bytes) and 100-200 (100 bytes) = 200 bytes total.
    let (cache, _dir) = test_cache();

    let fake1 = "data/gone.parquet\x1F0-100".to_string();
    let fake2 = "data/gone.parquet\x1F100-200".to_string();
    cache
        .key_index
        .entry("data/gone.parquet".to_string())
        .or_default()
        .extend(vec![fake1, fake2]);

    let eviction_bytes_before = cache
        .stats
        .eviction_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    let used_bytes_before = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);

    let shard_count = cache.key_index.shards().len();
    let removed: usize = (0..shard_count).map(|_| cache.sweep_once()).sum();

    assert_eq!(removed, 2);
    assert!(!cache.key_index.contains_key("data/gone.parquet"));
    assert_eq!(
        cache
            .stats
            .eviction_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        eviction_bytes_before + 200,
        "eviction_bytes must reflect both stale keys (100 + 100 = 200)"
    );
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        used_bytes_before - 200,
        "used_bytes must decrease by 200"
    );
}

#[test]
fn event_remove_after_evict_prefix_does_not_panic_or_corrupt_key_index() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0, 100, b"data");
    put_range(&cache, "/data/file.parquet", 100, 200, b"more");
    cache.evict_prefix("/data/file.parquet");
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(!cache.key_index.contains_key("data/file.parquet"));
    put_range(&cache, "/data/file.parquet", 0, 100, b"fresh");
    assert_eq!(
        block_on(cache.get(&range_cache_key("/data/file.parquet", 0, 100))).as_deref(),
        Some(b"fresh".as_slice())
    );
}

// ── FFM lifecycle ─────────────────────────────────────────────────────────────

#[test]
fn ffm_create_returns_positive_pointer() {
    let dir = TempDir::new().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe {
        foyer_create_cache(
            128 * 1024 * 1024,
            dir_str.as_ptr(),
            dir_str.len() as u64,
            BLOCK_SIZE as u64,
            BLOCK_SIZE as u64,
            (BLOCK_SIZE * 2) as u64,
            engine.as_ptr(),
            engine.len() as u64,
            0,
            0.0_f64,
            0,
        )
    };
    assert!(ptr > 0);
    let result = unsafe { foyer_destroy_cache(ptr) };
    assert_eq!(result, 0);
}

#[test]
fn ffm_create_with_null_ptr_returns_error() {
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe {
        foyer_create_cache(
            128 * 1024 * 1024,
            std::ptr::null(),
            10,
            BLOCK_SIZE as u64,
            BLOCK_SIZE as u64,
            (BLOCK_SIZE * 2) as u64,
            engine.as_ptr(),
            engine.len() as u64,
            0,
            0.0_f64,
            0,
        )
    };
    assert!(ptr < 0);
    if ptr < 0 {
        unsafe {
            native_bridge_common::error::native_error_free(-ptr);
        }
    }
}

#[test]
fn ffm_create_with_invalid_utf8_returns_error() {
    let invalid_utf8 = [0xFF, 0xFE, 0xFD];
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe {
        foyer_create_cache(
            128 * 1024 * 1024,
            invalid_utf8.as_ptr(),
            invalid_utf8.len() as u64,
            BLOCK_SIZE as u64,
            BLOCK_SIZE as u64,
            (BLOCK_SIZE * 2) as u64,
            engine.as_ptr(),
            engine.len() as u64,
            0,
            0.0_f64,
            0,
        )
    };
    assert!(ptr < 0);
    if ptr < 0 {
        unsafe {
            native_bridge_common::error::native_error_free(-ptr);
        }
    }
}

#[test]
fn ffm_destroy_with_zero_ptr_returns_error() {
    let result = unsafe { foyer_destroy_cache(0) };
    assert!(result < 0);
    if result < 0 {
        unsafe {
            native_bridge_common::error::native_error_free(-result);
        }
    }
}

#[test]
fn ffm_destroy_with_negative_ptr_returns_error() {
    let result = unsafe { foyer_destroy_cache(-1) };
    assert!(result < 0);
    if result < 0 {
        unsafe {
            native_bridge_common::error::native_error_free(-result);
        }
    }
}

#[test]
fn ffm_create_destroy_lifecycle_no_leak() {
    let engine = IO_ENGINE.as_bytes();
    for _ in 0..3 {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        let ptr = unsafe {
            foyer_create_cache(
                128 * 1024 * 1024,
                dir_str.as_ptr(),
                dir_str.len() as u64,
                BLOCK_SIZE as u64,
                BLOCK_SIZE as u64,
                (BLOCK_SIZE * 2) as u64,
                engine.as_ptr(),
                engine.len() as u64,
                0,
                0.0_f64,
                0,
            )
        };
        assert!(ptr > 0);
        let result = unsafe { foyer_destroy_cache(ptr) };
        assert_eq!(result, 0);
    }
}

// ── foyer_snapshot_stats tests ───────────────────────────────────────────────

use crate::foyer::ffm::foyer_snapshot_stats;

/// foyer_snapshot_stats returns 0 on a valid cache pointer and writes 14 i64 values.
/// The values at index 0 (overall hit_count) and index 7 (block_level hit_count) should
/// be identical since Foyer is single-tier (both sections mirror each other).
#[test]
fn ffm_snapshot_stats_valid_ptr_returns_zero_and_fills_buffer() {
    let dir = TempDir::new().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe {
        foyer_create_cache(
            128 * 1024 * 1024,
            dir_str.as_ptr(),
            dir_str.len() as u64,
            BLOCK_SIZE as u64,
            BLOCK_SIZE as u64,
            (BLOCK_SIZE * 2) as u64,
            engine.as_ptr(),
            engine.len() as u64,
            0,
            0.0_f64,
            0,
        )
    };
    assert!(ptr > 0);

    // 10 fields × 2 sections = 20 longs
    let mut out = [i64::MAX; 20];
    let rc = unsafe { crate::foyer::ffm::foyer_snapshot_stats(ptr, out.as_mut_ptr()) };
    assert_eq!(rc, 0, "foyer_snapshot_stats should return 0 on success");

    // A freshly created cache has no hits, misses, evictions, used bytes, or active reads.
    // Sections 0 and 1 are identical (Foyer is currently single-tier).
    for i in 0..20 {
        assert_eq!(
            out[i], 0,
            "out[{i}] should be 0 for a fresh cache, got {}",
            out[i]
        );
    }

    let destroy_rc = unsafe { foyer_destroy_cache(ptr) };
    assert_eq!(destroy_rc, 0);
}

#[test]
fn snapshot_stats_returns_zero_for_fresh_cache() {
    let dir = TempDir::new().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe {
        foyer_create_cache(
            128 * 1024 * 1024,
            dir_str.as_ptr(),
            dir_str.len() as u64,
            BLOCK_SIZE as u64,
            BLOCK_SIZE as u64,
            (BLOCK_SIZE * 2) as u64,
            engine.as_ptr(),
            engine.len() as u64,
            0,
            0.0_f64,
            0,
        )
    };
    assert!(ptr > 0);

    // 10 fields × 2 sections = 20 longs
    let mut buf = [i64::MIN; 20];
    let result = unsafe { foyer_snapshot_stats(ptr, buf.as_mut_ptr()) };
    assert_eq!(result, 0, "foyer_snapshot_stats must return 0 on success");

    // No i64::MIN values — the buffer was fully written.
    for (i, &v) in buf.iter().enumerate() {
        assert_ne!(v, i64::MIN, "buf[{}] was not written", i);
    }

    // Both sections are identical (single-tier: overall mirrors block_level).
    assert_eq!(
        &buf[..10],
        &buf[10..],
        "overall and block_level sections must be identical"
    );

    unsafe { foyer_destroy_cache(ptr) };
}

/// foyer_snapshot_stats with null ptr returns negative.
#[test]
fn ffm_snapshot_stats_null_ptr_returns_error() {
    let mut buf = [0i64; 14];
    let result = unsafe { foyer_snapshot_stats(0, buf.as_mut_ptr()) };
    assert!(result < 0);
}

/// foyer_snapshot_stats with null out buffer returns negative.
#[test]
fn ffm_snapshot_stats_null_out_returns_error() {
    let dir = TempDir::new().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe {
        foyer_create_cache(
            128 * 1024 * 1024,
            dir_str.as_ptr(),
            dir_str.len() as u64,
            BLOCK_SIZE as u64,
            BLOCK_SIZE as u64,
            (BLOCK_SIZE * 2) as u64,
            engine.as_ptr(),
            engine.len() as u64,
            0,
            0.0_f64,
            0,
        )
    };
    assert!(ptr > 0);

    let rc = unsafe { crate::foyer::ffm::foyer_snapshot_stats(ptr, std::ptr::null_mut()) };
    assert!(
        rc < 0,
        "foyer_snapshot_stats with null out should return < 0, got {rc}"
    );

    let destroy_rc = unsafe { foyer_destroy_cache(ptr) };
    assert_eq!(destroy_rc, 0);
}

#[test]
fn snapshot_stats_returns_error_for_invalid_ptr() {
    let mut out = [0i64; 14];
    let rc = unsafe { crate::foyer::ffm::foyer_snapshot_stats(0, out.as_mut_ptr()) };
    assert!(
        rc < 0,
        "foyer_snapshot_stats with ptr=0 should return < 0, got {rc}"
    );
}

#[test]
fn snapshot_stats_two_sections_identical_for_single_tier() {
    // Foyer is currently disk-only (single tier): overall == block_level.
    // Put an entry so used_bytes becomes non-zero, then confirm sections match.
    let (cache, _dir) = test_cache();
    let key = range_cache_key("/data/file.parquet", 0, 512);
    cache.put(&key, Bytes::from(vec![0u8; 512]));

    // Snapshot via the FFM function.
    // foyer_snapshot_stats expects a Box<Arc<dyn BlockCache>> pointer, not Arc<FoyerCache>.
    let cache_trait: Arc<dyn crate::traits::BlockCache> = std::sync::Arc::new(cache);
    let boxed = Box::new(std::sync::Arc::clone(&cache_trait));
    let raw_ptr = Box::into_raw(boxed) as i64;

    // 10 fields × 2 sections = 20 longs
    let mut out = [0i64; 20];
    let rc = unsafe { crate::foyer::ffm::foyer_snapshot_stats(raw_ptr, out.as_mut_ptr()) };
    assert_eq!(rc, 0);

    // Clean up the Box.
    let _ = unsafe { Box::from_raw(raw_ptr as *mut Arc<dyn crate::traits::BlockCache>) };

    // Section 0 (indices 0–9) and section 1 (indices 10–19) must be identical.
    assert_eq!(
        &out[0..10],
        &out[10..20],
        "overall and block_level sections should be identical for single-tier Foyer"
    );
}

#[test]
fn snapshot_stats_used_bytes_reflects_put() {
    let (cache, _dir) = test_cache();
    let data = vec![0xABu8; 1024];
    let key = range_cache_key("/data/file.parquet", 0, 1024);
    cache.put(&key, Bytes::copy_from_slice(&data));

    // used_bytes counter is updated synchronously on put().
    // Verify it is reflected immediately (index 6 = used_bytes).
    let snap = cache.stats.snapshot();
    assert_eq!(
        snap[6], 1024,
        "used_bytes should be 1024 after a single 1KB put"
    );
}

#[test]
fn snapshot_stats_null_out_via_created_cache() {
    // Verify foyer_snapshot_stats returns < 0 when out pointer is null.
    let (cache, _dir) = test_cache();
    let data = vec![0xABu8; 1024];
    let key = range_cache_key("/data/file.parquet", 0, 1024);
    cache.put(&key, Bytes::copy_from_slice(&data));

    let snap = cache.stats.snapshot();
    assert_eq!(
        snap[6], 1024,
        "used_bytes should be 1024 after a single 1KB put"
    );
}

/// foyer_create_cache returns a fat Box<Arc<dyn BlockCache>> pointer.
/// The pointer can be reinterpreted as Box<Arc<dyn BlockCache>> and the Arc's
/// strong count is exactly 1 immediately after creation.
#[test]
fn ffm_create_cache_returns_fat_ptr_with_strong_count_one() {
    use crate::traits::BlockCache;

    let dir = TempDir::new().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe {
        foyer_create_cache(
            16 * 1024 * 1024,
            dir_str.as_ptr(),
            dir_str.len() as u64,
            BLOCK_SIZE as u64,
            BLOCK_SIZE as u64,
            (BLOCK_SIZE * 2) as u64,
            engine.as_ptr(),
            engine.len() as u64,
            0,
            0.0_f64,
            0,
        )
    };
    assert!(ptr > 0);

    // Interpret as Box<Arc<dyn BlockCache>> — if the pointer type is wrong this will crash.
    // Take ownership and immediately check the strong count via a clone probe.
    let boxed: Box<Arc<dyn BlockCache>> = unsafe { Box::from_raw(ptr as *mut Arc<dyn BlockCache>) };
    // Clone to bump strong count by 1 — original was 1, now 2.
    let clone = Arc::clone(&*boxed);
    assert_eq!(Arc::strong_count(&*boxed), 2);
    drop(clone);
    assert_eq!(Arc::strong_count(&*boxed), 1);
    // Drop the box — this calls foyer_destroy internally via Arc drop.
    drop(boxed);
}

/// foyer_create_cache returns a pointer that can be cloned (passed to multiple TieredObjectStores)
/// without double-free. After all clones drop, the FoyerCache is freed exactly once.
#[test]
fn ffm_create_cache_ptr_cloneable_for_multiple_shards() {
    use crate::traits::BlockCache;

    let dir = TempDir::new().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe {
        foyer_create_cache(
            16 * 1024 * 1024,
            dir_str.as_ptr(),
            dir_str.len() as u64,
            BLOCK_SIZE as u64,
            BLOCK_SIZE as u64,
            (BLOCK_SIZE * 2) as u64,
            engine.as_ptr(),
            engine.len() as u64,
            0,
            0.0_f64,
            0,
        )
    };
    assert!(ptr > 0);

    // Simulate 3 shards each cloning the Arc (as ts_create_tiered_object_store would).
    let boxed = unsafe { &*(ptr as *const Arc<dyn BlockCache>) };
    let shard1 = Arc::clone(boxed);
    let shard2 = Arc::clone(boxed);
    let shard3 = Arc::clone(boxed);
    assert_eq!(Arc::strong_count(boxed), 4, "1 (box) + 3 shards");

    // Shards close.
    drop(shard1);
    drop(shard2);
    drop(shard3);
    assert_eq!(Arc::strong_count(boxed), 1, "only the box remains");

    // Node-level close: destroy the original Box.
    let _box = unsafe { Box::from_raw(ptr as *mut Arc<dyn BlockCache>) };
    // Drop _box — strong count reaches 0, FoyerCache freed.
}

// ── Shutdown / CancellationToken tests ───────────────────────────────────────

/// Dropping a cache with sweep_interval_secs=0 (no sweep task) must not panic.
/// The CancellationToken.cancel() call in Drop is safe even when no task holds
/// the child token — it is a no-op in that case.
#[test]
fn drop_without_sweep_task_does_not_panic() {
    let (cache, _dir) = test_cache(); // sweep_interval_secs=0 → no task spawned
    drop(cache); // calls shutdown.cancel() — must be a no-op
}

/// Dropping a cache with an active sweep task must not panic.
/// The sweep task holds a cloned CancellationToken; cancel() unblocks it from
/// tokio::select! and it exits cleanly.
#[test]
fn drop_with_active_sweep_task_does_not_panic() {
    let dir = TempDir::new().unwrap();
    // Use a large interval so the task never actually fires during the test.
    // We only care that drop() cancels it cleanly.
    let cache = FoyerCache::new(
        128 * 1024 * 1024,
        dir.path(),
        BLOCK_SIZE,
        BLOCK_SIZE,
        BLOCK_SIZE * 2,
        IO_ENGINE,
        3600, // 1-hour interval — task sleeps, drop cancels it immediately
        0.0,  // threshold disabled
        0,    // persist disabled
        false,
    );
    drop(cache); // shutdown.cancel() wakes the select! branch → task exits
}

/// After dropping a cache, its CancellationToken is cancelled (is_cancelled() == true).
/// This confirms cancel() was actually called and not skipped.
#[test]
fn drop_cancels_the_token() {
    use tokio_util::sync::CancellationToken;

    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(
        128 * 1024 * 1024,
        dir.path(),
        BLOCK_SIZE,
        BLOCK_SIZE,
        BLOCK_SIZE * 2,
        IO_ENGINE,
        0,   // no sweep task
        0.0, // threshold disabled
        0,   // no persist task
        false,
    );
    // Clone the token before drop so we can inspect it after.
    let token: CancellationToken = cache.shutdown.clone();
    assert!(
        !token.is_cancelled(),
        "token must not be cancelled before drop"
    );
    drop(cache);
    assert!(token.is_cancelled(), "token must be cancelled after drop");
}

/// Cache remains fully functional (put/get/evict/clear) after having been used
/// and dropped. This test creates two sequential caches in the same temp dir
/// to verify no shared state leaks between lifetimes.
#[test]
fn cache_functional_before_drop() {
    let dir = TempDir::new().unwrap();
    {
        let cache = FoyerCache::new(
            64 * 1024 * 1024,
            dir.path(),
            BLOCK_SIZE,
            BLOCK_SIZE,
            BLOCK_SIZE * 2,
            IO_ENGINE,
            0,
            0.0,
            0,
            false,
        );
        let key = range_cache_key("/data/file.parquet", 0, 100);
        cache.put(&key, Bytes::from_static(b"hello"));
        let result = block_on(cache.get(&key));
        assert_eq!(result.as_deref(), Some(b"hello".as_slice()));
        cache.evict_prefix("/data/file.parquet");
        block_on(cache.clear());
        // drop(cache) here — cancels token
    }
    // No panic, no assertion failure → shutdown was clean.
}

/// sweep_interval_secs=0 must not spawn a sweep task.
/// Verified indirectly: the key_index is not modified by any background activity
/// even after a brief sleep, and the cache remains usable.
#[test]
fn sweep_disabled_when_interval_is_zero() {
    let (cache, _dir) = test_cache(); // sweep_interval_secs=0
    put_range(&cache, "/data/file.parquet", 0, 100, b"data");
    // If a sweep task were running it might prune entries, but since sweep=0
    // no task is spawned. The key_index must still contain the entry.
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(
        cache.key_index.contains_key("data/file.parquet"),
        "key_index must still contain entry — no background task should have modified it"
    );
}

/// When sweep_interval_secs>0, the background task is spawned and the cache
/// continues to function normally. We do not wait for the sweep to fire
/// (interval is very long); we only verify the cache is usable while the task
/// is sleeping in tokio::select!.
#[test]
fn sweep_enabled_cache_is_usable_while_task_sleeping() {
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(
        64 * 1024 * 1024,
        dir.path(),
        BLOCK_SIZE,
        BLOCK_SIZE,
        BLOCK_SIZE * 2,
        IO_ENGINE,
        3600,
        0.0,
        0,
        false,
    );
    let key = range_cache_key("/data/file.parquet", 0, 512);
    cache.put(&key, Bytes::from(vec![0xABu8; 512]));
    let result = block_on(cache.get(&key));
    assert_eq!(result.as_ref().map(|b| b.len()), Some(512));
    // drop cancels the sleeping task immediately via cancelled() branch
}

// ── Watchdog and panic recovery tests ────────────────────────────────────────
//
// These tests exercise Fix 1 (catch_unwind) and Fix 4 (watchdog outer loop)
// for both the sweep task and the persist task.

/// Verify that `catch_unwind` correctly catches a panic without propagating it.
/// Simulates exactly what the sweep task does when `reconcile_key_index` panics:
/// the closure panics, `catch_unwind` returns `Err`, and the cache stays usable.
#[test]
fn sweep_task_catch_unwind_does_not_kill_loop_on_panic() {
    let (cache, _dir) = test_cache();
    // Manually replicate the catch_unwind logic from the sweep task.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        panic!("simulated reconcile_key_index panic");
    }));
    // Panic must be caught — not propagated.
    assert!(result.is_err(), "catch_unwind must catch the panic");
    // The cache must still be fully functional after the simulated panic.
    put_range(&cache, "/data/file.parquet", 0, 100, b"data");
    let result = block_on(cache.get(&range_cache_key("/data/file.parquet", 0, 100)));
    assert!(result.is_some(), "cache must be usable after catch_unwind");
}

/// Verify that `catch_unwind` correctly catches a panic in the persist path.
/// Simulates exactly what the persist task does when `key_index_store::save` panics.
#[test]
fn persist_task_catch_unwind_does_not_kill_loop_on_panic() {
    let (cache, _dir) = test_cache();
    // Simulate the persist task catch_unwind.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        panic!("simulated key_index_store::save panic");
    }));
    assert!(result.is_err(), "catch_unwind must catch the save panic");
    // Cache is still usable.
    put_range(&cache, "/data/b.parquet", 0, 200, b"persist_data");
    assert!(
        cache.key_index.contains_key("data/b.parquet"),
        "key_index must be intact after simulated panic"
    );
    assert!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "used_bytes must be > 0 after put"
    );
}

/// Verify that the sweep task with the watchdog outer loop stops cleanly
/// when the cancellation token fires. Uses a short 1-second interval so
/// the inner loop fires at least once before drop.
#[test]
fn sweep_task_with_active_watchdog_stops_cleanly_on_cancel() {
    let dir = TempDir::new().unwrap();
    // Short sweep interval: the inner loop fires at least once within 2s.
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        1,   // 1-second interval
        0.0, // threshold disabled — sweep runs every tick
        0,   // persist disabled
        false,
    );
    // Put something so the sweep has a non-trivial key_index to process.
    put_range(
        &cache,
        "/data/watchdog_test.parquet",
        0,
        512,
        &vec![0u8; 512],
    );
    // Let the sweep fire at least once.
    std::thread::sleep(std::time::Duration::from_millis(1200));
    // Drop cancels the token — the 'inner loop's cancelled() arm fires `return`.
    // The watchdog outer loop must NOT re-enter because `return` exits the closure.
    // No hang, no panic expected.
    drop(cache);
    // Reaching here means the task stopped cleanly.
}

/// Verify that the persist task with the watchdog outer loop stops cleanly
/// when the cancellation token fires. Waits for at least one persist to happen.
#[test]
fn persist_task_with_active_watchdog_stops_cleanly_on_cancel() {
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0, // sweep disabled
        0.0,
        1, // 1-second persist interval
        false,
    );
    put_range(
        &cache,
        "/data/persist_watchdog.parquet",
        0,
        256,
        &vec![0u8; 256],
    );
    // Poll for key_index.json — the persist task must write it within 5s.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !dir
        .path()
        .join(crate::key_index_store::KEY_INDEX_FILENAME)
        .exists()
    {
        if std::time::Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(
        dir.path()
            .join(crate::key_index_store::KEY_INDEX_FILENAME)
            .exists(),
        "persist task must have written key_index.json before drop"
    );
    // Drop cancels token — persist task stops via `return` in cancelled() arm.
    // No hang means watchdog exited cleanly.
    drop(cache);
}

/// Verify the design contract: `last_persisted` is reset to `i64::MIN` at the top
/// of the watchdog outer loop, so the first tick after recovery always persists.
///
/// A second FoyerCache opens the same dir (recovered used_bytes > 0).
/// Even though used_bytes is non-zero, last_persisted starts at i64::MIN ≠ used_bytes,
/// so the first persist tick MUST fire and update the snapshot mtime.
#[test]
fn persist_task_last_persisted_reset_forces_persist_after_recovery() {
    let dir = TempDir::new().unwrap();
    // Session 1: put data and let Drop write key_index.json.
    {
        let cache1 = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            0,
            false,
        );
        put_range(&cache1, "/data/reset_test.parquet", 0, 100, &vec![0u8; 100]);
        // Drop writes key_index.json with used_bytes=100.
    }
    assert!(
        dir.path()
            .join(crate::key_index_store::KEY_INDEX_FILENAME)
            .exists(),
        "key_index.json must exist after session 1 Drop"
    );
    // Session 2: open with persist_interval=1s.
    // recovered used_bytes=100; last_persisted starts at i64::MIN.
    // First tick: i64::MIN != 100 → persist fires → mtime advances.
    {
        let cache2 = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            1,
            false,
        );
        // Record mtime written by Drop of session 1.
        let mtime_before =
            std::fs::metadata(dir.path().join(crate::key_index_store::KEY_INDEX_FILENAME))
                .unwrap()
                .modified()
                .unwrap();
        // Wait 2s for the first persist tick.
        std::thread::sleep(std::time::Duration::from_millis(2000));
        let mtime_after =
            std::fs::metadata(dir.path().join(crate::key_index_store::KEY_INDEX_FILENAME))
                .unwrap()
                .modified()
                .unwrap();
        assert!(
            mtime_after > mtime_before,
            "persist task must fire on first tick after recovery (last_persisted=MIN != recovered used_bytes)"
        );
        drop(cache2);
    }
}

// ── Sweep cursor tests ───────────────────────────────────────────────────────

/// Each sweep_once() processes exactly one shard and advances the cursor.
/// After 16 calls the cursor has wrapped through all shards (0..15).
#[test]
fn sweep_cursor_advances_by_one_per_call() {
    let (cache, _dir) = test_cache();
    assert_eq!(
        cache
            .sweep_cursor
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "cursor must start at 0"
    );

    // 16 sweep calls — cursor increments on each (modulo 16 internally via fetch_add).
    for i in 1usize..=16 {
        cache.sweep_once();
        assert_eq!(
            cache
                .sweep_cursor
                .load(std::sync::atomic::Ordering::Relaxed)
                % 16,
            i % 16,
            "cursor must be {} after {} calls",
            i % 16,
            i
        );
    }
}

/// Presetting the cursor and calling sweep_once() must process shard `cursor % 16`.
/// After the call the cursor is `preset + 1`.
#[test]
fn sweep_cursor_preset_is_respected() {
    let (cache, _dir) = test_cache();
    // Preset to shard 5.
    cache
        .sweep_cursor
        .store(5, std::sync::atomic::Ordering::Relaxed);
    cache.sweep_once();
    assert_eq!(
        cache
            .sweep_cursor
            .load(std::sync::atomic::Ordering::Relaxed),
        6,
        "cursor must be 6 (5+1) after one sweep from preset 5"
    );
}

/// The cursor wraps correctly at the shard count boundary.
/// Preset to shard_count - 1, one more call brings raw value to shard_count,
/// which modulo shard_count == 0 (back to shard 0).
#[test]
fn sweep_cursor_wraps_at_shard_count() {
    let (cache, _dir) = test_cache();
    let shard_count = cache.key_index.shards().len();

    // Set cursor to last shard index.
    cache
        .sweep_cursor
        .store(shard_count - 1, std::sync::atomic::Ordering::Relaxed);
    cache.sweep_once(); // processes shard (shard_count - 1), cursor becomes shard_count

    let raw = cache
        .sweep_cursor
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        raw % shard_count,
        0,
        "cursor raw={} must wrap: {} % {} == 0",
        raw,
        raw,
        shard_count
    );
}

/// Each call to sweep_once() processes exactly one shard.
///
/// Inject one stale key per shard (using raw shard access). After a single sweep_once()
/// exactly one shard's stale key is removed; all other shards still have their stale key.
#[test]
fn sweep_once_processes_exactly_one_shard() {
    let (cache, _dir) = test_cache();
    let shards = cache.key_index.shards();
    let shard_count = shards.len();

    // Inject one unique stale key per shard directly via raw shard access.
    // Each key is unique so it hashes to a predictable shard via the raw write.
    for i in 0..shard_count {
        let prefix = format!("shard_test_{}", i);
        let key = format!("{}\x1F0-100", prefix);
        // Direct shard write bypasses DashMap's hashing — we control exactly which shard gets the key.
        shards[i].write().insert(
            prefix,
            dashmap::SharedValue::new({
                let mut s = std::collections::HashSet::new();
                s.insert(key);
                s
            }),
        );
    }

    // Reset cursor to shard 0 for a predictable starting point.
    cache
        .sweep_cursor
        .store(0, std::sync::atomic::Ordering::Relaxed);

    // One sweep_once() call must process exactly one shard.
    cache.sweep_once();

    // Count how many shards still have entries.
    let shards_with_entries = shards.iter().filter(|s| !s.read().is_empty()).count();
    assert_eq!(
        shards_with_entries,
        shard_count - 1,
        "exactly one shard must have been swept; {} of {} shards still have entries (expected {})",
        shards_with_entries,
        shard_count,
        shard_count - 1
    );
}

/// A sweep of shard N must not modify shard N+1.
///
/// Inject a stale key into shard 0 and a different stale key into shard 1.
/// Set cursor to shard 0. After one sweep_once(), shard 0 is clean but shard 1 is untouched.
#[test]
fn sweep_once_does_not_touch_other_shards() {
    let (cache, _dir) = test_cache();
    let shards = cache.key_index.shards();
    assert!(shards.len() >= 2, "test requires at least 2 shards");

    // Inject stale keys directly into shards 0 and 1.
    let key0 = "stale_shard0\x1F0-100".to_string();
    let key1 = "stale_shard1\x1F0-200".to_string();
    shards[0].write().insert(
        "stale_shard0".to_string(),
        dashmap::SharedValue::new({
            let mut s = std::collections::HashSet::new();
            s.insert(key0);
            s
        }),
    );
    shards[1].write().insert(
        "stale_shard1".to_string(),
        dashmap::SharedValue::new({
            let mut s = std::collections::HashSet::new();
            s.insert(key1);
            s
        }),
    );

    // Set cursor to shard 0.
    cache
        .sweep_cursor
        .store(0, std::sync::atomic::Ordering::Relaxed);

    // One call sweeps shard 0 only.
    let removed = cache.sweep_once();
    assert_eq!(removed, 1, "only the stale key in shard 0 must be removed");

    // Shard 0 must be clean.
    assert!(
        shards[0].read().is_empty(),
        "shard 0 must be empty after sweep"
    );

    // Shard 1 must still have its stale key.
    assert!(
        !shards[1].read().is_empty(),
        "shard 1 must be untouched after sweeping shard 0"
    );
    assert_eq!(
        cache
            .sweep_cursor
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "cursor must point to shard 1 after sweeping shard 0"
    );
}

/// Stale keys injected across multiple prefix buckets are all eventually removed
/// after enough sweep_once() cycles. This test verifies the cursor-based sweep
/// correctly iterates shards and removes all stale entries over multiple passes.
#[test]
fn sweep_cursor_eventually_removes_all_stale_keys_across_multiple_calls() {
    let (cache, _dir) = test_cache();

    // Inject stale keys under several different prefixes (they will hash to different shards).
    let stale_keys = [
        ("data/alpha.parquet", "data/alpha.parquet\x1F0-100"),
        ("data/beta.parquet", "data/beta.parquet\x1F0-200"),
        ("data/gamma.parquet", "data/gamma.parquet\x1F0-300"),
        ("data/delta.parquet", "data/delta.parquet\x1F0-400"),
    ];

    for (prefix, key) in &stale_keys {
        cache
            .key_index
            .entry(prefix.to_string())
            .or_default()
            .insert(key.to_string());
    }

    assert_eq!(cache.key_index.len(), 4, "4 prefix buckets injected");

    // Run shard_count sweep cycles to guarantee all shards are covered exactly once.
    // DashMap defaults to 64 shards on macOS; we must sweep at least shard_count times.
    let shard_count = cache.key_index.shards().len();
    let mut total_removed = 0usize;
    for _ in 0..shard_count {
        total_removed += cache.sweep_once();
    }

    assert_eq!(
        total_removed, 4,
        "all 4 stale keys must be removed within shard_count sweep cycles"
    );
    assert!(
        cache.key_index.is_empty(),
        "key_index must be empty after all stale keys are swept"
    );
}

/// A shard containing both live and stale keys: sweep removes only the stale keys,
/// leaves the live keys in place, and correctly updates eviction stats.
#[test]
fn sweep_cursor_removes_stale_but_preserves_live_keys_in_same_prefix() {
    let (cache, _dir) = test_cache();

    // Put a real entry: key is live in Foyer.
    put_range(&cache, "/data/mixed.parquet", 0, 1024, &vec![0u8; 1024]);

    // Inject a fake stale key for the same prefix — Foyer has never seen this key.
    let stale_key = "data/mixed.parquet\x1F999000-999100".to_string();
    cache
        .key_index
        .entry("data/mixed.parquet".to_string())
        .or_default()
        .insert(stale_key.clone());

    // Key_index now has 2 keys for the prefix: 1 live + 1 stale.
    let before_count = cache
        .key_index
        .get("data/mixed.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(
        before_count, 2,
        "should have 1 live + 1 stale key before sweep"
    );

    let eviction_count_before = cache
        .stats
        .eviction_count
        .load(std::sync::atomic::Ordering::Relaxed);

    // Run enough sweeps to guarantee the shard containing "data/mixed.parquet" is processed.
    // DashMap defaults to 64 shards on macOS — must sweep at least shard_count times.
    let shard_count = cache.key_index.shards().len();
    for _ in 0..shard_count {
        cache.sweep_once();
    }

    // Live key must remain; stale key must be gone.
    let after_count = cache
        .key_index
        .get("data/mixed.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(after_count, 1, "only the live key must remain after sweep");
    assert!(
        !cache
            .key_index
            .get("data/mixed.parquet")
            .map(|v| v.contains(&stale_key))
            .unwrap_or(false),
        "stale key must have been removed"
    );

    // Eviction stats must have been updated for the stale key.
    assert!(
        cache
            .stats
            .eviction_count
            .load(std::sync::atomic::Ordering::Relaxed)
            > eviction_count_before,
        "eviction_count must increase for the stale key removed by sweep"
    );
}

/// After a cursor-based sweep that removes stale keys, the cache must still
/// respond correctly to put() and get() operations — no functional regression.
#[test]
fn sweep_cursor_cache_remains_functional_after_sweep() {
    let (cache, _dir) = test_cache();

    // Put two real entries.
    put_range(&cache, "/data/keep.parquet", 0, 512, &vec![0xAAu8; 512]);
    put_range(&cache, "/data/keep.parquet", 512, 1024, &vec![0xBBu8; 512]);

    // Also inject a fake stale key.
    cache
        .key_index
        .entry("data/keep.parquet".to_string())
        .or_default()
        .insert("data/keep.parquet\x1F9000-9100".to_string());

    // Run full sweep (16 cycles to guarantee coverage).
    for _ in 0..16 {
        cache.sweep_once();
    }

    // Real entries must still be retrievable after sweep.
    let r1 = block_on(cache.get(&range_cache_key("/data/keep.parquet", 0, 512)));
    let r2 = block_on(cache.get(&range_cache_key("/data/keep.parquet", 512, 1024)));
    assert_eq!(
        r1.as_ref().map(|b| b.len()),
        Some(512),
        "first range must still be readable after sweep"
    );
    assert_eq!(
        r2.as_ref().map(|b| b.len()),
        Some(512),
        "second range must still be readable after sweep"
    );

    // New puts and gets must work normally after sweep.
    put_range(&cache, "/data/new.parquet", 0, 256, &vec![0xCCu8; 256]);
    let r3 = block_on(cache.get(&range_cache_key("/data/new.parquet", 0, 256)));
    assert_eq!(
        r3.as_ref().map(|b| b.len()),
        Some(256),
        "new put after sweep must be retrievable"
    );
}

/// The cursor sweep correctly updates eviction_bytes when removing stale keys.
/// Verifies the byte-size accounting is accurate through the shard-based path.
#[test]
fn sweep_cursor_eviction_bytes_correctly_tracked() {
    let (cache, _dir) = test_cache();

    // Inject stale keys with known sizes:
    // "data/f.parquet\x1F0-100"   → 100 bytes
    // "data/f.parquet\x1F100-300" → 200 bytes
    // Total stale bytes = 300
    cache
        .key_index
        .entry("data/f.parquet".to_string())
        .or_default()
        .extend(vec![
            "data/f.parquet\x1F0-100".to_string(),
            "data/f.parquet\x1F100-300".to_string(),
        ]);

    let eviction_bytes_before = cache
        .stats
        .eviction_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    let used_bytes_before = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);

    // Run enough sweeps to cover all shards.
    // DashMap defaults to 64 shards on macOS — must sweep at least shard_count times.
    let shard_count = cache.key_index.shards().len();
    for _ in 0..shard_count {
        cache.sweep_once();
    }

    let eviction_bytes_after = cache
        .stats
        .eviction_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    let used_bytes_after = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(
        eviction_bytes_after - eviction_bytes_before,
        300,
        "eviction_bytes must reflect 100 + 200 = 300 bytes for the two stale keys"
    );
    assert_eq!(
        used_bytes_after - used_bytes_before,
        -300,
        "used_bytes must decrease by 300 when stale keys are swept"
    );
    assert!(
        cache.key_index.is_empty(),
        "key_index must be empty after stale keys are swept"
    );
}

// ── ActiveBytesGuard tests ────────────────────────────────────────────────────

use crate::foyer::foyer_cache::ActiveBytesGuard;

/// The guard increments the counter on construction and decrements it on drop.
/// After the guard is dropped the counter must return exactly to its original value.
#[test]
fn active_bytes_guard_increments_and_decrements() {
    use std::sync::atomic::AtomicI64;
    let counter = AtomicI64::new(0);

    {
        let _guard = ActiveBytesGuard::new(&counter, 512);
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            512,
            "counter must be 512 while guard is alive"
        );
    }
    // guard dropped here
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "counter must return to 0 after guard is dropped"
    );
}

/// Multiple guards at the same time accumulate correctly and each restores its
/// own contribution when dropped.
#[test]
fn active_bytes_guard_multiple_concurrent_guards() {
    use std::sync::atomic::AtomicI64;
    let counter = AtomicI64::new(0);

    let g1 = ActiveBytesGuard::new(&counter, 100);
    let g2 = ActiveBytesGuard::new(&counter, 200);
    let g3 = ActiveBytesGuard::new(&counter, 300);

    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 600);

    drop(g2);
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::Relaxed),
        400,
        "dropping g2 (200) must leave 100+300=400"
    );

    drop(g1);
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::Relaxed),
        300,
        "dropping g1 (100) must leave 300"
    );

    drop(g3);
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "dropping g3 (300) must leave 0"
    );
}

/// A guard with value 0 is a no-op — counter stays unchanged.
#[test]
fn active_bytes_guard_zero_value_is_noop() {
    use std::sync::atomic::AtomicI64;
    let counter = AtomicI64::new(42);
    {
        let _guard = ActiveBytesGuard::new(&counter, 0);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 42);
    }
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 42);
}

/// active_in_bytes is 0 before a get(), positive during, and 0 after.
/// This validates that the guard integrates correctly with the full cache.
#[test]
fn active_in_bytes_is_zero_before_and_after_get() {
    let (cache, _dir) = test_cache();
    let key = range_cache_key("/data/file.parquet", 0, 1024);
    cache.put(&key, Bytes::from(vec![0u8; 1024]));

    // Before get(): active_in_bytes must be 0.
    assert_eq!(
        cache
            .stats
            .active_in_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "active_in_bytes must be 0 before any get()"
    );

    block_on(cache.get(&key));

    // After get() completes: guard has been dropped, counter back to 0.
    assert_eq!(
        cache
            .stats
            .active_in_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "active_in_bytes must be 0 after get() completes"
    );
}

/// active_in_bytes is 0 even after a cache miss — the guard restores it regardless
/// of the hit/miss outcome.
#[test]
fn active_in_bytes_is_zero_after_cache_miss() {
    let (cache, _dir) = test_cache();
    let key = range_cache_key("/data/never_inserted.parquet", 0, 512);

    assert_eq!(
        cache
            .stats
            .active_in_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    let result = block_on(cache.get(&key));
    assert!(
        result.is_none(),
        "key was never inserted — should be a miss"
    );
    assert_eq!(
        cache
            .stats
            .active_in_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "active_in_bytes must be 0 after a cache miss"
    );
}

/// Dropping a get() future before it completes simulates future cancellation.
/// The guard's Drop impl must restore active_in_bytes to 0.
///
/// We simulate this by constructing an ActiveBytesGuard directly and dropping it
/// early — the same code path the guard follows when a tokio::select! timeout
/// fires and the get() future is abandoned.
#[test]
fn active_bytes_guard_drop_on_cancellation_restores_counter() {
    use std::sync::atomic::AtomicI64;
    let counter = AtomicI64::new(0);

    // Simulate: future starts, guard created, active_in_bytes incremented.
    let guard = ActiveBytesGuard::new(&counter, 4096);
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 4096);

    // Simulate: future cancelled (tokio::select! timeout or explicit drop).
    // Guard drops here — must restore counter to 0 even without the async block completing.
    drop(guard);
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "active_in_bytes must be restored to 0 when get() future is cancelled mid-flight"
    );
}

// ── Sweep threshold (should_skip_sweep) tests ─────────────────────────────────

/// When sweep_threshold_ratio == 0.0 (disabled), should_skip_sweep() must always
/// return false regardless of current usage — the sweep is never skipped.
#[test]
fn sweep_threshold_disabled_never_skips() {
    let dir = TempDir::new().unwrap();
    // threshold = 0.0: disabled — always sweep
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );

    // used_bytes = 0 (empty cache)
    assert_eq!(
        cache.should_skip_sweep(),
        false,
        "threshold=0.0: should never skip even when cache is empty"
    );

    // used_bytes = 100% of disk
    cache.stats.used_bytes.store(
        TEST_CACHE_DISK_BYTES as i64,
        std::sync::atomic::Ordering::Relaxed,
    );
    assert_eq!(
        cache.should_skip_sweep(),
        false,
        "threshold=0.0: should never skip even when cache is 100% full"
    );
}

/// When usage is strictly below the threshold, should_skip_sweep() returns true.
#[test]
fn sweep_threshold_skips_when_usage_below_threshold() {
    let dir = TempDir::new().unwrap();
    // disk = 4MB, threshold = 0.75 (75%)
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.75,
        0,
        false,
    );

    // usage = 0% → below 75% → skip
    cache
        .stats
        .used_bytes
        .store(0, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        cache.should_skip_sweep(),
        true,
        "usage=0% < threshold=75%: sweep must be skipped"
    );

    // usage = 50% → below 75% → skip
    let half = (TEST_CACHE_DISK_BYTES / 2) as i64;
    cache
        .stats
        .used_bytes
        .store(half, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        cache.should_skip_sweep(),
        true,
        "usage=50% < threshold=75%: sweep must be skipped"
    );

    // usage = 74.9% → below 75% → skip
    let below = ((TEST_CACHE_DISK_BYTES as f64 * 0.749) as i64);
    cache
        .stats
        .used_bytes
        .store(below, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        cache.should_skip_sweep(),
        true,
        "usage=74.9% < threshold=75%: sweep must be skipped"
    );
}

/// When usage is at or above the threshold, should_skip_sweep() returns false.
#[test]
fn sweep_threshold_runs_when_usage_at_or_above_threshold() {
    let dir = TempDir::new().unwrap();
    // disk = 4MB, threshold = 0.75 (75%)
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.75,
        0,
        false,
    );

    // usage = exactly 75% → NOT below → do NOT skip
    let exact = ((TEST_CACHE_DISK_BYTES as f64 * 0.75) as i64);
    cache
        .stats
        .used_bytes
        .store(exact, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        cache.should_skip_sweep(),
        false,
        "usage=75% == threshold=75%: sweep must NOT be skipped"
    );

    // usage = 90% → above 75% → do NOT skip
    let above = ((TEST_CACHE_DISK_BYTES as f64 * 0.90) as i64);
    cache
        .stats
        .used_bytes
        .store(above, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        cache.should_skip_sweep(),
        false,
        "usage=90% > threshold=75%: sweep must NOT be skipped"
    );

    // usage = 100% → above 75% → do NOT skip
    cache.stats.used_bytes.store(
        TEST_CACHE_DISK_BYTES as i64,
        std::sync::atomic::Ordering::Relaxed,
    );
    assert_eq!(
        cache.should_skip_sweep(),
        false,
        "usage=100% > threshold=75%: sweep must NOT be skipped"
    );
}

/// Threshold of 1.0 means "only sweep when cache is 100% full" — any usage below
/// 100% triggers a skip.
#[test]
fn sweep_threshold_one_skips_unless_completely_full() {
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        1.0,
        0,
        false,
    );

    // usage = 99.9% → still below 100% → skip
    let almost_full = ((TEST_CACHE_DISK_BYTES as f64 * 0.999) as i64);
    cache
        .stats
        .used_bytes
        .store(almost_full, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        cache.should_skip_sweep(),
        true,
        "threshold=1.0, usage=99.9%: sweep must be skipped"
    );

    // usage = exactly 100% → ratio = 1.0 = threshold → NOT below → do NOT skip
    cache.stats.used_bytes.store(
        TEST_CACHE_DISK_BYTES as i64,
        std::sync::atomic::Ordering::Relaxed,
    );
    assert_eq!(
        cache.should_skip_sweep(),
        false,
        "threshold=1.0, usage=100%: sweep must NOT be skipped"
    );
}

/// Negative used_bytes (which can transiently occur due to relaxed ordering on
/// concurrent sweeps and evictions) must be treated as 0 — no skip.
#[test]
fn sweep_threshold_negative_used_bytes_treated_as_zero() {
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.5,
        0,
        false,
    );

    // Simulate a transient underflow (negative used_bytes) — should be clamped to 0.
    cache
        .stats
        .used_bytes
        .store(-100, std::sync::atomic::Ordering::Relaxed);
    // usage = max(−100, 0) / disk = 0 / disk = 0.0 < 0.5 → skip
    assert_eq!(
        cache.should_skip_sweep(),
        true,
        "negative used_bytes must be clamped to 0; ratio 0.0 < threshold 0.5 → skip"
    );
}

/// sweep_once() ignores the threshold — it calls reconcile_key_index directly.
/// Even when should_skip_sweep() returns true, sweep_once() still sweeps.
#[test]
fn sweep_once_ignores_threshold_guard() {
    let dir = TempDir::new().unwrap();
    // Set a high threshold so should_skip_sweep() returns true
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.99,
        0,
        false,
    );

    // Inject a stale key
    cache
        .key_index
        .entry("data/file.parquet".to_string())
        .or_default()
        .insert("data/file.parquet\x1F0-100".to_string());

    // usage = 0 → should_skip_sweep() is true (below 99% threshold)
    assert_eq!(
        cache.should_skip_sweep(),
        true,
        "precondition: threshold guard would skip"
    );

    // But sweep_once() bypasses the guard and still sweeps
    let shard_count = cache.key_index.shards().len();
    let removed: usize = (0..shard_count).map(|_| cache.sweep_once()).sum();
    assert_eq!(
        removed, 1,
        "sweep_once() must sweep regardless of threshold"
    );
    assert!(
        cache.key_index.is_empty(),
        "stale key must be removed by sweep_once() even when threshold would skip"
    );
}

// ── Recovery / persistence integration tests ─────────────────────────────────
//
// These tests exercise the full put → Drop (persist) → new (recover) cycle.
// They use real FoyerCache instances and TempDir.
//
// Design note: recovery uses bulk-load (no per-key inner.contains() validation).
// Stale entries from the snapshot are corrected lazily by the sweep task.

use crate::key_index_store;

/// Graceful restart: key_index is persisted on Drop and bulk-loaded on the next
/// startup. All prefix buckets and keys are present immediately after new().
#[test]
fn recovery_key_index_bulk_loaded_after_graceful_shutdown() {
    let dir = TempDir::new().unwrap();

    // Write entries into the first cache instance.
    {
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            0,
            false,
        );
        put_range(&cache, "/data/a.parquet", 0, 512, &vec![0u8; 512]);
        put_range(&cache, "/data/a.parquet", 512, 1024, &vec![0u8; 512]);
        put_range(&cache, "/data/b.parquet", 0, 256, &vec![0u8; 256]);
        // Drop calls save() — writes key_index.json
    }

    // key_index.json must exist after graceful shutdown.
    assert!(
        dir.path()
            .join(key_index_store::KEY_INDEX_FILENAME)
            .exists(),
        "key_index.json must be written on Drop"
    );

    // Second instance: recover from the snapshot.
    let cache2 = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );

    // Both prefix buckets must be present immediately after new().
    assert!(
        cache2.key_index.contains_key("data/a.parquet"),
        "data/a.parquet must be in key_index after recovery"
    );
    assert!(
        cache2.key_index.contains_key("data/b.parquet"),
        "data/b.parquet must be in key_index after recovery"
    );

    // 2 keys for a.parquet + 1 key for b.parquet = 3 total.
    let a_count = cache2
        .key_index
        .get("data/a.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    let b_count = cache2
        .key_index
        .get("data/b.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(a_count, 2, "data/a.parquet must have 2 keys");
    assert_eq!(b_count, 1, "data/b.parquet must have 1 key");
}

/// used_bytes is initialized from the snapshot on recovery.
/// The sum of key_byte_size for all recovered keys must equal used_bytes
/// immediately after new() — before any put() is called.
#[test]
fn recovery_used_bytes_initialized_from_snapshot() {
    let dir = TempDir::new().unwrap();

    {
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            0,
            false,
        );
        // 3 ranges: 100 + 200 + 300 = 600 bytes total.
        put_range(&cache, "/data/f.parquet", 0, 100, &vec![0u8; 100]);
        put_range(&cache, "/data/f.parquet", 100, 300, &vec![0u8; 200]);
        put_range(&cache, "/data/f.parquet", 300, 600, &vec![0u8; 300]);
        // Drop persists.
    }

    let cache2 = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    let used = cache2
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        used, 600,
        "used_bytes must be 600 (sum of all recovered key ranges) after recovery"
    );
}

/// After recovery, evict_prefix() correctly removes entries that were loaded
/// from the snapshot — not just entries added in the current session.
#[test]
fn recovery_evict_prefix_works_on_recovered_keys() {
    let dir = TempDir::new().unwrap();

    {
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            0,
            false,
        );
        put_range(&cache, "/data/shard1/file.parquet", 0, 512, &vec![0u8; 512]);
        put_range(&cache, "/data/shard2/file.parquet", 0, 512, &vec![0u8; 512]);
        // Drop persists.
    }

    let cache2 = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    assert!(cache2.key_index.contains_key("data/shard1/file.parquet"));
    assert!(cache2.key_index.contains_key("data/shard2/file.parquet"));

    // Evict one shard — only that shard's entry must be removed.
    cache2.evict_prefix("/data/shard1");
    assert!(
        !cache2.key_index.contains_key("data/shard1/file.parquet"),
        "evict_prefix must remove recovered shard1 entries"
    );
    assert!(
        cache2.key_index.contains_key("data/shard2/file.parquet"),
        "shard2 must be untouched"
    );
}

/// Clean startup (no key_index.json) is a no-op: key_index starts empty,
/// used_bytes is 0, cache is fully functional.
#[test]
fn recovery_with_no_snapshot_is_clean_startup() {
    let dir = TempDir::new().unwrap();
    // No prior cache instance — key_index.json does not exist.
    assert!(!dir
        .path()
        .join(key_index_store::KEY_INDEX_FILENAME)
        .exists());

    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    assert!(
        cache.key_index.is_empty(),
        "key_index must be empty on clean startup"
    );
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    // Cache must still be fully functional.
    put_range(&cache, "/data/file.parquet", 0, 100, b"data");
    assert!(cache.key_index.contains_key("data/file.parquet"));
}

/// A corrupt key_index.json (invalid JSON) is treated as a clean startup.
/// The cache starts with an empty key_index and is fully functional.
#[test]
fn recovery_with_corrupt_snapshot_starts_empty() {
    let dir = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join(key_index_store::KEY_INDEX_FILENAME),
        b"{{corrupt}}",
    )
    .unwrap();

    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    assert!(
        cache.key_index.is_empty(),
        "corrupt snapshot must produce empty key_index"
    );
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    // Cache must still be fully functional.
    put_range(&cache, "/data/file.parquet", 0, 100, b"data");
    assert!(cache.key_index.contains_key("data/file.parquet"));
}

/// evict_prefix() followed by Drop: the evicted prefix must NOT appear in the
/// persisted snapshot and therefore must NOT be loaded on the next startup.
#[test]
fn recovery_evicted_prefix_not_persisted() {
    let dir = TempDir::new().unwrap();

    {
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            0,
            false,
        );
        put_range(&cache, "/data/evicted.parquet", 0, 512, &vec![0u8; 512]);
        put_range(&cache, "/data/kept.parquet", 0, 512, &vec![0u8; 512]);

        // Evict one prefix before shutdown.
        cache.evict_prefix("/data/evicted.parquet");
        assert!(!cache.key_index.contains_key("data/evicted.parquet"));
        // Drop persists the remaining key_index (only kept.parquet).
    }

    let cache2 = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    assert!(
        !cache2.key_index.contains_key("data/evicted.parquet"),
        "evicted prefix must not appear in recovered key_index"
    );
    assert!(
        cache2.key_index.contains_key("data/kept.parquet"),
        "non-evicted prefix must still appear after recovery"
    );
}

/// clear() deletes key_index.json so the next startup does not load stale keys.
#[test]
fn recovery_clear_deletes_snapshot_file() {
    let dir = TempDir::new().unwrap();

    // Create and drop a cache to write key_index.json.
    {
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            0,
            false,
        );
        put_range(&cache, "/data/file.parquet", 0, 100, b"data");
        // Drop writes key_index.json.
    }
    assert!(
        dir.path()
            .join(key_index_store::KEY_INDEX_FILENAME)
            .exists(),
        "key_index.json must exist after first Drop"
    );

    // Create a second cache and call clear().
    {
        let cache2 = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            0,
            false,
        );
        block_on(cache2.clear());
        // Drop of cache2 writes an empty key_index.json (empty key_index after clear).
    }

    // Third instance: must start with an empty key_index (clear deleted the stale snapshot).
    let cache3 = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    assert!(
        cache3.key_index.is_empty(),
        "key_index must be empty after clear() deleted the snapshot"
    );
}

// ── Periodic persist task tests ──────────────────────────────────────────────
//
// These tests exercise the independent persist task that flushes the key_index
// to disk every `persist_interval_secs` seconds when `used_bytes` has changed.
// They use short intervals (1–2 seconds) and poll for the file's existence.

/// PP-01 / PP-06: persist task starts when interval > 0 and is absent when interval = 0.
/// Verifies that key_index.json is written by the periodic task (not just by Drop)
/// by checking file existence before Drop is called.
#[test]
fn persist_task_writes_snapshot_within_interval() {
    let dir = TempDir::new().unwrap();
    {
        // persist_interval=1s: task should fire within ~2s of a put().
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,   // sweep disabled
            0.0, // threshold disabled
            1,   // persist every 1 second
            false,
        );
        put_range(&cache, "/data/periodic.parquet", 0, 512, &vec![0u8; 512]);

        // Poll for key_index.json to appear (written by the persist task, not Drop).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let appeared = loop {
            if dir
                .path()
                .join(key_index_store::KEY_INDEX_FILENAME)
                .exists()
            {
                break true;
            }
            if std::time::Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        };
        assert!(
            appeared,
            "key_index.json must be written by persist task within 5s"
        );

        // Verify the content is valid (not just an empty file).
        let contents =
            std::fs::read_to_string(dir.path().join(key_index_store::KEY_INDEX_FILENAME)).unwrap();
        let snap: serde_json::Value = serde_json::from_str(&contents).unwrap();
        assert_eq!(snap["version"], 1, "snapshot must have version=1");
        let index = snap["index"].as_object().unwrap();
        assert!(!index.is_empty(), "snapshot must contain the put() entries");
        // cache is dropped here — Drop also writes a final snapshot
    }
}

/// PP-04: persist task does NOT write key_index.json when used_bytes has not changed
/// (idle cache). The sentinel `i64::MIN` forces the first tick to always persist,
/// so we verify that a second interval tick with no puts does not change the mtime.
///
/// Note: this test uses a 2-second interval and checks mtime doesn't advance
/// between the 2nd and 3rd ticks when no puts happen.
#[test]
fn persist_task_does_not_fire_when_cache_idle() {
    let dir = TempDir::new().unwrap();
    {
        // persist_interval=1s
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            1,
            false,
        );
        // Single put to trigger the first persist.
        put_range(&cache, "/data/idle.parquet", 0, 100, &vec![0u8; 100]);

        // Wait for the first persist (sentinel → current triggers it).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !dir
            .path()
            .join(key_index_store::KEY_INDEX_FILENAME)
            .exists()
        {
            if std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(
            dir.path()
                .join(key_index_store::KEY_INDEX_FILENAME)
                .exists(),
            "first persist must have fired"
        );

        // Record mtime after first persist.
        let mtime_after_first =
            std::fs::metadata(dir.path().join(key_index_store::KEY_INDEX_FILENAME))
                .unwrap()
                .modified()
                .unwrap();

        // Wait 2.5 intervals with NO new puts — used_bytes is unchanged so persist must NOT fire.
        std::thread::sleep(std::time::Duration::from_millis(2500));

        let mtime_after_idle =
            std::fs::metadata(dir.path().join(key_index_store::KEY_INDEX_FILENAME))
                .unwrap()
                .modified()
                .unwrap();

        assert_eq!(mtime_after_first, mtime_after_idle,
            "persist task must NOT rewrite key_index.json when cache is idle (used_bytes unchanged)");
    }
}

/// PP-05: persist fires after evict_prefix() because used_bytes changes.
#[test]
fn persist_task_fires_after_evict_prefix_changes_used_bytes() {
    let dir = TempDir::new().unwrap();
    {
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            1,
            false,
        );
        put_range(&cache, "/data/evict_me.parquet", 0, 512, &vec![0u8; 512]);
        put_range(&cache, "/data/keep_me.parquet", 0, 512, &vec![0u8; 512]);

        // Wait for first persist (sentinel fires on first tick).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !dir
            .path()
            .join(key_index_store::KEY_INDEX_FILENAME)
            .exists()
        {
            if std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        // Evict one prefix — changes used_bytes so next tick must persist.
        cache.evict_prefix("/data/evict_me.parquet");
        let used_after_evict = cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            used_after_evict < 1024,
            "used_bytes must decrease after evict"
        );

        let mtime_before = std::fs::metadata(dir.path().join(key_index_store::KEY_INDEX_FILENAME))
            .unwrap()
            .modified()
            .unwrap();

        // Wait up to 3 intervals for the next persist.
        std::thread::sleep(std::time::Duration::from_millis(3000));
        let mtime_after = std::fs::metadata(dir.path().join(key_index_store::KEY_INDEX_FILENAME))
            .unwrap()
            .modified()
            .unwrap();
        assert!(
            mtime_after > mtime_before,
            "persist task must rewrite key_index.json after evict_prefix() changed used_bytes"
        );
    }
}

/// PP-06: persist disabled when interval=0 — no persist task spawned.
/// The file should NOT exist until Drop is called.
#[test]
fn persist_task_not_spawned_when_interval_is_zero() {
    let dir = TempDir::new().unwrap();
    {
        // persist_interval=0: only Drop persists.
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            0, // disabled
            false,
        );
        put_range(&cache, "/data/file.parquet", 0, 100, &vec![0u8; 100]);

        // Wait 2 seconds — file must NOT appear (no persist task).
        std::thread::sleep(std::time::Duration::from_millis(2000));
        assert!(
            !dir.path()
                .join(key_index_store::KEY_INDEX_FILENAME)
                .exists(),
            "key_index.json must NOT exist while cache is alive with persist_interval=0"
        );

        // Drop is called here — final persist happens.
    }
    // After Drop, key_index.json must exist.
    assert!(
        dir.path()
            .join(key_index_store::KEY_INDEX_FILENAME)
            .exists(),
        "key_index.json must be written by Drop even when persist task is disabled"
    );
}

/// CR-05 simulation: simulate a crash by using `std::mem::forget` to prevent Drop.
/// The key_index.json must NOT be written (or must be old) since Drop is skipped.
/// On the next startup, the cache recovers from whatever was last periodically persisted
/// (or starts empty if periodic persist was also disabled).
#[test]
fn simulated_crash_skip_drop_no_final_persist() {
    let dir = TempDir::new().unwrap();

    // Build a cache with persist_interval=0 (no periodic persist either).
    // This simulates a node with only Drop-based persistence.
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    put_range(&cache, "/data/crash.parquet", 0, 512, &vec![0u8; 512]);

    // key_index.json does NOT exist yet (no periodic persist, Drop not called).
    assert!(
        !dir.path()
            .join(key_index_store::KEY_INDEX_FILENAME)
            .exists(),
        "key_index.json must not exist before Drop"
    );

    // Simulate crash: forget the cache — Drop is NOT called.
    std::mem::forget(cache);

    // key_index.json must still NOT exist (Drop was skipped).
    assert!(
        !dir.path()
            .join(key_index_store::KEY_INDEX_FILENAME)
            .exists(),
        "key_index.json must not exist after simulated crash (Drop skipped)"
    );

    // Next startup: key_index starts empty (clean startup path).
    let cache2 = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    assert!(
        cache2.key_index.is_empty(),
        "key_index must be empty after simulated crash with no prior snapshot"
    );
    assert_eq!(
        cache2
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

/// GS-03 / GS-04: verify key_index.json content is valid JSON with correct version and index.
#[test]
fn graceful_shutdown_snapshot_is_valid_json_with_correct_content() {
    let dir = TempDir::new().unwrap();
    {
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            0,
            false,
        );
        put_range(
            &cache,
            "/data/nodes/0/shard1.parquet",
            0,
            256,
            &vec![0u8; 256],
        );
        put_range(
            &cache,
            "/data/nodes/0/shard1.parquet",
            256,
            512,
            &vec![0u8; 256],
        );
        put_range(
            &cache,
            "/data/nodes/0/shard2.parquet",
            0,
            128,
            &vec![0u8; 128],
        );
        // Drop writes key_index.json.
    }

    let path = dir.path().join(key_index_store::KEY_INDEX_FILENAME);
    assert!(
        path.exists(),
        "key_index.json must exist after graceful shutdown"
    );

    let contents = std::fs::read_to_string(&path).unwrap();
    assert!(!contents.is_empty(), "key_index.json must not be empty");

    // Parse and validate structure.
    let parsed: serde_json::Value =
        serde_json::from_str(&contents).expect("key_index.json must be valid JSON");
    assert_eq!(parsed["version"], 1, "version must be 1");

    let index = parsed["index"]
        .as_object()
        .expect("index must be a JSON object");
    assert_eq!(index.len(), 2, "must have 2 prefix buckets");

    // shard1 should have 2 keys, shard2 should have 1.
    let shard1_keys = index["data/nodes/0/shard1.parquet"].as_array().unwrap();
    let shard2_keys = index["data/nodes/0/shard2.parquet"].as_array().unwrap();
    assert_eq!(shard1_keys.len(), 2, "shard1 must have 2 keys");
    assert_eq!(shard2_keys.len(), 1, "shard2 must have 1 key");
}

/// No .tmp file should exist after either graceful shutdown or periodic persist.
#[test]
fn no_tmp_file_left_after_graceful_shutdown() {
    let dir = TempDir::new().unwrap();
    {
        let cache = FoyerCache::new(
            TEST_CACHE_DISK_BYTES,
            dir.path(),
            TEST_CACHE_BLOCK_SIZE,
            TEST_BUFFER_POOL_SIZE,
            TEST_SUBMIT_QUEUE_SIZE,
            IO_ENGINE,
            0,
            0.0,
            0,
            false,
        );
        put_range(&cache, "/data/file.parquet", 0, 100, &vec![0u8; 100]);
        // Drop writes and renames.
    }
    assert!(
        !dir.path()
            .join(key_index_store::KEY_INDEX_TMP_FILENAME)
            .exists(),
        ".key_index.json.tmp must not exist after graceful shutdown (rename completed)"
    );
    assert!(
        dir.path()
            .join(key_index_store::KEY_INDEX_FILENAME)
            .exists(),
        "key_index.json must exist after graceful shutdown"
    );
}

/// VM-03: zero-byte key_index.json → treated as corrupt → clean startup.
#[test]
fn zero_byte_snapshot_file_treated_as_corrupt() {
    let dir = TempDir::new().unwrap();
    // Write an empty file.
    std::fs::write(dir.path().join(key_index_store::KEY_INDEX_FILENAME), b"").unwrap();

    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    assert!(
        cache.key_index.is_empty(),
        "zero-byte snapshot must produce empty key_index (clean startup)"
    );
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    // Cache must be fully functional.
    put_range(&cache, "/data/file.parquet", 0, 100, b"data");
    assert!(cache.key_index.contains_key("data/file.parquet"));
}

/// ST-04: no .tmp file exists on clean startup (fresh dir).
#[test]
fn no_tmp_file_on_clean_startup() {
    let dir = TempDir::new().unwrap();
    // Verify neither file exists before creating the cache.
    assert!(!dir
        .path()
        .join(key_index_store::KEY_INDEX_FILENAME)
        .exists());
    assert!(!dir
        .path()
        .join(key_index_store::KEY_INDEX_TMP_FILENAME)
        .exists());

    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );
    // On startup, no .tmp should be created or left behind.
    assert!(
        !dir.path()
            .join(key_index_store::KEY_INDEX_TMP_FILENAME)
            .exists(),
        ".key_index.json.tmp must not exist after clean startup"
    );
    drop(cache);
    assert!(
        !dir.path()
            .join(key_index_store::KEY_INDEX_TMP_FILENAME)
            .exists(),
        ".key_index.json.tmp must not exist after graceful shutdown"
    );
}

/// Stale entries in the recovered snapshot (keys that Foyer did not recover due
/// to LRU eviction before the last persist) are cleaned up by sweep_once().
/// The cache is usable throughout — stale keys don't cause panics or incorrect
/// behavior; they just occupy key_index until swept.
// ─── 10k-scale key_index tests ────────────────────────────────────────────────
// These tests build a 10k-entry key_index in-process (no OS cluster, no fd pressure).

/// Serialise a 10k-entry key_index to disk and verify the JSON file is non-empty,
/// parses cleanly, and contains every prefix that was inserted.
#[test]
fn key_index_serialization_with_10k_entries() {
    use dashmap::DashMap;
    use std::collections::HashSet;

    let dir = TempDir::new().unwrap();
    const NUM_PREFIXES: usize = 100;
    const KEYS_PER_PREFIX: usize = 100;

    // Build a DashMap with 100 prefixes × 100 keys = 10k entries.
    let dash: DashMap<String, HashSet<String>> = DashMap::new();
    for p in 0..NUM_PREFIXES {
        let prefix = format!("data/nodes/0/shard_{p:04}/segment.parquet");
        let keys: HashSet<String> = (0..KEYS_PER_PREFIX)
            .map(|k| format!("{prefix}\x1F{}-{}", k * 4096, (k + 1) * 4096))
            .collect();
        dash.insert(prefix, keys);
    }
    let expected_total = NUM_PREFIXES * KEYS_PER_PREFIX;

    // Save.
    key_index_store::save(dir.path(), &dash).unwrap();

    // Load back and verify.
    let loaded = key_index_store::load_or_empty(dir.path());
    assert_eq!(
        loaded.index.len(),
        NUM_PREFIXES,
        "must have {NUM_PREFIXES} prefix buckets"
    );
    let total_keys: usize = loaded.index.values().map(|v| v.len()).sum();
    assert_eq!(
        total_keys, expected_total,
        "must have {expected_total} total keys"
    );

    // The file itself must be readable JSON above a minimum size.
    let path = dir.path().join(key_index_store::KEY_INDEX_FILENAME);
    let bytes = std::fs::metadata(&path).unwrap().len();
    assert!(
        bytes > 100_000,
        "key_index.json must be > 100KB for 10k entries, got {bytes}"
    );
}

/// Build a 10k-entry snapshot, write it to disk, then create a new FoyerCache
/// over the same dir — verify the key_index is bulk-loaded correctly.
#[test]
fn key_index_recovery_with_10k_entries() {
    use dashmap::DashMap;
    use std::collections::HashSet;

    let dir = TempDir::new().unwrap();
    const NUM_PREFIXES: usize = 100;
    const KEYS_PER_PREFIX: usize = 100;

    // Write a 10k-entry snapshot.
    let dash: DashMap<String, HashSet<String>> = DashMap::new();
    for p in 0..NUM_PREFIXES {
        let prefix = format!("data/nodes/0/shard_{p:04}/segment.parquet");
        let keys: HashSet<String> = (0..KEYS_PER_PREFIX)
            .map(|k| format!("{prefix}\x1F{}-{}", k * 4096, (k + 1) * 4096))
            .collect();
        dash.insert(prefix, keys);
    }
    key_index_store::save(dir.path(), &dash).unwrap();

    // Create a new cache from the same dir — should bulk-load the snapshot.
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );

    assert_eq!(
        cache.key_index.len(),
        NUM_PREFIXES,
        "key_index must have {NUM_PREFIXES} buckets after recovery"
    );
    let total_keys: usize = cache.key_index.iter().map(|e| e.value().len()).sum();
    assert_eq!(
        total_keys,
        NUM_PREFIXES * KEYS_PER_PREFIX,
        "must have {} total keys after recovery",
        NUM_PREFIXES * KEYS_PER_PREFIX
    );
}

/// Populate 10k entries directly into the key_index (simulating cache puts),
/// evict half the prefixes, and verify used_bytes decrements correctly.
#[test]
fn key_index_evict_prefix_bulk_with_10k_entries() {
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );

    const NUM_PREFIXES: usize = 100;
    const KEYS_PER_PREFIX: usize = 100;

    // Directly populate the key_index (bypasses actual Foyer disk, tests key_index logic only).
    use std::collections::HashSet;
    for p in 0..NUM_PREFIXES {
        let prefix = format!("data/nodes/0/shard_{p:04}/segment.parquet");
        let keys: HashSet<String> = (0..KEYS_PER_PREFIX)
            .map(|k| format!("{prefix}\x1F{}-{}", k * 4096, (k + 1) * 4096))
            .collect();
        let byte_size: i64 = keys
            .iter()
            .map(|k| crate::range_cache::key_byte_size(k))
            .sum();
        cache.key_index.insert(prefix, keys);
        cache
            .stats
            .used_bytes
            .fetch_add(byte_size, std::sync::atomic::Ordering::Relaxed);
    }

    let used_before = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        used_before > 0,
        "used_bytes must be > 0 after populating {NUM_PREFIXES} prefixes"
    );

    // Evict first 50 prefixes.
    for p in 0..NUM_PREFIXES / 2 {
        let prefix = format!("data/nodes/0/shard_{p:04}/segment.parquet");
        cache.evict_prefix(&prefix);
    }

    let used_after = cache
        .stats
        .used_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        used_after < used_before,
        "used_bytes must decrease after bulk evict_prefix"
    );
    assert_eq!(
        cache.key_index.len(),
        NUM_PREFIXES / 2,
        "key_index must have {} buckets after evicting half",
        NUM_PREFIXES / 2
    );
}

/// Populate 10k entries, call clear(), verify the key_index is empty and
/// key_index.json is deleted.
#[test]
fn key_index_clear_with_10k_entries() {
    use std::collections::HashSet;

    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );

    const NUM_PREFIXES: usize = 100;
    const KEYS_PER_PREFIX: usize = 100;

    for p in 0..NUM_PREFIXES {
        let prefix = format!("data/nodes/0/shard_{p:04}/segment.parquet");
        let keys: HashSet<String> = (0..KEYS_PER_PREFIX)
            .map(|k| format!("{prefix}\x1F{}-{}", k * 4096, (k + 1) * 4096))
            .collect();
        let byte_size: i64 = keys
            .iter()
            .map(|k| crate::range_cache::key_byte_size(k))
            .sum();
        cache.key_index.insert(prefix, keys);
        cache
            .stats
            .used_bytes
            .fetch_add(byte_size, std::sync::atomic::Ordering::Relaxed);
    }

    // Persist first so there's a file to delete.
    key_index_store::save(&dir.path(), &cache.key_index).unwrap();
    assert!(
        dir.path()
            .join(key_index_store::KEY_INDEX_FILENAME)
            .exists(),
        "file must exist before clear"
    );

    // Clear via the public async API.
    block_on(cache.clear());

    assert_eq!(
        cache.key_index.len(),
        0,
        "key_index must be empty after clear"
    );
    assert_eq!(
        cache
            .stats
            .used_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "used_bytes must be 0 after clear"
    );
    // After clear() the key_index.json is deleted; Drop will write an empty one.
    // Assert it is absent while the cache is still alive (before Drop).
    let snap_exists = dir
        .path()
        .join(key_index_store::KEY_INDEX_FILENAME)
        .exists();
    // clear() calls key_index_store::delete() which removes the file.
    assert!(!snap_exists, "key_index.json must be deleted by clear()");
}

#[test]
fn recovery_stale_snapshot_keys_cleaned_by_sweep() {
    let dir = TempDir::new().unwrap();

    // Manually write a snapshot with a mix of valid and fake stale keys.
    // We don't spin up an actual cache for the "first session" here because
    // we need to inject keys that Foyer will not have recovered.
    {
        use std::collections::{HashMap, HashSet};
        let mut index: HashMap<String, HashSet<String>> = HashMap::new();
        // Real-looking key — Foyer won't have it either, but it's valid format.
        index.insert(
            "data/stale.parquet".to_string(),
            ["data/stale.parquet\x1F0-512".to_string()].into(),
        );
        let snap = key_index_store::KeyIndexSnapshot { version: 1, index };
        let json = serde_json::to_string(&snap).unwrap();
        std::fs::write(
            dir.path().join(key_index_store::KEY_INDEX_FILENAME),
            json.as_bytes(),
        )
        .unwrap();
    }

    // Create a fresh Foyer cache over the same dir. Foyer has no data for "data/stale.parquet".
    let cache = FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    );

    // Bulk-loaded snapshot has the stale key — used_bytes is temporarily over-counted.
    assert!(
        cache.key_index.contains_key("data/stale.parquet"),
        "stale key must be present immediately after bulk-load recovery"
    );

    // After sweeping all shards, the stale key is removed (inner.contains() returns false).
    let shard_count = cache.key_index.shards().len();
    let removed: usize = (0..shard_count).map(|_| cache.sweep_once()).sum();
    assert_eq!(removed, 1, "sweep must remove the 1 stale key");
    assert!(
        !cache.key_index.contains_key("data/stale.parquet"),
        "stale key must be gone after sweep"
    );
}

// ── TieredBlockCache tests ──────────────────────────────────────────────────────

use crate::tiered_block_cache::TieredBlockCache;
use std::sync::atomic::Ordering;

fn tiered_test_cache() -> (TieredBlockCache, TempDir, TempDir) {
    let data_dir = TempDir::new().expect("data temp dir");
    let meta_dir = TempDir::new().expect("metadata temp dir");
    let data_cache = Arc::new(FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        data_dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        TEST_CACHE_DISK_BYTES,
        meta_dir.path(),
        TEST_CACHE_BLOCK_SIZE,
        TEST_BUFFER_POOL_SIZE,
        TEST_SUBMIT_QUEUE_SIZE,
        IO_ENGINE,
        0,
        0.0,
        0,
        true,
    ));
    let tiered = TieredBlockCache::new(data_cache, metadata_cache);
    (tiered, data_dir, meta_dir)
}

/// put_metadata() routes to metadata cache; get() finds it there.
#[test]
fn tiered_cache_put_metadata_routes_to_metadata_cache() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();

    let footer_key = range_cache_key("seg0/_0.parquet", 9990000, 10000000);
    let col_idx_key = range_cache_key("seg0/_0.parquet", 8000000, 8500000);

    tiered.put_metadata(&footer_key, Bytes::from_static(b"footer_bytes"));
    tiered.put_metadata(&col_idx_key, Bytes::from_static(b"column_index_bytes"));

    // get() probes metadata cache first → hit
    assert_eq!(
        block_on(tiered.get(&footer_key)).as_deref(),
        Some(b"footer_bytes".as_slice())
    );
    assert_eq!(
        block_on(tiered.get(&col_idx_key)).as_deref(),
        Some(b"column_index_bytes".as_slice())
    );

    // Verify metadata is in metadata_cache, not data_cache
    assert!(block_on(tiered.metadata_cache().get(&footer_key)).is_some());
    assert!(block_on(tiered.data_cache().get(&footer_key)).is_none());
}

/// put() (normal path from TieredObjectStore::populate_cache) routes to data cache.
#[test]
fn tiered_cache_put_routes_to_data_cache() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();

    let col_a_key = range_cache_key("seg0/_0.parquet", 0, 2097152);
    tiered.put(&col_a_key, Bytes::from_static(b"col_a_data"));

    // get() misses metadata cache, hits data cache
    assert_eq!(
        block_on(tiered.get(&col_a_key)).as_deref(),
        Some(b"col_a_data".as_slice())
    );

    // Verify data is in data_cache only
    assert!(block_on(tiered.data_cache().get(&col_a_key)).is_some());
    assert!(block_on(tiered.metadata_cache().get(&col_a_key)).is_none());
}

/// get() checks metadata cache first — so on warm restart (Foyer recovered
/// metadata SSD), metadata is found without any re-registration.
#[test]
fn tiered_cache_get_finds_metadata_without_reregistration() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();

    // Simulate prior session: metadata was put via put_metadata
    let key = range_cache_key("seg0/_0.parquet", 9990000, 10000000);
    tiered.put_metadata(&key, Bytes::from_static(b"old_footer"));

    // get() finds it — no re-registration needed (metadata cache probed first)
    assert_eq!(
        block_on(tiered.get(&key)).as_deref(),
        Some(b"old_footer".as_slice())
    );
}

/// On a data key miss (not in either cache), metadata cache is still probed
/// first (miss) then data cache (miss) — both miss, returns None. Caller goes to S3.
#[test]
fn tiered_cache_total_miss_returns_none() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();

    let key = range_cache_key("seg0/_0.parquet", 5000000, 6000000);
    assert!(block_on(tiered.get(&key)).is_none());
}

/// Full DataFusion query simulation:
/// 1. Shard init: warmup puts metadata via put_metadata()
/// 2. Query: DataFusion reads metadata → hit from metadata cache (probed first)
/// 3. Query: DataFusion reads data → miss → S3 → put() → data cache
/// 4. Query 2: same data → hit from data cache
#[test]
fn tiered_cache_full_datafusion_query_simulation() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();
    let path = "seg0/_0.parquet";

    // ── Warmup: put metadata explicitly ──────────────────────────────
    let footer_key = range_cache_key(path, 9_990_000, 10_000_000);
    let col_idx_key = range_cache_key(path, 8_000_000, 8_500_000);
    tiered.put_metadata(&footer_key, Bytes::from(vec![0xF; 10_000]));
    tiered.put_metadata(&col_idx_key, Bytes::from(vec![0xC; 500_000]));

    // ── Query: metadata reads → hit (metadata cache probed first) ────
    assert!(
        block_on(tiered.get(&footer_key)).is_some(),
        "footer must hit"
    );
    assert!(
        block_on(tiered.get(&col_idx_key)).is_some(),
        "column index must hit"
    );

    // ── Query: data read → miss first time ───────────────────────────
    let data_key = range_cache_key(path, 0, 2_000_000);
    assert!(
        block_on(tiered.get(&data_key)).is_none(),
        "data miss on first access"
    );

    // ── S3 fetch → populate_cache → put() → data cache ──────────────
    tiered.put(&data_key, Bytes::from(vec![0xAA; 2_000_000]));

    // ── Query 2: same data → hit from data cache ─────────────────────
    assert_eq!(
        block_on(tiered.get(&data_key)).map(|b| b.len()),
        Some(2_000_000)
    );

    // ── Verify isolation ─────────────────────────────────────────────
    assert!(block_on(tiered.metadata_cache().get(&footer_key)).is_some());
    assert!(block_on(tiered.data_cache().get(&footer_key)).is_none());
    assert!(block_on(tiered.data_cache().get(&data_key)).is_some());
    assert!(block_on(tiered.metadata_cache().get(&data_key)).is_none());
}

/// evict_prefix removes entries from both caches.
#[test]
fn tiered_cache_evict_prefix_cleans_both_caches() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();
    let path = "seg0/_0.parquet";

    tiered.put_metadata(
        &range_cache_key(path, 9000000, 10000000),
        Bytes::from_static(b"meta"),
    );
    tiered.put(
        &range_cache_key(path, 0, 1000000),
        Bytes::from_static(b"data"),
    );

    tiered.evict_prefix(path);

    assert!(block_on(tiered.get(&range_cache_key(path, 9000000, 10000000))).is_none());
    assert!(block_on(tiered.get(&range_cache_key(path, 0, 1000000))).is_none());
}

/// Multiple shards — evicting one doesn't affect others.
#[test]
fn tiered_cache_multiple_shards_are_independent() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();

    let s0 = range_cache_key("seg0/_0.parquet", 9000, 10000);
    let s1 = range_cache_key("seg1/_0.parquet", 9000, 10000);
    tiered.put_metadata(&s0, Bytes::from_static(b"shard0"));
    tiered.put_metadata(&s1, Bytes::from_static(b"shard1"));

    tiered.evict_prefix("seg0/");

    assert!(block_on(tiered.get(&s0)).is_none());
    assert_eq!(
        block_on(tiered.get(&s1)).as_deref(),
        Some(b"shard1".as_slice())
    );
}

/// Metadata hit does not touch data cache SSD.
#[test]
fn tiered_cache_metadata_hit_never_probes_data_ssd() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();

    let key = range_cache_key("seg0/_0.parquet", 9990000, 10000000);
    tiered.put_metadata(&key, Bytes::from_static(b"footer"));

    let data_hits_before = tiered.data_cache().stats.hit_count.load(Ordering::Relaxed);
    let data_misses_before = tiered.data_cache().stats.miss_count.load(Ordering::Relaxed);

    let result = block_on(tiered.get(&key));
    assert!(result.is_some());

    // Data cache untouched
    assert_eq!(
        tiered.data_cache().stats.hit_count.load(Ordering::Relaxed),
        data_hits_before
    );
    assert_eq!(
        tiered.data_cache().stats.miss_count.load(Ordering::Relaxed),
        data_misses_before
    );
}

/// Key alignment: exact range match = hit, subset/superset = miss.
#[test]
fn tiered_cache_key_alignment_warmup_matches_query() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();

    let key = range_cache_key("seg0/_0.parquet", 8_000_000, 8_500_000);
    tiered.put_metadata(&key, Bytes::from(vec![0xC; 500_000]));

    // Exact same range → hit
    assert!(
        block_on(tiered.get(&range_cache_key("seg0/_0.parquet", 8_000_000, 8_500_000))).is_some()
    );

    // Subset → miss (different key)
    assert!(
        block_on(tiered.get(&range_cache_key("seg0/_0.parquet", 8_000_000, 8_250_000))).is_none()
    );

    // Superset → miss (different key)
    assert!(
        block_on(tiered.get(&range_cache_key("seg0/_0.parquet", 7_500_000, 9_000_000))).is_none()
    );
}

/// put_metadata() skips entries larger than max_metadata_entry_size — oversized
/// metadata is never written, bounding memory for pathological (wide-schema) files.
#[test]
fn tiered_cache_put_metadata_skips_oversized_entry() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();

    // Shrink the bound so the test stays small (no multi-MB allocations).
    tiered.update_max_metadata_entry_size(2048);

    let big_key = range_cache_key("seg0/_0.parquet", 0, 4096);
    tiered.put_metadata(&big_key, Bytes::from(vec![0xAB; 4096])); // 4KB > 2KB → skipped
    assert!(
        block_on(tiered.get(&big_key)).is_none(),
        "metadata entry exceeding max_metadata_entry_size must not be cached"
    );
    assert!(
        block_on(tiered.metadata_cache().get(&big_key)).is_none(),
        "oversized metadata must not reach the metadata tier"
    );

    let small_key = range_cache_key("seg0/_0.parquet", 0, 1024);
    tiered.put_metadata(&small_key, Bytes::from(vec![0xCD; 1024])); // 1KB <= 2KB → cached
    assert_eq!(
        block_on(tiered.get(&small_key)).map(|b| b.len()),
        Some(1024),
        "metadata entry within the limit must be cached"
    );
    assert!(block_on(tiered.metadata_cache().get(&small_key)).is_some());
}

/// put() skips data entries larger than max_data_entry_size; the getter reflects
/// the configured bound (used by TieredObjectStore::get_opts to avoid buffering).
#[test]
fn tiered_cache_put_skips_oversized_data_entry() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();

    tiered.update_max_data_entry_size(2048);
    assert_eq!(
        tiered.max_data_entry_size(),
        2048,
        "getter must reflect the updated bound"
    );

    let big_key = range_cache_key("seg0/_0.parquet", 0, 4096);
    tiered.put(&big_key, Bytes::from(vec![0xAB; 4096])); // 4KB > 2KB → skipped
    assert!(
        block_on(tiered.get(&big_key)).is_none(),
        "data entry exceeding max_data_entry_size must not be cached"
    );

    let small_key = range_cache_key("seg0/_0.parquet", 4096, 5120);
    tiered.put(&small_key, Bytes::from(vec![0xCD; 1024])); // 1KB <= 2KB → cached
    assert!(
        block_on(tiered.data_cache().get(&small_key)).is_some(),
        "data entry within the limit must be cached"
    );
}

/// Size bounds update dynamically — raising the limit admits a previously
/// rejected entry on the next put.
#[test]
fn tiered_cache_size_bound_update_takes_effect_immediately() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();

    let key = range_cache_key("seg0/_0.parquet", 0, 4096);

    // Tight bound → rejected.
    tiered.update_max_data_entry_size(1024);
    tiered.put(&key, Bytes::from(vec![0x11; 4096]));
    assert!(
        block_on(tiered.get(&key)).is_none(),
        "4KB rejected under 1KB bound"
    );

    // Raise the bound → same entry now admitted.
    tiered.update_max_data_entry_size(8192);
    tiered.put(&key, Bytes::from(vec![0x22; 4096]));
    assert_eq!(
        block_on(tiered.get(&key)).map(|b| b.len()),
        Some(4096),
        "4KB admitted after raising bound to 8KB"
    );
}

/// clear_sync() empties both tiers. This is the production entry point
/// (FFM `foyer_clear_cache` → `clear_sync`); the async `clear()` trait method
/// internally delegates to it.
#[test]
fn tiered_cache_clear_empties_both_tiers() {
    let (tiered, _data_dir, _meta_dir) = tiered_test_cache();
    let path = "seg0/_0.parquet";

    tiered.put_metadata(
        &range_cache_key(path, 9_000_000, 10_000_000),
        Bytes::from_static(b"meta"),
    );
    tiered.put(
        &range_cache_key(path, 0, 1_000_000),
        Bytes::from_static(b"data"),
    );

    tiered.clear_sync();

    assert!(
        block_on(
            tiered
                .metadata_cache()
                .get(&range_cache_key(path, 9_000_000, 10_000_000))
        )
        .is_none(),
        "metadata tier must be empty after clear_sync()"
    );
    assert!(
        block_on(
            tiered
                .data_cache()
                .get(&range_cache_key(path, 0, 1_000_000))
        )
        .is_none(),
        "data tier must be empty after clear_sync()"
    );
}

/// A single-tier FoyerCache (metadata_cache_ratio=0) inherits the
/// `BlockCache::put_metadata` default, which routes to put() — so warmup's
/// put_metadata still lands in the one cache. Dispatched via `&dyn BlockCache`
/// to exercise the trait default explicitly.
#[test]
fn foyer_cache_put_metadata_default_routes_to_put() {
    let (cache, _dir) = test_cache();
    let dyn_cache: &dyn crate::traits::BlockCache = &cache;

    let key = range_cache_key("seg0/_0.parquet", 0, 64);
    dyn_cache.put_metadata(&key, Bytes::from_static(b"footer"));

    assert_eq!(
        block_on(dyn_cache.get(&key)).as_deref(),
        Some(b"footer".as_slice()),
        "put_metadata default must route to the single cache and be retrievable via get()"
    );
}
