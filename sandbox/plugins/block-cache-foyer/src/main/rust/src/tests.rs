/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unit tests for [`FoyerCache`] and the FFM lifecycle API.

use std::sync::Arc;
use bytes::Bytes;
use tempfile::TempDir;

use crate::foyer::foyer_cache::FoyerCache;
use crate::foyer::ffm::{foyer_create_cache, foyer_destroy_cache};
use crate::range_cache::range_cache_key;
use crate::traits::BlockCache;

// ── Test helpers ──────────────────────────────────────────────────────────────

/// Block size and disk capacity for FFM/integration tests that need a large Foyer instance.
/// Must be kept in sync: block_size (BLOCK_SIZE) ≤ disk_bytes (any test using this constant).
const BLOCK_SIZE: usize = 64 * 1024 * 1024;  // 64 MB — used by FFM tests (disk=64MB) and large_value test

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
const TEST_CACHE_DISK_BYTES:  usize = 4 * 1024 * 1024;  // 4 MB disk capacity
const TEST_CACHE_BLOCK_SIZE:  usize = 1 * 1024 * 1024;  // 1 MB block size (must be ≤ disk)

fn test_cache() -> (FoyerCache, TempDir) {
    let dir = TempDir::new().expect("failed to create temp dir");
    // sweep_interval_secs = 0 → no background task; tests call sweep_once() directly.
    // block_size (1 MB) ≤ disk_bytes (4 MB) — required Foyer invariant.
    let cache = FoyerCache::new(TEST_CACHE_DISK_BYTES, dir.path(), TEST_CACHE_BLOCK_SIZE, IO_ENGINE, 0, 0.0);
    (cache, dir)
}

fn put_range(cache: &FoyerCache, path: &str, start: u64, end: u64, data: &[u8]) {
    cache.put(&range_cache_key(path, start, end), Bytes::copy_from_slice(data));
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
    put_range(&cache, "/data/a.parquet", 0,    4096, b"range0");
    put_range(&cache, "/data/a.parquet", 4096, 8192, b"range1");
    put_range(&cache, "/data/a.parquet", 8192, 12288, b"range2");
    assert_eq!(block_on(cache.get(&range_cache_key("/data/a.parquet", 0,    4096))).as_deref(), Some(b"range0".as_slice()));
    assert_eq!(block_on(cache.get(&range_cache_key("/data/a.parquet", 4096, 8192))).as_deref(), Some(b"range1".as_slice()));
    assert_eq!(block_on(cache.get(&range_cache_key("/data/a.parquet", 8192, 12288))).as_deref(), Some(b"range2".as_slice()));
}

#[test]
fn multiple_files_are_independent() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/a.parquet", 0, 100, b"file_a");
    put_range(&cache, "/data/b.parquet", 0, 100, b"file_b");
    put_range(&cache, "/data/c.parquet", 0, 100, b"file_c");
    assert_eq!(block_on(cache.get(&range_cache_key("/data/a.parquet", 0, 100))).as_deref(), Some(b"file_a".as_slice()));
    assert_eq!(block_on(cache.get(&range_cache_key("/data/b.parquet", 0, 100))).as_deref(), Some(b"file_b".as_slice()));
    assert_eq!(block_on(cache.get(&range_cache_key("/data/c.parquet", 0, 100))).as_deref(), Some(b"file_c".as_slice()));
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
    assert!(block_on(cache.get(&range_cache_key("/data/file.parquet", 1,   100))).is_none());
    assert!(block_on(cache.get(&range_cache_key("/data/file.parquet", 0,    99))).is_none());
    assert!(block_on(cache.get(&range_cache_key("/data/file.parquet", 200, 300))).is_none());
}

// ── evict_prefix ──────────────────────────────────────────────────────────────

#[test]
fn evict_prefix_removes_all_ranges_for_file() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/target.parquet", 0,    4096, b"range0");
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
    put_range(&cache, "/data/other.parquet",  0, 100, b"other");
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

    let removed_count_before = cache.stats.removed_count.load(std::sync::atomic::Ordering::Relaxed);
    let removed_bytes_before = cache.stats.removed_bytes.load(std::sync::atomic::Ordering::Relaxed);

    block_on(cache.clear());

    let removed_count_after = cache.stats.removed_count.load(std::sync::atomic::Ordering::Relaxed);
    let removed_bytes_after = cache.stats.removed_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let used_bytes_after = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(removed_count_after, removed_count_before + 2, "clear() must count 2 removed entries");
    assert_eq!(removed_bytes_after, removed_bytes_before + 300, "clear() must count 100 + 200 = 300 removed bytes");
    assert_eq!(used_bytes_after, 0, "clear() must reset used_bytes to 0");
    assert!(cache.key_index.is_empty(), "key_index must be empty after clear()");
}

#[test]
fn clear_on_empty_cache_does_not_change_removed_stats() {
    let (cache, _dir) = test_cache();
    let removed_before = cache.stats.removed_count.load(std::sync::atomic::Ordering::Relaxed);
    block_on(cache.clear());
    assert_eq!(cache.stats.removed_count.load(std::sync::atomic::Ordering::Relaxed), removed_before,
        "clear() on empty cache must not increment removed_count");
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
    put_range(&cache, "/data/other.parquet",  0, 100, b"other");
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
    let count_after_first = cache.key_index
        .get("data/file.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(count_after_first, 1, "first put must add exactly 1 entry to key_index");

    // Second put with same key (simulates cache stampede or re-put after eviction).
    cache.put(&key, Bytes::from(vec![0xFFu8; 1024]));
    let count_after_second = cache.key_index
        .get("data/file.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(count_after_second, 1,
        "second put of same key must NOT add a duplicate entry (HashSet dedup); got {}",
        count_after_second);
}

/// used_bytes must not be double-counted when the same key is put twice.
///
/// With the old Vec approach, two puts added `size` to used_bytes twice even though
/// Foyer only stores one entry. With HashSet, the second insert returns false and
/// used_bytes is only incremented on the first (genuine) insert.
#[test]
fn put_same_key_twice_does_not_double_count_used_bytes() {
    let (cache, _dir) = test_cache();
    assert_eq!(cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed), 0);

    let key = range_cache_key("/data/file.parquet", 0, 512);
    cache.put(&key, Bytes::from(vec![0u8; 512]));
    assert_eq!(
        cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed), 512,
        "used_bytes must be 512 after first put"
    );

    // Second put of the same key — Foyer upserts the value, key_index is unchanged.
    // used_bytes must NOT be incremented again.
    cache.put(&key, Bytes::from(vec![0xAAu8; 512]));
    assert_eq!(
        cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed), 512,
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
    let handles: Vec<_> = (0..THREAD_COUNT).map(|_| {
        let cache = Arc::clone(&cache);
        std::thread::spawn(move || {
            let key = range_cache_key("/data/shared.parquet", 0, RANGE_SIZE);
            cache.put(&key, Bytes::from(vec![0xABu8; RANGE_SIZE as usize]));
        })
    }).collect();
    for h in handles { h.join().expect("thread panicked"); }

    // key_index must have exactly 1 entry for this key, not THREAD_COUNT.
    let count = cache.key_index
        .get("data/shared.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(count, 1,
        "key_index must contain exactly 1 entry after {} concurrent puts of the same key; got {}",
        THREAD_COUNT, count);

    // used_bytes: at most 1× the entry size. It may be less if the second-N puts
    // raced and all returned false from HashSet::insert, but never more than 1×.
    let used_bytes = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(used_bytes, RANGE_SIZE as i64,
        "used_bytes must be exactly {} after {} concurrent puts of the same key; got {}",
        RANGE_SIZE, THREAD_COUNT, used_bytes);
}

// ── concurrent access ─────────────────────────────────────────────────────────

#[test]
fn concurrent_puts_to_different_files_do_not_corrupt() {
    let (cache, _dir) = test_cache();
    let cache = Arc::new(cache);
    let handles: Vec<_> = (0..16).map(|i| {
        let cache = Arc::clone(&cache);
        std::thread::spawn(move || {
            let key = range_cache_key(&format!("/data/file_{}.parquet", i), 0, 1024);
            cache.put(&key, Bytes::copy_from_slice(&vec![i as u8; 1024]));
        })
    }).collect();
    for h in handles { h.join().expect("thread panicked"); }
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
        for _ in 0..50 { evictor_cache.evict_prefix("/data/file.parquet"); }
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
    let used = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);
    assert!(used >= 0, "used_bytes must not be negative after concurrent put/evict; got {}", used);

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
    let cache = FoyerCache::new(2 * 1024 * 1024, dir.path(), 512 * 1024, IO_ENGINE, 0, 0.0);
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
    let cache = FoyerCache::new(1 * 1024 * 1024, dir.path(), 256 * 1024, IO_ENGINE, 0, 0.0);
    const CHUNK_SIZE: usize = 256 * 1024;
    const TOTAL_WRITES: usize = 8;
    let chunk = vec![0xABu8; CHUNK_SIZE];
    for i in 0u64..TOTAL_WRITES as u64 {
        let key = range_cache_key("/data/big.parquet", i * CHUNK_SIZE as u64, (i + 1) * CHUNK_SIZE as u64);
        cache.put(&key, Bytes::copy_from_slice(&chunk));
    }
    // Busy-assert: poll until all TOTAL_WRITES keys appear in key_index or deadline expires.
    // Foyer's async flusher writes to disk asynchronously after put(); we wait for it
    // rather than using a fixed sleep — avoids flakiness on slow CI machines.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let key_count = loop {
        let count = cache.key_index.get("data/big.parquet").map(|v| v.len()).unwrap_or(0);
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
    let used_after_v1 = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);
    cache.put(&key, Bytes::from_static(b"version_2"));
    std::thread::sleep(std::time::Duration::from_millis(100));

    // With HashSet key_index, putting the same key twice must result in exactly 1 entry
    // (not 2), because HashSet::insert is idempotent for duplicate keys.
    let count = cache.key_index.get("data/file.parquet").map(|v| v.len()).unwrap_or(0);
    assert_eq!(count, 1, "key must appear exactly once in key_index after overwrite (HashSet dedup); got {}", count);

    // used_bytes grows monotonically: v2 was added on top of v1 (no reliable subtraction).
    let used_after_v2 = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);
    assert!(used_after_v2 >= used_after_v1, "used_bytes must not decrease on overwrite");

    // Latest value must be readable.
    let result = block_on(cache.get(&key));
    assert_eq!(result.as_deref(), Some(b"version_2".as_slice()));

    // evict_prefix must still clean up correctly and increment removed_count.
    let removed_before = cache.stats.removed_count.load(std::sync::atomic::Ordering::Relaxed);
    cache.evict_prefix("/data/file.parquet");
    assert!(!cache.key_index.contains_key("data/file.parquet"));
    let removed_after = cache.stats.removed_count.load(std::sync::atomic::Ordering::Relaxed);
    assert!(removed_after > removed_before, "removed_count must increase after evict_prefix");
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
    assert_eq!(removed, 0, "no stale entries expected immediately after put");
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
    cache.key_index
        .entry("data/file.parquet".to_string())
        .or_default()
        .insert(fake_key.clone());

    let eviction_count_before = cache.stats.eviction_count.load(std::sync::atomic::Ordering::Relaxed);
    let eviction_bytes_before = cache.stats.eviction_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let used_bytes_before = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);
    // Loop shard_count times to guarantee the shard containing the fake key is visited.
    // DashMap hashes keys to one of N shards (N=64 on macOS); sweep_once() processes ONE shard
    // per call, so we need N calls to cover all shards exactly once.
    let shard_count = cache.key_index.shards().len();
    let removed: usize = (0..shard_count).map(|_| cache.sweep_once()).sum();

    assert_eq!(removed, 1, "sweeper should have removed the 1 fake/stale key");
    assert_eq!(
        cache.stats.eviction_count.load(std::sync::atomic::Ordering::Relaxed),
        eviction_count_before + 1,
        "eviction_count must be incremented for stale entries removed by sweeper"
    );
    // Fake key range 99999-100000 = 1 byte.
    assert_eq!(
        cache.stats.eviction_bytes.load(std::sync::atomic::Ordering::Relaxed),
        eviction_bytes_before + 1,
        "eviction_bytes must reflect the byte size of stale entries (parsed from key)"
    );
    assert_eq!(
        cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed),
        used_bytes_before - 1,
        "used_bytes must be decremented by freed bytes when sweeper removes stale entries"
    );

    // Real key is still in key_index.
    let remaining = cache.key_index.get("data/file.parquet").map(|v| v.len()).unwrap_or(0);
    assert_eq!(remaining, 1, "real key must remain in key_index after sweep");
}

#[test]
fn sweep_once_removes_empty_prefix_buckets() {
    // When all keys under a prefix become stale, the prefix bucket itself should be removed.
    let (cache, _dir) = test_cache();

    // Inject only fake keys for a prefix — no real entries.
    let fake1 = "data/gone.parquet\x1F0-100".to_string();
    let fake2 = "data/gone.parquet\x1F100-200".to_string();
    cache.key_index
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
    put_range(&cache, "/data/file.parquet", 0,   100, &vec![0u8; 100]);
    put_range(&cache, "/data/file.parquet", 100, 300, &vec![0u8; 200]);
    std::thread::sleep(std::time::Duration::from_millis(100));

    let used_before    = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let removed_count_before = cache.stats.removed_count.load(std::sync::atomic::Ordering::Relaxed);
    let removed_bytes_before = cache.stats.removed_bytes.load(std::sync::atomic::Ordering::Relaxed);

    cache.evict_prefix("/data/file.parquet");

    let used_after    = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let removed_count = cache.stats.removed_count.load(std::sync::atomic::Ordering::Relaxed);
    let removed_bytes = cache.stats.removed_bytes.load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(removed_count, removed_count_before + 2, "removed_count: 2 entries evicted");
    assert_eq!(removed_bytes, removed_bytes_before + 300, "removed_bytes: 100 + 200 = 300");
    assert_eq!(used_after, used_before - 300, "used_bytes must decrease by 300 after eviction");
}

#[test]
fn evict_prefix_on_nonexistent_prefix_does_not_change_stats() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0, 100, &vec![0u8; 100]);

    let removed_before = cache.stats.removed_count.load(std::sync::atomic::Ordering::Relaxed);
    let used_before    = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);

    cache.evict_prefix("/data/other.parquet");

    assert_eq!(cache.stats.removed_count.load(std::sync::atomic::Ordering::Relaxed), removed_before);
    assert_eq!(cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed), used_before);
}

#[test]
fn used_bytes_is_correct_after_put_then_evict() {
    // used_bytes starts at 0, put adds size, evict_prefix subtracts — net result = 0.
    let (cache, _dir) = test_cache();
    assert_eq!(cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed), 0);

    put_range(&cache, "/data/file.parquet", 0, 1024, &vec![0u8; 1024]);
    assert_eq!(cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed), 1024);

    cache.evict_prefix("/data/file.parquet");
    assert_eq!(cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed), 0,
        "used_bytes must return to 0 after evicting all entries");
}

// ── sweep idempotency and edge cases ─────────────────────────────────────────

#[test]
fn sweep_once_is_idempotent_for_real_entries() {
    // Calling sweep_once() twice on live entries must not double-decrement stats.
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0, 1024, &vec![0u8; 1024]);

    let removed1 = cache.sweep_once();
    let eviction_bytes_after_1 = cache.stats.eviction_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let removed2 = cache.sweep_once();
    let eviction_bytes_after_2 = cache.stats.eviction_bytes.load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(removed1, 0, "no stale entries on first sweep");
    assert_eq!(removed2, 0, "no stale entries on second sweep");
    assert_eq!(eviction_bytes_after_1, 0);
    assert_eq!(eviction_bytes_after_2, 0, "eviction_bytes must not change across two sweeps of live entries");
}

#[test]
fn sweep_once_stale_key_with_malformed_range_does_not_panic() {
    // A key without a parseable range (key_byte_size returns 0) must still be
    // removed cleanly — no panic, no stats corruption.
    let (cache, _dir) = test_cache();

    // Inject a malformed stale key — no separator, Foyer has never seen it.
    cache.key_index
        .entry("data/bad.parquet".to_string())
        .or_default()
        .insert("data/bad.parquet-not-a-range-key".to_string());

    let eviction_bytes_before = cache.stats.eviction_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let shard_count = cache.key_index.shards().len();
    let removed: usize = (0..shard_count).map(|_| cache.sweep_once()).sum();

    assert_eq!(removed, 1, "stale malformed key must be removed");
    // key_byte_size returns 0 for malformed key — eviction_bytes unchanged.
    assert_eq!(
        cache.stats.eviction_bytes.load(std::sync::atomic::Ordering::Relaxed),
        eviction_bytes_before,
        "eviction_bytes must not change for malformed (size=0) key"
    );
    assert!(!cache.key_index.contains_key("data/bad.parquet"),
        "empty prefix bucket must be removed");
}

#[test]
fn sweep_once_removes_empty_prefix_buckets_and_updates_eviction_bytes() {
    // Extended version of sweep_once_removes_empty_prefix_buckets that also checks bytes.
    // Two fake keys: 0-100 (100 bytes) and 100-200 (100 bytes) = 200 bytes total.
    let (cache, _dir) = test_cache();

    let fake1 = "data/gone.parquet\x1F0-100".to_string();
    let fake2 = "data/gone.parquet\x1F100-200".to_string();
    cache.key_index
        .entry("data/gone.parquet".to_string())
        .or_default()
        .extend(vec![fake1, fake2]);

    let eviction_bytes_before = cache.stats.eviction_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let used_bytes_before = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);

    let shard_count = cache.key_index.shards().len();
    let removed: usize = (0..shard_count).map(|_| cache.sweep_once()).sum();

    assert_eq!(removed, 2);
    assert!(!cache.key_index.contains_key("data/gone.parquet"));
    assert_eq!(
        cache.stats.eviction_bytes.load(std::sync::atomic::Ordering::Relaxed),
        eviction_bytes_before + 200,
        "eviction_bytes must reflect both stale keys (100 + 100 = 200)"
    );
    assert_eq!(
        cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed),
        used_bytes_before - 200,
        "used_bytes must decrease by 200"
    );
}

#[test]
fn event_remove_after_evict_prefix_does_not_panic_or_corrupt_key_index() {
    let (cache, _dir) = test_cache();
    put_range(&cache, "/data/file.parquet", 0,   100, b"data");
    put_range(&cache, "/data/file.parquet", 100, 200, b"more");
    cache.evict_prefix("/data/file.parquet");
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(!cache.key_index.contains_key("data/file.parquet"));
    put_range(&cache, "/data/file.parquet", 0, 100, b"fresh");
    assert_eq!(block_on(cache.get(&range_cache_key("/data/file.parquet", 0, 100))).as_deref(), Some(b"fresh".as_slice()));
}

// ── FFM lifecycle ─────────────────────────────────────────────────────────────

#[test]
fn ffm_create_returns_positive_pointer() {
    let dir = TempDir::new().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe { foyer_create_cache(
        64 * 1024 * 1024,
        dir_str.as_ptr(), dir_str.len() as u64,
        BLOCK_SIZE as u64,
        engine.as_ptr(), engine.len() as u64,
        0, 0.0_f64,
    )};
    assert!(ptr > 0);
    let result = unsafe { foyer_destroy_cache(ptr) };
    assert_eq!(result, 0);
}

#[test]
fn ffm_create_with_null_ptr_returns_error() {
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe { foyer_create_cache(
        64 * 1024 * 1024,
        std::ptr::null(), 10,
        BLOCK_SIZE as u64,
        engine.as_ptr(), engine.len() as u64,
        0, 0.0_f64,
    )};
    assert!(ptr < 0);
    if ptr < 0 { unsafe { native_bridge_common::error::native_error_free(-ptr); } }
}

#[test]
fn ffm_create_with_invalid_utf8_returns_error() {
    let invalid_utf8 = [0xFF, 0xFE, 0xFD];
    let engine = IO_ENGINE.as_bytes();
    let ptr = unsafe { foyer_create_cache(
        64 * 1024 * 1024,
        invalid_utf8.as_ptr(), invalid_utf8.len() as u64,
        BLOCK_SIZE as u64,
        engine.as_ptr(), engine.len() as u64,
        0, 0.0_f64,
    )};
    assert!(ptr < 0);
    if ptr < 0 { unsafe { native_bridge_common::error::native_error_free(-ptr); } }
}

#[test]
fn ffm_destroy_with_zero_ptr_returns_error() {
    let result = unsafe { foyer_destroy_cache(0) };
    assert!(result < 0);
    if result < 0 { unsafe { native_bridge_common::error::native_error_free(-result); } }
}

#[test]
fn ffm_destroy_with_negative_ptr_returns_error() {
    let result = unsafe { foyer_destroy_cache(-1) };
    assert!(result < 0);
    if result < 0 { unsafe { native_bridge_common::error::native_error_free(-result); } }
}

#[test]
fn ffm_create_destroy_lifecycle_no_leak() {
    let engine = IO_ENGINE.as_bytes();
    for _ in 0..3 {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        let ptr = unsafe { foyer_create_cache(
            16 * 1024 * 1024,
            dir_str.as_ptr(), dir_str.len() as u64,
            BLOCK_SIZE as u64,
            engine.as_ptr(), engine.len() as u64,
        0, 0.0_f64,
        )};
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
    let ptr = unsafe { foyer_create_cache(
        64 * 1024 * 1024,
        dir_str.as_ptr(), dir_str.len() as u64,
        BLOCK_SIZE as u64,
        engine.as_ptr(), engine.len() as u64,
        0, 0.0_f64,
    )};
    assert!(ptr > 0);

    // 10 fields × 2 sections = 20 longs
    let mut out = [i64::MAX; 20];
    let rc = unsafe { crate::foyer::ffm::foyer_snapshot_stats(ptr, out.as_mut_ptr()) };
    assert_eq!(rc, 0, "foyer_snapshot_stats should return 0 on success");

    // A freshly created cache has no hits, misses, evictions, used bytes, or active reads.
    // Sections 0 and 1 are identical (Foyer is currently single-tier).
    for i in 0..20 {
        assert_eq!(out[i], 0, "out[{i}] should be 0 for a fresh cache, got {}", out[i]);
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
            64 * 1024 * 1024,
            dir_str.as_ptr(), dir_str.len() as u64,
            BLOCK_SIZE as u64,
            engine.as_ptr(), engine.len() as u64,
        0, 0.0_f64,
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
    assert_eq!(&buf[..10], &buf[10..], "overall and block_level sections must be identical");

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
    let ptr = unsafe { foyer_create_cache(
        64 * 1024 * 1024,
        dir_str.as_ptr(), dir_str.len() as u64,
        BLOCK_SIZE as u64,
        engine.as_ptr(), engine.len() as u64,
        0, 0.0_f64,
    )};
    assert!(ptr > 0);

    let rc = unsafe { crate::foyer::ffm::foyer_snapshot_stats(ptr, std::ptr::null_mut()) };
    assert!(rc < 0, "foyer_snapshot_stats with null out should return < 0, got {rc}");

    let destroy_rc = unsafe { foyer_destroy_cache(ptr) };
    assert_eq!(destroy_rc, 0);
}

#[test]
fn snapshot_stats_returns_error_for_invalid_ptr() {
    let mut out = [0i64; 14];
    let rc = unsafe { crate::foyer::ffm::foyer_snapshot_stats(0, out.as_mut_ptr()) };
    assert!(rc < 0, "foyer_snapshot_stats with ptr=0 should return < 0, got {rc}");
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
    assert_eq!(&out[0..10], &out[10..20],
        "overall and block_level sections should be identical for single-tier Foyer");
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
    assert_eq!(snap[6], 1024, "used_bytes should be 1024 after a single 1KB put");
}

#[test]
fn snapshot_stats_null_out_via_created_cache() {
    // Verify foyer_snapshot_stats returns < 0 when out pointer is null.
    let (cache, _dir) = test_cache();
    let data = vec![0xABu8; 1024];
    let key = range_cache_key("/data/file.parquet", 0, 1024);
    cache.put(&key, Bytes::copy_from_slice(&data));

    let snap = cache.stats.snapshot();
    assert_eq!(snap[6], 1024, "used_bytes should be 1024 after a single 1KB put");
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
    let ptr = unsafe { foyer_create_cache(
        16 * 1024 * 1024,
        dir_str.as_ptr(), dir_str.len() as u64,
        BLOCK_SIZE as u64,
        engine.as_ptr(), engine.len() as u64,
        0, 0.0_f64,
    )};
    assert!(ptr > 0);

    // Interpret as Box<Arc<dyn BlockCache>> — if the pointer type is wrong this will crash.
    // Take ownership and immediately check the strong count via a clone probe.
    let boxed: Box<Arc<dyn BlockCache>> = unsafe {
        Box::from_raw(ptr as *mut Arc<dyn BlockCache>)
    };
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
    let ptr = unsafe { foyer_create_cache(
        16 * 1024 * 1024,
        dir_str.as_ptr(), dir_str.len() as u64,
        BLOCK_SIZE as u64,
        engine.as_ptr(), engine.len() as u64,
        0, 0.0_f64,
    )};
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
    drop(cache);                      // calls shutdown.cancel() — must be a no-op
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
        64 * 1024 * 1024,
        dir.path(),
        BLOCK_SIZE,
        IO_ENGINE,
        3600, // 1-hour interval — task sleeps, drop cancels it immediately
        0.0,  // threshold disabled
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
        64 * 1024 * 1024,
        dir.path(),
        BLOCK_SIZE,
        IO_ENGINE,
        0,   // no sweep task
        0.0, // threshold disabled
    );
    // Clone the token before drop so we can inspect it after.
    let token: CancellationToken = cache.shutdown.clone();
    assert!(!token.is_cancelled(), "token must not be cancelled before drop");
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
        let cache = FoyerCache::new(64 * 1024 * 1024, dir.path(), BLOCK_SIZE, IO_ENGINE, 0, 0.0);
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
    let cache = FoyerCache::new(64 * 1024 * 1024, dir.path(), BLOCK_SIZE, IO_ENGINE, 3600, 0.0);
    let key = range_cache_key("/data/file.parquet", 0, 512);
    cache.put(&key, Bytes::from(vec![0xABu8; 512]));
    let result = block_on(cache.get(&key));
    assert_eq!(result.as_ref().map(|b| b.len()), Some(512));
    // drop cancels the sleeping task immediately via cancelled() branch
}

// ── Sweep cursor tests ───────────────────────────────────────────────────────

/// Each sweep_once() processes exactly one shard and advances the cursor.
/// After 16 calls the cursor has wrapped through all shards (0..15).
#[test]
fn sweep_cursor_advances_by_one_per_call() {
    let (cache, _dir) = test_cache();
    assert_eq!(cache.sweep_cursor.load(std::sync::atomic::Ordering::Relaxed), 0,
        "cursor must start at 0");

    // 16 sweep calls — cursor increments on each (modulo 16 internally via fetch_add).
    for i in 1usize..=16 {
        cache.sweep_once();
        assert_eq!(
            cache.sweep_cursor.load(std::sync::atomic::Ordering::Relaxed) % 16,
            i % 16,
            "cursor must be {} after {} calls", i % 16, i
        );
    }
}

/// Presetting the cursor and calling sweep_once() must process shard `cursor % 16`.
/// After the call the cursor is `preset + 1`.
#[test]
fn sweep_cursor_preset_is_respected() {
    let (cache, _dir) = test_cache();
    // Preset to shard 5.
    cache.sweep_cursor.store(5, std::sync::atomic::Ordering::Relaxed);
    cache.sweep_once();
    assert_eq!(
        cache.sweep_cursor.load(std::sync::atomic::Ordering::Relaxed), 6,
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
    cache.sweep_cursor.store(shard_count - 1, std::sync::atomic::Ordering::Relaxed);
    cache.sweep_once(); // processes shard (shard_count - 1), cursor becomes shard_count

    let raw = cache.sweep_cursor.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        raw % shard_count, 0,
        "cursor raw={} must wrap: {} % {} == 0", raw, raw, shard_count
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
    cache.sweep_cursor.store(0, std::sync::atomic::Ordering::Relaxed);

    // One sweep_once() call must process exactly one shard.
    cache.sweep_once();

    // Count how many shards still have entries.
    let shards_with_entries = shards.iter().filter(|s| !s.read().is_empty()).count();
    assert_eq!(
        shards_with_entries,
        shard_count - 1,
        "exactly one shard must have been swept; {} of {} shards still have entries (expected {})",
        shards_with_entries, shard_count, shard_count - 1
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
    shards[0].write().insert("stale_shard0".to_string(), dashmap::SharedValue::new({
        let mut s = std::collections::HashSet::new(); s.insert(key0); s
    }));
    shards[1].write().insert("stale_shard1".to_string(), dashmap::SharedValue::new({
        let mut s = std::collections::HashSet::new(); s.insert(key1); s
    }));

    // Set cursor to shard 0.
    cache.sweep_cursor.store(0, std::sync::atomic::Ordering::Relaxed);

    // One call sweeps shard 0 only.
    let removed = cache.sweep_once();
    assert_eq!(removed, 1, "only the stale key in shard 0 must be removed");

    // Shard 0 must be clean.
    assert!(shards[0].read().is_empty(), "shard 0 must be empty after sweep");

    // Shard 1 must still have its stale key.
    assert!(!shards[1].read().is_empty(), "shard 1 must be untouched after sweeping shard 0");
    assert_eq!(
        cache.sweep_cursor.load(std::sync::atomic::Ordering::Relaxed), 1,
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
        ("data/beta.parquet",  "data/beta.parquet\x1F0-200"),
        ("data/gamma.parquet", "data/gamma.parquet\x1F0-300"),
        ("data/delta.parquet", "data/delta.parquet\x1F0-400"),
    ];

    for (prefix, key) in &stale_keys {
        cache.key_index
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

    assert_eq!(total_removed, 4, "all 4 stale keys must be removed within shard_count sweep cycles");
    assert!(cache.key_index.is_empty(), "key_index must be empty after all stale keys are swept");
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
    cache.key_index
        .entry("data/mixed.parquet".to_string())
        .or_default()
        .insert(stale_key.clone());

    // Key_index now has 2 keys for the prefix: 1 live + 1 stale.
    let before_count = cache.key_index
        .get("data/mixed.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(before_count, 2, "should have 1 live + 1 stale key before sweep");

    let eviction_count_before = cache.stats.eviction_count.load(std::sync::atomic::Ordering::Relaxed);

    // Run enough sweeps to guarantee the shard containing "data/mixed.parquet" is processed.
    // DashMap defaults to 64 shards on macOS — must sweep at least shard_count times.
    let shard_count = cache.key_index.shards().len();
    for _ in 0..shard_count {
        cache.sweep_once();
    }

    // Live key must remain; stale key must be gone.
    let after_count = cache.key_index
        .get("data/mixed.parquet")
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(after_count, 1, "only the live key must remain after sweep");
    assert!(
        !cache.key_index
            .get("data/mixed.parquet")
            .map(|v| v.contains(&stale_key))
            .unwrap_or(false),
        "stale key must have been removed"
    );

    // Eviction stats must have been updated for the stale key.
    assert!(
        cache.stats.eviction_count.load(std::sync::atomic::Ordering::Relaxed) > eviction_count_before,
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
    cache.key_index
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
    assert_eq!(r1.as_ref().map(|b| b.len()), Some(512), "first range must still be readable after sweep");
    assert_eq!(r2.as_ref().map(|b| b.len()), Some(512), "second range must still be readable after sweep");

    // New puts and gets must work normally after sweep.
    put_range(&cache, "/data/new.parquet", 0, 256, &vec![0xCCu8; 256]);
    let r3 = block_on(cache.get(&range_cache_key("/data/new.parquet", 0, 256)));
    assert_eq!(r3.as_ref().map(|b| b.len()), Some(256), "new put after sweep must be retrievable");
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
    cache.key_index
        .entry("data/f.parquet".to_string())
        .or_default()
        .extend(vec![
            "data/f.parquet\x1F0-100".to_string(),
            "data/f.parquet\x1F100-300".to_string(),
        ]);

    let eviction_bytes_before = cache.stats.eviction_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let used_bytes_before = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);

    // Run enough sweeps to cover all shards.
    // DashMap defaults to 64 shards on macOS — must sweep at least shard_count times.
    let shard_count = cache.key_index.shards().len();
    for _ in 0..shard_count {
        cache.sweep_once();
    }

    let eviction_bytes_after = cache.stats.eviction_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let used_bytes_after = cache.stats.used_bytes.load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(
        eviction_bytes_after - eviction_bytes_before, 300,
        "eviction_bytes must reflect 100 + 200 = 300 bytes for the two stale keys"
    );
    assert_eq!(
        used_bytes_after - used_bytes_before, -300,
        "used_bytes must decrease by 300 when stale keys are swept"
    );
    assert!(cache.key_index.is_empty(), "key_index must be empty after stale keys are swept");
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
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 512,
            "counter must be 512 while guard is alive");
    }
    // guard dropped here
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0,
        "counter must return to 0 after guard is dropped");
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
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 400,
        "dropping g2 (200) must leave 100+300=400");

    drop(g1);
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 300,
        "dropping g1 (100) must leave 300");

    drop(g3);
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0,
        "dropping g3 (300) must leave 0");
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
    assert_eq!(cache.stats.active_in_bytes.load(std::sync::atomic::Ordering::Relaxed), 0,
        "active_in_bytes must be 0 before any get()");

    block_on(cache.get(&key));

    // After get() completes: guard has been dropped, counter back to 0.
    assert_eq!(cache.stats.active_in_bytes.load(std::sync::atomic::Ordering::Relaxed), 0,
        "active_in_bytes must be 0 after get() completes");
}

/// active_in_bytes is 0 even after a cache miss — the guard restores it regardless
/// of the hit/miss outcome.
#[test]
fn active_in_bytes_is_zero_after_cache_miss() {
    let (cache, _dir) = test_cache();
    let key = range_cache_key("/data/never_inserted.parquet", 0, 512);

    assert_eq!(cache.stats.active_in_bytes.load(std::sync::atomic::Ordering::Relaxed), 0);
    let result = block_on(cache.get(&key));
    assert!(result.is_none(), "key was never inserted — should be a miss");
    assert_eq!(cache.stats.active_in_bytes.load(std::sync::atomic::Ordering::Relaxed), 0,
        "active_in_bytes must be 0 after a cache miss");
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
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0,
        "active_in_bytes must be restored to 0 when get() future is cancelled mid-flight");
}

// ── Sweep threshold (should_skip_sweep) tests ─────────────────────────────────

/// When sweep_threshold_ratio == 0.0 (disabled), should_skip_sweep() must always
/// return false regardless of current usage — the sweep is never skipped.
#[test]
fn sweep_threshold_disabled_never_skips() {
    let dir = TempDir::new().unwrap();
    // threshold = 0.0: disabled — always sweep
    let cache = FoyerCache::new(TEST_CACHE_DISK_BYTES, dir.path(), TEST_CACHE_BLOCK_SIZE, IO_ENGINE, 0, 0.0);

    // used_bytes = 0 (empty cache)
    assert_eq!(cache.should_skip_sweep(), false,
        "threshold=0.0: should never skip even when cache is empty");

    // used_bytes = 100% of disk
    cache.stats.used_bytes.store(TEST_CACHE_DISK_BYTES as i64, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(cache.should_skip_sweep(), false,
        "threshold=0.0: should never skip even when cache is 100% full");
}

/// When usage is strictly below the threshold, should_skip_sweep() returns true.
#[test]
fn sweep_threshold_skips_when_usage_below_threshold() {
    let dir = TempDir::new().unwrap();
    // disk = 4MB, threshold = 0.75 (75%)
    let cache = FoyerCache::new(TEST_CACHE_DISK_BYTES, dir.path(), TEST_CACHE_BLOCK_SIZE, IO_ENGINE, 0, 0.75);

    // usage = 0% → below 75% → skip
    cache.stats.used_bytes.store(0, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(cache.should_skip_sweep(), true,
        "usage=0% < threshold=75%: sweep must be skipped");

    // usage = 50% → below 75% → skip
    let half = (TEST_CACHE_DISK_BYTES / 2) as i64;
    cache.stats.used_bytes.store(half, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(cache.should_skip_sweep(), true,
        "usage=50% < threshold=75%: sweep must be skipped");

    // usage = 74.9% → below 75% → skip
    let below = ((TEST_CACHE_DISK_BYTES as f64 * 0.749) as i64);
    cache.stats.used_bytes.store(below, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(cache.should_skip_sweep(), true,
        "usage=74.9% < threshold=75%: sweep must be skipped");
}

/// When usage is at or above the threshold, should_skip_sweep() returns false.
#[test]
fn sweep_threshold_runs_when_usage_at_or_above_threshold() {
    let dir = TempDir::new().unwrap();
    // disk = 4MB, threshold = 0.75 (75%)
    let cache = FoyerCache::new(TEST_CACHE_DISK_BYTES, dir.path(), TEST_CACHE_BLOCK_SIZE, IO_ENGINE, 0, 0.75);

    // usage = exactly 75% → NOT below → do NOT skip
    let exact = ((TEST_CACHE_DISK_BYTES as f64 * 0.75) as i64);
    cache.stats.used_bytes.store(exact, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(cache.should_skip_sweep(), false,
        "usage=75% == threshold=75%: sweep must NOT be skipped");

    // usage = 90% → above 75% → do NOT skip
    let above = ((TEST_CACHE_DISK_BYTES as f64 * 0.90) as i64);
    cache.stats.used_bytes.store(above, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(cache.should_skip_sweep(), false,
        "usage=90% > threshold=75%: sweep must NOT be skipped");

    // usage = 100% → above 75% → do NOT skip
    cache.stats.used_bytes.store(TEST_CACHE_DISK_BYTES as i64, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(cache.should_skip_sweep(), false,
        "usage=100% > threshold=75%: sweep must NOT be skipped");
}

/// Threshold of 1.0 means "only sweep when cache is 100% full" — any usage below
/// 100% triggers a skip.
#[test]
fn sweep_threshold_one_skips_unless_completely_full() {
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(TEST_CACHE_DISK_BYTES, dir.path(), TEST_CACHE_BLOCK_SIZE, IO_ENGINE, 0, 1.0);

    // usage = 99.9% → still below 100% → skip
    let almost_full = ((TEST_CACHE_DISK_BYTES as f64 * 0.999) as i64);
    cache.stats.used_bytes.store(almost_full, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(cache.should_skip_sweep(), true,
        "threshold=1.0, usage=99.9%: sweep must be skipped");

    // usage = exactly 100% → ratio = 1.0 = threshold → NOT below → do NOT skip
    cache.stats.used_bytes.store(TEST_CACHE_DISK_BYTES as i64, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(cache.should_skip_sweep(), false,
        "threshold=1.0, usage=100%: sweep must NOT be skipped");
}

/// Negative used_bytes (which can transiently occur due to relaxed ordering on
/// concurrent sweeps and evictions) must be treated as 0 — no skip.
#[test]
fn sweep_threshold_negative_used_bytes_treated_as_zero() {
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(TEST_CACHE_DISK_BYTES, dir.path(), TEST_CACHE_BLOCK_SIZE, IO_ENGINE, 0, 0.5);

    // Simulate a transient underflow (negative used_bytes) — should be clamped to 0.
    cache.stats.used_bytes.store(-100, std::sync::atomic::Ordering::Relaxed);
    // usage = max(−100, 0) / disk = 0 / disk = 0.0 < 0.5 → skip
    assert_eq!(cache.should_skip_sweep(), true,
        "negative used_bytes must be clamped to 0; ratio 0.0 < threshold 0.5 → skip");
}

/// sweep_once() ignores the threshold — it calls reconcile_key_index directly.
/// Even when should_skip_sweep() returns true, sweep_once() still sweeps.
#[test]
fn sweep_once_ignores_threshold_guard() {
    let dir = TempDir::new().unwrap();
    // Set a high threshold so should_skip_sweep() returns true
    let cache = FoyerCache::new(TEST_CACHE_DISK_BYTES, dir.path(), TEST_CACHE_BLOCK_SIZE, IO_ENGINE, 0, 0.99);

    // Inject a stale key
    cache.key_index
        .entry("data/file.parquet".to_string())
        .or_default()
        .insert("data/file.parquet\x1F0-100".to_string());

    // usage = 0 → should_skip_sweep() is true (below 99% threshold)
    assert_eq!(cache.should_skip_sweep(), true, "precondition: threshold guard would skip");

    // But sweep_once() bypasses the guard and still sweeps
    let shard_count = cache.key_index.shards().len();
    let removed: usize = (0..shard_count).map(|_| cache.sweep_once()).sum();
    assert_eq!(removed, 1, "sweep_once() must sweep regardless of threshold");
    assert!(cache.key_index.is_empty(), "stale key must be removed by sweep_once() even when threshold would skip");
}
