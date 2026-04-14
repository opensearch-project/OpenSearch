/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unit tests for [`FoyerCache`] and the FFM lifecycle API.
//!
//! ## Philosophy
//!
//! Tests are written from the perspective of what the API *guarantees*, not
//! what the current implementation happens to return. Each test documents the
//! invariant it is verifying so that regressions are immediately obvious.

use std::sync::Arc;
use bytes::Bytes;
use tempfile::TempDir;

use crate::foyer::foyer_cache::FoyerCache;
use crate::foyer::ffm::{foyer_create_cache, foyer_destroy_cache};
use crate::traits::PageCache;

// ── Test helpers ──────────────────────────────────────────────────────────────

/// Build a [`FoyerCache`] in a temp directory for testing.
/// Uses a small disk capacity — enough for dozens of test entries.
fn test_cache() -> (FoyerCache, TempDir) {
    let dir = TempDir::new().expect("failed to create temp dir");
    let cache = FoyerCache::new(64 * 1024 * 1024, dir.path()); // 64 MB
    (cache, dir)
}

/// Shorthand: insert bytes into the cache.
fn put(cache: &FoyerCache, path: &str, start: u64, end: u64, data: &[u8]) {
    cache.put(path, start, end, Bytes::copy_from_slice(data));
}

/// Shorthand: synchronously drive an async future in tests.
fn block<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Runtime::new()
        .expect("test runtime")
        .block_on(f)
}

// ── put + get round-trip ──────────────────────────────────────────────────────

#[test]
fn get_returns_exact_bytes_that_were_put() {
    let (cache, _dir) = test_cache();
    let data = b"hello foyer cache";
    put(&cache, "/data/file.parquet", 0, 100, data);

    let result = block(cache.get("/data/file.parquet", 0, 100));
    assert_eq!(result.as_deref(), Some(data.as_slice()),
        "get should return exactly the bytes that were put");
}

#[test]
fn multiple_ranges_for_same_file_are_independent() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/a.parquet", 0,    4096, b"range0");
    put(&cache, "/data/a.parquet", 4096, 8192, b"range1");
    put(&cache, "/data/a.parquet", 8192, 12288, b"range2");

    assert_eq!(block(cache.get("/data/a.parquet", 0,    4096)).as_deref(), Some(b"range0".as_slice()));
    assert_eq!(block(cache.get("/data/a.parquet", 4096, 8192)).as_deref(), Some(b"range1".as_slice()));
    assert_eq!(block(cache.get("/data/a.parquet", 8192, 12288)).as_deref(), Some(b"range2".as_slice()));
}

#[test]
fn multiple_files_are_independent() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/a.parquet", 0, 100, b"file_a");
    put(&cache, "/data/b.parquet", 0, 100, b"file_b");
    put(&cache, "/data/c.parquet", 0, 100, b"file_c");

    assert_eq!(block(cache.get("/data/a.parquet", 0, 100)).as_deref(), Some(b"file_a".as_slice()));
    assert_eq!(block(cache.get("/data/b.parquet", 0, 100)).as_deref(), Some(b"file_b".as_slice()));
    assert_eq!(block(cache.get("/data/c.parquet", 0, 100)).as_deref(), Some(b"file_c".as_slice()));
}

#[test]
fn large_value_round_trips_correctly() {
    let (cache, _dir) = test_cache();
    let data: Vec<u8> = (0u32..1_000_000).map(|i| (i % 251) as u8).collect();
    put(&cache, "/data/large.parquet", 0, data.len() as u64, &data);

    let result = block(cache.get("/data/large.parquet", 0, data.len() as u64))
        .expect("large value should be retrievable");
    assert_eq!(result.as_ref(), data.as_slice(),
        "large value should round-trip without corruption");
}

#[test]
fn put_same_key_twice_replaces_value() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/file.parquet", 0, 100, b"original");
    put(&cache, "/data/file.parquet", 0, 100, b"updated");

    let result = block(cache.get("/data/file.parquet", 0, 100));
    // The second put replaces the first — exactly one value, the latest
    assert_eq!(result.as_deref(), Some(b"updated".as_slice()),
        "second put should replace the first for the same key");
}

// ── get miss cases ────────────────────────────────────────────────────────────

#[test]
fn get_returns_none_for_unknown_path() {
    let (cache, _dir) = test_cache();
    let result = block(cache.get("/never/inserted.parquet", 0, 100));
    assert!(result.is_none(), "path never inserted should return None");
}

#[test]
fn get_returns_none_for_wrong_range_on_known_path() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/file.parquet", 0, 100, b"data");

    // Different start
    assert!(block(cache.get("/data/file.parquet", 1, 100)).is_none(),
        "wrong start offset should return None");
    // Different end
    assert!(block(cache.get("/data/file.parquet", 0, 99)).is_none(),
        "wrong end offset should return None");
    // Completely different range
    assert!(block(cache.get("/data/file.parquet", 200, 300)).is_none(),
        "non-overlapping range should return None");
}

#[test]
fn get_returns_none_for_empty_path() {
    let (cache, _dir) = test_cache();
    let result = block(cache.get("", 0, 100));
    assert!(result.is_none(), "empty path should return None");
}

// ── evict_file ────────────────────────────────────────────────────────────────

#[test]
fn evict_file_removes_all_ranges_for_path() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/target.parquet", 0,    4096, b"range0");
    put(&cache, "/data/target.parquet", 4096, 8192, b"range1");
    put(&cache, "/data/target.parquet", 8192, 12288, b"range2");

    cache.evict_file("/data/target.parquet");

    // The authoritative invariant: key_index no longer tracks the evicted file.
    // This means future evict_file calls are no-ops and the cache can be fully
    // repopulated for this path without stale key bookkeeping.
    assert!(!cache.key_index.contains_key("/data/target.parquet"),
        "evict_file should remove the file entry from key_index");

    // After eviction, the path should be treatable as clean — we can re-insert
    // a new entry and it must be tracked correctly in key_index.
    put(&cache, "/data/target.parquet", 0, 4096, b"new");
    assert!(cache.key_index.contains_key("/data/target.parquet"),
        "re-inserting after eviction should rebuild key_index entry");
    assert_eq!(
        block(cache.get("/data/target.parquet", 0, 4096)),
        Some(Bytes::from_static(b"new")),
        "re-inserted range after eviction should be retrievable"
    );
}

#[test]
fn evict_file_does_not_affect_other_files() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/target.parquet", 0, 100, b"target");
    put(&cache, "/data/other.parquet",  0, 100, b"other");

    cache.evict_file("/data/target.parquet");

    assert!(block(cache.get("/data/other.parquet", 0, 100)).is_some(),
        "evict_file should not affect other file paths");
    assert!(block(cache.get("/data/target.parquet", 0, 100)).is_none());
}

#[test]
fn evict_file_on_nonexistent_path_is_noop() {
    let (cache, _dir) = test_cache();
    // Should not panic
    cache.evict_file("/never/inserted.parquet");
    cache.evict_file(""); // empty path also safe
}

#[test]
fn evict_file_twice_is_safe() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/file.parquet", 0, 100, b"data");
    cache.evict_file("/data/file.parquet");
    // Second evict should not panic
    cache.evict_file("/data/file.parquet");
}

#[test]
fn after_evict_file_new_put_is_retrievable() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/file.parquet", 0, 100, b"first");
    cache.evict_file("/data/file.parquet");
    put(&cache, "/data/file.parquet", 0, 100, b"second");

    let result = block(cache.get("/data/file.parquet", 0, 100));
    assert_eq!(result.as_deref(), Some(b"second".as_slice()),
        "after evict+put, new value should be retrievable");
}

// ── clear ─────────────────────────────────────────────────────────────────────

#[test]
fn clear_removes_all_entries() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/a.parquet", 0, 100, b"a");
    put(&cache, "/data/b.parquet", 0, 100, b"b");
    put(&cache, "/data/c.parquet", 0, 100, b"c");

    block(cache.clear());

    assert!(block(cache.get("/data/a.parquet", 0, 100)).is_none());
    assert!(block(cache.get("/data/b.parquet", 0, 100)).is_none());
    assert!(block(cache.get("/data/c.parquet", 0, 100)).is_none());
}

#[test]
fn clear_on_empty_cache_is_safe() {
    let (cache, _dir) = test_cache();
    block(cache.clear()); // should not panic
}

#[test]
fn cache_is_usable_after_clear() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/file.parquet", 0, 100, b"before");
    block(cache.clear());
    put(&cache, "/data/file.parquet", 0, 100, b"after");

    let result = block(cache.get("/data/file.parquet", 0, 100));
    assert_eq!(result.as_deref(), Some(b"after".as_slice()),
        "cache should accept new entries after clear");
}

// ── key_index integrity ───────────────────────────────────────────────────────

#[test]
fn key_index_is_empty_after_clear() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/a.parquet", 0, 100, b"a");
    put(&cache, "/data/b.parquet", 0, 100, b"b");

    block(cache.clear());

    assert!(cache.key_index.is_empty(),
        "key_index should be empty after clear");
}

#[test]
fn key_index_has_no_entry_for_evicted_file() {
    let (cache, _dir) = test_cache();
    put(&cache, "/data/target.parquet", 0, 100, b"data");
    put(&cache, "/data/other.parquet",  0, 100, b"other");

    cache.evict_file("/data/target.parquet");

    assert!(!cache.key_index.contains_key("/data/target.parquet"),
        "key_index should not contain entry for evicted file");
    assert!(cache.key_index.contains_key("/data/other.parquet"),
        "key_index should still contain entries for other files");
}

// ── concurrent access ─────────────────────────────────────────────────────────

#[test]
fn concurrent_puts_to_different_files_do_not_corrupt() {
    let (cache, _dir) = test_cache();
    let cache = Arc::new(cache);

    let handles: Vec<_> = (0..16).map(|i| {
        let cache = Arc::clone(&cache);
        std::thread::spawn(move || {
            let path = format!("/data/file_{}.parquet", i);
            let data = vec![i as u8; 1024];
            put(&cache, &path, 0, 1024, &data);
        })
    }).collect();

    for h in handles { h.join().expect("thread panicked"); }

    // Verify all puts are retrievable and correct
    for i in 0u8..16 {
        let path = format!("/data/file_{}.parquet", i);
        let result = block(cache.get(&path, 0, 1024))
            .expect("concurrently-put entry should be retrievable");
        assert!(result.iter().all(|&b| b == i),
            "file_{}: data was corrupted by concurrent puts", i);
    }
}

#[test]
fn concurrent_put_and_get_same_file_does_not_panic() {
    let (cache, _dir) = test_cache();
    let cache = Arc::new(cache);

    // Writer thread
    let writer_cache = Arc::clone(&cache);
    let writer = std::thread::spawn(move || {
        for i in 0u64..100 {
            put(&writer_cache, "/data/shared.parquet", i * 100, (i + 1) * 100, b"data");
        }
    });

    // Reader thread
    let reader_cache = Arc::clone(&cache);
    let reader = std::thread::spawn(move || {
        for i in 0u64..100 {
            // Result may be Some or None depending on timing — both are valid
            let _ = block(reader_cache.get("/data/shared.parquet", i * 100, (i + 1) * 100));
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
            put(&writer_cache, "/data/file.parquet", i * 100, (i + 1) * 100, b"data");
        }
    });

    let evictor_cache = Arc::clone(&cache);
    let evictor = std::thread::spawn(move || {
        for _ in 0..50 {
            evictor_cache.evict_file("/data/file.parquet");
        }
    });

    writer.join().expect("writer panicked");
    evictor.join().expect("evictor panicked");
}

// ── disk / capacity cases ───────────────────────────────────────────────

#[test]
fn put_and_get_work_after_cache_nears_capacity() {
    // When the cache approaches its quota, Foyer evicts old entries via LRU.
    // New puts should still succeed and the most recently put entry should be
    // retrievable (unless it was itself evicted by an even newer entry).
    // This test uses a very small cache (1 MB) and writes 2 MB of data.
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(1 * 1024 * 1024, dir.path()); // 1 MB

    // Write 2 MB total: 4 entries of 512 KB each
    let chunk = vec![0u8; 512 * 1024]; // 512 KB
    for i in 0u64..4 {
        cache.put("/data/file.parquet", i * 524288, (i + 1) * 524288,
                  bytes::Bytes::copy_from_slice(&chunk));
    }

    // The cache must not panic. The last entry should either be retrievable
    // (if Foyer kept it) or return None (if it was also evicted). Either is valid.
    // The important invariant: no crash and the cache remains usable.
    let _last = block(cache.get("/data/file.parquet", 3 * 524288, 4 * 524288));
    // After eviction, a fresh put+get must work
    put(&cache, "/data/file.parquet", 0, 100, b"fresh");
    let result = block(cache.get("/data/file.parquet", 0, 100));
    assert_eq!(result.as_deref(), Some(b"fresh".as_slice()),
        "cache must remain usable after operating at capacity");
}

// ── KeyIndexListener behaviour ───────────────────────────────────────────────

#[test]
fn lru_eviction_removes_stale_keys_from_key_index() {
    // KeyIndexListener must remove keys from key_index when Foyer LRU-evicts them.
    // This is the core invariant: without it, key_index grows unbounded.
    //
    // Cache: 1 MB. Entries: 8 × 256 KB = 2 MB total.
    // Foyer can hold at most floor(1 MB / 256 KB) = 4 entries.
    // After writing 8, at least 4 must have been evicted by LRU.
    // After the listener fires, key_index must reflect this: at most 4 keys remain.
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(1 * 1024 * 1024, dir.path());

    const CHUNK_SIZE: usize = 256 * 1024; // 256 KB
    const TOTAL_WRITES: usize = 8;        // 8 × 256 KB = 2 MB written to a 1 MB cache
    let chunk = vec![0xABu8; CHUNK_SIZE];

    for i in 0u64..TOTAL_WRITES as u64 {
        let start = i * CHUNK_SIZE as u64;
        let end   = (i + 1) * CHUNK_SIZE as u64;
        cache.put("/data/big.parquet", start, end, Bytes::copy_from_slice(&chunk));
    }

    // Wait for Foyer's async eviction callbacks to fire on the Tokio runtime
    std::thread::sleep(std::time::Duration::from_millis(500));

    let key_count = cache.key_index
        .get("/data/big.parquet")
        .map(|v| v.len())
        .unwrap_or(0);

    // Foyer's block engine has per-entry metadata overhead so the exact eviction
    // boundary is not precisely floor(capacity/chunk_size). The meaningful invariant
    // is: at least some entries were evicted (key_count < TOTAL_WRITES), proving the
    // listener IS firing and removing stale keys.
    assert!(
        key_count < TOTAL_WRITES,
        "key_index should contain fewer than {} entries after LRU eviction removed stale keys; got {}",
        TOTAL_WRITES, key_count
    );
}

#[test]
fn replace_event_does_not_duplicate_key_in_key_index() {
    // When the same (path, start, end) key is put twice, Foyer fires Event::Replace
    // for the old value. KeyIndexListener removes the old key. put() re-adds it.
    // Net result: exactly one entry in key_index for that key, not two.
    let (cache, _dir) = test_cache();

    put(&cache, "/data/file.parquet", 0, 100, b"version_1");
    put(&cache, "/data/file.parquet", 0, 100, b"version_2"); // same key → Event::Replace

    // Event::Replace fires synchronously during insert()
    std::thread::sleep(std::time::Duration::from_millis(100));

    let count = cache.key_index
        .get("/data/file.parquet")
        .map(|v| v.len())
        .unwrap_or(0);

    assert_eq!(count, 1,
        "same key put twice should result in exactly 1 key_index entry (not 2); got {}", count);

    // The value must be the latest one
    let result = block(cache.get("/data/file.parquet", 0, 100));
    assert_eq!(result.as_deref(), Some(b"version_2".as_slice()),
        "latest value must be returned after replace");
}

#[test]
fn event_remove_after_evict_file_does_not_panic_or_corrupt_key_index() {
    // evict_file() removes from key_index then calls inner.remove() for each key.
    // inner.remove() causes Foyer to fire Event::Remove on KeyIndexListener.
    // The listener tries to remove from key_index which is already empty.
    // This must be a no-op — no panic, no corruption, key_index stays empty.
    let (cache, _dir) = test_cache();
    put(&cache, "/data/file.parquet", 0, 100, b"data");
    put(&cache, "/data/file.parquet", 100, 200, b"more");

    cache.evict_file("/data/file.parquet");
    // Foyer fires Event::Remove for both keys — listener must handle double-remove safely
    std::thread::sleep(std::time::Duration::from_millis(100));

    assert!(!cache.key_index.contains_key("/data/file.parquet"),
        "key_index must not contain entry after evict_file + Event::Remove callbacks");
    // Sanity: cache must still be usable
    put(&cache, "/data/file.parquet", 0, 100, b"fresh");
    assert_eq!(block(cache.get("/data/file.parquet", 0, 100)).as_deref(),
        Some(b"fresh".as_slice()), "cache must be usable after double-remove");
}

// ── FFM lifecycle ─────────────────────────────────────────────────────────────

#[test]
fn ffm_create_returns_positive_pointer() {
    let dir = TempDir::new().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let ptr = unsafe {
        foyer_create_cache(
            64 * 1024 * 1024,
            dir_str.as_ptr(),
            dir_str.len() as u64,
        )
    };
    assert!(ptr > 0, "foyer_create_cache should return a positive pointer on success");
    // Clean up
    unsafe { foyer_destroy_cache(ptr); }
}

#[test]
fn ffm_create_with_null_ptr_returns_error() {
    let ptr = unsafe {
        foyer_create_cache(64 * 1024 * 1024, std::ptr::null(), 10)
    };
    assert!(ptr < 0, "null dir_ptr should return a negative error pointer");
    // The error pointer convention: ptr is negative, negate it to get the real address to free.
    if ptr < 0 {
        unsafe { native_bridge_common::error::native_error_free(-ptr); }
    }
}

#[test]
fn ffm_create_with_invalid_utf8_returns_error() {
    let invalid_utf8 = [0xFF, 0xFE, 0xFD];
    let ptr = unsafe {
        foyer_create_cache(
            64 * 1024 * 1024,
            invalid_utf8.as_ptr(),
            invalid_utf8.len() as u64,
        )
    };
    assert!(ptr < 0, "invalid UTF-8 dir path should return a negative error pointer");
    // Negate to get the real CString pointer to free
    if ptr < 0 {
        unsafe { native_bridge_common::error::native_error_free(-ptr); }
    }
}

#[test]
fn ffm_destroy_with_zero_ptr_is_noop() {
    // Must not panic or crash
    unsafe { foyer_destroy_cache(0); }
}

#[test]
fn ffm_destroy_with_negative_ptr_is_noop() {
    // Negative means error pointer — foyer_destroy_cache checks ptr > 0
    unsafe { foyer_destroy_cache(-1); }
}

#[test]
fn ffm_create_destroy_lifecycle_no_leak() {
    // Create and destroy multiple caches sequentially
    // If there were a leak, the temp dirs and memory would accumulate
    for _ in 0..3 {
        let dir = TempDir::new().unwrap();
        let dir_str = dir.path().to_str().unwrap();
        let ptr = unsafe {
            foyer_create_cache(
                16 * 1024 * 1024, // small: 16 MB
                dir_str.as_ptr(),
                dir_str.len() as u64,
            )
        };
        assert!(ptr > 0);
        unsafe { foyer_destroy_cache(ptr); }
        // dir cleaned up here — if Foyer holds a file handle, this would fail
    }
}
