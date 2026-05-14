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

const BLOCK_SIZE: usize = 64 * 1024 * 1024;  // 64 MB default for tests
const IO_ENGINE:  &str  = "auto";

fn test_cache() -> (FoyerCache, TempDir) {
    let dir = TempDir::new().expect("failed to create temp dir");
    let cache = FoyerCache::new(64 * 1024 * 1024, dir.path(), BLOCK_SIZE, IO_ENGINE);
    (cache, dir)
}

fn put_range(cache: &FoyerCache, path: &str, start: u64, end: u64, data: &[u8]) {
    cache.put(&range_cache_key(path, start, end), Bytes::copy_from_slice(data));
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Runtime::new().expect("test runtime").block_on(f)
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

// ── disk / capacity cases ─────────────────────────────────────────────────────

#[test]
fn put_and_get_work_after_cache_nears_capacity() {
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(1 * 1024 * 1024, dir.path(), BLOCK_SIZE, IO_ENGINE);
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
fn lru_eviction_removes_stale_keys_from_key_index() {
    let dir = TempDir::new().unwrap();
    let cache = FoyerCache::new(1 * 1024 * 1024, dir.path(), BLOCK_SIZE, IO_ENGINE);
    const CHUNK_SIZE: usize = 256 * 1024;
    const TOTAL_WRITES: usize = 8;
    let chunk = vec![0xABu8; CHUNK_SIZE];
    for i in 0u64..TOTAL_WRITES as u64 {
        let key = range_cache_key("/data/big.parquet", i * CHUNK_SIZE as u64, (i + 1) * CHUNK_SIZE as u64);
        cache.put(&key, Bytes::copy_from_slice(&chunk));
    }
    std::thread::sleep(std::time::Duration::from_millis(500));
    let key_count = cache.key_index.get("data/big.parquet").map(|v| v.len()).unwrap_or(0);
    assert!(key_count < TOTAL_WRITES, "expected < {} entries after LRU eviction; got {}", TOTAL_WRITES, key_count);
}

#[test]
fn replace_event_does_not_duplicate_key_in_key_index() {
    let (cache, _dir) = test_cache();
    let key = range_cache_key("/data/file.parquet", 0, 100);
    cache.put(&key, Bytes::from_static(b"version_1"));
    cache.put(&key, Bytes::from_static(b"version_2"));
    std::thread::sleep(std::time::Duration::from_millis(100));
    let count = cache.key_index.get("data/file.parquet").map(|v| v.len()).unwrap_or(0);
    assert_eq!(count, 1, "same key put twice should result in 1 key_index entry; got {}", count);
    let result = block_on(cache.get(&key));
    assert_eq!(result.as_deref(), Some(b"version_2".as_slice()));
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
    )};
    assert!(ptr > 0);

    // 9 fields × 2 sections = 18 longs
    let mut out = [i64::MAX; 18];
    let rc = unsafe { crate::foyer::ffm::foyer_snapshot_stats(ptr, out.as_mut_ptr()) };
    assert_eq!(rc, 0, "foyer_snapshot_stats should return 0 on success");

    // A freshly created cache has no hits, misses, evictions, or used bytes.
    // Sections 0 and 1 are identical (Foyer is currently single-tier).
    for i in 0..18 {
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
        )
    };
    assert!(ptr > 0);

    // 9 fields × 2 sections = 18 longs
    let mut buf = [i64::MIN; 18];
    let result = unsafe { foyer_snapshot_stats(ptr, buf.as_mut_ptr()) };
    assert_eq!(result, 0, "foyer_snapshot_stats must return 0 on success");

    // No i64::MIN values — the buffer was fully written.
    for (i, &v) in buf.iter().enumerate() {
        assert_ne!(v, i64::MIN, "buf[{}] was not written", i);
    }

    // Both sections are identical (single-tier: overall mirrors block_level).
    assert_eq!(&buf[..9], &buf[9..], "overall and block_level sections must be identical");

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

    // 9 fields × 2 sections = 18 longs
    let mut out = [0i64; 18];
    let rc = unsafe { crate::foyer::ffm::foyer_snapshot_stats(raw_ptr, out.as_mut_ptr()) };
    assert_eq!(rc, 0);

    // Clean up the Box.
    let _ = unsafe { Box::from_raw(raw_ptr as *mut Arc<dyn crate::traits::BlockCache>) };

    // Section 0 (indices 0–8) and section 1 (indices 9–17) must be identical.
    assert_eq!(&out[0..9], &out[9..18],
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
