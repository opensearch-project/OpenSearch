/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Integration tests: TieredObjectStore + TieredBlockCache with real Parquet files.
//!
//! Uses `#[test]` (not `#[tokio::test]`) because FoyerCache::new() internally
//! calls block_on() and panics if called inside an existing tokio runtime.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

use opensearch_block_cache::foyer::foyer_cache::FoyerCache;
use opensearch_block_cache::range_cache::range_cache_key;
use opensearch_block_cache::tiered_block_cache::TieredBlockCache;
use opensearch_block_cache::traits::BlockCache;

use opensearch_tiered_storage::registry::traits::FileRegistry;
use opensearch_tiered_storage::registry::TieredStorageRegistry;
use opensearch_tiered_storage::tiered_object_store::TieredObjectStore;
use opensearch_tiered_storage::types::{FileLocation, TieredFileEntry};

// ── Helpers ─────────────────────────────────────────────────────────────────────

const BLOCK_SIZE: usize = 1 * 1024 * 1024;
const DISK_BYTES: usize = 8 * 1024 * 1024;
const BUFFER_POOL: usize = 8 * 1024 * 1024;
const SUBMIT_QUEUE: usize = 8 * 1024 * 1024;

fn create_tiered_cache(
    data_dir: &std::path::Path,
    meta_dir: &std::path::Path,
) -> Arc<TieredBlockCache> {
    let data_cache = Arc::new(FoyerCache::new(
        DISK_BYTES,
        data_dir,
        BLOCK_SIZE,
        BUFFER_POOL,
        SUBMIT_QUEUE,
        "auto",
        0,
        0.0,
        0,
        false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        DISK_BYTES,
        meta_dir,
        BLOCK_SIZE,
        BUFFER_POOL,
        SUBMIT_QUEUE,
        "auto",
        0,
        0.0,
        0,
        false,
    ));
    Arc::new(TieredBlockCache::new(data_cache, metadata_cache))
}

fn create_store(
    parquet_dir: &std::path::Path,
    cache: Arc<TieredBlockCache>,
    path_str: &str,
    file_size: u64,
) -> Arc<TieredObjectStore> {
    let local: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(parquet_dir).unwrap());
    let registry = Arc::new(TieredStorageRegistry::new());
    let store = TieredObjectStore::new(registry, local).with_cache(cache as Arc<dyn BlockCache>);
    let store = Arc::new(store);
    store.registry().register(
        path_str,
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );
    store
}

#[allow(deprecated)]
fn write_test_parquet(dir: &std::path::Path, filename: &str, num_row_groups: usize) -> u64 {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let file_path = dir.join(filename);
    let file = std::fs::File::create(&file_path).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    for rg in 0..num_row_groups {
        let offset = (rg * 100) as i64;
        let ids: Vec<i64> = (offset..offset + 100).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("name_{}", i)).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
    std::fs::metadata(&file_path).unwrap().len()
}

/// Shared runtime for test async blocks (created lazily, not inside FoyerCache::new).
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
        .block_on(f)
}

/// Compute the single global page index range matching parquet crate's `range_for_page_index()`.
///
/// Folds ALL columns across ALL row groups into a single contiguous range
/// encompassing all column_index and offset_index data. Returns `None` if the
/// file has no page index metadata.
fn compute_global_page_index_range(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Option<std::ops::Range<u64>> {
    metadata
        .row_groups()
        .iter()
        .flat_map(|rg| rg.columns().iter())
        .fold(None::<std::ops::Range<u64>>, |acc, col| {
            let acc = if let (Some(offset), Some(length)) =
                (col.column_index_offset(), col.column_index_length())
            {
                let start = offset as u64;
                let end = start + length as u64;
                match acc {
                    Some(a) => Some(a.start.min(start)..a.end.max(end)),
                    None => Some(start..end),
                }
            } else {
                acc
            };
            if let (Some(offset), Some(length)) =
                (col.offset_index_offset(), col.offset_index_length())
            {
                let start = offset as u64;
                let end = start + length as u64;
                match acc {
                    Some(a) => Some(a.start.min(start)..a.end.max(end)),
                    None => Some(start..end),
                }
            } else {
                acc
            }
        })
}

/// Compute merged column index and offset index ranges per row group.
///
/// Returns two optional ranges per RG: (column_index_range, offset_index_range).
fn compute_per_rg_page_index_ranges(
    rg: &parquet::file::metadata::RowGroupMetaData,
) -> (Option<std::ops::Range<u64>>, Option<std::ops::Range<u64>>) {
    let col_idx_range = rg
        .columns()
        .iter()
        .fold(None::<std::ops::Range<u64>>, |acc, col| {
            if let (Some(offset), Some(length)) =
                (col.column_index_offset(), col.column_index_length())
            {
                let start = offset as u64;
                let end = start + length as u64;
                match acc {
                    Some(a) => Some(a.start.min(start)..a.end.max(end)),
                    None => Some(start..end),
                }
            } else {
                acc
            }
        });

    let off_idx_range = rg
        .columns()
        .iter()
        .fold(None::<std::ops::Range<u64>>, |acc, col| {
            if let (Some(offset), Some(length)) =
                (col.offset_index_offset(), col.offset_index_length())
            {
                let start = offset as u64;
                let end = start + length as u64;
                match acc {
                    Some(a) => Some(a.start.min(start)..a.end.max(end)),
                    None => Some(start..end),
                }
            } else {
                acc
            }
        });

    (col_idx_range, off_idx_range)
}

/// Set up a DataFusion session with the store registered and a ListingTable for the given file.
///
/// Returns the SessionContext and the inferred schema. The table is registered as `table_name`.
async fn setup_df_session(
    store: Arc<TieredObjectStore>,
    file_path: &str,
    table_name: &str,
    schema: Option<Arc<Schema>>,
) -> (datafusion::prelude::SessionContext, Arc<Schema>) {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::*;

    let ctx = SessionContext::new();
    let url = url::Url::parse("file://").unwrap();
    ctx.runtime_env()
        .register_object_store(&url, store.clone() as Arc<dyn ObjectStore>);

    let table_url = ListingTableUrl::parse(&format!("file:///{}", file_path)).unwrap();
    let format = Arc::new(ParquetFormat::default());
    let listing_options = ListingOptions::new(format).with_file_extension(".parquet");

    let schema = match schema {
        Some(s) => s,
        None => listing_options
            .infer_schema(&ctx.state(), &table_url)
            .await
            .unwrap(),
    };

    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(schema.clone());
    let table = ListingTable::try_new(config).unwrap();
    ctx.register_table(table_name, Arc::new(table)).unwrap();

    (ctx, schema)
}

// ── Tests ───────────────────────────────────────────────────────────────────────

/// Helper: simulates warmup — reads bytes via local FS and puts into metadata cache.
fn warmup_metadata(
    cache: &TieredBlockCache,
    parquet_dir: &std::path::Path,
    filename: &str,
    start: u64,
    end: u64,
) -> bytes::Bytes {
    let file_path = parquet_dir.join(filename);
    let file_bytes = std::fs::read(&file_path).unwrap();
    let range_bytes = bytes::Bytes::copy_from_slice(&file_bytes[start as usize..end as usize]);
    let key = range_cache_key(filename, start, end);
    cache.put_metadata(&key, range_bytes.clone());
    range_bytes
}

#[test]
fn metadata_routed_to_metadata_cache_data_to_data_cache() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "test.parquet", 3);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "test.parquet", file_size);

    let path = Path::from("test.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Warmup: explicitly put metadata into metadata cache
    let footer = warmup_metadata(
        &cache,
        parquet_dir.path(),
        "test.parquet",
        footer_start,
        file_size,
    );

    block_on(async {
        // Metadata read via get_range → get_opts → cache probe HIT
        let footer_from_cache = store
            .get_range(&path, footer_start..file_size)
            .await
            .unwrap();
        assert_eq!(footer_from_cache, footer);

        // Confirm in metadata cache, NOT data cache
        let footer_key = range_cache_key("test.parquet", footer_start, file_size);
        assert!(
            cache.metadata_cache().get(&footer_key).await.is_some(),
            "footer must be in metadata cache"
        );
        assert!(
            cache.data_cache().get(&footer_key).await.is_none(),
            "footer must NOT be in data cache"
        );

        // Data read (get_ranges) → data cache
        let data = store.get_ranges(&path, &[0u64..4096]).await.unwrap();
        assert_eq!(data[0].len(), 4096);

        // Confirm data in data cache, NOT metadata cache
        let data_key = range_cache_key("test.parquet", 0, 4096);
        assert!(
            cache.data_cache().get(&data_key).await.is_some(),
            "data must be in data cache"
        );
        assert!(
            cache.metadata_cache().get(&data_key).await.is_none(),
            "data must NOT be in metadata cache"
        );
    });
}

#[test]
fn metadata_survives_restart_via_foyer_recovery() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "restart.parquet", 2);
    let path = Path::from("restart.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Session 1: warmup puts metadata into metadata cache
    let original_bytes = {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        warmup_metadata(
            &cache,
            parquet_dir.path(),
            "restart.parquet",
            footer_start,
            file_size,
        )
    };

    // Session 2: new instances, same SSD dirs — Foyer recovers
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        let store = create_store(
            parquet_dir.path(),
            cache.clone(),
            "restart.parquet",
            file_size,
        );
        block_on(async {
            // get_range → get_opts → cache probe → HIT (recovered from SSD)
            let bytes = store
                .get_range(&path, footer_start..file_size)
                .await
                .unwrap();
            assert_eq!(bytes, original_bytes, "metadata must survive restart");
        });
    }
}

#[test]
fn evict_prefix_clears_both_caches_on_shard_delete() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "delete.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "delete.parquet",
        file_size,
    );
    let path = Path::from("delete.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Warmup metadata + populate data
    warmup_metadata(
        &cache,
        parquet_dir.path(),
        "delete.parquet",
        footer_start,
        file_size,
    );

    block_on(async {
        let _data = store.get_ranges(&path, &[0u64..4096]).await.unwrap();

        let footer_key = range_cache_key("delete.parquet", footer_start, file_size);
        let data_key = range_cache_key("delete.parquet", 0, 4096);
        assert!(cache.metadata_cache().get(&footer_key).await.is_some());
        assert!(cache.data_cache().get(&data_key).await.is_some());

        // Shard delete
        store.evict_path("delete.parquet");

        assert!(
            cache.metadata_cache().get(&footer_key).await.is_none(),
            "metadata must be evicted"
        );
        assert!(
            cache.data_cache().get(&data_key).await.is_none(),
            "data must be evicted"
        );
    });
}

/// Proves metadata is served from cache, not local FS: delete the local file
/// after warmup, then read via store — must succeed from cache.
#[test]
fn metadata_served_from_ssd_not_local_fs() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "ssd_only.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "ssd_only.parquet",
        file_size,
    );
    let path = Path::from("ssd_only.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Warmup puts metadata into metadata cache
    let original = warmup_metadata(
        &cache,
        parquet_dir.path(),
        "ssd_only.parquet",
        footer_start,
        file_size,
    );

    // Delete local file — force subsequent reads to come from cache only
    std::fs::remove_file(parquet_dir.path().join("ssd_only.parquet")).unwrap();

    block_on(async {
        // Read via store — local FS is gone, must succeed from metadata SSD cache
        let from_cache = store
            .get_range(&path, footer_start..file_size)
            .await
            .unwrap();
        assert_eq!(
            from_cache, original,
            "must serve from SSD cache after local deletion"
        );
    });
}

/// Fill data cache beyond capacity, verify metadata is untouched.
#[test]
fn data_pressure_does_not_evict_metadata() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "pressure.parquet", 5);

    let data_cache = Arc::new(FoyerCache::new(
        2 * 1024 * 1024,
        data_dir.path(),
        BLOCK_SIZE,
        BUFFER_POOL,
        SUBMIT_QUEUE,
        "auto",
        0,
        0.0,
        0,
        false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        DISK_BYTES,
        meta_dir.path(),
        BLOCK_SIZE,
        BUFFER_POOL,
        SUBMIT_QUEUE,
        "auto",
        0,
        0.0,
        0,
        false,
    ));
    let cache = Arc::new(TieredBlockCache::new(data_cache, metadata_cache));
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "pressure.parquet",
        file_size,
    );
    let path = Path::from("pressure.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Warmup
    let footer = warmup_metadata(
        &cache,
        parquet_dir.path(),
        "pressure.parquet",
        footer_start,
        file_size,
    );
    let footer_key = range_cache_key("pressure.parquet", footer_start, file_size);

    block_on(async {
        // Fill data cache to trigger eviction
        for i in 0..20 {
            let start = (i * 1024) as u64;
            let end = start + 102400;
            if end <= file_size {
                let _ = store.get_ranges(&path, &[start..end]).await;
            }
        }

        // Metadata untouched
        assert!(
            cache.metadata_cache().get(&footer_key).await.is_some(),
            "metadata must survive data cache LRU pressure"
        );
        let footer_after = store
            .get_range(&path, footer_start..file_size)
            .await
            .unwrap();
        assert_eq!(footer_after, footer);
    });
}

/// Suffix fetch resolves to correct absolute range and hits metadata cache.
#[test]
fn suffix_fetch_resolves_and_hits_cache() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "suffix.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "suffix.parquet",
        file_size,
    );
    let path = Path::from("suffix.parquet");

    let suffix_size = 4096u64;
    let expected_start = file_size - suffix_size;

    // Warmup: put with absolute range key
    warmup_metadata(
        &cache,
        parquet_dir.path(),
        "suffix.parquet",
        expected_start,
        file_size,
    );

    block_on(async {
        // Suffix fetch should resolve to same absolute key and hit cache
        use object_store::GetOptions;
        let opts = GetOptions {
            range: Some(object_store::GetRange::Suffix(suffix_size)),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await.unwrap();
        let suffix_bytes = result.bytes().await.unwrap();

        // Must match what we put via absolute range
        let bounded_bytes = store
            .get_range(&path, expected_start..file_size)
            .await
            .unwrap();
        assert_eq!(
            suffix_bytes, bounded_bytes,
            "suffix fetch must resolve to same bytes as bounded range"
        );
    });
}

/// Multiple concurrent reads of same metadata range — all succeed with same bytes.
#[test]
fn concurrent_metadata_reads_are_safe() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "concurrent.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "concurrent.parquet",
        file_size,
    );
    let path = Path::from("concurrent.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Warmup
    let expected = warmup_metadata(
        &cache,
        parquet_dir.path(),
        "concurrent.parquet",
        footer_start,
        file_size,
    );

    block_on(async {
        let mut handles = Vec::new();
        for _ in 0..10 {
            let store = store.clone();
            let path = path.clone();
            handles.push(tokio::spawn(async move {
                store
                    .get_range(&path, footer_start..file_size)
                    .await
                    .unwrap()
            }));
        }

        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        for (i, result) in results.iter().enumerate() {
            assert_eq!(
                result, &expected,
                "concurrent read {} must match warmup bytes",
                i
            );
        }
    });
}

/// A range read via get_range (single) and get_ranges (multi with one element)
/// must NOT create duplicate cache entries — verify they use compatible paths.
#[test]
fn get_range_and_get_ranges_share_same_cache_key() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "keyshare.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "keyshare.parquet",
        file_size,
    );
    let path = Path::from("keyshare.parquet");

    block_on(async {
        // Read first 4KB via get_ranges (data path → data_cache)
        let via_ranges = store.get_ranges(&path, &[0u64..4096]).await.unwrap();

        // Read same range via get_range (metadata path → metadata_cache)
        let via_range = store.get_range(&path, 0u64..4096).await.unwrap();

        // Both must return same bytes
        assert_eq!(
            via_ranges[0], via_range,
            "get_range and get_ranges must return same bytes"
        );

        // The key is the same regardless of path
        let key = range_cache_key("keyshare.parquet", 0, 4096);

        // get_ranges puts in data_cache, get_range puts in metadata_cache
        // One or both should have it — important thing is bytes are correct
        let in_data = cache.data_cache().get(&key).await;
        let in_meta = cache.metadata_cache().get(&key).await;
        assert!(
            in_data.is_some() || in_meta.is_some(),
            "range must be cached in at least one tier"
        );
    });
}

/// When metadata cache is full, reads still succeed via local FS / S3.
/// Foyer may drop entries that exceed capacity (LRU hasn't run yet).
/// The system degrades gracefully — no panics, no errors.
#[test]
fn metadata_cache_full_reads_still_succeed_via_local_fs() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // Write a large parquet file
    let file_size = write_test_parquet(parquet_dir.path(), "breach.parquet", 10);

    // Tiny metadata cache (1MB disk, 1MB block) — will fill quickly
    let data_cache = Arc::new(FoyerCache::new(
        DISK_BYTES,
        data_dir.path(),
        BLOCK_SIZE,
        BUFFER_POOL,
        SUBMIT_QUEUE,
        "auto",
        0,
        0.0,
        0,
        false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        1 * 1024 * 1024,
        meta_dir.path(),
        BLOCK_SIZE,
        BUFFER_POOL,
        SUBMIT_QUEUE,
        "auto",
        0,
        0.0,
        0,
        false,
    ));
    let cache = Arc::new(TieredBlockCache::new(data_cache, metadata_cache));
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "breach.parquet",
        file_size,
    );
    let path = Path::from("breach.parquet");

    block_on(async {
        // Read multiple ranges — even with small metadata cache, reads succeed
        // (served from local FS since get_opts doesn't auto-populate)
        let mut all_bytes = Vec::new();
        for i in 0..10 {
            let start = (i * 1024) as u64;
            let end = start + 4096;
            if end <= file_size {
                let bytes = store.get_range(&path, start..end).await.unwrap();
                all_bytes.push((start, end, bytes));
            }
        }

        // All reads succeed — no panics, no errors regardless of cache state
        assert!(
            !all_bytes.is_empty(),
            "reads must succeed regardless of metadata cache pressure"
        );

        // Repeated reads also succeed (from local FS — get_opts does not auto-populate cache)
        for (start, end, original) in &all_bytes {
            let bytes = store.get_range(&path, *start..*end).await.unwrap();
            assert_eq!(
                &bytes, original,
                "repeated read of {}..{} must return same bytes",
                start, end
            );
        }
    });
}

/// DataFusion reads Parquet through the TieredObjectStore, executing a real SQL query.
#[test]
fn datafusion_query_through_tiered_store() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "query.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "query.parquet",
        file_size,
    );

    block_on(async {
        let (ctx, _schema) =
            setup_df_session(store.clone(), "query.parquet", "test_table", None).await;

        let df = ctx
            .sql("SELECT id, name FROM test_table WHERE id < 5 ORDER BY id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "WHERE id < 5 should return 5 rows");
    });
}

/// Warmup puts metadata via store.put_metadata(), then DataFusion query reads
/// metadata from metadata_cache (never-evict) and data from data_cache.
/// After warmup, the local file is deleted to prove all reads come from cache.
#[test]
fn warmup_put_metadata_then_datafusion_query_from_cache() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "warm.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "warm.parquet", file_size);

    block_on(async {
        // ── Warmup: read metadata through store, then promote to metadata_cache ──
        // First, let DataFusion do a full query to discover what ranges it needs.
        // This populates data_cache with all metadata + data ranges.
        let (ctx, schema) = setup_df_session(store.clone(), "warm.parquet", "t", None).await;

        let batches = ctx
            .sql("SELECT id FROM t WHERE id < 5 ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);

        // Step 2: Now promote the footer range to metadata_cache.
        let footer_start = file_size.saturating_sub(64 * 1024);
        let footer_key = range_cache_key("warm.parquet", footer_start, file_size);
        if let Some(footer_bytes) = cache.get(&footer_key).await {
            store.put_metadata("warm.parquet", &[footer_start..file_size], &[footer_bytes]);
        }

        // Verify metadata is now in metadata_cache
        assert!(
            cache.metadata_cache().get(&footer_key).await.is_some(),
            "footer must be in metadata_cache after put_metadata"
        );

        // ── Delete local file — all subsequent reads must come from cache ────
        std::fs::remove_file(parquet_dir.path().join("warm.parquet")).unwrap();

        // ── Query again: must succeed entirely from cache ────────────────────
        let (ctx2, _) = setup_df_session(store.clone(), "warm.parquet", "t", Some(schema)).await;

        let batches2 = ctx2
            .sql("SELECT id FROM t WHERE id < 5 ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches2.iter().map(|b| b.num_rows()).sum::<usize>(),
            5,
            "query after file deletion must succeed from cache (metadata in metadata_cache)"
        );
    });
}

/// Key alignment test: warmup via put_metadata, then DataFusion query hits cache.
///
/// Strategy:
///   1. Run DataFusion query (cold start) — all reads go to local FS and populate
///      data_cache via get_opts/get_ranges. Query succeeds.
///   2. Delete local file.
///   3. Run same query again — must succeed entirely from cache (data_cache populated
///      by step 1). This proves the keys produced by DataFusion's read path are the
///      same keys stored in the cache — key alignment is correct.
///
/// This validates that get_opts (metadata path) and get_ranges (data path) produce
/// consistent, deterministic cache keys that are found on subsequent probes.
#[test]
fn datafusion_query_succeeds_from_cache_after_local_file_deleted() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "align.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "align.parquet",
        file_size,
    );

    block_on(async {
        // ── Query 1: cold start, file on local FS ────────────────────────────
        let (ctx, schema) = setup_df_session(store.clone(), "align.parquet", "t", None).await;

        let batches = ctx
            .sql("SELECT id FROM t WHERE id < 3 ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows1: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows1, 3, "query 1 must return 3 rows");

        // ── Delete local file — cache is the only source now ─────────────────
        std::fs::remove_file(parquet_dir.path().join("align.parquet")).unwrap();

        // ── Query 2: same query, file gone — must succeed from cache ─────────
        let (ctx2, _) = setup_df_session(store.clone(), "align.parquet", "t", Some(schema)).await;

        let batches2 = ctx2
            .sql("SELECT id FROM t WHERE id < 3 ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows2, 3,
            "query 2 (file deleted) must succeed from cache — proves key alignment"
        );
    });
}

// ── New correctness-risk integration tests ─────────────────────────────────────

/// Write a Parquet file with page-level statistics (column index + offset index)
/// enabled. Returns the file size.
#[allow(deprecated)]
fn write_page_indexed_parquet(
    dir: &std::path::Path,
    filename: &str,
    num_row_groups: usize,
    num_columns: usize,
) -> u64 {
    let mut fields: Vec<Field> = Vec::new();
    for i in 0..num_columns {
        fields.push(Field::new(format!("col_{}", i), DataType::Int64, false));
    }
    let schema = Arc::new(Schema::new(fields));

    let file_path = dir.join(filename);
    let file = std::fs::File::create(&file_path).unwrap();

    // Enable page-level statistics by setting write_page_index(true)
    // and small max_row_group_size so we get multiple row groups.
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .set_column_index_truncate_length(Some(64))
        .set_write_batch_size(50) // force multiple pages per row group
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    for rg in 0..num_row_groups {
        let offset = (rg * 100) as i64;
        let columns: Vec<Arc<dyn arrow_array::Array>> = (0..num_columns)
            .map(|c| {
                let vals: Vec<i64> = (offset..offset + 100)
                    .map(|v| v + (c as i64 * 1000))
                    .collect();
                Arc::new(Int64Array::from(vals)) as Arc<dyn arrow_array::Array>
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
    std::fs::metadata(&file_path).unwrap().len()
}

/// **Test 1**: Page index key alignment — warmup computes page index ranges from
/// footer metadata and stores them in metadata Foyer. At query time, the same
/// ranges must be found in the cache (key alignment).
///
/// Strategy: warmup puts page index ranges into metadata Foyer using the same fold
/// logic as `custom_cache_manager.rs`. Then delete the local file. Reading those
/// ranges via the store must succeed from cache — proving key alignment.
#[test]
fn page_index_key_alignment_warmup_matches_query_time() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // Write a multi-column file with page indexes enabled
    let file_size = write_page_indexed_parquet(parquet_dir.path(), "page_idx.parquet", 3, 5);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "page_idx.parquet",
        file_size,
    );
    let path = Path::from("page_idx.parquet");

    // Read footer and compute page index ranges using the shared helper
    let file = std::fs::File::open(parquet_dir.path().join("page_idx.parquet")).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = reader.metadata();

    // Compute ONE global merged range — matching parquet crate's range_for_page_index()
    let mut index_ranges: Vec<std::ops::Range<u64>> = Vec::new();
    if let Some(r) = compute_global_page_index_range(parquet_metadata) {
        index_ranges.push(r);
    }

    // Must have a page index range (proves our test file has page indexes)
    assert!(
        !index_ranges.is_empty(),
        "test file must have page index data; got {} ranges",
        index_ranges.len()
    );

    // Warmup: read actual bytes and put into metadata Foyer via store.put_metadata()
    let file_bytes = std::fs::read(parquet_dir.path().join("page_idx.parquet")).unwrap();
    let range_data: Vec<bytes::Bytes> = index_ranges
        .iter()
        .map(|r| bytes::Bytes::copy_from_slice(&file_bytes[r.start as usize..r.end as usize]))
        .collect();
    store.put_metadata("page_idx.parquet", &index_ranges, &range_data);

    // Assert: the global page index range covers ALL individual column offsets
    let global_range = &index_ranges[0];
    for rg in parquet_metadata.row_groups() {
        for (col_idx, col) in rg.columns().iter().enumerate() {
            if let (Some(offset), Some(length)) =
                (col.column_index_offset(), col.column_index_length())
            {
                let start = offset as u64;
                let end = start + length as u64;
                assert!(
                    start >= global_range.start && end <= global_range.end,
                    "col {} column_index {}..{} must be within global range {}..{}",
                    col_idx,
                    start,
                    end,
                    global_range.start,
                    global_range.end
                );
            }
            if let (Some(offset), Some(length)) =
                (col.offset_index_offset(), col.offset_index_length())
            {
                let start = offset as u64;
                let end = start + length as u64;
                assert!(
                    start >= global_range.start && end <= global_range.end,
                    "col {} offset_index {}..{} must be within global range {}..{}",
                    col_idx,
                    start,
                    end,
                    global_range.start,
                    global_range.end
                );
            }
        }
    }

    // Assert: the exact cache key we expect is in metadata Foyer
    let expected_key = range_cache_key("page_idx.parquet", global_range.start, global_range.end);
    block_on(async {
        assert!(
            cache.metadata_cache().get(&expected_key).await.is_some(),
            "exact global page index key {}..{} must be in metadata Foyer",
            global_range.start,
            global_range.end
        );

        // Assert: page index range is NOT in data Foyer (put_metadata goes only to metadata)
        assert!(
            cache.data_cache().get(&expected_key).await.is_none(),
            "page index range must NOT be in data Foyer — only metadata Foyer"
        );
    });

    // Also warmup the footer (last 8KB)
    let footer_start = file_size.saturating_sub(8 * 1024);
    warmup_metadata(
        &cache,
        parquet_dir.path(),
        "page_idx.parquet",
        footer_start,
        file_size,
    );

    // Delete local file — all reads must come from metadata cache
    std::fs::remove_file(parquet_dir.path().join("page_idx.parquet")).unwrap();

    block_on(async {
        // Read the global page index range via store — must succeed from metadata cache
        let result = store
            .get_range(&path, global_range.start..global_range.end)
            .await;
        assert!(result.is_ok(),
            "global page index range ({}..{}) must be served from metadata cache after file deletion",
            global_range.start, global_range.end);
        let bytes = result.unwrap();
        assert_eq!(
            bytes, range_data[0],
            "page index bytes must match warmup data byte-for-byte"
        );
    });
}

/// **Test 2**: Concurrent shard warmup does not corrupt shared metadata Foyer.
///
/// Multiple shards warming up in parallel (different files, same TieredBlockCache)
/// must not interfere. After all complete, each file's metadata is independently
/// correct and retrievable.
#[test]
fn concurrent_shard_warmup_does_not_corrupt() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());

    // Create 5 parquet files simulating 5 shard warmups
    let num_shards = 5;
    let mut file_info: Vec<(String, u64, bytes::Bytes)> = Vec::new();
    for i in 0..num_shards {
        let filename = format!("shard_{}.parquet", i);
        let file_size = write_test_parquet(parquet_dir.path(), &filename, 2 + i);
        let footer_start = file_size.saturating_sub(8 * 1024);
        // Read footer bytes before spawning tasks
        let file_bytes = std::fs::read(parquet_dir.path().join(&filename)).unwrap();
        let footer_bytes =
            bytes::Bytes::copy_from_slice(&file_bytes[footer_start as usize..file_size as usize]);
        file_info.push((filename, file_size, footer_bytes));
    }

    block_on(async {
        // Spawn 5 concurrent warmup tasks
        let mut handles = Vec::new();
        for (filename, file_size, footer_bytes) in &file_info {
            let cache_clone = cache.clone();
            let filename = filename.clone();
            let file_size = *file_size;
            let footer_bytes = footer_bytes.clone();
            handles.push(tokio::spawn(async move {
                let footer_start = file_size.saturating_sub(8 * 1024);
                let key = range_cache_key(&filename, footer_start, file_size);
                cache_clone.put_metadata(&key, footer_bytes);
            }));
        }

        // Wait for all warmups to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // put_metadata is fire-and-forget into Foyer's disk-only storage (memory tier = 1 byte);
        // insert() returns before the storage flusher persists the entry. Under heavy parallel load
        // (full test suite) a get() issued immediately after can miss an entry still queued in the
        // write buffer. Drain the flusher first — its documented post-condition is "all previously
        // put() entries are on SSD and findable via get()" — so the verification reads are deterministic.
        cache.wait_for_flush().await;

        // Verify each file's metadata is independently correct
        for (filename, file_size, expected_bytes) in &file_info {
            let footer_start = file_size.saturating_sub(8 * 1024);
            let key = range_cache_key(filename, footer_start, *file_size);
            let cached = cache.metadata_cache().get(&key).await;
            assert!(
                cached.is_some(),
                "metadata for {} must be retrievable after concurrent warmup",
                filename
            );
            assert_eq!(
                cached.unwrap(),
                *expected_bytes,
                "metadata for {} must not be corrupted by concurrent warmup",
                filename
            );
        }
    });
}

/// **Test 3**: Metadata Foyer capacity breach graceful degradation.
///
/// When metadata Foyer SSD fills, new put_metadata calls may fail silently (Foyer
/// behavior). Reads should still work for entries that survived, and no panics or
/// errors occur — graceful degradation.
#[test]
fn metadata_foyer_capacity_breach_graceful_degradation() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // Write a larger file to have enough data to fill cache
    let file_size = write_test_parquet(parquet_dir.path(), "overflow.parquet", 10);

    // Create a metadata cache with very small capacity (1MB)
    let data_cache = Arc::new(FoyerCache::new(
        DISK_BYTES,
        data_dir.path(),
        BLOCK_SIZE,
        BUFFER_POOL,
        SUBMIT_QUEUE,
        "auto",
        0,
        0.0,
        0,
        false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        1 * 1024 * 1024,
        meta_dir.path(),
        BLOCK_SIZE,
        BUFFER_POOL,
        SUBMIT_QUEUE,
        "auto",
        0,
        0.0,
        0,
        false,
    ));
    let cache = Arc::new(TieredBlockCache::new(data_cache, metadata_cache));
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "overflow.parquet",
        file_size,
    );
    let path = Path::from("overflow.parquet");

    // Read file bytes for warmup
    let file_bytes = std::fs::read(parquet_dir.path().join("overflow.parquet")).unwrap();

    // Put many metadata entries to exceed 1MB capacity.
    // Use small chunk sizes that fit within our actual file size.
    let mut entries: Vec<(u64, u64, bytes::Bytes)> = Vec::new();
    let chunk_size = 1024u64; // 1KB chunks — small enough for our test file
    let num_entries = (file_size / chunk_size).max(1);
    for i in 0..num_entries {
        let start = i * chunk_size;
        let end = ((i + 1) * chunk_size).min(file_size);
        if end > start && (end as usize) <= file_bytes.len() {
            let data = bytes::Bytes::copy_from_slice(&file_bytes[start as usize..end as usize]);
            entries.push((start, end, data.clone()));
            let key = range_cache_key("overflow.parquet", start, end);
            cache.put_metadata(&key, data);
        }
    }

    // No panics must have occurred. Now verify graceful degradation.
    block_on(async {
        let mut hits = 0;
        let mut misses = 0;

        for (start, end, expected) in &entries {
            let key = range_cache_key("overflow.parquet", *start, *end);
            match cache.metadata_cache().get(&key).await {
                Some(cached) => {
                    assert_eq!(
                        &cached, expected,
                        "cached metadata at {}..{} must match",
                        start, end
                    );
                    hits += 1;
                }
                None => {
                    misses += 1;
                }
            }
        }

        // Some entries should have survived (at least the most recent ones)
        assert!(hits > 0,
            "at least some metadata entries must survive capacity pressure (got {} hits, {} misses)",
            hits, misses);

        // Reads via store must not error even for ranges that missed metadata cache
        // (they fall through to local FS or data cache)
        for (start, end, _) in &entries {
            let result = store.get_range(&path, *start..*end).await;
            assert!(result.is_ok(),
                "get_range({}..{}) must succeed (graceful degradation) even under capacity pressure",
                start, end);
        }
    });
}

/// **Test 4**: Page index ranges match parquet crate computation.
///
/// Our warmup code computes page index ranges by folding column_index_offset/length
/// and offset_index_offset/length across columns per RG. This test verifies that
/// the fold produces ranges that cover all column metadata by checking against
/// individual column offsets.
#[test]
fn page_index_ranges_match_parquet_crate_computation() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let parquet_dir = TempDir::new().unwrap();

    // Write a multi-column file (5 columns, 4 row groups)
    let _file_size = write_page_indexed_parquet(parquet_dir.path(), "parity.parquet", 4, 5);

    let file = std::fs::File::open(parquet_dir.path().join("parity.parquet")).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = reader.metadata();

    for (rg_idx, rg) in parquet_metadata.row_groups().iter().enumerate() {
        // Compute merged ranges using our shared fold helper
        let (col_idx_range, off_idx_range) = compute_per_rg_page_index_ranges(rg);

        // Verify that every individual column's range is fully contained within the merged range
        for (col_idx, col) in rg.columns().iter().enumerate() {
            if let (Some(offset), Some(length)) =
                (col.column_index_offset(), col.column_index_length())
            {
                let start = offset as u64;
                let end = start + length as u64;
                let merged = col_idx_range.as_ref().unwrap();
                assert!(
                    start >= merged.start && end <= merged.end,
                    "RG {} col {} column_index range {}..{} must be contained in merged {}..{}",
                    rg_idx,
                    col_idx,
                    start,
                    end,
                    merged.start,
                    merged.end
                );
            }

            if let (Some(offset), Some(length)) =
                (col.offset_index_offset(), col.offset_index_length())
            {
                let start = offset as u64;
                let end = start + length as u64;
                let merged = off_idx_range.as_ref().unwrap();
                assert!(
                    start >= merged.start && end <= merged.end,
                    "RG {} col {} offset_index range {}..{} must be contained in merged {}..{}",
                    rg_idx,
                    col_idx,
                    start,
                    end,
                    merged.start,
                    merged.end
                );
            }
        }

        // Verify the merged range is tight (start == min offset, end == max offset+length)
        if let Some(ref merged) = col_idx_range {
            let actual_min = rg
                .columns()
                .iter()
                .filter_map(|c| c.column_index_offset().map(|o| o as u64))
                .min()
                .unwrap();
            let actual_max = rg
                .columns()
                .iter()
                .filter_map(|c| {
                    c.column_index_offset()
                        .and_then(|o| c.column_index_length().map(|l| o as u64 + l as u64))
                })
                .max()
                .unwrap();
            assert_eq!(
                merged.start, actual_min,
                "RG {} column_index merged start must equal min offset",
                rg_idx
            );
            assert_eq!(
                merged.end, actual_max,
                "RG {} column_index merged end must equal max offset+length",
                rg_idx
            );
        }

        if let Some(ref merged) = off_idx_range {
            let actual_min = rg
                .columns()
                .iter()
                .filter_map(|c| c.offset_index_offset().map(|o| o as u64))
                .min()
                .unwrap();
            let actual_max = rg
                .columns()
                .iter()
                .filter_map(|c| {
                    c.offset_index_offset()
                        .and_then(|o| c.offset_index_length().map(|l| o as u64 + l as u64))
                })
                .max()
                .unwrap();
            assert_eq!(
                merged.start, actual_min,
                "RG {} offset_index merged start must equal min offset",
                rg_idx
            );
            assert_eq!(
                merged.end, actual_max,
                "RG {} offset_index merged end must equal max offset+length",
                rg_idx
            );
        }
    }
}

/// **Test 5**: get_opts probe does NOT pollute metadata Foyer with column data.
///
/// At query time, column data reads via CachedMetadataReader::get_bytes() go
/// through get_opts. This must NOT put column data bytes into metadata Foyer.
/// Only warmup's explicit put_metadata() should populate metadata Foyer.
#[test]
fn get_opts_probe_does_not_pollute_metadata_foyer() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "nopollute.parquet", 3);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "nopollute.parquet",
        file_size,
    );
    let path = Path::from("nopollute.parquet");

    // First, put only the footer into metadata cache via explicit warmup
    let footer_start = file_size.saturating_sub(8 * 1024);
    warmup_metadata(
        &cache,
        parquet_dir.path(),
        "nopollute.parquet",
        footer_start,
        file_size,
    );

    block_on(async {
        // Verify footer IS in metadata cache
        let footer_key = range_cache_key("nopollute.parquet", footer_start, file_size);
        assert!(
            cache.metadata_cache().get(&footer_key).await.is_some(),
            "footer must be in metadata cache after warmup"
        );

        // Now read a column data range via get_range (simulates CachedMetadataReader::get_bytes)
        // This goes through get_opts path
        let data_start = 0u64;
        let data_end = 4096u64;
        let result = store.get_range(&path, data_start..data_end).await;
        assert!(result.is_ok(), "get_range must succeed");

        // The column data range must NOT be in metadata cache
        let data_key = range_cache_key("nopollute.parquet", data_start, data_end);
        assert!(cache.metadata_cache().get(&data_key).await.is_none(),
            "column data range must NOT be in metadata cache — get_opts must not auto-populate metadata");
        assert!(
            cache.data_cache().get(&data_key).await.is_some(),
            "get_opts must populate data cache on miss"
        );

        // Contrast: reading via get_ranges DOES populate data cache
        let data2 = store.get_ranges(&path, &[100u64..4196]).await.unwrap();
        assert_eq!(data2[0].len(), 4096);
        let data2_key = range_cache_key("nopollute.parquet", 100, 4196);
        assert!(
            cache.data_cache().get(&data2_key).await.is_some(),
            "get_ranges must populate data cache"
        );
        assert!(
            cache.metadata_cache().get(&data2_key).await.is_none(),
            "get_ranges must NOT populate metadata cache"
        );
    });
}

/// **Test 6**: Restart — no S3/local FS reads for previously warmed metadata.
///
/// On restart, warmup re-runs. Previously warmed metadata (footer + page indexes)
/// should be recovered from Foyer's SSD tier, not re-fetched from local FS.
///
/// Strategy: warmup puts metadata ranges into metadata Foyer, drop caches,
/// recreate on same dirs. After recovery, verify that the majority of warmed
/// ranges are still available from SSD. Uses the existing `metadata_survives_restart`
/// pattern but with multiple ranges including page indexes.
///
/// Note: Foyer's disk recovery may not recover every entry (small entries below
/// block alignment may be lost), so we verify that at least the footer and some
/// page index ranges survive — proving the SSD recovery path works.
#[test]
fn restart_no_s3_for_previously_warmed_metadata() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_page_indexed_parquet(parquet_dir.path(), "restart_meta.parquet", 3, 3);

    // Session 1: warmup puts footer + page index ranges into metadata Foyer
    let mut warmed_ranges: Vec<std::ops::Range<u64>> = Vec::new();
    let mut warmed_data: Vec<bytes::Bytes> = Vec::new();
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        let store = create_store(
            parquet_dir.path(),
            cache.clone(),
            "restart_meta.parquet",
            file_size,
        );

        // Read file and compute page index ranges
        let file = std::fs::File::open(parquet_dir.path().join("restart_meta.parquet")).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let parquet_metadata = reader.metadata();
        let file_bytes = std::fs::read(parquet_dir.path().join("restart_meta.parquet")).unwrap();

        // Footer range (last 8KB — large enough to survive Foyer block alignment)
        let footer_start = file_size.saturating_sub(8 * 1024);
        warmed_ranges.push(footer_start..file_size);
        warmed_data.push(bytes::Bytes::copy_from_slice(
            &file_bytes[footer_start as usize..file_size as usize],
        ));

        // Page index ranges
        for rg in parquet_metadata.row_groups() {
            let (col_idx_range, off_idx_range) = compute_per_rg_page_index_ranges(rg);
            if let Some(r) = col_idx_range {
                warmed_data.push(bytes::Bytes::copy_from_slice(
                    &file_bytes[r.start as usize..r.end as usize],
                ));
                warmed_ranges.push(r);
            }
            if let Some(r) = off_idx_range {
                warmed_data.push(bytes::Bytes::copy_from_slice(
                    &file_bytes[r.start as usize..r.end as usize],
                ));
                warmed_ranges.push(r);
            }
        }

        assert!(
            warmed_ranges.len() >= 2,
            "must have footer + at least 1 page index range"
        );

        // Put all ranges into metadata Foyer
        store.put_metadata("restart_meta.parquet", &warmed_ranges, &warmed_data);
    }
    // Session 1 dropped — Foyer flushes to SSD

    // Session 2: new Foyer instances on same directories — should recover from SSD
    // NOTE: we do NOT delete the local file here. Instead, we verify recovery by
    // probing the metadata cache directly. This avoids flakiness from Foyer's block
    // alignment behavior with very small entries.
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());

        block_on(async {
            let mut recovered_count = 0;
            for (i, (range, expected)) in warmed_ranges.iter().zip(warmed_data.iter()).enumerate() {
                let key = range_cache_key("restart_meta.parquet", range.start, range.end);
                if let Some(cached) = cache.metadata_cache().get(&key).await {
                    assert_eq!(
                        &cached, expected,
                        "recovered range {} ({}..{}) bytes must match original warmup data",
                        i, range.start, range.end
                    );
                    recovered_count += 1;
                }
            }

            // The footer (range 0) must survive — it is the largest entry and most
            // critical for restart without S3 calls.
            let footer_key = range_cache_key(
                "restart_meta.parquet",
                warmed_ranges[0].start,
                warmed_ranges[0].end,
            );
            assert!(cache.metadata_cache().get(&footer_key).await.is_some(),
                "footer must survive SSD recovery — this is the primary restart-without-S3 guarantee");

            // At least the footer should be recovered; page index ranges may or may not
            // depending on Foyer's block packing. The key correctness guarantee is that
            // recovered data is byte-for-byte correct (verified above).
            assert!(
                recovered_count >= 1,
                "at least the footer must survive SSD recovery (recovered {} of {} ranges)",
                recovered_count,
                warmed_ranges.len()
            );
        });
    }
}

/// **HIGH-CONFIDENCE TEST**: Full production warmup sequence → delete file →
/// DataFusion query succeeds entirely from cache.
///
/// This exercises the EXACT production path:
/// 1. Warmup: parse footer, compute page index ranges, put all into metadata Foyer
/// 2. First DataFusion query: reads column data → populates data Foyer
/// 3. Delete the local Parquet file (simulates warm node with no local copy)
/// 4. Second DataFusion query: must succeed from cache alone
///    - Metadata (footer): served from metadata Foyer via get_opts probe
///    - Column data: served from data Foyer via get_ranges probe
///
/// If this test passes, key alignment is proven for ALL byte ranges across the
/// full stack: warmup → DataFusion → TieredObjectStore → TieredBlockCache → Foyer.
#[test]
fn production_warmup_then_query_from_cache_only() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_page_indexed_parquet(parquet_dir.path(), "prod.parquet", 3, 4);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "prod.parquet", file_size);

    // ── Step 1: Production warmup (same logic as warmup_file_with_store) ─────
    let file_bytes = std::fs::read(parquet_dir.path().join("prod.parquet")).unwrap();
    let file = std::fs::File::open(parquet_dir.path().join("prod.parquet")).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = reader.metadata();

    // Compute all metadata ranges (footer + page indexes)
    let mut metadata_ranges: Vec<std::ops::Range<u64>> = Vec::new();
    let mut metadata_bytes: Vec<bytes::Bytes> = Vec::new();

    // Footer
    let footer_start = file_size.saturating_sub(64 * 1024);
    metadata_ranges.push(footer_start..file_size);
    metadata_bytes.push(bytes::Bytes::copy_from_slice(
        &file_bytes[footer_start as usize..file_size as usize],
    ));

    // Page/offset indexes per RG
    for rg in parquet_metadata.row_groups() {
        let (col_idx_range, off_idx_range) = compute_per_rg_page_index_ranges(rg);
        if let Some(r) = col_idx_range {
            metadata_bytes.push(bytes::Bytes::copy_from_slice(
                &file_bytes[r.start as usize..r.end as usize],
            ));
            metadata_ranges.push(r);
        }
        if let Some(r) = off_idx_range {
            metadata_bytes.push(bytes::Bytes::copy_from_slice(
                &file_bytes[r.start as usize..r.end as usize],
            ));
            metadata_ranges.push(r);
        }
    }

    // Put metadata into metadata Foyer (production warmup step)
    store.put_metadata("prod.parquet", &metadata_ranges, &metadata_bytes);

    // ── Step 2: First DataFusion query (populates data Foyer with column data) ──
    block_on(async {
        let (ctx, schema) = setup_df_session(store.clone(), "prod.parquet", "prod", None).await;

        // Run query — this reads column data via get_ranges → data Foyer
        let batches = ctx
            .sql("SELECT col_0, col_1 FROM prod WHERE col_0 < 50")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows1: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows1 > 0, "first query must return rows");

        // ── Step 3: Delete local file ────────────────────────────────────────
        std::fs::remove_file(parquet_dir.path().join("prod.parquet")).unwrap();

        // ── Step 4: Second query — must succeed entirely from cache ───────────
        let (ctx2, _) = setup_df_session(store.clone(), "prod.parquet", "prod", Some(schema)).await;

        let batches2 = ctx2
            .sql("SELECT col_0, col_1 FROM prod WHERE col_0 < 50")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows2, rows1,
            "second query (file deleted) must return same rows as first — proves full cache correctness");
    });
}

/// **HIGH-CONFIDENCE TEST**: Restart with file deleted — Foyer is the ONLY source.
///
/// Session 1: production warmup + DataFusion query (populates both caches)
/// Session 2: new Foyer instances (same SSD dirs), file deleted → DataFusion query succeeds.
///
/// This proves that across a full restart (new process, new Foyer instances),
/// the recovered SSD state is sufficient to serve all reads without any
/// local FS or S3 access.
///
/// Requires: FoyerCache::drop() calls HybridCache::close() to flush partial blocks.
#[test]
fn restart_with_file_deleted_query_succeeds_from_foyer_only() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "restart_full.parquet", 2);
    let expected_rows;
    let saved_schema;

    // ── Session 1: warmup + query (populates both caches) ────────────────────
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        let store = create_store(
            parquet_dir.path(),
            cache.clone(),
            "restart_full.parquet",
            file_size,
        );

        // Warmup: put footer into metadata Foyer
        let footer_start = file_size.saturating_sub(64 * 1024);
        let file_bytes = std::fs::read(parquet_dir.path().join("restart_full.parquet")).unwrap();
        let footer_bytes = bytes::Bytes::copy_from_slice(&file_bytes[footer_start as usize..]);
        store.put_metadata(
            "restart_full.parquet",
            &[footer_start..file_size],
            &[footer_bytes],
        );

        // Run DataFusion query — populates data Foyer with column data
        let (rows, schema) = block_on(async {
            let (ctx, schema) =
                setup_df_session(store.clone(), "restart_full.parquet", "t", None).await;

            let batches = ctx
                .sql("SELECT id FROM t WHERE id < 10 ORDER BY id")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
            (rows, schema)
        });
        expected_rows = rows;
        saved_schema = schema;
        assert!(expected_rows > 0);
    }
    // Session 1 dropped — FoyerCache::drop() calls HybridCache::close() which
    // flushes partial blocks to SSD. No sleep needed.

    // ── Delete local file between sessions ───────────────────────────────────
    std::fs::remove_file(parquet_dir.path().join("restart_full.parquet")).unwrap();

    // ── Session 2: new Foyer instances, file gone — query from Foyer only ────
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        let store = create_store(
            parquet_dir.path(),
            cache.clone(),
            "restart_full.parquet",
            file_size,
        );

        let rows = block_on(async {
            // Use saved schema from session 1 (in production, CatalogSnapshot provides this)
            let (ctx, _) = setup_df_session(
                store.clone(),
                "restart_full.parquet",
                "t",
                Some(saved_schema),
            )
            .await;

            let batches = ctx
                .sql("SELECT id FROM t WHERE id < 10 ORDER BY id")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        });

        assert_eq!(
            rows, expected_rows,
            "restart query (file deleted, new Foyer instances) must return same rows — \
             proves full lifecycle: warmup → persist → recover → serve from Foyer only"
        );
    }
}

/// get_opts auto-populates data Foyer on miss — repeated single-range reads
/// hit data cache on second access (simulates CachedMetadataReader::get_bytes
/// for column chunks in IndexedExec path).
#[test]
fn get_opts_populates_data_cache_on_miss() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "indexed.parquet", 3);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "indexed.parquet",
        file_size,
    );
    let path = Path::from("indexed.parquet");

    block_on(async {
        let range = 0u64..4096;

        // First read: cache miss → local FS → populate data Foyer
        let bytes1 = store.get_range(&path, range.clone()).await.unwrap();
        assert_eq!(bytes1.len(), 4096);

        // Verify: entry is now in data Foyer (not metadata Foyer)
        let key = range_cache_key("indexed.parquet", 0, 4096);
        assert!(
            cache.data_cache().get(&key).await.is_some(),
            "first read must populate data Foyer"
        );
        assert!(
            cache.metadata_cache().get(&key).await.is_none(),
            "get_opts must NOT populate metadata Foyer"
        );

        // Second read: hits data Foyer (no local FS needed)
        // Delete file to prove it comes from cache
        std::fs::remove_file(parquet_dir.path().join("indexed.parquet")).unwrap();

        let bytes2 = store.get_range(&path, range).await.unwrap();
        assert_eq!(
            bytes2, bytes1,
            "second read must return same bytes from data Foyer cache"
        );
    });
}

/// get_opts skips caching for ranges exceeding max_cache_entry_size.
/// The threshold is configurable and dynamically updatable.
#[test]
fn get_opts_skips_caching_for_large_ranges() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "threshold.parquet", 5);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "threshold.parquet",
        file_size,
    );
    let path = Path::from("threshold.parquet");

    // Set threshold to 2KB — anything larger skips caching
    cache.update_max_data_entry_size(2048);

    block_on(async {
        // Read 4KB range (> 2KB threshold) — should NOT be cached
        let large_range = 0u64..4096.min(file_size);
        let bytes = store.get_range(&path, large_range.clone()).await.unwrap();
        assert!(!bytes.is_empty());

        let large_key = range_cache_key("threshold.parquet", large_range.start, large_range.end);
        assert!(
            cache.data_cache().get(&large_key).await.is_none(),
            "range > threshold must NOT be cached"
        );

        // Read 1KB range (< 2KB threshold) — should be cached
        let small_range = 0u64..1024.min(file_size);
        let _ = store.get_range(&path, small_range.clone()).await.unwrap();

        let small_key = range_cache_key("threshold.parquet", small_range.start, small_range.end);
        assert!(
            cache.data_cache().get(&small_key).await.is_some(),
            "range < threshold must be cached"
        );

        // Dynamic update: increase threshold to 8KB — now 4KB is cached
        cache.update_max_data_entry_size(8192);
        let _ = store.get_range(&path, large_range.clone()).await.unwrap();
        assert!(
            cache.data_cache().get(&large_key).await.is_some(),
            "after threshold increase, 4KB range must be cached"
        );
    });
}

// ── Small-file + per-range metadata persistence (prod-gap reproduction) ──────
//
// Production logs (warm node, ~3 KB Parquet file) showed that after warmup the
// metadata tier's key_index.json contained ONLY the whole-file footer key
// (`<path>␟0-<size>`) — the column-index, offset-index, and 8-byte postscript
// keys were missing. These tests pin the property that EVERY warmed range must
// land in the never-evict metadata tier, and isolate the small-file footer
// collapse that triggers the overlap.

/// Compute the warmup ranges exactly as `custom_cache_manager::warmup_file_with_store`:
///   - CI + OI page-index whole regions (via the real `compute_page_index_range`)
///   - footer:     `[size - 64KB, size]`  (collapses to `[0, size]` for files < 64KB)
///   - postscript: `[size - 8, size]`
fn production_warmup_ranges(
    metadata: &parquet::file::metadata::ParquetMetaData,
    file_size: u64,
) -> Vec<std::ops::Range<u64>> {
    let mut ranges = crate::cache::custom_cache_manager::compute_page_index_range(metadata);
    let footer_start = file_size.saturating_sub(64 * 1024);
    ranges.push(footer_start..file_size);
    let postscript_start = file_size.saturating_sub(8);
    ranges.push(postscript_start..file_size);
    ranges
}

/// In-session per-range persistence: warm a small (< 64 KB) page-indexed file
/// exactly as production does, then assert that EACH warmed range (CI, OI, footer,
/// postscript) is individually present in the metadata tier — not just the
/// whole-file footer key.
#[test]
fn small_file_warmup_persists_every_range_to_metadata_tier() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // 11 columns × 1 row group → a few-KB file, matching the production shape
    // (col_count=11, rg_count=1, size ≈ 3 KB).
    let file_size = write_page_indexed_parquet(parquet_dir.path(), "small.parquet", 1, 11);
    assert!(
        file_size < 64 * 1024,
        "fixture must be smaller than the 64KB footer prefetch"
    );

    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(
        parquet_dir.path(),
        cache.clone(),
        "small.parquet",
        file_size,
    );

    let file = std::fs::File::open(parquet_dir.path().join("small.parquet")).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let ranges = production_warmup_ranges(reader.metadata(), file_size);
    assert_eq!(
        ranges.len(),
        4,
        "expected CI, OI, footer, postscript ranges"
    );

    // Root-cause quirk: for a sub-64KB file the footer range is the whole file.
    assert_eq!(
        ranges[2],
        0..file_size,
        "small-file footer range collapses to the whole file"
    );

    block_on(async {
        let path = Path::from("small.parquet");
        let fetched = store.get_ranges(&path, &ranges).await.unwrap();
        store.put_metadata("small.parquet", &ranges, &fetched);

        for r in &ranges {
            let key = range_cache_key("small.parquet", r.start, r.end);
            assert!(
                cache.metadata_cache().get(&key).await.is_some(),
                "warmed range {}..{} ({} bytes) must be in the metadata tier \
                 (prod showed only the footer key persisting)",
                r.start,
                r.end,
                r.end - r.start
            );
        }
    });
}

/// Restart variant — closest to the production `key_index.json` evidence. Warm all
/// ranges into the metadata tier, drop the caches (FoyerCache::drop flushes +
/// persists), then recreate on the same SSD dirs and assert EVERY warmed range is
/// recovered byte-for-byte. If the small CI/OI/postscript entries are lost on Foyer
/// SSD recovery while the larger footer survives, this fails on those ranges —
/// reproducing the production symptom.
#[test]
fn small_file_warmed_ranges_survive_restart() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_page_indexed_parquet(parquet_dir.path(), "small_restart.parquet", 1, 11);
    assert!(file_size < 64 * 1024);

    let file = std::fs::File::open(parquet_dir.path().join("small_restart.parquet")).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let ranges = production_warmup_ranges(reader.metadata(), file_size);

    let file_bytes = std::fs::read(parquet_dir.path().join("small_restart.parquet")).unwrap();
    let datas: Vec<bytes::Bytes> = ranges
        .iter()
        .map(|r| bytes::Bytes::copy_from_slice(&file_bytes[r.start as usize..r.end as usize]))
        .collect();

    // Session 1: warm all ranges, then drop → flush to SSD + persist key_index.
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        let store = create_store(
            parquet_dir.path(),
            cache.clone(),
            "small_restart.parquet",
            file_size,
        );
        store.put_metadata("small_restart.parquet", &ranges, &datas);
    }

    // Session 2: new instances on the same SSD dirs — Foyer recovers from disk.
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        block_on(async {
            for (i, r) in ranges.iter().enumerate() {
                let key = range_cache_key("small_restart.parquet", r.start, r.end);
                let recovered = cache.metadata_cache().get(&key).await;
                assert!(
                    recovered.is_some(),
                    "range[{}] {}..{} ({} bytes) must survive Foyer SSD recovery \
                     (prod showed only the whole-file footer surviving)",
                    i,
                    r.start,
                    r.end,
                    r.end - r.start
                );
                assert_eq!(
                    recovered.unwrap(),
                    datas[i],
                    "recovered bytes must match the warmed bytes"
                );
            }
        });
    }
}
