/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! DataFusion parquet bridge — isolates ALL DataFusion parquet-specific API calls.
//!
//! Everything that touches `ParquetSource`, `FileScanConfigBuilder`,
//! `DataSourceExec`, `ParquetAccessPlan`, `RowGroupAccess::Selection/Scan`,
//! `ParquetFileReaderFactory`, `ArrowReaderMetadata`, `ArrowReaderOptions`
//! lives here. `stream.rs` only uses this module's public API.
//!
//! All I/O goes through the caller-supplied `object_store::ObjectStore`. No
//! direct `LocalFileSystem` / `std::fs` usage — that was the PR #21164 version's
//! design and it was reworked here so the indexed path respects the same store
//! the vanilla path uses (file://, s3://, etc.).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
use datafusion::datasource::physical_plan::parquet::{
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory, RowGroupAccess,
};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::execution::cache::cache_manager::CachedFileMetadataEntry;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::{ObjectStore, ObjectStoreExt};
use prost::bytes::Bytes;

// ── Parquet Metadata Loading ─────────────────────────────────────────

/// Load footer-only parquet metadata, consulting the caller-supplied cache.
///
/// On a cache hit the cached (footer-only) metadata is returned with no IO.
/// On a cache miss we fetch with `PageIndexPolicy::Skip` — never fetching page
/// index bytes — then store the footer in the cache for future hits.
///
/// Issues a `head()` to learn the file's size + last-modified. Callers that
/// already hold an authoritative [`ObjectMeta`] (e.g. from the listing snapshot
/// passed into a `ParquetFileReaderFactory`) should call
/// [`load_parquet_metadata_with_meta`] instead — the `head()` is a `File::open`
/// + `fstat` syscall per call on local stores, which adds up under repeated
/// per-file reader creation.
pub async fn load_parquet_metadata(
    store: Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    metadata_cache: Arc<dyn FileMetadataCache>,
) -> std::result::Result<(SchemaRef, u64, Arc<ParquetMetaData>), String> {
    let meta = store
        .head(location)
        .await
        .map_err(|e| format!("object-store head {location}: {e}"))?;
    load_parquet_metadata_with_meta(store, location, meta, metadata_cache).await
}

/// Like [`load_parquet_metadata`] but uses a caller-supplied [`ObjectMeta`]
/// (size + last_modified) instead of issuing a `head()`. The cache validity
/// check ([`CachedFileMetadataEntry::is_valid_for`]) only consults `size` and
/// `last_modified`, both present in the listing's `ObjectMeta`, so no `head()`
/// is needed when the caller already has it.
pub async fn load_parquet_metadata_with_meta(
    store: Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    meta: object_store::ObjectMeta,
    metadata_cache: Arc<dyn FileMetadataCache>,
) -> std::result::Result<(SchemaRef, u64, Arc<ParquetMetaData>), String> {
    let size = meta.size;

    // Cache hit — return footer-only metadata without any IO.
    let pq_meta = if let Some(entry) = metadata_cache.get(location) {
        if entry.is_valid_for(&meta) {
            entry
                .file_metadata
                .as_any()
                .downcast_ref::<CachedParquetMetaData>()
                .map(|cached| Arc::clone(cached.parquet_metadata()))
        } else {
            None
        }
    } else {
        None
    };

    // Cache miss — fetch metadata. When the scoped page-index cache is enabled,
    // skip page index bytes (they are loaded lazily per-column by the scoped cache).
    // When disabled (fallback mode), fetch the full page index so the metadata cache
    // retains it and DataFusion's default page pruning path continues to work.
    // Scoped enabled: skip page index bytes entirely (scoped cache handles it lazily).
    // Scoped disabled (fallback): use Optional — reads page index if it falls within
    // the same footer fetch range, without issuing a separate IO request for it.
    let policy = if crate::cache::page_index::is_scoped_page_index_enabled() {
        PageIndexPolicy::Skip
    } else {
        PageIndexPolicy::Optional
    };
    let pq_meta = match pq_meta {
        Some(m) => m,
        None => {
            let mut reader = ParquetObjectReader::new(Arc::clone(&store), location.clone());
            let fetched = Arc::new(
                ParquetMetaDataReader::new()
                    .with_page_index_policy(policy)
                    .load_and_finish(&mut reader, size)
                    .await
                    .map_err(|e| format!("load parquet metadata {location}: {e}"))?,
            );
            metadata_cache.put(
                location,
                CachedFileMetadataEntry::new(
                    meta,
                    Arc::new(CachedParquetMetaData::new(Arc::clone(&fetched))),
                ),
            );
            fetched
        }
    };

    let file_meta = pq_meta.file_metadata();
    let schema = parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
        .map_err(|e| format!("parquet_to_arrow_schema {location}: {e}"))?;

    Ok((Arc::new(schema), size, pq_meta))
}

/// Shared accumulator for object-store read wall-time.
#[derive(Debug, Default)]
pub struct ReadIoStats {
    pub total_ns: AtomicU64,
    pub count: AtomicU64,
}

fn record_io(stats: &ReadIoStats, dur: Duration) {
    let ns = dur.as_nanos() as u64;
    stats.total_ns.fetch_add(ns, Ordering::Relaxed);
    stats.count.fetch_add(1, Ordering::Relaxed);
}

/// Configuration for creating a per-row-group parquet stream.
pub struct RowGroupStreamConfig {
    /// Object-store-relative path to the parquet file.
    pub file_path: String,
    pub file_size: u64,
    /// Object store the file lives in (resolved from the session's RuntimeEnv).
    pub store: Arc<dyn ObjectStore>,
    /// URL of the store for DataFusion's `FileScanConfig`.
    pub store_url: ObjectStoreUrl,
    pub full_schema: SchemaRef,
    pub metadata: Arc<ParquetMetaData>,
    pub projection: Option<Vec<usize>>,
    pub predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    pub io_stats: Arc<ReadIoStats>,
}

/// Create a stream that reads a single row group using `RowSelection`.
///
/// Predicate pushdown IS safe here — `RowSelection` is applied during decode,
/// so the predicate sees only selected rows and indices stay aligned.
pub fn create_row_selection_stream(
    config: &RowGroupStreamConfig,
    rg_index: usize,
    selection: RowSelection,
    push_predicate: bool,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let num_rgs = config.metadata.num_row_groups();
    let mut access_plan = ParquetAccessPlan::new_none(num_rgs);
    access_plan.set(rg_index, RowGroupAccess::Selection(selection));
    create_stream_with_access_plan(config, access_plan, push_predicate)
}

/// Create a stream that reads a single row group with full scan.
///
/// Predicate pushdown is NOT safe here — caller applies a `BooleanMask` AFTER
/// decode, so pushdown during decode would cause mask offset misalignment.
pub fn create_full_scan_stream(
    config: &RowGroupStreamConfig,
    rg_index: usize,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let num_rgs = config.metadata.num_row_groups();
    let mut access_plan = ParquetAccessPlan::new_none(num_rgs);
    // TODO(page-boundary-selection): replace `Scan` with a `Selection` built
    // from the caller's candidate bitmap at page boundaries. The idea:
    //   - Read the RG's `offset_index` to get per-page row counts.
    //   - For each page, select if any candidate bit falls within its row
    //     range, else skip.
    //   - Pass the resulting `RowSelection` via
    //     `RowGroupAccess::Selection(selection)`.
    // This keeps the selector Vec small (O(pages), not O(rows)) regardless of
    // candidate density, while letting parquet skip whole pages whose row
    // ranges are entirely outside the candidate set. Bigger I/O savings than
    // today's full-scan for dense-but-clustered matches, and cheap to build
    // for any selectivity — unifying today's split between `RowSelection`
    // strategy (<3%) and `BooleanMask` strategy (≥3%).
    //
    // Before implementing, verify parquet-rs's `Selection` delivery
    // semantics (does it deliver contiguous packed rows or original-position
    // rows with gaps?) so the caller's post-decode mask alignment stays
    // correct. Documented in `pr-reviews/EVALUATOR_HANDOFF.md`.
    access_plan.set(rg_index, RowGroupAccess::Scan);
    create_stream_with_access_plan(config, access_plan, false)
}

fn create_stream_with_access_plan(
    config: &RowGroupStreamConfig,
    access_plan: ParquetAccessPlan,
    push_predicate: bool,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let partitioned_file = PartitionedFile::new(config.file_path.clone(), config.file_size)
        .with_extensions(Arc::new(access_plan));

    let reader_factory = Arc::new(CachedMetadataReaderFactory::new(
        Arc::clone(&config.store),
        Arc::clone(&config.metadata),
        Arc::clone(&config.io_stats),
    )) as Arc<dyn ParquetFileReaderFactory>;

    let mut parquet_source = ParquetSource::new(config.full_schema.clone())
        .with_parquet_file_reader_factory(reader_factory)
        // cannot use page index because we have collector bitset matches that are not visible
        // with just parquet predicates
        .with_enable_page_index(false);

    if push_predicate {
        if let Some(ref pred) = config.predicate {
            parquet_source = parquet_source
                .with_predicate(Arc::clone(pred))
                .with_pushdown_filters(true)
                .with_reorder_filters(true);
        }
    }

    let mut config_builder =
        FileScanConfigBuilder::new(config.store_url.clone(), Arc::new(parquet_source))
            .with_file(partitioned_file);

    if let Some(ref proj) = config.projection {
        // Empty projection (e.g. COUNT(*)) is honoured as "read no
        // columns". Parquet delivers correct row counts via the
        // access plan but skips all column I/O.
        config_builder = config_builder.with_projection_indices(Some(proj.clone()))?;
    }

    let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config_builder.build());
    let ctx = Arc::new(datafusion::execution::TaskContext::default());
    let stream = exec.execute(0, ctx)?;
    Ok((stream, exec))
}

/// Factory that creates parquet readers with pre-cached metadata.
///
/// Avoids re-reading metadata for each row group.
#[derive(Debug)]
pub struct CachedMetadataReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata: Arc<ParquetMetaData>,
    io_stats: Arc<ReadIoStats>,
}

impl CachedMetadataReaderFactory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata: Arc<ParquetMetaData>,
        io_stats: Arc<ReadIoStats>,
    ) -> Self {
        Self {
            store,
            metadata,
            io_stats,
        }
    }
}

impl ParquetFileReaderFactory for CachedMetadataReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, file.object_meta.location.as_ref(), metrics);
        Ok(Box::new(CachedMetadataReader {
            store: Arc::clone(&self.store),
            location: file.object_meta.location.clone(),
            metadata: Arc::clone(&self.metadata),
            metrics: file_metrics,
            io_stats: Arc::clone(&self.io_stats),
        }))
    }
}

struct CachedMetadataReader {
    store: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    metadata: Arc<ParquetMetaData>,
    metrics: ParquetFileMetrics,
    io_stats: Arc<ReadIoStats>,
}

impl AsyncFileReader for CachedMetadataReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        self.metrics
            .bytes_scanned
            .add((range.end - range.start) as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        let io_stats = Arc::clone(&self.io_stats);
        async move {
            let t0 = Instant::now();
            let r = store
                .get_range(&location, range)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)));
            record_io(&io_stats, t0.elapsed());
            r
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Vec<Bytes>>> {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.metrics.bytes_scanned.add(total as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        let io_stats = Arc::clone(&self.io_stats);
        async move {
            let t0 = Instant::now();
            let r = store
                .get_ranges(&location, &ranges)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)));
            record_io(&io_stats, t0.elapsed());
            r
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
        _options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata = Arc::clone(&self.metadata);
        async move { Ok(metadata) }.boxed()
    }
}
