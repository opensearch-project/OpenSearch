/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
DataFusion parquet bridge — isolates ALL DataFusion parquet-specific API calls.

# Datafusion APIs in use

- `ParquetSource::new()` + `.with_parquet_file_reader_factory()` + `.with_predicate()`
- `FileScanConfigBuilder::new()` + `.with_file()` + `.with_projection_indices()`
- `DataSourceExec::from_data_source()`
- `PartitionedFile::new()` + `.with_extensions()`
- `ParquetAccessPlan::new_none()` + `.set()`
- `RowGroupAccess::Selection` / `RowGroupAccess::Scan`
- `ParquetFileReaderFactory` trait
- `ParquetFileMetrics`
- `ObjectStoreUrl::local_filesystem()`
- `ArrowReaderMetadata::load()` + `ArrowReaderOptions`
**/

use std::sync::Arc;

use prost::bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelection,
};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_datasource_parquet::{
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory, RowGroupAccess,
};
use datafusion_execution::object_store::ObjectStoreUrl;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::ObjectStore;

// ── Parquet Metadata Loading ───────────────────────────────────────────

/// Load parquet metadata with page index from a file.
pub fn load_parquet_metadata(
    file: &std::fs::File,
) -> std::result::Result<(SchemaRef, Arc<ParquetMetaData>), String> {
    let options = ArrowReaderOptions::new().with_page_index(true);
    let arrow_metadata = ArrowReaderMetadata::load(file, options)
        .map_err(|e| format!("Failed to load parquet metadata: {}", e))?;
    let schema = arrow_metadata.schema().clone();
    let metadata = arrow_metadata.metadata().clone();
    Ok((schema, metadata))
}

/// Load parquet metadata without page index (for row count matching).
pub fn load_parquet_metadata_basic(
    file: &std::fs::File,
) -> std::result::Result<Arc<ParquetMetaData>, String> {
    let options = ArrowReaderOptions::new();
    let arrow_metadata = ArrowReaderMetadata::load(file, options)
        .map_err(|e| format!("Failed to load parquet metadata: {}", e))?;
    Ok(arrow_metadata.metadata().clone())
}

/// Configuration for creating a per-row-group parquet stream.
pub struct RowGroupStreamConfig {
    /// Path to the parquet file.
    pub file_path: String,
    /// File size in bytes.
    pub file_size: u64,
    /// Full (unprojected) schema.
    pub full_schema: SchemaRef,
    /// Cached parquet metadata.
    pub metadata: Arc<ParquetMetaData>,
    /// Column projection indices.
    pub projection: Option<Vec<usize>>,
    /// Physical predicate for pushdown.
    pub predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
}

/// Create a stream that reads a single row group using RowSelection (v48 strategy).
///
/// Predicate pushdown IS safe here — RowSelection is applied during decode,
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

/// Create a stream that reads a single row group with full scan (v46 strategy).
///
/// Predicate pushdown is NOT safe here — BooleanMask is applied AFTER decode,
/// so predicate filtering during decode would cause mask offset misalignment.
pub fn create_full_scan_stream(
    config: &RowGroupStreamConfig,
    rg_index: usize,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let num_rgs = config.metadata.num_row_groups();
    let mut access_plan = ParquetAccessPlan::new_none(num_rgs);
    access_plan.set(rg_index, RowGroupAccess::Scan);

    create_stream_with_access_plan(config, access_plan, false)
}

/// Build a DataSourceExec stream for the given access plan.
///
/// This is the core DataFusion parquet construction — the ONLY place that
/// touches ParquetSource, FileScanConfigBuilder, DataSourceExec, etc.
fn create_stream_with_access_plan(
    config: &RowGroupStreamConfig,
    access_plan: ParquetAccessPlan,
    push_predicate: bool,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let partitioned_file =
        PartitionedFile::new(config.file_path.clone(), config.file_size)
            .with_extensions(Arc::new(access_plan));

    let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());

    let reader_factory = Arc::new(CachedMetadataReaderFactory::new(
        store,
        Arc::clone(&config.metadata),
    )) as Arc<dyn ParquetFileReaderFactory>;

    let mut parquet_source = ParquetSource::new(config.full_schema.clone())
        .with_parquet_file_reader_factory(reader_factory)
        .with_enable_page_index(false);

    if push_predicate {
        if let Some(ref pred) = config.predicate {
            parquet_source = parquet_source
                .with_predicate(Arc::clone(pred))
                .with_pushdown_filters(true)
                .with_reorder_filters(true);
        }
    }

    let mut config_builder = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(parquet_source),
    )
    .with_file(partitioned_file);

    if let Some(ref proj) = config.projection {
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
}

impl CachedMetadataReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>, metadata: Arc<ParquetMetaData>) -> Self {
        Self { store, metadata }
    }

    /// Create with page index stripped — avoids "Invalid offset in sparse
    /// column chunk data" errors when predicate pushdown is enabled.
    #[allow(dead_code)]
    pub fn new_without_page_index(
        store: Arc<dyn ObjectStore>,
        metadata: Arc<ParquetMetaData>,
    ) -> Self {
        use datafusion::parquet::file::metadata::ParquetMetaDataBuilder;
        let stripped = ParquetMetaDataBuilder::new_from_metadata((*metadata).clone())
            .set_column_index(None)
            .set_offset_index(None)
            .build();
        Self {
            store,
            metadata: Arc::new(stripped),
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
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            file.object_meta.location.as_ref(),
            metrics,
        );
        Ok(Box::new(CachedMetadataReader {
            store: Arc::clone(&self.store),
            location: file.object_meta.location.clone(),
            metadata: Arc::clone(&self.metadata),
            metrics: file_metrics,
        }))
    }
}

struct CachedMetadataReader {
    store: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    metadata: Arc<ParquetMetaData>,
    metrics: ParquetFileMetrics,
}

impl AsyncFileReader for CachedMetadataReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        self.metrics.bytes_scanned.add((range.end - range.start) as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        async move {
            store
                .get_range(&location, range)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)))
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
        async move {
            store
                .get_ranges(&location, &ranges)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)))
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
