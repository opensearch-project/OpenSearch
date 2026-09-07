use super::opener::LiquidParquetOpener;
use crate::cache::LiquidCacheParquetRef;
use crate::reader::plantime::engagement_policy::{
    CacheEngagementPolicy, default_engagement_policy,
};
use ahash::{HashMap, HashMapExt};
use arrow_schema::Schema;
use bytes::Bytes;
use datafusion::{
    config::TableParquetOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{
            FileScanConfig, FileSource, ParquetFileMetrics, ParquetFileReaderFactory,
            ParquetSource, parquet::PagePruningAccessPlanFilter,
        },
        table_schema::TableSchema,
    },
    error::Result,
    physical_expr::projection::ProjectionExprs,
    physical_expr_adapter::DefaultPhysicalExprAdapterFactory,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        PhysicalExpr,
        metrics::{ExecutionPlanMetricsSet, MetricBuilder},
    },
};
use futures::{FutureExt, future::BoxFuture};
use object_store::{ObjectStore, path::Path};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetObjectReader},
    },
    file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
};
use std::{
    ops::Range,
    sync::{Arc, LazyLock},
};
use tokio::sync::RwLock;

static META_CACHE: LazyLock<MetadataCache> = LazyLock::new(MetadataCache::new);

/// Pre-seed the metadata cache with already-loaded metadata.
/// Callers that have pre-loaded parquet metadata (e.g., from a custom
/// ParquetFileReaderFactory) can inject it here so that LC's opener
/// skips the expensive `ArrowReaderMetadata::load_async()` call.
pub async fn pre_seed_metadata_cache(path: &Path, metadata: Arc<ParquetMetaData>) {
    let mut cache = META_CACHE.val.write().await;
    cache.entry(path.clone()).or_insert(metadata);
}

#[derive(Debug)]
pub(crate) struct CachedMetaReaderFactory {
    store: Arc<dyn ObjectStore>,
}

impl CachedMetaReaderFactory {
    pub(crate) fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub(crate) fn create_liquid_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> ParquetMetadataCacheReader {
        let path = partitioned_file.object_meta.location.clone();
        let store = Arc::clone(&self.store);
        let mut inner = ParquetObjectReader::new(store, path.clone())
            .with_file_size(partitioned_file.object_meta.size);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint);
        }

        ParquetMetadataCacheReader {
            file_metrics: ParquetFileMetrics::new(partition_index, path.as_ref(), metrics),
            inner,
            path,
        }
    }
}

impl ParquetFileReaderFactory for CachedMetaReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let reader = self.create_liquid_reader(
            partition_index,
            partitioned_file,
            metadata_size_hint,
            metrics,
        );
        Ok(Box::new(reader))
    }
}

struct MetadataCache {
    val: RwLock<HashMap<Path, Arc<ParquetMetaData>>>,
}

impl MetadataCache {
    fn new() -> Self {
        Self {
            val: RwLock::new(HashMap::new()),
        }
    }
}

#[derive(Clone)]
pub struct ParquetMetadataCacheReader {
    file_metrics: ParquetFileMetrics,
    inner: ParquetObjectReader,
    path: Path,
}

impl AsyncFileReader for ParquetMetadataCacheReader {
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total as usize);
        self.inner.get_byte_ranges(ranges)
    }

    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.file_metrics
            .bytes_scanned
            .add((range.end - range.start) as usize);
        self.inner.get_bytes(range)
    }

    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let path = self.path.clone();
        let options = options.cloned();
        async move {
            // First check with read lock
            {
                let cache = META_CACHE.val.read().await;
                if let Some(meta) = cache.get(&path) {
                    log::debug!("[LC-Meta] HIT path={}", path);
                    return Ok(meta.clone());
                }
            }

            // Upgrade to write lock and double-check
            let mut cache = META_CACHE.val.write().await;
            match cache.entry(path.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    log::debug!("[LC-Meta] HIT (race) path={}", path);
                    Ok(entry.get().clone())
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    log::debug!("[LC-Meta] MISS (loading from store) path={}", path);
                    let meta = self.inner.get_metadata(options.as_ref()).await?;
                    let meta = Arc::try_unwrap(meta).unwrap_or_else(|e| e.as_ref().clone());
                    let mut reader = ParquetMetaDataReader::new_with_metadata(meta.clone())
                        .with_page_index_policy(PageIndexPolicy::Optional);
                    reader.load_page_index(&mut self.inner).await?;
                    let meta = Arc::new(reader.finish()?);
                    entry.insert(meta.clone());
                    Ok(meta)
                }
            }
        }
        .boxed()
    }
}

/// The data source for LiquidCache
#[derive(Clone)]
pub struct LiquidParquetSource {
    metrics: ExecutionPlanMetricsSet,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    page_pruning_predicate: Option<Arc<PagePruningAccessPlanFilter>>,
    table_parquet_options: TableParquetOptions,
    liquid_cache: LiquidCacheParquetRef,
    batch_size: Option<usize>,
    projection: ProjectionExprs,
    table_schema: TableSchema,
    /// Optional caller-provided reader factory with pre-loaded metadata.
    /// When set, LC's opener uses this to get metadata instantly instead of
    /// fetching from the object store.
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
    /// Policy that decides per-file whether to use LC stream or delegate to parquet.
    engagement_policy: Arc<dyn CacheEngagementPolicy>,
}

impl LiquidParquetSource {
    fn reorder_filters(&self) -> bool {
        self.table_parquet_options.global.reorder_filters
    }

    /// Set the table schema for the LiquidParquetSource
    pub fn with_table_schema(&self, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            ..self.clone()
        }
    }

    /// Set predicate information, also sets pruning_predicate and page_pruning_predicate attributes
    pub fn with_predicate(
        mut self,
        file_schema: Arc<Schema>,
        predicate: Arc<dyn PhysicalExpr>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let predicate_creation_errors =
            MetricBuilder::new(&metrics).global_counter("num_predicate_creation_errors");

        self.metrics = metrics;
        self.predicate = Some(Arc::clone(&predicate));

        match PruningPredicate::try_new(Arc::clone(&predicate), Arc::clone(&file_schema)) {
            Ok(pruning_predicate) => {
                if !pruning_predicate.always_true() {
                    self.pruning_predicate = Some(Arc::new(pruning_predicate));
                }
            }
            Err(e) => {
                log::debug!("Could not create pruning predicate for: {e}");
                predicate_creation_errors.add(1);
            }
        };

        let page_pruning_predicate = Arc::new(PagePruningAccessPlanFilter::new(
            &predicate,
            Arc::clone(&file_schema),
        ));
        self.page_pruning_predicate = Some(page_pruning_predicate);

        self
    }

    /// Set predicate for row_filter only — no page-index pruning.
    /// Used by the indexed path where the BoolNode's RowSelection is authoritative
    /// and page-level statistics must not override it.
    pub fn with_predicate_no_page_pruning(
        mut self,
        file_schema: Arc<Schema>,
        predicate: Arc<dyn PhysicalExpr>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let predicate_creation_errors =
            MetricBuilder::new(&metrics).global_counter("num_predicate_creation_errors");
        self.metrics = metrics;
        self.predicate = Some(Arc::clone(&predicate));

        match PruningPredicate::try_new(Arc::clone(&predicate), Arc::clone(&file_schema)) {
            Ok(pruning_predicate) => {
                if !pruning_predicate.always_true() {
                    self.pruning_predicate = Some(Arc::new(pruning_predicate));
                }
            }
            Err(e) => {
                log::debug!("Could not create pruning predicate for: {e}");
                predicate_creation_errors.add(1);
            }
        };

        // Deliberately skip page_pruning_predicate — page-index stats must not
        // prune pages that the caller's RowSelection already validated.
        self
    }

    /// Create a new LiquidParquetSource from a ParquetSource
    pub fn from_parquet_source(source: ParquetSource, liquid_cache: LiquidCacheParquetRef) -> Self {
        let predicate = source.filter();
        let reader_factory = source.parquet_file_reader_factory().cloned();

        let table_schema = source.table_schema().clone();
        let file_schema = table_schema.file_schema().clone();
        let projection = source.projection().cloned().unwrap_or_else(|| {
            let table_schema = table_schema.table_schema();
            ProjectionExprs::from_indices(
                &(0..table_schema.fields().len()).collect::<Vec<_>>(),
                table_schema,
            )
        });
        let mut v = Self {
            table_schema,
            table_parquet_options: source.table_parquet_options().clone(),
            batch_size: Some(liquid_cache.batch_size()),
            liquid_cache,
            projection,
            metrics: source.metrics().clone(),
            predicate: None,
            pruning_predicate: None,
            page_pruning_predicate: None,
            parquet_file_reader_factory: reader_factory,
            engagement_policy: default_engagement_policy(),
        };

        if let Some(predicate) = predicate {
            v = v.with_predicate(file_schema, predicate);
        }

        v
    }

    /// Set a custom cache engagement policy.
    pub fn with_engagement_policy(mut self, policy: Arc<dyn CacheEngagementPolicy>) -> Self {
        self.engagement_policy = policy;
        self
    }

    /// Get the predicate for the LiquidParquetSource
    pub fn predicate(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.predicate.clone()
    }
}

impl FileSource for LiquidParquetSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn datafusion::datasource::physical_plan::FileOpener>> {
        let expr_adapter_factory = base_config
            .expr_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultPhysicalExprAdapterFactory) as _);

        let reader_factory = Arc::new(CachedMetaReaderFactory::new(object_store));

        // If the caller provided a ParquetFileReaderFactory (with pre-loaded
        // metadata), pre-seed LC's metadata cache for all files in this partition.
        // This avoids expensive ArrowReaderMetadata::load_async() calls in the
        // opener for files whose metadata we already have.
        // Pass caller's reader factory to the opener so it can use pre-loaded
        // metadata (avoids re-fetching from object store).
        let caller_reader_factory = self.parquet_file_reader_factory.clone();

        let opener = LiquidParquetOpener::new(
            partition,
            self.projection.clone(),
            self.batch_size
                .expect("Batch size must be set before creating LiquidParquetOpener"),
            base_config.limit,
            self.predicate.clone(),
            self.table_schema.clone(),
            self.metrics.clone(),
            self.liquid_cache.clone(),
            reader_factory,
            self.reorder_filters(),
            expr_adapter_factory,
            caller_reader_factory,
            Arc::clone(&self.engagement_policy),
        );

        Ok(Arc::new(opener))
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        source.projection = self.projection.try_merge(projection)?;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "liquid_parquet"
    }
}
