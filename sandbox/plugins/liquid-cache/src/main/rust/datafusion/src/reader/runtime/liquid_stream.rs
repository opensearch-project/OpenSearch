use crate::cache::{CachedFileRef, CachedRowGroupRef};
use crate::reader::plantime::{LiquidRowFilter, ParquetMetadataCacheReader};
use arrow::array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use futures::Stream;
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowPredicate, RowSelection, RowSelector},
    },
    errors::ParquetError,
    file::metadata::ParquetMetaData,
};
use std::{
    collections::VecDeque,
    fmt::Formatter,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use super::liquid_cache_reader::{
    LiquidCacheReader, LiquidCacheReaderConfig, ParquetFallbackConfig,
};
use super::utils::{get_root_column_ids, limit_row_selection, offset_row_selection};

type PlanResult = Option<PlanningContext>;

struct ReaderFactory {
    metadata: Arc<ParquetMetaData>,

    input: ParquetMetadataCacheReader,

    filter: Option<LiquidRowFilter>,

    limit: Option<usize>,

    offset: Option<usize>,

    cached_file: CachedFileRef,
}

impl ReaderFactory {
    /// Plans what to read from cache vs parquet for the next row group
    fn plan_row_group(
        &mut self,
        row_group_idx: usize,
        selection: Option<RowSelection>,
        projection: ProjectionMask,
        batch_size: usize,
    ) -> PlanResult {
        let meta = self.metadata.row_group(row_group_idx);

        let mut predicate_projection: Option<ProjectionMask> = None;
        if let Some(filter) = self.filter.as_mut() {
            for predicate in filter.predicates_mut() {
                let p_projection = predicate.projection();
                if let Some(ref mut p) = predicate_projection {
                    p.union(p_projection);
                } else {
                    predicate_projection = Some(p_projection.clone());
                }
            }
        }

        let mut selection =
            selection.unwrap_or_else(|| vec![RowSelector::select(meta.num_rows() as usize)].into());

        let rows_before = selection.row_count();

        if rows_before == 0 {
            return None;
        }

        if let Some(offset) = self.offset {
            selection = offset_row_selection(selection, offset);
        }

        if let Some(limit) = self.limit {
            selection = limit_row_selection(selection, limit);
        }

        let rows_after = selection.row_count();

        // Update offset if necessary
        if let Some(offset) = &mut self.offset {
            // Reduction is either because of offset or limit, as limit is applied
            // after offset has been "exhausted" can just use saturating sub here
            *offset = offset.saturating_sub(rows_before - rows_after)
        }

        if rows_after == 0 {
            return None;
        }

        if let Some(limit) = &mut self.limit {
            *limit -= rows_after;
        }

        let mut cache_projection = projection.clone();
        if let Some(ref predicate_projection) = predicate_projection {
            cache_projection.union(predicate_projection);
        }

        let schema_descr = self.metadata.file_metadata().schema_descr();
        let cache_column_ids = get_root_column_ids(schema_descr, &cache_projection);
        // When no predicate is present, all projected columns are cacheable.
        // Without this, `is_predicate_column` is false for every column and
        // the cache is effectively disabled (get returns None, insert rejects).
        let predicate_column_ids = if let Some(ref predicate_projection) = predicate_projection {
            get_root_column_ids(schema_descr, predicate_projection)
        } else {
            cache_column_ids.clone()
        };

        if row_group_idx == 0 {
            log::debug!(
                "[LC-Stream] plan_row_group: rg={}, cache_cols={:?}, predicate_cols={:?}, \
                 has_filter={}, rows={}",
                row_group_idx,
                &cache_column_ids,
                &predicate_column_ids,
                self.filter.is_some(),
                self.metadata.row_group(row_group_idx).num_rows(),
            );
        }

        let cached_row_group = self
            .cached_file
            .create_row_group(row_group_idx as u64, predicate_column_ids.clone());

        let projection_column_ids = get_root_column_ids(schema_descr, &projection);

        let context = PlanningContext {
            row_group_idx,
            selection,
            batch_size,
            cached_row_group,
            cache_projection,
            projection_column_ids,
            cache_column_ids,
        };

        Some(context)
    }
}

fn build_projection_schema(file_schema: &SchemaRef, projection_column_ids: &[usize]) -> SchemaRef {
    let fields: Vec<_> = projection_column_ids
        .iter()
        .filter_map(|column_id| file_schema.fields().get(*column_id))
        .map(|field_ref| field_ref.as_ref().clone())
        .collect();
    Arc::new(Schema::new(fields))
}

/// Context for planning what to read from cache vs parquet
struct PlanningContext {
    row_group_idx: usize,
    selection: RowSelection,
    batch_size: usize,
    cached_row_group: CachedRowGroupRef,
    cache_projection: ProjectionMask,
    projection_column_ids: Vec<usize>,
    cache_column_ids: Vec<usize>,
}

fn build_liquid_cache_reader(
    reader_factory: &mut ReaderFactory,
    context: PlanningContext,
    schema: SchemaRef,
) -> LiquidCacheReader {
    let row_count = reader_factory
        .metadata
        .row_group(context.row_group_idx)
        .num_rows() as usize;
    let cache_batch_size = context.cached_row_group.batch_size();
    LiquidCacheReader::new(LiquidCacheReaderConfig {
        batch_size: context.batch_size,
        selection: context.selection,
        row_filter: reader_factory.filter.take(),
        cached_row_group: context.cached_row_group,
        projection_columns: context.projection_column_ids,
        schema,
        parquet_fallback: ParquetFallbackConfig {
            row_group_idx: context.row_group_idx,
            metadata: Arc::clone(&reader_factory.metadata),
            input: reader_factory.input.clone(),
            cache_projection: context.cache_projection,
            cache_column_ids: context.cache_column_ids,
            cache_batch_size,
            row_count,
        },
    })
}

enum StreamState {
    /// At the start of a new row group, or the end of the parquet stream
    Init,
    /// Decoding a batch from cache
    ReadFromCache(Box<LiquidCacheReader>),
}

impl std::fmt::Debug for StreamState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamState::Init => write!(f, "StreamState::Init"),
            StreamState::ReadFromCache(_) => write!(f, "StreamState::Decoding"),
        }
    }
}

pub struct LiquidStreamBuilder {
    pub(crate) input: ParquetMetadataCacheReader,

    pub(crate) metadata: Arc<ParquetMetaData>,

    pub(crate) batch_size: usize,

    pub(crate) row_groups: Option<Vec<usize>>,

    pub(crate) projection: ProjectionMask,

    pub(crate) filter: Option<LiquidRowFilter>,

    pub(crate) selection: Option<RowSelection>,

    pub(crate) limit: Option<usize>,

    pub(crate) offset: Option<usize>,
}

impl LiquidStreamBuilder {
    pub fn new(input: ParquetMetadataCacheReader, metadata: Arc<ParquetMetaData>) -> Self {
        Self {
            input,
            metadata,
            batch_size: 1024,
            row_groups: None,
            projection: ProjectionMask::all(),
            filter: None,
            selection: None,
            limit: None,
            offset: None,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_row_groups(mut self, row_groups: Vec<usize>) -> Self {
        self.row_groups = Some(row_groups);
        self
    }

    pub fn with_projection(mut self, projection: ProjectionMask) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_selection(mut self, selection: Option<RowSelection>) -> Self {
        self.selection = selection;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_row_filter(mut self, filter: LiquidRowFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn build(self, liquid_cache: CachedFileRef) -> Result<LiquidStream, ParquetError> {
        let num_row_groups = self.metadata.row_groups().len();

        let row_groups: VecDeque<usize> = match self.row_groups {
            Some(row_groups) => {
                if let Some(col) = row_groups.iter().find(|x| **x >= num_row_groups) {
                    return Err(ParquetError::ArrowError(format!(
                        "row group {col} out of bounds 0..{num_row_groups}"
                    )));
                }
                row_groups.into()
            }
            None => (0..self.metadata.row_groups().len()).collect(),
        };

        let batch_size = self
            .batch_size
            .min(self.metadata.file_metadata().num_rows() as usize);

        let schema_descr = self.metadata.file_metadata().schema_descr();
        let projection_column_ids = get_root_column_ids(schema_descr, &self.projection);
        let file_schema = liquid_cache.schema();
        let schema = build_projection_schema(&file_schema, &projection_column_ids);

        let reader = ReaderFactory {
            metadata: Arc::clone(&self.metadata),
            input: self.input,
            filter: self.filter,
            limit: self.limit,
            offset: self.offset,
            cached_file: liquid_cache,
        };

        Ok(LiquidStream {
            metadata: self.metadata,
            schema,
            row_groups,
            projection: self.projection,
            batch_size,
            selection: self.selection,
            reader: Some(reader),
            state: StreamState::Init,
        })
    }
}

pub struct LiquidStream {
    metadata: Arc<ParquetMetaData>,

    schema: SchemaRef,

    row_groups: VecDeque<usize>,

    projection: ProjectionMask,

    batch_size: usize,

    selection: Option<RowSelection>,

    /// This is an option so it can be moved into a future
    reader: Option<ReaderFactory>,

    state: StreamState,
}

impl std::fmt::Debug for LiquidStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetRecordBatchStream")
            .field("metadata", &self.metadata)
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .field("projection", &self.projection)
            .field("state", &self.state)
            .finish()
    }
}

impl LiquidStream {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl Stream for LiquidStream {
    type Item = Result<RecordBatch, parquet::errors::ParquetError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = std::mem::replace(&mut self.state, StreamState::Init);

            match state {
                StreamState::ReadFromCache(mut batch_reader) => {
                    match Pin::new(&mut *batch_reader).poll_next(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            self.state = StreamState::ReadFromCache(batch_reader);
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Poll::Ready(Some(Err(e))) => {
                            // Propagate decode failures as a stream error rather than
                            // panicking the executor worker.
                            return Poll::Ready(Some(Err(parquet::errors::ParquetError::General(
                                format!("liquid cache decode error: {e:?}"),
                            ))));
                        }
                        Poll::Ready(None) => {
                            let batch_reader = *batch_reader;
                            let filter = batch_reader.into_filter();
                            self.reader.as_mut().unwrap().filter = filter;
                            // state left as Init, continue loop to plan next row group
                        }
                        Poll::Pending => {
                            self.state = StreamState::ReadFromCache(batch_reader);
                            return Poll::Pending;
                        }
                    }
                }
                StreamState::Init => {
                    let row_group_idx = match self.row_groups.pop_front() {
                        Some(idx) => idx,
                        None => return Poll::Ready(None),
                    };

                    let row_count = self.metadata.row_group(row_group_idx).num_rows() as usize;

                    let selection = self.selection.as_mut().map(|s| s.split_off(row_count));

                    let projection = self.projection.clone();
                    let batch_size = self.batch_size;
                    let maybe_context = self.reader.as_mut().expect("lost reader").plan_row_group(
                        row_group_idx,
                        selection,
                        projection,
                        batch_size,
                    );
                    match maybe_context {
                        Some(context) => {
                            let schema = Arc::clone(&self.schema);
                            let reader_factory = self.reader.as_mut().unwrap();
                            let batch_reader =
                                build_liquid_cache_reader(reader_factory, context, schema);
                            self.state = StreamState::ReadFromCache(Box::new(batch_reader));
                        }
                        None => {
                            self.state = StreamState::Init;
                        }
                    }
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{BatchID, CachedFileRef, LiquidCacheParquet};
    use crate::reader::plantime::{
        CachedMetaReaderFactory, FilterCandidateBuilder, LiquidPredicate, LiquidRowFilter,
    };
    use arrow::array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use futures::StreamExt;
    use liquid_cache::cache::TranscodeEvict;
    use liquid_cache::cache_policies::LiquidPolicy;
    use object_store::local::LocalFileSystem;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use std::fs::File;
    use std::sync::Arc;

    fn write_two_row_group_file(path: &std::path::Path, schema: SchemaRef) {
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 11, 12, 13])),
            ],
        )
        .unwrap();
        let batch1 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6, 7])),
                Arc::new(Int32Array::from(vec![14, 15, 16, 17])),
            ],
        )
        .unwrap();
        writer.write(&batch0).unwrap();
        writer.flush().unwrap();
        writer.write(&batch1).unwrap();
        writer.close().unwrap();
    }

    fn make_liquid_stream(
        max_memory_bytes: usize,
        row_filter: Option<LiquidRowFilter>,
        projection_columns: Vec<usize>,
    ) -> (
        LiquidStream,
        Arc<LiquidCacheParquet>,
        CachedFileRef,
        tempfile::TempDir,
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let tmp_dir = tempfile::tempdir().unwrap();
        let parquet_path = tmp_dir.path().join("data.parquet");
        write_two_row_group_file(&parquet_path, schema.clone());
        let metadata_file = File::open(&parquet_path).unwrap();
        let reader_metadata =
            ArrowReaderMetadata::load(&metadata_file, ArrowReaderOptions::new()).unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).unwrap());
        let partitioned_file = PartitionedFile::new(
            "data.parquet",
            std::fs::metadata(&parquet_path).unwrap().len(),
        );
        let metrics = ExecutionPlanMetricsSet::new();
        let input = CachedMetaReaderFactory::new(object_store).create_liquid_reader(
            0,
            partitioned_file,
            None,
            &metrics,
        );

        let cache = Arc::new(LiquidCacheParquet::new(
            4,
            max_memory_bytes,
            Box::new(LiquidPolicy::new()),
            Box::new(TranscodeEvict),
        ));
        let cached_file = cache.register_or_get_file("data.parquet".to_string(), schema);
        let projection = ProjectionMask::roots(
            reader_metadata.metadata().file_metadata().schema_descr(),
            projection_columns,
        );
        let mut builder = LiquidStreamBuilder::new(input, Arc::clone(reader_metadata.metadata()))
            .with_batch_size(4)
            .with_row_groups(vec![0, 1])
            .with_projection(projection);
        if let Some(row_filter) = row_filter {
            builder = builder.with_row_filter(row_filter);
        }
        let stream = builder.build(cached_file.clone()).unwrap();
        (stream, cache, cached_file, tmp_dir)
    }

    /// A decode failure in the underlying parquet reader must surface as a
    /// stream `Err`, not a panic that aborts the executor worker.
    ///
    /// We build the stream over a valid file (so metadata loads cleanly), then
    /// overwrite the on-disk data-page region — leaving the footer/metadata
    /// intact — so the lazy row-group read fails when the stream is polled.
    /// Before the fix this hit `panic!("Decoding next batch error...")`; now it
    /// must yield `Some(Err(_))`. If it regressed to a panic, this `#[tokio::test]`
    /// fails on the unwind.
    #[tokio::test]
    async fn decode_error_is_propagated_not_panicked() {
        use std::io::{Seek, SeekFrom, Write};

        let (stream, _cache, _cached_file, tmp_dir) = make_liquid_stream(1 << 20, None, vec![0, 1]);

        // Corrupt the data pages right after the 4-byte "PAR1" header. Metadata
        // was already loaded above, so offsets stay consistent but page decode
        // fails.
        let parquet_path = tmp_dir.path().join("data.parquet");
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&parquet_path)
                .unwrap();
            f.seek(SeekFrom::Start(4)).unwrap();
            f.write_all(&[0xFFu8; 64]).unwrap();
            f.flush().unwrap();
        }

        let mut stream = std::pin::pin!(stream);
        let mut saw_error = false;
        while let Some(item) = stream.next().await {
            if item.is_err() {
                saw_error = true;
                break;
            }
        }
        assert!(
            saw_error,
            "corrupt parquet data pages must produce a stream Err, not a panic"
        );
    }

    async fn collect_liquid_values(stream: LiquidStream) -> (Vec<i32>, Vec<i32>) {
        let batches = stream
            .map(|batch| batch.expect("valid liquid stream batch"))
            .collect::<Vec<_>>()
            .await;
        let mut a = Vec::new();
        let mut b = Vec::new();
        for batch in batches {
            let a_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let b_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            a.extend(a_array.iter().map(|value| value.unwrap()));
            b.extend(b_array.iter().map(|value| value.unwrap()));
        }
        (a, b)
    }

    async fn collect_projected_a(stream: LiquidStream) -> Vec<i32> {
        let batches = stream
            .map(|batch| batch.expect("valid liquid stream batch"))
            .collect::<Vec<_>>()
            .await;
        let mut a = Vec::new();
        for batch in batches {
            let a_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            a.extend(a_array.iter().map(|value| value.unwrap()));
        }
        a
    }

    fn gt_filter_on(
        schema: SchemaRef,
        col_name: &str,
        col_idx: usize,
        literal: i32,
    ) -> LiquidRowFilter {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(col_name, col_idx)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(literal)))),
        ));
        let tmp_meta = tempfile::NamedTempFile::new().unwrap();
        write_two_row_group_file(tmp_meta.path(), schema.clone());
        let file = File::open(tmp_meta.path()).unwrap();
        let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).unwrap();
        let builder = FilterCandidateBuilder::new(expr, schema);
        let candidate = builder.build(metadata.metadata()).unwrap().unwrap();
        let projection = candidate.projection(metadata.metadata());
        let predicate = LiquidPredicate::try_new(candidate, projection).unwrap();
        LiquidRowFilter::new(vec![predicate])
    }

    /// Let the background cache-fill tasks (spawned via tokio::spawn) run.
    async fn drain_background_tasks() {
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
    }

    fn is_cached(row_group: &CachedRowGroupRef, column_id: usize, batch_idx: u16) -> bool {
        row_group
            .get_column(column_id as u64)
            .unwrap()
            .get_arrow_array_test_only(BatchID::from_raw(batch_idx))
            .is_some()
    }

    #[tokio::test]
    async fn reads_two_row_groups_without_filter() {
        let (stream, _cache, cached_file, _tmp_dir) =
            make_liquid_stream(usize::MAX, None, vec![0, 1]);

        let (a, b) = collect_liquid_values(stream).await;

        assert_eq!(a, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(b, vec![10, 11, 12, 13, 14, 15, 16, 17]);

        drain_background_tasks().await;
        // Without a predicate, all projected columns are cacheable.
        let row_group0 = cached_file.create_row_group(0, vec![0, 1]);
        let row_group1 = cached_file.create_row_group(1, vec![0, 1]);
        assert!(is_cached(&row_group0, 0, 0));
        assert!(is_cached(&row_group0, 1, 0));
        assert!(is_cached(&row_group1, 0, 0));
        assert!(is_cached(&row_group1, 1, 0));
    }

    #[tokio::test]
    async fn row_filter_filters_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let filter = gt_filter_on(schema, "a", 0, 2);
        let (stream, _cache, _cached_file, _tmp_dir) =
            make_liquid_stream(usize::MAX, Some(filter), vec![0, 1]);

        let (a, b) = collect_liquid_values(stream).await;

        assert_eq!(a, vec![3, 4, 5, 6, 7]);
        assert_eq!(b, vec![13, 14, 15, 16, 17]);
    }

    #[tokio::test]
    async fn predicate_fallback_uses_predicate_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let filter = gt_filter_on(schema, "b", 1, 13);
        let (stream, _cache, cached_file, _tmp_dir) =
            make_liquid_stream(usize::MAX, Some(filter), vec![0]);

        let a_values = collect_projected_a(stream).await;

        assert_eq!(a_values, vec![4, 5, 6, 7]);

        drain_background_tasks().await;
        // With a predicate on `b`, only `b` is a predicate column and cacheable.
        let row_group0 = cached_file.create_row_group(0, vec![1]);
        let row_group1 = cached_file.create_row_group(1, vec![1]);
        assert!(is_cached(&row_group0, 1, 0));
        assert!(is_cached(&row_group1, 1, 0));
    }

    #[tokio::test]
    async fn zero_memory_budget_recovers() {
        let (stream, _cache, cached_file, _tmp_dir) = make_liquid_stream(0, None, vec![0, 1]);

        let (a, b) = collect_liquid_values(stream).await;

        assert_eq!(a, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(b, vec![10, 11, 12, 13, 14, 15, 16, 17]);

        drain_background_tasks().await;
        let row_group0 = cached_file.create_row_group(0, vec![0, 1]);
        let row_group1 = cached_file.create_row_group(1, vec![0, 1]);
        assert!(!is_cached(&row_group0, 0, 0));
        assert!(!is_cached(&row_group0, 1, 0));
        assert!(!is_cached(&row_group1, 0, 0));
        assert!(!is_cached(&row_group1, 1, 0));
    }

    #[tokio::test]
    async fn pre_populated_cache_serves_reads() {
        let (stream, _cache, cached_file, _tmp_dir) =
            make_liquid_stream(usize::MAX, None, vec![0, 1]);
        let row_group0 = cached_file.create_row_group(0, vec![0, 1]);
        let row_group1 = cached_file.create_row_group(1, vec![0, 1]);
        let a0: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
        let a1: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6, 7]));
        row_group0
            .get_column(0)
            .unwrap()
            .insert(BatchID::from_raw(0), a0)
            .unwrap();
        row_group1
            .get_column(0)
            .unwrap()
            .insert(BatchID::from_raw(0), a1)
            .unwrap();

        let (a, b) = collect_liquid_values(stream).await;

        assert_eq!(a, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(b, vec![10, 11, 12, 13, 14, 15, 16, 17]);

        drain_background_tasks().await;
        // Missing column `b` was pulled from parquet and back-filled.
        assert!(is_cached(&row_group0, 1, 0));
        assert!(is_cached(&row_group1, 1, 0));
    }
}
