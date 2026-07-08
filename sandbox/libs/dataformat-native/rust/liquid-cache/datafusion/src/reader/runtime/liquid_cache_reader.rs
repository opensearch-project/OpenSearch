use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow::buffer::BooleanBuffer;
use arrow::compute::prep_null_mask_filter;
use arrow::record_batch::RecordBatchOptions;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use futures::{Stream, StreamExt, future::BoxFuture, stream::BoxStream};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;

use crate::cache::{BatchID, CachedRowGroupRef};
use crate::reader::plantime::{LiquidRowFilter, ParquetMetadataCacheReader};
use crate::reader::runtime::utils::take_next_batch;
use crate::utils::{boolean_buffer_and_then, row_selector_to_boolean_buffer};

pub(crate) struct LiquidCacheReader {
    state: ReaderState,
    row_filter: Option<LiquidRowFilter>,
}

enum ReaderState {
    Ready(Box<LiquidCacheReaderInner>),
    Processing(
        BoxFuture<
            'static,
            (
                LiquidCacheReaderInner,
                Option<LiquidRowFilter>,
                ProcessResult,
            ),
        >,
    ),
    Finished,
}

enum ProcessResult {
    Emit(Result<RecordBatch, ArrowError>),
    Skip,
}

struct LiquidCacheReaderInner {
    cached_row_group: CachedRowGroupRef,
    current_batch_id: BatchID,
    selection: VecDeque<RowSelector>,
    schema: SchemaRef,
    batch_size: usize,
    projection_columns: Vec<usize>,
    parquet_fallback: ParquetFallback,
    last_pull: Option<(BatchID, RecordBatch)>,
}

pub(crate) struct LiquidCacheReaderConfig {
    pub(crate) batch_size: usize,
    pub(crate) selection: RowSelection,
    pub(crate) row_filter: Option<LiquidRowFilter>,
    pub(crate) cached_row_group: CachedRowGroupRef,
    pub(crate) projection_columns: Vec<usize>,
    pub(crate) schema: SchemaRef,
    pub(crate) parquet_fallback: ParquetFallbackConfig,
}

#[derive(Clone)]
pub(crate) struct ParquetFallbackConfig {
    pub(crate) row_group_idx: usize,
    pub(crate) metadata: Arc<ParquetMetaData>,
    pub(crate) input: ParquetMetadataCacheReader,
    pub(crate) cache_projection: ProjectionMask,
    pub(crate) cache_column_ids: Vec<usize>,
    pub(crate) cache_batch_size: usize,
    pub(crate) row_count: usize,
}

struct ParquetFallback {
    row_group_idx: usize,
    metadata: Arc<ParquetMetaData>,
    input: ParquetMetadataCacheReader,
    cache_projection: ProjectionMask,
    cache_column_ids: Vec<usize>,
    cache_batch_size: usize,
    row_count: usize,
    stream: Option<BoxStream<'static, Result<RecordBatch, ParquetError>>>,
    next_batch_id: BatchID,
}

impl LiquidCacheReader {
    pub(crate) fn new(config: LiquidCacheReaderConfig) -> Self {
        let inner = LiquidCacheReaderInner::new(
            config.batch_size,
            config.selection,
            config.cached_row_group,
            config.projection_columns,
            Arc::clone(&config.schema),
            ParquetFallback::new(config.parquet_fallback),
        );
        Self {
            state: ReaderState::Ready(Box::new(inner)),
            row_filter: config.row_filter,
        }
    }

    pub(crate) fn into_filter(self) -> Option<LiquidRowFilter> {
        debug_assert!(
            matches!(self.state, ReaderState::Finished),
            "cannot extract filter before reader completes"
        );
        self.row_filter
    }
}

impl Stream for LiquidCacheReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = std::mem::replace(&mut self.state, ReaderState::Finished);

            match state {
                ReaderState::Processing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = ReaderState::Processing(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready((inner, row_filter, result)) => {
                        self.row_filter = row_filter;
                        self.state = ReaderState::Ready(Box::new(inner));
                        match result {
                            ProcessResult::Emit(item) => return Poll::Ready(Some(item)),
                            ProcessResult::Skip => continue,
                        }
                    }
                },
                ReaderState::Ready(mut inner) => {
                    match take_next_batch(&mut inner.selection, inner.batch_size) {
                        Some(selection) => {
                            let inner = *inner;
                            let future = inner.next_batch(self.row_filter.take(), selection);
                            self.state = ReaderState::Processing(future);
                            continue;
                        }
                        None => {
                            self.state = ReaderState::Finished;
                            return Poll::Ready(None);
                        }
                    }
                }
                ReaderState::Finished => {
                    self.state = ReaderState::Finished;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl ParquetFallback {
    fn new(config: ParquetFallbackConfig) -> Self {
        Self {
            row_group_idx: config.row_group_idx,
            metadata: config.metadata,
            input: config.input,
            cache_projection: config.cache_projection,
            cache_column_ids: config.cache_column_ids,
            cache_batch_size: config.cache_batch_size,
            row_count: config.row_count,
            stream: None,
            next_batch_id: BatchID::from_raw(0),
        }
    }

    async fn fetch_batch(&mut self, batch_id: BatchID) -> Result<RecordBatch, ParquetError> {
        if self.stream.is_none() || batch_id != self.next_batch_id {
            self.rebuild_stream(batch_id)?;
        }

        let stream = self.stream.as_mut().expect("fallback stream is present");
        let record_batch = stream.next().await.transpose()?.ok_or_else(|| {
            ParquetError::General(format!(
                "parquet fallback ended before batch {}",
                *batch_id as usize
            ))
        })?;

        self.next_batch_id = batch_id;
        self.next_batch_id.inc();
        Ok(record_batch)
    }

    fn rebuild_stream(&mut self, batch_id: BatchID) -> Result<(), ParquetError> {
        let reader_metadata =
            ArrowReaderMetadata::try_new(Arc::clone(&self.metadata), ArrowReaderOptions::new())?;
        let row_selection =
            build_row_selection_from(batch_id, self.cache_batch_size, self.row_count);

        let stream =
            ParquetRecordBatchStreamBuilder::new_with_metadata(self.input.clone(), reader_metadata)
                .with_projection(self.cache_projection.clone())
                .with_row_groups(vec![self.row_group_idx])
                .with_batch_size(self.cache_batch_size)
                .with_row_selection(row_selection)
                .build()?
                .boxed();

        self.stream = Some(stream);
        self.next_batch_id = batch_id;
        Ok(())
    }
}

fn build_row_selection_from(
    batch_id: BatchID,
    batch_size: usize,
    row_count: usize,
) -> RowSelection {
    let start = usize::from(*batch_id) * batch_size;
    let mut selectors = Vec::new();

    if start > 0 {
        selectors.push(RowSelector::skip(start.min(row_count)));
    }

    if start >= row_count {
        return RowSelection::from(selectors);
    }

    let mut remaining = row_count - start;
    while remaining > 0 {
        let selected = remaining.min(batch_size);
        selectors.push(RowSelector::select(selected));
        remaining -= selected;
    }

    RowSelection::from(selectors)
}

impl LiquidCacheReaderInner {
    fn new(
        batch_size: usize,
        selection: RowSelection,
        cached_row_group: CachedRowGroupRef,
        projection_columns: Vec<usize>,
        schema: SchemaRef,
        parquet_fallback: ParquetFallback,
    ) -> Self {
        Self {
            cached_row_group,
            current_batch_id: BatchID::from_raw(0),
            selection: selection.into(),
            schema,
            batch_size,
            projection_columns,
            parquet_fallback,
            last_pull: None,
        }
    }

    fn next_batch(
        self,
        row_filter: Option<LiquidRowFilter>,
        selection: Vec<RowSelector>,
    ) -> BoxFuture<'static, (Self, Option<LiquidRowFilter>, ProcessResult)> {
        Box::pin(async move {
            let mut inner = self;
            let mut row_filter = row_filter;
            inner.last_pull = None;

            let result = match inner
                .build_predicate_filter(&mut row_filter, selection)
                .await
            {
                Ok(selection) => match inner.read_from_cache(&selection).await {
                    // read_from_cache is async because a cache miss falls back
                    // to reading the batch from parquet.
                    Ok(Some(batch)) => {
                        inner.current_batch_id.inc();
                        ProcessResult::Emit(Ok(batch))
                    }
                    Ok(None) => {
                        inner.current_batch_id.inc();
                        ProcessResult::Skip
                    }
                    Err(e) => ProcessResult::Emit(Err(e)),
                },
                Err(e) => ProcessResult::Emit(Err(e)),
            };

            (inner, row_filter, result)
        })
    }

    async fn build_predicate_filter(
        &mut self,
        row_filter: &mut Option<LiquidRowFilter>,
        selection: Vec<RowSelector>,
    ) -> Result<BooleanBuffer, ArrowError> {
        let mut input_selection = row_selector_to_boolean_buffer(&selection);

        let Some(filter) = row_filter.as_mut() else {
            return Ok(input_selection);
        };

        for predicate in filter.predicates_mut() {
            if input_selection.count_set_bits() == 0 {
                break;
            }

            let boolean_array = match self.cached_row_group.evaluate_selection_with_predicate(
                self.current_batch_id,
                &input_selection,
                predicate,
            ) {
                Some(result) => result?,
                None => {
                    self.evaluate_predicate_after_materialize(&input_selection, predicate)
                        .await?
                }
            };

            let boolean_mask = if boolean_array.null_count() == 0 {
                boolean_array.into_parts().0
            } else {
                prep_null_mask_filter(&boolean_array).into_parts().0
            };

            input_selection = boolean_buffer_and_then(&input_selection, &boolean_mask);
        }

        Ok(input_selection)
    }

    async fn read_from_cache(
        &mut self,
        selection: &BooleanBuffer,
    ) -> Result<Option<RecordBatch>, ArrowError> {
        let selected_rows = selection.count_set_bits();
        if selected_rows == 0 {
            return Ok(None);
        }

        if self.projection_columns.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(selected_rows));
            let batch =
                RecordBatch::try_new_with_options(self.schema.clone(), Vec::new(), &options)
                    .unwrap();
            return Ok(Some(batch));
        }

        // Phase 1: Try cache for all columns, collect hits and track misses.
        let mut arrays: Vec<Option<ArrayRef>> = vec![None; self.projection_columns.len()];
        let mut has_miss = false;
        let mut hit_count = 0usize;
        let mut miss_count = 0usize;

        for (i, &column_idx) in self.projection_columns.iter().enumerate() {
            let column = self
                .cached_row_group
                .get_column(column_idx as u64)
                .ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "column {column_idx} not present in liquid cache"
                    ))
                })?;

            let array = column.get_arrow_array_with_filter(self.current_batch_id, selection);

            if let Some(arr) = array {
                arrays[i] = Some(arr);
                hit_count += 1;
            } else {
                has_miss = true;
                miss_count += 1;
            }
        }

        if *self.current_batch_id == 0 {
            log::debug!(
                "[LC-Reader] batch_id={}, selected_rows={}, cols={}, hits={}, misses={}, fallback={}",
                *self.current_batch_id,
                selected_rows,
                self.projection_columns.len(),
                hit_count,
                miss_count,
                has_miss,
            );
        }

        // Phase 2: If any columns missed, read from parquet ONCE for all misses.
        if has_miss {
            let record_batch = self
                .read_parquet_batch_and_fill_cache(self.current_batch_id)
                .await?;

            for (i, &column_idx) in self.projection_columns.iter().enumerate() {
                if arrays[i].is_none() {
                    let array = self.parquet_array(&record_batch, column_idx)?;
                    arrays[i] = Some(filter_array(array, selection)?);
                }
            }
        }

        let final_arrays: Vec<ArrayRef> = arrays.into_iter().map(|a| a.unwrap()).collect();
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), final_arrays).unwrap(),
        ))
    }

    async fn read_parquet_batch_and_fill_cache(
        &mut self,
        batch_id: BatchID,
    ) -> Result<RecordBatch, ArrowError> {
        if let Some((pulled_batch_id, record_batch)) = &self.last_pull
            && *pulled_batch_id == batch_id
        {
            return Ok(record_batch.clone());
        }

        let record_batch = self
            .parquet_fallback
            .fetch_batch(batch_id)
            .await
            .map_err(|e| ArrowError::ComputeError(format!("parquet fallback read failed: {e}")))?;

        // Spawn cache fill asynchronously — transcoding Arrow→Liquid should not
        // block the query hot path. The batch is returned immediately; cache
        // population happens in the background.
        let cached_row_group = self.cached_row_group.clone();
        let cache_column_ids = self.parquet_fallback.cache_column_ids.clone();
        let batch_for_cache = record_batch.clone();
        tokio::spawn(async move {
            for (col_idx, file_column_id) in cache_column_ids.iter().copied().enumerate() {
                let Some(column) = cached_row_group.get_column(file_column_id as u64) else {
                    continue;
                };
                let array = Arc::clone(batch_for_cache.column(col_idx));
                let _ = column.insert(batch_id, array);
            }
        });

        self.last_pull = Some((batch_id, record_batch.clone()));
        Ok(record_batch)
    }

    async fn evaluate_predicate_after_materialize(
        &mut self,
        selection: &BooleanBuffer,
        predicate: &mut crate::reader::LiquidPredicate,
    ) -> Result<BooleanArray, ArrowError> {
        let record_batch = self
            .read_parquet_batch_and_fill_cache(self.current_batch_id)
            .await?;

        if let Some(result) = self.cached_row_group.evaluate_selection_with_predicate(
            self.current_batch_id,
            selection,
            predicate,
        ) {
            return result;
        }

        let column_ids = predicate.predicate_column_ids();
        let mut arrays = Vec::with_capacity(column_ids.len());
        let mut fields = Vec::with_capacity(column_ids.len());

        for column_id in column_ids {
            let array = self.parquet_array(&record_batch, column_id)?;
            arrays.push(filter_array(array, selection)?);

            let field = self
                .cached_row_group
                .get_column(column_id as u64)
                .ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "column {column_id} not present in liquid cache"
                    ))
                })?
                .field()
                .as_ref()
                .clone();
            fields.push(field);
        }

        let schema = Arc::new(Schema::new(fields));
        let predicate_batch = if arrays.is_empty() {
            let options =
                RecordBatchOptions::new().with_row_count(Some(selection.count_set_bits()));
            RecordBatch::try_new_with_options(schema, arrays, &options)?
        } else {
            RecordBatch::try_new(schema, arrays)?
        };

        predicate.evaluate(predicate_batch)
    }

    fn parquet_array(
        &self,
        record_batch: &RecordBatch,
        file_column_id: usize,
    ) -> Result<ArrayRef, ArrowError> {
        let position = self
            .parquet_fallback
            .cache_column_ids
            .iter()
            .position(|column_id| *column_id == file_column_id)
            .ok_or_else(|| {
                ArrowError::ComputeError(format!(
                    "column {file_column_id} not present in parquet fallback projection"
                ))
            })?;

        Ok(Arc::clone(record_batch.column(position)))
    }
}

fn filter_array(array: ArrayRef, selection: &BooleanBuffer) -> Result<ArrayRef, ArrowError> {
    let selection_array = BooleanArray::new(selection.clone(), None);
    arrow::compute::filter(array.as_ref(), &selection_array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cache::LiquidCacheParquet,
        reader::plantime::CachedMetaReaderFactory,
        reader::{FilterCandidateBuilder, LiquidPredicate, LiquidRowFilter},
    };
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::{
        logical_expr::Operator,
        physical_expr::PhysicalExpr,
        physical_expr::expressions::{BinaryExpr, Column, Literal},
        scalar::ScalarValue,
    };
    use futures::{StreamExt, pin_mut};
    use liquid_cache::cache::TranscodeEvict;
    use liquid_cache::cache_policies::LiquidPolicy;
    use object_store::local::LocalFileSystem;
    use parquet::arrow::{
        ArrowWriter, ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector},
    };
    use std::fs::File;
    use std::sync::Arc;

    struct TestRowGroup {
        batch_size: usize,
        row_group: CachedRowGroupRef,
        schema: SchemaRef,
        fallback: ParquetFallbackConfig,
        _tmp_dir: tempfile::TempDir,
    }

    struct ReaderRequest {
        selection: RowSelection,
        row_filter: Option<LiquidRowFilter>,
        projection_columns: Vec<usize>,
        schema: SchemaRef,
    }

    impl TestRowGroup {
        fn reader(&self, request: ReaderRequest) -> LiquidCacheReader {
            LiquidCacheReader::new(LiquidCacheReaderConfig {
                batch_size: self.batch_size,
                selection: request.selection,
                row_filter: request.row_filter,
                cached_row_group: Arc::clone(&self.row_group),
                projection_columns: request.projection_columns,
                schema: request.schema,
                parquet_fallback: self.fallback.clone(),
            })
        }
    }

    async fn make_row_group(batch_size: usize, batches: &[Vec<i32>]) -> TestRowGroup {
        let tmp_dir = tempfile::tempdir().unwrap();
        let field = Arc::new(Field::new("col0", DataType::Int32, false));
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let parquet_path = tmp_dir.path().join("data.parquet");
        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        for values in batches {
            let array: ArrayRef = Arc::new(Int32Array::from(values.clone()));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();

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
        let projection = ProjectionMask::roots(
            reader_metadata.metadata().file_metadata().schema_descr(),
            [0],
        );

        let cache = LiquidCacheParquet::new(
            batch_size,
            usize::MAX,
            Box::new(LiquidPolicy::new()),
            Box::new(TranscodeEvict),
        );
        let file = cache.register_or_get_file("test".to_string(), schema.clone());
        // Mark col0 as a predicate column so it is cacheable in tests.
        let row_group = file.create_row_group(0, vec![0]);
        let column = row_group.get_column(0).unwrap();

        for (idx, values) in batches.iter().enumerate() {
            let array: ArrayRef = Arc::new(Int32Array::from(values.clone()));
            column
                .insert(BatchID::from_raw(idx as u16), array)
                .expect("cache insert");
        }

        TestRowGroup {
            batch_size,
            row_group,
            schema,
            fallback: ParquetFallbackConfig {
                row_group_idx: 0,
                metadata: Arc::clone(reader_metadata.metadata()),
                input,
                cache_projection: projection,
                cache_column_ids: vec![0],
                cache_batch_size: batch_size,
                row_count: flatten_batches(batches).len(),
            },
            _tmp_dir: tmp_dir,
        }
    }

    fn flatten_batches(batches: &[Vec<i32>]) -> Vec<i32> {
        batches.iter().flat_map(|b| b.iter().copied()).collect()
    }

    fn collect_batches(reader: LiquidCacheReader) -> Vec<RecordBatch> {
        futures::executor::block_on(
            reader
                .map(|batch| batch.expect("valid record batch"))
                .collect::<Vec<_>>(),
        )
    }

    fn as_i32_values(batch: &RecordBatch) -> Vec<i32> {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int column");
        array.iter().map(|v| v.expect("non-null")).collect()
    }

    fn build_filter(
        schema: SchemaRef,
        values: &[i32],
        expr: Arc<dyn PhysicalExpr>,
    ) -> LiquidRowFilter {
        let tmp_meta = tempfile::NamedTempFile::new().unwrap();
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array.clone()]).unwrap();
        let mut writer =
            ArrowWriter::try_new(tmp_meta.reopen().unwrap(), Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file = std::fs::File::open(tmp_meta.path()).unwrap();
        let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).unwrap();

        let builder = FilterCandidateBuilder::new(expr, Arc::clone(&schema));
        let candidate = builder.build(metadata.metadata()).unwrap().unwrap();
        let projection = candidate.projection(metadata.metadata());
        let predicate = LiquidPredicate::try_new(candidate, projection).unwrap();

        LiquidRowFilter::new(vec![predicate])
    }

    fn make_gt_filter(schema: SchemaRef, values: &[i32], literal: i32) -> LiquidRowFilter {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("col0", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(literal)))),
        ));
        build_filter(schema, values, expr)
    }

    #[tokio::test]
    async fn reads_batches_in_order() {
        let batch_size = 2;
        let test = make_row_group(batch_size, &[vec![1, 2], vec![3, 4]]).await;
        let selection = RowSelection::from(vec![RowSelector::select(4)]);

        let reader = test.reader(ReaderRequest {
            selection,
            row_filter: None,
            projection_columns: vec![0],
            schema: Arc::clone(&test.schema),
        });

        let batches = collect_batches(reader);
        assert_eq!(batches.len(), 2);
        assert_eq!(as_i32_values(&batches[0]), vec![1, 2]);
        assert_eq!(as_i32_values(&batches[1]), vec![3, 4]);
    }

    #[tokio::test]
    async fn skips_unselected_batches() {
        let batch_size = 2;
        let test = make_row_group(batch_size, &[vec![1, 2], vec![3, 4]]).await;
        let selection = RowSelection::from(vec![RowSelector::skip(2), RowSelector::select(2)]);

        let reader = test.reader(ReaderRequest {
            selection,
            row_filter: None,
            projection_columns: vec![0],
            schema: Arc::clone(&test.schema),
        });

        let batches = collect_batches(reader);
        assert_eq!(batches.len(), 1);
        assert_eq!(as_i32_values(&batches[0]), vec![3, 4]);
    }

    #[tokio::test]
    async fn empty_projection_emits_schema_only_batches() {
        let batch_size = 2;
        let test = make_row_group(batch_size, &[vec![10, 11]]).await;
        let selection = RowSelection::from(vec![RowSelector::select(2)]);

        let reader = test.reader(ReaderRequest {
            selection,
            row_filter: None,
            projection_columns: Vec::new(),
            schema: Arc::new(Schema::new(Vec::<Field>::new())),
        });

        let batches = collect_batches(reader);
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn into_filter_returns_stored_filter_after_completion() {
        let batch_size = 2;
        let test = make_row_group(batch_size, &[vec![1, 2]]).await;
        let selection = RowSelection::from(Vec::<RowSelector>::new());
        let filter = LiquidRowFilter::new(Vec::new());

        let mut reader = test.reader(ReaderRequest {
            selection,
            row_filter: Some(filter),
            projection_columns: vec![0],
            schema: Arc::clone(&test.schema),
        });

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            Pin::new(&mut reader).poll_next(&mut cx),
            Poll::Ready(None)
        ));

        assert!(reader.into_filter().is_some());
    }

    #[tokio::test]
    async fn predicate_filters_rows_across_batches() {
        let batches = vec![vec![1, 2], vec![3, 4]];
        let batch_size = 2;
        let all_values = flatten_batches(&batches);
        let test = make_row_group(batch_size, &batches).await;
        let filter = make_gt_filter(Arc::clone(&test.schema), &all_values, 2);
        let selection = RowSelection::from(vec![RowSelector::select(4)]);

        let reader = test.reader(ReaderRequest {
            selection,
            row_filter: Some(filter),
            projection_columns: vec![0],
            schema: Arc::clone(&test.schema),
        });

        let batches = collect_batches(reader);
        assert_eq!(batches.len(), 1);
        assert_eq!(as_i32_values(&batches[0]), vec![3, 4]);
    }

    fn make_or_filter(
        schema: SchemaRef,
        values: &[i32],
        gt_literal: i32,
        lt_literal: i32,
    ) -> LiquidRowFilter {
        let cond1: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("col0", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(gt_literal)))),
        ));
        let cond2: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("col0", 0)),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(lt_literal)))),
        ));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(cond1, Operator::Or, cond2));
        build_filter(schema, values, expr)
    }

    #[tokio::test]
    async fn predicate_filters_or_rows() {
        let batches = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
        let batch_size = 2;
        let all_values = flatten_batches(&batches);
        let test = make_row_group(batch_size, &batches).await;
        let filter = make_or_filter(Arc::clone(&test.schema), &all_values, 4, 2);
        let selection = RowSelection::from(vec![RowSelector::select(6)]);

        let reader = test.reader(ReaderRequest {
            selection,
            row_filter: Some(filter),
            projection_columns: vec![0],
            schema: Arc::clone(&test.schema),
        });

        let batches = collect_batches(reader);
        assert_eq!(batches.len(), 2);
        assert_eq!(as_i32_values(&batches[0]), vec![1]);
        assert_eq!(as_i32_values(&batches[1]), vec![5, 6]);
    }

    #[tokio::test]
    async fn predicate_combines_with_selection() {
        let batches = vec![vec![1, 2, 3, 4]];
        let batch_size = 4;
        let all_values = flatten_batches(&batches);
        let test = make_row_group(batch_size, &batches).await;
        let filter = make_gt_filter(Arc::clone(&test.schema), &all_values, 2);
        let selection = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(2),
            RowSelector::skip(1),
        ]);

        let reader = test.reader(ReaderRequest {
            selection,
            row_filter: Some(filter),
            projection_columns: vec![0],
            schema: Arc::clone(&test.schema),
        });

        let mut batches = collect_batches(reader);
        assert_eq!(batches.len(), 1);
        assert_eq!(as_i32_values(&batches.pop().unwrap()), vec![3]);
    }

    #[tokio::test]
    async fn predicate_can_filter_all_rows() {
        let batches = vec![vec![1, 2]];
        let batch_size = 2;
        let all_values = flatten_batches(&batches);
        let test = make_row_group(batch_size, &batches).await;
        let filter = make_gt_filter(Arc::clone(&test.schema), &all_values, 10);
        let selection = RowSelection::from(vec![RowSelector::select(2)]);

        let reader = test.reader(ReaderRequest {
            selection,
            row_filter: Some(filter),
            projection_columns: vec![0],
            schema: Arc::clone(&test.schema),
        });

        let next_batch = futures::executor::block_on(async {
            pin_mut!(reader);
            reader.next().await
        });

        assert!(next_batch.is_none());
    }
}
