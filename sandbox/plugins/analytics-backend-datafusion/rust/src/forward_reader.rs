/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Forward-only, page-lazy Parquet batch reader for the doc-values read path.
//!
//! Reads a single projected column forward-only over one retained Arrow reader:
//! complete pages between the current position and the requested row are skipped
//! without fetching or decoding them, and pages the ColumnIndex marks entirely
//! null are satisfied without decoding at all.
//!
//! The reader works over either a local file (fast path) or a DataFusion
//! object-store [`AsyncFileReader`] (remote/tiered storage), selected by
//! [`ParquetForwardBatchReaderFactory`]. Repeated (multi-valued) columns are not
//! supported yet.
//!
//! The factory holds everything a reader needs to be rebuilt without new IO: the
//! `ParquetMetaData` (footer plus the page index for the projected column), the
//! `ProjectionMask` naming that column, the decode batch size, and the byte
//! source. [`ParquetForwardBatchReaderFactory::open`] is therefore also the reset
//! path. Page bounds come from the `OffsetIndex`; all-null pages are recognised
//! from the `ColumnIndex`.

use bytes::Bytes;
use datafusion::arrow::array::new_null_array;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::errors::{ParquetError as ArrowParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
use datafusion::parquet::file::reader::{ChunkReader, Length};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::PartitionedFile;
use parking_lot::Mutex;
use std::fmt::Debug;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// OffsetIndex/ColumnIndex information for one projected data page.
#[derive(Debug, Clone)]
pub struct ParquetForwardPage {
    /// First logical row of the page within the file.
    pub first_row: usize,
    /// Number of rows in the page.
    pub row_count: usize,
    /// Whether every value in the page is null, so it can be satisfied without
    /// decoding.
    pub all_null: bool,
}

/// A forward-only, page-lazy Parquet batch reader over a single projected leaf
/// column, backed by either a local file or a DataFusion [`AsyncFileReader`].
///
/// [`Self::read_batch_at`] only moves forward: complete pages between the
/// current position and the requested row are skipped without fetching or
/// decoding them. The projected column must have an OffsetIndex on every
/// non-empty row group so Arrow can request whole page ranges lazily.
pub struct ParquetForwardBatchReader {
    reader: ParquetRecordBatchReader,
    physical_position: usize, // deviates from position for null rows
    position: usize,
    row_count: usize,
    pages: Vec<ParquetForwardPage>,
}

/// Reusable factory for independent forward readers over the same file,
/// metadata, and projection.
///
/// [`Self::open`] uses a synchronous retained file descriptor when the file
/// exists locally, and otherwise builds a DataFusion object-store reader for
/// remote/tiered storage.
pub struct ParquetForwardBatchReaderFactory {
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    file: PartitionedFile,
    metadata: Arc<ParquetMetaData>, // Arc as it is shared by readers
    projection: ProjectionMask,
    batch_size: usize,
    runtime: Arc<Runtime>,
    local_file: Option<PathBuf>,
}

impl ParquetForwardBatchReaderFactory {
    /// Creates a factory whose readers use the supplied DataFusion file reader
    /// factory and cached Parquet metadata.
    pub fn new(
        reader_factory: Arc<dyn ParquetFileReaderFactory>,
        file: PartitionedFile,
        metadata: Arc<ParquetMetaData>,
        projection: ProjectionMask,
        batch_size: usize,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            reader_factory,
            file,
            metadata,
            projection,
            batch_size,
            runtime,
            local_file: None,
        }
    }

    /// Uses a synchronous retained file descriptor for `path`, which the caller resolves from the
    /// store that owns the file. `None` continues through DataFusion's reader factory.
    ///
    /// Resolving the path is the store's job, not this module's: only a store backed by the local
    /// filesystem can name a descriptor, and only it knows how an object path maps onto one.
    pub fn with_local_file(mut self, path: Option<PathBuf>) -> Self {
        self.local_file = path;
        self
    }

    /// Opens a new retained forward reader (local descriptor if available, else
    /// a DataFusion object-store reader).
    pub fn open(&self) -> ParquetResult<ParquetForwardBatchReader> {
        if let Some(path) = self.local_file.as_ref() {
            let file = std::fs::File::open(path)
                .map_err(|error| ArrowParquetError::External(Box::new(error)))?;
            return ParquetForwardBatchReader::try_new_with_chunk_reader(
                file,
                Arc::clone(&self.metadata),
                self.projection.clone(),
                self.batch_size,
            );
        }
        // create_reader requires a metrics sink; we do not surface these read
        // metrics, so pass a throwaway set.
        let metrics = ExecutionPlanMetricsSet::new();
        let async_reader = self
            .reader_factory
            .create_reader(0, self.file.clone(), None, &metrics)
            .map_err(|error| ArrowParquetError::External(Box::new(error)))?;
        ParquetForwardBatchReader::try_new(
            async_reader,
            self.file.object_meta.size,
            Arc::clone(&self.metadata),
            self.projection.clone(),
            self.batch_size,
            Arc::clone(&self.runtime),
        )
    }
}

impl Debug for ParquetForwardBatchReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetForwardBatchReader")
            .field("position", &self.position)
            .field("physical_position", &self.physical_position)
            .field("row_count", &self.row_count)
            .field("page_count", &self.pages.len())
            .finish_non_exhaustive()
    }
}

impl ParquetForwardBatchReader {
    /// Creates a retained Arrow reader over a DataFusion object-store async
    /// reader (remote/tiered storage).
    pub fn try_new(
        async_reader: Box<dyn AsyncFileReader + Send>,
        file_len: u64,
        metadata: Arc<ParquetMetaData>,
        projection: ProjectionMask,
        batch_size: usize,
        runtime: Arc<Runtime>,
    ) -> ParquetResult<Self> {
        let chunk_reader = AsyncFileChunkReader {
            reader: Mutex::new(async_reader),
            file_len,
            runtime,
        };
        Self::try_new_with_chunk_reader(chunk_reader, metadata, projection, batch_size)
    }

    /// Creates a retained Arrow reader over an existing synchronous chunk reader
    /// (a local file), reusing Parquet metadata already loaded by DataFusion.
    pub fn try_new_with_chunk_reader<T>(
        chunk_reader: T,
        metadata: Arc<ParquetMetaData>,
        projection: ProjectionMask,
        batch_size: usize,
    ) -> ParquetResult<Self>
    where
        T: ChunkReader + 'static,
    {
        let pages = projected_pages(&metadata, &projection)?;

        let row_count = usize::try_from(metadata.file_metadata().num_rows()).map_err(|_| {
            ArrowParquetError::General("Parquet row count does not fit in usize".to_string())
        })?;
        let arrow_metadata =
            ArrowReaderMetadata::try_new(Arc::clone(&metadata), ArrowReaderOptions::new())?;
        let reader =
            ParquetRecordBatchReaderBuilder::new_with_metadata(chunk_reader, arrow_metadata)
                .with_projection(projection)
                .with_batch_size(batch_size.max(1))
                .build()?;

        Ok(Self {
            reader,
            physical_position: 0,
            position: 0,
            row_count,
            pages,
        })
    }

    /// Skips to `target_row` and decodes the next Arrow batch, clamped to the end
    /// of `target_row`'s page.
    ///
    /// Returns `None` when `target_row == self.row_count()`. Backward seeks and
    /// rows beyond the end of the file return an error.
    pub fn read_batch_at(
        &mut self,
        target_row: usize,
        max_rows: usize,
    ) -> ParquetResult<Option<RecordBatch>> {
        if target_row > self.row_count {
            return Err(ArrowParquetError::General(format!(
                "row {target_row} is beyond Parquet row count {}",
                self.row_count
            )));
        }
        if target_row < self.position {
            return Err(ArrowParquetError::General(format!(
                "backward seek from {} to {target_row} is not supported",
                self.position
            )));
        }
        if target_row == self.row_count {
            return Ok(None);
        }
        if max_rows == 0 {
            return Err(ArrowParquetError::General(
                "forward batch size must be greater than zero".to_string(),
            ));
        }

        let page_containing_row = self.page_at(target_row)?.clone();
        let rows_to_read = max_rows
            .min(page_containing_row.first_row + page_containing_row.row_count - target_row);
        if page_containing_row.all_null {
            let page_end = page_containing_row.first_row + page_containing_row.row_count;
            if self.physical_position < page_end {
                let to_skip = page_end - self.physical_position;
                let skipped = self.reader.skip_rows(to_skip)?;
                if skipped != to_skip {
                    return Err(ArrowParquetError::General(format!(
                        "requested all-null page skip of {to_skip} rows but skipped {skipped}"
                    )));
                }
                self.physical_position = page_end;
            }
            let schema = self.reader.schema();
            let columns = schema
                .fields()
                .iter()
                .map(|field| new_null_array(field.data_type(), rows_to_read))
                .collect();
            let batch = RecordBatch::try_new(schema, columns)?;
            self.position = target_row + rows_to_read;
            return Ok(Some(batch));
        }

        if target_row < self.physical_position {
            return Err(ArrowParquetError::General(format!(
                "physical reader is at {} before non-null row {target_row}",
                self.physical_position
            )));
        }
        let to_skip = target_row - self.physical_position;
        let skipped = self.reader.skip_rows(to_skip)?;
        if skipped != to_skip {
            return Err(ArrowParquetError::General(format!(
                "requested skip of {to_skip} rows but skipped {skipped}"
            )));
        }

        let batch = self.reader.read_next_batch(rows_to_read)?.ok_or_else(|| {
            ArrowParquetError::General(format!("Parquet reader exhausted before row {target_row}"))
        })?;
        self.physical_position = target_row + batch.num_rows();
        self.position = self.physical_position;
        Ok(Some(batch))
    }

    /// Current physical row position of the retained Arrow reader.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Total rows in the Parquet file.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Number of physical rows in the page containing `target_row`.
    pub fn page_row_count(&self, target_row: usize) -> ParquetResult<usize> {
        Ok(self.page_at(target_row)?.row_count)
    }

    /// Number of physical rows from `target_row` through the end of its page.
    pub fn rows_remaining_in_page(&self, target_row: usize) -> ParquetResult<usize> {
        let page = self.page_at(target_row)?;
        Ok(page.first_row + page.row_count - target_row)
    }

    fn page_at(&self, target_row: usize) -> ParquetResult<&ParquetForwardPage> {
        let index = self
            .pages
            .partition_point(|page| page.first_row + page.row_count <= target_row);
        self.pages
            .get(index)
            .filter(|page| {
                target_row >= page.first_row && target_row < page.first_row + page.row_count
            })
            .ok_or_else(|| {
                ArrowParquetError::General(format!("OffsetIndex does not contain row {target_row}"))
            })
    }
}

/// Builds the per-page table for the single projected leaf column.
///
/// Requires exactly one projected leaf column with an OffsetIndex on every
/// non-empty row group. Repeated (multi-valued) columns are rejected until the
/// repeated read path is implemented.
fn projected_pages(
    metadata: &ParquetMetaData,
    projection: &ProjectionMask,
) -> ParquetResult<Vec<ParquetForwardPage>> {
    let schema = metadata.file_metadata().schema_descr();
    let projected_columns = (0..schema.num_columns())
        .filter(|&column_idx| projection.leaf_included(column_idx))
        .collect::<Vec<_>>();
    let [column_idx] = projected_columns.as_slice() else {
        return Err(ArrowParquetError::General(format!(
            "ParquetForwardBatchReader requires exactly one projected leaf column, got {}",
            projected_columns.len()
        )));
    };
    let column_idx = *column_idx;
    // TODO: add support for repeated columns
    if schema.column(column_idx).max_rep_level() > 0 {
        return Err(ArrowParquetError::General(
            "ParquetForwardBatchReader does not support repeated columns yet".to_string(),
        ));
    }

    let offset_index = metadata.offset_index().ok_or_else(|| {
        ArrowParquetError::General("ParquetForwardBatchReader requires an OffsetIndex".to_string())
    })?;
    let column_index = metadata.column_index();
    // Accumulated across the loop: `pages` is the list of page entries returned to
    // the caller; `row_group_start` tracks the file-absolute first row of each row group.
    let mut row_group_start = 0usize;
    let mut pages = vec![];

    // Walk every row group, appending one page entry per data page to `pages`.
    for (row_group_idx, row_group) in metadata.row_groups().iter().enumerate() {
        let row_group_rows = usize::try_from(row_group.num_rows()).map_err(|_| {
            ArrowParquetError::General(format!("negative row count for row group {row_group_idx}"))
        })?;
        if row_group_rows == 0 {
            continue;
        }
        let locations = &offset_index
            .get(row_group_idx)
            .and_then(|row_group| row_group.get(column_idx))
            .filter(|index| !index.page_locations.is_empty())
            .ok_or_else(|| {
                ArrowParquetError::General(format!(
                    "OffsetIndex missing for row group {row_group_idx}, column {column_idx}"
                ))
            })?
            .page_locations;

        let page_statistics = column_index
            .and_then(|index| index.get(row_group_idx))
            .and_then(|row_group| row_group.get(column_idx))
            .filter(|index| !matches!(index, ColumnIndexMetaData::NONE));

        // Append one page entry per data page in this row group.
        for (page_idx, location) in locations.iter().enumerate() {
            let start = usize::try_from(location.first_row_index).map_err(|_| {
                ArrowParquetError::General(format!(
                    "negative first row for row group {row_group_idx}, page {page_idx}"
                ))
            })?;
            let end = match locations.get(page_idx + 1) {
                Some(next) => usize::try_from(next.first_row_index).map_err(|_| {
                    ArrowParquetError::General(format!(
                        "negative first row for row group {row_group_idx}, page {}",
                        page_idx + 1
                    ))
                })?,
                None => row_group_rows,
            };
            if start >= end || end > row_group_rows {
                return Err(ArrowParquetError::General(format!(
                    "invalid OffsetIndex row range {start}..{end} for row group {row_group_idx}"
                )));
            }
            let null_count = page_statistics.and_then(|index| {
                (page_idx < index.num_pages() as usize)
                    .then(|| index.null_count(page_idx))
                    .flatten()
            });
            let all_null = page_statistics.is_some_and(|index| {
                page_idx < index.num_pages() as usize && index.is_null_page(page_idx)
            }) || null_count == Some((end - start) as i64);
            pages.push(ParquetForwardPage {
                first_row: row_group_start + start,
                row_count: end - start,
                all_null,
            });
        }
        row_group_start += row_group_rows;
    }
    Ok(pages)
}

/// Adapts DataFusion's async object-store reader to Arrow's synchronous lazy
/// page reader. Calls are serialized because `AsyncFileReader` takes `&mut
/// self`, matching the single-threaded cursor contract.
struct AsyncFileChunkReader {
    reader: Mutex<Box<dyn AsyncFileReader + Send>>,
    file_len: u64,
    runtime: Arc<Runtime>,
}

impl Length for AsyncFileChunkReader {
    fn len(&self) -> u64 {
        self.file_len
    }
}

impl ChunkReader for AsyncFileChunkReader {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        Err(ArrowParquetError::General(format!(
            "page-header scanning at byte {start} is disabled; an OffsetIndex is required"
        )))
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| ArrowParquetError::General("page range overflow".to_string()))?;
        if end > self.file_len {
            return Err(ArrowParquetError::General(format!(
                "page range {start}..{end} exceeds file length {}",
                self.file_len
            )));
        }
        self.runtime
            .block_on(self.reader.lock().get_bytes(start..end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, ArrayRef, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::metadata::{
        FileMetaData, PageIndexPolicy, ParquetMetaDataBuilder, ParquetMetaDataReader,
    };
    use datafusion::parquet::file::properties::WriterProperties;
    use std::fs::File;

    /// Writes `values` to a temp Parquet file (row groups of 8 rows, pages of 3
    /// rows, OffsetIndex + ColumnIndex enabled) and returns it with its metadata.
    fn write_fixture(values: Vec<Option<i32>>) -> (File, Arc<ParquetMetaData>) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let column: ArrayRef = Arc::new(Int32Array::from(values));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        let file = tempfile::tempfile().unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(8))
            .set_data_page_row_count_limit(3)
            // Page-row limits are only checked at write-batch boundaries, so make
            // the batch small enough to actually split each row group into pages.
            .set_write_batch_size(3)
            .set_offset_index_disabled(false)
            .build();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)
            .unwrap();
        (File::from(file), Arc::new(metadata))
    }

    /// Opens a forward reader over the single column of a fixture file.
    fn reader_for(values: Vec<Option<i32>>) -> ParquetForwardBatchReader {
        let (file, metadata) = write_fixture(values);
        let projection = ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [0]);
        ParquetForwardBatchReader::try_new_with_chunk_reader(file, metadata, projection, 4096)
            .unwrap()
    }

    fn dense_reader() -> ParquetForwardBatchReader {
        reader_for((0..20).map(Some).collect())
    }

    fn ints(batch: &RecordBatch) -> Vec<Option<i32>> {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..column.len())
            .map(|i| column.is_valid(i).then(|| column.value(i)))
            .collect()
    }

    #[test]
    fn read_batch_at_clamps_to_page_end() {
        // Pages are 3 rows wide; asking for 10 rows from row 2 yields just row 2.
        let batch = dense_reader().read_batch_at(2, 10).unwrap().unwrap();
        assert_eq!(ints(&batch), vec![Some(2)]);
    }

    #[test]
    fn forward_jump_skips_intervening_pages() {
        let mut reader = dense_reader();
        // Jump straight to row 6, skipping pages [0,3) and [3,6) without decoding.
        let batch = reader.read_batch_at(6, 2).unwrap().unwrap();
        assert_eq!(ints(&batch), vec![Some(6), Some(7)]);
        assert_eq!(reader.position(), 8);
    }

    #[test]
    fn backward_seek_is_rejected() {
        let mut reader = dense_reader();
        reader.read_batch_at(6, 2).unwrap();
        assert!(reader.read_batch_at(1, 1).is_err());
    }

    #[test]
    fn target_row_at_row_count_returns_none() {
        let mut reader = dense_reader();
        assert!(reader.read_batch_at(20, 1).unwrap().is_none());
    }

    #[test]
    fn a_short_skip_is_rejected_rather_than_decoding_misaligned_rows() {
        // A footer that claims more rows than the pages hold: 8 rows are written, then the row group
        // and file counts are inflated to 12, stretching the last page to [6, 12). Skipping to row 9
        // then runs out of data. The skip must fail rather than leave `physical_position` out of step
        // with the reader, which would decode every later row from the wrong offset.
        let (file, metadata) = write_fixture((0..8).map(Some).collect());
        let inflated_rows = 12i64;
        let file_meta = metadata.file_metadata();
        let inflated_file_meta = FileMetaData::new(
            file_meta.version(),
            inflated_rows,
            file_meta.created_by().map(str::to_string),
            file_meta.key_value_metadata().cloned(),
            file_meta.schema_descr_ptr(),
            file_meta.column_orders().cloned(),
        );
        let inflated_row_group = metadata
            .row_group(0)
            .clone()
            .into_builder()
            .set_num_rows(inflated_rows)
            .build()
            .unwrap();
        let inflated = Arc::new(
            ParquetMetaDataBuilder::new(inflated_file_meta)
                .set_row_groups(vec![inflated_row_group])
                .set_column_index(metadata.column_index().cloned())
                .set_offset_index(metadata.offset_index().cloned())
                .build(),
        );

        let projection = ProjectionMask::leaves(inflated.file_metadata().schema_descr(), [0]);
        let mut reader =
            ParquetForwardBatchReader::try_new_with_chunk_reader(file, inflated, projection, 4096)
                .unwrap();

        let error = reader.read_batch_at(9, 1).unwrap_err().to_string();
        assert!(
            error.contains("requested skip of 9 rows but skipped 8"),
            "{error}"
        );
    }

    #[test]
    fn an_all_null_final_page_skips_to_end_of_file_without_a_short_skip() {
        // Row groups of 8 and pages of 3 leave the last row in its own page, [19, 20). Nulling it
        // makes that final page all-null, so serving it skips the physical reader to exactly the end
        // of the file - the boundary where a short skip would be reported if one were possible.
        let mut values: Vec<Option<i32>> = (0..20).map(Some).collect();
        values[19] = None;
        let mut reader = reader_for(values);
        let batch = reader.read_batch_at(19, 1).unwrap().unwrap();
        assert_eq!(ints(&batch), vec![None]);
        assert!(
            reader.read_batch_at(20, 1).unwrap().is_none(),
            "the file must be exhausted after its final page"
        );
    }

    #[test]
    fn all_null_page_is_served_without_decoding() {
        // Rows 3,4,5 form an entirely-null page; reading it returns nulls and the
        // physical reader skips past it.
        let mut values: Vec<Option<i32>> = (0..20).map(Some).collect();
        for slot in values.iter_mut().take(6).skip(3) {
            *slot = None;
        }
        let mut reader = reader_for(values);
        let batch = reader.read_batch_at(3, 3).unwrap().unwrap();
        assert_eq!(ints(&batch), vec![None, None, None]);
        // Next page decodes normally.
        let next = reader.read_batch_at(6, 2).unwrap().unwrap();
        assert_eq!(ints(&next), vec![Some(6), Some(7)]);
    }
}
