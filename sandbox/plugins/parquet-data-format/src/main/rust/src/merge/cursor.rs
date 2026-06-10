/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fs::File;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use arrow_ipc::reader::FileReader as IpcFileReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::schema::types::SchemaDescriptor;

use super::error::{MergeError, MergeResult};
use super::heap::{get_sort_values, SortKey};
use super::io_task::get_merge_pool;
use super::schema::projection_indices_excluding_row_id;

/// A cursor over a single sorted input file (Parquet or IPC).
///
/// Each cursor reads batches sequentially and prefetches the next batch on the
/// shared Rayon pool to overlap IO with merge computation.
pub struct FileCursor {
    reader: Arc<Mutex<Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>> + Send>>>,
    prefetch_rx: std::sync::mpsc::Receiver<Option<MergeResult<RecordBatch>>>,
    prefetch_tx: std::sync::mpsc::SyncSender<Option<MergeResult<RecordBatch>>>,
    prefetch_pending: bool,
    pub current_batch: Option<RecordBatch>,
    pub row_idx: usize,
    pub file_id: usize,
    pub sort_col_indices: Vec<usize>,
    pub sort_col_types: Vec<ArrowDataType>,
    pub nulls_first: Vec<bool>,
}

impl FileCursor {
    /// Opens a Parquet file and creates a cursor positioned at the first row.
    ///
    /// Returns `(cursor, projected_arrow_schema, parquet_schema_descriptor, writer_generation)`
    /// so the caller can build union schemas without re-opening the file.
    pub fn new(
        path: &str,
        file_id: usize,
        sort_columns: &[String],
        nulls_first: &[bool],
        batch_size: usize,
    ) -> MergeResult<(Self, Arc<ArrowSchema>, SchemaDescriptor, i64, usize)> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let writer_generation = crate::writer_properties_builder::read_writer_generation(builder.metadata().file_metadata(), file_id);
        let total_row_count = builder.metadata().file_metadata().num_rows() as usize;

        let mut sort_col_types = Vec::with_capacity(sort_columns.len());
        for col_name in sort_columns {
            let dt = schema
                .fields()
                .iter()
                .find(|f| f.name() == col_name.as_str())
                .map(|f| f.data_type().clone())
                .ok_or_else(|| {
                    MergeError::Logic(format!(
                        "Sort column '{}' not found in file '{}' (cursor {})",
                        col_name, path, file_id
                    ))
                })?;
            sort_col_types.push(dt);
        }

        let parquet_schema_descr = builder.parquet_schema().clone();
        let projection_indices = projection_indices_excluding_row_id(&schema);

        let projection =
            parquet::arrow::ProjectionMask::roots(&parquet_schema_descr, projection_indices);

        let mut reader = builder
            .with_batch_size(batch_size)
            .with_projection(projection)
            .build()?;

        let first_batch = match reader.next() {
            Some(Ok(b)) if b.num_rows() > 0 => b,
            Some(Err(e)) => return Err(e.into()),
            _ => {
                return Err(MergeError::Logic(format!(
                    "File '{}' (cursor {}) yielded no rows despite passing validation",
                    path, file_id
                )));
            }
        };

        let projected_schema = first_batch.schema();

        let mut sort_col_indices = Vec::with_capacity(sort_columns.len());
        for col_name in sort_columns {
            let idx = projected_schema
                .fields()
                .iter()
                .position(|f| f.name() == col_name.as_str())
                .ok_or_else(|| {
                    MergeError::Logic(format!(
                        "Sort column '{}' not found after projection in file '{}'",
                        col_name, path
                    ))
                })?;
            sort_col_indices.push(idx);
        }

        let (prefetch_tx, prefetch_rx) =
            std::sync::mpsc::sync_channel::<Option<MergeResult<RecordBatch>>>(1);

        let reader: Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>> + Send> =
            Box::new(reader);
        let reader = Arc::new(Mutex::new(reader));

        let mut cursor = Self {
            reader,
            prefetch_rx,
            prefetch_tx,
            prefetch_pending: false,
            current_batch: Some(first_batch),
            row_idx: 0,
            file_id,
            sort_col_indices,
            sort_col_types,
            nulls_first: nulls_first.to_vec(),
        };

        cursor.start_prefetch();

        Ok((cursor, projected_schema, parquet_schema_descr, writer_generation, total_row_count))
    }

    /// Opens an Arrow IPC file and creates a cursor positioned at the first row.
    ///
    /// Unlike `new()`, which reads metadata from the Parquet file, this constructor
    /// accepts pre-known metadata (schema, writer_generation, total_row_count) from
    /// the caller — typically the `SortingChunkedWriter` that produced the IPC files.
    ///
    /// Returns `(cursor, projected_arrow_schema, parquet_schema_descriptor, writer_generation, total_row_count)`.
    pub fn new_from_ipc(
        path: &str,
        file_id: usize,
        sort_columns: &[String],
        nulls_first: &[bool],
        batch_size: usize,
        schema: Arc<ArrowSchema>,
        writer_generation: i64,
        total_row_count: usize,
    ) -> MergeResult<(Self, Arc<ArrowSchema>, SchemaDescriptor, i64, usize)> {
        let file = File::open(path)?;
        let ipc_reader = IpcFileReader::try_new(file, None).map_err(|e| {
            MergeError::Arrow(e)
        })?;

        let mut sort_col_types = Vec::with_capacity(sort_columns.len());
        for col_name in sort_columns {
            let dt = schema
                .fields()
                .iter()
                .find(|f| f.name() == col_name.as_str())
                .map(|f| f.data_type().clone())
                .ok_or_else(|| {
                    MergeError::Logic(format!(
                        "Sort column '{}' not found in IPC file '{}' (cursor {})",
                        col_name, path, file_id
                    ))
                })?;
            sort_col_types.push(dt);
        }

        // Build a SchemaDescriptor from the Arrow schema for MergeContext compatibility.
        // We exclude the __row_id__ column from the projected schema (same as Parquet path),
        // then build a parquet SchemaDescriptor using the same approach as the Parquet path.
        let projection_indices = projection_indices_excluding_row_id(&schema);

        // Create a projected schema (excluding __row_id__)
        let projected_fields: Vec<_> = projection_indices
            .iter()
            .map(|&i| schema.field(i).clone())
            .collect();
        let projected_arrow_schema = Arc::new(ArrowSchema::new(projected_fields));

        // Build a Parquet SchemaDescriptor from the full schema (including __row_id__).
        let converter = parquet::arrow::ArrowSchemaConverter::new();
        let parquet_schema_descr = converter.convert(&schema)?;

        // Wrap the IPC reader as a filtering iterator that projects away __row_id__
        // and respects batch_size by slicing.
        let projection_indices_clone = projection_indices.clone();
        let projected_schema_for_iter = projected_arrow_schema.clone();
        let ipc_iter = IpcProjectedIterator {
            inner: ipc_reader,
            projection_indices: projection_indices_clone,
            projected_schema: projected_schema_for_iter.clone(),
            batch_size,
            buffered: None,
            offset: 0,
        };

        // Read the first batch
        let mut boxed_iter: Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>> + Send> =
            Box::new(ipc_iter);

        let first_batch = match boxed_iter.next() {
            Some(Ok(b)) if b.num_rows() > 0 => b,
            Some(Err(e)) => return Err(e.into()),
            _ => {
                return Err(MergeError::Logic(format!(
                    "IPC file '{}' (cursor {}) yielded no rows",
                    path, file_id
                )));
            }
        };

        let projected_schema = first_batch.schema();

        let mut sort_col_indices = Vec::with_capacity(sort_columns.len());
        for col_name in sort_columns {
            let idx = projected_schema
                .fields()
                .iter()
                .position(|f| f.name() == col_name.as_str())
                .ok_or_else(|| {
                    MergeError::Logic(format!(
                        "Sort column '{}' not found after projection in IPC file '{}'",
                        col_name, path
                    ))
                })?;
            sort_col_indices.push(idx);
        }

        let (prefetch_tx, prefetch_rx) =
            std::sync::mpsc::sync_channel::<Option<MergeResult<RecordBatch>>>(1);

        let reader = Arc::new(Mutex::new(boxed_iter));

        let mut cursor = Self {
            reader,
            prefetch_rx,
            prefetch_tx,
            prefetch_pending: false,
            current_batch: Some(first_batch),
            row_idx: 0,
            file_id,
            sort_col_indices,
            sort_col_types,
            nulls_first: nulls_first.to_vec(),
        };

        cursor.start_prefetch();

        Ok((cursor, projected_schema, parquet_schema_descr, writer_generation, total_row_count))
    }

    fn start_prefetch(&mut self) {
        if self.prefetch_pending {
            return;
        }
        self.prefetch_pending = true;

        let reader = Arc::clone(&self.reader);
        let tx = self.prefetch_tx.clone();

        get_merge_pool(None).spawn(move || {
            let mut reader = reader.lock().unwrap();
            let result = match reader.next() {
                Some(Ok(batch)) if batch.num_rows() > 0 => Some(Ok(batch)),
                Some(Err(e)) => Some(Err(MergeError::Arrow(e))),
                _ => None,
            };
            let _ = tx.send(result);
        });
    }

    pub fn load_next_batch(&mut self) -> MergeResult<bool> {
        self.current_batch = None;

        match self.prefetch_rx.recv() {
            Ok(Some(Ok(batch))) => {
                self.current_batch = Some(batch);
                self.row_idx = 0;
                self.prefetch_pending = false;
                self.start_prefetch();
                Ok(true)
            }
            Ok(Some(Err(e))) => {
                self.prefetch_pending = false;
                Err(e)
            }
            Ok(None) | Err(_) => {
                self.prefetch_pending = false;
                Ok(false)
            }
        }
    }

    #[inline]
    pub fn current_sort_values(&self) -> MergeResult<Vec<SortKey>> {
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        get_sort_values(batch, self.row_idx, &self.sort_col_indices, &self.sort_col_types, &self.nulls_first)
    }

    #[inline]
    pub fn last_sort_values(&self) -> MergeResult<Vec<SortKey>> {
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        get_sort_values(
            batch,
            batch.num_rows() - 1,
            &self.sort_col_indices,
            &self.sort_col_types,
            &self.nulls_first,
        )
    }

    #[inline]
    pub fn batch_height(&self) -> usize {
        self.current_batch.as_ref().map_or(0, |b| b.num_rows())
    }

    #[inline]
    pub fn take_slice(&self, start: usize, len: usize) -> RecordBatch {
        self.current_batch.as_ref().unwrap().slice(start, len)
    }

    pub fn advance(&mut self) -> MergeResult<bool> {
        if self.current_batch.is_none() {
            return Ok(false);
        }
        self.row_idx += 1;
        if self.row_idx >= self.current_batch.as_ref().unwrap().num_rows() {
            self.current_batch = None;
            return self.load_next_batch();
        }
        Ok(true)
    }

    pub fn advance_past_batch(&mut self) -> MergeResult<bool> {
        self.current_batch = None;
        self.load_next_batch()
    }
}

/// Iterator adapter that wraps an IPC `FileReader`, projects away columns not in
/// `projection_indices`, and slices output batches to at most `batch_size` rows.
/// This ensures the IPC cursor produces batches with the same schema and size
/// characteristics as the Parquet cursor.
struct IpcProjectedIterator {
    inner: IpcFileReader<File>,
    projection_indices: Vec<usize>,
    projected_schema: Arc<ArrowSchema>,
    batch_size: usize,
    /// Leftover rows from the last IPC batch that didn't fit in batch_size.
    buffered: Option<RecordBatch>,
    /// Current offset within `buffered`.
    offset: usize,
}

// Safety: IpcFileReader<File> is Send (File is Send, the reader holds no Rc/Cell).
unsafe impl Send for IpcProjectedIterator {}

impl IpcProjectedIterator {
    /// Project a batch to only the columns in `projection_indices`.
    fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, arrow::error::ArrowError> {
        let columns: Vec<_> = self.projection_indices.iter().map(|&i| batch.column(i).clone()).collect();
        RecordBatch::try_new(self.projected_schema.clone(), columns)
    }

    /// Try to fill `buffered` from the inner IPC reader. Returns true if data available.
    fn fill_buffer(&mut self) -> Option<Result<(), arrow::error::ArrowError>> {
        loop {
            match self.inner.next() {
                Some(Ok(batch)) if batch.num_rows() > 0 => {
                    match self.project_batch(&batch) {
                        Ok(projected) => {
                            self.buffered = Some(projected);
                            self.offset = 0;
                            return Some(Ok(()));
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                Some(Ok(_)) => continue, // skip empty batches
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

impl Iterator for IpcProjectedIterator {
    type Item = Result<RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // If no buffered batch, try to read one from the IPC reader
        if self.buffered.is_none() {
            match self.fill_buffer() {
                Some(Ok(())) => {}
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }

        let batch = self.buffered.as_ref().unwrap();
        let remaining = batch.num_rows() - self.offset;
        let take = remaining.min(self.batch_size);
        let slice = batch.slice(self.offset, take);
        self.offset += take;

        if self.offset >= batch.num_rows() {
            self.buffered = None;
            self.offset = 0;
        }

        Some(Ok(slice))
    }
}
