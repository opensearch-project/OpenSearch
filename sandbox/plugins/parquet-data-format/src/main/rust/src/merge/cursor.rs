/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use arrow_ipc::reader::{FileReader as IpcFileReader, StreamReader as IpcStreamReader};
use lz4_flex::frame::FrameDecoder;
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
    /// Returns `(cursor, projected_arrow_schema, parquet_schema_descriptor, writer_generation, total_row_count)`
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
    /// Uses a dedicated reader thread that drains the IPC file and sends projected,
    /// batch_size-sliced batches through a bounded channel. This keeps memory bounded
    /// to at most 2 × batch_size rows per cursor (one in channel + one active) without
    /// requiring `unsafe impl Send` on `IpcFileReader`.
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
        let mut sort_col_types = Vec::with_capacity(sort_columns.len());
        for col_name in sort_columns {
            let dt = schema
                .fields()
                .iter()
                .find(|f| f.name() == col_name.as_str())
                .map(|f| f.data_type().clone())
                .ok_or_else(|| {
                    MergeError::Logic(format!(
                        "Sort column '{}' not found in IPC schema (cursor {})",
                        col_name, file_id
                    ))
                })?;
            sort_col_types.push(dt);
        }

        let projection_indices = projection_indices_excluding_row_id(&schema);

        let projected_fields: Vec<_> = projection_indices
            .iter()
            .map(|&i| schema.field(i).clone())
            .collect();
        let projected_arrow_schema = Arc::new(ArrowSchema::new(projected_fields));

        let converter = parquet::arrow::ArrowSchemaConverter::new();
        let parquet_schema_descr = converter.convert(&schema)?;

        // Spawn a reader thread that opens the IPC file, projects + slices batches,
        // and sends them through a bounded channel. The IpcFileReader never crosses
        // thread boundaries — it lives entirely on this spawned thread.
        let (batch_tx, batch_rx) = std::sync::mpsc::sync_channel::<Option<Result<RecordBatch, arrow::error::ArrowError>>>(1);
        let path_owned = path.to_string();
        let proj_indices = projection_indices.clone();
        let proj_schema = projected_arrow_schema.clone();
        let bs = batch_size;

        std::thread::spawn(move || {
            let file = match File::open(&path_owned) {
                Ok(f) => f,
                Err(e) => {
                    let _ = batch_tx.send(Some(Err(arrow::error::ArrowError::IoError(e.to_string(), e))));
                    return;
                }
            };
            let lz4_reader = FrameDecoder::new(BufReader::new(file));
            let ipc_reader = match IpcStreamReader::try_new(lz4_reader, None) {
                Ok(r) => r,
                Err(e) => {
                    let _ = batch_tx.send(Some(Err(e)));
                    return;
                }
            };

            for batch_result in ipc_reader {
                let batch = match batch_result {
                    Ok(b) if b.num_rows() > 0 => b,
                    Ok(_) => continue,
                    Err(e) => {
                        let _ = batch_tx.send(Some(Err(e)));
                        return;
                    }
                };

                // Project away __row_id__
                let columns: Vec<_> = proj_indices.iter().map(|&i| batch.column(i).clone()).collect();
                let projected = match RecordBatch::try_new(proj_schema.clone(), columns) {
                    Ok(p) => p,
                    Err(e) => {
                        let _ = batch_tx.send(Some(Err(e)));
                        return;
                    }
                };

                // Slice to batch_size and send
                let num_rows = projected.num_rows();
                let mut offset = 0;
                while offset < num_rows {
                    let len = (num_rows - offset).min(bs);
                    let slice = projected.slice(offset, len);
                    if batch_tx.send(Some(Ok(slice))).is_err() {
                        return; // receiver dropped
                    }
                    offset += len;
                }
            }
            // Signal EOF
            let _ = batch_tx.send(None);
        });

        // Read the first batch from the channel
        let first_batch = match batch_rx.recv() {
            Ok(Some(Ok(b))) if b.num_rows() > 0 => b,
            Ok(Some(Err(e))) => return Err(MergeError::Arrow(e)),
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

        // Wrap the channel receiver as a boxed iterator for the prefetch mechanism.
        let channel_iter = ChannelIterator { rx: batch_rx };
        let reader: Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>> + Send> =
            Box::new(channel_iter);
        let reader = Arc::new(Mutex::new(reader));

        let (prefetch_tx, prefetch_rx) =
            std::sync::mpsc::sync_channel::<Option<MergeResult<RecordBatch>>>(1);

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

/// Iterator adapter that receives batches from a dedicated IPC reader thread
/// through a bounded channel. Implements `Send` trivially (Receiver is Send).
/// Memory bounded: at most one batch buffered in the channel at any time.
struct ChannelIterator {
    rx: std::sync::mpsc::Receiver<Option<Result<RecordBatch, arrow::error::ArrowError>>>,
}

impl Iterator for ChannelIterator {
    type Item = Result<RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rx.recv() {
            Ok(Some(result)) => Some(result),
            Ok(None) | Err(_) => None,
        }
    }
}
