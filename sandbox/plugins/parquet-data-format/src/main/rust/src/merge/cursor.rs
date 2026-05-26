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
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::schema::types::SchemaDescriptor;

use native_bridge_common::memory_pool::MemoryReservation;

use super::error::{MergeError, MergeResult};
use super::heap::{get_sort_values, SortKey};
use super::io_task::get_merge_pool;
use super::schema::projection_indices_excluding_row_id;

/// A cursor over a single sorted Parquet input file.
pub struct FileCursor {
    reader: Arc<Mutex<parquet::arrow::arrow_reader::ParquetRecordBatchReader>>,
    prefetch_rx: std::sync::mpsc::Receiver<Option<MergeResult<RecordBatch>>>,
    prefetch_tx: std::sync::mpsc::SyncSender<Option<MergeResult<RecordBatch>>>,
    prefetch_pending: bool,
    pub current_batch: Option<RecordBatch>,
    pub row_idx: usize,
    pub file_id: usize,
    pub sort_col_indices: Vec<usize>,
    pub sort_col_types: Vec<ArrowDataType>,
    pub nulls_first: Vec<bool>,
    /// Bytes currently tracked in the reservation for this cursor's batches.
    current_batch_bytes: usize,
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
        reservation: &mut MemoryReservation,
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

        // Estimate cursor memory: batch_size rows × avg_row_bytes × 2 (current + prefetch)
        // RESERVATION: reserve_estimated for cursor memory (current batch + prefetch buffer).
        // Estimate = 2× (num_fields × batch_size × 8). Reconciled after first batch read.
        let total_uncompressed: usize = parquet_schema_descr.root_schema().get_fields().len() * batch_size * 8; // rough estimate
        let cursor_estimate = total_uncompressed * 2;
        let estimate = reservation.reserve_estimated(cursor_estimate)
            .map_err(|e| MergeError::Logic(format!("Merge memory limit exceeded (cursor {}): {}", file_id, e)))?;

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

        // RESERVATION: reconcile estimate with actual first batch size × 2 (current + prefetch).
        // If actual < estimate: shrinks the over-reservation. If actual > estimate: grows (infallible).
        let actual_batch_bytes = first_batch.get_array_memory_size();
        let actual_cursor_bytes = actual_batch_bytes * 2;
        reservation.reconcile(estimate, actual_cursor_bytes);

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
            current_batch_bytes: actual_batch_bytes,
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

    /// Loads the next batch from the prefetch channel, replacing the current batch.
    ///
    /// # Reservation accounting (infallible grow/shrink on parent reservation):
    /// - On success: adjusts by net delta (new_bytes - old_bytes). Uses infallible `grow`/`shrink`
    ///   because the memory is already allocated by the prefetch thread — we're just tracking it.
    ///   This means pool.used can temporarily exceed pool.limit without rejection.
    /// - On exhaustion/error: shrinks by old_bytes (cursor no longer holds any batch).
    pub fn load_next_batch(&mut self, reservation: &mut MemoryReservation) -> MergeResult<bool> {
        let old_bytes = self.current_batch_bytes;
        self.current_batch = None;

        match self.prefetch_rx.recv() {
            Ok(Some(Ok(batch))) => {
                let new_bytes = batch.get_array_memory_size();
                self.current_batch = Some(batch);
                self.row_idx = 0;
                self.prefetch_pending = false;
                self.start_prefetch();
                // RESERVATION: infallible delta adjustment — memory already exists
                if new_bytes > old_bytes {
                    reservation.grow(new_bytes - old_bytes);
                } else if new_bytes < old_bytes {
                    reservation.shrink(old_bytes - new_bytes);
                }
                self.current_batch_bytes = new_bytes;
                Ok(true)
            }
            Ok(Some(Err(e))) => {
                self.prefetch_pending = false;
                // RESERVATION: cursor error — release all batch memory
                reservation.shrink(old_bytes);
                self.current_batch_bytes = 0;
                Err(e)
            }
            Ok(None) | Err(_) => {
                self.prefetch_pending = false;
                // RESERVATION: cursor exhausted — release all batch memory
                reservation.shrink(old_bytes);
                self.current_batch_bytes = 0;
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

    pub fn advance(&mut self, reservation: &mut MemoryReservation) -> MergeResult<bool> {
        if self.current_batch.is_none() {
            return Ok(false);
        }
        self.row_idx += 1;
        if self.row_idx >= self.current_batch.as_ref().unwrap().num_rows() {
            self.current_batch = None;
            return self.load_next_batch(reservation);
        }
        Ok(true)
    }

    pub fn advance_past_batch(&mut self, reservation: &mut MemoryReservation) -> MergeResult<bool> {
        self.current_batch = None;
        self.load_next_batch(reservation)
    }
}
