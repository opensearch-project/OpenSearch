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

use super::error::{MergeError, MergeResult};
use super::heap::{get_sort_values, SortKey};
use super::io_task::get_merge_pool;
use super::schema::projection_indices_excluding_row_id;

use native_bridge_common::memory_pool::MemoryReservation;

/// A cursor over a single sorted Parquet input file.
///
/// When deferred mode is active (controlled by the dynamic index setting
/// `index.parquet.merge_deferred_column_threshold`, default 0 = always deferred),
/// uses two readers: a sort-only reader for the merge heap and a data reader
/// loaded on demand. Otherwise uses a single all-column reader.
pub struct FileCursor {
    sort_reader: Arc<Mutex<parquet::arrow::arrow_reader::ParquetRecordBatchReader>>,
    sort_prefetch_rx: std::sync::mpsc::Receiver<Option<MergeResult<RecordBatch>>>,
    sort_prefetch_tx: std::sync::mpsc::SyncSender<Option<MergeResult<RecordBatch>>>,
    sort_prefetch_pending: bool,
    pub sort_batch: Option<RecordBatch>,

    data_reader: Option<parquet::arrow::arrow_reader::ParquetRecordBatchReader>,
    data_batch: Option<RecordBatch>,
    sort_batch_index: usize,
    data_batch_index: usize,
    deferred: bool,

    pub row_idx: usize,
    pub file_id: usize,
    pub sort_col_indices: Vec<usize>,
    pub sort_col_types: Vec<ArrowDataType>,
    pub nulls_first: Vec<bool>,
    current_sort_batch_bytes: usize,
    current_data_batch_bytes: usize,
}

impl FileCursor {
    pub fn new(
        path: &str,
        file_id: usize,
        sort_columns: &[String],
        nulls_first: &[bool],
        batch_size: usize,
        deferred_threshold: usize,
        reservation: &mut MemoryReservation,
    ) -> MergeResult<(Self, Arc<ArrowSchema>, SchemaDescriptor, i64, usize)> {
        // Open file and read metadata
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let writer_generation = crate::writer_properties_builder::read_writer_generation(
            builder.metadata().file_metadata(),
            file_id,
        );
        let total_row_count = builder.metadata().file_metadata().num_rows() as usize;
        let parquet_schema_descr = builder.parquet_schema().clone();

        // Resolve sort column types
        let sort_col_types: Vec<ArrowDataType> = sort_columns
            .iter()
            .map(|col| {
                schema
                    .fields()
                    .iter()
                    .find(|f| f.name() == col.as_str())
                    .map(|f| f.data_type().clone())
                    .ok_or_else(|| {
                        MergeError::Logic(format!(
                            "Sort column '{}' not found in file '{}' (cursor {})",
                            col, path, file_id
                        ))
                    })
            })
            .collect::<MergeResult<_>>()?;

        // Decide mode based on schema width
        let sort_col_set: std::collections::HashSet<&str> =
            sort_columns.iter().map(|s| s.as_str()).collect();
        let deferred = schema
            .fields()
            .iter()
            .filter(|f| !sort_col_set.contains(f.name().as_str()))
            .filter(|f| f.name() != super::schema::ROW_ID_COLUMN_NAME)
            .filter(|f| {
                matches!(
                    f.data_type(),
                    ArrowDataType::Utf8
                        | ArrowDataType::LargeUtf8
                        | ArrowDataType::Binary
                        | ArrowDataType::LargeBinary
                )
            })
            .count()
            >= deferred_threshold;

        // Data projection: all columns except __row_id__
        let data_projection_indices = projection_indices_excluding_row_id(&schema);

        // Build sort reader (sort-only in deferred, all-columns in eager)
        let file1 = File::open(path)?;
        let builder1 = ParquetRecordBatchReaderBuilder::try_new(file1)?;
        let sort_projection = if deferred {
            let sort_indices: Vec<usize> = sort_columns
                .iter()
                .filter_map(|c| schema.fields().iter().position(|f| f.name() == c.as_str()))
                .collect();
            parquet::arrow::ProjectionMask::roots(builder1.parquet_schema(), sort_indices)
        } else {
            parquet::arrow::ProjectionMask::roots(
                builder1.parquet_schema(),
                data_projection_indices.clone(),
            )
        };
        let mut sort_reader = builder1
            .with_batch_size(batch_size)
            .with_projection(sort_projection)
            .build()?;

        // Build data reader (only in deferred mode)
        let data_reader = if deferred {
            let file2 = File::open(path)?;
            let builder2 = ParquetRecordBatchReaderBuilder::try_new(file2)?;
            let data_proj = parquet::arrow::ProjectionMask::roots(
                builder2.parquet_schema(),
                data_projection_indices.clone(),
            );
            Some(
                builder2
                    .with_batch_size(batch_size)
                    .with_projection(data_proj)
                    .build()?,
            )
        } else {
            None
        };

        // Projected schema from file metadata
        let projected_schema = Arc::new(ArrowSchema::new(
            data_projection_indices
                .iter()
                .map(|&i| schema.field(i).clone())
                .collect::<Vec<_>>(),
        ));

        // Read first sort batch
        let first_sort_batch = match sort_reader.next() {
            Some(Ok(b)) if b.num_rows() > 0 => b,
            Some(Err(e)) => return Err(e.into()),
            _ => {
                return Err(MergeError::Logic(format!(
                    "File '{}' (cursor {}) yielded no rows",
                    path, file_id
                )))
            }
        };

        // Resolve sort column indices within the sort batch schema
        let sort_batch_schema = first_sort_batch.schema();
        let sort_col_indices: Vec<usize> = sort_columns
            .iter()
            .map(|col| {
                sort_batch_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == col.as_str())
                    .ok_or_else(|| {
                        MergeError::Logic(format!(
                            "Sort column '{}' not found in projected batch for file '{}'",
                            col, path
                        ))
                    })
            })
            .collect::<MergeResult<_>>()?;

        let (sort_prefetch_tx, sort_prefetch_rx) =
            std::sync::mpsc::sync_channel::<Option<MergeResult<RecordBatch>>>(1);
        let sort_reader = Arc::new(Mutex::new(sort_reader));

        let mut cursor = Self {
            sort_reader,
            sort_prefetch_rx,
            sort_prefetch_tx,
            sort_prefetch_pending: false,
            sort_batch: Some(first_sort_batch),
            data_reader,
            data_batch: None,
            sort_batch_index: 0,
            data_batch_index: 0,
            deferred,
            row_idx: 0,
            file_id,
            sort_col_indices,
            sort_col_types,
            nulls_first: nulls_first.to_vec(),
            current_sort_batch_bytes: 0,
            current_data_batch_bytes: 0,
        };

        // Track sort batch + prefetch (estimate 2x first batch)
        let batch_bytes = cursor.sort_batch.as_ref().unwrap().get_array_memory_size();
        reservation.grow(batch_bytes * 2);
        cursor.current_sort_batch_bytes = batch_bytes;

        cursor.start_sort_prefetch();
        Ok((
            cursor,
            projected_schema,
            parquet_schema_descr,
            writer_generation,
            total_row_count,
        ))
    }

    fn start_sort_prefetch(&mut self) {
        if self.sort_prefetch_pending {
            return;
        }
        self.sort_prefetch_pending = true;
        let reader = Arc::clone(&self.sort_reader);
        let tx = self.sort_prefetch_tx.clone();
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

    pub fn load_next_batch(&mut self, reservation: &mut MemoryReservation) -> MergeResult<bool> {
        let old_sort_bytes = self.current_sort_batch_bytes;
        self.sort_batch = None;

        // Release data batch tracking — previous data_batch is dropped
        if self.current_data_batch_bytes > 0 {
            reservation.shrink(self.current_data_batch_bytes);
            self.current_data_batch_bytes = 0;
        }
        self.data_batch = None;

        let sort_result = match self.sort_prefetch_rx.recv() {
            Ok(Some(Ok(batch))) => Some(batch),
            Ok(Some(Err(e))) => {
                self.sort_prefetch_pending = false;
                // Error: release sort batch tracking since cursor is now exhausted
                reservation.shrink(old_sort_bytes);
                self.current_sort_batch_bytes = 0;
                return Err(e);
            }
            Ok(None) | Err(_) => None,
        };
        self.sort_prefetch_pending = false;

        match sort_result {
            Some(batch) => {
                let new_bytes = batch.get_array_memory_size();
                self.sort_batch = Some(batch);
                self.row_idx = 0;
                self.sort_batch_index += 1;
                self.start_sort_prefetch();
                // Delta-adjust: new sort batch may differ in size from previous
                if new_bytes > old_sort_bytes {
                    reservation.grow(new_bytes - old_sort_bytes);
                } else if new_bytes < old_sort_bytes {
                    reservation.shrink(old_sort_bytes - new_bytes);
                }
                self.current_sort_batch_bytes = new_bytes;
                Ok(true)
            }
            None => {
                // Cursor exhausted — release all sort batch tracking
                self.data_reader = None;
                reservation.shrink(old_sort_bytes);
                self.current_sort_batch_bytes = 0;
                Ok(false)
            }
        }
    }

    fn ensure_data_loaded(&mut self, reservation: &mut MemoryReservation) -> MergeResult<()> {
        if !self.deferred {
            return Ok(());
        }
        if self.data_batch.is_some() && self.data_batch_index == self.sort_batch_index + 1 {
            return Ok(());
        }

        match self.try_load_data(reservation) {
            Ok(()) => Ok(()),
            Err(e) => {
                // Error path: close reader and release any data_batch memory
                self.data_reader = None;
                if self.current_data_batch_bytes > 0 {
                    reservation.shrink(self.current_data_batch_bytes);
                    self.current_data_batch_bytes = 0;
                }
                Err(e)
            }
        }
    }

    fn try_load_data(&mut self, reservation: &mut MemoryReservation) -> MergeResult<()> {
        let reader = self
            .data_reader
            .as_mut()
            .ok_or_else(|| MergeError::Logic("Data reader already closed".into()))?;

        // Release previous data_batch — about to load a new one
        if self.current_data_batch_bytes > 0 {
            reservation.shrink(self.current_data_batch_bytes);
            self.current_data_batch_bytes = 0;
        }
        self.data_batch = None;

        while self.data_batch_index <= self.sort_batch_index {
            match reader.next() {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        return Err(MergeError::Logic(format!(
                            "Data reader returned empty batch at position {}",
                            self.data_batch_index
                        )));
                    }
                    if self.data_batch_index == self.sort_batch_index {
                        // Target batch — validate and keep
                        if let Some(ref sb) = self.sort_batch {
                            if batch.num_rows() != sb.num_rows() {
                                return Err(MergeError::Logic(format!(
                                    "Data batch rows ({}) != sort batch rows ({}) at index {}",
                                    batch.num_rows(),
                                    sb.num_rows(),
                                    self.sort_batch_index
                                )));
                            }
                        }
                        let data_bytes = batch.get_array_memory_size();
                        // Track full-column data batch — already allocated by data_reader.next()
                        reservation.grow(data_bytes);
                        self.current_data_batch_bytes = data_bytes;
                        self.data_batch = Some(batch);
                    }
                    // Skipped batch — discard
                    self.data_batch_index += 1;
                }
                Some(Err(e)) => return Err(e.into()),
                None => {
                    return Err(MergeError::Logic(format!(
                        "Data reader exhausted at position {}, needed sort_batch_index={}",
                        self.data_batch_index, self.sort_batch_index
                    )))
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn current_sort_values(&self) -> MergeResult<Vec<SortKey>> {
        let batch = self
            .sort_batch
            .as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        get_sort_values(
            batch,
            self.row_idx,
            &self.sort_col_indices,
            &self.sort_col_types,
            &self.nulls_first,
        )
    }

    #[inline]
    pub fn last_sort_values(&self) -> MergeResult<Vec<SortKey>> {
        let batch = self
            .sort_batch
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
        self.sort_batch.as_ref().map_or(0, |b| b.num_rows())
    }

    #[inline]
    pub fn take_slice(
        &mut self,
        start: usize,
        len: usize,
        reservation: &mut MemoryReservation,
    ) -> MergeResult<RecordBatch> {
        if self.deferred {
            self.ensure_data_loaded(reservation)?;
            let batch = self
                .data_batch
                .as_ref()
                .ok_or_else(|| MergeError::Logic("Data batch not loaded".into()))?;
            Ok(batch.slice(start, len))
        } else {
            let batch = self
                .sort_batch
                .as_ref()
                .ok_or_else(|| MergeError::Logic("Batch is None".into()))?;
            Ok(batch.slice(start, len))
        }
    }

    pub fn advance(&mut self, reservation: &mut MemoryReservation) -> MergeResult<bool> {
        if self.sort_batch.is_none() {
            return Ok(false);
        }
        self.row_idx += 1;
        if self.row_idx >= self.sort_batch.as_ref().unwrap().num_rows() {
            self.sort_batch = None;
            self.data_batch = None;
            // Batch boundary crossed — release data_batch before loading next sort batch
            if self.current_data_batch_bytes > 0 {
                reservation.shrink(self.current_data_batch_bytes);
                self.current_data_batch_bytes = 0;
            }
            return self.load_next_batch(reservation);
        }
        Ok(true)
    }

    pub fn advance_past_batch(&mut self, reservation: &mut MemoryReservation) -> MergeResult<bool> {
        self.sort_batch = None;
        self.data_batch = None;
        // Skip remaining rows — release data_batch before loading next sort batch
        if self.current_data_batch_bytes > 0 {
            reservation.shrink(self.current_data_batch_bytes);
            self.current_data_batch_bytes = 0;
        }
        self.load_next_batch(reservation)
    }
}
