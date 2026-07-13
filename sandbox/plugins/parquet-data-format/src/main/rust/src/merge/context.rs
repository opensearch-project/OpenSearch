/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use parquet::arrow::arrow_writer::{compute_leaves, ArrowRowGroupWriterFactory};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::SchemaDescriptor;
use rayon::prelude::*;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

use crate::crc_writer::CrcWriter;
use crate::rate_limited_writer::RateLimitedWriter;
use crate::writer_properties_builder::WriterPropertiesBuilder;
use crate::{log_debug, log_error, SETTINGS_STORE};

use native_bridge_common::memory_pool::MemoryReservation;

use super::error::{MergeError, MergeResult};
use super::io_task::{get_merge_pool, spawn_io_task, IoCommand, RATE_LIMIT_MB_PER_SEC};
use super::schema::{append_row_id, build_parquet_root_schema, ROW_ID_COLUMN_NAME};

/// Owns all shared state for a merge operation: schemas, writer factory,
/// IO channel, buffered batches, and counters. Used by both sorted and
/// unsorted merge paths.
pub struct MergeContext {
    data_schema: Arc<ArrowSchema>,
    output_schema: Arc<ArrowSchema>,
    rg_writer_factory: ArrowRowGroupWriterFactory,
    io_tx: tokio_mpsc::Sender<IoCommand>,
    col_writers: Option<Vec<parquet::arrow::arrow_writer::ArrowColumnWriter>>,
    output_row_count: usize,
    output_flush_rows: usize,
    row_group_index: usize,
    next_row_id: i64,
    total_rows_written: usize,
    rayon_threads: Option<usize>,
    // Per-merge counters returned via `finish()` and forwarded to the per-shard tracker on the
    // Java side (see NativeParquetMergeStrategy + ParquetShardStatsTracker).
    flush_and_sort_chunk_count: i64,
    flush_and_sort_chunk_time_millis: i64,
    reservation: MemoryReservation,
    tracked_writer_bytes: usize,
}

impl MergeContext {
    /// Creates a new merge context: builds union schemas, opens the output
    /// writer, and spawns the background IO task.
    pub fn new(
        arrow_schemas: Vec<ArrowSchema>,
        parquet_descriptors: &[SchemaDescriptor],
        output_path: &str,
        index_name: &str,
        output_flush_rows: usize,
        rayon_threads: Option<usize>,
        io_threads: Option<usize>,
        output_writer_generation: i64,
        reservation: MemoryReservation,
    ) -> MergeResult<Self> {
        if let Some(parent) = Path::new(output_path).parent() {
            if !parent.exists() {
                return Err(MergeError::Logic(format!(
                    "Output directory '{}' does not exist.",
                    parent.display()
                )));
            }
        }

        let union_data_schema = ArrowSchema::try_merge(arrow_schemas).map_err(|e| {
            MergeError::Logic(format!(
                "Failed to compute union schema across input files: {}",
                e
            ))
        })?;
        let data_schema = Arc::new(union_data_schema);

        let mut output_fields: Vec<ArrowField> = data_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        output_fields.push(ArrowField::new(
            ROW_ID_COLUMN_NAME,
            ArrowDataType::Int64,
            false,
        ));
        let output_schema = Arc::new(ArrowSchema::new(output_fields));

        let parquet_root = build_parquet_root_schema(parquet_descriptors)?;

        let output_file = File::create(output_path)?;
        let throttled_writer =
            RateLimitedWriter::new(output_file, RATE_LIMIT_MB_PER_SEC).map_err(MergeError::Io)?;

        let (crc_writer, crc_handle) = CrcWriter::new(throttled_writer);

        let config = SETTINGS_STORE
            .get(index_name)
            .map(|r| r.clone())
            .unwrap_or_default();
        let writer_props = Arc::new(
            WriterPropertiesBuilder::build_with_generation(
                &config,
                Some(output_writer_generation),
                &output_schema,
            )
            .map_err(|e| {
                MergeError::Logic(format!("Invalid encoding/compression config: {}", e))
            })?,
        );

        let writer = SerializedFileWriter::new(crc_writer, parquet_root, writer_props)?;
        let rg_writer_factory = ArrowRowGroupWriterFactory::new(&writer, output_schema.clone());
        let io_tx = spawn_io_task(writer, crc_handle, io_threads);

        let col_writers = rg_writer_factory.create_column_writers(0)?;

        Ok(Self {
            data_schema,
            output_schema,
            rg_writer_factory,
            io_tx,
            col_writers: Some(col_writers),
            output_row_count: 0,
            output_flush_rows,
            row_group_index: 0,
            next_row_id: 0,
            total_rows_written: 0,
            rayon_threads,
            flush_and_sort_chunk_count: 0,
            flush_and_sort_chunk_time_millis: 0,
            reservation,
            tracked_writer_bytes: 0,
        })
    }

    pub fn data_schema(&self) -> &Arc<ArrowSchema> {
        &self.data_schema
    }

    /// Writes a batch directly to column writers and triggers a row group flush
    /// when the row count threshold is reached. Each batch is written and dropped
    /// immediately — no buffering — so only one batch's decoded data (~64 MB) is
    /// ever live at a time.
    pub fn push_batch(&mut self, batch: RecordBatch) -> MergeResult<()> {
        let num_rows = batch.num_rows();
        let with_id = append_row_id(&batch, self.next_row_id, &self.output_schema)?;
        let with_id_bytes = with_id.get_array_memory_size();
        drop(batch);

        // Track with_id batch (transient — alive during column write)
        self.reservation.grow(with_id_bytes);

        let col_writers = self
            .col_writers
            .as_mut()
            .ok_or_else(|| MergeError::Logic("Column writers not initialized".into()))?;

        // Compute leaf columns (O(columns) pointer math), then parallel-write
        // across columns via rayon. Each column writer encodes independently.
        let mut all_leaves: Vec<parquet::arrow::arrow_writer::ArrowLeafColumn> =
            Vec::with_capacity(col_writers.len());
        for (arr, field) in with_id.columns().iter().zip(self.output_schema.fields()) {
            let leaves = compute_leaves(field, arr)?;
            all_leaves.extend(leaves);
        }

        let write_errors: Vec<_> = get_merge_pool(self.rayon_threads).install(|| {
            col_writers
                .par_iter_mut()
                .zip(all_leaves.into_par_iter())
                .filter_map(|(writer, leaf)| writer.write(&leaf).err())
                .collect()
        });

        if let Some(e) = write_errors.into_iter().next() {
            log_error!("[RUST] Column write failed during push_batch: {}", e);
            self.reservation.shrink(with_id_bytes);
            return Err(e.into());
        }

        // with_id dropped here — release its tracking
        self.reservation.shrink(with_id_bytes);

        // Track actual column writer memory using memory_size() API
        let actual_writer_bytes: usize = col_writers.iter().map(|w| w.memory_size()).sum();
        if actual_writer_bytes > self.tracked_writer_bytes {
            self.reservation
                .grow(actual_writer_bytes - self.tracked_writer_bytes);
        } else if actual_writer_bytes < self.tracked_writer_bytes {
            self.reservation
                .shrink(self.tracked_writer_bytes - actual_writer_bytes);
        }
        self.tracked_writer_bytes = actual_writer_bytes;

        self.next_row_id += num_rows as i64;
        self.output_row_count += num_rows;
        self.total_rows_written += num_rows;

        if self.output_row_count >= self.output_flush_rows {
            self.flush()?;
        }
        Ok(())
    }

    /// Close current column writers in parallel (encode + compress), send the
    /// encoded row group to the IO task, and open fresh writers for the next
    /// row group.
    ///
    /// `flush_and_sort_chunk_count` and `flush_and_sort_chunk_time_millis` are
    /// always recorded — including on failure paths — to keep this counter
    /// symmetric with rayon's `merge_wall_millis`.
    pub fn flush(&mut self) -> MergeResult<()> {
        if self.output_row_count == 0 {
            return Ok(());
        }
        let flush_start = std::time::Instant::now();
        self.flush_and_sort_chunk_count += 1;

        let result = self.do_flush();

        self.flush_and_sort_chunk_time_millis += flush_start.elapsed().as_millis() as i64;
        result
    }

    fn do_flush(&mut self) -> MergeResult<()> {
        let col_writers = self
            .col_writers
            .take()
            .ok_or_else(|| MergeError::Logic("Column writers not initialized".into()))?;
        let n = self.output_row_count;

        // Parallel close: each column writer encodes and compresses its buffered
        // pages. This is the CPU-heavy step and benefits from rayon parallelism.
        let chunk_results: Vec<
            Result<parquet::arrow::arrow_writer::ArrowColumnChunk, parquet::errors::ParquetError>,
        > = super::metrics::record_merge(|| {
            let results: Vec<_> = get_merge_pool(self.rayon_threads).install(|| {
                col_writers
                    .into_par_iter()
                    .map(|col_writer| col_writer.close())
                    .collect()
            });
            Ok(results)
        })?;

        let mut encoded_chunks = Vec::with_capacity(chunk_results.len());
        for r in chunk_results {
            encoded_chunks.push(r?);
        }

        self.io_tx
            .blocking_send(IoCommand::WriteRowGroup(encoded_chunks))
            .map_err(|_| MergeError::Logic("IO task terminated unexpectedly".into()))?;

        self.row_group_index += 1;
        self.output_row_count = 0;

        // Column writers closed via close() — release tracked writer memory
        self.reservation.shrink(self.tracked_writer_bytes);
        self.tracked_writer_bytes = 0;

        // Open writers for the next row group.
        self.col_writers = Some(
            self.rg_writer_factory
                .create_column_writers(self.row_group_index)?,
        );

        log_debug!(
            "[RUST] Flushed row group {}: {} rows (total: {})",
            self.row_group_index - 1,
            n,
            self.total_rows_written
        );

        Ok(())
    }

    /// Final flush + close the IO task. Returns Parquet metadata, CRC32, and per-merge stats
    /// to be forwarded to the per-shard tracker on the Java side.
    pub fn finish(mut self) -> MergeResult<MergeFinishStats> {
        self.flush()?;
        // Drop the freshly-created writers for the next (unused) row group.
        if let Some(writers) = self.col_writers.take() {
            for w in writers {
                let _ = w.close();
            }
        }
        // Capture the per-merge counters BEFORE moving `self` into the IO send below.
        let final_row_id_max = self.next_row_id;
        let flush_count = self.flush_and_sort_chunk_count;
        let flush_time_millis = self.flush_and_sort_chunk_time_millis;

        let (reply_tx, reply_rx) =
            oneshot::channel::<MergeResult<(parquet::file::metadata::ParquetMetaData, u32)>>();

        self.io_tx
            .blocking_send(IoCommand::Close(reply_tx))
            .map_err(|_| MergeError::Logic("IO task terminated before close".into()))?;

        drop(self.io_tx);

        let (metadata, crc32) = reply_rx
            .blocking_recv()
            .map_err(|_| MergeError::Logic("IO task terminated during close".into()))??;

        Ok(MergeFinishStats {
            metadata,
            crc32,
            flush_and_sort_chunk_count: flush_count,
            flush_and_sort_chunk_time_millis: flush_time_millis,
            row_id_mapping_max: final_row_id_max,
        })
    }
}

/// Result returned from `MergeContext::finish()`.
/// Carries the parquet metadata + CRC plus per-merge stat counters that the caller
/// forwards to the per-shard `ParquetShardStatsTracker`.
pub struct MergeFinishStats {
    pub metadata: parquet::file::metadata::ParquetMetaData,
    pub crc32: u32,
    pub flush_and_sort_chunk_count: i64,
    pub flush_and_sort_chunk_time_millis: i64,
    pub row_id_mapping_max: i64,
}
