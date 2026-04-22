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
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use parquet::arrow::arrow_writer::{ArrowRowGroupWriterFactory, compute_leaves};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::SchemaDescriptor;
use rayon::prelude::*;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

use crate::rate_limited_writer::RateLimitedWriter;
use crate::writer_properties_builder::WriterPropertiesBuilder;
use crate::{log_debug, SETTINGS_STORE};

use super::error::{MergeError, MergeResult};
use super::io_task::{
    get_merge_pool, spawn_io_task, IoCommand, RATE_LIMIT_MB_PER_SEC,
};
use super::schema::{append_row_id, build_parquet_root_schema, ROW_ID_COLUMN_NAME};

/// Owns all shared state for a merge operation: schemas, writer factory,
/// IO channel, buffered batches, and counters. Used by both sorted and
/// unsorted merge paths.
pub struct MergeContext {
    data_schema: Arc<ArrowSchema>,
    output_schema: Arc<ArrowSchema>,
    rg_writer_factory: ArrowRowGroupWriterFactory,
    io_tx: tokio_mpsc::Sender<IoCommand>,
    output_chunks: Vec<RecordBatch>,
    output_row_count: usize,
    output_flush_rows: usize,
    row_group_index: usize,
    next_row_id: i64,
    total_rows_written: usize,
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

        let config = SETTINGS_STORE
            .get(index_name)
            .map(|r| r.clone())
            .unwrap_or_default();
        let writer_props = Arc::new(WriterPropertiesBuilder::build(&config));

        let writer = SerializedFileWriter::new(throttled_writer, parquet_root, writer_props)?;
        let rg_writer_factory = ArrowRowGroupWriterFactory::new(&writer, output_schema.clone());
        let io_tx = spawn_io_task(writer);

        Ok(Self {
            data_schema,
            output_schema,
            rg_writer_factory,
            io_tx,
            output_chunks: Vec::new(),
            output_row_count: 0,
            output_flush_rows,
            row_group_index: 0,
            next_row_id: 0,
            total_rows_written: 0,
        })
    }

    pub fn data_schema(&self) -> &Arc<ArrowSchema> {
        &self.data_schema
    }

    /// Buffers a batch (already padded to data_schema) and auto-flushes when
    /// the row count threshold is reached.
    pub fn push_batch(&mut self, batch: RecordBatch) -> MergeResult<()> {
        self.output_row_count += batch.num_rows();
        self.output_chunks.push(batch);
        if self.output_row_count >= self.output_flush_rows {
            self.flush()?;
        }
        Ok(())
    }

    /// Concat buffered batches, append row IDs, encode columns in parallel,
    /// and send the encoded row group to the IO task.
    pub fn flush(&mut self) -> MergeResult<()> {
        if self.output_chunks.is_empty() {
            return Ok(());
        }

        let merged = if self.output_chunks.len() == 1 {
            self.output_chunks.pop().unwrap()
        } else {
            let m = concat_batches(&self.data_schema, self.output_chunks.as_slice())?;
            self.output_chunks.clear();
            m
        };
        let n = merged.num_rows();

        let with_id = append_row_id(&merged, self.next_row_id, &self.output_schema)?;
        drop(merged);

        let col_writers = self
            .rg_writer_factory
            .create_column_writers(self.row_group_index)?;

        let mut leaves_and_writers = Vec::new();
        {
            let mut writer_iter = col_writers.into_iter();
            for (arr, field) in with_id.columns().iter().zip(self.output_schema.fields()) {
                for leaf in compute_leaves(field, arr)? {
                    let col_writer = writer_iter.next().ok_or_else(|| {
                        MergeError::Logic("Fewer column writers than leaf columns".into())
                    })?;
                    leaves_and_writers.push((leaf, col_writer));
                }
            }
        }

        let chunk_results: Vec<
            Result<parquet::arrow::arrow_writer::ArrowColumnChunk, parquet::errors::ParquetError>,
        > = get_merge_pool().install(|| {
            leaves_and_writers
                .into_par_iter()
                .map(|(leaf, mut col_writer)| {
                    col_writer.write(&leaf)?;
                    col_writer.close()
                })
                .collect()
        });

        let mut encoded_chunks = Vec::with_capacity(chunk_results.len());
        for r in chunk_results {
            encoded_chunks.push(r?);
        }

        self.io_tx
            .blocking_send(IoCommand::WriteRowGroup(encoded_chunks))
            .map_err(|_| MergeError::Logic("IO task terminated unexpectedly".into()))?;

        self.row_group_index += 1;
        self.next_row_id += n as i64;
        self.total_rows_written += n;
        self.output_row_count = 0;

        log_debug!(
            "[RUST] Flushed row group {}: {} rows (total: {})",
            self.row_group_index - 1,
            n,
            self.total_rows_written
        );

        Ok(())
    }

    /// Final flush + close the IO task. Returns Parquet metadata.
    pub fn finish(mut self) -> MergeResult<parquet::file::metadata::ParquetMetaData> {
        self.flush()?;

        let (reply_tx, reply_rx) =
            oneshot::channel::<MergeResult<parquet::file::metadata::ParquetMetaData>>();

        self.io_tx
            .blocking_send(IoCommand::Close(reply_tx))
            .map_err(|_| MergeError::Logic("IO task terminated before close".into()))?;

        drop(self.io_tx);

        reply_rx
            .blocking_recv()
            .map_err(|_| MergeError::Logic("IO task terminated during close".into()))?
    }
}
