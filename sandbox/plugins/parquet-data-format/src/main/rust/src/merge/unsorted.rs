/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fs::File;

use arrow::array::RecordBatchReader;
use arrow::datatypes::Schema as ArrowSchema;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::schema::types::SchemaDescriptor;

use crate::{log_debug, log_info};

use super::context::MergeContext;
use super::error::MergeResult;
use super::schema::{projection_indices_excluding_row_id, ColumnMapping};

/// Unsorted merge: reads each input file sequentially, pads to union schema,
/// rewrites `__row_id__` with globally sequential values. No sorting performed.
pub fn merge_unsorted(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
) -> MergeResult<u32> {
    let config = crate::writer::SETTINGS_STORE
        .get(index_name)
        .map(|r| r.clone())
        .unwrap_or_default();
    let batch_size = config.get_merge_batch_size();
    let output_flush_rows = config.get_row_group_max_rows();
    let rayon_threads = config.get_merge_rayon_threads();
    let io_threads = config.get_merge_io_threads();
    log_debug!(
        "[RUST] Starting unsorted merge: {} input files, output='{}'",
        input_files.len(),
        output_path
    );

    // Single pass: collect schemas and build readers.
    let mut arrow_schemas: Vec<ArrowSchema> = Vec::with_capacity(input_files.len());
    let mut parquet_descriptors: Vec<SchemaDescriptor> = Vec::with_capacity(input_files.len());
    let mut readers: Vec<ParquetRecordBatchReader> = Vec::with_capacity(input_files.len());

    for path in input_files {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let parquet_descr = builder.parquet_schema().clone();

        let projection_indices = projection_indices_excluding_row_id(&schema);
        let projection = parquet::arrow::ProjectionMask::roots(&parquet_descr, projection_indices);
        let reader = builder.with_batch_size(batch_size).with_projection(projection).build()?;

        // The reader's schema is the projected schema (__row_id__ excluded).
        arrow_schemas.push(reader.schema().as_ref().clone());
        parquet_descriptors.push(parquet_descr);
        readers.push(reader);
    }

    let mut ctx = MergeContext::new(
        arrow_schemas.clone(),
        &parquet_descriptors,
        output_path,
        index_name,
        output_flush_rows,
        rayon_threads,
        io_threads,
    )?;

    // Precompute column mappings per reader
    let col_mappings: Vec<ColumnMapping> = arrow_schemas.iter()
        .map(|s| ColumnMapping::new(s, ctx.data_schema()))
        .collect();

    // Iterate readers for data.
    for (file_idx, reader) in readers.into_iter().enumerate() {
        log_debug!(
            "[RUST] Unsorted merge: processing file {} of {}",
            file_idx + 1,
            input_files.len()
        );

        let mapping = &col_mappings[file_idx];
        for batch_result in reader {
            let batch = batch_result?;
            ctx.push_batch(mapping.pad_batch(&batch)?)?;
        }
    }

    let (_metadata, crc32) = ctx.finish()?;

    log_debug!(
        "[RUST] Unsorted merge complete: {} total rows written to '{}' in {} row groups, crc32={:#010x}",
        _metadata.file_metadata().num_rows(),
        output_path,
        _metadata.num_row_groups(),
        crc32
    );

    Ok(crc32)
}
