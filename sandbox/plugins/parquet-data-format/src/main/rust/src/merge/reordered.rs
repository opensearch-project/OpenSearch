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

use crate::log_debug;

use super::context::MergeContext;
use super::error::MergeResult;
use super::schema::{projection_indices_excluding_row_id, ColumnMapping};

/// Row ID mapping passed from Java: maps (generation, local_row_id) -> new_position.
pub struct ExternalRowIdMapping {
    pub mapping: Vec<i64>,
    pub gen_keys: Vec<i64>,
    pub gen_offsets: Vec<i32>,
    pub gen_sizes: Vec<i32>,
}

impl ExternalRowIdMapping {
    /// Returns the new position for a row given its generation and local row ID.
    pub fn get_new_position(&self, generation: i64, local_row_id: usize) -> Option<i64> {
        for i in 0..self.gen_keys.len() {
            if self.gen_keys[i] == generation {
                let offset = self.gen_offsets[i] as usize;
                if local_row_id < self.gen_sizes[i] as usize {
                    return Some(self.mapping[offset + local_row_id]);
                }
                return None;
            }
        }
        None
    }
}

/// Represents a row with its target position and source data.
struct PositionedRow {
    new_position: i64,
    file_id: usize,
    local_row_id: usize,
}

/// Merges Parquet files reordering rows according to an external RowIdMapping.
/// Used when Parquet is a secondary format and must align its row order with the primary.
///
/// Two-pass approach:
/// 1. Build a schedule: for each row in the input, determine its output position
/// 2. Sort the schedule by output position, then read and write rows in that order
pub fn merge_with_external_mapping(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    external_mapping: &ExternalRowIdMapping,
) -> MergeResult<super::MergeOutput> {
    let config = crate::writer::SETTINGS_STORE
        .get(index_name)
        .map(|r| r.clone())
        .unwrap_or_default();
    let batch_size = config.get_merge_batch_size();
    let output_flush_rows = config.get_row_group_max_rows();
    let rayon_threads = config.get_merge_rayon_threads();
    let io_threads = config.get_merge_io_threads();

    log_debug!(
        "[RUST] Starting reordered merge (secondary): {} input files, output='{}'",
        input_files.len(),
        output_path
    );

    // Phase 1: Collect schemas and metadata
    let mut arrow_schemas: Vec<ArrowSchema> = Vec::with_capacity(input_files.len());
    let mut parquet_descriptors: Vec<SchemaDescriptor> = Vec::with_capacity(input_files.len());
    let mut file_row_counts: Vec<usize> = Vec::with_capacity(input_files.len());
    let mut file_generations: Vec<i64> = Vec::with_capacity(input_files.len());

    for (file_idx, path) in input_files.iter().enumerate() {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let parquet_descr = builder.parquet_schema().clone();
        let num_rows = builder.metadata().file_metadata().num_rows() as usize;
        let generation = crate::writer_properties_builder::read_writer_generation(builder.metadata().file_metadata(), file_idx);

        arrow_schemas.push(schema.as_ref().clone());
        parquet_descriptors.push(parquet_descr);
        file_row_counts.push(num_rows);
        file_generations.push(generation);
    }

    let total_rows: usize = file_row_counts.iter().sum();

    // Phase 2: Build the output schedule — map each input row to its output position
    let mut schedule: Vec<PositionedRow> = Vec::with_capacity(total_rows);
    for (file_id, &generation) in file_generations.iter().enumerate() {
        let row_count = file_row_counts[file_id];
        for local_row_id in 0..row_count {
            let new_pos = external_mapping
                .get_new_position(generation, local_row_id)
                .unwrap_or(local_row_id as i64);
            schedule.push(PositionedRow {
                new_position: new_pos,
                file_id,
                local_row_id,
            });
        }
    }
    // Sort by output position so we write rows in sequential order
    schedule.sort_by_key(|r| r.new_position);

    // Phase 3: Read all rows into memory indexed by (file_id, local_row_id)
    // Then write in schedule order
    let mut ctx = MergeContext::new(
        arrow_schemas.clone(),
        &parquet_descriptors,
        output_path,
        index_name,
        output_flush_rows,
        rayon_threads,
        io_threads,
    )?;

    // Read all batches from all files into a per-file vector of rows (as RecordBatches)
    let mut all_batches: Vec<Vec<arrow::array::RecordBatch>> = Vec::with_capacity(input_files.len());
    for (file_idx, path) in input_files.iter().enumerate() {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let parquet_descr = builder.parquet_schema().clone();
        let schema = builder.schema().clone();
        let projection_indices = projection_indices_excluding_row_id(&schema);
        let projection = parquet::arrow::ProjectionMask::roots(&parquet_descr, projection_indices);
        let reader = builder.with_batch_size(batch_size).with_projection(projection).build()?;

        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result?);
        }
        all_batches.push(batches);
    }

    // Precompute column mappings
    let col_mappings: Vec<ColumnMapping> = arrow_schemas
        .iter()
        .map(|s| {
            let projected = remove_row_id_from_schema(s);
            ColumnMapping::new(&projected, ctx.data_schema())
        })
        .collect();

    // Phase 4: Write rows in schedule order
    // We process in chunks to form batches for the writer
    let chunk_size = batch_size;
    for chunk in schedule.chunks(chunk_size) {
        // For each row in this chunk, extract it from the source batch
        for positioned_row in chunk {
            let file_id = positioned_row.file_id;
            let local_row_id = positioned_row.local_row_id;

            // Find which batch this row belongs to
            let (batch_idx, row_in_batch) = find_row_in_batches(&all_batches[file_id], local_row_id);
            let batch = &all_batches[file_id][batch_idx];
            let slice = batch.slice(row_in_batch, 1);
            let col_mapping = &col_mappings[file_id];
            ctx.push_batch(col_mapping.pad_batch(&slice)?)?;
        }
    }

    let (metadata, crc32) = ctx.finish()?;

    log_debug!(
        "[RUST] Reordered merge complete: {} total rows written to '{}' in {} row groups, crc32={:#010x}",
        metadata.file_metadata().num_rows(),
        output_path,
        metadata.num_row_groups(),
        crc32
    );

    // Secondary format does not produce its own mapping
    Ok(super::MergeOutput {
        mapping: vec![],
        gen_keys: vec![],
        gen_offsets: vec![],
        gen_sizes: vec![],
        metadata,
        crc32,
    })
}

/// Finds which batch and row-within-batch a local_row_id maps to.
fn find_row_in_batches(batches: &[arrow::array::RecordBatch], local_row_id: usize) -> (usize, usize) {
    let mut offset = 0;
    for (batch_idx, batch) in batches.iter().enumerate() {
        if local_row_id < offset + batch.num_rows() {
            return (batch_idx, local_row_id - offset);
        }
        offset += batch.num_rows();
    }
    panic!(
        "local_row_id {} out of bounds (total rows: {})",
        local_row_id, offset
    );
}

/// Removes the __row_id__ field from the schema.
fn remove_row_id_from_schema(schema: &ArrowSchema) -> ArrowSchema {
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|f| f.name() != "__row_id__")
        .cloned()
        .collect();
    ArrowSchema::new(fields)
}
