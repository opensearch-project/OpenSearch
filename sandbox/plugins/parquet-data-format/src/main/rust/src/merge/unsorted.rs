/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fs::File;

use arrow::array::{BooleanArray, RecordBatchReader};
use arrow::compute::filter_record_batch;
use arrow::datatypes::Schema as ArrowSchema;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::schema::types::SchemaDescriptor;

use crate::log_debug;

use super::context::MergeContext;
use super::error::MergeResult;
use super::schema::{projection_indices_excluding_row_id, ColumnMapping};

/// Unsorted merge: reads each input file sequentially, pads to union schema,
/// rewrites `__row_id__` with globally sequential values. No sorting performed.
/// `live_docs_per_input` follows the same contract as `merge_sorted`.
pub fn merge_unsorted(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    live_docs_per_input: &[Option<Vec<u64>>],
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
        "[RUST] Starting unsorted merge: {} input files, output='{}'",
        input_files.len(),
        output_path
    );

    let mut arrow_schemas: Vec<ArrowSchema> = Vec::with_capacity(input_files.len());
    let mut parquet_descriptors: Vec<SchemaDescriptor> = Vec::with_capacity(input_files.len());
    let mut readers: Vec<ParquetRecordBatchReader> = Vec::with_capacity(input_files.len());
    let mut file_row_counts: Vec<usize> = Vec::with_capacity(input_files.len());
    let mut file_generations: Vec<i64> = Vec::with_capacity(input_files.len());

    for (file_idx, path) in input_files.iter().enumerate() {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let parquet_descr = builder.parquet_schema().clone();
        let num_rows = builder.metadata().file_metadata().num_rows() as usize;
        let generation = crate::writer_properties_builder::read_writer_generation(builder.metadata().file_metadata(), file_idx);

        let projection_indices = projection_indices_excluding_row_id(&schema);
        let projection = parquet::arrow::ProjectionMask::roots(&parquet_descr, projection_indices);
        let reader = builder.with_batch_size(batch_size).with_projection(projection).build()?;

        arrow_schemas.push(reader.schema().as_ref().clone());
        parquet_descriptors.push(parquet_descr);
        readers.push(reader);
        file_row_counts.push(num_rows);
        file_generations.push(generation);
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

    let col_mappings: Vec<ColumnMapping> = arrow_schemas.iter()
        .map(|s| ColumnMapping::new(s, ctx.data_schema()))
        .collect();

    // Row-ID mapping: sized to total source rows; dead rows map to -1.
    let total_rows: usize = file_row_counts.iter().sum();
    let mut mapping: Vec<i64> = vec![-1i64; total_rows];
    let mut gen_keys: Vec<i64> = Vec::with_capacity(input_files.len());
    let mut gen_offsets: Vec<i32> = Vec::with_capacity(input_files.len());
    let mut gen_sizes: Vec<i32> = Vec::with_capacity(input_files.len());

    let mut mapping_offset: usize = 0;
    let mut new_row_id: i64 = 0;

    for (file_idx, reader) in readers.into_iter().enumerate() {
        log_debug!(
            "[RUST] Unsorted merge: processing file {} of {}",
            file_idx + 1,
            input_files.len()
        );

        gen_keys.push(file_generations[file_idx]);
        gen_offsets.push(mapping_offset as i32);
        let file_start_offset = mapping_offset;
        let file_num_rows = file_row_counts[file_idx];
        let live_bits: Option<&Vec<u64>> = live_docs_per_input
            .get(file_idx)
            .and_then(|opt| opt.as_ref());

        let col_mapping = &col_mappings[file_idx];
        let mut base_row_id: u64 = 0;
        for batch_result in reader {
            let batch = batch_result?;
            let batch_rows = batch.num_rows();

            let (filtered, alive_count) = match live_bits {
                None => {
                    for _ in 0..batch_rows {
                        mapping[mapping_offset] = new_row_id;
                        mapping_offset += 1;
                        new_row_id += 1;
                    }
                    (batch, batch_rows)
                }
                Some(bits) => {
                    let mut mask_values: Vec<bool> = Vec::with_capacity(batch_rows);
                    let mut alive = 0usize;
                    for i in 0..batch_rows {
                        let abs = base_row_id + i as u64;
                        let alive_flag = is_alive(bits, abs, file_num_rows as u64);
                        if alive_flag {
                            mapping[mapping_offset] = new_row_id;
                            new_row_id += 1;
                            alive += 1;
                        }
                        mapping_offset += 1;
                        mask_values.push(alive_flag);
                    }
                    let mask = BooleanArray::from(mask_values);
                    (filter_record_batch(&batch, &mask)?, alive)
                }
            };
            base_row_id += batch_rows as u64;
            if alive_count > 0 {
                ctx.push_batch(col_mapping.pad_batch(&filtered)?)?;
            }
        }

        gen_sizes.push((mapping_offset - file_start_offset) as i32);
    }

    let (metadata, crc32) = ctx.finish()?;

    log_debug!(
        "[RUST] Unsorted merge complete: {} total rows written to '{}' within {} row groups, crc32={:#010x}",
        metadata.file_metadata().num_rows(),
        output_path,
        metadata.num_row_groups(),
        crc32
    );

    Ok(super::MergeOutput {
        mapping,
        gen_keys,
        gen_offsets,
        gen_sizes,
        metadata,
        crc32,
    })
}

#[inline]
fn is_alive(bits: &[u64], abs_row_id: u64, num_rows: u64) -> bool {
    if abs_row_id >= num_rows {
        return true;
    }
    let word = (abs_row_id / 64) as usize;
    let bit = (abs_row_id % 64) as u64;
    match bits.get(word) {
        Some(&w) => (w & (1u64 << bit)) != 0,
        None => true,
    }
}
