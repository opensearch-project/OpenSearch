/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use object_store::ObjectStore;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use crate::log_debug;

use super::context::MergeContext;
use super::error::MergeResult;
use super::reader::{build_source, read_source_meta, BatchSource};
use super::schema::{projection_indices_excluding_row_id, ColumnMapping};

use crate::memory::merge_pool;
use native_bridge_common::memory_pool::{MemoryReservation, PoolBehavior};

/// Unsorted merge: reads each input file sequentially, pads to union schema,
/// rewrites `__row_id__` with globally sequential values. No sorting performed.
pub fn merge_unsorted(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    output_writer_generation: i64,
    store: Option<Arc<dyn ObjectStore>>,
) -> MergeResult<super::MergeOutput> {
    let mut reservation =
        MemoryReservation::new(merge_pool(), "merge_unsorted", PoolBehavior::Reject);
    merge_unsorted_with_pool(
        input_files,
        output_path,
        index_name,
        output_writer_generation,
        &mut reservation,
        store,
    )
}

/// Unsorted merge with an explicit memory reservation.
pub fn merge_unsorted_with_pool(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    output_writer_generation: i64,
    reservation: &mut MemoryReservation,
    store: Option<Arc<dyn ObjectStore>>,
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

    // Single pass: collect schemas and build readers (local files or store objects).
    let mut arrow_schemas: Vec<ArrowSchema> = Vec::with_capacity(input_files.len());
    let mut parquet_descriptors: Vec<SchemaDescriptor> = Vec::with_capacity(input_files.len());
    let mut readers: Vec<BatchSource> = Vec::with_capacity(input_files.len());
    let mut file_row_counts: Vec<usize> = Vec::with_capacity(input_files.len());
    let mut file_generations: Vec<i64> = Vec::with_capacity(input_files.len());

    for (file_idx, path) in input_files.iter().enumerate() {
        let (schema, parquet_descr, generation, num_rows) =
            read_source_meta(&store, path, file_idx)?;

        let projection_indices = projection_indices_excluding_row_id(&schema);
        // Projected schema (__row_id__ excluded) — matches what the reader yields.
        let projected_schema = ArrowSchema::new(
            projection_indices
                .iter()
                .map(|&i| schema.field(i).clone())
                .collect::<Vec<_>>(),
        );
        let projection = ProjectionMask::roots(&parquet_descr, projection_indices);
        let reader = build_source(&store, path, batch_size, projection)?;

        arrow_schemas.push(projected_schema);
        parquet_descriptors.push(parquet_descr);
        readers.push(reader);
        file_row_counts.push(num_rows);
        file_generations.push(generation);
    }

    let ctx_reservation = reservation.child("merge:flush");
    let mut ctx = MergeContext::new(
        arrow_schemas.clone(),
        &parquet_descriptors,
        output_path,
        index_name,
        output_flush_rows,
        rayon_threads,
        io_threads,
        output_writer_generation,
        ctx_reservation,
        store.clone(),
    )?;

    // Precompute column mappings per reader
    let col_mappings: Vec<ColumnMapping> = arrow_schemas
        .iter()
        .map(|s| ColumnMapping::new(s, ctx.data_schema()))
        .collect();

    // Build row-ID mapping: for unsorted merge, files are concatenated sequentially.
    // old_row_id maps directly to new_row_id with a per-file offset.
    let total_rows: usize = file_row_counts.iter().sum();
    let mapping_bytes = total_rows * std::mem::size_of::<i64>();
    reservation
        .request(mapping_bytes)
        .map_err(|e| super::MergeError::Logic(format!("Merge pool exceeded (mapping): {}", e)))?;
    let mut mapping: Vec<i64> = vec![0i64; total_rows];
    let mut gen_keys: Vec<i64> = Vec::with_capacity(input_files.len());
    let mut gen_offsets: Vec<i32> = Vec::with_capacity(input_files.len());
    let mut gen_sizes: Vec<i32> = Vec::with_capacity(input_files.len());

    let mut mapping_offset: usize = 0;
    let mut new_row_id: i64 = 0;

    // Iterate readers for data.
    for (file_idx, mut reader) in readers.into_iter().enumerate() {
        log_debug!(
            "[RUST] Unsorted merge: processing file {} of {}",
            file_idx + 1,
            input_files.len()
        );

        gen_keys.push(file_generations[file_idx]);
        gen_offsets.push(mapping_offset as i32);
        let file_start_row_id = new_row_id;

        let col_mapping = &col_mappings[file_idx];
        let mut batch_tracked: usize = 0;
        while let Some(batch_result) = reader.next_batch() {
            let batch = batch_result?;
            let num_rows = batch.num_rows();
            let batch_bytes = batch.get_array_memory_size();
            // Track reader batch memory: grow on first batch, delta-adjust on subsequent
            if batch_tracked == 0 {
                reservation.grow(batch_bytes);
                batch_tracked = batch_bytes;
            } else if batch_bytes != batch_tracked {
                if batch_bytes > batch_tracked {
                    reservation.grow(batch_bytes - batch_tracked);
                } else {
                    reservation.shrink(batch_tracked - batch_bytes);
                }
                batch_tracked = batch_bytes;
            }
            for _ in 0..num_rows {
                mapping[mapping_offset] = new_row_id;
                mapping_offset += 1;
                new_row_id += 1;
            }
            ctx.push_batch(col_mapping.pad_batch(&batch)?)?;
        }
        // File done — release batch memory (reader dropped, batch no longer alive)
        reservation.shrink(batch_tracked);

        let file_rows = (new_row_id - file_start_row_id) as i32;
        gen_sizes.push(file_rows);
    }

    let stats = ctx.finish()?;

    log_debug!(
        "[RUST] Unsorted merge complete: {} total rows written to '{}' within {} row groups, crc32={:#010x}",
        stats.metadata.file_metadata().num_rows(),
        output_path,
        stats.metadata.num_row_groups(),
        stats.crc32
    );

    // Detach mapping from reservation — FFI layer will track via merge_pool().grow
    reservation.shrink(mapping_bytes);

    Ok(super::MergeOutput {
        mapping,
        gen_keys,
        gen_offsets,
        gen_sizes,
        metadata: stats.metadata,
        crc32: stats.crc32,
        flush_and_sort_chunk_count: stats.flush_and_sort_chunk_count,
        flush_and_sort_chunk_time_millis: stats.flush_and_sort_chunk_time_millis,
        row_id_mapping_max: stats.row_id_mapping_max,
    })
}
