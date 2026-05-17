/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use parquet::schema::types::SchemaDescriptor;

use crate::log_debug;

use super::context::MergeContext;
use super::cursor::FileCursor;
use super::heap::{cmp_sort_values, get_sort_values, HeapItem};
use super::io_task::get_merge_pool;
use super::schema::ColumnMapping;

/// Performs a streaming k-way merge with an explicit sort direction per column.
/// `live_docs_per_input` is parallel to `input_files`; a `None` (or padded) entry
/// disables filtering for that file (zero-copy fast path).
pub fn merge_sorted(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    sort_columns: &[String],
    reverse_sorts: &[bool],
    nulls_first: &[bool],
    live_docs_per_input: &[Option<Vec<u64>>],
) -> super::MergeResult<super::MergeOutput> {
    let config = crate::writer::SETTINGS_STORE
        .get(index_name)
        .map(|r| r.clone())
        .unwrap_or_default();
    let batch_size = config.get_merge_batch_size();
    let output_flush_rows = config.get_row_group_max_rows();
    let rayon_threads = config.get_merge_rayon_threads();
    let io_threads = config.get_merge_io_threads();
    if input_files.is_empty() {
        return Err(super::MergeError::Logic(
            "merge_sorted called with empty input_files".into(),
        ));
    }

    if sort_columns.is_empty() {
        return Err(super::MergeError::Logic(
            "merge_sorted called with empty sort_columns; use merge_unsorted instead".into(),
        ));
    }

    let pool = get_merge_pool(rayon_threads);
    let direction_label = if reverse_sorts.iter().all(|&r| !r) {
        "ascending"
    } else if reverse_sorts.iter().all(|&r| r) {
        "descending"
    } else {
        "mixed"
    };

    log_debug!(
        "[RUST] Starting streaming merge ({}): {} input files, sort_columns={:?}, \
         batch_size={}, flush_rows={}, merge_threads={}, output='{}'",
        direction_label,
        input_files.len(),
        sort_columns,
        batch_size,
        output_flush_rows,
        pool.current_num_threads(),
        output_path
    );

    // ── Phase 1: Initialize cursors and collect schemas ─────────────────
    let mut cursors: Vec<FileCursor> = Vec::with_capacity(input_files.len());
    let mut arrow_schemas: Vec<ArrowSchema> = Vec::with_capacity(input_files.len());
    let mut parquet_descriptors: Vec<SchemaDescriptor> = Vec::with_capacity(input_files.len());
    let mut file_generations: Vec<i64> = Vec::with_capacity(input_files.len());
    let mut file_row_counts: Vec<usize> = Vec::with_capacity(input_files.len());

    for (file_id, path) in input_files.iter().enumerate() {
        log_debug!("[RUST] Opening cursor {} for file: {}", file_id, path);
        let live_bits = live_docs_per_input
            .get(file_id)
            .and_then(|opt| opt.as_ref())
            .map(|bits| Arc::new(bits.clone()));
        let (cursor, projected_schema, parquet_descr, generation, row_count) =
            FileCursor::new(path, file_id, sort_columns, nulls_first, batch_size, live_bits)?;
        cursors.push(cursor);
        arrow_schemas.push(projected_schema.as_ref().clone());
        parquet_descriptors.push(parquet_descr);
        file_generations.push(generation);
        file_row_counts.push(row_count);
    }

    let num_cursors = cursors.len();

    // ── Phase 2: Create MergeContext (union schemas, writer, IO task) ───
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
    let mut gen_keys: Vec<i64> = Vec::with_capacity(num_cursors);
    let mut gen_offsets: Vec<i32> = Vec::with_capacity(num_cursors);
    let mut gen_sizes: Vec<i32> = Vec::with_capacity(num_cursors);

    let mut offset = 0i32;
    for file_id in 0..num_cursors {
        gen_keys.push(file_generations[file_id]);
        gen_offsets.push(offset);
        let size = file_row_counts[file_id] as i32;
        gen_sizes.push(size);
        offset += size;
    }

    let mut new_row_id: i64 = 0;

    log_debug!(
        "[RUST] Merge initialized ({}): {} cursors",
        direction_label,
        num_cursors
    );

    // ── Phase 3: Seed the heap ──────────────────────────────────────────
    let reverse_sorts_arc = Arc::new(reverse_sorts.to_vec());
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(num_cursors);
    for cursor in &cursors {
        // Cursors with all-dead data are already exhausted — skip.
        if cursor.current_batch.is_none() {
            continue;
        }
        let sv = cursor.current_sort_values()?;
        heap.push(HeapItem {
            sort_values: sv,
            file_id: cursor.file_id,
            reverse_sorts: Arc::clone(&reverse_sorts_arc),
        });
    }

    // ── Phase 4: K-way merge loop — three-tier cascade ──────────────────
    while let Some(item) = heap.pop() {
        let file_id = item.file_id;

        // TIER 1: Single cursor remaining — drain it
        if heap.is_empty() {
            let file_offset = gen_offsets[file_id] as usize;
            loop {
                let (remaining, start_idx, base_row_id) = {
                    let cursor = &cursors[file_id];
                    (cursor.batch_height() - cursor.row_idx, cursor.row_idx, cursor.base_row_id)
                };
                if remaining > 0 {
                    let slice = cursors[file_id].take_live_slice(start_idx, remaining)?;
                    // Build mapping for surviving rows
                    for i in 0..remaining {
                        let abs = base_row_id + (start_idx + i) as u64;
                        if cursors[file_id].is_row_id_alive(abs) {
                            mapping[file_offset + abs as usize] = new_row_id;
                            new_row_id += 1;
                        }
                    }
                    if slice.num_rows() > 0 {
                        ctx.push_batch(col_mappings[file_id].pad_batch(&slice)?)?;
                    }
                }
                if !cursors[file_id].advance_past_batch()? {
                    break;
                }
            }
            break;
        }

        // TIER 2 & 3: Multiple cursors active
        let file_offset = gen_offsets[file_id] as usize;

        loop {
            let heap_top = &heap.peek().unwrap().sort_values;

            // TIER 2: Entire remaining batch fits before heap top
            let last_val = cursors[file_id].last_sort_values()?;
            if cmp_sort_values(&last_val, heap_top, reverse_sorts) != Ordering::Greater {
                let (remaining, start_idx, base_row_id) = {
                    let cursor = &cursors[file_id];
                    (cursor.batch_height() - cursor.row_idx, cursor.row_idx, cursor.base_row_id)
                };
                let slice = cursors[file_id].take_live_slice(start_idx, remaining)?;
                for i in 0..remaining {
                    let abs = base_row_id + (start_idx + i) as u64;
                    if cursors[file_id].is_row_id_alive(abs) {
                        mapping[file_offset + abs as usize] = new_row_id;
                        new_row_id += 1;
                    }
                }
                if slice.num_rows() > 0 {
                    ctx.push_batch(col_mappings[file_id].pad_batch(&slice)?)?;
                }

                if !cursors[file_id].advance_past_batch()? {
                    break;
                }
                // Check if cursor should yield after loading new batch
                let val = cursor.current_sort_values()?;
                if cmp_sort_values(&val, heap_top, reverse_sorts) == Ordering::Greater {
                    heap.push(HeapItem {
                        sort_values: val,
                        file_id,
                        reverse_sorts: Arc::clone(&reverse_sorts_arc),
                    });
                    break;
                }
                continue;
            }

            // TIER 3: Binary search for the exact boundary
            let run_start = cursors[file_id].row_idx;
            let batch_h = cursors[file_id].batch_height();
            let base_row_id = cursors[file_id].base_row_id;

            let mut lo = run_start;
            let mut hi = batch_h - 1;

            {
                let cursor = &cursors[file_id];
                let batch = cursor.current_batch.as_ref().unwrap();
                while lo + 1 < hi {
                    let mid = lo + (hi - lo) / 2;
                    let mid_val = get_sort_values(
                        batch,
                        mid,
                        &cursor.sort_col_indices,
                        &cursor.sort_col_types,
                        &cursor.nulls_first,
                    )?;

                    if cmp_sort_values(&mid_val, heap_top, reverse_sorts) != Ordering::Greater {
                        lo = mid;
                    } else {
                        hi = mid;
                    }
                }
            }
            let run_end = lo;
            let run_len = run_end - run_start + 1;

            if run_len > 0 {
                let slice = cursors[file_id].take_live_slice(run_start, run_len)?;
                for i in 0..run_len {
                    let abs = base_row_id + (run_start + i) as u64;
                    if cursors[file_id].is_row_id_alive(abs) {
                        mapping[file_offset + abs as usize] = new_row_id;
                        new_row_id += 1;
                    }
                }
                if slice.num_rows() > 0 {
                    ctx.push_batch(col_mappings[file_id].pad_batch(&slice)?)?;
                }
            }

            cursors[file_id].row_idx = run_end;
            if !cursors[file_id].advance()? {
                break;
            }

            let next_val = cursors[file_id].current_sort_values()?;
            if cmp_sort_values(&next_val, heap_top, reverse_sorts) == Ordering::Greater {
                heap.push(HeapItem {
                    sort_values: next_val,
                    file_id,
                    reverse_sorts: Arc::clone(&reverse_sorts_arc),
                });
                break;
            }
        }
    }

    // ── Phase 5: Close ──────────────────────────────────────────────────
    let (metadata, crc32) = ctx.finish()?;

    log_debug!(
        "[RUST] Merge complete ({}): {} total rows written to '{}' in {} row groups, crc32={:#010x}",
        direction_label,
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
