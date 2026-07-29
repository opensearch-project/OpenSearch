/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

mod context;
mod cursor;
pub mod error;
pub mod heap;
pub mod io_task;
pub mod metrics;
pub mod schema;
mod sorted;
mod unsorted;

pub use error::{MergeError, MergeResult};
pub use sorted::merge_sorted;
pub use sorted::merge_sorted_with_pool;
pub use unsorted::merge_unsorted;
pub use unsorted::merge_unsorted_with_pool;

/// Output of a merge operation. Carries both the row-ID mapping (for remapping
/// secondary-format row IDs post-merge) and the Parquet file metadata + CRC32
/// of the merged output file.
pub struct MergeOutput {
    /// Flat mapping array: mapping[offset + old_row_id] = new_row_id
    pub mapping: Vec<i64>,
    /// Generation keys (parallel with gen_offsets and gen_sizes)
    pub gen_keys: Vec<i64>,
    /// Starting offset in `mapping` for each generation
    pub gen_offsets: Vec<i32>,
    /// Number of rows per generation
    pub gen_sizes: Vec<i32>,
    /// Parquet file metadata for the merged output file
    pub metadata: parquet::file::metadata::ParquetMetaData,
    /// Whole-file CRC32 of the merged output file
    pub crc32: u32,
    /// Per-merge: number of flush+sort+chunk passes that ran inside this merge.
    pub flush_and_sort_chunk_count: i64,
    /// Per-merge: cumulative wall-clock millis spent in flush+sort+chunk passes.
    pub flush_and_sort_chunk_time_millis: i64,
    /// Per-merge: highest row_id assigned (= total rows written by this merge).
    pub row_id_mapping_max: i64,
}
