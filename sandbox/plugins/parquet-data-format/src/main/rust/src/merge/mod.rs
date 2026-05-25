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
pub mod schema;
mod sorted;
mod unsorted;

pub use error::{MergeError, MergeResult};
pub use sorted::merge_sorted;
pub use unsorted::merge_unsorted;

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
}
