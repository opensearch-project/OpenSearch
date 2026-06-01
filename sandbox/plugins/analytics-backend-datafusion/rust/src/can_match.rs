/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Can-match evaluation against cached Parquet row-group statistics.
//!
//! Given a file path, column name, and a range [min_value, max_value], checks
//! whether any row group in the cached ParquetMetaData has column statistics
//! that overlap with the requested range.

use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::statistics::Statistics;
use std::fs::File;
use std::sync::Arc;

/// Result of a can-match evaluation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CanMatchResult {
    /// At least one row group may contain matching data.
    Yes,
    /// All row groups are provably outside the filter range.
    No,
    /// Could not determine — conservatively YES.
    Unknown,
}

/// Evaluate can-match by opening the parquet footer directly. Use this only when no
/// cached metadata is available — opens the file every call (~1–10KB footer read per
/// invocation; OS page cache amortises across calls but it's still a syscall).
pub fn can_match_range(
    file_path: &str,
    column_name: &str,
    filter_min: i64,
    filter_max: i64,
) -> CanMatchResult {
    let file = match File::open(file_path) {
        Ok(f) => f,
        Err(_) => return CanMatchResult::Unknown,
    };
    let reader = match SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(_) => return CanMatchResult::Unknown,
    };
    evaluate_metadata(reader.metadata(), column_name, filter_min, filter_max)
}

/// Evaluate can-match using pre-loaded ParquetMetaData. Skips the per-call footer read —
/// hot path when DataFusion's file metadata cache has already loaded the footer.
pub fn can_match_range_with_metadata(
    metadata: &Arc<ParquetMetaData>,
    column_name: &str,
    filter_min: i64,
    filter_max: i64,
) -> CanMatchResult {
    evaluate_metadata(metadata, column_name, filter_min, filter_max)
}

fn evaluate_metadata(
    metadata: &ParquetMetaData,
    column_name: &str,
    filter_min: i64,
    filter_max: i64,
) -> CanMatchResult {
    let schema = metadata.file_metadata().schema_descr();

    let col_idx = match find_column_index(schema, column_name) {
        Some(idx) => idx,
        None => return CanMatchResult::Unknown,
    };

    if metadata.num_row_groups() == 0 {
        return CanMatchResult::No;
    }

    for rg_idx in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(rg_idx);
        let col_meta = rg.column(col_idx);

        if let Some(stats) = col_meta.statistics() {
            match stats_overlap_range_i64(stats, filter_min, filter_max) {
                Some(true) => return CanMatchResult::Yes,
                Some(false) => {} // this RG doesn't match, continue checking
                None => return CanMatchResult::Unknown, // can't determine
            }
        } else {
            return CanMatchResult::Unknown;
        }
    }

    CanMatchResult::No
}

/// Check overlap. Returns Some(true) if overlaps, Some(false) if not, None if can't determine.
fn stats_overlap_range_i64(stats: &Statistics, filter_min: i64, filter_max: i64) -> Option<bool> {
    let (stat_min, stat_max) = match stats {
        Statistics::Int32(s) => {
            let min = s.min_opt()?;
            let max = s.max_opt()?;
            (i64::from(*min), i64::from(*max))
        }
        Statistics::Int64(s) => {
            let min = s.min_opt()?;
            let max = s.max_opt()?;
            (*min, *max)
        }
        _ => return None, // unsupported type
    };

    // Overlap: stat_max >= filter_min AND stat_min <= filter_max
    Some(stat_max >= filter_min && stat_min <= filter_max)
}

fn find_column_index(
    schema: &datafusion::parquet::schema::types::SchemaDescriptor,
    column_name: &str,
) -> Option<usize> {
    for i in 0..schema.num_columns() {
        if schema.column(i).name() == column_name {
            return Some(i);
        }
    }
    None
}
