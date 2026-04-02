/**
Helpers for building indexed table segments from parquet file paths.
**/

use super::parquet_bridge;
use super::stream::RowGroupInfo;
use super::table_provider::{coerce_binary_to_string, SegmentFileInfo};

/// Build `SegmentFileInfo` from parquet paths + segment max docs.
///
/// Each parquet file is one segment. Reads parquet metadata to discover
/// row groups (count + row counts).
pub fn build_segments(
    parquet_paths: &[String],
    segment_max_docs: &[i64],
) -> Result<(Vec<SegmentFileInfo>, arrow::datatypes::SchemaRef), String> {
    use std::path::PathBuf;

    if parquet_paths.len() != segment_max_docs.len() {
        return Err(format!(
            "parquet_paths.len()={} != segment_max_docs.len()={}",
            parquet_paths.len(),
            segment_max_docs.len()
        ));
    }

    let mut segments = Vec::with_capacity(parquet_paths.len());
    let mut schema: Option<arrow::datatypes::SchemaRef> = None;

    for (seg_ord, (path, &max_doc)) in parquet_paths.iter().zip(segment_max_docs).enumerate() {
        let file = std::fs::File::open(path).map_err(|e| format!("open {}: {}", path, e))?;
        let file_size = file
            .metadata()
            .map_err(|e| format!("stat {}: {}", path, e))?
            .len();

        let (file_schema, pq_meta) = parquet_bridge::load_parquet_metadata(&file)
            .map_err(|e| format!("parquet metadata {}: {}", path, e))?;

        if schema.is_none() {
            schema = Some(coerce_binary_to_string(file_schema));
        }

        let mut row_groups = Vec::new();
        let mut offset: i64 = 0;
        for rg_idx in 0..pq_meta.num_row_groups() {
            let num_rows = pq_meta.row_group(rg_idx).num_rows();
            row_groups.push(RowGroupInfo {
                index: rg_idx,
                first_row: offset,
                num_rows,
            });
            offset += num_rows;
        }

        segments.push(SegmentFileInfo {
            segment_ord: seg_ord as i32,
            max_doc,
            parquet_path: PathBuf::from(path),
            parquet_size: file_size,
            row_groups,
            metadata: pq_meta,
        });
    }

    let schema = schema.ok_or_else(|| "No parquet files provided".to_string())?;
    Ok((segments, schema))
}
