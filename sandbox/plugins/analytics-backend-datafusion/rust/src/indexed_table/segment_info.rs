/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Build `SegmentFileInfo`s from the query's object_metas by reading parquet
//! metadata over the object store — no direct filesystem access.
//!
//! Plain async code; nothing here is FFM- or FFI-specific. The module name
//! used to be `jni_helpers` (carried over from an earlier JNI-era layout);
//! renamed to `segment_info` since the contents are about segment metadata
//! construction, not any language bridge.

use super::parquet_bridge;
use super::stream::RowGroupInfo;
use super::table_provider::SegmentFileInfo;
use std::sync::Arc;

/// Build `SegmentFileInfo` from the shard's object metas. Each entry is one
/// segment; its max_doc is derived from the sum of row-group row counts.
pub async fn build_segments(
    store: Arc<dyn object_store::ObjectStore>,
    object_metas: &[object_store::ObjectMeta],
) -> Result<(Vec<SegmentFileInfo>, arrow::datatypes::SchemaRef), String> {
    let mut segments = Vec::with_capacity(object_metas.len());
    let mut schema: Option<arrow::datatypes::SchemaRef> = None;
    let mut cumulative_rows: u64 = 0;

    for (seg_ord, meta) in object_metas.iter().enumerate() {
        let (file_schema, size, pq_meta) =
            parquet_bridge::load_parquet_metadata(Arc::clone(&store), &meta.location)
                .await
                .map_err(|e| format!("parquet metadata {}: {}", meta.location, e))?;

        if schema.is_none() {
            schema = Some(file_schema);
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
        let max_doc = offset;
        let global_base = cumulative_rows;
        cumulative_rows += max_doc as u64;

        segments.push(SegmentFileInfo {
            segment_ord: seg_ord as i32,
            max_doc,
            object_path: meta.location.clone(),
            parquet_size: size,
            row_groups,
            metadata: pq_meta,
            global_base,
        });
    }

    let schema = schema.ok_or_else(|| "No parquet files provided".to_string())?;
    Ok((segments, schema))
}
