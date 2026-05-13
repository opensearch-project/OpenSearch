/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Build `SegmentFileInfo`s from the query's object_metas by reading parquet
//! metadata over the object store.
//!
//! Writer generation is read out of each parquet file's footer key-value
//! metadata under the `opensearch.writer_generation` key — the same key the
//! parquet writer stamps at write time (see
//! `parquet-data-format/src/main/rust/src/writer_properties_builder.rs`).

use datafusion::parquet::file::metadata::ParquetMetaData;

use super::parquet_bridge;
use super::stream::RowGroupInfo;
use super::table_provider::SegmentFileInfo;
use std::sync::Arc;

/// Parquet footer kv key under which the writer stamps the writer generation.
/// Must match `parquet-data-format`'s `WRITER_GENERATION_KEY`.
const WRITER_GENERATION_KEY: &str = "opensearch.writer_generation";

/// Extract the writer generation from a parquet file's footer key-value metadata.
fn read_writer_generation(pq_meta: &ParquetMetaData) -> Result<i64, String> {
    let kvs = pq_meta
        .file_metadata()
        .key_value_metadata()
        .ok_or_else(|| format!("parquet file missing footer key-value metadata (expected `{}`)", WRITER_GENERATION_KEY))?;
    let kv = kvs
        .iter()
        .find(|kv| kv.key == WRITER_GENERATION_KEY)
        .ok_or_else(|| format!("parquet footer missing `{}` attribute", WRITER_GENERATION_KEY))?;
    let value = kv
        .value
        .as_ref()
        .ok_or_else(|| format!("parquet footer `{}` has no value", WRITER_GENERATION_KEY))?;
    value
        .parse::<i64>()
        .map_err(|e| format!("parquet footer `{}` not parseable as i64 [{}]: {}", WRITER_GENERATION_KEY, value, e))
}

/// Build `SegmentFileInfo` from the shard's object metas. One `SegmentFileInfo`
/// per input meta; writer generation comes from the parquet footer.
pub async fn build_segments(
    store: Arc<dyn object_store::ObjectStore>,
    object_metas: &[object_store::ObjectMeta],
) -> Result<(Vec<SegmentFileInfo>, arrow::datatypes::SchemaRef), String> {
    let mut segments = Vec::with_capacity(object_metas.len());
    let mut schema: Option<arrow::datatypes::SchemaRef> = None;

    for meta in object_metas.iter() {
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

        let writer_generation = read_writer_generation(&pq_meta)
            .map_err(|e| format!("{} (file={})", e, meta.location))?;

        segments.push(SegmentFileInfo {
            writer_generation,
            max_doc,
            object_path: meta.location.clone(),
            parquet_size: size,
            row_groups,
            metadata: pq_meta,
        });
    }

    let schema = schema.ok_or_else(|| "No parquet files provided".to_string())?;
    Ok((segments, schema))
}
