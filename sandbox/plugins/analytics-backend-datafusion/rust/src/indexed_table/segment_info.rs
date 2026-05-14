/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Build `SegmentFileInfo`s from the query's object_metas by reading parquet metadata
//! over the object store.
//!
//! # Source of truth: the catalog snapshot
//!
//! Writer generation comes from the caller as a parallel slice
//! (`writer_generations[i]` is the generation of `object_metas[i]`). The values originate
//! in the Java-side catalog snapshot via `WriterFileSet.writerGeneration` and flow through
//! `create_reader` → `ShardView.writer_generations` → `SessionContextHandle` →
//! `build_segments`. The catalog is the authoritative source.

use datafusion::parquet::file::metadata::ParquetMetaData;

use super::parquet_bridge;
use super::stream::RowGroupInfo;
use super::table_provider::SegmentFileInfo;
use std::sync::Arc;

/// Parquet footer kv key under which the writer stamps the writer generation.
/// Must match `parquet-data-format`'s `WRITER_GENERATION_KEY`.
#[cfg(debug_assertions)]
const WRITER_GENERATION_KEY: &str = "opensearch.writer_generation";

/// Build `SegmentFileInfo` from the shard's object metas.
///
/// `writer_generations[i]` is the writer generation of `object_metas[i]`. Both slices
/// must have the same length.
pub async fn build_segments(
    store: Arc<dyn object_store::ObjectStore>,
    object_metas: &[object_store::ObjectMeta],
    writer_generations: &[i64],
) -> Result<(Vec<SegmentFileInfo>, arrow::datatypes::SchemaRef), String> {
    if object_metas.len() != writer_generations.len() {
        return Err(format!(
            "build_segments: object_metas ({}) and writer_generations ({}) must have the same length",
            object_metas.len(),
            writer_generations.len()
        ));
    }

    let mut segments = Vec::with_capacity(object_metas.len());
    let mut schema: Option<arrow::datatypes::SchemaRef> = None;

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

        let writer_generation = writer_generations[seg_ord];

        // Debug-only cross-check: the writer also stamps the generation into the parquet
        // footer. If the catalog and the footer disagree, that's a writer-side regression.
        // Only runs in dev/test builds; production never depends on the footer kv.
        #[cfg(debug_assertions)]
        debug_assert_footer_generation_matches_catalog(&pq_meta, writer_generation, &meta.location);

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

/// Read the writer generation out of a parquet footer's key-value metadata and panic if
/// it disagrees with what the catalog provided. Debug-only assertion — never on the
/// production read path.
#[cfg(debug_assertions)]
fn debug_assert_footer_generation_matches_catalog(
    pq_meta: &ParquetMetaData,
    catalog_generation: i64,
    location: &object_store::path::Path,
) {
    let kvs = match pq_meta.file_metadata().key_value_metadata() {
        Some(kvs) => kvs,
        None => {
            // Older files written before the writer started stamping the kv have no kv block at
            // all. We don't fail here — older files predate the assertion contract.
            return;
        }
    };
    let kv = match kvs.iter().find(|kv| kv.key == WRITER_GENERATION_KEY) {
        Some(kv) => kv,
        None => return,
    };
    let value = match kv.value.as_ref() {
        Some(v) => v,
        None => panic!(
            "parquet footer `{}` has no value (file={})",
            WRITER_GENERATION_KEY, location
        ),
    };
    let footer_gen: i64 = value.parse().unwrap_or_else(|e| {
        panic!(
            "parquet footer `{}` not parseable as i64 [{}] (file={}): {}",
            WRITER_GENERATION_KEY, value, location, e
        )
    });
    assert_eq!(
        footer_gen, catalog_generation,
        "parquet footer writer_generation ({}) disagrees with catalog ({}) for file {}",
        footer_gen, catalog_generation, location
    );
}
