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

use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;

/// Build `SegmentFileInfo` from the shard's object metas. Each entry is one
/// segment; its max_doc is derived from the sum of row-group row counts.
///
/// The returned schema is the **union across all segment parquet files**,
/// computed by `ParquetFormat::infer_schema` — the same function
/// `ListingTable` uses.
///
/// We do get all `ParquetFormat` knobs
/// (skip_metadata, coerce_int96, binary_as_string, force_view_types,
/// file_metadata_cache, meta_fetch_concurrency, parquet_encryption)
pub async fn build_segments(
    state: &dyn Session,
    store: Arc<dyn object_store::ObjectStore>,
    object_metas: &[object_store::ObjectMeta],
) -> Result<(Vec<SegmentFileInfo>, arrow::datatypes::SchemaRef), String> {
    if object_metas.is_empty() {
        return Err("No parquet files provided".to_string());
    }

    let mut segments = Vec::with_capacity(object_metas.len());
    let mut cumulative_rows: u64 = 0;

    for (seg_ord, meta) in object_metas.iter().enumerate() {
        // Per-segment parquet metadata (RG info, page index) — still needed
        // regardless of schema derivation. The file_metadata_cache on the
        // RuntimeEnv that `infer_schema` uses below shares this data when
        // both are pointed at the same object location, so this isn't a
        // duplicated cold fetch in practice.
        let (_file_schema, size, pq_meta) =
            parquet_bridge::load_parquet_metadata(Arc::clone(&store), &meta.location)
                .await
                .map_err(|e| format!("parquet metadata {}: {}", meta.location, e))?;

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

    // Delegate to DataFusion's canonical schema inference. Internally:
    //   1. Fetch each file's schema concurrently (meta_fetch_concurrency).
    //   2. Sort by location for determinism (see apache/datafusion#6629).
    //   3. Strip field-level metadata when skip_metadata=true (default).
    //   4. `arrow::datatypes::Schema::try_merge` — union by field name,
    //      first-seen order, nullability OR'd, types must match for
    //      primitives (recursively merged for Struct/List/Union).
    //   5. Apply `binary_as_string` and `force_view_types` transforms
    //      if configured.
    let format = ParquetFormat::new();
    let schema = FileFormat::infer_schema(&format, state, &store, object_metas)
        .await
        .map_err(|e| format!("infer_schema union: {}", e))?;

    Ok((segments, schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use datafusion::execution::context::SessionContext;
    use datafusion::parquet::arrow::ArrowWriter;
    use object_store::{local::LocalFileSystem, path::Path as ObjectPath, ObjectStore, ObjectStoreExt};
    use tempfile::tempdir;

    /// Write a parquet file with the given schema + arrays into `dir`
    /// under `filename`. Returns the absolute filesystem path.
    fn write_parquet(
        dir: &std::path::Path,
        filename: &str,
        schema: arrow::datatypes::SchemaRef,
        arrays: Vec<Arc<dyn arrow::array::Array>>,
    ) -> std::path::PathBuf {
        let path = dir.join(filename);
        let file = std::fs::File::create(&path).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    /// Build `[ObjectMeta]` pointing at the given absolute paths via
    /// the local filesystem object store.
    async fn object_metas(
        store: &dyn ObjectStore,
        paths: &[std::path::PathBuf],
    ) -> Vec<object_store::ObjectMeta> {
        let mut out = Vec::new();
        for p in paths {
            let loc = ObjectPath::from(p.to_string_lossy().as_ref());
            out.push(store.head(&loc).await.unwrap());
        }
        out
    }

    /// Two segments with identical schemas: the union equals that
    /// schema (field-level metadata stripped per `skip_metadata`
    /// default, which is fine for default writers that emit none).
    #[tokio::test]
    async fn identical_schemas_merge_to_themselves() {
        let dir = tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int32, false),
        ]));
        let p0 = write_parquet(
            dir.path(),
            "a.parquet",
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        );
        let p1 = write_parquet(
            dir.path(),
            "b.parquet",
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["z"])),
                Arc::new(Int32Array::from(vec![3])),
            ],
        );

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let metas = object_metas(store.as_ref(), &[p0, p1]).await;
        let ctx = SessionContext::new();
        let (segments, merged) = build_segments(&ctx.state(), Arc::clone(&store), &metas)
            .await
            .unwrap();

        assert_eq!(segments.len(), 2);
        assert_eq!(merged.fields().len(), 2);
        assert_eq!(merged.field(0).name(), "name");
        assert_eq!(merged.field(1).name(), "score");
    }

    /// Schema evolution: seg0 lacks `created_day` that seg1 has. The
    /// union must include every field, first-seen-order, with the
    /// "new" field nullable because seg0 didn't declare it as
    /// non-null.
    #[tokio::test]
    async fn union_contains_fields_from_all_segments() {
        let dir = tempdir().unwrap();
        let narrow = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int32, false),
        ]));
        let wide = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int32, false),
            Field::new("created_day", DataType::Int32, true),
        ]));
        let p0 = write_parquet(
            dir.path(),
            "a.parquet",
            narrow.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        );
        let p1 = write_parquet(
            dir.path(),
            "b.parquet",
            wide.clone(),
            vec![
                Arc::new(StringArray::from(vec!["y"])),
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![Some(42)])),
            ],
        );

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let metas = object_metas(store.as_ref(), &[p0, p1]).await;
        let ctx = SessionContext::new();
        let (_segments, merged) = build_segments(&ctx.state(), Arc::clone(&store), &metas)
            .await
            .unwrap();

        let names: Vec<&str> = merged.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["name", "score", "created_day"],
            "union must contain every field across segments in first-seen order"
        );
        let created_day = merged.field_with_name("created_day").unwrap();
        assert_eq!(created_day.data_type(), &DataType::Int32);
        assert!(
            created_day.is_nullable(),
            "field from only-one-segment must be nullable in the union"
        );
    }

    /// Determinism: schema merge is invariant under object-store
    /// listing order. `ParquetFormat` sorts by location before merge
    /// (apache/datafusion#6629). Assert both orderings produce
    /// identical field order.
    #[tokio::test]
    async fn merge_is_deterministic_across_input_ordering() {
        let dir = tempdir().unwrap();
        let schema_a = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("c", DataType::Int32, true),
        ]));
        let p0 = write_parquet(
            dir.path(),
            "a.parquet",
            schema_a,
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        );
        let p1 = write_parquet(
            dir.path(),
            "b.parquet",
            schema_b,
            vec![
                Arc::new(StringArray::from(vec!["y"])),
                Arc::new(Int32Array::from(vec![Some(2)])),
            ],
        );

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let ctx = SessionContext::new();
        let metas_ab = object_metas(store.as_ref(), &[p0.clone(), p1.clone()]).await;
        let metas_ba = object_metas(store.as_ref(), &[p1, p0]).await;

        let (_, schema_ab) = build_segments(&ctx.state(), Arc::clone(&store), &metas_ab)
            .await
            .unwrap();
        let (_, schema_ba) = build_segments(&ctx.state(), Arc::clone(&store), &metas_ba)
            .await
            .unwrap();

        let fields_ab: Vec<&str> = schema_ab
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let fields_ba: Vec<&str> = schema_ba
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            fields_ab, fields_ba,
            "schema order must not depend on object-meta input ordering"
        );
    }

    /// Incompatible types (Int32 vs Int64 on the same field name) is
    /// a fail-fast: the Arrow `Schema::try_merge` rejects it, and we
    /// bubble the error up. Catches accidental type widening at the
    /// shard-read boundary rather than silently at decode time.
    #[tokio::test]
    async fn incompatible_types_on_same_field_error() {
        let dir = tempdir().unwrap();
        let s_i32 = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let s_i64 = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let p0 = write_parquet(
            dir.path(),
            "a.parquet",
            s_i32,
            vec![Arc::new(Int32Array::from(vec![1]))],
        );
        let p1 = write_parquet(
            dir.path(),
            "b.parquet",
            s_i64,
            vec![Arc::new(arrow_array::Int64Array::from(vec![2i64]))],
        );

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let metas = object_metas(store.as_ref(), &[p0, p1]).await;
        let ctx = SessionContext::new();
        let result = build_segments(&ctx.state(), Arc::clone(&store), &metas).await;
        assert!(
            result.is_err(),
            "conflicting Int32 / Int64 on same field name must fail at schema-union time"
        );
        let msg = result.unwrap_err();
        assert!(
            msg.contains("infer_schema union"),
            "error must originate from infer_schema path, got: {}",
            msg
        );
    }
}
