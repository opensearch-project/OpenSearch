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

use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;

/// Parquet footer kv key under which the writer stamps the writer generation.
/// Must match `parquet-data-format`'s `WRITER_GENERATION_KEY`.
#[cfg(debug_assertions)]
const WRITER_GENERATION_KEY: &str = "opensearch.writer_generation";

/// Build `SegmentFileInfo` from the shard's object metas.
///
/// `writer_generations[i]` is the writer generation of `object_metas[i]`. Both slices
/// must have the same length.
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
    writer_generations: &[i64],
    metadata_cache: Arc<dyn FileMetadataCache>,
    sort_fields: &[String],
) -> Result<(Vec<SegmentFileInfo>, arrow::datatypes::SchemaRef), String> {
    if object_metas.len() != writer_generations.len() {
        return Err(format!(
            "build_segments: object_metas ({}) and writer_generations ({}) must have the same length",
            object_metas.len(),
            writer_generations.len()
        ));
    }

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
        // `meta` is the authoritative listing snapshot — pass it through so the
        // footer load resolves from cache without a redundant `head()` syscall
        // per segment.
        let (file_schema, size, pq_meta) = parquet_bridge::load_parquet_metadata_with_meta(
            Arc::clone(&store),
            &meta.location,
            meta.clone(),
            Arc::clone(&metadata_cache),
        )
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

        let writer_generation = writer_generations[seg_ord];

        // Debug-only cross-check: the writer also stamps the generation into the parquet
        // footer. If the catalog and the footer disagree, that's a writer-side regression.
        // Only runs in dev/test builds; production never depends on the footer kv.
        #[cfg(debug_assertions)]
        debug_assert_footer_generation_matches_catalog(&pq_meta, writer_generation, &meta.location);

        // Compute per-segment min/max on the LEAD `index.sort.field` column.
        // None,None when no sort field is configured, when the column isn't in this
        // file's schema, when the type isn't supported by `StatisticsConverter`, or
        // when any RG is missing stats. The per-RG min/max → segment min/max
        // aggregation mirrors what DataFusion's `validated_output_ordering` does
        // at `file_scan_config.rs:1347-1363` (chain check).
        let (sort_min, sort_max) = match sort_fields.first() {
            Some(lead) => compute_segment_sort_bounds(lead, &file_schema, &pq_meta),
            None => (None, None),
        };

        segments.push(SegmentFileInfo {
            writer_generation,
            max_doc,
            object_path: meta.location.clone(),
            parquet_size: size,
            row_groups,
            metadata: pq_meta,
            global_base,
            sort_min,
            sort_max,
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
    // Use Utf8View — ParquetOpener's apply_file_schema_type_coercions keeps the file/table
    // schemas aligned, so QTF's coordinator-declared Utf8View matches the produced batches.
    let format = ParquetFormat::default().with_force_view_types(true);
    let schema = FileFormat::infer_schema(&format, state, &store, object_metas)
        .await
        .map_err(|e| format!("infer_schema union: {}", e))?;

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

/// Read per-RG min/max stats for `lead_field` from the parquet footer and
/// reduce them to a per-segment (min-of-mins, max-of-maxes) pair.
///
/// Returns `(None, None)` when:
/// - `lead_field` is absent from this file's arrow schema (segment was
///   written before that field existed),
/// - `StatisticsConverter::try_new` rejects the type (logical types whose
///   parquet-side stats can't decode losslessly),
/// - any RG lacks stats for the column (one missing RG → "can't chain").
///
/// Mirrors how DataFusion's `MinMaxStatistics::new_from_files` aggregates
/// per-file stats; we operate at the RG-within-a-segment granularity instead.
fn compute_segment_sort_bounds(
    lead_field: &str,
    file_schema: &arrow::datatypes::SchemaRef,
    pq_meta: &ParquetMetaData,
) -> (Option<ScalarValue>, Option<ScalarValue>) {
    if file_schema.index_of(lead_field).is_err() {
        return (None, None);
    }

    let converter = match StatisticsConverter::try_new(
        lead_field,
        file_schema,
        pq_meta.file_metadata().schema_descr(),
    ) {
        Ok(c) => c,
        Err(_) => return (None, None),
    };

    let row_groups = pq_meta.row_groups();
    if row_groups.is_empty() {
        return (None, None);
    }

    let mins = match converter.row_group_mins(row_groups.iter()) {
        Ok(arr) => arr,
        Err(_) => return (None, None),
    };
    let maxes = match converter.row_group_maxes(row_groups.iter()) {
        Ok(arr) => arr,
        Err(_) => return (None, None),
    };

    // If any RG lacks stats, both arrays will have null entries at that
    // position. Treat that as "can't chain" — a single missing RG poisons the
    // whole-segment claim because we can't prove a chain across RGs.
    if mins.null_count() > 0 || maxes.null_count() > 0 {
        return (None, None);
    }

    // Reduce the per-RG stat arrays to (min-of-mins, max-of-maxes) by walking
    // the rows as ScalarValues. We can't use arrow::compute::min/max directly
    // because the result type isn't a ScalarValue without a per-DataType match.
    // ScalarValue's PartialOrd handles all the supported types we'd see in a
    // sort key (Int64/Date32/Date64/Timestamp/Utf8/etc.).
    let mut acc_min: Option<ScalarValue> = None;
    for i in 0..mins.len() {
        let v = match ScalarValue::try_from_array(&*mins, i) {
            Ok(v) => v,
            Err(_) => return (None, None),
        };
        acc_min = Some(match acc_min.take() {
            Some(prev) => match prev.partial_cmp(&v) {
                Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal) => prev,
                Some(std::cmp::Ordering::Greater) => v,
                None => return (None, None),
            },
            None => v,
        });
    }

    let mut acc_max: Option<ScalarValue> = None;
    for i in 0..maxes.len() {
        let v = match ScalarValue::try_from_array(&*maxes, i) {
            Ok(v) => v,
            Err(_) => return (None, None),
        };
        acc_max = Some(match acc_max.take() {
            Some(prev) => match prev.partial_cmp(&v) {
                Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal) => prev,
                Some(std::cmp::Ordering::Less) => v,
                None => return (None, None),
            },
            None => v,
        });
    }

    (acc_min, acc_max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use datafusion::execution::cache::DefaultFilesMetadataCache;
    use datafusion::execution::context::SessionContext;
    use datafusion::parquet::arrow::ArrowWriter;
    use object_store::{
        local::LocalFileSystem, path::Path as ObjectPath, ObjectStore, ObjectStoreExt,
    };
    use tempfile::tempdir;

    /// Mirror of what `CacheManager::try_new` auto-installs when no custom
    /// metadata cache is configured.
    fn default_metadata_cache() -> Arc<dyn FileMetadataCache> {
        Arc::new(DefaultFilesMetadataCache::new(50 * 1024 * 1024))
    }

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
        let gens: Vec<i64> = (0..metas.len() as i64).collect();
        let (segments, merged) = build_segments(
            &ctx.state(),
            Arc::clone(&store),
            &metas,
            &gens,
            default_metadata_cache(),
            &[],
        )
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
        let gens: Vec<i64> = (0..metas.len() as i64).collect();
        let (_segments, merged) = build_segments(
            &ctx.state(),
            Arc::clone(&store),
            &metas,
            &gens,
            default_metadata_cache(),
            &[],
        )
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

        let gens_ab: Vec<i64> = (0..metas_ab.len() as i64).collect();
        let gens_ba: Vec<i64> = (0..metas_ba.len() as i64).collect();

        let (_, schema_ab) = build_segments(
            &ctx.state(),
            Arc::clone(&store),
            &metas_ab,
            &gens_ab,
            default_metadata_cache(),
            &[],
        )
        .await
        .unwrap();
        let (_, schema_ba) = build_segments(
            &ctx.state(),
            Arc::clone(&store),
            &metas_ba,
            &gens_ba,
            default_metadata_cache(),
            &[],
        )
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
        let gens: Vec<i64> = (0..metas.len() as i64).collect();
        let result = build_segments(
            &ctx.state(),
            Arc::clone(&store),
            &metas,
            &gens,
            default_metadata_cache(),
            &[],
        )
        .await;
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
