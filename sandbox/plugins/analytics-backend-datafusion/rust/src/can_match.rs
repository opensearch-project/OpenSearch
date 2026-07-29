/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Can-match evaluation using parquet row-group statistics.
//!
//! Checks whether any row group in a parquet file has column statistics that
//! overlap with a given range `[filter_min, filter_max]`. Used by the coordinator
//! to prune shards before fragment dispatch.

use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use datafusion::parquet::file::statistics::Statistics;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::sync::Arc;

/// Result of a can-match evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanMatchResult {
    /// Shard may match — keep it.
    Yes,
    /// Shard provably cannot match — prune it.
    No,
    /// Unable to determine — treat as Yes (fail-open).
    Unknown,
}

/// Physical type the min/max came from. Part of the Java/Rust wire contract —
/// do not renumber.
pub const VALUE_KIND_INT32: u8 = 1;
pub const VALUE_KIND_INT64: u8 = 2;

/// Shard-wide min/max of one column.
///
/// Unlike [`CanMatchResult`], which may stop at the first hit, this is a **fold**:
/// every row group of every file has to be visited, or the range isn't shard-wide.
/// A too-narrow range would let the coordinator skip a shard that really holds a
/// top-N row, so a partial fold reports absent rather than approximate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    pub min: i64,
    pub max: i64,
    pub value_kind: u8,
}

impl Bounds {
    /// Widens `self` to cover `other`. `None` if they're different physical types,
    /// where a combined range would be meaningless.
    fn merge(self, other: Bounds) -> Option<Bounds> {
        if self.value_kind != other.value_kind {
            return None;
        }
        Some(Bounds {
            min: self.min.min(other.min),
            max: self.max.max(other.max),
            value_kind: self.value_kind,
        })
    }
}

/// Fold min/max of `column_name` from pre-loaded metadata (cache hit, zero I/O).
pub fn sort_bounds_with_metadata(metadata: &ParquetMetaData, column_name: &str) -> Option<Bounds> {
    fold_bounds(metadata, column_name)
}

/// Fold min/max of `column_name` by reading the footer via ObjectStore.
/// Cache-miss path; works on local disk and S3 alike.
pub async fn sort_bounds_via_store(
    store: Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: usize,
    column_name: &str,
) -> Option<Bounds> {
    let metadata = read_footer(store, path, file_size).await.ok()?;
    fold_bounds(&metadata, column_name)
}

/// Core fold: visits EVERY row group, no short-circuit. One row group without usable
/// statistics collapses the whole file to `None` — a bound from a subset of row groups
/// would be narrower than the truth.
fn fold_bounds(metadata: &ParquetMetaData, column_name: &str) -> Option<Bounds> {
    let schema = metadata.file_metadata().schema_descr();
    let col_idx = find_column_index(schema, column_name)?;

    let num_row_groups = metadata.num_row_groups();
    if num_row_groups == 0 {
        return None;
    }

    let mut folded: Option<Bounds> = None;
    for rg_idx in 0..num_row_groups {
        let rg = metadata.row_group(rg_idx);
        // Empty row groups hold no values, so missing statistics here aren't a gap.
        if rg.num_rows() == 0 {
            continue;
        }
        let stats = rg.column(col_idx).statistics()?;
        let rg_bounds = bounds_from_stats(stats)?;
        folded = Some(match folded {
            None => rg_bounds,
            Some(acc) => acc.merge(rg_bounds)?,
        });
    }
    folded
}

/// Extract min/max from one row group's statistics. Same supported physical types as
/// [`check_overlap`]; anything else yields `None`.
///
/// An all-null column has no min/max, so this returns `None` rather than a made-up range.
fn bounds_from_stats(stats: &Statistics) -> Option<Bounds> {
    match stats {
        Statistics::Int32(s) => Some(Bounds {
            min: *s.min_opt()? as i64,
            max: *s.max_opt()? as i64,
            value_kind: VALUE_KIND_INT32,
        }),
        Statistics::Int64(s) => Some(Bounds {
            min: *s.min_opt()?,
            max: *s.max_opt()?,
            value_kind: VALUE_KIND_INT64,
        }),
        _ => None,
    }
}

/// Evaluate can-match using pre-loaded ParquetMetaData (from cache).
/// Zero I/O — hot path when the metadata cache has the footer.
pub fn can_match_range_with_metadata(
    metadata: &ParquetMetaData,
    column_name: &str,
    filter_min: i64,
    filter_max: i64,
) -> CanMatchResult {
    evaluate_metadata(metadata, column_name, filter_min, filter_max)
}

/// Evaluate can-match by reading the parquet footer via ObjectStore.
/// Works uniformly on local disk (hot nodes) and S3 (warm nodes).
pub async fn can_match_range_via_store(
    store: Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: usize,
    column_name: &str,
    filter_min: i64,
    filter_max: i64,
) -> CanMatchResult {
    let metadata = match read_footer(store, path, file_size).await {
        Ok(m) => m,
        Err(_) => return CanMatchResult::Unknown,
    };
    evaluate_metadata(&metadata, column_name, filter_min, filter_max)
}

/// Core evaluation logic shared by both paths.
fn evaluate_metadata(
    metadata: &ParquetMetaData,
    column_name: &str,
    filter_min: i64,
    filter_max: i64,
) -> CanMatchResult {
    let file_metadata = metadata.file_metadata();
    let schema = file_metadata.schema_descr();

    let col_idx = match find_column_index(schema, column_name) {
        Some(idx) => idx,
        None => return CanMatchResult::Unknown,
    };

    let num_row_groups = metadata.num_row_groups();
    if num_row_groups == 0 {
        return CanMatchResult::No;
    }

    for rg_idx in 0..num_row_groups {
        let rg = metadata.row_group(rg_idx);
        let col = rg.column(col_idx);

        match col.statistics() {
            Some(stats) => {
                let overlaps = check_overlap(stats, filter_min, filter_max);
                match overlaps {
                    Some(true) => return CanMatchResult::Yes,
                    Some(false) => continue,
                    None => return CanMatchResult::Unknown,
                }
            }
            None => return CanMatchResult::Unknown,
        }
    }

    CanMatchResult::No
}

/// Check if the row-group column statistics overlap with [filter_min, filter_max].
fn check_overlap(stats: &Statistics, filter_min: i64, filter_max: i64) -> Option<bool> {
    match stats {
        Statistics::Int32(s) => {
            let rg_min = *s.min_opt()? as i64;
            let rg_max = *s.max_opt()? as i64;
            Some(rg_min <= filter_max && rg_max >= filter_min)
        }
        Statistics::Int64(s) => {
            let rg_min = *s.min_opt()?;
            let rg_max = *s.max_opt()?;
            Some(rg_min <= filter_max && rg_max >= filter_min)
        }
        _ => None,
    }
}

/// Find column index by name in the parquet schema.
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

/// Read parquet footer from ObjectStore using ParquetMetaDataReader.
async fn read_footer(
    store: Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_size: usize,
) -> Result<ParquetMetaData, String> {
    let size = file_size as u64;
    let mut reader = ParquetObjectReader::new(store, path.clone()).with_file_size(size);
    ParquetMetaDataReader::new()
        .load_and_finish(&mut reader, size)
        .await
        .map_err(|e| format!("metadata read failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;
    use datafusion::parquet::file::reader::FileReader;
    use datafusion::parquet::file::serialized_reader::SerializedFileReader;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    fn build_test_parquet(values: &[i64]) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let mut buf = Vec::new();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    /// One row group per slice in `groups`, so the fold has to visit them all.
    fn build_multi_row_group_parquet(groups: &[&[i64]]) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let mut buf = Vec::new();
        // max_row_group_size=1 would split each batch; instead flush explicitly
        // after each batch so row-group boundaries are exactly the slices.
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        for group in groups {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(group.to_vec()))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        buf
    }

    fn build_nullable_parquet(values: &[Option<i64>]) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
        let mut buf = Vec::new();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    fn metadata_from_bytes(data: &[u8]) -> ParquetMetaData {
        let reader = SerializedFileReader::new(Bytes::from(data.to_vec())).unwrap();
        reader.metadata().clone()
    }

    #[test]
    fn test_overlaps_yes() {
        let data = build_test_parquet(&[100, 200, 300]);
        let metadata = metadata_from_bytes(&data);
        assert_eq!(can_match_range_with_metadata(&metadata, "ts", 150, 250), CanMatchResult::Yes);
    }

    #[test]
    fn test_disjoint_above() {
        let data = build_test_parquet(&[100, 200, 300]);
        let metadata = metadata_from_bytes(&data);
        assert_eq!(can_match_range_with_metadata(&metadata, "ts", 400, 500), CanMatchResult::No);
    }

    #[test]
    fn test_disjoint_below() {
        let data = build_test_parquet(&[100, 200, 300]);
        let metadata = metadata_from_bytes(&data);
        assert_eq!(can_match_range_with_metadata(&metadata, "ts", 0, 50), CanMatchResult::No);
    }

    #[test]
    fn test_column_not_found() {
        let data = build_test_parquet(&[100, 200]);
        let metadata = metadata_from_bytes(&data);
        assert_eq!(can_match_range_with_metadata(&metadata, "nonexistent", 0, 1000), CanMatchResult::Unknown);
    }

    #[test]
    fn test_exact_boundary() {
        let data = build_test_parquet(&[100, 200]);
        let metadata = metadata_from_bytes(&data);
        assert_eq!(can_match_range_with_metadata(&metadata, "ts", 200, 300), CanMatchResult::Yes);
    }

    #[tokio::test]
    async fn test_via_object_store() {
        let data = build_test_parquet(&[100, 200, 300]);
        let file_size = data.len();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("test.parquet");
        store
            .put(&path, PutPayload::from_bytes(Bytes::from(data)))
            .await
            .unwrap();

        let result = can_match_range_via_store(Arc::clone(&store), &path, file_size, "ts", 150, 250).await;
        assert_eq!(result, CanMatchResult::Yes);

        let result = can_match_range_via_store(Arc::clone(&store), &path, file_size, "ts", 400, 500).await;
        assert_eq!(result, CanMatchResult::No);
    }

    #[tokio::test]
    async fn test_via_store_file_not_found() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("missing.parquet");
        let result = can_match_range_via_store(store, &path, 100, "ts", 0, 100).await;
        assert_eq!(result, CanMatchResult::Unknown);
    }

    // ---- sort bounds (fold) ----

    #[test]
    fn test_bounds_single_row_group() {
        let data = build_test_parquet(&[100, 200, 300]);
        let metadata = metadata_from_bytes(&data);
        let bounds = sort_bounds_with_metadata(&metadata, "ts").unwrap();
        assert_eq!(bounds.min, 100);
        assert_eq!(bounds.max, 300);
        assert_eq!(bounds.value_kind, VALUE_KIND_INT64);
    }

    /// The regression that matters: the fold must return the GLOBAL min/max, not
    /// the first row group's. `evaluate_metadata` short-circuits here; the fold
    /// must not.
    #[test]
    fn test_bounds_folds_all_row_groups_not_just_first() {
        let data = build_multi_row_group_parquet(&[&[500, 600], &[100, 200], &[900, 1000]]);
        let metadata = metadata_from_bytes(&data);
        assert!(
            metadata.num_row_groups() > 1,
            "fixture must produce multiple row groups to be meaningful"
        );
        let bounds = sort_bounds_with_metadata(&metadata, "ts").unwrap();
        assert_eq!(bounds.min, 100, "min must come from the SECOND row group");
        assert_eq!(bounds.max, 1000, "max must come from the THIRD row group");
    }

    #[test]
    fn test_bounds_missing_column() {
        let data = build_test_parquet(&[100, 200]);
        let metadata = metadata_from_bytes(&data);
        assert!(sort_bounds_with_metadata(&metadata, "nonexistent").is_none());
    }

    /// Nulls do not affect the range: min/max describe the non-null values, which
    /// is what a range comparison needs.
    #[test]
    fn test_bounds_ignores_nulls_in_range() {
        let data = build_nullable_parquet(&[Some(100), None, Some(300)]);
        let metadata = metadata_from_bytes(&data);
        let bounds = sort_bounds_with_metadata(&metadata, "ts").unwrap();
        assert_eq!(bounds.min, 100);
        assert_eq!(bounds.max, 300);
    }

    /// An all-null column has no min/max at all. Must report absent rather than
    /// fabricate a range (a `0..0` would order the shard first and be wrong).
    #[test]
    fn test_bounds_all_null_column_reports_absent() {
        let data = build_nullable_parquet(&[None, None, None]);
        let metadata = metadata_from_bytes(&data);
        assert!(
            sort_bounds_with_metadata(&metadata, "ts").is_none(),
            "all-null column must yield no bounds, not a fabricated range"
        );
    }

    #[test]
    fn test_bounds_int32_value_kind() {
        use arrow::array::Int32Array;
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![5, 42]))]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let metadata = metadata_from_bytes(&buf);
        let bounds = sort_bounds_with_metadata(&metadata, "n").unwrap();
        assert_eq!((bounds.min, bounds.max), (5, 42));
        assert_eq!(bounds.value_kind, VALUE_KIND_INT32);
    }

    /// Non-integer physical types are out of v1 scope — must report absent
    /// rather than a bogus range.
    #[test]
    fn test_bounds_unsupported_type() {
        use arrow::array::StringArray;
        let schema = Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, false)]));
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let metadata = metadata_from_bytes(&buf);
        assert!(sort_bounds_with_metadata(&metadata, "host").is_none());
    }

    #[tokio::test]
    async fn test_bounds_via_object_store() {
        let data = build_multi_row_group_parquet(&[&[500, 600], &[100, 200]]);
        let file_size = data.len();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("bounds.parquet");
        store
            .put(&path, PutPayload::from_bytes(Bytes::from(data)))
            .await
            .unwrap();

        let bounds = sort_bounds_via_store(Arc::clone(&store), &path, file_size, "ts")
            .await
            .unwrap();
        assert_eq!((bounds.min, bounds.max), (100, 600), "store path must fold too");
    }

    #[tokio::test]
    async fn test_bounds_via_store_file_not_found() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("missing.parquet");
        assert!(sort_bounds_via_store(store, &path, 100, "ts").await.is_none());
    }

    #[test]
    fn test_bounds_merge_rejects_mixed_kinds() {
        let a = Bounds { min: 1, max: 2, value_kind: VALUE_KIND_INT32 };
        let b = Bounds { min: 0, max: 9, value_kind: VALUE_KIND_INT64 };
        assert!(a.merge(b).is_none());
    }

    #[test]
    fn test_bounds_merge_widens_range() {
        let a = Bounds { min: 50, max: 60, value_kind: VALUE_KIND_INT64 };
        let b = Bounds { min: 10, max: 90, value_kind: VALUE_KIND_INT64 };
        let merged = a.merge(b).unwrap();
        assert_eq!((merged.min, merged.max), (10, 90), "merge must widen in both directions");
    }
}
