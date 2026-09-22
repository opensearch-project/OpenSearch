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
use datafusion::parquet::basic::{LogicalType, TimeUnit};
use datafusion::parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use datafusion::parquet::file::statistics::Statistics;
use datafusion::parquet::schema::types::ColumnDescriptor;
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

/// Value domain the min/max came from. Part of the Java/Rust wire contract —
/// do not renumber. Mirrored by `ShardSortBounds` on the Java side.
///
/// Physical type alone isn't enough: `date` and `date_nanos` are both parquet `Int64`
/// but scaled 10⁶ apart, so sharing one kind would let the coordinator compare millis
/// against nanos. Hence the separate timestamp kinds.
pub const VALUE_KIND_INT32: u8 = 1;
pub const VALUE_KIND_INT64: u8 = 2;
pub const VALUE_KIND_INT64_MILLIS: u8 = 3;
pub const VALUE_KIND_INT64_MICROS: u8 = 4;
pub const VALUE_KIND_INT64_NANOS: u8 = 5;

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
    /// True when any row group holds a null, or when the null count is unknown. Under
    /// `NULLS FIRST` a null outranks every real value, so the coordinator never
    /// eliminates a shard whose sort column may hold one.
    pub has_nulls: bool,
    pub value_kind: u8,
}

impl Bounds {
    /// Widens `self` to cover `other`. `None` if they're different value domains,
    /// where a combined range would be meaningless.
    pub fn merge(self, other: Bounds) -> Option<Bounds> {
        if self.value_kind != other.value_kind {
            return None;
        }
        Some(Bounds {
            min: self.min.min(other.min),
            max: self.max.max(other.max),
            // Nulls on either side make the folded range null-bearing.
            has_nulls: self.has_nulls || other.has_nulls,
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
    let descriptor = schema.column(col_idx);

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
        let rg_bounds = bounds_from_stats(stats, &descriptor)?;
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
///
/// `descriptor` supplies the column's logical type, the only thing separating a millisecond
/// timestamp from a nanosecond one — both are physically `Int64`.
fn bounds_from_stats(stats: &Statistics, descriptor: &ColumnDescriptor) -> Option<Bounds> {
    // An unknown null count can't prove the column is null-free, so assume nulls: a wrong
    // `true` costs one extra shard scan, a wrong `false` drops rows.
    let has_nulls = stats.null_count_opt().map_or(true, |n| n > 0);
    match stats {
        Statistics::Int32(s) => Some(Bounds {
            min: *s.min_opt()? as i64,
            max: *s.max_opt()? as i64,
            has_nulls,
            value_kind: VALUE_KIND_INT32,
        }),
        Statistics::Int64(s) => Some(Bounds {
            min: *s.min_opt()?,
            max: *s.max_opt()?,
            has_nulls,
            value_kind: int64_value_kind(descriptor),
        }),
        _ => None,
    }
}

/// Narrows a physically-`Int64` column to its logical value domain.
///
/// `date` writes `Timestamp(MILLIS)` and `date_nanos` writes `Timestamp(NANOS)`, both as
/// parquet `Int64`. One kind for both would let the coordinator compare bounds scaled 10⁶
/// apart — harmless for ordering, wrong results once shards get skipped on it.
///
/// Plain int64s (and timestamps with no logical annotation) stay [`VALUE_KIND_INT64`], a
/// generic integer domain distinct from every timestamp domain.
fn int64_value_kind(descriptor: &ColumnDescriptor) -> u8 {
    match descriptor.logical_type_ref() {
        Some(LogicalType::Timestamp { unit, .. }) => match unit {
            TimeUnit::MILLIS => VALUE_KIND_INT64_MILLIS,
            TimeUnit::MICROS => VALUE_KIND_INT64_MICROS,
            TimeUnit::NANOS => VALUE_KIND_INT64_NANOS,
        },
        _ => VALUE_KIND_INT64,
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
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
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
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
            .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    /// Arrow `Timestamp(unit)` writes parquet `Int64` annotated with that logical unit —
    /// the same shape `date` and `date_nanos` produce.
    fn build_timestamp_parquet(unit: arrow::datatypes::TimeUnit, values: &[i64]) -> Vec<u8> {
        use arrow::array::{
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray,
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            false,
        )]));
        let column: arrow::array::ArrayRef = match unit {
            arrow::datatypes::TimeUnit::Second => {
                Arc::new(TimestampSecondArray::from(values.to_vec()))
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                Arc::new(TimestampMillisecondArray::from(values.to_vec()))
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                Arc::new(TimestampMicrosecondArray::from(values.to_vec()))
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                Arc::new(TimestampNanosecondArray::from(values.to_vec()))
            }
        };
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, Arc::clone(&schema), None).unwrap();
        let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
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
        assert_eq!(
            can_match_range_with_metadata(&metadata, "ts", 150, 250),
            CanMatchResult::Yes
        );
    }

    #[test]
    fn test_disjoint_above() {
        let data = build_test_parquet(&[100, 200, 300]);
        let metadata = metadata_from_bytes(&data);
        assert_eq!(
            can_match_range_with_metadata(&metadata, "ts", 400, 500),
            CanMatchResult::No
        );
    }

    #[test]
    fn test_disjoint_below() {
        let data = build_test_parquet(&[100, 200, 300]);
        let metadata = metadata_from_bytes(&data);
        assert_eq!(
            can_match_range_with_metadata(&metadata, "ts", 0, 50),
            CanMatchResult::No
        );
    }

    #[test]
    fn test_column_not_found() {
        let data = build_test_parquet(&[100, 200]);
        let metadata = metadata_from_bytes(&data);
        assert_eq!(
            can_match_range_with_metadata(&metadata, "nonexistent", 0, 1000),
            CanMatchResult::Unknown
        );
    }

    #[test]
    fn test_exact_boundary() {
        let data = build_test_parquet(&[100, 200]);
        let metadata = metadata_from_bytes(&data);
        assert_eq!(
            can_match_range_with_metadata(&metadata, "ts", 200, 300),
            CanMatchResult::Yes
        );
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

        let result =
            can_match_range_via_store(Arc::clone(&store), &path, file_size, "ts", 150, 250).await;
        assert_eq!(result, CanMatchResult::Yes);

        let result =
            can_match_range_via_store(Arc::clone(&store), &path, file_size, "ts", 400, 500).await;
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
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![5, 42]))]).unwrap();
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
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
            .unwrap();
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
        assert_eq!(
            (bounds.min, bounds.max),
            (100, 600),
            "store path must fold too"
        );
    }

    #[tokio::test]
    async fn test_bounds_via_store_file_not_found() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = ObjectPath::from("missing.parquet");
        assert!(sort_bounds_via_store(store, &path, 100, "ts")
            .await
            .is_none());
    }

    #[test]
    fn test_bounds_merge_rejects_mixed_kinds() {
        let a = Bounds {
            min: 1,
            max: 2,
            has_nulls: false,
            value_kind: VALUE_KIND_INT32,
        };
        let b = Bounds {
            min: 0,
            max: 9,
            has_nulls: false,
            value_kind: VALUE_KIND_INT64,
        };
        assert!(a.merge(b).is_none());
    }

    #[test]
    fn test_bounds_merge_widens_range() {
        let a = Bounds {
            min: 50,
            max: 60,
            has_nulls: false,
            value_kind: VALUE_KIND_INT64,
        };
        let b = Bounds {
            min: 10,
            max: 90,
            has_nulls: false,
            value_kind: VALUE_KIND_INT64,
        };
        let merged = a.merge(b).unwrap();
        assert_eq!(
            (merged.min, merged.max),
            (10, 90),
            "merge must widen in both directions"
        );
    }

    // ---- has_nulls ----

    /// One null-bearing row group taints the whole fold: the coordinator must never
    /// eliminate a shard that could hold a top-ranked null.
    #[test]
    fn test_bounds_merge_ors_has_nulls() {
        let clean = Bounds {
            min: 1,
            max: 2,
            has_nulls: false,
            value_kind: VALUE_KIND_INT64,
        };
        let dirty = Bounds {
            min: 3,
            max: 4,
            has_nulls: true,
            value_kind: VALUE_KIND_INT64,
        };
        assert!(
            clean.merge(dirty).unwrap().has_nulls,
            "nulls on either side must survive the merge"
        );
        assert!(
            dirty.merge(clean).unwrap().has_nulls,
            "OR must be symmetric"
        );
    }

    #[test]
    fn test_bounds_detects_nulls() {
        let data = build_nullable_parquet(&[Some(100), None, Some(300)]);
        let metadata = metadata_from_bytes(&data);
        let bounds = sort_bounds_with_metadata(&metadata, "ts").unwrap();
        assert!(
            bounds.has_nulls,
            "a column holding a null must report has_nulls"
        );
    }

    #[test]
    fn test_bounds_no_nulls_when_all_present() {
        let data = build_test_parquet(&[100, 200, 300]);
        let metadata = metadata_from_bytes(&data);
        let bounds = sort_bounds_with_metadata(&metadata, "ts").unwrap();
        assert!(
            bounds.has_nulls == false,
            "a fully-populated column must report has_nulls == false"
        );
    }

    /// A null in ANY row group taints the shard-wide fold, even when the first group is clean.
    #[test]
    fn test_bounds_nulls_in_later_row_group_taint_the_fold() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, Arc::clone(&schema), None).unwrap();
        for group in [vec![Some(1i64), Some(2)], vec![Some(3), None]] {
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(group))])
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();

        let metadata = metadata_from_bytes(&buf);
        assert!(
            metadata.num_row_groups() > 1,
            "fixture must produce multiple row groups"
        );
        let bounds = sort_bounds_with_metadata(&metadata, "ts").unwrap();
        assert!(
            bounds.has_nulls,
            "a null in the SECOND row group must taint the fold"
        );
    }

    // ---- value_kind carries the logical timestamp unit ----

    /// Guards the millis-vs-nanos bug: both are parquet `Int64`, so a physical-type-only
    /// kind would let the coordinator compare values 10⁶ apart and skip the wrong shards.
    #[test]
    fn test_bounds_millis_and_nanos_have_distinct_value_kinds() {
        let millis = metadata_from_bytes(&build_timestamp_parquet(
            arrow::datatypes::TimeUnit::Millisecond,
            &[1_700_000_000_000],
        ));
        let nanos = metadata_from_bytes(&build_timestamp_parquet(
            arrow::datatypes::TimeUnit::Nanosecond,
            &[1_700_000_000_000_000_000],
        ));

        let millis_bounds = sort_bounds_with_metadata(&millis, "ts").unwrap();
        let nanos_bounds = sort_bounds_with_metadata(&nanos, "ts").unwrap();
        assert_eq!(millis_bounds.value_kind, VALUE_KIND_INT64_MILLIS);
        assert_eq!(nanos_bounds.value_kind, VALUE_KIND_INT64_NANOS);
        assert_ne!(
            millis_bounds.value_kind, nanos_bounds.value_kind,
            "millis and nanos must not share a value kind"
        );
        assert!(
            millis_bounds.merge(nanos_bounds).is_none(),
            "merging across timestamp units must be refused"
        );
    }

    #[test]
    fn test_bounds_micros_value_kind() {
        let metadata = metadata_from_bytes(&build_timestamp_parquet(
            arrow::datatypes::TimeUnit::Microsecond,
            &[1_700_000_000_000_000],
        ));
        let bounds = sort_bounds_with_metadata(&metadata, "ts").unwrap();
        assert_eq!(bounds.value_kind, VALUE_KIND_INT64_MICROS);
    }

    /// A plain `long` column has no timestamp annotation, so it keeps the generic int64
    /// domain — which is what stops it being compared against a millis column.
    #[test]
    fn test_bounds_plain_int64_keeps_generic_kind() {
        let data = build_test_parquet(&[100, 200]);
        let metadata = metadata_from_bytes(&data);
        let bounds = sort_bounds_with_metadata(&metadata, "ts").unwrap();
        assert_eq!(bounds.value_kind, VALUE_KIND_INT64);
        let millis = sort_bounds_with_metadata(
            &metadata_from_bytes(&build_timestamp_parquet(
                arrow::datatypes::TimeUnit::Millisecond,
                &[100],
            )),
            "ts",
        )
        .unwrap();
        assert!(
            bounds.merge(millis).is_none(),
            "plain int64 must not merge with a millis timestamp"
        );
    }
}
