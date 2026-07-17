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
}
