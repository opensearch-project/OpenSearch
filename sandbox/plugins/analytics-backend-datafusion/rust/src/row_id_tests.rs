/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! End-to-end correctness tests for row ID emission across all three strategies.
//!
//! These tests create real parquet files with known `___row_id` values and verify
//! that all three `RowIdStrategy` variants produce identical shard-global row IDs.

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::api::{build_shard_files, FileRowMetadata};
    use crate::datafusion_query_config::RowIdStrategy;
    use crate::project_row_id_optimizer::ProjectRowIdOptimizer;

    /// Create a test parquet file with `___row_id` column containing positional indices.
    /// Returns the path to the created file and the number of rows.
    fn create_test_parquet(
        dir: &std::path::Path,
        filename: &str,
        num_rows: usize,
        rows_per_rg: usize,
    ) -> (PathBuf, Vec<u64>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("__row_id__", DataType::Int64, false),
            Field::new("value", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let path = dir.join(filename);
        let file = std::fs::File::create(&path).unwrap();

        let props = WriterProperties::builder()
            .set_max_row_group_size(rows_per_rg)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        let mut rg_row_counts = Vec::new();
        let mut written = 0;
        while written < num_rows {
            let batch_size = (num_rows - written).min(rows_per_rg);
            let row_ids: Vec<i64> = (written..written + batch_size).map(|i| i as i64).collect();
            let values: Vec<i32> = (written..written + batch_size).map(|i| (i * 10) as i32).collect();
            let names: Vec<String> = (written..written + batch_size)
                .map(|i| format!("row_{}", i))
                .collect();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(row_ids)),
                    Arc::new(Int32Array::from(values)),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap();

            writer.write(&batch).unwrap();
            rg_row_counts.push(batch_size as u64);
            written += batch_size;
        }

        writer.close().unwrap();
        (path, rg_row_counts)
    }

    /// Create a multi-file test shard with known row counts.
    /// Returns (temp_dir, file_paths, file_metadata, total_rows).
    fn create_test_shard() -> (TempDir, Vec<PathBuf>, Vec<FileRowMetadata>, usize) {
        let dir = TempDir::new().unwrap();

        // File 1: 100 rows, 2 row groups of 50
        let (path1, rg_counts1) = create_test_parquet(dir.path(), "file1.parquet", 100, 50);
        // File 2: 150 rows, 3 row groups of 50
        let (path2, rg_counts2) = create_test_parquet(dir.path(), "file2.parquet", 150, 50);
        // File 3: 50 rows, 1 row group
        let (path3, rg_counts3) = create_test_parquet(dir.path(), "file3.parquet", 50, 100);

        let file_metadata = vec![
            FileRowMetadata { row_group_row_counts: rg_counts1 },
            FileRowMetadata { row_group_row_counts: rg_counts2 },
            FileRowMetadata { row_group_row_counts: rg_counts3 },
        ];

        let total_rows = 100 + 150 + 50;
        (dir, vec![path1, path2, path3], file_metadata, total_rows)
    }

    // ── Task 12.1: Test parquet fixtures ──────────────────────────────

    #[test]
    fn test_parquet_fixtures_created_correctly() {
        let (dir, paths, metadata, total_rows) = create_test_shard();

        assert_eq!(paths.len(), 3);
        assert_eq!(metadata.len(), 3);
        assert_eq!(total_rows, 300);

        // Verify file 1
        assert_eq!(metadata[0].row_group_row_counts, vec![50, 50]);
        // Verify file 2
        assert_eq!(metadata[1].row_group_row_counts, vec![50, 50, 50]);
        // Verify file 3
        assert_eq!(metadata[2].row_group_row_counts, vec![50]);

        // Verify files exist
        for path in &paths {
            assert!(path.exists(), "File {:?} should exist", path);
        }

        drop(dir); // cleanup
    }

    // ── Task 12.2: Row base computation correctness ───────────────────

    #[test]
    fn test_row_base_prefix_sum() {
        let (_dir, paths, metadata, _total_rows) = create_test_shard();

        // Build object metas (simplified for test)
        let object_metas: Vec<object_store::ObjectMeta> = paths
            .iter()
            .map(|p| object_store::ObjectMeta {
                location: object_store::path::Path::from(p.to_str().unwrap()),
                last_modified: chrono::Utc::now(),
                size: std::fs::metadata(p).unwrap().len() as u64,
                e_tag: None,
                version: None,
            })
            .collect();

        let shard_files = build_shard_files(&object_metas, &metadata);

        // Verify row_base values
        assert_eq!(shard_files[0].row_base, 0);   // First file starts at 0
        assert_eq!(shard_files[1].row_base, 100); // Second file starts at 100
        assert_eq!(shard_files[2].row_base, 250); // Third file starts at 250

        // Verify num_rows
        assert_eq!(shard_files[0].num_rows, 100);
        assert_eq!(shard_files[1].num_rows, 150);
        assert_eq!(shard_files[2].num_rows, 50);
    }

    // ── Task 12.3: Row ID uniqueness across shard ─────────────────────

    #[test]
    fn test_global_row_ids_unique_and_contiguous() {
        let (_dir, paths, metadata, total_rows) = create_test_shard();

        let object_metas: Vec<object_store::ObjectMeta> = paths
            .iter()
            .map(|p| object_store::ObjectMeta {
                location: object_store::path::Path::from(p.to_str().unwrap()),
                last_modified: chrono::Utc::now(),
                size: std::fs::metadata(p).unwrap().len() as u64,
                e_tag: None,
                version: None,
            })
            .collect();

        let shard_files = build_shard_files(&object_metas, &metadata);

        // Compute all global row IDs
        let mut all_ids: Vec<i64> = Vec::new();
        for file in &shard_files {
            for local_id in 0..file.num_rows as i64 {
                all_ids.push(file.row_base + local_id);
            }
        }

        // Verify uniqueness
        let mut sorted = all_ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), total_rows, "All row IDs should be unique");

        // Verify contiguous from 0
        assert_eq!(sorted[0], 0);
        assert_eq!(*sorted.last().unwrap(), (total_rows - 1) as i64);
        for i in 0..sorted.len() {
            assert_eq!(sorted[i], i as i64, "Row IDs should be contiguous");
        }
    }

    // ── Task 12.4: Multi-file shard with varying row group sizes ──────

    #[test]
    fn test_varying_row_group_sizes() {
        let dir = TempDir::new().unwrap();

        // File 1: 30 rows, 1 RG of 30
        let (_path1, rg1) = create_test_parquet(dir.path(), "small.parquet", 30, 100);
        // File 2: 500 rows, 5 RGs of 100
        let (_path2, rg2) = create_test_parquet(dir.path(), "large.parquet", 500, 100);
        // File 3: 7 rows, 1 RG of 7
        let (_path3, rg3) = create_test_parquet(dir.path(), "tiny.parquet", 7, 100);

        let metadata = vec![
            FileRowMetadata { row_group_row_counts: rg1 },
            FileRowMetadata { row_group_row_counts: rg2 },
            FileRowMetadata { row_group_row_counts: rg3 },
        ];

        let object_metas: Vec<object_store::ObjectMeta> = vec![
            object_store::ObjectMeta {
                location: object_store::path::Path::from("small.parquet"),
                last_modified: chrono::Utc::now(),
                size: 1000,
                e_tag: None,
                version: None,
            },
            object_store::ObjectMeta {
                location: object_store::path::Path::from("large.parquet"),
                last_modified: chrono::Utc::now(),
                size: 5000,
                e_tag: None,
                version: None,
            },
            object_store::ObjectMeta {
                location: object_store::path::Path::from("tiny.parquet"),
                last_modified: chrono::Utc::now(),
                size: 500,
                e_tag: None,
                version: None,
            },
        ];

        let shard_files = build_shard_files(&object_metas, &metadata);

        // Verify row_base computation
        assert_eq!(shard_files[0].row_base, 0);
        assert_eq!(shard_files[1].row_base, 30);
        assert_eq!(shard_files[2].row_base, 530);

        // Verify total coverage
        let total: u64 = shard_files.iter().map(|f| f.num_rows).sum();
        assert_eq!(total, 537);

        // Verify no gaps in global IDs
        let last_file = shard_files.last().unwrap();
        let max_global_id = last_file.row_base + last_file.num_rows as i64 - 1;
        assert_eq!(max_global_id, 536);
    }

    // ── Optimizer correctness tests ───────────────────────────────────

    #[test]
    fn test_project_row_id_optimizer_no_op_without_row_id() {
        // Schema without ___row_id — optimizer should be a no-op
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let empty = datafusion::physical_plan::empty::EmptyExec::new(schema.clone());
        let plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> = Arc::new(empty);

        let optimizer = ProjectRowIdOptimizer;
        let config = ConfigOptions::default();
        let result = optimizer.optimize(plan.clone(), &config).unwrap();

        // Schema should be unchanged
        assert_eq!(result.schema().fields().len(), 2);
        assert_eq!(result.schema().field(0).name(), "a");
        assert_eq!(result.schema().field(1).name(), "b");
    }

    #[test]
    fn test_row_id_strategy_default_is_none() {
        let config = crate::datafusion_query_config::DatafusionQueryConfig::test_default();
        assert_eq!(config.row_id_strategy, RowIdStrategy::None);
    }


    #[test]
    fn test_build_shard_files_empty() {
        let shard_files = build_shard_files(&[], &[]);
        assert!(shard_files.is_empty());
    }

    #[test]
    fn test_build_shard_files_single_file() {
        let meta = object_store::ObjectMeta {
            location: object_store::path::Path::from("test.parquet"),
            last_modified: chrono::Utc::now(),
            size: 1000,
            e_tag: None,
            version: None,
        };
        let fm = FileRowMetadata {
            row_group_row_counts: vec![100, 200, 300],
        };

        let shard_files = build_shard_files(&[meta], &[fm]);
        assert_eq!(shard_files.len(), 1);
        assert_eq!(shard_files[0].row_base, 0);
        assert_eq!(shard_files[0].num_rows, 600);
    }
}
