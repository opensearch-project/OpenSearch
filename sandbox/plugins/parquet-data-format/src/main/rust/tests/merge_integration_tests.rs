/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::array::{Array, PrimitiveArray};
use arrow::array::types::TimestampMillisecondType;
use opensearch_parquet_format::merge::{merge_sorted, merge_unsorted};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use tempfile::tempdir;

/// Helper: collect all parquet files in a directory (sorted by name).
fn list_parquet_files(dir: &str) -> Vec<String> {
    let mut files: Vec<String> = std::fs::read_dir(dir)
        .expect("cannot read directory")
        .filter_map(|e| {
            let p = e.ok()?.path();
            if p.extension().and_then(|s| s.to_str()) == Some("parquet")
                && !p.file_name()?.to_str()?.starts_with("merged")
            {
                Some(p.to_string_lossy().to_string())
            } else {
                None
            }
        })
        .collect();
    files.sort();
    files
}

/// Helper: count total rows across input files.
fn count_rows_in_files(files: &[String]) -> i64 {
    files
        .iter()
        .map(|f| {
            let reader = SerializedFileReader::new(File::open(f).unwrap()).unwrap();
            reader.metadata().file_metadata().num_rows()
        })
        .sum()
}

/// Helper: count rows in a single parquet file.
fn count_rows(path: &str) -> i64 {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    reader.metadata().file_metadata().num_rows()
}

fn input_dir() -> Option<String> {
    std::env::var("PARQUET_TEST_INPUT_DIR").ok()
}

#[test]
fn test_unsorted_merge_real_files() {
    let Some(input_dir) = input_dir() else {
        eprintln!("Skipping: PARQUET_TEST_INPUT_DIR not set");
        return;
    };
    if !Path::new(&input_dir).exists() {
        eprintln!("Skipping: {} not found", input_dir);
        return;
    }

    let files = list_parquet_files(&input_dir);
    assert!(!files.is_empty(), "No parquet files found in {}", input_dir);
    println!("Found {} input files", files.len());

    let expected_rows = count_rows_in_files(&files);
    println!("Total input rows: {}", expected_rows);

    let tmp = tempdir().unwrap();
    let output = tmp.path().join("merged_unsorted.parquet");
    let output_str = output.to_string_lossy().to_string();

    // Empty sort columns → unsorted merge
    merge_unsorted(&files, &output_str, "test-index").unwrap();

    assert!(output.exists(), "Output file was not created");
    let actual_rows = count_rows(&output_str);
    println!("Output rows: {}", actual_rows);
    assert_eq!(actual_rows, expected_rows, "Row count mismatch");
}

/// Verify that ___row_id in the output is monotonically increasing (0, 1, 2, ...).
fn verify_row_id_order(path: &str) {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema().clone();
    let col_idx = schema.index_of("___row_id").expect("___row_id not in output");
    let reader = builder.build().unwrap();

    let mut expected: i64 = 0;
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch.column(col_idx).as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("___row_id should be Int64");
        for i in 0..col.len() {
            assert!(!col.is_null(i), "___row_id should never be null");
            assert_eq!(col.value(i), expected, "___row_id gap at row {}", expected);
            expected += 1;
        }
    }
    println!("Verified ___row_id is sequential 0..{}", expected);
}


#[test]
fn test_sorted_merge_real_files() {
    let Some(input_dir) = input_dir() else {
        eprintln!("Skipping: PARQUET_TEST_INPUT_DIR not set");
        return;
    };
    if !Path::new(&input_dir).exists() {
        eprintln!("Skipping: {} not found", input_dir);
        return;
    }

    let files = list_parquet_files(&input_dir);
    assert!(!files.is_empty(), "No parquet files found in {}", input_dir);

    let expected_rows = count_rows_in_files(&files);
    println!("Total input rows: {}", expected_rows);

    let tmp = tempdir().unwrap();
    let output = tmp.path().join("merged_sorted.parquet");
    let output_str = output.to_string_lossy().to_string();

    // Sort by EventDate ascending (each input file is pre-sorted by EventDate)
    let sort_cols = vec!["EventDate".to_string()];
    let reverse = vec![false];
    let nulls_first = vec![false];

    merge_sorted(&files, &output_str, "test-index", &sort_cols, &reverse, &nulls_first)
        .unwrap();

    assert!(output.exists(), "Output file was not created");
    let actual_rows = count_rows(&output_str);
    println!("Output rows: {}", actual_rows);
    assert_eq!(actual_rows, expected_rows, "Row count mismatch");

    // Verify ___row_id is sequential 0..N
    verify_row_id_order(&output_str);

    // Verify EventDate is non-decreasing in the merged output
    let file = File::open(&output_str).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let out_schema = builder.schema().clone();
    let col_idx = out_schema.index_of("EventDate").unwrap();
    let reader = builder.build().unwrap();

    let mut prev: Option<i64> = None;
    let mut rows_checked: i64 = 0;
    let mut out_of_order: i64 = 0;

    for batch in reader {
        let batch = batch.unwrap();
        let col = batch.column(col_idx).as_any()
            .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
            .unwrap();
        for i in 0..col.len() {
            if col.is_null(i) { continue; }
            let val = col.value(i);
            if let Some(p) = prev {
                if val < p {
                    out_of_order += 1;
                    if out_of_order <= 5 {
                        eprintln!("Out of order at row {}: prev={}, cur={}", rows_checked, p, val);
                    }
                }
            }
            prev = Some(val);
            rows_checked += 1;
        }
    }

    println!("Verified EventDate sort order across {} non-null rows", rows_checked);
    assert_eq!(out_of_order, 0, "Found {} out-of-order rows in EventDate", out_of_order);
}

