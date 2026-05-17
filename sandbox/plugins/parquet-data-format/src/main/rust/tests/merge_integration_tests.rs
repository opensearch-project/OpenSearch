/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

use arrow::array::*;
use arrow::array::types::TimestampMillisecondType;
use arrow::datatypes::{DataType, Field, Schema};
use opensearch_parquet_format::merge::{merge_sorted, merge_unsorted};
use opensearch_parquet_format::native_settings::NativeSettings;
use opensearch_parquet_format::writer::SETTINGS_STORE;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// Register a small merge_batch_size for the given index so the cursor splits into multiple batches.
fn register_small_batch_size(index_name: &str, batch_size: usize) {
    SETTINGS_STORE.insert(index_name.to_string(), NativeSettings {
        merge_batch_size: Some(batch_size),
        ..Default::default()
    });
}

/// Write a single RecordBatch to a Parquet file.
fn write_parquet(path: &str, batch: &RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

/// Read all Int64 values from a column.
fn read_all_int64(path: &str, col: &str) -> Vec<i64> {
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    let mut vals = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let idx = batch.schema().index_of(col).unwrap();
        let arr = batch.column(idx).as_primitive::<arrow::datatypes::Int64Type>();
        for i in 0..arr.len() {
            vals.push(arr.value(i));
        }
    }
    vals
}

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

/// Verify that __row_id__ in the output is monotonically increasing (0, 1, 2, ...).
fn verify_row_id_order(path: &str) {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema().clone();
    let col_idx = schema.index_of("__row_id__").expect("__row_id__ not in output");
    let reader = builder.build().unwrap();

    let mut expected: i64 = 0;
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch.column(col_idx).as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("__row_id__ should be Int64");
        for i in 0..col.len() {
            assert!(!col.is_null(i), "__row_id__ should never be null");
            assert_eq!(col.value(i), expected, "__row_id__ gap at row {}", expected);
            expected += 1;
        }
    }
    println!("Verified __row_id__ is sequential 0..{}", expected);
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

    // Verify __row_id__ is sequential 0..N
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

// ─── TIER 2 yield-to-heap tests ─────────────────────────────────────────────
// These tests exercise the logic where after TIER 2 drains a full batch and
// loads a new one, the new batch's first value is greater than the heap top,
// so the cursor must yield back to the heap.

/// Two files where file A has two batches (small row group size forces batch boundary).
/// After TIER 2 drains A's first batch, A's second batch starts higher than B's
/// current position, so A must yield to the heap.
#[test]
fn test_tier2_yield_after_batch_boundary() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
    ]));

    // File A: [1, 2, 3, 10, 11, 12] with batch_size=3 → cursor batches [1,2,3] then [10,11,12]
    // File B: [5, 6, 7]
    //
    // Expected merge: 1,2,3,5,6,7,10,11,12
    // After TIER 2 drains A batch 0 [1,2,3], A loads batch 1 starting at 10.
    // heap_top from B is 5. Since 10 > 5, A must yield to heap.
    let index = "test_tier2_yield_after_batch_boundary";
    register_small_batch_size(index, 3);

    let batch_a = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 10, 11, 12]))],
    ).unwrap();
    let batch_b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![5, 6, 7]))],
    ).unwrap();

    let tmp = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &batch_a);
    write_parquet(&file_b, &batch_b);

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(
        &[file_a, file_b], &output, index,
        &["v".into()], &[false], &[false],
    ).unwrap();

    let vals = read_all_int64(&output, "v");
    assert_eq!(vals, vec![1, 2, 3, 5, 6, 7, 10, 11, 12]);
}

/// Three files with interleaved batch boundaries to force multiple yield events.
#[test]
fn test_tier2_yield_multiple_cursors() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
    ]));

    // File A: [1, 2, 20, 21] with batch_size=2 → cursor batches [1,2] and [20,21]
    // File B: [3, 4, 30, 31] with batch_size=2 → cursor batches [3,4] and [30,31]
    // File C: [10, 11, 12]
    //
    // Expected: 1,2,3,4,10,11,12,20,21,30,31
    let index = "test_tier2_yield_multiple_cursors";
    register_small_batch_size(index, 2);

    let batch_a = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 20, 21]))],
    ).unwrap();
    let batch_b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![3, 4, 30, 31]))],
    ).unwrap();
    let batch_c = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 11, 12]))],
    ).unwrap();

    let tmp = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    let file_c = tmp.path().join("c.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &batch_a);
    write_parquet(&file_b, &batch_b);
    write_parquet(&file_c, &batch_c);

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(
        &[file_a, file_b, file_c], &output, index,
        &["v".into()], &[false], &[false],
    ).unwrap();

    let vals = read_all_int64(&output, "v");
    assert_eq!(vals, vec![1, 2, 3, 4, 10, 11, 12, 20, 21, 30, 31]);
}

/// Descending sort with yield-to-heap after batch boundary.
#[test]
fn test_tier2_yield_descending() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
    ]));

    // File A: [12, 11, 10, 3, 2, 1] with batch_size=3 → cursor batches [12,11,10] and [3,2,1]
    // File B: [7, 6, 5]
    //
    // Descending merge expected: 12,11,10,7,6,5,3,2,1
    // After TIER 2 drains A batch 0 [12,11,10], A loads batch 1 starting at 3.
    // heap_top from B is 7. In descending, 3 < 7 means A should yield.
    let index = "test_tier2_yield_descending";
    register_small_batch_size(index, 3);

    let batch_a = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![12, 11, 10, 3, 2, 1]))],
    ).unwrap();
    let batch_b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![7, 6, 5]))],
    ).unwrap();

    let tmp = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &batch_a);
    write_parquet(&file_b, &batch_b);

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(
        &[file_a, file_b], &output, index,
        &["v".into()], &[true], &[false],
    ).unwrap();

    let vals = read_all_int64(&output, "v");
    assert_eq!(vals, vec![12, 11, 10, 7, 6, 5, 3, 2, 1]);
}

/// Edge case: new batch starts exactly equal to heap top (should NOT yield).
#[test]
fn test_tier2_no_yield_when_equal_to_heap_top() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
    ]));

    // File A: [1, 2, 5, 6] with batch_size=2 → cursor batches [1,2] and [5,6]
    // File B: [5, 7, 8]
    //
    // After TIER 2 drains A batch 0 [1,2], A loads batch 1 starting at 5.
    // heap_top from B is also 5. Since 5 == 5 (not Greater), A should NOT yield.
    // Expected: 1,2,5,5,6,7,8
    let index = "test_tier2_no_yield_when_equal_to_heap_top";
    register_small_batch_size(index, 2);

    let batch_a = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 5, 6]))],
    ).unwrap();
    let batch_b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![5, 7, 8]))],
    ).unwrap();

    let tmp = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &batch_a);
    write_parquet(&file_b, &batch_b);

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(
        &[file_a, file_b], &output, index,
        &["v".into()], &[false], &[false],
    ).unwrap();

    let vals = read_all_int64(&output, "v");
    assert_eq!(vals, vec![1, 2, 5, 5, 6, 7, 8]);
}

/// Stress test: many small batches forcing repeated yield-to-heap transitions.
#[test]
fn test_tier2_yield_many_small_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
    ]));

    // File A: [1,2, 11,12, 21,22, 31,32, 41,42] with batch_size=2
    // File B: [5,6, 15,16, 25,26, 35,36, 45,46] with batch_size=2
    //
    // This forces many yield events as each batch boundary in A jumps past B's
    // current position and vice versa.
    let index = "test_tier2_yield_many_small_batches";
    register_small_batch_size(index, 2);

    let a_vals: Vec<i64> = (0..5).flat_map(|i| vec![i * 10 + 1, i * 10 + 2]).collect();
    let b_vals: Vec<i64> = (0..5).flat_map(|i| vec![i * 10 + 5, i * 10 + 6]).collect();

    let batch_a = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(a_vals.clone()))],
    ).unwrap();
    let batch_b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(b_vals.clone()))],
    ).unwrap();

    let tmp = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &batch_a);
    write_parquet(&file_b, &batch_b);

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(
        &[file_a, file_b], &output, index,
        &["v".into()], &[false], &[false],
    ).unwrap();

    let vals = read_all_int64(&output, "v");
    let mut expected: Vec<i64> = a_vals.iter().chain(b_vals.iter()).copied().collect();
    expected.sort();
    assert_eq!(vals, expected);
}

// ─── Default batch size / row-group size tests ───────────────────────────────
// Uses default settings (merge_batch_size=100_000, row_group_max_rows=1_000_000).
// Writes 1.4M rows across 2 files to force exactly 2 row groups.
// Verifies:
//   - Exact row group sizes (first=1_000_000, second=400_000)
//   - Every single output value matches the expected sorted sequence
//   - RG boundary: last value of RG0 < first value of RG1 (ascending)
//   - Exact null count and null positions for nulls_first/nulls_last

const RG_MAX: i64     = 1_000_000;
const ROWS_PER_FILE: i64 = 700_000; // 2 files → 1_400_000 total → RG0=1_000_000, RG1=400_000

/// Returns (row_group_sizes, per_rg_first_value, per_rg_last_value) for a nullable Int64 column.
fn inspect_row_groups(path: &str, col: &str) -> (Vec<i64>, Vec<Option<i64>>, Vec<Option<i64>>) {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    let meta   = reader.metadata();
    let n_rg   = meta.num_row_groups();
    let sizes: Vec<i64> = (0..n_rg).map(|i| meta.row_group(i).num_rows()).collect();

    // Read all values in order, then slice per RG
    let file    = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let rdr     = builder.build().unwrap();
    let mut all: Vec<Option<i64>> = Vec::new();
    for batch in rdr {
        let batch = batch.unwrap();
        let idx   = batch.schema().index_of(col).unwrap();
        let arr   = batch.column(idx).as_primitive::<arrow::datatypes::Int64Type>();
        for i in 0..arr.len() {
            all.push(if arr.is_null(i) { None } else { Some(arr.value(i)) });
        }
    }

    let mut firsts = Vec::new();
    let mut lasts  = Vec::new();
    let mut offset = 0usize;
    for &sz in &sizes {
        let end = offset + sz as usize;
        firsts.push(all[offset]);
        lasts.push(all[end - 1]);
        offset = end;
    }
    (sizes, firsts, lasts)
}

/// Read every value of a nullable Int64 column.
fn read_col_i64(path: &str, col: &str) -> Vec<Option<i64>> {
    let file = File::open(path).unwrap();
    let rdr  = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    let mut vals = Vec::new();
    for batch in rdr {
        let batch = batch.unwrap();
        let idx   = batch.schema().index_of(col).unwrap();
        let arr   = batch.column(idx).as_primitive::<arrow::datatypes::Int64Type>();
        for i in 0..arr.len() {
            vals.push(if arr.is_null(i) { None } else { Some(arr.value(i)) });
        }
    }
    vals
}

#[test]
fn test_default_settings_ascending_nulls_last() {
    // File A: evens  [0, 2, 4, ..., (ROWS_PER_FILE-1)*2]   — 700_000 values
    // File B: odds   [1, 3, 5, ..., (ROWS_PER_FILE-1)*2+1] — 700_000 values
    // Merged ascending: 0, 1, 2, 3, ..., 1_399_999
    // RG0 = rows 0..999_999  → values 0..999_999
    // RG1 = rows 1_000_000..1_399_999 → values 1_000_000..1_399_999
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let a_vals: Vec<i64> = (0..ROWS_PER_FILE).map(|i| i * 2).collect();
    let b_vals: Vec<i64> = (0..ROWS_PER_FILE).map(|i| i * 2 + 1).collect();

    let tmp    = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(a_vals))]).unwrap());
    write_parquet(&file_b, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(b_vals))]).unwrap());

    let output = tmp.path().join("out.parquet").to_string_lossy().to_string();
    merge_sorted(&[file_a, file_b], &output, "test_default_asc_nulls_last",
        &["v".into()], &[false], &[false]).unwrap();

    // ── Row group structure ──────────────────────────────────────────────
    // With default batch_size=100_000 which divides evenly into RG_MAX=1_000_000,
    // RG0 is exactly RG_MAX rows. If batch_size didn't divide evenly, RG0 could
    // overshoot (see test_rg_size_overshoots_when_batch_straddles_threshold).
    let (rg_sizes, rg_firsts, rg_lasts) = inspect_row_groups(&output, "v");
    assert_eq!(rg_sizes.len(), 2, "expected exactly 2 row groups");
    assert_eq!(rg_sizes[0], RG_MAX,                    "RG0 size: exact because batch_size=100_000 divides RG_MAX");
    assert_eq!(rg_sizes[1], ROWS_PER_FILE * 2 - RG_MAX, "RG1 size");
    // RG boundary: last of RG0 must be strictly less than first of RG1
    assert_eq!(rg_lasts[0],  Some(RG_MAX - 1),         "RG0 last value");
    assert_eq!(rg_firsts[1], Some(RG_MAX),              "RG1 first value");

    // ── Exact value check ────────────────────────────────────────────────
    let vals = read_col_i64(&output, "v");
    assert_eq!(vals.len() as i64, ROWS_PER_FILE * 2);
    for (i, v) in vals.iter().enumerate() {
        assert_eq!(*v, Some(i as i64), "wrong value at row {}", i);
    }
}

#[test]
fn test_default_settings_descending_nulls_last() {
    // File A: evens descending  [(ROWS_PER_FILE-1)*2, ..., 2, 0]
    // File B: odds  descending  [(ROWS_PER_FILE-1)*2+1, ..., 3, 1]
    // Merged descending: 1_399_999, 1_399_998, ..., 1, 0
    // RG0 last  value = 1_399_999 - (RG_MAX-1) = 400_000
    // RG0 first value = 1_399_999
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let total  = ROWS_PER_FILE * 2;
    let a_vals: Vec<i64> = (0..ROWS_PER_FILE).rev().map(|i| i * 2).collect();
    let b_vals: Vec<i64> = (0..ROWS_PER_FILE).rev().map(|i| i * 2 + 1).collect();

    let tmp    = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(a_vals))]).unwrap());
    write_parquet(&file_b, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(b_vals))]).unwrap());

    let output = tmp.path().join("out.parquet").to_string_lossy().to_string();
    merge_sorted(&[file_a, file_b], &output, "test_default_desc_nulls_last",
        &["v".into()], &[true], &[false]).unwrap();

    // ── Row group structure ──────────────────────────────────────────────
    let (rg_sizes, rg_firsts, rg_lasts) = inspect_row_groups(&output, "v");
    assert_eq!(rg_sizes.len(), 2);
    assert_eq!(rg_sizes[0], RG_MAX);
    assert_eq!(rg_sizes[1], total - RG_MAX);
    // Descending: RG0 starts at max, ends at total-RG_MAX; RG1 starts one below that
    assert_eq!(rg_firsts[0], Some(total - 1),        "RG0 first value");
    assert_eq!(rg_lasts[0],  Some(total - RG_MAX),   "RG0 last value");
    assert_eq!(rg_firsts[1], Some(total - RG_MAX - 1), "RG1 first value");
    assert_eq!(rg_lasts[1],  Some(0),                "RG1 last value");

    // ── Exact value check ────────────────────────────────────────────────
    let vals = read_col_i64(&output, "v");
    assert_eq!(vals.len() as i64, total);
    for (i, v) in vals.iter().enumerate() {
        assert_eq!(*v, Some(total - 1 - i as i64), "wrong value at row {}", i);
    }
}

#[test]
fn test_default_settings_ascending_nulls_first() {
    // File A: [null * half, 0, 2, 4, ..., (half-1)*2]
    // File B: [null * half, 1, 3, 5, ..., (half-1)*2+1]
    // nulls_first=true → merged: null*(half*2), 0, 1, 2, ..., (half-1)*2+1
    //
    // half = 350_000, so:
    //   total nulls  = 700_000
    //   total non-nulls = 700_000  (values 0..699_999)
    //   total rows   = 1_400_000
    //
    // RG0 = 1_000_000 rows: all 700_000 nulls + first 300_000 non-nulls (0..299_999)
    // RG1 =   400_000 rows: remaining non-nulls (300_000..699_999)
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let half   = ROWS_PER_FILE / 2; // 350_000
    let total  = ROWS_PER_FILE * 2; // 1_400_000
    let total_nulls    = half * 2;  // 700_000
    let total_nonnulls = half * 2;  // 700_000  (values 0..699_999)

    let a_vals: Vec<Option<i64>> = (0..half).map(|_| None)
        .chain((0..half).map(|i| Some(i * 2)))
        .collect();
    let b_vals: Vec<Option<i64>> = (0..half).map(|_| None)
        .chain((0..half).map(|i| Some(i * 2 + 1)))
        .collect();

    let tmp    = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(a_vals))]).unwrap());
    write_parquet(&file_b, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(b_vals))]).unwrap());

    let output = tmp.path().join("out.parquet").to_string_lossy().to_string();
    merge_sorted(&[file_a, file_b], &output, "test_default_asc_nulls_first",
        &["v".into()], &[false], &[true]).unwrap();

    // ── Row group structure ──────────────────────────────────────────────
    let (rg_sizes, rg_firsts, rg_lasts) = inspect_row_groups(&output, "v");
    assert_eq!(rg_sizes.len(), 2);
    assert_eq!(rg_sizes[0], RG_MAX);
    assert_eq!(rg_sizes[1], total - RG_MAX);
    // RG0: all nulls first, then non-nulls 0..(RG_MAX - total_nulls - 1)
    assert_eq!(rg_firsts[0], None, "RG0 should start with null");
    let rg0_last_nonnull = RG_MAX - total_nulls - 1; // = 1_000_000 - 700_000 - 1 = 299_999
    assert_eq!(rg_lasts[0], Some(rg0_last_nonnull), "RG0 last value");
    // RG1: non-nulls (rg0_last_nonnull+1)..total_nonnulls-1
    assert_eq!(rg_firsts[1], Some(rg0_last_nonnull + 1), "RG1 first value");
    assert_eq!(rg_lasts[1],  Some(total_nonnulls - 1),   "RG1 last value");

    // ── Exact value check ────────────────────────────────────────────────
    let vals = read_col_i64(&output, "v");
    assert_eq!(vals.len() as i64, total);
    // First total_nulls rows must all be None
    for i in 0..total_nulls as usize {
        assert_eq!(vals[i], None, "expected null at row {}", i);
    }
    // Remaining rows must be exactly 0, 1, 2, ..., total_nonnulls-1
    for i in 0..total_nonnulls as usize {
        assert_eq!(vals[total_nulls as usize + i], Some(i as i64),
            "wrong non-null value at row {}", total_nulls as usize + i);
    }
}

// ─── Large-file edge case tests ───────────────────────────────────────────────────

/// Single file with >1M rows: no merge needed, just copy through.
/// Verifies TIER 1 drain handles multiple batch boundaries and produces
/// correct RG splits with exact values.
#[test]
fn test_single_large_file_passthrough() {
    // 1_500_000 rows in one file → RG0=1_000_000, RG1=500_000
    let n: i64 = 1_500_000;
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let vals: Vec<i64> = (0..n).collect();

    let tmp    = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))]).unwrap());

    let output = tmp.path().join("out.parquet").to_string_lossy().to_string();
    merge_sorted(&[file_a], &output, "test_single_large_file",
        &["v".into()], &[false], &[false]).unwrap();

    let (rg_sizes, rg_firsts, rg_lasts) = inspect_row_groups(&output, "v");
    assert_eq!(rg_sizes.len(), 2);
    assert_eq!(rg_sizes[0], RG_MAX);
    assert_eq!(rg_sizes[1], n - RG_MAX);
    assert_eq!(rg_firsts[0], Some(0));
    assert_eq!(rg_lasts[0],  Some(RG_MAX - 1));
    assert_eq!(rg_firsts[1], Some(RG_MAX));
    assert_eq!(rg_lasts[1],  Some(n - 1));

    let out_vals = read_col_i64(&output, "v");
    assert_eq!(out_vals.len() as i64, n);
    for (i, v) in out_vals.iter().enumerate() {
        assert_eq!(*v, Some(i as i64), "wrong value at row {}", i);
    }
}

/// Heavily skewed file sizes: file A has 1.2M rows, file B has 200K rows.
/// File B exhausts while file A is mid-batch, forcing TIER 1 drain of A
/// with row_idx > 0 (mid-batch position from a prior TIER 3 operation).
#[test]
fn test_skewed_file_sizes_large_small() {
    // File A: evens 0, 2, 4, ..., 2_399_998  (1_200_000 values)
    // File B: odds  1, 3, 5, ...,   399_999  (200_000 values, exhausts early)
    // Merged: 0,1,2,3,...,399_999, 400_000,400_002,...,2_399_998
    let large: i64 = 1_200_000;
    let small: i64 =   200_000;
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

    let a_vals: Vec<i64> = (0..large).map(|i| i * 2).collect();
    let b_vals: Vec<i64> = (0..small).map(|i| i * 2 + 1).collect();

    let tmp    = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(a_vals))]).unwrap());
    write_parquet(&file_b, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(b_vals))]).unwrap());

    let output = tmp.path().join("out.parquet").to_string_lossy().to_string();
    merge_sorted(&[file_a, file_b], &output, "test_skewed_large_small",
        &["v".into()], &[false], &[false]).unwrap();

    let total = large + small;
    let out_vals = read_col_i64(&output, "v");
    assert_eq!(out_vals.len() as i64, total);

    // Build expected: interleaved evens/odds up to 399_999, then remaining evens
    let mut expected: Vec<i64> = (0..small).map(|i| i * 2).collect();     // evens 0..399_998
    let odds: Vec<i64>         = (0..small).map(|i| i * 2 + 1).collect(); // odds  1..399_999
    expected.extend(odds);
    expected.sort();
    // After interleaved section: remaining evens from 400_000 to 2_399_998
    expected.extend((small..large).map(|i| i * 2));

    for (i, (got, exp)) in out_vals.iter().zip(expected.iter()).enumerate() {
        assert_eq!(*got, Some(*exp), "wrong value at row {}", i);
    }
}

/// Three files where the middle file exhausts first, forcing a TIER 1
/// transition mid-merge with two remaining cursors still active.
#[test]
fn test_three_files_middle_exhausts_first() {
    // File A: [0, 3, 6, ..., 299_997]  (100_000 values, step 3)
    // File B: [1, 4, 7, ...,  29_998]  ( 10_000 values, step 3 — exhausts first)
    // File C: [2, 5, 8, ..., 299_999]  (100_000 values, step 3)
    // Merged: 0,1,2,3,4,5,...,29_998,29_999(missing from B),30_000,...
    // After B exhausts, A and C continue interleaving.
    let a_count: i64 = 100_000;
    let b_count: i64 =  10_000;
    let c_count: i64 = 100_000;
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

    let a_vals: Vec<i64> = (0..a_count).map(|i| i * 3).collect();
    let b_vals: Vec<i64> = (0..b_count).map(|i| i * 3 + 1).collect();
    let c_vals: Vec<i64> = (0..c_count).map(|i| i * 3 + 2).collect();

    let tmp    = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    let file_c = tmp.path().join("c.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(a_vals.clone()))]).unwrap());
    write_parquet(&file_b, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(b_vals.clone()))]).unwrap());
    write_parquet(&file_c, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(c_vals.clone()))]).unwrap());

    let output = tmp.path().join("out.parquet").to_string_lossy().to_string();
    merge_sorted(&[file_a, file_b, file_c], &output, "test_three_files_middle_exhausts",
        &["v".into()], &[false], &[false]).unwrap();

    let total = a_count + b_count + c_count;
    let out_vals = read_col_i64(&output, "v");
    assert_eq!(out_vals.len() as i64, total);

    let mut expected: Vec<i64> = a_vals.iter().chain(b_vals.iter()).chain(c_vals.iter()).copied().collect();
    expected.sort();
    for (i, (got, exp)) in out_vals.iter().zip(expected.iter()).enumerate() {
        assert_eq!(*got, Some(*exp), "wrong value at row {}", i);
    }
}

/// All-duplicate values: every row in every file has the same sort key.
/// Verifies total row count is preserved and no rows are dropped or duplicated.
#[test]
fn test_all_duplicate_sort_keys_large() {
    // 3 files × 500_000 rows, all with value=42
    // Total: 1_500_000 rows → 2 RGs
    let n: i64 = 500_000;
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let vals: Vec<i64> = vec![42i64; n as usize];

    let tmp    = tempdir().unwrap();
    let files: Vec<String> = (0..3).map(|i| {
        let p = tmp.path().join(format!("{}.parquet", i)).to_string_lossy().to_string();
        write_parquet(&p, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals.clone()))]).unwrap());
        p
    }).collect();

    let output = tmp.path().join("out.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test_all_dupes_large",
        &["v".into()], &[false], &[false]).unwrap();

    let total = n * 3;
    let (rg_sizes, rg_firsts, rg_lasts) = inspect_row_groups(&output, "v");
    assert_eq!(rg_sizes.len(), 2);
    assert_eq!(rg_sizes[0], RG_MAX);
    assert_eq!(rg_sizes[1], total - RG_MAX);
    assert!(rg_firsts.iter().all(|v| *v == Some(42)));
    assert!(rg_lasts.iter().all(|v| *v == Some(42)));

    let out_vals = read_col_i64(&output, "v");
    assert_eq!(out_vals.len() as i64, total);
    assert!(out_vals.iter().all(|v| *v == Some(42)));
}

/// File sizes that are not multiples of batch_size (100_000):
/// 150_001 + 149_999 = 300_000 rows. Verifies no off-by-one at batch boundaries.
#[test]
fn test_non_multiple_of_batch_size() {
    let a_count: i64 = 150_001;
    let b_count: i64 = 149_999;
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

    // File A: evens, File B: odds — interleaved merge = 0,1,2,...
    // But counts differ so the last few values come only from one file.
    let a_vals: Vec<i64> = (0..a_count).map(|i| i * 2).collect();
    let b_vals: Vec<i64> = (0..b_count).map(|i| i * 2 + 1).collect();

    let tmp    = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let file_b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(a_vals.clone()))]).unwrap());
    write_parquet(&file_b, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(b_vals.clone()))]).unwrap());

    let output = tmp.path().join("out.parquet").to_string_lossy().to_string();
    merge_sorted(&[file_a, file_b], &output, "test_non_multiple_batch",
        &["v".into()], &[false], &[false]).unwrap();

    let total = a_count + b_count;
    let out_vals = read_col_i64(&output, "v");
    assert_eq!(out_vals.len() as i64, total);

    let mut expected: Vec<i64> = a_vals.iter().chain(b_vals.iter()).copied().collect();
    expected.sort();
    for (i, (got, exp)) in out_vals.iter().zip(expected.iter()).enumerate() {
        assert_eq!(*got, Some(*exp), "wrong value at row {}", i);
    }
}

/// Verifies that RG size can exceed RG_MAX when a pushed slice straddles the
/// flush threshold (output_row_count >= output_flush_rows fires AFTER the
/// batch is buffered, so the RG contains all buffered rows including the
/// overshoot).
///
/// Setup: batch_size=100_001 so each cursor batch is 100_001 rows.
/// output_flush_rows=1_000_000. After 9 batches: 900_009 rows buffered.
/// 10th batch pushes 100_001 more → 1_000_010 rows → flush with 1_000_010.
/// RG0 will be 1_000_010, not exactly 1_000_000.
#[test]
fn test_rg_size_overshoots_when_batch_straddles_threshold() {
    // Use a batch_size that doesn't divide evenly into RG_MAX
    let batch_size: usize = 100_001;
    let index = "test_rg_overshoot";
    SETTINGS_STORE.insert(index.to_string(), NativeSettings {
        merge_batch_size: Some(batch_size),
        ..Default::default()
    });

    // One file with 2_000_002 rows (20 full batches of 100_001)
    // RG0 flushes after 10 batches = 1_000_010 rows (overshoots by 10)
    // RG1 gets the remaining 1_000_010 - wait, 20 * 100_001 = 2_000_020 rows
    // RG0: 1_000_010, RG1: 1_000_010
    let n: i64 = (batch_size * 20) as i64; // 2_000_020
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let vals: Vec<i64> = (0..n).collect();

    let tmp    = tempdir().unwrap();
    let file_a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    write_parquet(&file_a, &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))]).unwrap());

    let output = tmp.path().join("out.parquet").to_string_lossy().to_string();
    merge_sorted(&[file_a], &output, index,
        &["v".into()], &[false], &[false]).unwrap();

    let (rg_sizes, rg_firsts, rg_lasts) = inspect_row_groups(&output, "v");
    assert_eq!(rg_sizes.len(), 2);

    // Each RG should be exactly 10 * batch_size = 1_000_010 (overshoots RG_MAX by 10)
    let expected_rg_size = (batch_size * 10) as i64; // 1_000_010
    assert_eq!(rg_sizes[0], expected_rg_size,
        "RG0 size should be {} (overshoots RG_MAX={} by {}), got {}.",
        expected_rg_size, RG_MAX, expected_rg_size - RG_MAX, rg_sizes[0]);
    assert_eq!(rg_sizes[1], expected_rg_size, "RG1 size");
    assert_eq!(rg_sizes.iter().sum::<i64>(), n);

    // Exact boundary values
    assert_eq!(rg_firsts[0], Some(0));
    assert_eq!(rg_lasts[0],  Some(expected_rg_size - 1));
    assert_eq!(rg_firsts[1], Some(expected_rg_size));
    assert_eq!(rg_lasts[1],  Some(n - 1));

    // Exact value check
    let out_vals = read_col_i64(&output, "v");
    assert_eq!(out_vals.len() as i64, n);
    for (i, v) in out_vals.iter().enumerate() {
        assert_eq!(*v, Some(i as i64), "wrong value at row {}", i);
    }
}

