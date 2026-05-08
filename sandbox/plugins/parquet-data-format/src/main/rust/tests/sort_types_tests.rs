/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Tests for merge_sorted across all supported sort column types:
//! Int64, Int32, Float64, Float32, Utf8, and multi-column combinations.

use std::fs::File;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use opensearch_parquet_format::merge::merge_sorted;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::tempdir;

/// Write a single RecordBatch to a new Parquet file.
fn write_parquet(path: &str, batch: &RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

/// Read all values of a typed primitive column from a Parquet file.
fn read_primitive_col<T: arrow::datatypes::ArrowPrimitiveType>(
    path: &str,
    col_name: &str,
) -> Vec<Option<T::Native>> {
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut vals = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let idx = batch.schema().index_of(col_name).unwrap();
        let col = batch.column(idx).as_primitive::<T>();
        for i in 0..col.len() {
            if col.is_null(i) {
                vals.push(None);
            } else {
                vals.push(Some(col.value(i)));
            }
        }
    }
    vals
}

/// Read all string values from a Utf8 column.
fn read_string_col(path: &str, col_name: &str) -> Vec<Option<String>> {
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut vals = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let idx = batch.schema().index_of(col_name).unwrap();
        let col = batch.column(idx).as_string::<i32>();
        for i in 0..col.len() {
            if col.is_null(i) {
                vals.push(None);
            } else {
                vals.push(Some(col.value(i).to_string()));
            }
        }
    }
    vals
}

/// Count rows in a Parquet file.
fn count_rows(path: &str) -> usize {
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    reader.map(|b| b.unwrap().num_rows()).sum()
}

// ─── Int64 ──────────────────────────────────────────────────────────────────

#[test]
fn test_merge_sort_by_int64() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int64, false),
    ]));

    // File A: [1, 3, 5]  File B: [2, 4, 6]  File C: [0, 7, 8]
    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 3, 5]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![2, 4, 6]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![0, 7, 8]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[false]).unwrap();

    let vals = read_primitive_col::<arrow::datatypes::Int64Type>(&output, "val");
    let vals: Vec<i64> = vals.into_iter().map(|v| v.unwrap()).collect();
    assert_eq!(vals, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(count_rows(&output), 9);
}

// ─── Int64 with nulls ───────────────────────────────────────────────────────

#[test]
fn test_merge_sort_by_int64_with_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int64, true),
    ]));

    // Each file pre-sorted: nulls last, then ascending
    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![Some(1), Some(5), None]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![Some(2), Some(4), None]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[false]).unwrap();

    let vals = read_primitive_col::<arrow::datatypes::Int64Type>(&output, "val");
    assert_eq!(vals, vec![Some(1), Some(2), Some(4), Some(5), None, None]);
}

// ─── Int32 ──────────────────────────────────────────────────────────────────

#[test]
fn test_merge_sort_by_int32() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int32, false),
    ]));

    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![10, 30]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![20, 40]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[false]).unwrap();

    let vals = read_primitive_col::<arrow::datatypes::Int32Type>(&output, "val");
    let vals: Vec<i32> = vals.into_iter().map(|v| v.unwrap()).collect();
    assert_eq!(vals, vec![10, 20, 30, 40]);
}

// ─── Float64 ────────────────────────────────────────────────────────────────

#[test]
fn test_merge_sort_by_float64() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Float64, false),
    ]));

    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(vec![1.1, 3.3, 5.5]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(vec![2.2, 4.4, 6.6]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[false]).unwrap();

    let vals = read_primitive_col::<arrow::datatypes::Float64Type>(&output, "val");
    let vals: Vec<f64> = vals.into_iter().map(|v| v.unwrap()).collect();
    assert_eq!(vals, vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6]);
}

// ─── Float64 with nulls ─────────────────────────────────────────────────────

#[test]
fn test_merge_sort_by_float64_with_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Float64, true),
    ]));

    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(vec![None, Some(1.5), Some(4.0)]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(vec![None, Some(2.5), Some(3.0)]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[true]).unwrap();

    let vals = read_primitive_col::<arrow::datatypes::Float64Type>(&output, "val");
    assert_eq!(vals, vec![None, None, Some(1.5), Some(2.5), Some(3.0), Some(4.0)]);
}

// ─── Float32 ────────────────────────────────────────────────────────────────

#[test]
fn test_merge_sort_by_float32() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Float32, false),
    ]));

    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Float32Array::from(vec![1.0f32, 3.0]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Float32Array::from(vec![2.0f32, 4.0]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[false]).unwrap();

    let vals = read_primitive_col::<arrow::datatypes::Float32Type>(&output, "val");
    let vals: Vec<f32> = vals.into_iter().map(|v| v.unwrap()).collect();
    assert_eq!(vals, vec![1.0, 2.0, 3.0, 4.0]);
}

// ─── Float32 with nulls ─────────────────────────────────────────────────────

#[test]
fn test_merge_sort_by_float32_with_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Float32, true),
    ]));

    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Float32Array::from(vec![Some(1.0f32), Some(3.0), None]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Float32Array::from(vec![Some(2.0f32), None, None]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[false]).unwrap();

    let vals = read_primitive_col::<arrow::datatypes::Float32Type>(&output, "val");
    assert_eq!(vals, vec![Some(1.0), Some(2.0), Some(3.0), None, None, None]);
}

// ─── Utf8 (String / keyword) ───────────────────────────────────────────────

#[test]
fn test_merge_sort_by_string() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Utf8, false),
    ]));

    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["apple", "cherry", "fig"]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["banana", "date", "grape"]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[false]).unwrap();

    let vals = read_string_col(&output, "val");
    let vals: Vec<String> = vals.into_iter().map(|v| v.unwrap()).collect();
    assert_eq!(vals, vec!["apple", "banana", "cherry", "date", "fig", "grape"]);
}

// ─── Utf8 with nulls ────────────────────────────────────────────────────────

#[test]
fn test_merge_sort_by_string_with_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Utf8, true),
    ]));

    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(vec![None, Some("banana"), Some("fig")])),
        ]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(vec![None, Some("apple"), Some("cherry")])),
        ]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[true]).unwrap();

    let vals = read_string_col(&output, "val");
    assert_eq!(vals, vec![None, None, Some("apple".into()), Some("banana".into()), Some("cherry".into()), Some("fig".into())]);
}

// ─── Descending sort ────────────────────────────────────────────────────────

#[test]
fn test_merge_sort_descending() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int64, false),
    ]));

    // Each file sorted descending
    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![8, 5, 2]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![7, 4, 1]))]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![9, 6, 3]))]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[true], &[false]).unwrap();

    let vals = read_primitive_col::<arrow::datatypes::Int64Type>(&output, "val");
    let vals: Vec<i64> = vals.into_iter().map(|v| v.unwrap()).collect();
    assert_eq!(vals, vec![9, 8, 7, 6, 5, 4, 3, 2, 1]);
}

// ─── Multi-column: String + Int64 ──────────────────────────────────────────

#[test]
fn test_merge_sort_multi_column_string_and_int() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new("priority", DataType::Int64, false),
    ]));

    // File A: (alpha,1), (alpha,3), (beta,1)
    // File B: (alpha,2), (beta,2), (beta,3)
    // Sorted by (category ASC, priority ASC)
    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(vec!["alpha", "alpha", "beta"])),
            Arc::new(Int64Array::from(vec![1, 3, 1])),
        ]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(vec!["alpha", "beta", "beta"])),
            Arc::new(Int64Array::from(vec![2, 2, 3])),
        ]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(
        &files, &output, "test",
        &["category".into(), "priority".into()],
        &[false, false],
        &[false, false],
    ).unwrap();

    let cats = read_string_col(&output, "category");
    let cats: Vec<String> = cats.into_iter().map(|v| v.unwrap()).collect();
    let pris = read_primitive_col::<arrow::datatypes::Int64Type>(&output, "priority");
    let pris: Vec<i64> = pris.into_iter().map(|v| v.unwrap()).collect();

    assert_eq!(cats, vec!["alpha", "alpha", "alpha", "beta", "beta", "beta"]);
    assert_eq!(pris, vec![1, 2, 3, 1, 2, 3]);
}

// ─── Nulls ──────────────────────────────────────────────────────────────────

#[test]
fn test_merge_sort_with_nulls_first() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int64, true),
    ]));

    // Each file pre-sorted with nulls first, then ascending
    // File A: [null, 2, 5]  File B: [null, 1, 4]
    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![None, Some(2), Some(5)])),
        ]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![None, Some(1), Some(4)])),
        ]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[true]).unwrap();

    let vals = read_primitive_col::<arrow::datatypes::Int64Type>(&output, "val");
    // nulls_first=true → nulls come first, then ascending
    assert_eq!(vals, vec![None, None, Some(1), Some(2), Some(4), Some(5)]);
}

#[test]
fn test_merge_sort_with_nulls_last() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int64, true),
    ]));

    let batches = vec![
        RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(3), None])),
        ]).unwrap(),
        RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![Some(2), None, None])),
        ]).unwrap(),
    ];

    let tmp = tempdir().unwrap();
    let files: Vec<String> = batches.iter().enumerate().map(|(i, b)| {
        let p = tmp.path().join(format!("input_{}.parquet", i));
        let s = p.to_string_lossy().to_string();
        write_parquet(&s, b);
        s
    }).collect();

    let output = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&files, &output, "test", &["val".into()], &[false], &[false]).unwrap();

    let vals = read_primitive_col::<arrow::datatypes::Int64Type>(&output, "val");
    // nulls_first=false → values ascending, then nulls
    assert_eq!(vals, vec![Some(1), Some(2), Some(3), None, None, None]);
}
