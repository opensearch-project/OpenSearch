/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! End-to-end tests for the live-docs path of merge_sorted and merge_unsorted.
//! Inputs are tiny self-contained Parquet files; outputs are read back and the
//! surviving rows compared against expectations.

use std::fs::File;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use opensearch_parquet_format::merge::{merge_sorted, merge_unsorted};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::tempdir;

// ─── Helpers ────────────────────────────────────────────────────────────────

fn write_parquet(path: &str, batch: &RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn read_int64_col(path: &str, col: &str) -> Vec<i64> {
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

fn count_rows(path: &str) -> usize {
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    reader.map(|b| b.unwrap().num_rows()).sum()
}

/// Pack alive-row positions into a Lucene-layout bitmap.
fn pack_bits(num_rows: usize, alive: &[usize]) -> Vec<u64> {
    let words = (num_rows + 63) / 64;
    let mut bits = vec![0u64; words];
    for &row in alive {
        bits[row / 64] |= 1u64 << (row % 64);
    }
    bits
}

fn write_int64_file(path: &str, vals: Vec<i64>) {
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals))]).unwrap();
    write_parquet(path, &batch);
}

// ─── Sorted merge ───────────────────────────────────────────────────────────

#[test]
fn sorted_merge_with_no_deletes_returns_full_output() {
    let tmp = tempdir().unwrap();
    let f1 = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let f2 = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_int64_file(&f1, vec![1, 3, 5]);
    write_int64_file(&f2, vec![2, 4, 6]);

    let out = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(&[f1, f2], &out, "test", &["val".into()], &[false], &[false], &[None, None]).unwrap();

    let vals = read_int64_col(&out, "val");
    assert_eq!(vals, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn sorted_merge_drops_deleted_rows() {
    let tmp = tempdir().unwrap();
    let f1 = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let f2 = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_int64_file(&f1, vec![1, 3, 5]);
    write_int64_file(&f2, vec![2, 4, 6]);

    // Drop row 1 from each file (i.e. value 3 from f1, value 4 from f2).
    let f1_alive = pack_bits(3, &[0, 2]);
    let f2_alive = pack_bits(3, &[0, 2]);

    let out = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(
        &[f1, f2],
        &out,
        "test",
        &["val".into()],
        &[false],
        &[false],
        &[Some(f1_alive), Some(f2_alive)],
    )
    .unwrap();

    let vals = read_int64_col(&out, "val");
    assert_eq!(vals, vec![1, 2, 5, 6]);
    assert_eq!(count_rows(&out), 4);
}

#[test]
fn sorted_merge_with_one_file_fully_dead() {
    let tmp = tempdir().unwrap();
    let f1 = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let f2 = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_int64_file(&f1, vec![1, 3, 5]);
    write_int64_file(&f2, vec![2, 4, 6]);

    let f1_all_dead = pack_bits(3, &[]);

    let out = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(
        &[f1, f2],
        &out,
        "test",
        &["val".into()],
        &[false],
        &[false],
        &[Some(f1_all_dead), None],
    )
    .unwrap();

    let vals = read_int64_col(&out, "val");
    assert_eq!(vals, vec![2, 4, 6]);
}

#[test]
fn sorted_merge_drops_first_row_so_initial_heap_value_is_alive() {
    let tmp = tempdir().unwrap();
    let f1 = tmp.path().join("a.parquet").to_string_lossy().to_string();
    write_int64_file(&f1, vec![1, 3, 5]);

    // Drop row 0 (val=1). Initial heap value must be 3, not 1.
    let alive = pack_bits(3, &[1, 2]);

    let out = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(
        &[f1],
        &out,
        "test",
        &["val".into()],
        &[false],
        &[false],
        &[Some(alive)],
    )
    .unwrap();

    let vals = read_int64_col(&out, "val");
    assert_eq!(vals, vec![3, 5]);
}

#[test]
fn sorted_merge_with_padding_treats_extra_inputs_as_all_alive() {
    let tmp = tempdir().unwrap();
    let f1 = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let f2 = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_int64_file(&f1, vec![1, 3, 5]);
    write_int64_file(&f2, vec![2, 4, 6]);

    // Provide live-docs for f1 only; f2 gets padded to None ⇒ all alive.
    let f1_alive = pack_bits(3, &[0, 1, 2]);

    let out = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_sorted(
        &[f1, f2],
        &out,
        "test",
        &["val".into()],
        &[false],
        &[false],
        &[Some(f1_alive)],
    )
    .unwrap();

    let vals = read_int64_col(&out, "val");
    assert_eq!(vals, vec![1, 2, 3, 4, 5, 6]);
}

// ─── Unsorted merge ─────────────────────────────────────────────────────────

#[test]
fn unsorted_merge_with_no_deletes_returns_full_output() {
    let tmp = tempdir().unwrap();
    let f1 = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let f2 = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_int64_file(&f1, vec![10, 20, 30]);
    write_int64_file(&f2, vec![40, 50]);

    let out = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_unsorted(&[f1, f2], &out, "test", &[None, None]).unwrap();

    let vals = read_int64_col(&out, "val");
    assert_eq!(vals, vec![10, 20, 30, 40, 50]);
}

#[test]
fn unsorted_merge_drops_deleted_rows() {
    let tmp = tempdir().unwrap();
    let f1 = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let f2 = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_int64_file(&f1, vec![10, 20, 30]);
    write_int64_file(&f2, vec![40, 50]);

    // Keep rows 0 and 2 from f1 (values 10, 30); keep row 1 from f2 (value 50).
    let f1_alive = pack_bits(3, &[0, 2]);
    let f2_alive = pack_bits(2, &[1]);

    let out = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_unsorted(&[f1, f2], &out, "test", &[Some(f1_alive), Some(f2_alive)]).unwrap();

    let vals = read_int64_col(&out, "val");
    assert_eq!(vals, vec![10, 30, 50]);
}

#[test]
fn unsorted_merge_skips_entirely_dead_input_file() {
    let tmp = tempdir().unwrap();
    let f1 = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let f2 = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_int64_file(&f1, vec![10, 20, 30]);
    write_int64_file(&f2, vec![40, 50]);

    let f1_all_dead = pack_bits(3, &[]);

    let out = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_unsorted(&[f1, f2], &out, "test", &[Some(f1_all_dead), None]).unwrap();

    let vals = read_int64_col(&out, "val");
    assert_eq!(vals, vec![40, 50]);
}

#[test]
fn unsorted_merge_assigns_sequential_row_ids_after_deletes() {
    let tmp = tempdir().unwrap();
    let f1 = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let f2 = tmp.path().join("b.parquet").to_string_lossy().to_string();
    write_int64_file(&f1, vec![10, 20, 30, 40]);
    write_int64_file(&f2, vec![50, 60]);

    // Drop two rows; the merger writes sequential __row_id__ across surviving rows.
    let f1_alive = pack_bits(4, &[0, 2]);
    let f2_alive = pack_bits(2, &[1]);

    let out = tmp.path().join("merged.parquet").to_string_lossy().to_string();
    merge_unsorted(&[f1, f2], &out, "test", &[Some(f1_alive), Some(f2_alive)]).unwrap();

    let row_ids = read_int64_col(&out, "__row_id__");
    assert_eq!(row_ids, vec![0, 1, 2]);
}
