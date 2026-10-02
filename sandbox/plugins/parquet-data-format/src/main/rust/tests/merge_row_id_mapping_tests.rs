/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! The row-ID mapping returned by `merge_sorted` must place every source row where the
//! merged file actually wrote it, including rows that sit past the first cursor batch.
//! The secondary (Lucene) format reorders its documents purely from this mapping, so a
//! wrong slot silently pairs a Lucene document with a different Parquet row.

use std::fs::File;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use opensearch_parquet_format::merge::{merge_sorted, MergeOutput};
use opensearch_parquet_format::native_settings::NativeSettings;
use opensearch_parquet_format::writer::SETTINGS_STORE;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

fn register_batch_size(index_name: &str, batch_size: usize) {
    SETTINGS_STORE.insert(
        index_name.to_string(),
        NativeSettings {
            merge_batch_size: Some(batch_size),
            ..Default::default()
        },
    );
}

/// Writes one file of (`k` sort key, `id` unique tag) rows. `k` must already be ascending.
fn write_file(path: &str, keys: &[i64], ids: &[i64]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(Int64Array::from(ids.to_vec())),
        ],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn read_ids(path: &str) -> Vec<i64> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for b in reader {
        let b = b.unwrap();
        let col = b.column(b.schema().index_of("id").unwrap());
        out.extend(
            col.as_primitive::<arrow::datatypes::Int64Type>()
                .values()
                .iter(),
        );
    }
    out
}

/// For every input row, the mapping slot `gen_offset + source_position` must hold the
/// position at which the merged file wrote that row.
fn assert_mapping_matches_output(out: &MergeOutput, inputs: &[Vec<i64>], merged_ids: &[i64]) {
    let mut out_pos = std::collections::HashMap::new();
    for (pos, id) in merged_ids.iter().enumerate() {
        out_pos.insert(*id, pos as i64);
    }
    let total: usize = inputs.iter().map(|v| v.len()).sum();
    assert_eq!(merged_ids.len(), total, "merged row count");
    assert_eq!(out.mapping.len(), total, "mapping length");
    for (file_id, ids) in inputs.iter().enumerate() {
        let offset = out.gen_offsets[file_id] as usize;
        for (src_pos, id) in ids.iter().enumerate() {
            assert_eq!(
                out.mapping[offset + src_pos],
                out_pos[id],
                "file {} source row {} (id {}) mapped to the wrong output row",
                file_id,
                src_pos,
                id
            );
        }
    }
}

fn run(index: &str, batch_size: usize, files: Vec<(Vec<i64>, Vec<i64>)>) {
    register_batch_size(index, batch_size);
    let tmp = tempdir().unwrap();
    let mut paths = Vec::new();
    for (i, (k, id)) in files.iter().enumerate() {
        let p = tmp
            .path()
            .join(format!("in{}.parquet", i))
            .to_string_lossy()
            .to_string();
        write_file(&p, k, id);
        paths.push(p);
    }
    let output = tmp
        .path()
        .join("merged.parquet")
        .to_string_lossy()
        .to_string();
    let out = merge_sorted(
        &paths,
        &output,
        index,
        &["k".into()],
        &[false],
        &[false],
        &[],
        0,
        &[],
    )
    .unwrap();
    let merged = read_ids(&output);
    let inputs: Vec<Vec<i64>> = files.into_iter().map(|(_, id)| id).collect();
    assert_mapping_matches_output(&out, &inputs, &merged);
}

/// Tier 1: once the other cursor is exhausted, the last cursor is drained batch by batch
/// via `advance_past_batch`. Its rows past the first batch must keep their own slots.
#[test]
fn mapping_correct_when_single_cursor_drains_several_batches() {
    // A = keys 0..10 (4 batches of 3), B = one row that sorts first.
    let a_keys: Vec<i64> = (1..=10).collect();
    let a_ids: Vec<i64> = (100..110).collect();
    run(
        "rowid_map_tier1",
        3,
        vec![(a_keys, a_ids), (vec![0], vec![900])],
    );
}

/// Tier 2: a whole cursor batch is emitted ahead of the heap top, then the cursor moves
/// to its next batch via `advance_past_batch`.
#[test]
fn mapping_correct_when_whole_batches_are_emitted_between_yields() {
    let a_keys = vec![1, 2, 3, 10, 11, 12, 20, 21, 22];
    let a_ids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
    let b_keys = vec![5, 6, 7, 15, 16, 17, 25, 26, 27];
    let b_ids = vec![11, 12, 13, 14, 15, 16, 17, 18, 19];
    run("rowid_map_tier2", 3, vec![(a_keys, a_ids), (b_keys, b_ids)]);
}

/// Tier 3: interleaved keys force row-by-row runs that cross batch boundaries through
/// `advance`, plus many duplicate keys (the shape of a low-cardinality index sort).
#[test]
fn mapping_correct_with_interleaved_keys_and_ties_across_batches() {
    let n = 1000;
    let a_keys: Vec<i64> = (0..n).map(|i| i / 7).collect();
    let b_keys: Vec<i64> = (0..n).map(|i| i / 5).collect();
    let c_keys: Vec<i64> = (0..n).map(|i| i / 11).collect();
    let a_ids: Vec<i64> = (0..n).collect();
    let b_ids: Vec<i64> = (n..2 * n).collect();
    let c_ids: Vec<i64> = (2 * n..3 * n).collect();
    run(
        "rowid_map_tier3",
        64,
        vec![(a_keys, a_ids), (b_keys, b_ids), (c_keys, c_ids)],
    );
}

/// The production shape that exposed the bug: one large, already-merged input (several
/// batches) merged with smaller fresh inputs that each fit in one batch.
#[test]
fn mapping_correct_for_large_merged_input_plus_small_inputs() {
    let big = 3000;
    let mut files = vec![(
        (0..big).map(|i| i % 8).collect::<Vec<i64>>(),
        (0..big).collect::<Vec<i64>>(),
    )];
    files[0].0.sort();
    for f in 0..6 {
        let n = 500;
        let mut keys: Vec<i64> = (0..n).map(|i| (i * 13 + f) % 8).collect();
        keys.sort();
        let ids: Vec<i64> = (0..n).map(|i| 10_000 * (f + 1) + i).collect();
        files.push((keys, ids));
    }
    run("rowid_map_big_plus_small", 1000, files);
}
