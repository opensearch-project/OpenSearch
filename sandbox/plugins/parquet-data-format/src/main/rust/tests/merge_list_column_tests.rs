/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Merge coverage for multi-valued (Arrow LIST / parquet repeated) columns.
//!
//! A LIST column breaks the "one leaf value per row" identity every flat column satisfies, so the
//! merge path needs its own coverage: the k-way merge slices batches by row, appends a fresh
//! `__row_id__`, and re-encodes each column through `compute_leaves`. All of that must keep a
//! repeated column's values grouped with their original row.

use std::fs::File;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use opensearch_parquet_format::merge::{merge_sorted, merge_unsorted};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

/// Schema: `id` (sort key, Int64) + `tags` (List<Utf8>) + `__row_id__`.
fn list_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
            true,
        ),
        Field::new("__row_id__", DataType::Int64, false),
    ]))
}

/// Build a file where row i has `id = ids[i]` and `tags = rows[i]` (None ⇒ null list).
fn write_nullable_list_file(path: &str, ids: &[i64], rows: &[Option<Vec<Option<&str>>>]) {
    assert_eq!(ids.len(), rows.len());
    let mut values: Vec<Option<String>> = Vec::new();
    let mut offsets: Vec<i32> = vec![0];
    let mut validity: Vec<bool> = Vec::new();
    for row in rows {
        match row {
            Some(row_values) => {
                values.extend(
                    row_values
                        .iter()
                        .map(|value| value.map(ToString::to_string)),
                );
                validity.push(true);
            }
            None => validity.push(false),
        }
        offsets.push(values.len() as i32);
    }
    let list = ListArray::new(
        Arc::new(Field::new("element", DataType::Utf8, true)),
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        Arc::new(StringArray::from(values)),
        Some(arrow::buffer::NullBuffer::from(validity)),
    );
    let schema = list_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(list) as ArrayRef,
            Arc::new(Int64Array::from((0..ids.len() as i64).collect::<Vec<_>>())) as ArrayRef,
        ],
    )
    .unwrap();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Build a LIST file with non-null child values.
fn write_list_file(path: &str, ids: &[i64], rows: &[Option<Vec<&str>>]) {
    let nullable_rows = rows
        .iter()
        .map(|row| {
            row.as_ref()
                .map(|values| values.iter().map(|value| Some(*value)).collect())
        })
        .collect::<Vec<_>>();
    write_nullable_list_file(path, ids, &nullable_rows);
}

fn scalar_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tags", DataType::Utf8, true),
        Field::new("__row_id__", DataType::Int64, false),
    ]))
}

fn write_scalar_file(path: &str, ids: &[i64], rows: &[Option<&str>]) {
    assert_eq!(ids.len(), rows.len());
    let schema = scalar_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(rows.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from((0..ids.len() as i64).collect::<Vec<_>>())) as ArrayRef,
        ],
    )
    .unwrap();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Read back `(id, tags)` pairs in file order.
fn read_nullable_pairs(path: &str) -> Vec<(i64, Option<Vec<Option<String>>>)> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let ids = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_primitive::<arrow::datatypes::Int64Type>();
        let lists = batch
            .column(batch.schema().index_of("tags").unwrap())
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("tags must decode as a ListArray");
        for row in 0..batch.num_rows() {
            let tags = if lists.is_null(row) {
                None
            } else {
                let values = lists.value(row);
                let strings = values.as_any().downcast_ref::<StringArray>().unwrap();
                Some(
                    (0..strings.len())
                        .map(|index| {
                            if strings.is_null(index) {
                                None
                            } else {
                                Some(strings.value(index).to_string())
                            }
                        })
                        .collect(),
                )
            };
            out.push((ids.value(row), tags));
        }
    }
    out
}

/// Read back `(id, tags)` pairs when all LIST child values are non-null.
fn read_pairs(path: &str) -> Vec<(i64, Option<Vec<String>>)> {
    read_nullable_pairs(path)
        .into_iter()
        .map(|(id, tags)| {
            (
                id,
                tags.map(|values| {
                    values
                        .into_iter()
                        .map(|value| value.expect("unexpected null LIST child"))
                        .collect()
                }),
            )
        })
        .collect()
}

/// Borrow an owned rows-of-strings structure as the `&str` shape `write_list_file` takes.
fn as_refs(rows: &[Option<Vec<String>>]) -> Vec<Option<Vec<&str>>> {
    rows.iter()
        .map(|r| r.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect()))
        .collect()
}

fn v(items: &[&str]) -> Option<Vec<String>> {
    Some(items.iter().map(|s| (*s).to_string()).collect())
}

#[test]
fn unsorted_merge_preserves_list_values() {
    let tmp = tempdir().unwrap();
    let a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    let out = tmp
        .path()
        .join("merged.parquet")
        .to_string_lossy()
        .to_string();

    // Varying list lengths, a duplicate, an empty list, and a null list.
    write_list_file(
        &a,
        &[1, 2, 3],
        &[
            Some(vec!["beta", "alpha", "beta"]),
            None,
            Some(vec!["solo"]),
        ],
    );
    write_list_file(&b, &[4, 5], &[Some(vec![]), Some(vec!["x", "y"])]);

    merge_unsorted(&[a, b], &out, "merge-list-unsorted", 0).unwrap();

    let pairs = read_pairs(&out);
    assert_eq!(pairs.len(), 5, "row count must be preserved");
    assert_eq!(pairs[0], (1, v(&["beta", "alpha", "beta"])));
    assert_eq!(pairs[1], (2, None), "null list must stay null");
    assert_eq!(pairs[2], (3, v(&["solo"])));
    assert_eq!(pairs[3].0, 4);
    assert_eq!(pairs[3].1, Some(vec![]), "empty list must stay empty");
    assert_eq!(pairs[4], (5, v(&["x", "y"])));
}

#[test]
fn unsorted_merge_promotes_scalar_values_to_singleton_lists() {
    let tmp = tempdir().unwrap();
    let scalar = tmp
        .path()
        .join("scalar.parquet")
        .to_string_lossy()
        .to_string();
    let list = tmp
        .path()
        .join("list.parquet")
        .to_string_lossy()
        .to_string();
    let out = tmp
        .path()
        .join("merged.parquet")
        .to_string_lossy()
        .to_string();

    write_scalar_file(&scalar, &[1, 2], &[Some("prod"), None]);
    write_list_file(
        &list,
        &[3, 4],
        &[Some(vec!["prod", "error"]), Some(vec!["solo"])],
    );

    merge_unsorted(&[scalar, list], &out, "merge-scalar-list", 0).unwrap();

    assert_eq!(
        read_pairs(&out),
        vec![
            (1, v(&["prod"])),
            (2, None),
            (3, v(&["prod", "error"])),
            (4, v(&["solo"])),
        ]
    );
}

#[test]
fn sorted_merge_keeps_list_values_with_their_row() {
    let tmp = tempdir().unwrap();
    let a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    let out = tmp
        .path()
        .join("merged.parquet")
        .to_string_lossy()
        .to_string();

    // Interleaving sort keys force the k-way merge to alternate between cursors, so a row-vs-value
    // confusion in the LIST column would surface as values attached to the wrong id.
    write_list_file(
        &a,
        &[1, 3, 5],
        &[
            Some(vec!["one"]),
            Some(vec!["three", "iii"]),
            Some(vec!["five"]),
        ],
    );
    write_list_file(
        &b,
        &[2, 4, 6],
        &[Some(vec!["two", "ii", "2"]), None, Some(vec!["six"])],
    );

    merge_sorted(
        &[a, b],
        &out,
        "merge-list-sorted",
        &["id".to_string()],
        &[false],
        &[false],
        0,
    )
    .unwrap();

    let pairs = read_pairs(&out);
    assert_eq!(pairs.len(), 6);
    // Sort order by id, and every row keeps exactly its own values.
    assert_eq!(pairs[0], (1, v(&["one"])));
    assert_eq!(pairs[1], (2, v(&["two", "ii", "2"])));
    assert_eq!(pairs[2], (3, v(&["three", "iii"])));
    assert_eq!(pairs[3], (4, None), "null list must stay null");
    assert_eq!(pairs[4], (5, v(&["five"])));
    assert_eq!(pairs[5], (6, v(&["six"])));
}

#[test]
fn sorted_merge_uses_minimum_list_element_as_sort_key() {
    let tmp = tempdir().unwrap();
    let a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    let ascending = tmp
        .path()
        .join("ascending.parquet")
        .to_string_lossy()
        .to_string();

    // Each input is already ordered by its per-row minimum. Source list order is deliberately
    // different so the merge cannot accidentally use the first element or lexicographic LIST order.
    write_list_file(
        &a,
        &[10, 30, 50],
        &[Some(vec!["z", "alpha"]), Some(vec!["delta"]), None],
    );
    write_list_file(
        &b,
        &[20, 40, 60],
        &[
            Some(vec!["beta", "zz"]),
            Some(vec!["omega", "gamma"]),
            Some(vec![]),
        ],
    );

    merge_sorted(
        &[a, b],
        &ascending,
        "merge-list-min-ascending",
        &["tags".to_string()],
        &[false],
        &[false],
        0,
    )
    .unwrap();

    let ascending_ids: Vec<i64> = read_pairs(&ascending)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(&ascending_ids[..4], &[10, 20, 30, 40]);
    assert!(ascending_ids[4..].contains(&50));
    assert!(ascending_ids[4..].contains(&60));

    let descending_a = tmp
        .path()
        .join("descending_a.parquet")
        .to_string_lossy()
        .to_string();
    let descending_b = tmp
        .path()
        .join("descending_b.parquet")
        .to_string_lossy()
        .to_string();
    let descending = tmp
        .path()
        .join("descending.parquet")
        .to_string_lossy()
        .to_string();
    write_list_file(
        &descending_a,
        &[40, 20, 50],
        &[Some(vec!["omega", "gamma"]), Some(vec!["beta", "zz"]), None],
    );
    write_list_file(
        &descending_b,
        &[30, 10, 60],
        &[Some(vec!["delta"]), Some(vec!["z", "alpha"]), Some(vec![])],
    );

    merge_sorted(
        &[descending_a, descending_b],
        &descending,
        "merge-list-min-descending",
        &["tags".to_string()],
        &[true],
        &[false],
        0,
    )
    .unwrap();

    let descending_ids: Vec<i64> = read_pairs(&descending)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(&descending_ids[..4], &[40, 30, 20, 10]);
    assert!(descending_ids[4..].contains(&50));
    assert!(descending_ids[4..].contains(&60));
}

#[test]
fn sorted_merge_orders_scalar_and_list_generations_by_minimum_value() {
    let tmp = tempdir().unwrap();
    let scalar = tmp
        .path()
        .join("scalar.parquet")
        .to_string_lossy()
        .to_string();
    let list = tmp
        .path()
        .join("list.parquet")
        .to_string_lossy()
        .to_string();
    let out = tmp
        .path()
        .join("merged.parquet")
        .to_string_lossy()
        .to_string();

    write_scalar_file(&scalar, &[10, 30], &[Some("alpha"), Some("delta")]);
    write_list_file(
        &list,
        &[20, 40],
        &[Some(vec!["zz", "beta"]), Some(vec!["omega", "gamma"])],
    );

    merge_sorted(
        &[scalar, list],
        &out,
        "merge-scalar-list-sorted",
        &["tags".to_string()],
        &[false],
        &[false],
        0,
    )
    .unwrap();

    let pairs = read_pairs(&out);
    assert_eq!(
        pairs.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![10, 20, 30, 40]
    );
    assert_eq!(pairs[0], (10, v(&["alpha"])));
    assert_eq!(pairs[1], (20, v(&["zz", "beta"])));
    assert_eq!(pairs[2], (30, v(&["delta"])));
    assert_eq!(pairs[3], (40, v(&["omega", "gamma"])));
}

/// Multi-batch cursors + deferred column decode.
///
/// Two settings make this the adversarial case for a repeated column:
/// `merge_batch_size` small enough that each cursor spans several batches (so the k-way merge
/// crosses batch boundaries mid-file, exercising `advance`/`load_next_batch` and `take_slice`), and
/// `merge_deferred_column_threshold = 0` which forces the cursor's *deferred* mode — sort columns
/// and data columns are read through two independent parquet readers kept in lockstep by batch
/// index. If a LIST column's leaf desynchronised from the sort reader, the two would drift.
#[test]
fn sorted_merge_list_across_batches_in_deferred_mode() {
    use opensearch_parquet_format::native_settings::NativeSettings;
    use opensearch_parquet_format::writer::SETTINGS_STORE;

    let index = "merge-list-deferred";
    SETTINGS_STORE.insert(
        index.to_string(),
        NativeSettings {
            merge_batch_size: Some(4),
            merge_deferred_column_threshold: Some(0),
            ..Default::default()
        },
    );

    let tmp = tempdir().unwrap();
    let a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    let out = tmp
        .path()
        .join("merged.parquet")
        .to_string_lossy()
        .to_string();

    // 40 rows total, interleaved odd/even ids, with per-row list lengths that vary 0..3 so the
    // value count is unrelated to the row count in every batch.
    let ids_a: Vec<i64> = (0..20).map(|i| i * 2 + 1).collect();
    let ids_b: Vec<i64> = (0..20).map(|i| i * 2 + 2).collect();
    let owned_a: Vec<Option<Vec<String>>> = ids_a
        .iter()
        .enumerate()
        .map(|(i, id)| match i % 4 {
            0 => None,
            1 => Some(vec![]),
            2 => Some(vec![format!("a{id}")]),
            _ => Some(vec![format!("a{id}"), format!("a{id}b"), format!("a{id}c")]),
        })
        .collect();
    let owned_b: Vec<Option<Vec<String>>> = ids_b
        .iter()
        .enumerate()
        .map(|(i, id)| match i % 3 {
            0 => Some(vec![format!("b{id}"), format!("b{id}x")]),
            1 => None,
            _ => Some(vec![format!("b{id}")]),
        })
        .collect();
    write_list_file(&a, &ids_a, &as_refs(&owned_a));
    write_list_file(&b, &ids_b, &as_refs(&owned_b));

    merge_sorted(
        &[a, b],
        &out,
        index,
        &["id".to_string()],
        &[false],
        &[false],
        0,
    )
    .unwrap();

    let pairs = read_pairs(&out);
    assert_eq!(pairs.len(), 40, "all rows must survive the merge");

    // Rebuild the expected (id → tags) mapping and check every row independently, so a single
    // misattached value fails loudly with the offending id.
    let mut expected: std::collections::HashMap<i64, Option<Vec<String>>> =
        std::collections::HashMap::new();
    for (id, tags) in ids_a.iter().zip(owned_a.iter()) {
        expected.insert(*id, tags.clone());
    }
    for (id, tags) in ids_b.iter().zip(owned_b.iter()) {
        expected.insert(*id, tags.clone());
    }

    let mut prev_id = i64::MIN;
    for (id, tags) in &pairs {
        assert!(
            *id > prev_id,
            "output must be sorted by id, saw {id} after {prev_id}"
        );
        prev_id = *id;
        let want = expected
            .remove(id)
            .unwrap_or_else(|| panic!("unexpected id {id}"));
        assert_eq!(
            tags, &want,
            "row id={id} changed null/empty state or lost values in the merge"
        );
    }
    assert!(
        expected.is_empty(),
        "rows missing from output: {:?}",
        expected.keys()
    );
}

#[test]
fn unsorted_merge_preserves_null_empty_and_null_children_exactly() {
    let tmp = tempdir().unwrap();
    let input = tmp
        .path()
        .join("input.parquet")
        .to_string_lossy()
        .to_string();
    let out = tmp
        .path()
        .join("merged.parquet")
        .to_string_lossy()
        .to_string();

    write_nullable_list_file(
        &input,
        &[1, 2, 3, 4],
        &[
            None,
            Some(vec![]),
            Some(vec![Some("alpha"), None, Some("beta")]),
            Some(vec![None]),
        ],
    );

    merge_unsorted(&[input], &out, "merge-list-null-children", 0).unwrap();

    assert_eq!(
        read_nullable_pairs(&out),
        vec![
            (1, None),
            (2, Some(vec![])),
            (
                3,
                Some(vec![
                    Some("alpha".to_string()),
                    None,
                    Some("beta".to_string()),
                ]),
            ),
            (4, Some(vec![None])),
        ]
    );
}

#[test]
fn merge_rejects_incompatible_scalar_and_list_child_types() {
    let tmp = tempdir().unwrap();
    let scalar = tmp
        .path()
        .join("scalar.parquet")
        .to_string_lossy()
        .to_string();
    let list = tmp
        .path()
        .join("list.parquet")
        .to_string_lossy()
        .to_string();
    let out = tmp
        .path()
        .join("merged.parquet")
        .to_string_lossy()
        .to_string();

    write_scalar_file(&scalar, &[1], &[Some("prod")]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
            true,
        ),
        Field::new("__row_id__", DataType::Int64, false),
    ]));
    let offsets = arrow::buffer::OffsetBuffer::new(vec![0_i32, 2].into());
    let values = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
    let lists = ListArray::new(
        Arc::new(Field::new("element", DataType::Int32, true)),
        offsets,
        values,
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
            Arc::new(lists) as ArrayRef,
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&list).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let error = match merge_unsorted(&[scalar, list], &out, "merge-incompatible-list-child", 0) {
        Ok(_) => panic!("Utf8 must not promote to List<Int32>"),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(
        message.contains("tags") || message.contains("Failed to compute union schema"),
        "unexpected error: {message}"
    );
}

#[test]
fn sorted_merge_uses_scalar_tiebreaker_after_list_minimum() {
    let tmp = tempdir().unwrap();
    let a = tmp.path().join("a.parquet").to_string_lossy().to_string();
    let b = tmp.path().join("b.parquet").to_string_lossy().to_string();
    let out = tmp
        .path()
        .join("merged.parquet")
        .to_string_lossy()
        .to_string();

    // Each input is sorted by MIN(tags) ASC and then id DESC.
    write_list_file(
        &a,
        &[30, 10, 50],
        &[
            Some(vec!["z", "alpha"]),
            Some(vec!["alpha"]),
            Some(vec!["beta"]),
        ],
    );
    write_list_file(
        &b,
        &[40, 20, 5],
        &[
            Some(vec!["alpha", "zz"]),
            Some(vec!["m", "beta"]),
            Some(vec!["gamma"]),
        ],
    );

    merge_sorted(
        &[a, b],
        &out,
        "merge-list-multi-column",
        &["tags".to_string(), "id".to_string()],
        &[false, true],
        &[false, false],
        0,
    )
    .unwrap();

    let pairs = read_pairs(&out);
    assert_eq!(
        pairs.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![40, 30, 10, 50, 20, 5]
    );
    assert_eq!(pairs[0], (40, v(&["alpha", "zz"])));
    assert_eq!(pairs[1], (30, v(&["z", "alpha"])));
    assert_eq!(pairs[4], (20, v(&["m", "beta"])));
}
