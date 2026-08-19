/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Predicate-column name → parquet leaf-index resolution.
//!
//! Resolution is done against the file's OWN schema (derived from the footer)
//! rather than the shared table schema to ensure correct leaf indices under
//! schema evolution (see [`resolve_predicate_parquet_columns`] for details).

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::file::metadata::ParquetMetaData;

/// Map the query's predicate-column names to **this file's** parquet leaf
/// indices, resolving against the file's OWN schema so the indices are correct
/// even when the file is missing columns (schema evolution).
///
/// # Why the file's own schema, not the shared table schema
///
/// `StatisticsConverter`/`parquet_column` map a column by finding its position in
/// the supplied arrow schema and then matching that position to a parquet leaf
/// (`get_column_root_idx`). The table schema is the **union** of all
/// files' columns [N]; a given file may physically contain fewer[M] (e.g.
/// the merged file has M leaves — the absent columns are all-null and not
/// written). Resolving against the N-field union therefore maps a column to the
/// WRONG leaf in a M-leaf file. We would then build
/// the scoped ColumnIndex/OffsetIndex at the wrong leaf and leave the real one an
/// empty placeholder — and DataFusion's pruner, which resolves against the file's
/// physical schema, reads the real leaf and panics on the empty `page_locations`
/// (`statistics.rs` `page_locations.last().unwrap()`).
///
/// Deriving the arrow schema from the file footer (`parquet_to_arrow_schema`)
/// gives a 1:1 field↔leaf correspondence for that file, so the resolved index
/// matches what DataFusion dereferences. Columns absent from the file are skipped.
pub fn resolve_predicate_parquet_columns(
    _arrow_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    predicate_column_names: &[String],
    file_schema: &SchemaRef,
) -> Vec<usize> {
    let parquet_schema = metadata.file_metadata().schema_descr();
    resolve_with_schema(file_schema, metadata, predicate_column_names)
}

/// Resolve TWO name-sets (e.g. predicate columns and projection columns) against
/// the same file in one pass. Deriving the per-file arrow schema
/// (`parquet_to_arrow_schema`) is the dominant cost of name→leaf resolution on
/// wide schemas (it rebuilds the whole file's Schema); the two callers in the
/// indexed setup loop previously each rebuilt it, so doing it once here removes a
/// full schema reconstruction per file per query. Pure refactor — each returned
/// Vec is identical to calling `resolve_predicate_parquet_columns` separately.
pub fn resolve_predicate_parquet_columns_pair(
    _union_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    predicate_col_names: &[String],
    projection_col_names: &[String],
    file_schema: &SchemaRef,
) -> (Vec<usize>, Vec<usize>) {
    (
        resolve_with_schema(file_schema, metadata, predicate_col_names),
        resolve_with_schema(file_schema, metadata, projection_col_names),
    )
}

/// Resolve predicate column names → parquet leaf indices against a specific arrow
/// schema, via the same `StatisticsConverter` mapping DataFusion's pruner uses.
///
/// Nested (e.g. `List`) columns need their own arm: `StatisticsConverter` delegates to
/// arrow-rs `parquet_column`, which **silently returns `None` for any nested field**
/// ("Nested fields are not supported and require non-trivial logic"). Dropping the name
/// here would leave a *referenced* nested column with only a placeholder OffsetIndex,
/// and no single-page placeholder can describe a repeated leaf — pages of a repeated
/// leaf hold VALUES while `first_row_index` is defined in ROWS, so the reader derives
/// the wrong byte range ("Src size is incorrect" / "StructArrayReader out of sync in
/// read_records"). For nested fields we therefore map the arrow root to ALL parquet
/// leaves under that root — the same root-positional correspondence `parquet_column`
/// itself uses for the flat case, and sound here because `arrow_schema` is derived from
/// this file's own footer (`parquet_to_arrow_schema`), giving 1:1 field↔root order.
/// Unreferenced nested columns still get placeholders, which is fine: a placeholder is
/// only ever dereferenced for columns a read actually touches.
pub(super) fn resolve_with_schema(
    arrow_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    predicate_column_names: &[String],
) -> Vec<usize> {
    let parquet_schema = metadata.file_metadata().schema_descr();
    let mut set = HashSet::new();
    for name in predicate_column_names {
        if let Some((root_idx, field)) = arrow_schema.fields().find(name) {
            if field.data_type().is_nested() {
                for leaf in 0..parquet_schema.num_columns() {
                    if parquet_schema.get_column_root_idx(leaf) == root_idx {
                        set.insert(leaf);
                    }
                }
                continue;
            }
        }
        if let Ok(conv) = StatisticsConverter::try_new(name, arrow_schema, parquet_schema) {
            if let Some(idx) = conv.parquet_column_index() {
                set.insert(idx);
            }
        }
    }
    set.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, ListArray, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;

    /// id (flat, leaf 0), tags = List<Utf8> (repeated, leaf 1: tags.list.element),
    /// score (flat, leaf 2). The nested column shifts nothing here (one leaf per root),
    /// but names must resolve through the nested arm, which StatisticsConverter refuses.
    fn file_with_list_column() -> (tempfile::TempDir, ParquetMetaData, SchemaRef) {
        let child = Arc::new(Field::new("element", DataType::Utf8, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tags", DataType::List(child.clone()), true),
            Field::new("score", DataType::Int64, true),
        ]));
        let values = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0, 2, 3].into());
        let list = ListArray::new(child, offsets, Arc::new(values), None);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(list) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            ],
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let f = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let meta = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap())
            .unwrap()
            .metadata()
            .as_ref()
            .clone();
        (dir, meta, schema)
    }

    #[test]
    fn nested_column_resolves_to_its_leaves() {
        let (_dir, meta, schema) = file_with_list_column();
        let mut got = resolve_with_schema(&schema, &meta, &["tags".to_string()]);
        got.sort_unstable();
        // tags is arrow root 1; its single parquet leaf is index 1 (tags.list.element).
        assert_eq!(got, vec![1], "nested column must map to its real leaves");
    }

    #[test]
    fn flat_columns_unchanged_by_nested_arm() {
        let (_dir, meta, schema) = file_with_list_column();
        let mut got = resolve_with_schema(
            &schema,
            &meta,
            &["id".to_string(), "score".to_string()],
        );
        got.sort_unstable();
        assert_eq!(got, vec![0, 2]);
    }

    #[test]
    fn mixed_flat_and_nested_names() {
        let (_dir, meta, schema) = file_with_list_column();
        let mut got = resolve_with_schema(
            &schema,
            &meta,
            &["tags".to_string(), "score".to_string()],
        );
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn unknown_name_is_skipped() {
        let (_dir, meta, schema) = file_with_list_column();
        let got = resolve_with_schema(&schema, &meta, &["does_not_exist".to_string()]);
        assert!(got.is_empty());
    }
}
