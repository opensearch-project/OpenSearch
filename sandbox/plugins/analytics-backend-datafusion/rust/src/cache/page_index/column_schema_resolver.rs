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
pub(super) fn resolve_with_schema(
    arrow_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    predicate_column_names: &[String],
) -> Vec<usize> {
    let parquet_schema = metadata.file_metadata().schema_descr();
    let mut set = HashSet::new();
    for name in predicate_column_names {
        let resolved = StatisticsConverter::try_new(name, arrow_schema, parquet_schema)
            .ok()
            .and_then(|conv| conv.parquet_column_index());
        if let Some(idx) = resolved {
            set.insert(idx);
            continue;
        }

        // parquet-rs intentionally does not resolve nested Arrow fields through
        // `parquet_column()`: LIST/MAP/STRUCT roots may map to one or more physical
        // leaves. Since `arrow_schema` is derived from this file's footer, its root
        // index matches `SchemaDescriptor::get_column_root_idx`; include every leaf
        // under the requested root so any projected nested field receives a real
        // OffsetIndex rather than a scoped-out placeholder.
        if let Some((root_idx, _)) = arrow_schema.fields().find(name) {
            for leaf_idx in 0..parquet_schema.num_columns() {
                if parquet_schema.get_column_root_idx(leaf_idx) == root_idx {
                    set.insert(leaf_idx);
                }
            }
        }
    }
    set.into_iter().collect()
}
