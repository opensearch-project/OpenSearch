/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::Schema as ArrowSchema;
use parquet::basic::Repetition;
use parquet::schema::types::Type;

use super::error::MergeResult;

/// Reserved column name for the synthetic row identifier added during merge.
pub const ROW_ID_COLUMN_NAME: &str = "___row_id";

/// Builds the output Parquet schema as the union of pre-read schema descriptors.
///
/// The output schema contains every column seen across all inputs, except:
/// - Any existing `___row_id` column is removed.
/// - A fresh `___row_id` INT64 REQUIRED column is appended at the end.
pub fn build_parquet_root_schema(
    schema_descriptors: &[parquet::schema::types::SchemaDescriptor],
) -> MergeResult<Arc<Type>> {
    let mut seen_names: HashSet<String> = HashSet::new();
    let mut parquet_fields: Vec<Arc<Type>> = Vec::new();

    for descr in schema_descriptors {
        let root = descr.root_schema();
        for field in root.get_fields() {
            if field.name() != ROW_ID_COLUMN_NAME
                && seen_names.insert(field.name().to_string())
            {
                parquet_fields.push(Arc::new(field.as_ref().clone()));
            }
        }
    }

    let row_id_type =
        Type::primitive_type_builder(ROW_ID_COLUMN_NAME, parquet::basic::Type::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
    parquet_fields.push(Arc::new(row_id_type));

    let parquet_root = Type::group_type_builder("schema")
        .with_fields(parquet_fields)
        .build()?;

    Ok(Arc::new(parquet_root))
}

/// Returns column indices that exclude `___row_id`, for use as a projection mask.
pub fn projection_indices_excluding_row_id(schema: &ArrowSchema) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name() != ROW_ID_COLUMN_NAME)
        .map(|(i, _)| i)
        .collect()
}

/// Pads a batch to conform to the target schema by adding null-filled columns
/// for any fields present in `target_schema` but missing from the batch.
///
/// Returns the batch unchanged (no copy) when schemas already match.
pub fn pad_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &Arc<ArrowSchema>,
) -> MergeResult<RecordBatch> {
    let batch_schema = batch.schema();
    if batch_schema.fields() == target_schema.fields() {
        return Ok(batch.clone());
    }

    let num_rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        match batch_schema.index_of(field.name()) {
            Ok(col_idx) => columns.push(batch.column(col_idx).clone()),
            Err(_) => {
                columns.push(arrow::array::new_null_array(field.data_type(), num_rows));
            }
        }
    }

    Ok(RecordBatch::try_new(target_schema.clone(), columns)?)
}

/// Appends a `___row_id` column with sequential values `[start_id, start_id + N)`
/// to the given batch, producing a new batch with the output schema.
pub fn append_row_id(
    batch: &RecordBatch,
    start_id: i64,
    output_schema: &Arc<ArrowSchema>,
) -> MergeResult<RecordBatch> {
    let n = batch.num_rows() as i64;
    let row_ids = Int64Array::from_iter_values(start_id..start_id + n);
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(row_ids));
    let result = RecordBatch::try_new(output_schema.clone(), columns)?;
    Ok(result)
}
