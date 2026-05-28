/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Schema coercion at the Substrait/DataFusion scan boundary.
//!
//! Substrait's type system is narrower than Arrow's. The DataFusion Substrait
//! consumer rejects scans whose table-provider schema is not
//! `datatype_is_logically_equal` to the schema declared in the plan. The pairs
//! we hit on this path:
//!
//!   - `BinaryView`  ← parquet emits this for variable-length byte columns
//!     (including OpenSearch's 16-byte `ip` and `binary` mappings) when
//!     `schema_force_view_types` is on. Substrait has no view types — isthmus
//!     serializes `VARBINARY` as plain `binary`, which arrives as
//!     `DataType::Binary`.
//!
//!   - `UInt64`      ← parquet emits this for `unsigned_long` columns.
//!     Substrait integers are signed only — Calcite `BIGINT` serializes as
//!     `i64`, which arrives as `DataType::Int64`.
//!
//!   - `Float16`     ← parquet emits this for `half_float` columns. Calcite has
//!     no fp16 type; `OpenSearchSchemaBuilder` maps it to `REAL` which Substrait
//!     serializes as `fp32`, arriving as `DataType::Float32`.
//!
//! `Utf8View` doesn't need coercing: DataFusion 53's
//! `DFSchema::datatype_is_logically_equal` (in `datafusion-common/src/dfschema.rs`)
//! has hardcoded match arms `(Utf8, Utf8View) => true` and `(Utf8View, Utf8) => true`,
//! so a Substrait plan declaring `Utf8` binds cleanly against a table column
//! reporting `Utf8View`. The two cases above are different in nature:
//!
//!   - `(Binary, BinaryView)` is a missing equivalence in DataFusion. The two
//!     types are semantically identical, but `datatype_is_logically_equal` has
//!     no arm for them today (DF 53). If it becomes available in DataFusion,
//!     the `BinaryView → Binary` rewrite here can be removed.
//!
//!     TODO: every record batch goes through a `BinaryView → Binary` cast in the
//!     SchemaAdapter (offset+data buffer copy), and downstream operators see
//!     `Binary` rather than `BinaryView`. Drop this arm when we have a proper
//!     solution.
//!
//!   - `(Int64, UInt64)` is a Substrait + Calcite gap. Substrait's integer
//!     types are signed-only and Calcite has no unsigned `BIGINT`, so the
//!     unsigned semantics are lost before the plan reaches DataFusion. Values
//!     above `2^63 - 1` wrap into negatives — documented in
//!     `OpenSearchSchemaBuilder.mapFieldType`. Narrowing at the scan boundary
//!     is the only fix until Substrait grows unsigned types (see Substrait
//!     `proto/type.proto`).
//!
//!     TODO: values above `2^63 - 1` wrap into negatives. Drop this arm when we
//!     have a proper solution.
//!
//!   - `(Float32, Float16)` is a Calcite-side gap: Calcite has no fp16 type, so
//!     `half_float` columns are widened to `REAL` (fp32) at the Java planner.
//!     Every record batch goes through a `Float16 → Float32` cast in the
//!     SchemaAdapter, and downstream operators see `Float32` rather than
//!     `Float16` — so we lose the half-precision storage benefit at compute time.
//!
//!     TODO: every record batch goes through a `Float16 → Float32` cast and
//!     downstream operators see `Float32`. Drop this arm when we have a proper
//!     solution.
//!
//! We rewrite the inferred schema at the table-provider boundary: substitute the
//! Arrow-only types with their Substrait-compatible counterparts before handing
//! the schema to `ListingTableConfig`. The parquet reader's `SchemaAdapter` then
//! inserts the per-batch cast at read time (zero-copy bit reinterpret for
//! `UInt64 → Int64`; cheap buffer relabeling for `BinaryView → Binary`).
//!
//! Alternatives considered:
//!
//!   - Disable view types entirely with
//!     `execution.parquet.schema_force_view_types = false` (threaded into
//!     `ParquetFormat::with_options`). Makes the reader emit `Utf8/Binary` instead
//!     of `Utf8View/BinaryView`. Removes the BinaryView mismatch but also strips
//!     `Utf8View` from string columns, giving up the inline-prefix optimization
//!     on filter/group-by/hash paths.
//!
//!   - Skip `infer_schema` entirely and construct the Arrow schema directly from
//!     the OpenSearch mapping (the same source Calcite reads). Single source of
//!     truth, no post-process step. Costs the cross-language plumbing to ship
//!     the schema from Java to Rust and adds a second mapping table to keep in
//!     sync with `OpenSearchSchemaBuilder.mapFieldType`.

use std::sync::Arc;

use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;

/// Rewrite the schema to forms Substrait can bind against:
///   - `BinaryView` → `Binary`
///   - `UInt64`     → `Int64`
///   - `Float16`    → `Float32`
///
/// Recurses into `List`, `LargeList`, `FixedSizeList`, `Map`, `Struct`,
/// `Union`, and `Dictionary`. Returns the input unchanged when no rewrite is
/// needed so callers avoid an unnecessary `Arc` reallocation in the common
/// case.
pub fn coerce_inferred_schema(schema: SchemaRef) -> SchemaRef {
    if !schema_needs_coerce(&schema) {
        return schema;
    }
    let rewritten_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| rewrite_field(f))
        .collect();
    Arc::new(Schema::new_with_metadata(
        rewritten_fields,
        schema.metadata().clone(),
    ))
}

/// Casts every column of `batch` to the corresponding field type in `target`,
/// returning a batch whose schema is exactly `target`.
///
/// Used at the streaming-partition boundary: a producer fragment emits batches
/// in DataFusion's raw physical types (`UInt64` for `row_number()`, `BinaryView`
/// for view-typed columns, `Float16`), but the consumer fragment's Substrait
/// plan declares the coerced forms (`Int64` / `Binary` / `Float32`) — the same
/// rewrite [`coerce_inferred_schema`] applies to scan schemas. Without this the
/// registered `StreamingTable` schema diverges from the consumer plan and
/// DataFusion rejects the wire with "Substrait schema has a different type than
/// the corresponding field in the table schema".
///
/// Columns already matching their target type are reused without a copy; only
/// divergent columns go through [`arrow::compute::cast`] (e.g. the cheap
/// `UInt64 → Int64` reinterpret). Returns the input unchanged when its schema
/// already equals `target`.
pub fn coerce_record_batch(
    batch: RecordBatch,
    target: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    if &batch.schema() == target {
        return Ok(batch);
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, column) in batch.columns().iter().enumerate() {
        let want = target.field(i).data_type();
        if column.data_type() == want {
            columns.push(Arc::clone(column));
        } else {
            columns.push(cast(column, want).map_err(|e| {
                DataFusionError::Execution(format!(
                    "coerce_record_batch: cast of column '{}' from {:?} to {:?} failed: {}",
                    target.field(i).name(),
                    column.data_type(),
                    want,
                    e
                ))
            })?);
        }
    }
    RecordBatch::try_new(Arc::clone(target), columns)
        .map_err(|e| DataFusionError::Execution(format!("coerce_record_batch: {}", e)))
}

fn schema_needs_coerce(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| contains_incompatible(f.data_type()))
}

fn contains_incompatible(dt: &DataType) -> bool {
    match dt {
        DataType::BinaryView | DataType::UInt64 | DataType::Float16 => true,
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            contains_incompatible(f.data_type())
        }
        DataType::Map(f, _) => contains_incompatible(f.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| contains_incompatible(f.data_type())),
        DataType::Union(fields, _) => fields.iter().any(|(_, f)| contains_incompatible(f.data_type())),
        DataType::Dictionary(_, value_type) => contains_incompatible(value_type),
        _ => false,
    }
}

fn rewrite_field(field: &Field) -> Field {
    let new_type = rewrite_data_type(field.data_type());
    Field::new(field.name(), new_type, field.is_nullable())
        .with_metadata(field.metadata().clone())
}

fn rewrite_data_type(dt: &DataType) -> DataType {
    match dt {
        DataType::BinaryView => DataType::Binary,
        DataType::UInt64 => DataType::Int64,
        DataType::Float16 => DataType::Float32,
        DataType::List(f) => DataType::List(Arc::new(rewrite_field(f))),
        DataType::LargeList(f) => DataType::LargeList(Arc::new(rewrite_field(f))),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(Arc::new(rewrite_field(f)), *n),
        DataType::Map(f, sorted) => DataType::Map(Arc::new(rewrite_field(f)), *sorted),
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields.iter().map(|f| rewrite_field(f)).collect();
            DataType::Struct(Fields::from(new_fields))
        }
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(rewrite_data_type(value)))
        }
        other => other.clone(),
    }
}

/// Appends to `registered` any `expected` field whose name is absent, as a nullable column.
/// `Some(augmented)` if anything was added, `None` if `registered` already covers `expected`.
///
/// The Substrait consumer binds `base_schema` to the provider BY NAME, so the registered schema
/// only needs to *contain* every expected column — order is irrelevant and present columns keep
/// their inferred (coerced) types. Appended columns are forced nullable; DataFusion's parquet
/// `SchemaAdapter` null-fills them at read time.
pub fn append_missing_nullable(registered: &Schema, expected: &Schema) -> Option<SchemaRef> {
    let mut added: Vec<Field> = Vec::new();
    for ef in expected.fields() {
        if registered.field_with_name(ef.name()).is_err() {
            added.push(
                Field::new(ef.name(), ef.data_type().clone(), true).with_metadata(ef.metadata().clone()),
            );
        }
    }
    if added.is_empty() {
        return None;
    }
    let mut fields: Vec<Field> = registered.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.extend(added);
    Some(Arc::new(Schema::new_with_metadata(fields, registered.metadata().clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, UInt64Array};

    #[test]
    fn coerce_record_batch_rewrites_uint64_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("rn", DataType::UInt64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap();
        let target = coerce_inferred_schema(Arc::clone(&schema));
        assert_eq!(target.field(0).data_type(), &DataType::Int64);

        let out = coerce_record_batch(batch, &target).unwrap();
        assert_eq!(out.schema(), target);
        assert_eq!(out.column(0).data_type(), &DataType::Int64);
        let rn = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("coerced to Int64");
        assert_eq!(rn.values(), &[1i64, 2, 3]);
    }

    #[test]
    fn coerce_record_batch_passthrough_when_already_matching() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![7i64, 8]))],
        )
        .unwrap();
        let out = coerce_record_batch(batch, &schema).unwrap();
        assert_eq!(out.schema(), schema);
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn append_missing_adds_absent_columns_as_nullable() {
        let registered = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]);
        // `alias` is declared non-nullable in the plan; it must still be appended as nullable
        // since the shard has no data for it.
        let expected = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("alias", DataType::Utf8, false),
        ]);

        let merged = append_missing_nullable(&registered, &expected).expect("alias missing → augmented");
        assert_eq!(merged.fields().len(), 3);
        let alias = merged.field_with_name("alias").unwrap();
        assert_eq!(alias.data_type(), &DataType::Utf8);
        assert!(alias.is_nullable(), "appended column must be nullable");
        assert!(merged.field_with_name("name").is_ok());
        assert!(merged.field_with_name("age").is_ok());
    }

    #[test]
    fn append_missing_returns_none_when_registered_covers_expected() {
        let registered = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]);
        // Registered may carry extra columns the plan doesn't reference — still nothing to add.
        let expected = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);
        assert!(append_missing_nullable(&registered, &expected).is_none());
    }

    #[test]
    fn top_level_binary_view_gets_rewritten() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::BinaryView, true),
        ]));
        let out = coerce_inferred_schema(schema);
        assert_eq!(out.field(0).data_type(), &DataType::Int64);
        assert_eq!(out.field(1).data_type(), &DataType::Binary);
    }

    #[test]
    fn top_level_uint64_gets_rewritten() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let out = coerce_inferred_schema(schema);
        assert_eq!(out.field(0).data_type(), &DataType::Int64);
        assert_eq!(out.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn schema_without_incompatible_types_is_returned_unchanged() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8View, true),
        ]));
        let before = Arc::as_ptr(&schema);
        let out = coerce_inferred_schema(schema);
        assert_eq!(Arc::as_ptr(&out), before, "unchanged schema must not reallocate");
    }

    #[test]
    fn nested_list_of_binary_view_gets_rewritten() {
        let inner = Field::new("item", DataType::BinaryView, true);
        let schema = Arc::new(Schema::new(vec![
            Field::new("xs", DataType::List(Arc::new(inner)), true),
        ]));
        let out = coerce_inferred_schema(schema);
        match out.field(0).data_type() {
            DataType::List(f) => assert_eq!(f.data_type(), &DataType::Binary),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn nested_list_of_uint64_gets_rewritten() {
        let inner = Field::new("item", DataType::UInt64, true);
        let schema = Arc::new(Schema::new(vec![
            Field::new("xs", DataType::List(Arc::new(inner)), true),
        ]));
        let out = coerce_inferred_schema(schema);
        match out.field(0).data_type() {
            DataType::List(f) => assert_eq!(f.data_type(), &DataType::Int64),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn field_metadata_and_nullability_preserved() {
        let mut md = std::collections::HashMap::new();
        md.insert("key".to_string(), "value".to_string());
        let f = Field::new("b", DataType::BinaryView, false).with_metadata(md.clone());
        let schema = Arc::new(Schema::new(vec![f]));
        let out = coerce_inferred_schema(schema);
        assert!(!out.field(0).is_nullable());
        assert_eq!(out.field(0).metadata(), &md);
    }

    #[test]
    fn utf8_view_is_left_alone() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8View, true),
            Field::new("b", DataType::BinaryView, true),
        ]));
        let out = coerce_inferred_schema(schema);
        assert_eq!(out.field(0).data_type(), &DataType::Utf8View);
        assert_eq!(out.field(1).data_type(), &DataType::Binary);
    }

    #[test]
    fn other_unsigned_ints_are_left_alone() {
        // Only UInt64 ↔ Int64 needs coercion for OpenSearch's unsigned_long
        // mapping today. Smaller unsigned widths aren't produced by any current
        // OpenSearch field type, and Calcite has no SMALLINT-equivalent
        // unsigned type to mismatch against, so we leave them untouched.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt8, true),
            Field::new("b", DataType::UInt16, true),
            Field::new("c", DataType::UInt32, true),
        ]));
        let before = Arc::as_ptr(&schema);
        let out = coerce_inferred_schema(schema);
        assert_eq!(Arc::as_ptr(&out), before);
    }
}
