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
//! `datatype_is_logically_equal` to the schema declared in the plan. That
//! function in DataFusion 53 has hardcoded equivalences for `(Utf8, Utf8View)`
//! but not for several other Arrow/Substrait-incompatible pairs we hit:
//!
//!   - `BinaryView`  ← parquet emits this for variable-length byte columns
//!     (and for fixed-width byte arrays like OpenSearch's 16-byte `ip`) when
//!     `schema_force_view_types` is on. Substrait has no view types — isthmus
//!     serializes `VARBINARY` as plain `binary`, which arrives as
//!     `DataType::Binary`.
//!
//!   - `UInt64`      ← parquet emits this for `unsigned_long` columns.
//!     Substrait integers are signed only — Calcite `BIGINT` serializes as
//!     `i64`, which arrives as `DataType::Int64`.
//!
//! Until upstream patches add equivalence arms (mirroring the existing
//! Utf8/Utf8View pair), we normalize the inferred schema at the table-provider
//! boundary: substitute the Arrow-only types with their Substrait-compatible
//! counterparts before handing the schema to `ListingTableConfig`. The parquet
//! reader's `SchemaAdapter` inserts the per-batch cast at read time.
//!
//! Strings keep their `Utf8View` layout because the upstream Utf8/Utf8View
//! equivalence already lets them pass through without a coerce.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

/// Rewrite the schema to forms Substrait can bind against:
///   - `BinaryView` → `Binary`
///   - `UInt64`     → `Int64`
///
/// Recurses into `List`, `LargeList`, `FixedSizeList`, `Map`, `Struct`,
/// `Union`, and `Dictionary`. Returns the input unchanged when no rewrite is
/// needed so callers avoid an unnecessary `Arc` reallocation in the common
/// case.
pub fn coerce_for_substrait(schema: SchemaRef) -> SchemaRef {
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

fn schema_needs_coerce(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| contains_incompatible(f.data_type()))
}

fn contains_incompatible(dt: &DataType) -> bool {
    match dt {
        DataType::BinaryView | DataType::UInt64 => true,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_level_binary_view_gets_rewritten() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::BinaryView, true),
        ]));
        let out = coerce_for_substrait(schema);
        assert_eq!(out.field(0).data_type(), &DataType::Int64);
        assert_eq!(out.field(1).data_type(), &DataType::Binary);
    }

    #[test]
    fn top_level_uint64_gets_rewritten() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let out = coerce_for_substrait(schema);
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
        let out = coerce_for_substrait(schema);
        assert_eq!(Arc::as_ptr(&out), before, "unchanged schema must not reallocate");
    }

    #[test]
    fn nested_list_of_binary_view_gets_rewritten() {
        let inner = Field::new("item", DataType::BinaryView, true);
        let schema = Arc::new(Schema::new(vec![
            Field::new("xs", DataType::List(Arc::new(inner)), true),
        ]));
        let out = coerce_for_substrait(schema);
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
        let out = coerce_for_substrait(schema);
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
        let out = coerce_for_substrait(schema);
        assert!(!out.field(0).is_nullable());
        assert_eq!(out.field(0).metadata(), &md);
    }

    #[test]
    fn utf8_view_is_left_alone() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8View, true),
            Field::new("b", DataType::BinaryView, true),
        ]));
        let out = coerce_for_substrait(schema);
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
        let out = coerce_for_substrait(schema);
        assert_eq!(Arc::as_ptr(&out), before);
    }
}
