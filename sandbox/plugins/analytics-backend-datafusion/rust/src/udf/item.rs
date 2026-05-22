/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `item(map, key)` — extract a value from a `map<utf8, utf8>` by key. Lowering
//! target for Calcite's `SqlStdOperatorTable.ITEM` operator on map operands.
//! PPL `parse <field> '<regex>'` lowers each named group to
//! `INTERNAL_ITEM(INTERNAL_PARSE(...), '<group>')`, which DataFusionFragmentConvertor
//! routes to this UDF via the `item` extension name.
//!
//! Scope: maps from utf8 keys to utf8 values only (the type produced by the
//! `parse` UDF). Calcite's `ITEM` is also defined on arrays, but PPL never
//! emits the array form on the parse path, and array indexing doesn't share an
//! adapter — keep this UDF focused so the error message stays specific.
//!
//! Semantics:
//!   * Key not present in the map → SQL NULL output (matches Calcite's
//!     `MAP[key]` convention and the legacy PPL `RegexExpression` behaviour
//!     that produces "" for absent groups; PPL adapts this to the same
//!     observable "" downstream because the parse UDF stores "" rather than
//!     omitting the key, but `item` itself stays NULL-on-absent for any
//!     other Map producer).
//!   * Map cell is null → NULL output.
//!   * Key cell is null → NULL output (no entry can match a NULL key).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, MapArray, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ItemUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ItemUdf {
    signature: Signature,
}

impl ItemUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for ItemUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ItemUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "item"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("item expects 2 arguments, got {}", arg_types.len());
        }
        // Inner value type is what we return — the only producer in scope is
        // `parse` (utf8 values), so we hard-code Utf8 here. Generalising means
        // inspecting arg_types[0]'s Map<…, V> value field; deferred until a
        // second Map producer needs it.
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!("item expects 2 arguments, got {}", arg_types.len());
        }
        // First arg passes through as-is — DataFusion's substrait consumer
        // can't reconstruct a Map<…, …> through CoerceMode and Calcite's
        // ITEM already produces it with the right shape. Second arg
        // canonicalises to Utf8 so column-valued keys (string view, large
        // string) work uniformly.
        if !matches!(arg_types[0], DataType::Map(_, _)) {
            return plan_err!(
                "item: arg 0 must be a Map type, got {:?}",
                arg_types[0]
            );
        }
        let key_target = match arg_types[1] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            ref other => return plan_err!(
                "item: arg 1 must be a string key, got {other:?}"
            ),
        };
        Ok(vec![arg_types[0].clone(), key_target])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("item expects 2 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        let map_arr = args.args[0].clone().into_array(n)?;
        let map = map_arr
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| DataFusionError::Internal(format!(
                "item: arg 0 expected MapArray, got {:?}",
                map_arr.data_type(),
            )))?;

        // Fast-path the common case: literal key (every PPL parse → ITEM call
        // hits this, since the named-group identifier is a string literal).
        // Pull it once here; column-valued keys are rare so the slow path is
        // simpler rather than micro-optimised.
        let scalar_key = scalar_str(&args.args[1]);

        let key_arr_ref: Option<ArrayRef> = if scalar_key.is_none() {
            Some(args.args[1].clone().into_array(n)?)
        } else {
            None
        };
        let key_arr: Option<&StringArray> = key_arr_ref
            .as_ref()
            .and_then(|a| a.as_any().downcast_ref::<StringArray>());

        let mut builder = StringBuilder::with_capacity(n, 0);
        for i in 0..n {
            if map.is_null(i) {
                builder.append_null();
                continue;
            }
            let key: &str = match (&scalar_key, key_arr) {
                (Some(k), _) => k.as_str(),
                (None, Some(a)) if !a.is_null(i) => a.value(i),
                _ => {
                    builder.append_null();
                    continue;
                }
            };

            // MapArray entries for row i live at offsets[i]..offsets[i+1] of
            // the underlying StructArray. Walk those entries linearly looking
            // for the requested key. Linear walk is fine for the parse use
            // case because the map has at most a handful of named groups.
            let entries = map.value(i);
            let keys = entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal(
                    "item: map keys are not Utf8".into(),
                ))?;
            let values = entries
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal(
                    "item: map values are not Utf8".into(),
                ))?;

            let mut found = false;
            for j in 0..entries.len() {
                if !keys.is_null(j) && keys.value(j) == key {
                    if values.is_null(j) {
                        builder.append_null();
                    } else {
                        builder.append_value(values.value(j));
                    }
                    found = true;
                    break;
                }
            }
            if !found {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn scalar_str(cv: &ColumnarValue) -> Option<String> {
    let ColumnarValue::Scalar(sv) = cv else {
        return None;
    };
    match sv {
        ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) | ScalarValue::Utf8View(opt) => {
            opt.clone()
        }
        _ => None,
    }
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{MapBuilder, MapFieldNames};
    use datafusion::arrow::datatypes::Field;

    fn build_map(rows: Vec<Option<Vec<(&str, &str)>>>) -> MapArray {
        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        );
        for row in rows {
            match row {
                Some(entries) => {
                    for (k, v) in entries {
                        builder.keys().append_value(k);
                        builder.values().append_value(v);
                    }
                    builder.append(true).unwrap();
                }
                None => {
                    builder.append(false).unwrap();
                }
            }
        }
        builder.finish()
    }

    fn run(map: MapArray, key: ColumnarValue) -> StringArray {
        let udf = ItemUdf::new();
        let n = map.len();
        let map_dt = map.data_type().clone();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(map)), key],
            number_rows: n,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let _ = map_dt;
        let out = udf.invoke_with_args(args).unwrap();
        match out {
            ColumnarValue::Array(a) => a.as_any().downcast_ref::<StringArray>().unwrap().clone(),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn returns_value_for_present_key() {
        let map = build_map(vec![Some(vec![("a", "1"), ("b", "2")])]);
        let out = run(map, ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".into()))));
        assert_eq!(out.value(0), "1");
    }

    #[test]
    fn returns_null_for_absent_key() {
        let map = build_map(vec![Some(vec![("a", "1")])]);
        let out = run(map, ColumnarValue::Scalar(ScalarValue::Utf8(Some("missing".into()))));
        assert!(out.is_null(0));
    }

    #[test]
    fn returns_null_for_null_map_cell() {
        let map = build_map(vec![None]);
        let out = run(map, ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".into()))));
        assert!(out.is_null(0));
    }

    #[test]
    fn returns_null_for_null_key_scalar() {
        let map = build_map(vec![Some(vec![("a", "1")])]);
        let out = run(map, ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        assert!(out.is_null(0));
    }

    #[test]
    fn handles_column_valued_keys_per_row() {
        let map = build_map(vec![
            Some(vec![("a", "1"), ("b", "2")]),
            Some(vec![("a", "3")]),
        ]);
        let key_arr = StringArray::from(vec![Some("b"), Some("a")]);
        let out = run(map, ColumnarValue::Array(Arc::new(key_arr)));
        assert_eq!(out.value(0), "2");
        assert_eq!(out.value(1), "3");
    }

    #[test]
    fn return_type_is_value_type_of_map() {
        let udf = ItemUdf::new();
        let key = Arc::new(Field::new("key", DataType::Utf8, false));
        let value = Arc::new(Field::new("value", DataType::Utf8, true));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![key, value].into()),
            false,
        ));
        let map_dt = DataType::Map(entries, false);
        assert_eq!(
            udf.return_type(&[map_dt, DataType::Utf8]).unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn coerce_types_rejects_non_map_first_arg() {
        let udf = ItemUdf::new();
        let err = udf
            .coerce_types(&[DataType::Utf8, DataType::Utf8])
            .unwrap_err();
        assert!(err.to_string().contains("must be a Map type"));
    }

    #[test]
    fn coerce_types_rejects_non_string_key() {
        let udf = ItemUdf::new();
        let key = Arc::new(Field::new("key", DataType::Utf8, false));
        let value = Arc::new(Field::new("value", DataType::Utf8, true));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![key, value].into()),
            false,
        ));
        let map_dt = DataType::Map(entries, false);
        let err = udf
            .coerce_types(&[map_dt, DataType::Int32])
            .unwrap_err();
        assert!(err.to_string().contains("must be a string key"));
    }
}
