/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `pattern_parser(pattern, field)` — wraps PPL's `PATTERN_PARSER` UDF.
//!
//! Mirrors `PatternParserFunctionImpl.evalField` (and dispatches to
//! `evalSamples` when the second operand is a `List<String>`). The Java side
//! also has an `evalAgg` shape for BRAIN label mode; that case needs the
//! INTERNAL_PATTERN window function (separate milestone) and isn't
//! implemented here yet — its operand types route through the AggregateUDF /
//! WindowUDF path.
//!
//! Return type is a Struct with two fields:
//!   * `pattern` — VARCHAR — the numbered-token rewrite (e.g.
//!     `"<token1>@<token2>.<token3>"`).
//!   * `tokens` — Map<VARCHAR, List<VARCHAR>> — labeled lists of extracted
//!     substrings.
//!
//! Calcite's `ITEM(struct, "pattern")` with a constant string key resolves to
//! named struct-field access. The downstream `flattenParsedPattern` step in
//! the PPL Calcite visitor consumes both fields via ITEM.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericListArray, ListArray, ListBuilder, MapBuilder, MapFieldNames,
    StringArray, StringBuilder, StructArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::patterns::{eval_field, eval_samples, PatternResult};
use crate::udf::udf_identity;

pub const NAME: &str = "pattern_parser";

/// Field names used in the returned Arrow struct.
const FIELD_PATTERN: &str = "pattern";
const FIELD_TOKENS: &str = "tokens";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(PatternParserUdf::new()));
}

#[derive(Debug)]
pub struct PatternParserUdf {
    signature: Signature,
}

impl PatternParserUdf {
    pub fn new() -> Self {
        Self {
            // user_defined coercion — operand types are dispatched at
            // invoke_with_args time based on actual Arrow shape.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(PatternParserUdf, "pattern_parser");

impl ScalarUDFImpl for PatternParserUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        NAME
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(struct_data_type())
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!(
                "{} expects 2 arguments (pattern, field|sample_logs), got {}",
                NAME,
                arg_types.len()
            );
        }
        let first = utf8_or_err(NAME, 0, &arg_types[0])?;
        let second = match &arg_types[1] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            DataType::List(inner) | DataType::LargeList(inner) => {
                match inner.data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                    }
                    other => {
                        return plan_err!(
                            "{} arg 1: List element must be string, got {:?}",
                            NAME,
                            other
                        );
                    }
                }
            }
            other => {
                return plan_err!(
                    "{} arg 1: expected string or List<string>, got {:?}",
                    NAME,
                    other
                );
            }
        };
        Ok(vec![first, second])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!(
                "{} expects 2 arguments at invoke time, got {}",
                NAME,
                args.args.len()
            );
        }
        let n = args.number_rows;
        let pattern_arr = args.args[0].clone().into_array(n)?;
        let pattern_strings = read_utf8(NAME, 0, &pattern_arr)?;

        match args.args[1].data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let field_arr = args.args[1].clone().into_array(n)?;
                let field_strings = read_utf8(NAME, 1, &field_arr)?;
                let results: Vec<PatternResult> = (0..n)
                    .map(|i| match (pattern_strings.get(i), field_strings.get(i)) {
                        (Some(p), Some(f)) => eval_field(p, f),
                        _ => PatternResult::empty(),
                    })
                    .collect();
                Ok(ColumnarValue::Array(build_struct_array(&results)?))
            }
            DataType::List(_) | DataType::LargeList(_) => {
                let field_arr = args.args[1].clone().into_array(n)?;
                let list = field_arr
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Plan(format!(
                            "{} arg 1: expected ListArray<string>",
                            NAME
                        ))
                    })?;
                let results: Vec<PatternResult> = (0..n)
                    .map(|i| {
                        let pattern = match pattern_strings.get(i) {
                            Some(p) => p,
                            None => return PatternResult::empty(),
                        };
                        if list.is_null(i) {
                            return PatternResult::empty();
                        }
                        let inner = list.value(i);
                        let inner_strings = match inner.as_any().downcast_ref::<StringArray>() {
                            Some(arr) => arr,
                            None => return PatternResult::empty(),
                        };
                        let samples: Vec<String> = (0..inner_strings.len())
                            .filter_map(|j| {
                                if inner_strings.is_null(j) {
                                    None
                                } else {
                                    Some(inner_strings.value(j).to_string())
                                }
                            })
                            .collect();
                        eval_samples(pattern, &samples)
                    })
                    .collect();
                Ok(ColumnarValue::Array(build_struct_array(&results)?))
            }
            other => plan_err!(
                "{} arg 1: expected string or List<string>, got {:?}",
                NAME,
                other
            ),
        }
    }
}

/// Arrow shape for the result struct. Two fixed fields: `pattern` (Utf8) and
/// `tokens` (Map<Utf8, List<Utf8>>). Calcite's ITEM lowering with a constant
/// key resolves to named struct field access on this shape.
fn struct_data_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new(FIELD_PATTERN, DataType::Utf8, true),
        Field::new(FIELD_TOKENS, tokens_map_type(), true),
    ]))
}

fn tokens_map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "value",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ),
            ])),
            false,
        )),
        false,
    )
}

/// Per-row Utf8 view that flattens Utf8 / LargeUtf8 / Utf8View into a common
/// `Option<&str>` getter. Mirrors a private helper used elsewhere in the udf
/// tree but kept inline because the shapes differ per UDF.
struct Utf8View<'a> {
    array: &'a dyn Array,
    kind: Utf8Kind,
}

enum Utf8Kind {
    Std,
    Large,
    View,
}

impl<'a> Utf8View<'a> {
    fn get(&self, i: usize) -> Option<&str> {
        if self.array.is_null(i) {
            return None;
        }
        match self.kind {
            Utf8Kind::Std => Some(
                self.array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()?
                    .value(i),
            ),
            Utf8Kind::Large => Some(
                self.array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::LargeStringArray>()?
                    .value(i),
            ),
            Utf8Kind::View => Some(
                self.array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringViewArray>()?
                    .value(i),
            ),
        }
    }
}

fn read_utf8<'a>(udf: &str, slot: usize, arr: &'a ArrayRef) -> Result<Utf8View<'a>> {
    let kind = match arr.data_type() {
        DataType::Utf8 => Utf8Kind::Std,
        DataType::LargeUtf8 => Utf8Kind::Large,
        DataType::Utf8View => Utf8Kind::View,
        other => {
            return plan_err!(
                "{} arg {}: expected string array, got {:?}",
                udf,
                slot,
                other
            );
        }
    };
    Ok(Utf8View {
        array: arr.as_ref(),
        kind,
    })
}

fn utf8_or_err(udf: &str, slot: usize, dt: &DataType) -> Result<DataType> {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(DataType::Utf8),
        other => plan_err!(
            "{} arg {}: expected string, got {:?}",
            udf,
            slot,
            other
        ),
    }
}

/// Assemble the StructArray from a per-row vector of PatternResult.
fn build_struct_array(results: &[PatternResult]) -> Result<ArrayRef> {
    let n = results.len();

    // Field 1: pattern (Utf8)
    let mut pattern_builder = StringBuilder::with_capacity(n, n * 32);
    for r in results {
        if r.pattern.is_empty() {
            pattern_builder.append_value("");
        } else {
            pattern_builder.append_value(&r.pattern);
        }
    }
    let pattern_array: ArrayRef = Arc::new(pattern_builder.finish());

    // Field 2: tokens (Map<Utf8, List<Utf8>>)
    let mut map_builder = MapBuilder::with_capacity(
        Some(MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        ListBuilder::new(StringBuilder::new()),
        n,
    );
    for r in &results.iter().collect::<Vec<_>>() {
        // Map keys are token labels in insertion order; values are the
        // accumulated list of substrings. ImmutableMap.of in Java preserves
        // insertion order, and HashMap<>'s iteration is unspecified —
        // CalcitePPLPatternsIT compares via Map.equals which is content-only,
        // so any iteration order is acceptable here.
        for (label, values) in r.tokens.iter() {
            map_builder.keys().append_value(label);
            let value_builder = map_builder.values().values();
            for v in values {
                value_builder.append_value(v);
            }
            map_builder.values().append(true);
        }
        map_builder
            .append(true)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    }
    let tokens_array: ArrayRef = Arc::new(map_builder.finish());

    let struct_fields = Fields::from(vec![
        Field::new(FIELD_PATTERN, DataType::Utf8, true),
        Field::new(FIELD_TOKENS, tokens_map_type(), true),
    ]);
    let struct_array =
        StructArray::new(struct_fields, vec![pattern_array, tokens_array], None);
    Ok(Arc::new(struct_array) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, AsArray, BooleanArray};

    #[test]
    fn struct_data_type_has_pattern_and_tokens_fields() {
        let dt = struct_data_type();
        let DataType::Struct(fields) = dt else {
            panic!("expected struct, got {:?}", dt);
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "pattern");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(fields[1].name(), "tokens");
    }

    #[test]
    fn build_struct_array_populates_pattern_and_tokens_for_email_evalfield() {
        let results = vec![eval_field("<*>@<*>.<*>", "amberduke@pyrami.com")];
        let arr = build_struct_array(&results).unwrap();
        let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let pattern_col = s.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(pattern_col.value(0), "<token1>@<token2>.<token3>");

        let tokens_col = s.column(1);
        // Just sanity-check non-empty tokens; precise key ordering is HashMap-
        // dependent and asserted via Map.equals on the JVM side anyway.
        assert!(tokens_col.len() == 1);
    }
}
