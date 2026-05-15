/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `parse(input, pattern_lit, method_lit)` — extract named regex groups into a
//! `map<varchar, varchar>`. Lowering target for PPL's `parse <field> '<regex>'`
//! command.
//!
//! # Division of labor with the Java adapter
//!
//! Plan-time work (Java `ParseAdapter`):
//!   * Verify `pattern` and `method` operands are string literals — column-valued
//!     pattern or method makes no sense for parse and is rejected up front.
//!   * Reject methods other than `regex` with a clear error mentioning
//!     `grok` / `patterns` aren't on the analytics path yet.
//!
//! Runtime work (this file):
//!   * Compile the regex once per call (literal pattern from the adapter), anchor
//!     it with `^…$` so the match consumes the entire input — equivalent to
//!     Java's `Matcher.matches()` semantic that PPL's `RegexExpression` relies on.
//!   * For every input row, populate a `map<utf8, utf8>` with one entry per
//!     named group in pattern definition order. A row that fails to match (or
//!     a group that didn't participate in the match) yields `""` for that
//!     group's value — same behaviour as the legacy `RegexExpression`
//!     where `extractNamedGroup` returns null and `ParseFunction` wraps that
//!     to `ExprStringValue("")`.
//!   * A SQL NULL input row is treated like a row that didn't match: every
//!     named group still appears in the map with `""` as its value. Mirrors
//!     `ParseFunction.parse(null, …)` in core PPL.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MapBuilder, MapFieldNames, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex::Regex;

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ParseUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseUdf {
    signature: Signature,
}

impl ParseUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for ParseUdf {
    fn default() -> Self {
        Self::new()
    }
}

/// Arrow `Map<Utf8, Utf8>` field shape returned by this UDF — keys non-null,
/// values nullable. Constructed once per call rather than per row.
fn map_return_type() -> DataType {
    let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
    let value_field = Arc::new(Field::new("value", DataType::Utf8, true));
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(vec![key_field, value_field].into()),
        false,
    ));
    DataType::Map(entries_field, false)
}

impl ScalarUDFImpl for ParseUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "parse"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("parse expects 3 arguments, got {}", arg_types.len());
        }
        Ok(map_return_type())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "parse",
            arg_types,
            &[CoerceMode::Utf8, CoerceMode::Utf8, CoerceMode::Utf8],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return plan_err!("parse expects 3 arguments, got {}", args.args.len());
        }

        // The Java adapter validates that pattern + method are non-null string
        // literals at plan time. Re-check defensively so a misuse from an
        // unknown caller produces an actionable plan error instead of a panic.
        let pattern = scalar_str(&args.args[1])
            .ok_or_else(|| DataFusionError::Plan(
                "parse: pattern must be a non-null string literal".into(),
            ))?;
        let method = scalar_str(&args.args[2])
            .ok_or_else(|| DataFusionError::Plan(
                "parse: method must be a non-null string literal".into(),
            ))?;

        if method != "regex" {
            return Err(DataFusionError::Plan(format!(
                "parse: method '{method}' is not supported by the analytics engine; \
                 only 'regex' is currently supported (use 'grok' / 'patterns' on the legacy engine)",
            )));
        }

        // Anchor pattern so we honour Java's `Matcher.matches()` semantics:
        // the match must consume the entire input. Wrapping in a non-capturing
        // group is safe — it does not perturb the named-group indexing the
        // caller relies on.
        let anchored = format!("^(?:{pattern})$");
        let regex = Regex::new(&anchored).map_err(|e| {
            DataFusionError::Plan(format!("parse: invalid regex [{pattern}]: {e}"))
        })?;

        // Collect named groups in pattern definition order so the resulting
        // map's key order matches what users see in legacy PPL output.
        let group_names: Vec<String> = regex
            .capture_names()
            .filter_map(|name| name.map(String::from))
            .collect();
        if group_names.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "parse: regex [{pattern}] has no named capture groups",
            )));
        }

        let n = args.number_rows;
        let input_arr = args.args[0].clone().into_array(n)?;
        let input = input_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal(format!(
                "parse: expected Utf8 input, got {:?}",
                input_arr.data_type(),
            )))?;

        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        );

        for i in 0..n {
            // For each row emit one entry per group_name. A null input row,
            // a non-matching row, and a row where the group did not
            // participate all collapse to "" for that group's value — this
            // mirrors `extractNamedGroup` returning null and ParseFunction
            // wrapping it to ExprStringValue("").
            let captures = if input.is_null(i) {
                None
            } else {
                regex.captures(input.value(i))
            };
            for name in &group_names {
                builder.keys().append_value(name);
                let value = match &captures {
                    Some(c) => c.name(name).map(|m| m.as_str()).unwrap_or(""),
                    None => "",
                };
                builder.values().append_value(value);
            }
            // append(true) closes the map entry for this row. The map cell
            // itself is never null — see the comment above for why.
            builder.append(true)?;
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Pull the string value out of a literal column. Returns None if the column
/// is non-scalar, a non-string scalar, or a SQL NULL — the adapter is meant to
/// have caught these at plan time but we re-check at runtime.
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
    use datafusion::arrow::array::MapArray;
    use datafusion::arrow::datatypes::TimeUnit;

    fn run_parse(
        input: Vec<Option<&str>>,
        pattern: &str,
        method: &str,
    ) -> Result<MapArray> {
        let udf = ParseUdf::new();
        let n = input.len();
        let input_arr = StringArray::from(input);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(input_arr)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(method.to_string()))),
            ],
            number_rows: n,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("out", map_return_type(), true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args)?;
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        Ok(arr.as_any().downcast_ref::<MapArray>().unwrap().clone())
    }

    fn entries_of(map: &MapArray, row: usize) -> Vec<(String, String)> {
        let entries = map.value(row);
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..entries.len())
            .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
            .collect()
    }

    #[test]
    fn extracts_named_groups_in_pattern_order() {
        let map = run_parse(
            vec![Some("ip=10.0.0.1 port=443")],
            r"ip=(?<ip>\S+) port=(?<port>\d+)",
            "regex",
        )
        .unwrap();
        assert_eq!(
            entries_of(&map, 0),
            vec![
                ("ip".to_string(), "10.0.0.1".to_string()),
                ("port".to_string(), "443".to_string()),
            ]
        );
    }

    #[test]
    fn whole_string_anchoring_is_enforced() {
        // Same pattern — the trailing junk means the row should NOT match,
        // so all named groups are emitted with "".
        let map = run_parse(
            vec![Some("ip=10.0.0.1 port=443 trailing-junk")],
            r"ip=(?<ip>\S+) port=(?<port>\d+)",
            "regex",
        )
        .unwrap();
        assert_eq!(
            entries_of(&map, 0),
            vec![
                ("ip".to_string(), "".to_string()),
                ("port".to_string(), "".to_string()),
            ]
        );
    }

    #[test]
    fn null_input_emits_empty_values_with_full_schema() {
        // Mirrors ParseFunction.parse(null, …) in core PPL — the map cell is
        // present (never null) and every named group key is there with "".
        let map = run_parse(
            vec![None, Some("name=alice")],
            r"name=(?<name>\w+)",
            "regex",
        )
        .unwrap();
        assert!(!map.is_null(0)); // row 0 cell is present
        assert_eq!(entries_of(&map, 0), vec![("name".to_string(), "".to_string())]);
        assert_eq!(entries_of(&map, 1), vec![("name".to_string(), "alice".to_string())]);
    }

    #[test]
    fn non_regex_method_is_rejected_with_actionable_message() {
        let err = run_parse(vec![Some("x")], r"(?<x>.)", "grok").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("'grok'"), "{msg}");
        assert!(msg.contains("'regex'"), "{msg}");
    }

    #[test]
    fn invalid_regex_surfaces_as_plan_error() {
        let err = run_parse(vec![Some("x")], r"(?<x>[unbalanced", "regex").unwrap_err();
        assert!(err.to_string().contains("invalid regex"));
    }

    #[test]
    fn pattern_without_named_groups_is_rejected() {
        // PPL parse without named groups is a user error: the result map would
        // be empty and the downstream INTERNAL_ITEM(parse(...), name) lookup
        // would always return NULL. Fail loudly at plan time.
        let err = run_parse(vec![Some("x")], r"\w+", "regex").unwrap_err();
        assert!(err.to_string().contains("no named capture groups"));
    }

    #[test]
    fn return_type_is_map_utf8_utf8() {
        let udf = ParseUdf::new();
        let dt = udf
            .return_type(&[DataType::Utf8, DataType::Utf8, DataType::Utf8])
            .unwrap();
        assert_eq!(dt, map_return_type());
    }

    #[test]
    fn coerce_types_normalises_string_variants() {
        let udf = ParseUdf::new();
        let out = udf
            .coerce_types(&[DataType::LargeUtf8, DataType::Utf8View, DataType::Utf8])
            .unwrap();
        assert_eq!(
            out,
            vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]
        );
    }

    #[test]
    fn coerce_types_rejects_non_string_input() {
        let udf = ParseUdf::new();
        let err = udf
            .coerce_types(&[
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Utf8,
                DataType::Utf8,
            ])
            .unwrap_err();
        assert!(err.to_string().contains("expected string"));
    }

    #[test]
    fn return_type_rejects_wrong_arity() {
        let udf = ParseUdf::new();
        assert!(udf.return_type(&[DataType::Utf8, DataType::Utf8]).is_err());
    }
}
