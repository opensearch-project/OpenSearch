/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `rex_extract_multi(input, pattern, group, max_match)` — extract up to
//! `max_match` occurrences of a regex named or numbered capture group from
//! `input`, returned as a `list<varchar>`.
//!
//! Lowering target for PPL's `rex field=f "(?<g>...)" max_match=N`. Mirrors
//! the SQL plugin's `RexExtractMultiFunction.extractMultipleGroups`.
//!
//! `max_match=0` is unbounded (matches the Java semantics:
//! `(maxMatch == 0 || matchCount < maxMatch)` in the loop guard).
//!
//! Returns NULL (not an empty list) when no matches are found — matches the
//! Java UDF's `matches.isEmpty() ? null : matches` final return.
//!
//! Pattern compilation errors surface as planning errors (same as
//! `rex_extract`). Group resolution semantics match `rex_extract`: numeric
//! string → 1-based index, otherwise named-group lookup.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex::Regex;

use super::{coerce_args, CoerceMode};
use super::rex_extract::compile_pattern;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(RexExtractMultiUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RexExtractMultiUdf {
    signature: Signature,
}

impl RexExtractMultiUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for RexExtractMultiUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for RexExtractMultiUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "rex_extract_multi"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 4 {
            return plan_err!("rex_extract_multi expects 4 arguments, got {}", arg_types.len());
        }
        // List<String?> — element type is nullable Utf8 to match the Java
        // signature `ARRAY(VARCHAR_2000_NULLABLE)`.
        Ok(DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "rex_extract_multi",
            arg_types,
            &[
                CoerceMode::Utf8,
                CoerceMode::Utf8,
                CoerceMode::Utf8,
                CoerceMode::Int64,
            ],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 4 {
            return plan_err!("rex_extract_multi expects 4 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        let pattern_scalar = utf8_scalar(&args.args[1]);
        let group_scalar = utf8_scalar(&args.args[2]);
        let max_match_scalar = i64_scalar(&args.args[3]);

        let scalar_regex = match &pattern_scalar {
            Some(p) => Some(compile_pattern(p)?),
            None => None,
        };

        let input = args.args[0].clone().into_array(n)?;
        let input = input
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "rex_extract_multi: expected Utf8 input, got {:?}",
                    input.data_type()
                ))
            })?;

        let pattern_arr_ref: Option<ArrayRef> = if pattern_scalar.is_none() && matches!(&args.args[1], ColumnarValue::Array(_)) {
            Some(args.args[1].clone().into_array(n)?)
        } else {
            None
        };
        let group_arr_ref: Option<ArrayRef> = if group_scalar.is_none() && matches!(&args.args[2], ColumnarValue::Array(_)) {
            Some(args.args[2].clone().into_array(n)?)
        } else {
            None
        };
        let max_match_arr_ref: Option<ArrayRef> = if max_match_scalar.is_none() && matches!(&args.args[3], ColumnarValue::Array(_)) {
            Some(args.args[3].clone().into_array(n)?)
        } else {
            None
        };
        let pattern_array: Option<&StringArray> = pattern_arr_ref.as_ref().and_then(|a| a.as_any().downcast_ref::<StringArray>());
        let group_array: Option<&StringArray> = group_arr_ref.as_ref().and_then(|a| a.as_any().downcast_ref::<StringArray>());
        // After coerce_types(Int64) the array may arrive as Int32 in some plans;
        // accept either by widening on read.
        let max_match_array_i32: Option<&Int32Array> = max_match_arr_ref
            .as_ref()
            .and_then(|a| a.as_any().downcast_ref::<Int32Array>());

        let value_builder = StringBuilder::new();
        let mut builder = ListBuilder::new(value_builder).with_field(Arc::new(Field::new(
            "item", DataType::Utf8, true,
        )));

        for i in 0..n {
            if input.is_null(i) {
                builder.append_null();
                continue;
            }

            let regex_owned;
            let regex: &Regex = match (&scalar_regex, pattern_array) {
                (Some(r), _) => r,
                (None, Some(arr)) if !arr.is_null(i) => {
                    regex_owned = compile_pattern(arr.value(i))?;
                    &regex_owned
                }
                _ => {
                    builder.append_null();
                    continue;
                }
            };

            let group_name: &str = match (&group_scalar, group_array) {
                (Some(g), _) => g.as_str(),
                (None, Some(arr)) if !arr.is_null(i) => arr.value(i),
                _ => {
                    builder.append_null();
                    continue;
                }
            };

            let max_match: i64 = match (max_match_scalar, max_match_array_i32) {
                (Some(m), _) => m,
                (None, Some(arr)) if !arr.is_null(i) => arr.value(i) as i64,
                _ => {
                    builder.append_null();
                    continue;
                }
            };

            let matches = collect_matches(regex, input.value(i), group_name, max_match);
            if matches.is_empty() {
                builder.append_null();
            } else {
                for m in matches {
                    builder.values().append_value(m);
                }
                builder.append(true);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Collect up to `max_match` occurrences of the named-or-numbered group across
/// the input. `max_match == 0` is unbounded (matches the Java semantics in the
/// `(maxMatch == 0 || matchCount < maxMatch)` loop guard).
///
/// Visible for unit testing — the iteration logic is the substantive part.
pub(crate) fn collect_matches<'a>(
    regex: &Regex,
    input: &'a str,
    group: &str,
    max_match: i64,
) -> Vec<&'a str> {
    let parsed_index = group.parse::<usize>().ok().filter(|i| *i >= 1);
    let mut out: Vec<&'a str> = Vec::new();
    for caps in regex.captures_iter(input) {
        let value: Option<&str> = match parsed_index {
            Some(idx) => caps.get(idx).map(|m| m.as_str()),
            None => caps.name(group).map(|m| m.as_str()),
        };
        match value {
            Some(s) => {
                out.push(s);
                if max_match > 0 && (out.len() as i64) >= max_match {
                    break;
                }
            }
            None => {
                // Java behavior: a `null` from the extractor breaks the loop
                // (treats it as "this group doesn't exist anywhere"). Without
                // this guard, an unknown group name would loop forever
                // accumulating no matches.
                break;
            }
        }
    }
    out
}

fn utf8_scalar(cv: &ColumnarValue) -> Option<String> {
    if let ColumnarValue::Scalar(sv) = cv {
        match sv {
            ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) | ScalarValue::Utf8View(opt) => {
                opt.clone()
            }
            _ => None,
        }
    } else {
        None
    }
}

fn i64_scalar(cv: &ColumnarValue) -> Option<i64> {
    if let ColumnarValue::Scalar(sv) = cv {
        match sv {
            ScalarValue::Int8(Some(v)) => Some(*v as i64),
            ScalarValue::Int16(Some(v)) => Some(*v as i64),
            ScalarValue::Int32(Some(v)) => Some(*v as i64),
            ScalarValue::Int64(Some(v)) => Some(*v),
            ScalarValue::UInt8(Some(v)) => Some(*v as i64),
            ScalarValue::UInt16(Some(v)) => Some(*v as i64),
            ScalarValue::UInt32(Some(v)) => Some(*v as i64),
            ScalarValue::UInt64(Some(v)) => Some(*v as i64),
            _ => None,
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udf::rex_extract::compile_pattern;

    #[test]
    fn collect_unbounded_returns_every_match() {
        let r = compile_pattern("(?<n>\\d+)").unwrap();
        assert_eq!(
            collect_matches(&r, "12 then 34 then 56", "n", 0),
            vec!["12", "34", "56"]
        );
    }

    #[test]
    fn collect_bounded_caps_at_max_match() {
        let r = compile_pattern("(?<n>\\d+)").unwrap();
        assert_eq!(
            collect_matches(&r, "12 then 34 then 56", "n", 2),
            vec!["12", "34"]
        );
    }

    #[test]
    fn collect_returns_empty_when_no_match() {
        let r = compile_pattern("(?<n>\\d+)").unwrap();
        assert!(collect_matches(&r, "no digits", "n", 0).is_empty());
    }

    #[test]
    fn collect_breaks_on_unknown_group() {
        // Java behavior: `Matcher.group(name)` throws IAE for unknown names,
        // which the Java extractor catches and returns null, breaking the loop.
        // The Rust `caps.name(...)` returns None for unknown names; same break.
        let r = compile_pattern("(\\d+)").unwrap();
        assert!(collect_matches(&r, "12 34 56", "missing", 0).is_empty());
    }

    #[test]
    fn collect_numeric_index_works_for_each_match() {
        let r = compile_pattern("(\\w+)=(\\w+)").unwrap();
        assert_eq!(
            collect_matches(&r, "a=1 b=2 c=3", "2", 0),
            vec!["1", "2", "3"]
        );
    }
}
