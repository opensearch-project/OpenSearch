/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `rex_extract(input, pattern, group)` — extract the first occurrence of a
//! regex named or numbered capture group from `input`.
//!
//! Lowering target for PPL's `rex field=f "(?<g>...)"` extract command
//! (single-match form). Mirrors the SQL plugin's
//! `org.opensearch.sql.expression.function.udf.RexExtractFunction.extractGroup`.
//!
//! # Division of labor with the Java adapter
//!
//! `pattern` and `group` are validated as string literals at plan time by
//! `RexExtractAdapter` on the Java side — column-valued patterns/groups are
//! rejected before substrait serialization because the regex is compiled
//! per-call here, not per-row, and a column-valued pattern would either
//! recompile at every row (catastrophic) or silently degrade. The Rust UDF
//! treats every operand as if it were a column ref defensively, but in
//! practice positions 1 and 2 always arrive as scalars.
//!
//! # Group resolution
//!
//! The Calcite visitor (CalciteRelNodeVisitor.visitRex) parses named-group
//! candidates from the pattern up front and emits one call per group with
//! `group` as the literal group name. So `group` is always a string here
//! (named-group lookup). If `group` parses as a positive integer, this UDF
//! falls back to numbered-group lookup — matches the Java UDF's two
//! signatures (`int groupIndex` and `String groupName`).
//!
//! Returns NULL when:
//!   * `input` or `pattern` is NULL
//!   * the pattern doesn't match `input`
//!   * the requested group doesn't exist in the pattern
//!   * the pattern matches but the requested group didn't participate

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex::Regex;

use super::json_common::StringArrayView;
use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(RexExtractUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RexExtractUdf {
    signature: Signature,
}

impl RexExtractUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for RexExtractUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for RexExtractUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "rex_extract"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("rex_extract expects 3 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "rex_extract",
            arg_types,
            &[CoerceMode::Utf8, CoerceMode::Utf8, CoerceMode::Utf8],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return plan_err!("rex_extract expects 3 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        // Pattern and group are typically scalars at this point — Java adapter
        // enforced literal-only at plan time. Compile the regex once; reuse for
        // every input row. If pattern is column-valued (a degenerate case), we
        // fall back to per-row compile (slow but correct).
        let pattern_scalar = utf8_scalar(&args.args[1]);
        let group_scalar = utf8_scalar(&args.args[2]);

        let scalar_regex = match &pattern_scalar {
            Some(p) => Some(compile_pattern(p)?),
            None => None,
        };

        let input_arr = args.args[0].clone().into_array(n)?;
        let input = StringArrayView::from_array(&input_arr)?;

        // Materialize column-valued operands lazily; keep the ArrayRef alive
        // alongside the StringArrayView (the view borrows the underlying buffers).
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
        let pattern_array: Option<StringArrayView<'_>> =
            pattern_arr_ref.as_ref().map(StringArrayView::from_array).transpose()?;
        let group_array: Option<StringArrayView<'_>> =
            group_arr_ref.as_ref().map(StringArrayView::from_array).transpose()?;

        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            let Some(input_value) = input.cell(i) else {
                builder.append_null();
                continue;
            };

            // Resolve the regex — scalar fast-path or per-row column lookup.
            let regex_owned;
            let regex: &Regex = match (&scalar_regex, pattern_array.as_ref().and_then(|a| a.cell(i))) {
                (Some(r), _) => r,
                (None, Some(s)) => {
                    regex_owned = compile_pattern(s)?;
                    &regex_owned
                }
                _ => {
                    builder.append_null();
                    continue;
                }
            };

            // Resolve the group name (or numeric index, if it parses as one).
            let group_name: &str = match (&group_scalar, group_array.as_ref().and_then(|a| a.cell(i))) {
                (Some(g), _) => g.as_str(),
                (None, Some(s)) => s,
                _ => {
                    builder.append_null();
                    continue;
                }
            };

            match extract_group(regex, input_value, group_name) {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Compile a regex pattern, returning a planning error on syntax issues
/// (matches the Java UDF's `IllegalArgumentException` for `PatternSyntaxException`).
pub(crate) fn compile_pattern(pat: &str) -> Result<Regex> {
    Regex::new(pat).map_err(|e| {
        DataFusionError::Plan(format!(
            "Error in 'rex' command: encountered an error while compiling the regex '{pat}': {e}"
        ))
    })
}

/// Look up a capture group in the first match of `regex` against `input`.
/// `group` is treated as a 1-based numeric index if it parses as a positive
/// integer; otherwise as a named group. Returns `None` if no match, the group
/// doesn't exist, or the group didn't participate in the match.
pub(crate) fn extract_group<'a>(regex: &Regex, input: &'a str, group: &str) -> Option<&'a str> {
    let caps = regex.captures(input)?;
    if let Ok(idx) = group.parse::<usize>() {
        // Java's `Matcher.group(int)` is 1-based; idx 0 is the full match.
        // We accept idx >= 1 to match Java's `if (groupIndex > 0)` guard.
        if idx >= 1 {
            return caps.get(idx).map(|m| m.as_str());
        }
        return None;
    }
    caps.name(group).map(|m| m.as_str())
}

/// If `cv` is a Utf8/LargeUtf8/Utf8View string scalar, return the String.
/// Returns `None` for NULL scalars, non-scalar values, or non-string types.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_named_group_first_match() {
        let r = compile_pattern("(?<host>[^@]+)@(?<domain>.+)").unwrap();
        assert_eq!(extract_group(&r, "user@example.com", "host"), Some("user"));
        assert_eq!(extract_group(&r, "user@example.com", "domain"), Some("example.com"));
    }

    #[test]
    fn extract_returns_none_when_pattern_does_not_match() {
        let r = compile_pattern("(?<g>\\d+)").unwrap();
        assert_eq!(extract_group(&r, "no digits here", "g"), None);
    }

    #[test]
    fn extract_returns_none_for_unknown_group_name() {
        let r = compile_pattern("(?<g>\\w+)").unwrap();
        assert_eq!(extract_group(&r, "hello", "missing"), None);
    }

    #[test]
    fn extract_supports_numeric_group_index() {
        // 1-based indexing — group 0 is the whole match (Java doesn't expose
        // it via this UDF; idx >= 1 only).
        let r = compile_pattern("(\\w+) (\\w+)").unwrap();
        assert_eq!(extract_group(&r, "hello world", "1"), Some("hello"));
        assert_eq!(extract_group(&r, "hello world", "2"), Some("world"));
        assert_eq!(extract_group(&r, "hello world", "3"), None);
    }

    #[test]
    fn extract_takes_first_match_only() {
        // `extract_group` uses `captures(...)` which returns the first match.
        // Subsequent matches are the job of `rex_extract_multi`.
        let r = compile_pattern("(?<n>\\d+)").unwrap();
        assert_eq!(extract_group(&r, "12 then 34 then 56", "n"), Some("12"));
    }

    #[test]
    fn compile_pattern_returns_plan_error_on_invalid_syntax() {
        let err = compile_pattern("(unclosed").unwrap_err();
        assert!(err.to_string().contains("Error in 'rex' command"));
    }
}
