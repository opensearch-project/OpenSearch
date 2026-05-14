/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `rex_offset(input, pattern)` — compute start-end offsets of every named
//! capture group from the first regex match, formatted as
//! `"name1=start1-end1&name2=start2-end2"` with group names sorted
//! alphabetically.
//!
//! Lowering target for PPL's `rex field=f "(?<g>...)" offset_field=name`.
//! Mirrors the SQL plugin's `RexOffsetFunction.calculateOffsets`.
//!
//! End position is **inclusive** (matches the Java implementation:
//! `start + "-" + (end - 1)`).
//!
//! Returns NULL when:
//!   * `input` or `pattern` is NULL
//!   * the pattern doesn't match `input`
//!   * the pattern matches but has zero named groups (the Java code guards
//!     `offsetPairs.isEmpty() ? null : ...`)
//!
//! The Java implementation parses named-group names from the pattern itself
//! (regex `\(\?<([^>]+)>`) and walks them in declaration order, mapping each
//! to capture-group index 1, 2, 3, …. This UDF mirrors that walk: we use
//! Rust's `regex::Regex::capture_names` which returns names in the same
//! declaration order. After computing positions, we sort the result
//! alphabetically (matching `Collections.sort(offsetPairs)` in the Java code).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
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
    ctx.register_udf(ScalarUDF::from(RexOffsetUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RexOffsetUdf {
    signature: Signature,
}

impl RexOffsetUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for RexOffsetUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for RexOffsetUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "rex_offset"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("rex_offset expects 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "rex_offset",
            arg_types,
            &[CoerceMode::Utf8, CoerceMode::Utf8],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("rex_offset expects 2 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        let pattern_scalar = utf8_scalar(&args.args[1]);
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
                    "rex_offset: expected Utf8 input, got {:?}",
                    input.data_type()
                ))
            })?;

        let pattern_arr_ref: Option<ArrayRef> = if pattern_scalar.is_none() && matches!(&args.args[1], ColumnarValue::Array(_)) {
            Some(args.args[1].clone().into_array(n)?)
        } else {
            None
        };
        let pattern_array: Option<&StringArray> = pattern_arr_ref.as_ref().and_then(|a| a.as_any().downcast_ref::<StringArray>());

        let mut builder = StringBuilder::with_capacity(n, n * 32);
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

            match calculate_offsets(regex, input.value(i)) {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Build the `"name1=s1-e1&name2=s2-e2"` offset string from the first regex
/// match. Names are sorted alphabetically in the output. Returns None if no
/// match or no named groups participated.
///
/// The end position is the index of the last matched character (inclusive),
/// matching `RexOffsetFunction.calculateOffsets` in the SQL plugin:
/// `groupName + "=" + start + "-" + (end - 1)`.
///
/// Note: positions are byte offsets (Rust `regex` returns byte indices), which
/// matches Java's UTF-16 code-unit indices for ASCII-only inputs. For
/// non-ASCII input this may diverge from the Java behavior — kept consistent
/// with the rest of the analytics-engine route, which already uses byte
/// offsets throughout the regex stack.
pub(crate) fn calculate_offsets(regex: &Regex, input: &str) -> Option<String> {
    let caps = regex.captures(input)?;
    let mut pairs: Vec<String> = Vec::new();
    for (idx, name_opt) in regex.capture_names().enumerate() {
        if let Some(name) = name_opt {
            // Index 0 is the whole match; named groups start at 1. The
            // `capture_names` iterator yields a `None` at index 0, so the
            // `name_opt.is_some()` branch only fires for actual capture
            // groups — the `idx` here aligns with `caps.get(idx)`.
            if let Some(m) = caps.get(idx) {
                let start = m.start();
                let end = m.end();
                if end >= 1 {
                    pairs.push(format!("{name}={start}-{}", end - 1));
                }
            }
        }
    }
    if pairs.is_empty() {
        None
    } else {
        pairs.sort();
        Some(pairs.join("&"))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udf::rex_extract::compile_pattern;

    #[test]
    fn offsets_returns_alphabetically_sorted_pairs() {
        let r = compile_pattern("(?<host>[^@]+)@(?<domain>.+)").unwrap();
        let result = calculate_offsets(&r, "user@example.com").unwrap();
        // Named groups are 'host' (start=0,end=4 → 0-3) and 'domain' (start=5,end=16 → 5-15).
        // Java sorts alphabetically: domain first, then host.
        assert_eq!(result, "domain=5-15&host=0-3");
    }

    #[test]
    fn offsets_returns_none_when_pattern_does_not_match() {
        let r = compile_pattern("(?<g>\\d+)").unwrap();
        assert_eq!(calculate_offsets(&r, "no digits"), None);
    }

    #[test]
    fn offsets_returns_none_when_pattern_has_no_named_groups() {
        // Pattern matches but no `(?<name>...)` groups — Java returns null for
        // empty offsetPairs; we mirror that.
        let r = compile_pattern("\\w+").unwrap();
        assert_eq!(calculate_offsets(&r, "hello"), None);
    }

    #[test]
    fn offsets_uses_first_match_only() {
        // Java calls `matcher.find()` once, then walks named groups within
        // that single match. Subsequent matches don't contribute.
        let r = compile_pattern("(?<n>\\d+)").unwrap();
        assert_eq!(calculate_offsets(&r, "12 then 34"), Some("n=0-1".to_string()));
    }

    #[test]
    fn offsets_handles_optional_groups_that_did_not_participate() {
        // (?<a>x)?(?<b>y) on input "y": group 'a' didn't match (None), group
        // 'b' matched at 0-0. Java's `matcher.start(idx)` returns -1 for
        // non-participating groups; the `if (start >= 0 && end >= 0)` guard
        // skips them. Rust's `caps.get(idx)` returns None — same skip.
        let r = compile_pattern("(?<a>x)?(?<b>y)").unwrap();
        assert_eq!(calculate_offsets(&r, "y"), Some("b=0-0".to_string()));
    }
}
