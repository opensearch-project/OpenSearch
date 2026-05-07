/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared helpers for the PPL `json_*` UDFs.
//!
//! Three concerns live here so the per-function modules stay thin:
//! 1. **PPL-path → JSONPath** conversion (`convert_ppl_path`) — mirrors the
//!    SQL-plugin's `JsonUtils.convertToJsonPath`: `a{i}.b{}` ⇒ `$.a[i].b[*]`.
//! 2. **Parsing** (`parse`) — `serde_json::from_str` with malformed-to-`None`.
//! 3. **Arity guards** — `plan_err!` wrappers at the top of `invoke_with_args`.
//!
//! Kept deliberately small: only helpers that at least two UDFs use land here.

// Consumers (`json_valid`, `json_keys`, `json_extract`, mutation UDFs) land in
// follow-up commits on the same PR; silence dead-code warnings for the parser
// commit so `cargo check` stays clean.
#![allow(dead_code)]

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use serde_json::Value;

/// Convert a PPL-style path (`a.b{0}.c{}`) to a JSONPath expression
/// (`$.a.b[0].c[*]`). Empty input → `"$"` (document root), matching the
/// legacy contract. Returns a planning error for an unmatched `{`.
pub(crate) fn convert_ppl_path(input: &str) -> Result<String> {
    if input.is_empty() {
        return Ok("$".into());
    }
    let mut out = String::with_capacity(input.len() + 2);
    out.push_str("$.");
    let mut rest = input;
    while !rest.is_empty() {
        match rest.as_bytes()[0] {
            b'{' => {
                let end = rest.find('}').ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Unmatched '{{' in JSON path: {input}"
                    ))
                })?;
                let idx = rest[1..end].trim();
                if idx.is_empty() {
                    out.push_str("[*]");
                } else {
                    out.push('[');
                    out.push_str(idx);
                    out.push(']');
                }
                rest = &rest[end + 1..];
            }
            b'.' => {
                out.push('.');
                rest = &rest[1..];
            }
            _ => {
                let cut = rest.find(['.', '{']).unwrap_or(rest.len());
                out.push_str(&rest[..cut]);
                rest = &rest[cut..];
            }
        }
    }
    Ok(out)
}

/// Parse a JSON string; returns `None` on malformed input. Matches the
/// "malformed → NULL" convention across all json_* UDFs (see
/// `json_udf_legacy_semantics.md`).
pub(crate) fn parse(s: &str) -> Option<Value> {
    serde_json::from_str(s).ok()
}

/// Standard arity guard.
pub(crate) fn check_arity(udf: &str, observed: usize, expected: usize) -> Result<()> {
    (observed == expected)
        .then_some(())
        .ok_or_else(|| plan_err_msg(format!("{udf} expects {expected} arguments, got {observed}")))
}

/// Inclusive-range arity guard for varargs UDFs.
pub(crate) fn check_arity_range(udf: &str, observed: usize, min: usize, max: usize) -> Result<()> {
    if (min..=max).contains(&observed) {
        Ok(())
    } else {
        plan_err!("{udf} expects between {min} and {max} arguments, got {observed}")
    }
}

fn plan_err_msg(msg: String) -> DataFusionError {
    DataFusionError::Plan(msg)
}

/// Downcast an `ArrayRef` to `StringArray`. `coerce_types` with `CoerceMode::Utf8`
/// canonicalizes every string input to `Utf8` before this point, so a failure
/// indicates a planner bug rather than bad user input.
pub(crate) fn as_utf8_array(arr: &ArrayRef) -> Result<&StringArray> {
    arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Internal(format!("expected Utf8, got {:?}", arr.data_type()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ppl_path_mirrors_legacy_convert_to_jsonpath() {
        for (input, want) in [
            ("", "$"),
            ("a", "$.a"),
            ("a.b", "$.a.b"),
            ("a{0}", "$.a[0]"),
            ("a{}", "$.a[*]"),
            ("a{0}.b{}.c", "$.a[0].b[*].c"),
            ("a{  2  }", "$.a[2]"),
        ] {
            assert_eq!(convert_ppl_path(input).unwrap(), want, "input={input}");
        }
        assert!(convert_ppl_path("a{0").unwrap_err().to_string().contains("Unmatched"));
    }

    #[test]
    fn parse_handles_malformed_and_valid() {
        assert!(parse("{not json").is_none());
        assert!(parse("[1,2,3]").is_some());
    }

    #[test]
    fn arity_guards() {
        assert!(check_arity("f", 1, 1).is_ok());
        assert!(check_arity("f", 2, 1).is_err());
        assert!(check_arity_range("f", 3, 2, 4).is_ok());
        assert!(check_arity_range("f", 1, 2, 4).is_err());
    }
}
