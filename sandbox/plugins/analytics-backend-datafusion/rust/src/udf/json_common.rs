/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared helpers for the PPL `json_*` UDFs: PPL-path parsing (to both JSONPath
//! strings and typed segment vectors), a segment-based mutation walker used by
//! the write UDFs, and a malformed-to-`None` JSON parser.

use datafusion::arrow::array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
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

/// One tokenised step of a PPL path. Mirrors the three cases `convert_ppl_path`
/// handles: bare identifier (field), `{n}` (array index), `{}` (array wildcard).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Segment<'a> {
    Field(&'a str),
    Index(usize),
    Wildcard,
}

/// Tokenise a PPL path into `Segment`s without allocating for field names.
/// Returns a planning error for unmatched `{` or a non-numeric index — the
/// same inputs `convert_ppl_path` rejects.
pub(crate) fn parse_ppl_segments(input: &str) -> Result<Vec<Segment<'_>>> {
    let mut out = Vec::new();
    let mut rest = input;
    while !rest.is_empty() {
        match rest.as_bytes()[0] {
            b'{' => {
                let end = rest.find('}').ok_or_else(|| {
                    DataFusionError::Plan(format!("Unmatched '{{' in JSON path: {input}"))
                })?;
                let idx = rest[1..end].trim();
                if idx.is_empty() {
                    out.push(Segment::Wildcard);
                } else {
                    let parsed = idx.parse::<usize>().map_err(|_| {
                        DataFusionError::Plan(format!(
                            "Non-numeric array index '{idx}' in JSON path: {input}"
                        ))
                    })?;
                    out.push(Segment::Index(parsed));
                }
                rest = &rest[end + 1..];
            }
            b'.' => rest = &rest[1..],
            _ => {
                let cut = rest.find(['.', '{']).unwrap_or(rest.len());
                if cut > 0 {
                    out.push(Segment::Field(&rest[..cut]));
                }
                rest = &rest[cut..];
            }
        }
    }
    Ok(out)
}

/// Drive `apply` against every terminal `(parent, final_segment)` reached by
/// `segments` inside `root`. Missing intermediate keys / out-of-range indices
/// are silently skipped (matching Jayway's `SUPPRESS_EXCEPTIONS` behaviour
/// that legacy mutation UDFs rely on). Wildcard segments fan out across every
/// element of the current array; descending through a non-container
/// short-circuits that branch.
///
/// Empty `segments` is a no-op: PPL mutation UDFs reject a root-only path at
/// the call site before reaching the walker.
pub(crate) fn walk_mut<F>(root: &mut Value, segments: &[Segment<'_>], mut apply: F)
where
    F: FnMut(&mut Value, &Segment<'_>),
{
    if segments.is_empty() {
        return;
    }
    walk_mut_inner(root, segments, &mut apply);
}

fn walk_mut_inner<F>(node: &mut Value, segments: &[Segment<'_>], apply: &mut F)
where
    F: FnMut(&mut Value, &Segment<'_>),
{
    let (head, tail) = segments.split_first().expect("non-empty checked by caller");
    if tail.is_empty() {
        // Parent is `node`; the final segment names the slot to mutate.
        apply(node, head);
        return;
    }
    match head {
        Segment::Field(name) => {
            if let Value::Object(map) = node {
                if let Some(child) = map.get_mut(*name) {
                    walk_mut_inner(child, tail, apply);
                }
            }
        }
        Segment::Index(i) => {
            if let Value::Array(arr) = node {
                if let Some(child) = arr.get_mut(*i) {
                    walk_mut_inner(child, tail, apply);
                }
            }
        }
        Segment::Wildcard => {
            if let Value::Array(arr) = node {
                for child in arr.iter_mut() {
                    walk_mut_inner(child, tail, apply);
                }
            }
        }
    }
}

/// Standard arity guard.
pub(crate) fn check_arity(udf: &str, observed: usize, expected: usize) -> Result<()> {
    (observed == expected).then_some(()).ok_or_else(|| {
        DataFusionError::Plan(format!("{udf} expects {expected} arguments, got {observed}"))
    })
}

/// View over an Arrow string array of any logical width (`Utf8`, `LargeUtf8`,
/// `Utf8View`). Dispatch happens once in [`Self::from_array`]; per-row access
/// via [`Self::cell`] is a small enum match.
pub(crate) enum StringArrayView<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> StringArrayView<'a> {
    pub(crate) fn from_array(arr: &'a ArrayRef) -> Result<Self> {
        match arr.data_type() {
            DataType::Utf8 => Ok(Self::Utf8(
                arr.as_any().downcast_ref::<StringArray>().expect("Utf8 downcast"),
            )),
            DataType::LargeUtf8 => Ok(Self::LargeUtf8(
                arr.as_any().downcast_ref::<LargeStringArray>().expect("LargeUtf8 downcast"),
            )),
            DataType::Utf8View => Ok(Self::Utf8View(
                arr.as_any().downcast_ref::<StringViewArray>().expect("Utf8View downcast"),
            )),
            other => Err(DataFusionError::Internal(format!(
                "expected string array (Utf8/LargeUtf8/Utf8View), got {other:?}"
            ))),
        }
    }

    /// Returns `Some(value)` for non-null rows, `None` for nulls.
    #[inline]
    pub(crate) fn cell(&self, i: usize) -> Option<&str> {
        match self {
            Self::Utf8(a) => (!a.is_null(i)).then(|| a.value(i)),
            Self::LargeUtf8(a) => (!a.is_null(i)).then(|| a.value(i)),
            Self::Utf8View(a) => (!a.is_null(i)).then(|| a.value(i)),
        }
    }
}

/// Extract `&str` from a string-typed `ColumnarValue::Scalar`. Returns `None`
/// for non-scalars, non-string scalars, and `NULL` scalars. Shared by every
/// json-family UDF's all-scalar fast path.
pub(crate) fn scalar_utf8(v: &ColumnarValue) -> Option<&str> {
    match v {
        ColumnarValue::Scalar(
            ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s),
        ) => s.as_deref(),
        _ => None,
    }
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
    }

    #[test]
    fn parse_ppl_segments_tokenises_field_index_and_wildcard() {
        assert_eq!(parse_ppl_segments("").unwrap(), Vec::<Segment>::new());
        assert_eq!(parse_ppl_segments("a").unwrap(), vec![Segment::Field("a")]);
        assert_eq!(
            parse_ppl_segments("a.b{0}.c{}").unwrap(),
            vec![
                Segment::Field("a"),
                Segment::Field("b"),
                Segment::Index(0),
                Segment::Field("c"),
                Segment::Wildcard,
            ]
        );
        assert!(parse_ppl_segments("a{0").is_err());
        assert!(parse_ppl_segments("a{x}").is_err());
    }

    fn v(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn walk_mut_deletes_flat_key() {
        let mut doc = v(r#"{"a":1,"b":2,"c":3}"#);
        let segs = parse_ppl_segments("b").unwrap();
        walk_mut(&mut doc, &segs, |parent, seg| {
            if let (Value::Object(map), Segment::Field(name)) = (parent, seg) {
                map.shift_remove(*name);
            }
        });
        assert_eq!(serde_json::to_string(&doc).unwrap(), r#"{"a":1,"c":3}"#);
    }

    #[test]
    fn walk_mut_handles_missing_path_as_noop() {
        let mut doc = v(r#"{"f1":"abc","f2":{"f3":"a"}}"#);
        let segs = parse_ppl_segments("f2.nope").unwrap();
        walk_mut(&mut doc, &segs, |parent, seg| {
            if let (Value::Object(map), Segment::Field(name)) = (parent, seg) {
                map.shift_remove(*name);
            }
        });
        assert_eq!(
            serde_json::to_string(&doc).unwrap(),
            r#"{"f1":"abc","f2":{"f3":"a"}}"#
        );
    }

    #[test]
    fn walk_mut_wildcard_fans_out_across_array() {
        let mut doc = v(r#"{"xs":[{"k":1,"v":10},{"k":2,"v":20}]}"#);
        let segs = parse_ppl_segments("xs{}.v").unwrap();
        walk_mut(&mut doc, &segs, |parent, seg| {
            if let (Value::Object(map), Segment::Field(name)) = (parent, seg) {
                map.shift_remove(*name);
            }
        });
        assert_eq!(
            serde_json::to_string(&doc).unwrap(),
            r#"{"xs":[{"k":1},{"k":2}]}"#
        );
    }

    #[test]
    fn walk_mut_index_out_of_range_is_noop() {
        let mut doc = v(r#"{"xs":[{"k":1}]}"#);
        let segs = parse_ppl_segments("xs{5}.k").unwrap();
        walk_mut(&mut doc, &segs, |parent, seg| {
            if let (Value::Object(map), Segment::Field(name)) = (parent, seg) {
                map.shift_remove(*name);
            }
        });
        assert_eq!(serde_json::to_string(&doc).unwrap(), r#"{"xs":[{"k":1}]}"#);
    }
}
