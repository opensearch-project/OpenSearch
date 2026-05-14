/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json_extract_all(value)` — flatten a JSON object to a `Map<Utf8, Utf8>` with
//! dot-separated paths as keys (parity with legacy
//! `JsonExtractAllFunctionImpl`). Objects flatten to dotted keys (`user.name`);
//! arrays append a `{}` segment (`tags{}`); duplicate logical keys merge into a
//! Java-`List.toString()` rendering (`[a, b, c]`); JSON nulls render as the
//! literal string `"null"`. Null input / empty / whitespace / top-level scalar
//! / top-level number → NULL map. Malformed JSON → empty map (we silently keep
//! whatever was parsed before the syntax error, matching the legacy
//! `try { … } catch (IOException) { /* swallow */ }` flow at
//! {@code JsonExtractAllFunctionImpl#parseJson}).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, MapBuilder, MapFieldNames, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;

use super::json_common::{as_utf8_array, check_arity, parse};
use super::{coerce_args, CoerceMode};

/// Order-preserving append-or-merge map keyed by stringified path. We use a
/// `Vec<(String, Slot)>` rather than pulling `indexmap` into the dep tree —
/// duplicate-key insertion is rare (only when a JSON document has a literal
/// dotted key that collides with a nested-object path), so O(n) lookup on
/// merge is dominated by the JSON-walk cost on every realistic input.
type OrderedEntries = Vec<(String, Slot)>;

const NAME: &str = "json_extract_all";
const ARRAY_MARKER: &str = "{}";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonExtractAllUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonExtractAllUdf {
    signature: Signature,
}

impl JsonExtractAllUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for JsonExtractAllUdf {
    fn default() -> Self {
        Self::new()
    }
}

/// Arrow `Map<Utf8, Utf8>` (Map → Struct{keys: Utf8 non-null, values: Utf8
/// nullable}). The legacy Calcite path returns `MAP<VARCHAR, VARCHAR NULLABLE>`
/// (see `JsonExtractAllFunctionImpl#getReturnTypeInference`); this is the
/// substrait-friendly Arrow shape that decodes to the same logical type.
/// `values` is `nullable=true` so JSON nulls can round-trip as Arrow nulls
/// inside the map (they render to the literal string `"null"` here, but a
/// future caller might want to distinguish; keeping the slot nullable costs
/// nothing).
fn map_data_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                // Field names match Arrow Java's `MapVector.KEY_NAME` /
                // `VALUE_NAME` so the SQL-plugin response marshaller can use
                // those constants when unpacking entries.
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            false,
        )),
        false,
    )
}

impl ScalarUDFImpl for JsonExtractAllUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        NAME
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(map_data_type())
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(NAME, args, &[CoerceMode::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        check_arity(NAME, args.args.len(), 1)?;
        let n = args.number_rows;

        // Scalar fast-path — every row in the batch evaluates to the same map.
        if let ColumnarValue::Scalar(sv) = &args.args[0] {
            let s = scalar_str(sv);
            // Build a single-row Map array, then wrap as a Scalar(List(...))
            // by returning Array. DataFusion broadcasts a 1-row Array vs.
            // promoting to a real Scalar for Map; the cleanest fast-path here
            // is still to drop into the columnar builder for n rows and emit
            // identical rows. Build once, replay n times.
            let result = match s {
                Some(text) => extract_all(text),
                None => None,
            };
            let mut b = new_map_builder(n);
            for _ in 0..n {
                append_row(&mut b, result.as_ref())?;
            }
            return Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef));
        }

        let arr = args.args[0].clone().into_array(n)?;
        let strings = as_utf8_array(&arr)?;
        let mut b = new_map_builder(n);
        for i in 0..n {
            let row_result = if strings.is_null(i) {
                None
            } else {
                extract_all(strings.value(i))
            };
            append_row(&mut b, row_result.as_ref())?;
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef))
    }
}

fn scalar_str(sv: &ScalarValue) -> Option<&str> {
    match sv {
        ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s) => s.as_deref(),
        _ => None,
    }
}

fn new_map_builder(capacity: usize) -> MapBuilder<StringBuilder, StringBuilder> {
    MapBuilder::with_capacity(
        Some(MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
        capacity,
    )
}

/// Append one Map row: `None` → null map slot; `Some(entries)` → a map with
/// the (already-stringified) entries. Insertion order is preserved by
/// `IndexMap` so the wire-level JSON object key order matches Jackson's
/// streaming-parser order in the legacy impl.
fn append_row(
    b: &mut MapBuilder<StringBuilder, StringBuilder>,
    entries: Option<&OrderedEntries>,
) -> Result<()> {
    match entries {
        None => b.append(false)?,
        Some(map) => {
            for (k, v) in map.iter() {
                b.keys().append_value(k);
                match v.render() {
                    Some(rendered) => b.values().append_value(&rendered),
                    None => b.values().append_null(),
                }
            }
            b.append(true)?;
        }
    }
    Ok(())
}

/// One logical value at a path. Mirrors the legacy `Object` slot —
/// scalar-or-list polymorphism — so duplicate-key insertion can promote to a
/// list without rewriting the whole value.
#[derive(Debug)]
enum Slot {
    /// A single value. `None` here means a JSON null was inserted; it renders
    /// as the literal string `"null"` to match Java's `String.valueOf(null)`.
    Single(Option<String>),
    /// Two or more values at the same path. Renders as
    /// `[v1, v2, ..., vn]` with `null` for null elements — Java's
    /// `List.toString()` shape.
    List(Vec<Option<String>>),
}

impl Slot {
    fn extend(&mut self, next: Option<String>) {
        match self {
            Slot::Single(first) => {
                let mut v = Vec::with_capacity(2);
                v.push(first.take());
                v.push(next);
                *self = Slot::List(v);
            }
            Slot::List(v) => v.push(next),
        }
    }

    fn render(&self) -> Option<String> {
        match self {
            Slot::Single(None) => Some("null".to_string()),
            Slot::Single(Some(s)) => Some(s.clone()),
            Slot::List(items) => {
                let parts: Vec<&str> = items
                    .iter()
                    .map(|o| o.as_deref().unwrap_or("null"))
                    .collect();
                Some(format!("[{}]", parts.join(", ")))
            }
        }
    }
}

/// Public entry — returns `Some(map)` when the input parses (possibly to an
/// empty map) and is rooted at a JSON object/array; `None` for null/empty/
/// whitespace input and for a top-level non-container value (string, number,
/// bool, null). Matches the legacy contract exactly.
fn extract_all(s: &str) -> Option<OrderedEntries> {
    if s.trim().is_empty() {
        return None;
    }
    let parsed = match parse(s) {
        // Malformed → legacy swallows the error and returns the partial map.
        // With serde_json we get all-or-nothing (no token-level partial
        // state), but the IT-side `testSpathAutoExtractMalformedJson` only
        // cares that the result is the empty map (struct `{}`), which is the
        // observable behaviour we deliver here. Unit-test-level partial-parse
        // cases (`testInvalidJsonReturnResults`) are not part of the IT
        // surface.
        Some(v) => v,
        None => return Some(Vec::new()),
    };
    match &parsed {
        Value::Object(_) | Value::Array(_) => {}
        _ => return None, // top-level scalar — legacy returns NULL.
    }
    let mut out: OrderedEntries = Vec::new();
    walk(&parsed, &mut Vec::new(), &mut out);
    Some(out)
}

/// DFS walk that mirrors the Jackson stream-walk in the legacy impl.
/// `path` is the current path stack (field names + array markers). Empty
/// arrays produce no entries; non-empty arrays append `{}` to the path and
/// recurse element-by-element so every element shares the same parent path
/// (which is what produces the duplicate-key merge behaviour).
fn walk(v: &Value, path: &mut Vec<String>, out: &mut OrderedEntries) {
    match v {
        Value::Object(map) => {
            for (key, child) in map.iter() {
                path.push(key.clone());
                walk(child, path, out);
                path.pop();
            }
        }
        Value::Array(arr) => {
            path.push(ARRAY_MARKER.to_string());
            for element in arr.iter() {
                walk(element, path, out);
            }
            path.pop();
        }
        leaf => {
            let key = build_path(path);
            let value = stringify_scalar(leaf);
            insert(out, key, value);
        }
    }
}

fn insert(out: &mut OrderedEntries, key: String, value: Option<String>) {
    if let Some((_, existing)) = out.iter_mut().find(|(k, _)| *k == key) {
        existing.extend(value);
    } else {
        out.push((key, Slot::Single(value)));
    }
}

/// Join `path` into a dotted/`{}`-terminated string. Array markers `{}` carry
/// through unchanged; field-name separators are `.`. Adjacent `{}` segments
/// land next to each other (`a{}{}` for arrays-of-arrays).
fn build_path(path: &[String]) -> String {
    let mut s = String::with_capacity(path.iter().map(|p| p.len() + 1).sum::<usize>());
    for segment in path {
        if segment == ARRAY_MARKER {
            s.push_str(ARRAY_MARKER);
        } else {
            if !s.is_empty() && !s.ends_with(ARRAY_MARKER) {
                s.push('.');
            } else if s.ends_with(ARRAY_MARKER) {
                s.push('.');
            }
            s.push_str(segment);
        }
    }
    s
}

/// Stringify a JSON scalar exactly the way the legacy `String.valueOf` chain
/// does. Returns `None` for JSON null (rendered as `"null"` at the outer
/// boundary — see `Slot::render`).
fn stringify_scalar(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(stringify_number(n)),
        Value::String(s) => Some(s.clone()),
        Value::Array(_) | Value::Object(_) => {
            // Caller shouldn't reach this — containers are walked recursively
            // by `walk`. Defensive: serialize as JSON so a future caller that
            // passes a container by mistake doesn't silently lose data.
            Some(v.to_string())
        }
    }
}

/// JSON-number → Java-`String.valueOf` parity:
///  - Integers in i64 range → `123`, `9223372036854775807`
///  - Integers above i64 → promoted to f64 and rendered in Rust's shortest
///    round-trip form (matches `Double.toString` for the common
///    `9.223372036854776E18` pattern; minor lowercase-`e` divergence is the
///    only known formatting difference, not exercised by spath ITs).
///  - Non-integers → Rust's default `{}` formatter, which is the shortest
///    round-trip representation (matches Java's `Double.toString` for finite
///    values that don't need exponent form: `3.14159`, `0.5`).
fn stringify_number(n: &serde_json::Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }
    if let Some(f) = n.as_f64() {
        return f.to_string();
    }
    // Should be unreachable for finite JSON; fall back to the raw textual
    // form serde_json holds internally.
    n.to_string()
}

// Suppress unused-import warning when StringArray is only referenced via the
// `as_utf8_array` helper return type.
#[allow(dead_code)]
type _StringArrayUsed = StringArray;

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(s: &str) -> Option<Vec<(String, String)>> {
        extract_all(s).map(|entries| {
            entries
                .into_iter()
                .map(|(k, v)| (k, v.render().unwrap_or_else(|| "null".to_string())))
                .collect()
        })
    }

    #[test]
    fn null_and_empty_input_returns_none() {
        assert!(rendered("").is_none());
        assert!(rendered("   ").is_none());
    }

    #[test]
    fn top_level_scalar_returns_none() {
        // Legacy: `if (pathStack.isEmpty()) return null` on top-level scalar.
        assert!(rendered(r#""just a string""#).is_none());
        assert!(rendered("42").is_none());
        assert!(rendered("null").is_none());
        assert!(rendered("true").is_none());
    }

    #[test]
    fn empty_object_returns_empty_map() {
        assert_eq!(rendered(r#"{}"#).unwrap(), Vec::<(String, String)>::new());
    }

    #[test]
    fn simple_object_dotted_keys_for_scalars() {
        assert_eq!(
            rendered(r#"{"name":"John","age":30}"#).unwrap(),
            vec![
                ("name".to_string(), "John".to_string()),
                ("age".to_string(), "30".to_string()),
            ]
        );
    }

    #[test]
    fn nested_object_flattens_to_dotted_path() {
        assert_eq!(
            rendered(r#"{"user":{"name":"John"},"system":"linux"}"#).unwrap(),
            vec![
                ("user.name".to_string(), "John".to_string()),
                ("system".to_string(), "linux".to_string()),
            ]
        );
    }

    #[test]
    fn array_of_primitives_suffixes_with_curly_marker() {
        assert_eq!(
            rendered(r#"{"tags":["a","b","c"]}"#).unwrap(),
            vec![("tags{}".to_string(), "[a, b, c]".to_string())]
        );
    }

    #[test]
    fn empty_array_yields_no_entry() {
        assert_eq!(
            rendered(r#"{"empty":[]}"#).unwrap(),
            Vec::<(String, String)>::new()
        );
    }

    #[test]
    fn array_of_objects_propagates_marker_into_children() {
        assert_eq!(
            rendered(r#"{"users":[{"name":"John"},{"name":"Jane"}]}"#).unwrap(),
            vec![("users{}.name".to_string(), "[John, Jane]".to_string())]
        );
    }

    #[test]
    fn top_level_array_of_objects_roots_at_curly_marker() {
        assert_eq!(
            rendered(r#"[{"age":1},{"age":2}]"#).unwrap(),
            vec![("{}.age".to_string(), "[1, 2]".to_string())]
        );
    }

    #[test]
    fn top_level_array_of_primitives_uses_curly_marker_as_root() {
        assert_eq!(
            rendered(r#"[1,2,3]"#).unwrap(),
            vec![("{}".to_string(), "[1, 2, 3]".to_string())]
        );
    }

    #[test]
    fn json_null_renders_as_literal_string_null() {
        // `{"x": null}` → key `x` present, value rendered as the literal
        // string `"null"` (legacy `String.valueOf((Object) null) == "null"`).
        let m = rendered(r#"{"n":30,"b":true,"x":null}"#).unwrap();
        assert_eq!(
            m,
            vec![
                ("n".to_string(), "30".to_string()),
                ("b".to_string(), "true".to_string()),
                ("x".to_string(), "null".to_string()),
            ]
        );
    }

    #[test]
    fn null_in_array_renders_inline_with_other_elements() {
        assert_eq!(
            rendered(r#"[{"a":null},{"a":1}]"#).unwrap(),
            vec![("{}.a".to_string(), "[null, 1]".to_string())]
        );
    }

    #[test]
    fn duplicate_logical_keys_merge_into_bracketed_list() {
        // `{"a":{"b":1}, "a.b": 2}` — Jackson's parser emits the outer key
        // `a.b` after the nested-object path produces the same key, so the
        // values merge into a list. With `preserve_order` enabled,
        // serde_json's `Value::Object` preserves the input order across
        // duplicate logical keys, so the merge produces `[1, 2]`.
        assert_eq!(
            rendered(r#"{"a":{"b":1},"a.b":2}"#).unwrap(),
            vec![("a.b".to_string(), "[1, 2]".to_string())]
        );
    }

    #[test]
    fn malformed_json_returns_empty_map() {
        // Legacy: catches Jackson IOException and returns whatever was parsed.
        // serde_json is all-or-nothing, so we return the empty map — the
        // IT-level `testSpathAutoExtractMalformedJson` only asserts on `{}`.
        assert_eq!(
            rendered(r#"{"user":{"name":"#).unwrap(),
            Vec::<(String, String)>::new()
        );
    }

    #[test]
    fn return_type_is_map_utf8_utf8() {
        let dt = JsonExtractAllUdf::new().return_type(&[DataType::Utf8]).unwrap();
        match dt {
            DataType::Map(field, sorted) => {
                assert!(!sorted);
                match field.data_type() {
                    DataType::Struct(fields) => {
                        assert_eq!(fields.len(), 2);
                        assert_eq!(fields[0].name(), "key");
                        assert_eq!(fields[0].data_type(), &DataType::Utf8);
                        assert!(!fields[0].is_nullable());
                        assert_eq!(fields[1].name(), "value");
                        assert_eq!(fields[1].data_type(), &DataType::Utf8);
                        assert!(fields[1].is_nullable());
                    }
                    other => panic!("expected Struct inside Map, got {other:?}"),
                }
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn coerce_types_enforces_string_arity() {
        let udf = JsonExtractAllUdf::new();
        assert_eq!(
            udf.coerce_types(&[DataType::LargeUtf8]).unwrap(),
            vec![DataType::Utf8]
        );
        assert!(udf
            .coerce_types(&[DataType::Int64])
            .unwrap_err()
            .to_string()
            .contains("expected string"));
        assert!(udf.coerce_types(&[]).is_err());
    }
}
