/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `grok(input, grok_pattern_lit, method_lit)` — extract named groups from a
//! grok pattern into a `map<varchar, varchar>`. Lowering target for PPL's
//! `grok <field> '<grok-pattern>'` command, the grok-syntax sibling of `parse`.
//!
//! # Why grok needs its own UDF rather than reusing `parse`
//!
//! `parse` takes a raw regex; grok takes grok syntax (`%{HOSTNAME:host}`). Two
//! consequences force a separate implementation:
//!
//!   1. **Dictionary resolution.** Grok references like `%{COMMONAPACHELOG}` must
//!      be recursively expanded against a pattern dictionary before matching. The
//!      SQL plugin's grok library (`org.opensearch.sql.common.grok`) is not on
//!      OpenSearch core's classpath, so the 108-line default dictionary
//!      (`grok_patterns.txt`, forked from logstash, same file the SQL plugin
//!      ships) and the resolution algorithm are ported here verbatim from
//!      `GrokCompiler.compile`.
//!   2. **Regex features.** The resolved patterns use lookbehind `(?<!…)`,
//!      lookahead `(?!…)` and atomic groups `(?>…)` (e.g. `BASE10NUM`, `IPV4`)
//!      that the RE2-based `regex` crate backing `parse` cannot compile. Matching
//!      uses `fancy-regex` instead, whose backtracking engine supports the full
//!      java.util.regex feature set grok relies on.
//!
//! # Division of labor with the Java adapter
//!
//! Plan-time work (Java `GrokAdapter`):
//!   * Verify `pattern` and `method` operands are non-null string literals.
//!   * Require `method == "grok"`.
//!
//! Runtime work (this file), mirroring the SQL plugin's grok semantics
//! (`GrokCompiler` + `Grok` + `Match` + `GrokExpression`):
//!   * Resolve the grok pattern to a `fancy-regex` once per call (literal
//!     pattern), recording the synthetic-group → field-name map.
//!   * Match **unanchored** — grok uses `Matcher.find()` (substring match), in
//!     contrast to `parse`'s anchored `^…$`.
//!   * For every input row, populate a `map<utf8, utf8>` with one entry per
//!     distinct user-facing field name (the `%{NAME:field}` subname; `UNWANTED`
//!     groups are dropped). A field whose group did not participate — and any
//!     row that does not match, or a SQL NULL input — yields `""`, mirroring
//!     `GrokExpression.parseValue` returning `ExprStringValue("")`. Captured
//!     values are quote-stripped via `clean_string`, matching `Match.cleanString`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use datafusion::arrow::array::{
    Array, ArrayRef, MapBuilder, MapFieldNames, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use fancy_regex::Regex as FancyRegex;
use regex::Regex;

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(GrokUdf::new()));
}

/// Default grok pattern dictionary — forked from logstash, identical to the
/// SQL plugin's `common/src/main/resources/patterns/patterns` (loaded there via
/// `GrokCompiler.registerDefaultPatterns`).
static GROK_DICTIONARY: &str = include_str!("grok_patterns.txt");

/// `%{NAME}`, `%{NAME:subname}`, `%{NAME:subname:type}`, and inline
/// `%{NAME=definition}` reference matcher — ported from
/// `GrokUtils.GROK_PATTERN`. No lookaround/atomic features, so the linear
/// `regex` crate is used (avoids backtracking on the meta-pattern).
static GROK_REFERENCE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"%\{(?<name>(?<pattern>[a-zA-Z0-9_]+)(?::(?<subname>[a-zA-Z0-9_:;,\-/\s.']+))?)(?:=(?<definition>(?:(?:[^{}]+|\.+)+)+))?\}",
    )
    .expect("GROK_REFERENCE is a valid regex")
});

/// Matches the synthetic `(?<nameN>` capture-group openers in a resolved grok
/// regex — ported from `GrokUtils.NAMED_REGEX`. Used to recover the group order
/// for output, mirroring `GrokUtils.getNameGroups`.
static NAMED_GROUP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\(\?<([a-zA-Z][a-zA-Z0-9]*)>").expect("NAMED_GROUP is a valid regex")
});

/// Parsed dictionary: pattern name → definition. Built once.
static DICTIONARY: LazyLock<HashMap<String, String>> = LazyLock::new(|| {
    let mut defs = HashMap::new();
    for line in GROK_DICTIONARY.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        // `NAME definition` — split on the first run of whitespace, exactly like
        // GrokCompiler.register's `^(\w+)\s+(.*)$` line parser.
        if let Some(idx) = trimmed.find(char::is_whitespace) {
            let name = trimmed[..idx].to_string();
            let definition = trimmed[idx..].trim_start().to_string();
            defs.insert(name, definition);
        }
    }
    defs
});

/// A grok pattern resolved to a concrete regex plus the synthetic-group → field
/// mapping needed to key the output map.
struct ResolvedGrok {
    regex: FancyRegex,
    /// `name0` → user field name (`host`, `UNWANTED`, or the pattern name when
    /// no subname was given), as produced by `GrokCompiler.compile`.
    collection: HashMap<String, String>,
    /// The named groups in the order they appear in the resolved regex, deduped,
    /// matching `GrokUtils.getNameGroups` (a `LinkedHashSet`).
    group_order: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GrokUdf {
    signature: Signature,
}

impl GrokUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for GrokUdf {
    fn default() -> Self {
        Self::new()
    }
}

/// Arrow `Map<Utf8, Utf8>` field shape returned by this UDF — keys non-null,
/// values nullable. Identical to `parse`'s output shape so `ITEM` binds the same.
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

/// Resolve a grok pattern to a `fancy-regex` plus its group→field mapping.
/// Ported from `GrokCompiler.compile(pattern, ...)`.
fn resolve_grok(pattern: &str) -> Result<ResolvedGrok> {
    if pattern.trim().is_empty() {
        return plan_err!("grok: pattern should not be empty or null");
    }

    let mut named_regex = pattern.to_string();
    // Local copy so inline `%{NAME=definition}` defs don't leak across calls.
    let mut pattern_defs = DICTIONARY.clone();
    let mut collection: HashMap<String, String> = HashMap::new();
    let mut index: usize = 0;
    let mut iterations_left = 1000;

    loop {
        if iterations_left <= 0 {
            return plan_err!("grok: deep recursion pattern compilation of [{pattern}]");
        }
        iterations_left -= 1;

        // GROK_REFERENCE never uses fancy features, so this find is infallible.
        let caps = match GROK_REFERENCE.captures(&named_regex) {
            Some(c) => c,
            None => break,
        };

        let mut name = caps.name("name").unwrap().as_str().to_string();
        let pattern_name = caps.name("pattern").unwrap().as_str().to_string();
        let subname = caps.name("subname").map(|m| m.as_str().to_string());
        let definition = caps.name("definition").map(|m| m.as_str().to_string());

        if let Some(def) = &definition {
            pattern_defs.insert(pattern_name.clone(), def.clone());
            name = format!("{name}={def}");
        }

        let token = format!("%{{{name}}}");
        let count = named_regex.matches(&token).count();
        for _ in 0..count {
            let definition_of_pattern = pattern_defs.get(&pattern_name).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "grok: no definition for key '{pattern_name}' found, aborting"
                ))
            })?;
            let replacement = format!("(?<name{index}>{definition_of_pattern})");
            collection.insert(
                format!("name{index}"),
                subname.clone().unwrap_or_else(|| pattern_name.clone()),
            );
            named_regex = replace_first(&named_regex, &token, &replacement);
            index += 1;
        }
    }

    if named_regex.is_empty() {
        return plan_err!("grok: pattern not found in [{pattern}]");
    }

    let regex = FancyRegex::new(&named_regex).map_err(|e| {
        DataFusionError::Plan(format!("grok: invalid resolved regex for [{pattern}]: {e}"))
    })?;

    // Recover the named-group order from the resolved regex (LinkedHashSet
    // semantics: insertion order, deduped) — mirrors GrokUtils.getNameGroups.
    let mut group_order: Vec<String> = Vec::new();
    for caps in NAMED_GROUP.captures_iter(&named_regex) {
        let g = caps.get(1).unwrap().as_str().to_string();
        if !group_order.contains(&g) {
            group_order.push(g);
        }
    }

    Ok(ResolvedGrok {
        regex,
        collection,
        group_order,
    })
}

/// Literal single-occurrence string replacement — equivalent to
/// `StringUtils.replace(s, token, replacement, 1)`. The replacement is spliced
/// in literally (no regex/`$` interpretation), matching the Java behaviour.
fn replace_first(s: &str, token: &str, replacement: &str) -> String {
    match s.find(token) {
        Some(pos) => {
            let mut out = String::with_capacity(s.len() - token.len() + replacement.len());
            out.push_str(&s[..pos]);
            out.push_str(replacement);
            out.push_str(&s[pos + token.len()..]);
            out
        }
        None => s.to_string(),
    }
}

/// Strip a matching pair of surrounding single/double quotes from a captured
/// value, but only when the quote char does not also appear in the interior —
/// ported from `Match.cleanString`.
fn clean_string(value: &str) -> String {
    if value.is_empty() {
        return value.to_string();
    }
    let chars: Vec<char> = value.chars().collect();
    let first = chars[0];
    let last = chars[chars.len() - 1];
    if first == last && (first == '"' || first == '\'') {
        if chars.len() <= 2 {
            return String::new();
        }
        let interior = &chars[1..chars.len() - 1];
        if !interior.contains(&first) {
            return interior.iter().collect();
        }
    }
    value.to_string()
}

/// Ordered, deduped list of user-facing output field names for a resolved grok
/// pattern (drops `UNWANTED`) — mirrors `GrokExpression.getNamedGroupCandidates`
/// composed with `Match.capture`'s first-wins-per-key map.
fn output_fields(resolved: &ResolvedGrok) -> Vec<(String, String)> {
    let mut fields: Vec<(String, String)> = Vec::new();
    for group in &resolved.group_order {
        let field = match resolved.collection.get(group) {
            Some(f) => f,
            None => continue,
        };
        if field == "UNWANTED" {
            continue;
        }
        if fields.iter().any(|(f, _)| f == field) {
            continue; // first synthetic group for a field wins, like Match.capture
        }
        fields.push((field.clone(), group.clone()));
    }
    fields
}

impl ScalarUDFImpl for GrokUdf {
    fn name(&self) -> &str {
        "grok"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("grok expects 3 arguments, got {}", arg_types.len());
        }
        Ok(map_return_type())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // Validate each slot is a string variant, then map down to Utf8:
        // invoke_with_args downcasts the input column to StringArray. Same
        // contract as parse's coerce_types.
        let validated = coerce_args(
            "grok",
            arg_types,
            &[CoerceMode::Utf8, CoerceMode::Utf8, CoerceMode::Utf8],
        )?;
        Ok(validated.into_iter().map(|_| DataType::Utf8).collect())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return plan_err!("grok expects 3 arguments, got {}", args.args.len());
        }

        // The Java adapter validates pattern + method are non-null string
        // literals and gates method == "grok" at plan time. Re-check defensively
        // so a misuse from an unknown caller produces an actionable plan error.
        let pattern = scalar_str(&args.args[1]).ok_or_else(|| {
            DataFusionError::Plan("grok: pattern must be a non-null string literal".into())
        })?;
        let method = scalar_str(&args.args[2]).ok_or_else(|| {
            DataFusionError::Plan("grok: method must be a non-null string literal".into())
        })?;
        if method != "grok" {
            return Err(DataFusionError::Plan(format!(
                "grok: method '{method}' is not supported by this UDF; expected 'grok'"
            )));
        }

        let resolved = resolve_grok(&pattern)?;
        // Output keys in pattern (group) order; deduped; UNWANTED dropped.
        let fields = output_fields(&resolved);

        let n = args.number_rows;
        let input_arr = args.args[0].clone().into_array(n)?;
        let input = input_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "grok: expected Utf8 input, got {:?}",
                    input_arr.data_type(),
                ))
            })?;

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
            // Unanchored find (Java grok's Matcher.find). A null input row, a row
            // that does not match anywhere, and a group that did not participate
            // all collapse to "" for that field — mirroring
            // GrokExpression.parseValue returning ExprStringValue("").
            let captures = if input.is_null(i) {
                None
            } else {
                // fancy-regex returns Result<Option<Captures>>; a runtime regex
                // error (e.g. backtrack-limit) surfaces as a DataFusion error
                // rather than a silent miss.
                resolved
                    .regex
                    .captures(input.value(i))
                    .map_err(|e| DataFusionError::Execution(format!("grok: match failed: {e}")))?
            };
            for (field, group) in &fields {
                builder.keys().append_value(field);
                let value = match &captures {
                    Some(c) => match c.name(group) {
                        Some(m) => clean_string(m.as_str()),
                        None => String::new(),
                    },
                    None => String::new(),
                };
                builder.values().append_value(value);
            }
            builder.append(true)?;
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Pull the string value out of a literal column. Returns None if the column is
/// non-scalar, a non-string scalar, or a SQL NULL — the adapter is meant to have
/// caught these at plan time but we re-check at runtime.
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

    fn run_grok(input: Vec<Option<&str>>, pattern: &str) -> Result<MapArray> {
        let udf = GrokUdf::new();
        let n = input.len();
        let input_arr = StringArray::from(input);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(input_arr)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("grok".to_string()))),
            ],
            number_rows: n,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("out", map_return_type(), true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args)?;
        let arr = out.into_array(n)?;
        Ok(arr.as_any().downcast_ref::<MapArray>().unwrap().clone())
    }

    /// Read row `i` of a MapArray into a (key,value) vec for assertions.
    fn row_entries(map: &MapArray, i: usize) -> Vec<(String, String)> {
        let entries = map.value(i);
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
        (0..keys.len())
            .map(|j| (keys.value(j).to_string(), values.value(j).to_string()))
            .collect()
    }

    fn value_for(map: &MapArray, i: usize, key: &str) -> Option<String> {
        row_entries(map, i)
            .into_iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    #[test]
    fn hostname_extraction_matches_legacy() {
        // testGrokEmail: ".+@%{HOSTNAME:host}" against an email.
        let map = run_grok(vec![Some("amberduke@pyrami.com")], ".+@%{HOSTNAME:host}").unwrap();
        assert_eq!(value_for(&map, 0, "host").as_deref(), Some("pyrami.com"));
    }

    #[test]
    fn number_greedydata_with_lookbehind_and_atomic_group() {
        // testGrokAddressOverriding: "%{NUMBER} %{GREEDYDATA:address}" — NUMBER →
        // BASE10NUM uses lookbehind + atomic group (regex crate would reject).
        let map = run_grok(
            vec![Some("880 Holmes Lane")],
            "%{NUMBER} %{GREEDYDATA:address}",
        )
        .unwrap();
        assert_eq!(
            value_for(&map, 0, "address").as_deref(),
            Some("Holmes Lane")
        );
    }

    #[test]
    fn commonapachelog_named_fields() {
        // testGrokApacheLog: COMMONAPACHELOG → timestamp + response extraction.
        let line = "177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] \"HEAD /e-business/mindshare HTTP/1.0\" 404 19927";
        let map = run_grok(vec![Some(line)], "%{COMMONAPACHELOG}").unwrap();
        assert_eq!(
            value_for(&map, 0, "timestamp").as_deref(),
            Some("28/Sep/2022:10:15:57 -0700")
        );
        assert_eq!(value_for(&map, 0, "response").as_deref(), Some("404"));
    }

    #[test]
    fn non_matching_and_null_rows_yield_empty_strings() {
        let map = run_grok(vec![None, Some("no-at-sign-here")], ".+@%{HOSTNAME:host}").unwrap();
        assert_eq!(value_for(&map, 0, "host").as_deref(), Some(""));
        assert_eq!(value_for(&map, 1, "host").as_deref(), Some(""));
    }

    #[test]
    fn unwanted_groups_are_dropped() {
        // NUMBER has no subname → UNWANTED → must not appear as an output key.
        let map = run_grok(
            vec![Some("880 Holmes Lane")],
            "%{NUMBER} %{GREEDYDATA:address}",
        )
        .unwrap();
        let keys: Vec<String> = row_entries(&map, 0).into_iter().map(|(k, _)| k).collect();
        assert!(keys.contains(&"address".to_string()));
        assert!(!keys.contains(&"UNWANTED".to_string()));
    }

    #[test]
    fn clean_string_strips_balanced_quotes() {
        assert_eq!(clean_string("\"foo\""), "foo");
        assert_eq!(clean_string("'bar'"), "bar");
        assert_eq!(clean_string("\"\""), "");
        assert_eq!(clean_string("\"a\"b\""), "\"a\"b\""); // interior quote → untouched
        assert_eq!(clean_string("plain"), "plain");
    }
}
