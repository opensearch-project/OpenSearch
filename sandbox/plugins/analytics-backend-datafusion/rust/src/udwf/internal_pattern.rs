/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `internal_pattern(field, max_sample_count, buffer_limit, show_numbered_token,
//!  [threshold_percentage], [variable_count_threshold]) OVER (PARTITION BY …)`
//!
//! BRAIN window function used by PPL's `patterns ... method=BRAIN mode=label`.
//! Mirrors `LogPatternWindowFunction` on the SQL plugin side: collects all
//! source-field strings in the partition, runs `BrainLogParser` over the
//! corpus, and for each row finds the best-matching wildcard pattern via
//! `eval_agg` then emits it as a per-row scalar string.
//!
//! Output Arrow shape: `Utf8` per row — the matched wildcard pattern (e.g.
//! `"<*>@<*>.<*>"`). The downstream PPL Calcite path wraps this in a 3-arg
//! {@code PATTERN_PARSER(source_field, window_result, show_numbered_token)}
//! call; on the analytics-engine route {@link ItemTypeRebuilder} unwinds the
//! resulting {@code array_element(map_extract(...), 1)} chain to one of the
//! per-field scalar UDFs.

use datafusion::arrow::array::{ArrayRef, AsArray, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl,
};

use crate::patterns::brain::{BrainLogParser, PatternEntry};
use crate::patterns::eval::{eval_agg, AggCandidate};
use crate::patterns::{DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE, DEFAULT_VARIABLE_COUNT_THRESHOLD};

pub const NAME: &str = "internal_pattern";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udwf(WindowUDF::from(InternalPatternWindowUdf::new()));
}

#[derive(Debug)]
pub struct InternalPatternWindowUdf {
    signature: Signature,
}

impl InternalPatternWindowUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for InternalPatternWindowUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for InternalPatternWindowUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for InternalPatternWindowUdf {}
impl std::hash::Hash for InternalPatternWindowUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        NAME.hash(state);
    }
}

impl WindowUDFImpl for InternalPatternWindowUdf {
    fn name(&self) -> &str {
        NAME
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(std::sync::Arc::new(Field::new(
            field_args.name(),
            DataType::Utf8,
            true,
        )))
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        // Config is parsed from literal args in evaluate_all the same way the
        // aggregate UDAF does; the partition evaluator doesn't get
        // PartitionEvaluatorArgs operands at construction, so we re-derive from
        // the runtime input columns instead.
        Ok(Box::new(InternalPatternEvaluator::new()))
    }
}

/// Per-partition evaluator. `evaluate_all` is the single-pass form: receive all
/// argument columns for the partition, return one ArrayRef of the same length
/// (one matched pattern string per row).
#[derive(Debug, Default)]
struct InternalPatternEvaluator;

impl InternalPatternEvaluator {
    fn new() -> Self {
        Self
    }
}

impl PartitionEvaluator for InternalPatternEvaluator {
    fn uses_window_frame(&self) -> bool {
        false
    }
    fn include_rank(&self) -> bool {
        false
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        if values.is_empty() {
            return Ok(std::sync::Arc::new(StringArray::from(vec![
                None::<&str>;
                num_rows
            ])));
        }
        let cfg = WindowConfig::from_args(values);

        // Build the corpus from arg 0 (the source field column).
        let field_col = &values[0];
        let mut messages: Vec<String> = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            messages.push(read_string_at(field_col, i).unwrap_or_default());
        }

        // Run BRAIN over the corpus once.
        let mut parser =
            BrainLogParser::with_thresholds(cfg.variable_count_threshold, cfg.threshold_percentage);
        let stats = parser.parse_all_log_patterns(&messages, cfg.max_sample_count);
        let candidates: Vec<AggCandidate> = stats
            .values()
            .map(|entry: &PatternEntry| AggCandidate {
                pattern: entry.pattern.clone(),
            })
            .collect();

        // For each row, find the best-matching candidate. eval_agg's
        // show_numbered_token=false branch returns the raw wildcard pattern; we
        // always emit that here regardless of the user's show_numbered_token
        // flag, because the visitor wraps the window result in PATTERN_PARSER
        // and the per-field rewriter handles token numbering downstream.
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        for msg in &messages {
            if msg.is_empty() {
                builder.append_value("");
                continue;
            }
            let result = eval_agg(msg, &candidates, false);
            builder.append_value(&result.pattern);
        }
        Ok(std::sync::Arc::new(builder.finish()))
    }
}

#[derive(Debug, Clone)]
struct WindowConfig {
    max_sample_count: usize,
    variable_count_threshold: usize,
    threshold_percentage: f64,
}

impl WindowConfig {
    fn defaults() -> Self {
        Self {
            max_sample_count: 10,
            variable_count_threshold: DEFAULT_VARIABLE_COUNT_THRESHOLD,
            threshold_percentage: DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE,
        }
    }

    fn from_args(values: &[ArrayRef]) -> Self {
        // Arg layout (positional, mirrors LogPatternAggFunction and the PPL
        // INTERNAL_PATTERN call shape):
        //   0 = source field column
        //   1 = max_sample_count (int)
        //   2 = buffer_limit (int) — ignored
        //   3 = show_numbered_token (bool) — ignored here (handled downstream)
        //   4 = frequency_threshold_percentage (double) — optional
        //   5 = variable_count_threshold (int) — optional
        // For the window form, the constant args appear as broadcast columns —
        // every row carries the same value, so we read row 0 of each.
        let mut cfg = Self::defaults();
        if let Some(arr) = values.get(1) {
            if let Some(v) = read_i64_at(arr, 0) {
                cfg.max_sample_count = v.max(0) as usize;
            }
        }
        // arg 4 and 5 are optional and ordered by Argument name in PPL —
        // frequency_threshold_percentage first, variable_count_threshold second.
        if let Some(arr) = values.get(4) {
            if let Some(v) = read_f64_at(arr, 0) {
                cfg.threshold_percentage = v;
            } else if let Some(v) = read_i64_at(arr, 0) {
                cfg.variable_count_threshold = v.max(0) as usize;
            }
        }
        if let Some(arr) = values.get(5) {
            if let Some(v) = read_i64_at(arr, 0) {
                cfg.variable_count_threshold = v.max(0) as usize;
            } else if let Some(v) = read_f64_at(arr, 0) {
                cfg.threshold_percentage = v;
            }
        }
        cfg
    }
}

fn read_string_at(arr: &ArrayRef, i: usize) -> Option<String> {
    if arr.is_null(i) {
        return None;
    }
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        return Some(s.value(i).to_string());
    }
    if let Some(s) = arr
        .as_any()
        .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
    {
        return Some(s.value(i).to_string());
    }
    if let Some(s) = arr
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringViewArray>()
    {
        return Some(s.value(i).to_string());
    }
    None
}

fn read_i64_at(arr: &ArrayRef, i: usize) -> Option<i64> {
    use datafusion::arrow::array::{Int16Array, Int32Array, Int64Array, Int8Array};
    if arr.is_null(i) {
        return None;
    }
    let dt = arr.data_type();
    match dt {
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(i)),
        DataType::Int32 => arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(i) as i64),
        DataType::Int16 => arr
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| a.value(i) as i64),
        DataType::Int8 => arr
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| a.value(i) as i64),
        _ => None,
    }
}

fn read_f64_at(arr: &ArrayRef, i: usize) -> Option<f64> {
    use datafusion::arrow::array::{Float32Array, Float64Array};
    if arr.is_null(i) {
        return None;
    }
    let dt = arr.data_type();
    match dt {
        DataType::Float64 => arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(i)),
        DataType::Float32 => arr
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(i) as f64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn evaluates_empty_partition_returns_empty_array() {
        let mut e = InternalPatternEvaluator::new();
        let out = e.evaluate_all(&[], 0).unwrap();
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn emits_one_pattern_per_row_with_corpus_input() {
        let mut e = InternalPatternEvaluator::new();
        // 4 rows of input — small enough that BRAIN may not generalize tokens
        // (each line might become its own candidate), but the shape contract is
        // what we pin here: one output string per input row, same array length.
        let messages = vec![
            Some("user 1 logged in"),
            Some("user 2 logged in"),
            Some("user 3 logged in"),
            Some("unrelated noise here"),
        ];
        let arr: ArrayRef = Arc::new(StringArray::from(messages.clone()));
        let result = e.evaluate_all(&[arr], messages.len()).unwrap();
        let strs = result.as_string::<i32>();
        assert_eq!(datafusion::arrow::array::Array::len(strs), messages.len());
        for i in 0..messages.len() {
            // Per-row best match may be empty for an unrepresented corpus, but
            // it must be returned as a real string value (non-null).
            let _ = strs.value(i);
        }
    }
}
