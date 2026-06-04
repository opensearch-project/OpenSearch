/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `span_bucket(value, span)` — fixed-width bucket label.
//!
//! Ports the OpenSearch-SQL bespoke `SPAN_BUCKET` UDF (see
//! `sql/core/.../binning/SpanBucketFunction.java`). Given a numeric value and a
//! positive numeric `span`, returns the half-open bucket `[binStart, binEnd)`
//! as a VARCHAR label like `"10-20"` or `"3.0-4.5"`.
//!
//! This is NOT the standard-SQL `width_bucket` / Calcite `SPAN_BUCKET`; it's a
//! PPL-specific binning helper that returns the bucket *label* (not a bucket
//! index). Any PPL `bin span=<n>` command routes through here.
//!
//! Semantics:
//! * `binStart = floor(value / span) * span`
//! * `binEnd   = binStart + span`
//! * Label is rendered with integer formatting when `span` is an integer and
//!   both bounds are integer-valued; otherwise floating with decimal places
//!   chosen by `span` magnitude (>=1 → 1dp, >=0.1 → 2dp, >=0.01 → 3dp, else 4dp)
//!   to match the Java reference.
//! * `span <= 0` → null (Java returns null, matches).
//! * Any null input → null (null propagation, matches Java's `NullPolicy.ANY`).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SpanBucketUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SpanBucketUdf {
    signature: Signature,
}

impl SpanBucketUdf {
    pub fn new() -> Self {
        // Both args canonicalised to Float64 via `coerce_types` — matches the
        // Java reference which boxes to `Number` and calls `doubleValue()`.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for SpanBucketUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SpanBucketUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "span_bucket"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("span_bucket expects 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "span_bucket",
            arg_types,
            &[CoerceMode::Float64, CoerceMode::Float64],
        )
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("span_bucket expects 2 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;
        let value = args.args[0].clone().into_array(n)?;
        let span = args.args[1].clone().into_array(n)?;

        let value = value
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "span_bucket: value expected Float64, got {:?}",
                    value.data_type()
                ))
            })?;
        let span = span
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "span_bucket: span expected Float64, got {:?}",
                    span.data_type()
                ))
            })?;

        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            if value.is_null(i) || span.is_null(i) {
                builder.append_null();
                continue;
            }
            match calculate(value.value(i), span.value(i)) {
                Some(s) => builder.append_value(&s),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Core span-bucket math. Direct port of Java
/// `SpanBucketFunction.calculateSpanBucket`.
///
/// Returns `None` when `span` is non-finite (NaN or ±Inf) or non-positive.
/// The Java reference only checks `span <= 0`, but an infinite span would
/// otherwise flow through `floor` / `is_integer_span` and produce garbage
/// labels (division by +Inf → 0, `0 + Inf = Inf`, formatting Inf as f64
/// yields `"inf-inf"`). Match the subtraitupdates port which adds the
/// `is_finite()` guard explicitly.
fn calculate(value: f64, span: f64) -> Option<String> {
    if !span.is_finite() || span <= 0.0 {
        // Covers NaN (is_finite false), ±Inf (is_finite false), zero, and negatives.
        return None;
    }
    let bin_start = (value / span).floor() * span;
    let bin_end = bin_start + span;
    Some(format_range(bin_start, bin_end, span))
}

fn format_range(bin_start: f64, bin_end: f64, span: f64) -> String {
    if is_integer_span(span) && is_integer_value(bin_start) && is_integer_value(bin_end) {
        // Integer path — match Java `String.format("%d-%d", (long)...)`.
        format!("{}-{}", bin_start as i64, bin_end as i64)
    } else {
        let decimals = decimal_places_for(span);
        format!(
            "{:.*}-{:.*}",
            decimals, bin_start, decimals, bin_end
        )
    }
}

fn is_integer_span(span: f64) -> bool {
    // Matches Java `span == Math.floor(span) && !Double.isInfinite(span)`.
    span.is_finite() && span == span.floor()
}

fn is_integer_value(v: f64) -> bool {
    // Matches Java `Math.abs(v - Math.round(v)) < 1e-10`.
    (v - v.round()).abs() < 1e-10
}

fn decimal_places_for(span: f64) -> usize {
    if span >= 1.0 {
        1
    } else if span >= 0.1 {
        2
    } else if span >= 0.01 {
        3
    } else {
        4
    }
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Expected values seeded from the Java reference
    //! `SpanBucketFunction.calculateSpanBucket` — any behavioural drift from the
    //! Java implementation is a bug in the port, not the test.
    use super::*;

    // ── Integer path ───────────────────────────────────────────────────────

    #[test]
    fn integer_value_and_span_render_as_long_range() {
        assert_eq!(calculate(15.0, 10.0), Some("10-20".to_string()));
        assert_eq!(calculate(25.0, 10.0), Some("20-30".to_string()));
        assert_eq!(calculate(0.0, 10.0), Some("0-10".to_string()));
    }

    #[test]
    fn value_on_bin_boundary_belongs_to_upper_bin() {
        // Java: floor(10.0 / 10.0) = 1.0 → binStart = 10, binEnd = 20.
        // The bin is half-open [start, end), so boundary value maps to upper bin.
        assert_eq!(calculate(10.0, 10.0), Some("10-20".to_string()));
        assert_eq!(calculate(20.0, 10.0), Some("20-30".to_string()));
    }

    #[test]
    fn negative_value_rounds_down_toward_negative_infinity() {
        // floor(-5/10) = -1 → binStart = -10, binEnd = 0.
        assert_eq!(calculate(-5.0, 10.0), Some("-10-0".to_string()));
        // floor(-0.1/10) = -1 → binStart = -10, binEnd = 0.
        assert_eq!(calculate(-0.1, 10.0), Some("-10-0".to_string()));
        // Negative on boundary: floor(-10/10) = -1 → binStart = -10.
        assert_eq!(calculate(-10.0, 10.0), Some("-10-0".to_string()));
    }

    // ── Floating-point path — decimal places from span magnitude ───────────

    #[test]
    fn non_integer_span_gte_one_renders_one_decimal() {
        // span=1.5 → 1dp. floor(3.7/1.5)=2 → binStart=3.0, binEnd=4.5.
        assert_eq!(calculate(3.7, 1.5), Some("3.0-4.5".to_string()));
    }

    #[test]
    fn span_between_point_one_and_one_renders_two_decimals() {
        // span=0.1. floor(0.25/0.1) = floor(2.5) = 2 → binStart=0.2, binEnd=0.3.
        assert_eq!(calculate(0.25, 0.1), Some("0.20-0.30".to_string()));
    }

    #[test]
    fn span_between_point_zero_one_and_point_one_renders_three_decimals() {
        // span=0.05 (< 0.1, >= 0.01) → 3dp. floor(0.17/0.05) = floor(3.4) = 3
        // → binStart=0.15, binEnd=0.2.
        assert_eq!(calculate(0.17, 0.05), Some("0.150-0.200".to_string()));
    }

    #[test]
    fn span_below_point_zero_one_renders_four_decimals() {
        // span=0.005 → 4dp. floor(0.013/0.005) = floor(2.6) = 2
        // → binStart=0.01, binEnd=0.015.
        assert_eq!(calculate(0.013, 0.005), Some("0.0100-0.0150".to_string()));
    }

    // ── Null-producing inputs ──────────────────────────────────────────────

    #[test]
    fn zero_span_returns_null() {
        assert_eq!(calculate(42.0, 0.0), None);
    }

    #[test]
    fn negative_span_returns_null() {
        assert_eq!(calculate(42.0, -1.0), None);
    }

    #[test]
    fn nan_span_returns_null() {
        assert_eq!(calculate(42.0, f64::NAN), None);
    }

    #[test]
    fn infinite_span_returns_null() {
        // `span > 0.0` is true for +Inf but `is_integer_span` would say "no"
        // and we'd fall into the float formatter, producing garbage. Guard
        // matches Java's `span <= 0` behaviour and subtraitupdates's impl
        // (which additionally explicitly checks `span.is_finite()`).
        assert_eq!(calculate(42.0, f64::INFINITY), None);
        assert_eq!(calculate(42.0, f64::NEG_INFINITY), None);
    }

    // ── Edge: value is null / span is null ─────────────────────────────────
    // Handled by the caller (`invoke_with_args`) via arrow null-bitmap checks,
    // not by `calculate`. Covered indirectly in batch tests below.

    // ── Batch invocation via DataFusion scalar harness ─────────────────────

    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::ScalarFunctionArgs;
    use std::sync::Arc;

    fn invoke_batch(values: Vec<Option<f64>>, spans: Vec<Option<f64>>) -> Vec<Option<String>> {
        assert_eq!(values.len(), spans.len());
        let n = values.len();
        let udf = SpanBucketUdf::new();
        let value_arr = Arc::new(Float64Array::from(values)) as ArrayRef;
        let span_arr = Arc::new(Float64Array::from(spans)) as ArrayRef;
        let return_field = Arc::new(Field::new("out", DataType::Utf8, true));
        let arg_fields = vec![
            Arc::new(Field::new("value", DataType::Float64, true)),
            Arc::new(Field::new("span", DataType::Float64, true)),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(value_arr),
                ColumnarValue::Array(span_arr),
            ],
            arg_fields,
            number_rows: n,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(a) = out else { panic!("expected array") };
        let s = a.as_string::<i32>();
        (0..n)
            .map(|i| if s.is_null(i) { None } else { Some(s.value(i).to_string()) })
            .collect()
    }

    #[test]
    fn batch_happy_path_renders_each_row_independently() {
        let out = invoke_batch(
            vec![Some(15.0), Some(0.0), Some(-5.0)],
            vec![Some(10.0), Some(10.0), Some(10.0)],
        );
        assert_eq!(
            out,
            vec![
                Some("10-20".to_string()),
                Some("0-10".to_string()),
                Some("-10-0".to_string())
            ]
        );
    }

    #[test]
    fn batch_null_value_propagates_null() {
        let out = invoke_batch(vec![None, Some(15.0)], vec![Some(10.0), Some(10.0)]);
        assert_eq!(out, vec![None, Some("10-20".to_string())]);
    }

    #[test]
    fn batch_null_span_propagates_null() {
        let out = invoke_batch(vec![Some(15.0), Some(25.0)], vec![None, Some(10.0)]);
        assert_eq!(out, vec![None, Some("20-30".to_string())]);
    }

    #[test]
    fn batch_nonpositive_span_produces_null_not_error() {
        let out = invoke_batch(
            vec![Some(42.0), Some(42.0), Some(42.0)],
            vec![Some(0.0), Some(-1.0), Some(5.0)],
        );
        assert_eq!(out, vec![None, None, Some("40-45".to_string())]);
    }

    // ── coerce_types accepts every integer/float source ────────────────────

    #[test]
    fn coerce_types_canonicalises_mixed_numeric_inputs() {
        let udf = SpanBucketUdf::new();
        let out = udf
            .coerce_types(&[DataType::Int32, DataType::Int64])
            .unwrap();
        assert_eq!(out, vec![DataType::Float64, DataType::Float64]);
    }

    #[test]
    fn coerce_types_rejects_string_input() {
        let udf = SpanBucketUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Float64])
            .is_err());
    }

    #[test]
    fn return_type_enforces_two_arg_arity() {
        let udf = SpanBucketUdf::new();
        assert_eq!(
            udf.return_type(&[DataType::Float64, DataType::Float64])
                .unwrap(),
            DataType::Utf8
        );
        assert!(udf.return_type(&[DataType::Float64]).is_err());
        assert!(udf
            .return_type(&[DataType::Float64, DataType::Float64, DataType::Float64])
            .is_err());
    }
}
