/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `minspan_bucket(value, min_span, data_range, max_value)` — minimum-span
//! histogram bucket label.
//!
//! Ports the OpenSearch-SQL bespoke `MINSPAN_BUCKET` UDF (see
//! `sql/core/.../binning/MinspanBucketFunction.java`). Given a numeric value
//! and a minimum-span floor, picks the larger of the data's natural order-of-
//! magnitude width and the user-requested minimum span, then returns the
//! half-open bucket `[binStart, binEnd)` containing `value` as a VARCHAR label.
//!
//! Semantics:
//! * `minSpan <= 0` → null (Java: `minSpan <= 0` check)
//! * `range <= 0` or non-finite → null
//! * `minspanWidth = 10^ceil(log10(minSpan))` — round minSpan UP to next
//!   power of 10
//! * `defaultWidth  = 10^floor(log10(range))` — round range DOWN to next
//!   power of 10
//! * Pick `defaultWidth` when `defaultWidth >= minSpan` (range already
//!   produces coarse-enough bins); otherwise `minspanWidth` (user constraint
//!   dominates)
//! * Same integer/float label formatting as `span_bucket` / `width_bucket`
//!
//! The `max_value` argument is carried through the signature for PPL call-shape
//! parity but is unused by the algorithm (matches Java's `// currently unused
//! but kept for compatibility` comment). Removing it from the Rust signature
//! would require diverging from PPL's emit pattern, which this port explicitly
//! avoids.

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
    ctx.register_udf(ScalarUDF::from(MinspanBucketUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MinspanBucketUdf {
    signature: Signature,
}

impl MinspanBucketUdf {
    pub fn new() -> Self {
        // All 4 slots Float64 post-coerce — matches `span_bucket` /
        // `width_bucket`'s downcast-once contract. num_bins-like integer
        // slots are not applicable here (Java algorithm uses minSpan as a
        // numeric floor, not an integer count).
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for MinspanBucketUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for MinspanBucketUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "minspan_bucket"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 4 {
            return plan_err!("minspan_bucket expects 4 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "minspan_bucket",
            arg_types,
            &[
                CoerceMode::Float64,
                CoerceMode::Float64,
                CoerceMode::Float64,
                CoerceMode::Float64,
            ],
        )
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 4 {
            return plan_err!("minspan_bucket expects 4 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;
        let value = args.args[0].clone().into_array(n)?;
        let min_span = args.args[1].clone().into_array(n)?;
        let range = args.args[2].clone().into_array(n)?;
        let max_value = args.args[3].clone().into_array(n)?;

        let value = downcast_f64(&value, "value")?;
        let min_span = downcast_f64(&min_span, "min_span")?;
        let range = downcast_f64(&range, "data_range")?;
        let max_value = downcast_f64(&max_value, "max_value")?;

        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            if value.is_null(i) || min_span.is_null(i) || range.is_null(i) || max_value.is_null(i) {
                builder.append_null();
                continue;
            }
            match calculate(value.value(i), min_span.value(i), range.value(i), max_value.value(i))
            {
                Some(s) => builder.append_value(&s),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn downcast_f64<'a>(arr: &'a ArrayRef, slot: &str) -> Result<&'a Float64Array> {
    arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "minspan_bucket: slot '{slot}' expected Float64 post-coerce, got {:?}",
            arr.data_type()
        ))
    })
}

/// Core minspan-bucket math. Direct port of Java
/// `MinspanBucketFunction.calculateMinspanBucket`.
///
/// `_max_value` is unused but preserved in the signature to match Java's
/// 4-arg API (which has the same no-op parameter). See module doc.
fn calculate(value: f64, min_span: f64, range: f64, _max_value: f64) -> Option<String> {
    if !min_span.is_finite() || min_span <= 0.0 {
        return None;
    }
    if !range.is_finite() || range <= 0.0 {
        return None;
    }
    // minspanWidth = 10^ceil(log10(minSpan)) — round minSpan up to next power of 10.
    let minspan_width = 10f64.powf(min_span.log10().ceil());
    // defaultWidth = 10^floor(log10(range)) — round range down to next power of 10.
    let default_width = 10f64.powf(range.log10().floor());
    // Java: `useDefault = defaultWidth >= minSpan`, i.e. data's natural order-
    // of-magnitude width is already at least minSpan, so use it; otherwise
    // use minspan_width so every bin is at least minSpan wide.
    let width = if default_width >= min_span { default_width } else { minspan_width };
    if !width.is_finite() || width <= 0.0 {
        return None;
    }
    let bin_start = (value / width).floor() * width;
    let bin_end = bin_start + width;
    Some(format_range(bin_start, bin_end, width))
}

// Same formatter as span_bucket / width_bucket; duplicated locally per the
// same rationale documented on width_bucket's format_range.
fn format_range(bin_start: f64, bin_end: f64, span: f64) -> String {
    if is_integer_span(span) && is_integer_value(bin_start) && is_integer_value(bin_end) {
        format!("{}-{}", bin_start as i64, bin_end as i64)
    } else {
        let decimals = decimal_places_for(span);
        format!("{:.*}-{:.*}", decimals, bin_start, decimals, bin_end)
    }
}

fn is_integer_span(span: f64) -> bool {
    span.is_finite() && span == span.floor()
}

fn is_integer_value(v: f64) -> bool {
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
    //! Expected values seeded from Java reference
    //! `MinspanBucketFunction.calculateMinspanBucket`. Where the branching
    //! matters (use_default vs. use_minspan), the test names describe the
    //! invariant being exercised.
    use super::*;

    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::ScalarFunctionArgs;

    // ── `defaultWidth >= minSpan` branch (data's order-of-magnitude wins) ──

    #[test]
    fn default_width_wins_when_range_is_order_of_magnitude_above_min_span() {
        // minSpan=3 → minspanWidth=10^1=10
        // range=100 → defaultWidth=10^2=100
        // 100 >= 3 → width=100
        // value=15 → binStart=0, binEnd=100 → "0-100"
        assert_eq!(
            calculate(15.0, 3.0, 100.0, 100.0),
            Some("0-100".to_string())
        );
    }

    #[test]
    fn default_width_wins_produces_label_covering_full_range() {
        // minSpan=0.05 → minspanWidth=10^ceil(-1.301)=10^-1=0.1
        // range=1.0 → defaultWidth=10^floor(0)=10^0=1
        // 1 >= 0.05 → width=1 (integer)
        // value=0.7 → binStart=0, binEnd=1 → "0-1"
        assert_eq!(
            calculate(0.7, 0.05, 1.0, 1.0),
            Some("0-1".to_string())
        );
    }

    // ── `defaultWidth < minSpan` branch (user floor wins) ──────────────────

    #[test]
    fn minspan_width_wins_when_range_narrow_relative_to_min_span() {
        // minSpan=3 → minspanWidth=10
        // range=5 → defaultWidth=10^0=1
        // 1 >= 3 false → width=10
        // value=15 → binStart=10, binEnd=20 → "10-20"
        assert_eq!(
            calculate(15.0, 3.0, 5.0, 5.0),
            Some("10-20".to_string())
        );
    }

    #[test]
    fn minspan_width_applied_even_when_value_outside_range() {
        // Tests that the UDF doesn't gate on value being inside [0, range].
        // minSpan=0.5 → minspanWidth=10^0=1
        // range=0.3 → defaultWidth=10^-1=0.1
        // 0.1 >= 0.5 false → width=1
        // value=42 → binStart=42, binEnd=43 → "42-43"
        assert_eq!(
            calculate(42.0, 0.5, 0.3, 0.3),
            Some("42-43".to_string())
        );
    }

    // ── min_span guards ────────────────────────────────────────────────────

    #[test]
    fn zero_min_span_returns_null() {
        assert_eq!(calculate(5.0, 0.0, 100.0, 100.0), None);
    }

    #[test]
    fn negative_min_span_returns_null() {
        assert_eq!(calculate(5.0, -1.0, 100.0, 100.0), None);
    }

    #[test]
    fn non_finite_min_span_returns_null() {
        assert_eq!(calculate(5.0, f64::NAN, 100.0, 100.0), None);
        assert_eq!(calculate(5.0, f64::INFINITY, 100.0, 100.0), None);
        assert_eq!(calculate(5.0, f64::NEG_INFINITY, 100.0, 100.0), None);
    }

    // ── range guards ───────────────────────────────────────────────────────

    #[test]
    fn zero_range_returns_null() {
        assert_eq!(calculate(5.0, 3.0, 0.0, 100.0), None);
    }

    #[test]
    fn negative_range_returns_null() {
        assert_eq!(calculate(5.0, 3.0, -1.0, 100.0), None);
    }

    #[test]
    fn non_finite_range_returns_null() {
        assert_eq!(calculate(5.0, 3.0, f64::NAN, 100.0), None);
        assert_eq!(calculate(5.0, 3.0, f64::INFINITY, 100.0), None);
    }

    // ── max_value is ignored (per Java's // currently unused comment) ──────

    #[test]
    fn max_value_is_ignored_in_current_algorithm() {
        // Same (value, min_span, range) with different max_value values —
        // results must be identical. This is a regression guard: if someone
        // later adds max_value into the algorithm (as width_bucket does),
        // this test reminds them to audit Java semantics first.
        let a = calculate(15.0, 3.0, 100.0, 100.0);
        let b = calculate(15.0, 3.0, 100.0, 99999.0);
        let c = calculate(15.0, 3.0, 100.0, -1.0);
        assert_eq!(a, Some("0-100".to_string()));
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    // ── Formatter coverage — non-integer widths ────────────────────────────

    #[test]
    fn float_width_uses_decimal_format() {
        // minSpan=0.05 → minspanWidth=0.1
        // range=0.3 → defaultWidth=10^-1=0.1 (log10(0.3) ≈ -0.523, floor=-1)
        // 0.1 >= 0.05 → width=0.1
        // value=0.17 → binStart = floor(0.17/0.1)*0.1 = floor(1.7)*0.1 = 0.1, binEnd=0.2
        // width 0.1: non-integer → 2dp → "0.10-0.20"
        assert_eq!(
            calculate(0.17, 0.05, 0.3, 0.3),
            Some("0.10-0.20".to_string())
        );
    }

    // ── Batch layer: null propagation ──────────────────────────────────────

    fn invoke_batch(
        values: Vec<Option<f64>>,
        min_spans: Vec<Option<f64>>,
        ranges: Vec<Option<f64>>,
        maxes: Vec<Option<f64>>,
    ) -> Vec<Option<String>> {
        let n = values.len();
        assert_eq!(n, min_spans.len());
        assert_eq!(n, ranges.len());
        assert_eq!(n, maxes.len());
        let udf = MinspanBucketUdf::new();
        let return_field = Arc::new(Field::new("out", DataType::Utf8, true));
        let arg_fields = vec![
            Arc::new(Field::new("value", DataType::Float64, true)),
            Arc::new(Field::new("min_span", DataType::Float64, true)),
            Arc::new(Field::new("data_range", DataType::Float64, true)),
            Arc::new(Field::new("max_value", DataType::Float64, true)),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(values)) as ArrayRef),
                ColumnarValue::Array(Arc::new(Float64Array::from(min_spans)) as ArrayRef),
                ColumnarValue::Array(Arc::new(Float64Array::from(ranges)) as ArrayRef),
                ColumnarValue::Array(Arc::new(Float64Array::from(maxes)) as ArrayRef),
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
    fn batch_null_in_any_slot_propagates_null() {
        let out = invoke_batch(
            vec![None, Some(5.0), Some(5.0), Some(5.0), Some(5.0)],
            vec![Some(3.0), None, Some(3.0), Some(3.0), Some(3.0)],
            vec![Some(100.0), Some(100.0), None, Some(100.0), Some(100.0)],
            vec![Some(100.0), Some(100.0), Some(100.0), None, Some(100.0)],
        );
        assert_eq!(out, vec![None, None, None, None, Some("0-100".to_string())]);
    }

    // ── coerce_types ───────────────────────────────────────────────────────

    #[test]
    fn coerce_types_canonicalises_mixed_numeric_inputs() {
        let udf = MinspanBucketUdf::new();
        let out = udf
            .coerce_types(&[
                DataType::Int32,
                DataType::Float32,
                DataType::Int64,
                DataType::Float64,
            ])
            .unwrap();
        assert_eq!(
            out,
            vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ]
        );
    }

    #[test]
    fn coerce_types_rejects_non_numeric() {
        let udf = MinspanBucketUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Float64, DataType::Float64, DataType::Float64])
            .is_err());
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = MinspanBucketUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Float64, DataType::Float64, DataType::Float64])
            .is_err());
    }

    #[test]
    fn return_type_enforces_four_arg_arity() {
        let udf = MinspanBucketUdf::new();
        assert_eq!(
            udf.return_type(&[
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64
            ])
            .unwrap(),
            DataType::Utf8
        );
        assert!(udf.return_type(&[DataType::Float64]).is_err());
    }
}
