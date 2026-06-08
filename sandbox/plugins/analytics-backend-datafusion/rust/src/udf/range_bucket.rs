/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `range_bucket(value, data_min, data_max, start_param, end_param)` —
//! range-based bucket label (PPL).
//!
//! Ports the OpenSearch-SQL bespoke `RANGE_BUCKET` UDF (see sql/core's
//! `RangeBucketFunction`) which PPL emits via
//! `PPLBuiltinOperators.RANGE_BUCKET`. Unlike `width_bucket` / `minspan_
//! bucket`, this one has asymmetric null-handling: `value` / `data_min` /
//! `data_max` being null propagates null, but `start_param` / `end_param`
//! are nullable sentinels meaning "use the observed data bound".
//!
//! <b>Window-dependency flag:</b> the PPL frontend's `bin start=... end=...`
//! command wraps `data_min` and `data_max` in `MIN(field) OVER ()` and
//! `MAX(field) OVER ()` empty-partition window aggregates. End-to-end
//! pushdown through the backend via the `bin` command therefore requires
//! the backend to support windowed aggregates over empty partitions.
//! This UDF is independent of that capability — it's a pure scalar — but
//! the IT suite calls it directly via `eval range_bucket(value, literal,
//! literal, null, null)` to avoid the window wrapper. Once empty-partition
//! window pushdown lands, the `bin` command exercises this UDF too.
//!
//! Algorithm:
//! 1. Expansion-only effective bounds:
//!    - `effective_min = min(start_param, data_min)` when start_param not null
//!    - `effective_max = max(end_param,   data_max)` when end_param   not null
//! 2. `effective_range = effective_max - effective_min`; `<= 0 → null`
//! 3. Magnitude-based width:
//!    - if `effective_range` is exactly a power of 10 → width =
//!      `10^(floor(log10(range)) - 1)` (one magnitude finer)
//!    - else width = `10^floor(log10(range))`
//! 4. `first_bin_start = floor(effective_min / width) * width`
//! 5. `bin_index = floor((value - first_bin_start) / width)`
//! 6. `bin_start = bin_index * width + first_bin_start`; `bin_end = bin_start
//!    + width`; label via the same integer/float formatter as span_bucket
//!    and width_bucket.

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
    ctx.register_udf(ScalarUDF::from(RangeBucketUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RangeBucketUdf {
    signature: Signature,
}

impl RangeBucketUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for RangeBucketUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for RangeBucketUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "range_bucket"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 5 {
            return plan_err!("range_bucket expects 5 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "range_bucket",
            arg_types,
            &[
                CoerceMode::Float64,
                CoerceMode::Float64,
                CoerceMode::Float64,
                CoerceMode::Float64,
                CoerceMode::Float64,
            ],
        )
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 5 {
            return plan_err!("range_bucket expects 5 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;
        let value = args.args[0].clone().into_array(n)?;
        let data_min = args.args[1].clone().into_array(n)?;
        let data_max = args.args[2].clone().into_array(n)?;
        let start_param = args.args[3].clone().into_array(n)?;
        let end_param = args.args[4].clone().into_array(n)?;

        let value = downcast_f64(&value, "value")?;
        let data_min = downcast_f64(&data_min, "data_min")?;
        let data_max = downcast_f64(&data_max, "data_max")?;
        let start_param = downcast_f64(&start_param, "start_param")?;
        let end_param = downcast_f64(&end_param, "end_param")?;

        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            // Null propagation on the three hard-required slots.
            if value.is_null(i) || data_min.is_null(i) || data_max.is_null(i) {
                builder.append_null();
                continue;
            }
            // start_param / end_param are nullable sentinels for "use the
            // observed data bound". Don't null-propagate from them.
            let start = if start_param.is_null(i) {
                None
            } else {
                Some(start_param.value(i))
            };
            let end = if end_param.is_null(i) {
                None
            } else {
                Some(end_param.value(i))
            };
            match calculate(value.value(i), data_min.value(i), data_max.value(i), start, end) {
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
            "range_bucket: slot '{slot}' expected Float64 post-coerce, got {:?}",
            arr.data_type()
        ))
    })
}

/// Core range-bucket math. Direct port of Java
/// `RangeBucketFunction.calculateRangeBucket`.
fn calculate(
    value: f64,
    data_min: f64,
    data_max: f64,
    start_param: Option<f64>,
    end_param: Option<f64>,
) -> Option<String> {
    // Expansion-only: start_param can only lower effective_min; end_param
    // can only raise effective_max. Missing params leave the observed
    // bounds unchanged.
    let effective_min = match start_param {
        Some(s) => s.min(data_min),
        None => data_min,
    };
    let effective_max = match end_param {
        Some(e) => e.max(data_max),
        None => data_max,
    };
    let effective_range = effective_max - effective_min;
    if !effective_range.is_finite() || effective_range <= 0.0 {
        return None;
    }
    let width = magnitude_based_width(effective_range);
    if !width.is_finite() || width <= 0.0 {
        return None;
    }
    let first_bin_start = (effective_min / width).floor() * width;
    let adjusted = value - first_bin_start;
    let bin_index = (adjusted / width).floor();
    let bin_start = bin_index * width + first_bin_start;
    let bin_end = bin_start + width;
    Some(format_range(bin_start, bin_end, width))
}

/// Mirrors `RangeBucketFunction.calculateMagnitudeBasedWidth`. Exact powers
/// of 10 get one magnitude finer width (so a range of exactly 100 yields
/// width 10, not width 100, producing 10 labelled bins instead of 1).
fn magnitude_based_width(effective_range: f64) -> f64 {
    let log10_range = effective_range.log10();
    let floor_log = log10_range.floor();
    let is_exact_power_of_10 = (log10_range - floor_log).abs() < 1e-10;
    let adjusted = if is_exact_power_of_10 { floor_log - 1.0 } else { floor_log };
    10f64.powf(adjusted)
}

// Same format helpers as span_bucket / width_bucket / minspan_bucket;
// duplicated locally per the same rationale.
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
    //! Expected values seeded from the Java reference
    //! `RangeBucketFunction.calculateRangeBucket`.
    use super::*;

    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Field;

    // ── Happy path with no user-supplied start/end ─────────────────────────

    #[test]
    fn no_params_uses_data_bounds_as_effective_range() {
        // data_min=0, data_max=100 → range=100 (exact power of 10) →
        //   width = 10^(2-1) = 10.
        // first_bin_start = floor(0/10)*10 = 0.
        // value=50 → adjusted=50, bin_index=5, bin_start=50, bin_end=60.
        assert_eq!(
            calculate(50.0, 0.0, 100.0, None, None),
            Some("50-60".to_string())
        );
    }

    #[test]
    fn non_power_of_ten_range_uses_same_magnitude_width() {
        // data_min=10, data_max=99 → range=89 (not power of 10) →
        //   width = 10^floor(log10(89)) = 10^1 = 10.
        // first_bin_start = floor(10/10)*10 = 10.
        // value=50 → adjusted=40, bin_index=4, bin_start=50, bin_end=60.
        assert_eq!(
            calculate(50.0, 10.0, 99.0, None, None),
            Some("50-60".to_string())
        );
    }

    // ── Expansion-only semantics ───────────────────────────────────────────

    #[test]
    fn start_param_expands_effective_min_downward() {
        // data_min=10, start=0 → effective_min = min(0, 10) = 0.
        // data_max=100, end=null → effective_max = 100. range=100 → width=10.
        // first_bin_start = floor(0/10)*10 = 0. value=5 → bin_index=0,
        // bin_start=0, bin_end=10.
        assert_eq!(
            calculate(5.0, 10.0, 100.0, Some(0.0), None),
            Some("0-10".to_string())
        );
    }

    #[test]
    fn start_param_greater_than_data_min_is_ignored() {
        // data_min=10, start=20 → effective_min = min(20, 10) = 10 (expand-only).
        // Matches Java's "only expand, never shrink" comment.
        // data_max=100, end=null → range=100−10=90 → not power of 10 → width=10.
        // first_bin_start = 10. value=50 → adjusted=40, bin_index=4, binStart=50.
        assert_eq!(
            calculate(50.0, 10.0, 100.0, Some(20.0), None),
            Some("50-60".to_string())
        );
    }

    #[test]
    fn end_param_expands_effective_max_upward() {
        // data_min=-5, data_max=10, end=20 → effective_max = max(20, 10) = 20.
        // start=-10 → effective_min = min(-10, -5) = -10. range=30 → not power
        // of 10 → width=10.
        // first_bin_start = floor(-10/10)*10 = -10. value=5 → adjusted=15,
        // bin_index=1, bin_start=0, bin_end=10.
        assert_eq!(
            calculate(5.0, -5.0, 10.0, Some(-10.0), Some(20.0)),
            Some("0-10".to_string())
        );
    }

    #[test]
    fn end_param_less_than_data_max_is_ignored() {
        // data_max=100, end=50 → effective_max = max(50, 100) = 100.
        // data_min=0 → range=100 → power of 10 → width=10. value=75 → binStart=70.
        assert_eq!(
            calculate(75.0, 0.0, 100.0, None, Some(50.0)),
            Some("70-80".to_string())
        );
    }

    // ── Range guards ───────────────────────────────────────────────────────

    #[test]
    fn non_positive_range_returns_null() {
        assert_eq!(calculate(5.0, 10.0, 10.0, None, None), None);
        assert_eq!(calculate(5.0, 20.0, 10.0, None, None), None);
    }

    #[test]
    fn non_finite_bound_yields_null_via_range_guard() {
        assert_eq!(calculate(5.0, f64::NEG_INFINITY, 100.0, None, None), None);
        assert_eq!(calculate(5.0, 0.0, f64::INFINITY, None, None), None);
        assert_eq!(calculate(5.0, 0.0, f64::NAN, None, None), None);
    }

    // ── Magnitude-based width ──────────────────────────────────────────────

    #[test]
    fn exact_power_of_ten_range_drops_width_one_magnitude() {
        // range=100 → log10(range)=2 (exact) → width = 10^(2-1) = 10.
        // range=10 → log10=1 (exact) → width = 10^0 = 1.
        // range=1 → log10=0 (exact) → width = 10^(0-1) = 0.1.
        assert!((magnitude_based_width(100.0) - 10.0).abs() < 1e-12);
        assert!((magnitude_based_width(10.0) - 1.0).abs() < 1e-12);
        assert!((magnitude_based_width(1.0) - 0.1).abs() < 1e-12);
    }

    #[test]
    fn non_power_of_ten_range_uses_floor_magnitude() {
        // range=50 → log10≈1.699 → floor=1 → width=10.
        // range=500 → log10≈2.699 → floor=2 → width=100.
        assert!((magnitude_based_width(50.0) - 10.0).abs() < 1e-12);
        assert!((magnitude_based_width(500.0) - 100.0).abs() < 1e-12);
    }

    // ── Fractional width formatter branch ──────────────────────────────────

    #[test]
    fn fractional_width_triggers_decimal_format() {
        // range=1 → power of 10 → width=0.1.
        // dMin=0, dMax=1 → first_bin_start = floor(0/0.1)*0.1 = 0.
        // value=0.55 → adjusted=0.55, bin_index=floor(5.5)=5, bin_start=0.5,
        // bin_end=0.6. width=0.1 → 2dp → "0.50-0.60".
        assert_eq!(
            calculate(0.55, 0.0, 1.0, None, None),
            Some("0.50-0.60".to_string())
        );
    }

    // ── Batch: null propagation differs per slot ───────────────────────────

    fn invoke_batch(
        values: Vec<Option<f64>>,
        data_mins: Vec<Option<f64>>,
        data_maxes: Vec<Option<f64>>,
        start_params: Vec<Option<f64>>,
        end_params: Vec<Option<f64>>,
    ) -> Vec<Option<String>> {
        let n = values.len();
        let udf = RangeBucketUdf::new();
        let return_field = Arc::new(Field::new("out", DataType::Utf8, true));
        let arg_fields = vec![
            Arc::new(Field::new("value", DataType::Float64, true)),
            Arc::new(Field::new("data_min", DataType::Float64, true)),
            Arc::new(Field::new("data_max", DataType::Float64, true)),
            Arc::new(Field::new("start_param", DataType::Float64, true)),
            Arc::new(Field::new("end_param", DataType::Float64, true)),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(values)) as ArrayRef),
                ColumnarValue::Array(Arc::new(Float64Array::from(data_mins)) as ArrayRef),
                ColumnarValue::Array(Arc::new(Float64Array::from(data_maxes)) as ArrayRef),
                ColumnarValue::Array(Arc::new(Float64Array::from(start_params)) as ArrayRef),
                ColumnarValue::Array(Arc::new(Float64Array::from(end_params)) as ArrayRef),
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
    fn batch_null_in_value_or_bounds_propagates_null() {
        let out = invoke_batch(
            vec![None, Some(50.0), Some(50.0), Some(50.0)],
            vec![Some(0.0), None, Some(0.0), Some(0.0)],
            vec![Some(100.0), Some(100.0), None, Some(100.0)],
            vec![None, None, None, None],
            vec![None, None, None, None],
        );
        assert_eq!(out, vec![None, None, None, Some("50-60".to_string())]);
    }

    #[test]
    fn batch_null_start_or_end_params_do_not_null_propagate() {
        // Asymmetric: null start_param/end_param means "no expansion from
        // that side" — the row still produces a non-null label (unlike a
        // null value / data_min / data_max which would propagate null).
        //
        // Same (value=50, data_min=0, data_max=100) across 4 rows, varying
        // start/end params. Each expansion changes the effective range and
        // therefore the width and bin alignment, so outputs legitimately
        // differ row-to-row; the invariant this test guards is purely
        // "no row is null".
        //
        // Row 0: start=None, end=None     → range=100 (power-of-10) → w=10 → "50-60"
        // Row 1: start=-10, end=None      → effective [-10,100], range=110 → w=100,
        //         first_bin_start=floor(-10/100)*100=-100, value=50 →
        //         adjusted=150, bin_index=1 → binStart=0, binEnd=100 → "0-100"
        // Row 2: start=None, end=200      → effective [0,200],   range=200 → w=100,
        //         first_bin_start=0, value=50 → binStart=0, binEnd=100 → "0-100"
        // Row 3: start=-10, end=200       → effective [-10,200], range=210 → w=100,
        //         first_bin_start=-100, value=50 → adjusted=150, bin_index=1,
        //         binStart=0, binEnd=100 → "0-100"
        let out = invoke_batch(
            vec![Some(50.0), Some(50.0), Some(50.0), Some(50.0)],
            vec![Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
            vec![Some(100.0), Some(100.0), Some(100.0), Some(100.0)],
            vec![None, Some(-10.0), None, Some(-10.0)],
            vec![None, None, Some(200.0), Some(200.0)],
        );
        assert_eq!(
            out,
            vec![
                Some("50-60".to_string()),
                Some("0-100".to_string()),
                Some("0-100".to_string()),
                Some("0-100".to_string())
            ]
        );
        // All four rows are non-null — the key invariant for this test.
        for (i, label) in out.iter().enumerate() {
            assert!(label.is_some(), "row {i} unexpectedly null-propagated");
        }
    }

    // ── coerce_types ───────────────────────────────────────────────────────

    #[test]
    fn coerce_types_canonicalises_five_numeric_slots() {
        let udf = RangeBucketUdf::new();
        let out = udf
            .coerce_types(&[
                DataType::Int32,
                DataType::Float32,
                DataType::Int64,
                DataType::Float64,
                DataType::UInt32,
            ])
            .unwrap();
        assert_eq!(
            out,
            vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ]
        );
    }

    #[test]
    fn coerce_types_rejects_non_numeric() {
        let udf = RangeBucketUdf::new();
        assert!(udf
            .coerce_types(&[
                DataType::Utf8,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64
            ])
            .is_err());
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = RangeBucketUdf::new();
        assert!(udf
            .coerce_types(&[
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64
            ])
            .is_err());
    }

    #[test]
    fn return_type_enforces_five_arg_arity() {
        let udf = RangeBucketUdf::new();
        assert_eq!(
            udf.return_type(&[
                DataType::Float64,
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
