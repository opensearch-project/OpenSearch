/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `width_bucket(value, num_bins, data_range, max_value)` — histogram bucket label.
//!
//! Ports the OpenSearch-SQL bespoke `WIDTH_BUCKET` UDF (see
//! `sql/core/.../binning/WidthBucketFunction.java`). Given a numeric value,
//! requested bin count, total data range, and dataset max, returns the
//! half-open bucket `[binStart, binEnd)` that contains `value` as a VARCHAR
//! label (e.g. `"30000-40000"`).
//!
//! <b>Not</b> ISO-SQL `WIDTH_BUCKET(value, min, max, count) → INT`. The PPL
//! variant returns a VARCHAR bucket label via a nice-number magnitude-based
//! algorithm. The two collide in name only; any future wiring of real ISO SQL
//! width_bucket (DataFusion has it natively) needs a distinct enum entry.
//!
//! Semantics:
//! * `numBins` outside `[MIN_BINS, MAX_BINS]` → null
//! * `range <= 0` or non-finite → null
//! * Optimal width = `10^ceil(log10(range/numBins))`; bumped one magnitude
//!   higher if `ceil(range/width) + (maxValue % width == 0 ? 1 : 0)` would
//!   exceed `numBins`. Label uses the same integer/float decimal-place rules
//!   as `span_bucket`.
//! * Any null input → null (null propagation).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{coerce_args, CoerceMode};

/// Mirror `org.opensearch.sql.calcite.utils.binning.BinConstants.MIN_BINS`.
const MIN_BINS: i64 = 2;
/// Mirror `BinConstants.MAX_BINS`.
const MAX_BINS: i64 = 50_000;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(WidthBucketUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct WidthBucketUdf {
    signature: Signature,
}

impl WidthBucketUdf {
    pub fn new() -> Self {
        // coerce_types normalizes slot 1 (num_bins) to Int64 and slots 0/2/3
        // (numeric value, range, max) to Float64 — same pattern as
        // SpanBucketUdf. invoke_with_args downcasts once per slot.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for WidthBucketUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for WidthBucketUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "width_bucket"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 4 {
            return plan_err!("width_bucket expects 4 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "width_bucket",
            arg_types,
            &[
                CoerceMode::Float64,
                CoerceMode::Int64,
                CoerceMode::Float64,
                CoerceMode::Float64,
            ],
        )
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 4 {
            return plan_err!("width_bucket expects 4 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;
        let value = args.args[0].clone().into_array(n)?;
        let num_bins = args.args[1].clone().into_array(n)?;
        let range = args.args[2].clone().into_array(n)?;
        let max_value = args.args[3].clone().into_array(n)?;

        let value = downcast_f64(&value, "value")?;
        let num_bins = downcast_i64(&num_bins, "num_bins")?;
        let range = downcast_f64(&range, "data_range")?;
        let max_value = downcast_f64(&max_value, "max_value")?;

        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            if value.is_null(i) || num_bins.is_null(i) || range.is_null(i) || max_value.is_null(i) {
                builder.append_null();
                continue;
            }
            match calculate(value.value(i), num_bins.value(i), range.value(i), max_value.value(i)) {
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
            "width_bucket: slot '{slot}' expected Float64 post-coerce, got {:?}",
            arr.data_type()
        ))
    })
}

fn downcast_i64<'a>(arr: &'a ArrayRef, slot: &str) -> Result<&'a Int64Array> {
    arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "width_bucket: slot '{slot}' expected Int64 post-coerce, got {:?}",
            arr.data_type()
        ))
    })
}

/// Core width-bucket math. Direct port of Java
/// `WidthBucketFunction.calculateWidthBucket`.
fn calculate(value: f64, num_bins: i64, range: f64, max_value: f64) -> Option<String> {
    if !(MIN_BINS..=MAX_BINS).contains(&num_bins) {
        return None;
    }
    if !range.is_finite() || range <= 0.0 {
        return None;
    }
    let width = optimal_width(range, max_value, num_bins);
    if !width.is_finite() || width <= 0.0 {
        return None;
    }
    let bin_start = (value / width).floor() * width;
    let bin_end = bin_start + width;
    Some(format_range(bin_start, bin_end, width))
}

/// Mirrors `WidthBucketFunction.calculateOptimalWidth` — the Java reference
/// admits an explicit fallback of `1.0` when `range <= 0` or `bins <= 0`;
/// we can't hit those from `calculate` (already guarded), but we keep the
/// fallback so this function's contract matches Java's exactly and anyone
/// reusing it directly gets identical behavior.
fn optimal_width(data_range: f64, max_value: f64, requested_bins: i64) -> f64 {
    if data_range <= 0.0 || requested_bins <= 0 {
        return 1.0;
    }
    let target_width = data_range / requested_bins as f64;
    let exponent = target_width.log10().ceil();
    let mut width = 10f64.powf(exponent);
    let mut actual_bins = (data_range / width).ceil();
    // Java: `if (maxValue % optimalWidth == 0) actualBins++;`
    if max_value % width == 0.0 {
        actual_bins += 1.0;
    }
    if actual_bins > requested_bins as f64 {
        width = 10f64.powf(exponent + 1.0);
    }
    width
}

// Formatting helpers — duplicated from span_bucket rather than moved to a
// shared module. The PPL binning family is expected to converge over time
// (range_bucket and minspan_bucket both reuse this exact helper), but keeping
// each UDF self-contained makes it trivial to later give one of them a
// distinct format policy without untangling a shared dependency.
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
    //! `WidthBucketFunction.calculateWidthBucket`. Any drift from Java is a
    //! bug in this port, not the test. Where the algorithm's intermediate
    //! steps matter (optimal-width rescale branch), we pin `optimal_width`
    //! directly so the test names describe the invariant being exercised.
    use super::*;

    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::ScalarFunctionArgs;

    // ── Happy path: exact bank-fixture case from subtraitupdates's tests ───

    #[test]
    fn bank_fixture_width_10000() {
        // range=33539, bins=10 → targetWidth=3353.9 → exponent=ceil(log10(...))=4
        //   → width=10000. actualBins=ceil(33539/10000)=4. 33539 % 10000 = 3539 ≠ 0,
        //   no bump. 4 > 10 false → width stays 10000.
        // value=39225 → binStart=floor(39225/10000)*10000=30000, binEnd=40000.
        assert_eq!(
            calculate(39225.0, 10, 33539.0, 33539.0),
            Some("30000-40000".to_string())
        );
    }

    // ── Boundary of numBins ────────────────────────────────────────────────

    #[test]
    fn num_bins_below_minimum_returns_null() {
        // MIN_BINS = 2.
        assert_eq!(calculate(5.0, 1, 100.0, 100.0), None);
        assert_eq!(calculate(5.0, 0, 100.0, 100.0), None);
        assert_eq!(calculate(5.0, -1, 100.0, 100.0), None);
    }

    #[test]
    fn num_bins_above_maximum_returns_null() {
        // MAX_BINS = 50_000.
        assert_eq!(calculate(5.0, 50_001, 100.0, 100.0), None);
        assert_eq!(calculate(5.0, i64::MAX, 100.0, 100.0), None);
    }

    #[test]
    fn num_bins_at_boundary_accepted() {
        assert!(calculate(5.0, MIN_BINS, 100.0, 100.0).is_some());
        assert!(calculate(5.0, MAX_BINS, 100.0, 100.0).is_some());
    }

    // ── Range guards ───────────────────────────────────────────────────────

    #[test]
    fn zero_range_returns_null() {
        assert_eq!(calculate(5.0, 10, 0.0, 100.0), None);
    }

    #[test]
    fn negative_range_returns_null() {
        assert_eq!(calculate(5.0, 10, -1.0, 100.0), None);
    }

    #[test]
    fn non_finite_range_returns_null() {
        assert_eq!(calculate(5.0, 10, f64::NAN, 100.0), None);
        assert_eq!(calculate(5.0, 10, f64::INFINITY, 100.0), None);
        assert_eq!(calculate(5.0, 10, f64::NEG_INFINITY, 100.0), None);
    }

    // ── Optimal-width rescale branch ───────────────────────────────────────

    #[test]
    fn optimal_width_rescales_when_max_on_boundary_exceeds_bins() {
        // range=50, bins=5, max=50 → targetWidth=10 → exponent=1 → width=10.
        // ceil(50/10)=5. 50 % 10 == 0 → actualBins=6. 6 > 5 → rescale to 10^2=100.
        assert_eq!(optimal_width(50.0, 50.0, 5), 100.0);
    }

    #[test]
    fn optimal_width_no_rescale_when_max_offboundary() {
        // range=99, bins=10, max=99 → targetWidth=9.9 → exponent=1 → width=10.
        // ceil(99/10)=10. 99 % 10 = 9 ≠ 0, no bump. 10 > 10 false → width=10.
        assert_eq!(optimal_width(99.0, 99.0, 10), 10.0);
    }

    #[test]
    fn optimal_width_target_under_one_yields_unit_width() {
        // range=1, bins=10 → targetWidth=0.1 → exponent=ceil(log10(0.1))=-1 → width=0.1.
        // ceil(1/0.1)=10. 1 % 0.1 is fp-ugly (~0.1) ≠ 0, no bump. 10 > 10 false → width=0.1.
        let w = optimal_width(1.0, 1.0, 10);
        assert!((w - 0.1).abs() < 1e-12, "expected ~0.1, got {w}");
    }

    // ── Null inputs at the batch layer ─────────────────────────────────────
    // `calculate` takes primitives and can't observe a null; null propagation
    // happens in `invoke_with_args`. Batch test below covers it.

    // ── Batch invocation via DataFusion scalar harness ─────────────────────

    fn invoke_batch(
        values: Vec<Option<f64>>,
        num_bins: Vec<Option<i64>>,
        ranges: Vec<Option<f64>>,
        maxes: Vec<Option<f64>>,
    ) -> Vec<Option<String>> {
        let n = values.len();
        assert_eq!(n, num_bins.len());
        assert_eq!(n, ranges.len());
        assert_eq!(n, maxes.len());
        let udf = WidthBucketUdf::new();
        let value_arr = Arc::new(Float64Array::from(values)) as ArrayRef;
        let bins_arr = Arc::new(Int64Array::from(num_bins)) as ArrayRef;
        let range_arr = Arc::new(Float64Array::from(ranges)) as ArrayRef;
        let max_arr = Arc::new(Float64Array::from(maxes)) as ArrayRef;
        let return_field = Arc::new(Field::new("out", DataType::Utf8, true));
        let arg_fields = vec![
            Arc::new(Field::new("value", DataType::Float64, true)),
            Arc::new(Field::new("num_bins", DataType::Int64, true)),
            Arc::new(Field::new("data_range", DataType::Float64, true)),
            Arc::new(Field::new("max_value", DataType::Float64, true)),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(value_arr),
                ColumnarValue::Array(bins_arr),
                ColumnarValue::Array(range_arr),
                ColumnarValue::Array(max_arr),
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
        // Each row has exactly one null slot; all rows should produce null labels.
        let out = invoke_batch(
            vec![None, Some(5.0), Some(5.0), Some(5.0)],
            vec![Some(10), None, Some(10), Some(10)],
            vec![Some(100.0), Some(100.0), None, Some(100.0)],
            vec![Some(100.0), Some(100.0), Some(100.0), None],
        );
        assert_eq!(out, vec![None, None, None, None]);
    }

    #[test]
    fn batch_happy_path_non_null_rows() {
        let out = invoke_batch(
            vec![Some(39225.0)],
            vec![Some(10)],
            vec![Some(33539.0)],
            vec![Some(33539.0)],
        );
        assert_eq!(out, vec![Some("30000-40000".to_string())]);
    }

    // ── coerce_types ───────────────────────────────────────────────────────

    #[test]
    fn coerce_types_canonicalises_mixed_numeric_inputs() {
        let udf = WidthBucketUdf::new();
        let out = udf
            .coerce_types(&[
                DataType::Int32,
                DataType::Int32,
                DataType::Float32,
                DataType::Int64,
            ])
            .unwrap();
        assert_eq!(
            out,
            vec![
                DataType::Float64,
                DataType::Int64,
                DataType::Float64,
                DataType::Float64,
            ]
        );
    }

    #[test]
    fn coerce_types_rejects_string_in_value_slot() {
        let udf = WidthBucketUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Int64, DataType::Float64, DataType::Float64])
            .is_err());
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = WidthBucketUdf::new();
        assert!(udf.coerce_types(&[DataType::Int64, DataType::Int64, DataType::Int64]).is_err());
    }

    #[test]
    fn return_type_enforces_four_arg_arity() {
        let udf = WidthBucketUdf::new();
        assert_eq!(
            udf.return_type(&[
                DataType::Float64,
                DataType::Int64,
                DataType::Float64,
                DataType::Float64
            ])
            .unwrap(),
            DataType::Utf8
        );
        assert!(udf.return_type(&[DataType::Float64]).is_err());
    }
}
