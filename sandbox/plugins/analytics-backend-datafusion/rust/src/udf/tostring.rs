/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`tostring(value, format)`](https://docs.opensearch.org/latest/sql-and-ppl/ppl/functions/conversion/#tostring)
//! format modes in one UDF.
//!
//! Mirror's PPL's
//! [`ToStringFunction`](https://github.com/opensearch-project/sql/blob/main/core/src/main/java/org/opensearch/sql/expression/function/udf/ToStringFunction.java)
//!
//! Supported format values:
//! * `"binary"` — number → base-2 string of its integer part. Negative values use the
//!   signed-magnitude representation `-1xxxx` (matches `BigInteger.toString(2)`).
//! * `"hex"` — number → lowercase hexadecimal of its integer part. Negative values use
//!   the signed-magnitude representation `-xx` (matches `BigInteger.toString(16)`).
//! * `"commas"` — number with comma grouping; rounded to 2 decimals when the value has
//!   a fractional component, otherwise no decimals.
//! * `"duration"` — integer seconds → `HH:MM:SS` **wall-clock** rendering.
//! * `"duration_millis"` — integer milliseconds → `HH:MM:SS` wall-clock rendering.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Float64Type, Int64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ToStringUdf::new()));
}

/// `tostring(bigint, varchar)` / `tostring(fp64, varchar)` → varchar.
#[derive(Debug)]
pub struct ToStringUdf {
    signature: Signature,
}

impl ToStringUdf {
    pub fn new() -> Self {
        // The format arg accepts any string variant — the body dispatches via
        // StringArrayView so the producer's Utf8View doesn't get cast to Utf8
        // at the planner level.
        let exacts = [DataType::Int64, DataType::Float64]
            .into_iter()
            .flat_map(|value_ty| {
                [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View]
                    .into_iter()
                    .map(move |fmt_ty| TypeSignature::Exact(vec![value_ty.clone(), fmt_ty]))
            })
            .collect();
        Self {
            signature: Signature::one_of(exacts, Volatility::Immutable),
        }
    }
}

impl Default for ToStringUdf {
    fn default() -> Self {
        Self::new()
    }
}

// `ScalarUDFImpl` requires DynEq+DynHash. All `ToStringUdf` instances are functionally
// identical — there's no meaningful "parameterization" — so they compare equal and hash
// identically.
impl PartialEq for ToStringUdf {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ToStringUdf {}
impl Hash for ToStringUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "tostring".hash(state);
    }
}

impl ScalarUDFImpl for ToStringUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "tostring"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!(
                "tostring expects exactly 2 arguments (value, format), got {}",
                args.args.len()
            );
        }

        let format_col = &args.args[1];

        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(scalar_to_str(*value, format_col, 0, format_i64_as)?),
            )),
            ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(scalar_to_str(*value, format_col, 0, format_f64_as)?),
            )),
            ColumnarValue::Scalar(other) => {
                exec_err!("tostring: expected BIGINT or DOUBLE value, got {other:?}")
            }

            ColumnarValue::Array(arr) => match arr.data_type() {
                DataType::Int64 => {
                    let typed = arr.as_primitive::<Int64Type>();
                    let out: StringArray = (0..typed.len())
                        .map(|i| {
                            if typed.is_null(i) {
                                Ok(None)
                            } else {
                                scalar_to_str(Some(typed.value(i)), format_col, i, format_i64_as)
                            }
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
                }
                DataType::Float64 => {
                    let typed = arr.as_primitive::<Float64Type>();
                    let out: StringArray = (0..typed.len())
                        .map(|i| {
                            if typed.is_null(i) {
                                Ok(None)
                            } else {
                                scalar_to_str(Some(typed.value(i)), format_col, i, format_f64_as)
                            }
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
                }
                other => exec_err!("tostring: expected Int64 or Float64 value array, got {other:?}"),
            },
        }
    }
}

/// Per-row dispatcher for one concrete value type.
///
/// * `value` — the already-unwrapped optional; {@code None} short-circuits to `Ok(None)`
///   (null propagation).
/// * `format_col` — the format `ColumnarValue`. Resolved once per row via
///   [`format_at`]; the row index is only consulted when the caller passed an array.
/// * `row` — only used when `format_col` is an array; ignored for the scalar path.
/// * `formatter` — per-value-type rendering function (one for `i64`, one for `f64`).
fn scalar_to_str<T: Copy>(
    value: Option<T>,
    format_col: &ColumnarValue,
    row: usize,
    formatter: fn(T, &str) -> String,
) -> Result<Option<String>> {
    let Some(v) = value else {
        return Ok(None);
    };
    let format = format_at(format_col, row)?;
    match format {
        Some(f) => Ok(Some(formatter(v, f.as_str()))),
        None => Ok(None),
    }
}

/// Pulls the format string for `row` out of `format_col`. Returns `Ok(None)` for null
/// format cells.
fn format_at(format_col: &ColumnarValue, row: usize) -> Result<Option<String>> {
    match format_col {
        ColumnarValue::Scalar(
            ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) | ScalarValue::Utf8View(opt),
        ) => Ok(opt.clone()),
        ColumnarValue::Scalar(other) => {
            exec_err!("tostring: format must be VARCHAR, got {other:?}")
        }
        ColumnarValue::Array(arr) => {
            let view = super::json_common::StringArrayView::from_array(arr)
                .map_err(|e| datafusion::common::DataFusionError::Execution(format!("tostring: {e}")))?;
            Ok(view.cell(row).map(|s| s.to_string()))
        }
    }
}

/// Format modes, case-sensitive to match the SQL plugin's Java reference
/// (`ToStringFunction.DURATION_FORMAT`, etc.).
mod mode {
    pub const BINARY: &str = "binary";
    pub const HEX: &str = "hex";
    pub const COMMAS: &str = "commas";
    pub const DURATION: &str = "duration";
    pub const DURATION_MILLIS: &str = "duration_millis";
}

/// Render an `i64` value per the requested format. Unknown modes fall through to plain
/// decimal rendering.
fn format_i64_as(value: i64, format: &str) -> String {
    match format {
        mode::BINARY => format_binary_i64(value),
        mode::HEX => format_hex_i64(value),
        mode::COMMAS => format_commas_i64(value),
        mode::DURATION => format_duration_seconds(value),
        mode::DURATION_MILLIS => format_duration_seconds(value.div_euclid(1_000)),
        _ => value.to_string(),
    }
}

/// Render an `f64` value per the requested format. Mirrors the Java reference's strategy
/// of routing `binary` / `hex` / `duration*` through `BigDecimal.toBigInteger()` — i.e.
/// truncate toward zero, then apply the integer formatter.
fn format_f64_as(value: f64, format: &str) -> String {
    match format {
        mode::BINARY => format_binary_i64(truncate_to_i64(value)),
        mode::HEX => format_hex_i64(truncate_to_i64(value)),
        mode::COMMAS => format_commas_f64(value),
        mode::DURATION => format_duration_seconds(truncate_to_i64(value)),
        mode::DURATION_MILLIS => format_duration_seconds(truncate_to_i64(value).div_euclid(1_000)),
        _ => {
            if !value.is_finite() {
                return value.to_string();
            }
            // Drop a trailing `.0` so `tostring(42.0)` reads as `"42"`, matching
            // `BigDecimal.valueOf(42.0).toString() == "42.0"` → passed to `NumberFormat` with no
            // decimals would print `"42"`.
            let rendered = format!("{value}");
            rendered.strip_suffix(".0").map_or(rendered.clone(), |s| s.to_string())
        }
    }
}

fn truncate_to_i64(value: f64) -> i64 {
    if !value.is_finite() {
        return 0;
    }
    value as i64
}

fn format_binary_i64(v: i64) -> String {
    // Mirrors `BigInteger.toString(2)` for positive values; for negatives the Java
    // reference emits a leading '-' via BigInteger's signed radix representation. We match
    // that by formatting the absolute value and re-prepending the sign.
    if v < 0 {
        // Work in i128 so i64::MIN doesn't overflow on abs.
        format!("-{:b}", (v as i128).unsigned_abs())
    } else {
        format!("{:b}", v as u64)
    }
}

fn format_hex_i64(v: i64) -> String {
    // Same rationale as `format_binary_i64` — BigInteger.toString(16) on negatives prints
    // a leading '-'. Lowercase to match the Java reference (BigInteger uses lowercase).
    if v < 0 {
        format!("-{:x}", (v as i128).unsigned_abs())
    } else {
        format!("{:x}", v as u64)
    }
}

fn format_commas_i64(v: i64) -> String {
    let is_negative = v < 0;
    let abs_str = if is_negative {
        format!("{}", (v as i128).unsigned_abs())
    } else {
        v.to_string()
    };
    let mut out = String::with_capacity(abs_str.len() + abs_str.len() / 3 + 1);
    if is_negative {
        out.push('-');
    }
    insert_thousands_separators(&abs_str, &mut out);
    out
}

fn format_commas_f64(v: f64) -> String {
    // Non-finite fall through to native rendering (matches Double.toString for Infinity/NaN).
    if !v.is_finite() {
        return v.to_string();
    }
    let is_negative = v.is_sign_negative();
    // rounds the number to the nearest two decimal places.
    let rounded = format!("{:.2}", v.abs());
    let (whole, frac) = rounded
        .split_once('.')
        .expect("{:.2} always produces a decimal point");
    let mut out = String::with_capacity(rounded.len() + whole.len() / 3 + 2);
    if is_negative {
        out.push('-');
    }
    insert_thousands_separators(whole, &mut out);
    // Drop `.00` so integral values render without a decimal tail (`39225 → "39,225"`),
    // but keep a single-digit fractional (`.50`) when present
    if frac != "00" {
        out.push('.');
        // Trim a trailing '0' when exactly one digit would be meaningful, e.g. `.50 → .5`.
        if frac.ends_with('0') {
            out.push_str(&frac[..frac.len() - 1]);
        } else {
            out.push_str(frac);
        }
    }
    out
}

/// Allocation-free thousands-separator insertion. Appends `digits` to `out` with a `,`
/// after every 3 digits counted from the right.
fn insert_thousands_separators(digits: &str, out: &mut String) {
    let bytes = digits.as_bytes();
    let len = bytes.len();
    for (i, b) in bytes.iter().enumerate() {
        let from_right = len - i;
        out.push(*b as char);
        if from_right > 1 && (from_right - 1) % 3 == 0 {
            out.push(',');
        }
    }
}

/// Format a signed number of seconds to  wall-clock `HH:MM:SS` format
fn format_duration_seconds(total_seconds: i64) -> String {
    // Use `rem_euclid` to get the non-negative second-of-day.
    let second_of_day = total_seconds.rem_euclid(86_400);
    let h = second_of_day / 3600;
    let m = (second_of_day / 60) % 60;
    let s = second_of_day % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::datatypes::Field;

    fn udf() -> ToStringUdf {
        ToStringUdf::new()
    }

    fn invoke_scalar(
        value: ScalarValue,
        value_type: DataType,
        format: &str,
    ) -> Result<ColumnarValue> {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Utf8, true));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(value),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(format.to_string()))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("v", value_type, true)),
                Arc::new(Field::new("f", DataType::Utf8, true)),
            ],
            number_rows: 1,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        u.invoke_with_args(args)
    }

    fn invoke_array(value: ArrayRef, format: &str) -> Result<ColumnarValue> {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Utf8, true));
        let value_type = value.data_type().clone();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(value),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(format.to_string()))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("v", value_type, true)),
                Arc::new(Field::new("f", DataType::Utf8, true)),
            ],
            number_rows: 3,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        u.invoke_with_args(args)
    }

    fn utf8(v: ColumnarValue) -> String {
        match v {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s,
            other => panic!("expected Utf8 scalar, got {other:?}"),
        }
    }

    #[test]
    fn hex_matches_bigint_tohex() {
        let out = invoke_scalar(ScalarValue::Int64(Some(39225)), DataType::Int64, "hex").unwrap();
        assert_eq!(utf8(out), "9939");
    }

    #[test]
    fn binary_matches_biginteger_tostring_2() {
        let out = invoke_scalar(ScalarValue::Int64(Some(39225)), DataType::Int64, "binary").unwrap();
        assert_eq!(utf8(out), "1001100100111001");
    }

    #[test]
    fn binary_negative_uses_signed_biginteger_repr() {
        let out = invoke_scalar(ScalarValue::Int64(Some(-5)), DataType::Int64, "binary").unwrap();
        assert_eq!(utf8(out), "-101");
    }

    #[test]
    fn commas_integer_doc_example() {
        let out = invoke_scalar(ScalarValue::Int64(Some(39225)), DataType::Int64, "commas").unwrap();
        assert_eq!(utf8(out), "39,225");
    }

    #[test]
    fn commas_float_rounds_to_two_decimals() {
        let out = invoke_scalar(
            ScalarValue::Float64(Some(1234.5678)),
            DataType::Float64,
            "commas",
        )
        .unwrap();
        assert_eq!(utf8(out), "1,234.57");
    }

    #[test]
    fn commas_float_drops_trailing_double_zero() {
        let out = invoke_scalar(
            ScalarValue::Float64(Some(39225.0)),
            DataType::Float64,
            "commas",
        )
        .unwrap();
        assert_eq!(utf8(out), "39,225");
    }

    #[test]
    fn duration_seconds_doc_example() {
        let out = invoke_scalar(ScalarValue::Int64(Some(6500)), DataType::Int64, "duration").unwrap();
        assert_eq!(utf8(out), "01:48:20");
    }

    #[test]
    fn duration_bigdecimal_positive_example() {
        let out = invoke_scalar(ScalarValue::Int64(Some(3661)), DataType::Int64, "duration").unwrap();
        assert_eq!(utf8(out), "01:01:01");
    }

    #[test]
    fn duration_negative_wraps_to_pre_epoch_clock_time() {
        let out = invoke_scalar(
            ScalarValue::Float64(Some(-3661.4)),
            DataType::Float64,
            "duration",
        )
        .unwrap();
        assert_eq!(utf8(out), "22:58:59");
    }

    #[test]
    fn duration_wraps_modulo_24h() {
        let day = invoke_scalar(
            ScalarValue::Int64(Some(86_400)),
            DataType::Int64,
            "duration",
        )
        .unwrap();
        assert_eq!(utf8(day), "00:00:00");

        let day_plus_hour = invoke_scalar(
            ScalarValue::Int64(Some(86_400 + 3_600)),
            DataType::Int64,
            "duration",
        )
        .unwrap();
        assert_eq!(utf8(day_plus_hour), "01:00:00");
    }

    #[test]
    fn duration_millis_truncates_subseconds() {
        let out = invoke_scalar(
            ScalarValue::Int64(Some(6_500_999)),
            DataType::Int64,
            "duration_millis",
        )
        .unwrap();
        assert_eq!(utf8(out), "01:48:20");
    }

    #[test]
    fn duration_millis_negative_wraps_via_floor_div() {
        let out = invoke_scalar(
            ScalarValue::Int64(Some(-3_661_000)),
            DataType::Int64,
            "duration_millis",
        )
        .unwrap();
        assert_eq!(utf8(out), "22:58:59");
    }

    #[test]
    fn unknown_format_falls_through_to_plain_decimal() {
        let out = invoke_scalar(ScalarValue::Int64(Some(42)), DataType::Int64, "xyzzy").unwrap();
        assert_eq!(utf8(out), "42");
    }

    #[test]
    fn null_value_yields_null() {
        let out = invoke_scalar(ScalarValue::Int64(None), DataType::Int64, "hex").unwrap();
        match out {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("expected Utf8(None), got {other:?}"),
        }
    }

    #[test]
    fn null_format_yields_null() {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Utf8, true));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(42))),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            arg_fields: vec![
                Arc::new(Field::new("v", DataType::Int64, true)),
                Arc::new(Field::new("f", DataType::Utf8, true)),
            ],
            number_rows: 1,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        let out = u.invoke_with_args(args).unwrap();
        match out {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("expected Utf8(None), got {other:?}"),
        }
    }

    #[test]
    fn array_int_hex_with_scalar_format() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(15), None, Some(255)]));
        let out = invoke_array(array, "hex").unwrap();
        match out {
            ColumnarValue::Array(arr) => {
                let s = arr.as_string::<i32>();
                assert_eq!(s.value(0), "f");
                assert!(s.is_null(1));
                assert_eq!(s.value(2), "ff");
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn array_float_commas_with_scalar_format() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![Some(1234.5), None, Some(0.0)]));
        let out = invoke_array(array, "commas").unwrap();
        match out {
            ColumnarValue::Array(arr) => {
                let s = arr.as_string::<i32>();
                assert_eq!(s.value(0), "1,234.5");
                assert!(s.is_null(1));
                assert_eq!(s.value(2), "0");
            }
            other => panic!("expected array, got {other:?}"),
        }
    }
}
