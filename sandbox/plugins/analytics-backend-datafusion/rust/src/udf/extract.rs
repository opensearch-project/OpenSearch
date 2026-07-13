/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `extract(unit, datetime)` — MySQL-style calendar component extractor (22 units incl. composites).
//! Composite units (e.g. DAY_MICROSECOND) join min-width-padded fields then parse as i64,
//! so leading zeros on the first field collapse (`0709` → `709`). WEEK is ISO, DOW is Mon=1..Sun=7.

use std::sync::Arc;

use super::udf_identity;

use chrono::{DateTime, Datelike, NaiveTime, TimeZone, Timelike, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int64Builder, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ExtractUdf::new()));
}

#[derive(Debug)]
pub struct ExtractUdf {
    signature: Signature,
}

impl ExtractUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(ExtractUdf, "opensearch_extract");

impl ScalarUDFImpl for ExtractUdf {
    fn name(&self) -> &str {
        "opensearch_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("extract expects 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Int64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!("extract expects 2 arguments, got {}", arg_types.len());
        }
        let unit = match &arg_types[0] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            other => return plan_err!("extract: arg 0 expected string unit, got {other:?}"),
        };
        let ts = match &arg_types[1] {
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            // Keep Utf8 so we can parse both ISO datetimes and bare time-of-day
            // strings internally (DataFusion's Utf8→Timestamp cast rejects the latter).
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            // Time32/Time64: handle directly to avoid a Time→Timestamp cast kernel.
            DataType::Time32(_) | DataType::Time64(_) => arg_types[1].clone(),
            other => {
                return plan_err!(
                    "extract: arg 1 expected timestamp/date/time/string, got {other:?}"
                )
            }
        };
        Ok(vec![unit, ts])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("extract expects 2 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        if let (ColumnarValue::Scalar(unit), ColumnarValue::Scalar(ts)) =
            (&args.args[0], &args.args[1])
        {
            let unit_str = scalar_utf8(unit)?;
            let micros = match ts {
                ScalarValue::TimestampMicrosecond(v, _) => *v,
                // Time-of-day → μs since 1970-01-01 so compute() reuses hour/minute/second.
                ScalarValue::Time32Second(v) => v.map(|s| (s as i64) * 1_000_000),
                ScalarValue::Time32Millisecond(v) => v.map(|ms| (ms as i64) * 1_000),
                ScalarValue::Time64Microsecond(v) => *v,
                ScalarValue::Time64Nanosecond(v) => v.map(|ns| ns / 1_000),
                ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => {
                    v.as_deref().and_then(parse_string_to_micros)
                }
                other => return exec_err!("extract: unsupported ts scalar: {other:?}"),
            };
            let out = match (unit_str, micros) {
                (Some(u), Some(m)) => compute(&u, m),
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Int64(out)));
        }

        let unit_arr = args.args[0].clone().into_array(n)?;
        let ts_arr = args.args[1].clone().into_array(n)?;
        let mut builder = Int64Builder::with_capacity(n);
        match ts_arr.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let ts = ts_arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| time_array_downcast_err(ts_arr.data_type()))?;
                fill_int_builder(&mut builder, &unit_arr, ts.len(), |i| {
                    if ts.is_null(i) { None } else { Some(ts.value(i)) }
                })?;
            }
            DataType::Time32(TimeUnit::Second) => {
                let arr = ts_arr.as_any().downcast_ref::<Time32SecondArray>()
                    .ok_or_else(|| time_array_downcast_err(ts_arr.data_type()))?;
                fill_int_builder(&mut builder, &unit_arr, arr.len(), |i| {
                    if arr.is_null(i) { None } else { Some((arr.value(i) as i64) * 1_000_000) }
                })?;
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                let arr = ts_arr.as_any().downcast_ref::<Time32MillisecondArray>()
                    .ok_or_else(|| time_array_downcast_err(ts_arr.data_type()))?;
                fill_int_builder(&mut builder, &unit_arr, arr.len(), |i| {
                    if arr.is_null(i) { None } else { Some((arr.value(i) as i64) * 1_000) }
                })?;
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let arr = ts_arr.as_any().downcast_ref::<Time64MicrosecondArray>()
                    .ok_or_else(|| time_array_downcast_err(ts_arr.data_type()))?;
                fill_int_builder(&mut builder, &unit_arr, arr.len(), |i| {
                    if arr.is_null(i) { None } else { Some(arr.value(i)) }
                })?;
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let arr = ts_arr.as_any().downcast_ref::<Time64NanosecondArray>()
                    .ok_or_else(|| time_array_downcast_err(ts_arr.data_type()))?;
                fill_int_builder(&mut builder, &unit_arr, arr.len(), |i| {
                    if arr.is_null(i) { None } else { Some(arr.value(i) / 1_000) }
                })?;
            }
            DataType::Utf8 => {
                let arr = ts_arr.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| time_array_downcast_err(ts_arr.data_type()))?;
                fill_int_builder(&mut builder, &unit_arr, arr.len(), |i| {
                    if arr.is_null(i) { None } else { parse_string_to_micros(arr.value(i)) }
                })?;
            }
            other => {
                return exec_err!(
                    "extract: expected Timestamp(Microsecond, None), Time32/Time64, or Utf8 after coercion, got {other:?}"
                )
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn time_array_downcast_err(dt: &DataType) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Execution(format!(
        "extract: array downcast failed for type {dt:?}"
    ))
}

/// Parses an ISO datetime or bare time-of-day string into μs since Unix epoch (UTC).
/// Time-of-day is anchored at 1970-01-01. Returns None on parse failure.
fn parse_string_to_micros(s: &str) -> Option<i64> {
    let trimmed = s.trim();
    for fmt in &[
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Some(naive.and_utc().timestamp_micros());
        }
        // %Y-%m-%d alone
        if *fmt == "%Y-%m-%d" {
            if let Ok(date) = chrono::NaiveDate::parse_from_str(trimmed, fmt) {
                return Some(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros());
            }
        }
    }
    // Bare time-of-day → attach 1970-01-01.
    for fmt in &["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"] {
        if let Ok(time) = NaiveTime::parse_from_str(trimmed, fmt) {
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
            return Some(date.and_time(time).and_utc().timestamp_micros());
        }
    }
    None
}

/// Fills the int64 builder by reading per-row μs values via `value_at` and running
/// them through `compute`. Time-of-day inputs are anchored at 1970-01-01.
fn fill_int_builder<F>(
    builder: &mut Int64Builder,
    unit_arr: &ArrayRef,
    n: usize,
    value_at: F,
) -> Result<()>
where
    F: Fn(usize) -> Option<i64>,
{
    for i in 0..n {
        match value_at(i) {
            None => builder.append_null(),
            Some(v) => match unit_at(unit_arr, i)? {
                None => builder.append_null(),
                Some(u) => match compute(&u, v) {
                    Some(out) => builder.append_value(out),
                    None => builder.append_null(),
                },
            },
        }
    }
    Ok(())
}

fn scalar_utf8(s: &ScalarValue) -> Result<Option<String>> {
    match s {
        ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) => Ok(opt.clone()),
        other => exec_err!("extract: unit must be string, got {other:?}"),
    }
}

fn unit_at(array: &ArrayRef, row: usize) -> Result<Option<String>> {
    let (is_null, value) = match array.data_type() {
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            (a.is_null(row), a.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            (a.is_null(row), a.value(row).to_string())
        }
        other => return exec_err!("extract: expected string unit array, got {other:?}"),
    };
    Ok(if is_null { None } else { Some(value) })
}

fn compute(unit: &str, micros: i64) -> Option<i64> {
    let seconds = micros.div_euclid(1_000_000);
    let micro_fraction = micros.rem_euclid(1_000_000) as u32;
    let dt = Utc
        .timestamp_opt(seconds, micro_fraction * 1_000)
        .single()?;
    extract_for_unit(&unit.to_ascii_uppercase(), dt)
}

/// Unknown unit → None (PPL throws; we surface null to avoid aborting the whole query).
fn extract_for_unit(unit: &str, dt: DateTime<Utc>) -> Option<i64> {
    let us = (dt.nanosecond() / 1_000) as i64;
    let (ss, mm, hh) = (dt.second() as i64, dt.minute() as i64, dt.hour() as i64);
    let (dd, mo, yy) = (dt.day() as i64, dt.month() as i64, dt.year() as i64);
    match unit {
        "MICROSECOND" => Some(us),
        // Millisecond fraction within the second (0..=999), matching SQL plugin semantics.
        "MILLISECOND" => Some(us / 1_000),
        "SECOND" => Some(ss),
        "MINUTE" => Some(mm),
        "HOUR" => Some(hh),
        "DAY" => Some(dd),
        "WEEK" => Some(dt.iso_week().week() as i64),
        "MONTH" => Some(mo),
        "QUARTER" => Some(((mo - 1) / 3 + 1) as i64),
        "YEAR" => Some(yy),
        "DOW" => Some(dt.weekday().number_from_monday() as i64),
        "DOY" => Some(dt.ordinal() as i64),
        "SECOND_MICROSECOND" => concat(&[(ss, 2), (us, 6)]),
        "MINUTE_MICROSECOND" => concat(&[(mm, 2), (ss, 2), (us, 6)]),
        "MINUTE_SECOND" => concat(&[(mm, 2), (ss, 2)]),
        "HOUR_MICROSECOND" => concat(&[(hh, 2), (mm, 2), (ss, 2), (us, 6)]),
        "HOUR_SECOND" => concat(&[(hh, 2), (mm, 2), (ss, 2)]),
        "HOUR_MINUTE" => concat(&[(hh, 2), (mm, 2)]),
        "DAY_MICROSECOND" => concat(&[(dd, 2), (hh, 2), (mm, 2), (ss, 2), (us, 6)]),
        "DAY_SECOND" => concat(&[(dd, 2), (hh, 2), (mm, 2), (ss, 2)]),
        "DAY_MINUTE" => concat(&[(dd, 2), (hh, 2), (mm, 2)]),
        "DAY_HOUR" => concat(&[(dd, 2), (hh, 2)]),
        "YEAR_MONTH" => concat(&[(yy, 4), (mo, 2)]),
        _ => None,
    }
}

/// Integer-math equivalent of Java's `parseLong(format(dt))`: acc = acc*10^w + v.
fn concat(parts: &[(i64, u32)]) -> Option<i64> {
    let mut acc: i64 = 0;
    for &(v, w) in parts {
        if v < 0 {
            return None;
        }
        let pow = 10_i64.checked_pow(w)?;
        acc = acc.checked_mul(pow)?.checked_add(v)?;
    }
    Some(acc)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2020-03-15 10:30:45.123456 UTC in micros since epoch.
    const SAMPLE_MICROS: i64 = 1_584_268_245_123_456;

    fn eval(unit: &str) -> Option<i64> {
        compute(unit, SAMPLE_MICROS)
    }

    fn us(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
            .unwrap()
            .timestamp()
            * 1_000_000
    }

    #[test]
    fn simple_and_composite_units_on_reference_sample() {
        // Reference sample (Sunday, 2020 leap year, ISO week 11):
        for (unit, want) in [
            ("MICROSECOND", 123_456_i64),
            ("SECOND", 45),
            ("MINUTE", 30),
            ("HOUR", 10),
            ("DAY", 15),
            ("MONTH", 3),
            ("QUARTER", 1),
            ("YEAR", 2020),
            ("DOY", 75),
            ("WEEK", 11),
            ("DOW", 7), // 2020-03-15 is a Sunday (ISO DOW=7)
            ("DAY_MICROSECOND", 15_103_045_123_456),
            ("DAY_SECOND", 15_103_045),
            ("DAY_MINUTE", 151_030),
            ("DAY_HOUR", 1510),
            ("HOUR_MICROSECOND", 103_045_123_456),
            ("HOUR_SECOND", 103_045),
            ("HOUR_MINUTE", 1030),
            ("MINUTE_MICROSECOND", 3_045_123_456),
            ("MINUTE_SECOND", 3045),
            ("SECOND_MICROSECOND", 45_123_456),
            ("YEAR_MONTH", 202_003),
        ] {
            assert_eq!(eval(unit), Some(want), "unit={unit}");
        }
    }

    #[test]
    fn millisecond_returns_fraction_within_second() {
        // Sample is 2020-03-15 10:30:45.123456 → ms fraction 123, μs fraction 123_456.
        assert_eq!(eval("MILLISECOND"), Some(123));
        assert_eq!(eval("millisecond"), Some(123));
        // Boundary: a timestamp with no sub-second component → MILLISECOND = 0.
        assert_eq!(compute("MILLISECOND", us(2024, 6, 15, 10, 30, 0)), Some(0));
    }

    #[test]
    fn unit_name_is_case_insensitive() {
        assert_eq!(eval("year"), Some(2020));
        assert_eq!(eval("dAy_HoUr"), Some(1510));
    }

    #[test]
    fn dow_monday_and_quarter_edges() {
        // Bump sample by one day → 2020-03-16 Monday (DOW=1).
        assert_eq!(compute("DOW", SAMPLE_MICROS + 86_400 * 1_000_000), Some(1));
        assert_eq!(compute("QUARTER", us(2020, 1, 1, 0, 0, 0)), Some(1));
        assert_eq!(compute("QUARTER", us(2020, 4, 1, 0, 0, 0)), Some(2));
        assert_eq!(compute("QUARTER", us(2020, 12, 31, 23, 59, 59)), Some(4));
    }

    #[test]
    fn leading_zero_on_first_field_collapses() {
        // 2020-01-07 09:05:02 → DAY_HOUR "0709"→709, HOUR_MINUTE "0905"→905.
        let m = us(2020, 1, 7, 9, 5, 2);
        assert_eq!(compute("DAY_HOUR", m), Some(709));
        assert_eq!(compute("HOUR_MINUTE", m), Some(905));
    }

    #[test]
    fn unknown_unit_yields_null() {
        assert_eq!(eval("NANOSECOND"), None);
        assert_eq!(eval(""), None);
    }
}
