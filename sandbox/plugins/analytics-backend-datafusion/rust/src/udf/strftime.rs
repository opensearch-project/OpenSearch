/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `strftime(value, format)` — POSIX-style datetime formatter mirroring PPL `StrftimeFunction`.

use std::any::Any;
use std::sync::Arc;

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::udf_identity;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(StrftimeUdf::new()));
}

/// SQL plugin's `StrftimeFunction.MAX_UNIX_TIMESTAMP` — values beyond this yield null.
const MAX_UNIX_SECONDS: i64 = 32_536_771_199;
/// Numeric inputs with `abs(v) >= 1e11` are auto-detected as milliseconds
/// (divided by 1000), matching the SQL plugin's behavior.
const MILLIS_HEURISTIC_THRESHOLD: f64 = 1e11;

#[derive(Debug)]
pub struct StrftimeUdf {
    signature: Signature,
}

impl StrftimeUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(StrftimeUdf, "strftime");

impl ScalarUDFImpl for StrftimeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "strftime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("strftime expects 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!("strftime expects 2 arguments, got {}", arg_types.len());
        }
        let value = match &arg_types[0] {
            t if t.is_numeric() => DataType::Float64,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Float64,
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            other => return plan_err!("strftime: arg 0 expected numeric/timestamp/date/string, got {other:?}"),
        };
        let format = match &arg_types[1] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            other => return plan_err!("strftime: arg 1 expected string, got {other:?}"),
        };
        Ok(vec![value, format])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("strftime expects 2 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        if let (ColumnarValue::Scalar(val), ColumnarValue::Scalar(fmt)) =
            (&args.args[0], &args.args[1])
        {
            let format = scalar_utf8(fmt)?;
            let rendered = match val {
                ScalarValue::Float64(v) => {
                    v.and_then(|v| render_from_seconds(v, format.as_deref()))
                }
                ScalarValue::TimestampMicrosecond(v, _) => {
                    v.and_then(|v| render_from_micros(v, format.as_deref()))
                }
                other => {
                    return exec_err!(
                        "strftime: unsupported scalar value type after coercion: {other:?}"
                    )
                }
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(rendered)));
        }

        let value_array = args.args[0].clone().into_array(n)?;
        let format_array = args.args[1].clone().into_array(n)?;
        let out: StringArray = match value_array.data_type() {
            DataType::Float64 => {
                let values = value_array.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
                (0..n)
                    .map(|i| if values.is_null(i) { None } else {
                        match format_at(&format_array, i) {
                            Ok(Some(f)) => render_from_seconds(values.value(i), Some(&f)),
                            _ => None,
                        }
                    })
                    .collect()
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let values = value_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("coerce_types canonicalizes to TimestampMicrosecond");
                (0..n)
                    .map(|i| if values.is_null(i) { None } else {
                        match format_at(&format_array, i) {
                            Ok(Some(f)) => render_from_micros(values.value(i), Some(&f)),
                            _ => None,
                        }
                    })
                    .collect()
            }
            other => return exec_err!("strftime: unsupported value array type after coercion: {other:?}"),
        };
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

fn scalar_utf8(s: &ScalarValue) -> Result<Option<String>> {
    match s {
        ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) => Ok(opt.clone()),
        other => exec_err!("strftime: format must be VARCHAR, got {other:?}"),
    }
}

fn format_at(array: &ArrayRef, row: usize) -> Result<Option<String>> {
    let (is_null, value) = match array.data_type() {
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            (a.is_null(row), a.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            (a.is_null(row), a.value(row).to_string())
        }
        other => return exec_err!("strftime: expected string format array, got {other:?}"),
    };
    Ok(if is_null { None } else { Some(value) })
}

fn render_from_seconds(value: f64, format: Option<&str>) -> Option<String> {
    let format = format?;
    if !value.is_finite() {
        return None;
    }
    // Numeric inputs with `abs(v) >= 1e11` are auto-detected as milliseconds,
    // matching the SQL plugin's `StrftimeFormatterUtil` threshold.
    let seconds_value = if value.abs() >= MILLIS_HEURISTIC_THRESHOLD {
        value / 1000.0
    } else {
        value
    };
    let seconds = seconds_value.trunc() as i64;
    if !(-MAX_UNIX_SECONDS..=MAX_UNIX_SECONDS).contains(&seconds) {
        return None;
    }
    let fraction = seconds_value - seconds as f64;
    let nanos = (fraction * 1_000_000_000.0) as i32;
    // `Instant.ofEpochSecond(s, n)` normalizes negative n; chrono requires
    // non-negative nanos paired with an adjusted seconds value.
    let (adj_seconds, adj_nanos) = if nanos < 0 {
        (seconds - 1, (nanos + 1_000_000_000) as u32)
    } else {
        (seconds, nanos as u32)
    };
    let dt = Utc.timestamp_opt(adj_seconds, adj_nanos).single()?;
    Some(format_with_directives(dt, format))
}

fn render_from_micros(value: i64, format: Option<&str>) -> Option<String> {
    let format = format?;
    let seconds = value.div_euclid(1_000_000);
    let micros_remainder = value.rem_euclid(1_000_000) as u32;
    if !(-MAX_UNIX_SECONDS..=MAX_UNIX_SECONDS).contains(&seconds) {
        return None;
    }
    let dt = Utc
        .timestamp_opt(seconds, micros_remainder * 1_000)
        .single()?;
    Some(format_with_directives(dt, format))
}

// Directive engine — mirrors `StrftimeFormatterUtil`'s handler table. Unknown
// directives fall through as literal text (matches the SQL plugin).
fn format_with_directives(dt: DateTime<Utc>, format: &str) -> String {
    let bytes = format.as_bytes();
    let mut out = String::with_capacity(format.len() + 16);
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'%' {
            out.push(bytes[i] as char);
            i += 1;
            continue;
        }
        // Lookahead: at least one char after `%`.
        if i + 1 >= bytes.len() {
            out.push('%');
            break;
        }

        // %:z / %::z / %:::z
        if bytes[i + 1] == b':' {
            let mut colons = 1;
            while i + 1 + colons < bytes.len() && bytes[i + 1 + colons] == b':' {
                colons += 1;
            }
            if i + 1 + colons < bytes.len() && bytes[i + 1 + colons] == b'z' {
                append_tz_colon(&mut out, colons);
                i += 2 + colons;
                continue;
            }
            out.push('%');
            i += 1;
            continue;
        }

        // %Ez (extended tz offset in minutes)
        if bytes[i + 1] == b'E' && i + 2 < bytes.len() && bytes[i + 2] == b'z' {
            append_tz_offset_minutes(&mut out, dt);
            i += 3;
            continue;
        }

        // %<digit>N / %<digit>Q with optional precision
        if bytes[i + 1].is_ascii_digit()
            && i + 2 < bytes.len()
            && (bytes[i + 2] == b'N' || bytes[i + 2] == b'Q')
        {
            let precision = (bytes[i + 1] - b'0') as usize;
            append_subsecond(&mut out, dt, bytes[i + 2], precision);
            i += 3;
            continue;
        }
        if bytes[i + 1] == b'N' || bytes[i + 1] == b'Q' {
            let kind = bytes[i + 1];
            // Default precision per StrftimeFormatterUtil: %N=9 nanos, %Q=3 millis.
            let precision = if kind == b'N' { 9 } else { 3 };
            append_subsecond(&mut out, dt, kind, precision);
            i += 2;
            continue;
        }

        let directive = bytes[i + 1];
        if append_simple_directive(&mut out, dt, directive) {
            i += 2;
        } else {
            out.push('%');
            out.push(directive as char);
            i += 2;
        }
    }
    out
}

fn append_simple_directive(out: &mut String, dt: DateTime<Utc>, directive: u8) -> bool {
    let h12 = {
        let h = dt.hour() % 12;
        if h == 0 { 12 } else { h }
    };
    match directive {
        b'%' => out.push('%'),
        b'c' => out.push_str(&format!("{} {} {:02} {:02}:{:02}:{:02} {:04}",
            weekday_short(dt.weekday()), month_short(dt.month()),
            dt.day(), dt.hour(), dt.minute(), dt.second(), dt.year())),
        b'+' => out.push_str(&format!("{} {} {:02} {:02}:{:02}:{:02} UTC {:04}",
            weekday_short(dt.weekday()), month_short(dt.month()),
            dt.day(), dt.hour(), dt.minute(), dt.second(), dt.year())),
        b'f' => out.push_str(&format!("{:06}", dt.nanosecond() / 1000)),
        b'H' => out.push_str(&format!("{:02}", dt.hour())),
        b'I' => out.push_str(&format!("{:02}", h12)),
        b'k' => out.push_str(&format!("{:2}", dt.hour())),
        b'M' => out.push_str(&format!("{:02}", dt.minute())),
        b'p' => out.push_str(if dt.hour() < 12 { "AM" } else { "PM" }),
        b'S' => out.push_str(&format!("{:02}", dt.second())),
        b's' => out.push_str(&dt.timestamp().to_string()),
        b'T' | b'X' => out.push_str(&format!("{:02}:{:02}:{:02}", dt.hour(), dt.minute(), dt.second())),
        b'Z' => out.push_str("UTC"),
        b'z' => out.push_str("+0000"),
        b'F' => out.push_str(&format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day())),
        b'x' => out.push_str(&format!("{:02}/{:02}/{:04}", dt.month(), dt.day(), dt.year())),
        b'A' => out.push_str(weekday_full(dt.weekday())),
        b'a' => out.push_str(weekday_short(dt.weekday())),
        b'w' => out.push_str(&weekday_numeric_sunday_zero(dt.weekday()).to_string()),
        b'd' => out.push_str(&format!("{:02}", dt.day())),
        b'e' => out.push_str(&format!("{:2}", dt.day())),
        b'j' => out.push_str(&format!("{:03}", dt.ordinal())),
        b'V' => out.push_str(&format!("{:02}", dt.iso_week().week())),
        b'U' => out.push_str(&format!("{:02}", week_of_year_sunday_start(dt))),
        b'b' => out.push_str(month_short(dt.month())),
        b'B' => out.push_str(month_full(dt.month())),
        b'm' => out.push_str(&format!("{:02}", dt.month())),
        b'C' => out.push_str(&format!("{:02}", dt.year() / 100)),
        b'g' => out.push_str(&format!("{:02}", dt.iso_week().year() % 100)),
        b'G' => out.push_str(&format!("{:04}", dt.iso_week().year())),
        b'y' => out.push_str(&format!("{:02}", dt.year() % 100)),
        b'Y' => out.push_str(&format!("{:04}", dt.year())),
        _ => return false,
    }
    true
}

fn append_tz_colon(out: &mut String, colons: usize) {
    match colons {
        1 => out.push_str("+00:00"),
        2 => out.push_str("+00:00:00"),
        3 => out.push_str("+0"),
        _ => out.push_str("+00:00"),
    }
}

fn append_tz_offset_minutes(out: &mut String, _dt: DateTime<Utc>) {
    out.push_str("+0");
}

fn append_subsecond(out: &mut String, dt: DateTime<Utc>, kind: u8, precision: usize) {
    let nanos = dt.nanosecond();
    match kind {
        b'N' => {
            let scaled = nanos as f64 / 1_000_000_000.0;
            let truncated = (scaled * 10f64.powi(precision as i32)) as u64;
            out.push_str(&format!("{:0>width$}", truncated, width = precision));
        }
        b'Q' => match precision {
            6 => out.push_str(&format!("{:06}", nanos / 1_000)),
            9 => out.push_str(&format!("{:09}", nanos)),
            _ => out.push_str(&format!("{:03}", nanos / 1_000_000)),
        },
        _ => {}
    }
}

fn weekday_short(w: Weekday) -> &'static str {
    match w {
        Weekday::Mon => "Mon", Weekday::Tue => "Tue", Weekday::Wed => "Wed",
        Weekday::Thu => "Thu", Weekday::Fri => "Fri", Weekday::Sat => "Sat", Weekday::Sun => "Sun",
    }
}

fn weekday_full(w: Weekday) -> &'static str {
    match w {
        Weekday::Mon => "Monday", Weekday::Tue => "Tuesday", Weekday::Wed => "Wednesday",
        Weekday::Thu => "Thursday", Weekday::Fri => "Friday",
        Weekday::Sat => "Saturday", Weekday::Sun => "Sunday",
    }
}

fn weekday_numeric_sunday_zero(w: Weekday) -> u32 {
    match w {
        Weekday::Sun => 0, Weekday::Mon => 1, Weekday::Tue => 2, Weekday::Wed => 3,
        Weekday::Thu => 4, Weekday::Fri => 5, Weekday::Sat => 6,
    }
}

fn month_short(m: u32) -> &'static str {
    ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        .get(m as usize).copied().unwrap_or("")
}

fn month_full(m: u32) -> &'static str {
    ["", "January", "February", "March", "April", "May", "June",
     "July", "August", "September", "October", "November", "December"]
        .get(m as usize).copied().unwrap_or("")
}

/// `%U` — week of year, Sunday-start; week 0 is the partial week before the
/// first Sunday of the year (matches `WeekFields.SUNDAY_START.weekOfYear() - 1`).
fn week_of_year_sunday_start(dt: DateTime<Utc>) -> u32 {
    let doy = dt.ordinal();
    let jan1 = NaiveDate::from_ymd_opt(dt.year(), 1, 1).expect("valid jan 1").weekday();
    let days_to_first_sunday = match jan1 {
        Weekday::Sun => 1, Weekday::Mon => 7, Weekday::Tue => 6, Weekday::Wed => 5,
        Weekday::Thu => 4, Weekday::Fri => 3, Weekday::Sat => 2,
    };
    if doy < days_to_first_sunday { 0 } else { (doy - days_to_first_sunday) / 7 + 1 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::datatypes::Field;

    fn udf() -> StrftimeUdf {
        StrftimeUdf::new()
    }

    fn invoke(args: Vec<ColumnarValue>, arg_types: &[DataType], n: usize) -> Result<ColumnarValue> {
        let u = udf();
        let arg_fields = arg_types
            .iter()
            .enumerate()
            .map(|(i, t)| Arc::new(Field::new(format!("a{i}"), t.clone(), true)))
            .collect();
        u.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: n,
            return_field: Arc::new(Field::new(u.name(), DataType::Utf8, true)),
            config_options: Arc::new(Default::default()),
        })
    }

    fn call_f64(v: f64, f: &str) -> Option<String> {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(f.to_string()))),
        ];
        utf8_scalar(invoke(args, &[DataType::Float64, DataType::Utf8], 1).unwrap())
    }

    fn call_ts_us(us: i64, f: &str) -> Option<String> {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(us), None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(f.to_string()))),
        ];
        utf8_scalar(
            invoke(
                args,
                &[DataType::Timestamp(TimeUnit::Microsecond, None), DataType::Utf8],
                1,
            )
            .unwrap(),
        )
    }

    fn utf8_scalar(v: ColumnarValue) -> Option<String> {
        match v {
            ColumnarValue::Scalar(ScalarValue::Utf8(opt)) => opt,
            other => panic!("expected Utf8 scalar, got {other:?}"),
        }
    }

    #[test]
    fn complex_format_matches_it() {
        assert_eq!(
            call_f64(1521467703.0, "%a, %b %d, %Y %I:%M:%S %p %Z").unwrap(),
            "Mon, Mar 19, 2018 01:55:03 PM UTC"
        );
    }

    #[test]
    fn integer_seconds_ymd_hms() {
        assert_eq!(call_f64(1521467703.0, "%Y-%m-%d %H:%M:%S").unwrap(), "2018-03-19 13:55:03");
    }

    #[test]
    fn fractional_seconds_with_3q_milliseconds() {
        assert_eq!(
            call_f64(1521467703.123456, "%Y-%m-%d %H:%M:%S.%3Q").unwrap(),
            "2018-03-19 13:55:03.123"
        );
    }

    #[test]
    fn negative_epoch_rendering() {
        assert_eq!(call_f64(-1.0, "%Y-%m-%d %H:%M:%S").unwrap(), "1969-12-31 23:59:59");
        assert_eq!(call_f64(-86400.0, "%Y-%m-%d").unwrap(), "1969-12-31");
        assert_eq!(call_f64(-31_536_000.0, "%Y-%m-%d").unwrap(), "1969-01-01");
        assert_eq!(call_f64(-946_771_200.0, "%Y-%m-%d").unwrap(), "1940-01-01");
    }

    #[test]
    fn timestamp_micros_renders_date_and_time() {
        assert_eq!(call_ts_us(1_521_467_703_000_000, "%F").unwrap(), "2018-03-19");
        assert_eq!(call_ts_us(1_521_467_703_000_000, "%H:%M:%S").unwrap(), "13:55:03");
    }

    #[test]
    fn ms_autodetect_above_1e11() {
        // `abs(v) >= 1e11` auto-treats input as ms; below stays in seconds.
        assert_eq!(
            call_f64(1_521_467_703_000.0, "%Y-%m-%d %H:%M:%S").unwrap(),
            "2018-03-19 13:55:03"
        );
        assert_eq!(call_f64(1_521_467_703.0, "%Y-%m-%d").unwrap(), "2018-03-19");
    }

    #[test]
    fn out_of_range_and_non_finite_yield_null() {
        // 5e10 is above MAX_UNIX_SECONDS (3.25e10) but below the 1e11 ms threshold.
        assert!(call_f64(5e10, "%Y-%m-%d").is_none());
        assert!(call_f64(f64::NAN, "%Y-%m-%d").is_none());
    }

    #[test]
    fn null_operand_yields_null() {
        for args in [
            vec![
                ColumnarValue::Scalar(ScalarValue::Float64(None)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("%Y".into()))),
            ],
            vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
        ] {
            let out = invoke(args, &[DataType::Float64, DataType::Utf8], 1).unwrap();
            assert!(utf8_scalar(out).is_none());
        }
    }

    #[test]
    fn weekday_directives() {
        // 2018-03-19 = Monday
        assert_eq!(call_f64(1521467703.0, "%A").unwrap(), "Monday");
        assert_eq!(call_f64(1521467703.0, "%a").unwrap(), "Mon");
        assert_eq!(call_f64(1521467703.0, "%w").unwrap(), "1");
    }

    #[test]
    fn twelve_hour_clock_and_ampm() {
        assert_eq!(call_f64(1521467703.0, "%I").unwrap(), "01");
        assert_eq!(call_f64(1521467703.0, "%p").unwrap(), "PM");
        assert_eq!(call_f64(1521417600.0, "%I").unwrap(), "12"); // midnight
    }

    #[test]
    fn week_ordinal_and_literal() {
        assert_eq!(call_f64(1521467703.0, "%V").unwrap(), "12");
        assert_eq!(call_f64(1521467703.0, "%j").unwrap(), "078");
        assert_eq!(call_f64(1521467703.0, "100%%").unwrap(), "100%");
        // Unknown directive passes through verbatim (matches SQL plugin).
        assert_eq!(call_f64(1521467703.0, "%Y-%q").unwrap(), "2018-%q");
    }

    #[test]
    fn tz_directives_render_utc() {
        assert_eq!(call_f64(1521467703.0, "%z").unwrap(), "+0000");
        assert_eq!(call_f64(1521467703.0, "%:z").unwrap(), "+00:00");
        assert_eq!(call_f64(1521467703.0, "%Ez").unwrap(), "+0");
    }

    #[test]
    fn subsecond_default_precision() {
        // 1521467703.123456 isn't exactly f64-representable (≈ ...01), so %N shows trailing 001.
        assert_eq!(call_f64(1521467703.123456, "%N").unwrap(), "123456001");
        assert_eq!(call_f64(1521467703.123456, "%Q").unwrap(), "123");
    }

    #[test]
    fn array_f64_with_scalar_format() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![Some(1521467703.0), None, Some(-86400.0)]));
        let u = udf();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(arr),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("%Y-%m-%d".into()))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("v", DataType::Float64, true)),
                Arc::new(Field::new("f", DataType::Utf8, true)),
            ],
            number_rows: 3,
            return_field: Arc::new(Field::new(u.name(), DataType::Utf8, true)),
            config_options: Arc::new(Default::default()),
        };
        let out = u.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(a) = out else { panic!("expected array"); };
        let s = a.as_string::<i32>();
        assert_eq!(s.value(0), "2018-03-19");
        assert!(s.is_null(1));
        assert_eq!(s.value(2), "1969-12-31");
    }

    #[test]
    fn coerce_types_folds_inputs() {
        let u = udf();
        // Numeric + string fold onto Float64.
        for t in [DataType::Int64, DataType::Int32, DataType::UInt8, DataType::Float32, DataType::Utf8] {
            assert_eq!(u.coerce_types(&[t.clone(), DataType::Utf8]).unwrap()[0], DataType::Float64, "{t:?}");
        }
        // Temporal types fold onto Timestamp(us).
        let canonical = DataType::Timestamp(TimeUnit::Microsecond, None);
        for t in [
            DataType::Date32, DataType::Date64,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ] {
            assert_eq!(u.coerce_types(&[t.clone(), DataType::Utf8]).unwrap()[0], canonical, "{t:?}");
        }
        // Non-numeric / non-temporal rejected.
        let err = u.coerce_types(&[DataType::Boolean, DataType::Utf8]).unwrap_err();
        assert!(err.to_string().contains("expected numeric/timestamp"));
        // Int64 shape sanity — exercised by the array test above.
        let _arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(1521467703i64)]));
    }
}
