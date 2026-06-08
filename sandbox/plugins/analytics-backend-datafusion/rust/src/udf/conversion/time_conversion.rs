/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


//! * `ctime(value, format)` — UNIX epoch seconds → formatted time string. Fractional seconds are
//!   preserved as `nanos` adjustment
//! * `mktime(value, format)` — formatted time string → UNIX epoch seconds as double. If the
//!   input already parses as a number it is forwarded unchanged

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(TimeConversionUdf::new(TimeConversionFn::Ctime)));
    ctx.register_udf(ScalarUDF::from(TimeConversionUdf::new(TimeConversionFn::Mktime)));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TimeConversionFn {
    Ctime,
    Mktime,
}

impl TimeConversionFn {
    fn name(self) -> &'static str {
        match self {
            TimeConversionFn::Ctime => "ctime",
            TimeConversionFn::Mktime => "mktime",
        }
    }

    fn return_type(self) -> DataType {
        match self {
            TimeConversionFn::Ctime => DataType::Utf8,
            TimeConversionFn::Mktime => DataType::Float64,
        }
    }
}

/// Signature shared across both functions: `(Utf8, Utf8)` or `(LargeUtf8, LargeUtf8)`
fn signature() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
        ],
        Volatility::Immutable,
    )
}

#[derive(Debug)]
pub struct TimeConversionUdf {
    kind: TimeConversionFn,
    signature: Signature,
}

impl TimeConversionUdf {
    fn new(kind: TimeConversionFn) -> Self {
        Self { kind, signature: signature() }
    }
}

impl PartialEq for TimeConversionUdf {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}
impl Eq for TimeConversionUdf {}
impl Hash for TimeConversionUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
    }
}

impl ScalarUDFImpl for TimeConversionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.kind.return_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!(
                "{} expects exactly 2 arguments, got {}",
                self.kind.name(),
                args.args.len()
            );
        }
        let value = &args.args[0];
        let format = &args.args[1];
        match self.kind {
            TimeConversionFn::Ctime => invoke_ctime(value, format),
            TimeConversionFn::Mktime => invoke_mktime(value, format),
        }
    }
}

// ── ctime ──────────────────────────────────────────────────────────────────────────────────

fn invoke_ctime(value: &ColumnarValue, format: &ColumnarValue) -> Result<ColumnarValue> {
    let format_scalar = extract_scalar_string(format, "ctime: format")?;
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(opt))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(opt)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Utf8(opt.as_ref().and_then(|s| format_ctime(s, &format_scalar))),
        )),
        ColumnarValue::Scalar(other) => exec_err!("ctime: expected Utf8 value, got {other:?}"),
        ColumnarValue::Array(arr) => {
            let out: StringArray = match arr.data_type() {
                DataType::Utf8 => arr
                    .as_string::<i32>()
                    .iter()
                    .map(|opt| opt.and_then(|s| format_ctime(s, &format_scalar)))
                    .collect(),
                DataType::LargeUtf8 => arr
                    .as_string::<i64>()
                    .iter()
                    .map(|opt| opt.and_then(|s| format_ctime(s, &format_scalar)))
                    .collect(),
                other => {
                    return exec_err!("ctime: expected Utf8 array, got {other:?}");
                }
            };
            Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
        }
    }
}

fn format_ctime(value: &str, format: &Option<String>) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let epoch_seconds: f64 = trimmed.parse().ok()?;
    let format_str = format.as_deref()?.trim();
    if format_str.is_empty() {
        return None;
    }
    let (whole, frac) = split_epoch_seconds(epoch_seconds);
    // Seconds + nanos
    let nanos = (frac * 1_000_000_000.0) as u32;
    let dt: DateTime<Utc> = Utc.timestamp_opt(whole, nanos).single()?;
    let chrono_fmt = format_str.to_string();
    Some(format!("{}", dt.format(&chrono_fmt)))
}

// ── mktime ─────────────────────────────────────────────────────────────────────────────────

fn invoke_mktime(value: &ColumnarValue, format: &ColumnarValue) -> Result<ColumnarValue> {
    let format_scalar = extract_scalar_string(format, "mktime: format")?;
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(opt))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(opt)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Float64(opt.as_ref().and_then(|s| parse_mktime(s, &format_scalar))),
        )),
        ColumnarValue::Scalar(other) => exec_err!("mktime: expected Utf8 value, got {other:?}"),
        ColumnarValue::Array(arr) => {
            let out: Float64Array = match arr.data_type() {
                DataType::Utf8 => arr
                    .as_string::<i32>()
                    .iter()
                    .map(|opt| opt.and_then(|s| parse_mktime(s, &format_scalar)))
                    .collect(),
                DataType::LargeUtf8 => arr
                    .as_string::<i64>()
                    .iter()
                    .map(|opt| opt.and_then(|s| parse_mktime(s, &format_scalar)))
                    .collect(),
                other => {
                    return exec_err!("mktime: expected Utf8 array, got {other:?}");
                }
            };
            Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
        }
    }
}

fn parse_mktime(value: &str, format: &Option<String>) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(n) = trimmed.parse::<f64>() {
        return Some(n);
    }
    let format_str = format.as_deref()?.trim();
    if format_str.is_empty() {
        return None;
    }
    NaiveDateTime::parse_from_str(trimmed, format_str)
        .ok()
        .map(|parsed| parsed.and_utc().timestamp() as f64)
}

// ── helpers ────────────────────────────────────────────────────────────────────────────────

fn extract_scalar_string(column: &ColumnarValue, ctx: &str) -> Result<Option<String>> {
    match column {
        ColumnarValue::Scalar(ScalarValue::Utf8(opt))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(opt)) => Ok(opt.clone()),
        ColumnarValue::Scalar(other) => exec_err!("{ctx}: expected Utf8 scalar, got {other:?}"),
        ColumnarValue::Array(_) => exec_err!(
            "{ctx}: format must be a scalar literal, got an array"
        ),
    }
}

/// Split a fractional epoch value into `(whole seconds, fractional seconds)`
fn split_epoch_seconds(epoch: f64) -> (i64, f64) {
    let whole = epoch.floor() as i64;
    let frac = epoch - (whole as f64);
    (whole, frac)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_FORMAT: &str = "%m/%d/%Y %H:%M:%S";

    fn ctime_default(value: &str) -> Option<String> {
        format_ctime(value, &Some(DEFAULT_FORMAT.to_string()))
    }

    fn mktime_default(value: &str) -> Option<f64> {
        parse_mktime(value, &Some(DEFAULT_FORMAT.to_string()))
    }

    #[test]
    fn ctime_default_format_matches() {
        assert_eq!(
            ctime_default("1066507633"),
            Some("10/18/2003 20:07:13".to_string())
        );
        assert_eq!(
            ctime_default("0"),
            Some("01/01/1970 00:00:00".to_string())
        );
    }

    #[test]
    fn ctime_formats_epoch_with_default_format() {
        // 1_700_000_000 = 2023-11-14 22:13:20 UTC.
        let out = format_ctime(
            "1700000000",
            &Some("%m/%d/%Y %H:%M:%S".to_string()),
        );
        assert_eq!(out, Some("11/14/2023 22:13:20".to_string()));
    }

    #[test]
    fn ctime_respects_custom_format() {
        let out = format_ctime("1700000000", &Some("%Y-%m-%d".to_string()));
        assert_eq!(out, Some("2023-11-14".to_string()));
    }

    #[test]
    fn ctime_custom_format_matches() {
        assert_eq!(
            format_ctime("1066507633", &Some("%Y-%m-%d %H:%M:%S".to_string())),
            Some("2003-10-18 20:07:13".to_string())
        );
        assert_eq!(
            format_ctime("1066507633", &Some("%d/%m/%Y".to_string())),
            Some("18/10/2003".to_string())
        );
        assert_eq!(
            format_ctime("0", &Some("%Y".to_string())),
            Some("1970".to_string())
        );
        assert_eq!(format_ctime("1066507633", &Some(String::new())), None);
    }

    #[test]
    fn ctime_rejects_unparseable_and_empty() {
        assert_eq!(ctime_default("invalid"), None);
        assert_eq!(ctime_default(""), None);
        assert_eq!(ctime_default("abc123"), None);
    }

    #[test]
    fn ctime_rejects_empty_and_unparseable() {
        assert_eq!(format_ctime("", &Some("%H".to_string())), None);
        assert_eq!(format_ctime("abc", &Some("%H".to_string())), None);
    }

    #[test]
    fn ctime_rejects_empty_format() {
        assert_eq!(format_ctime("0", &Some(String::new())), None);
        assert_eq!(format_ctime("0", &None), None);
    }

    #[test]
    fn mktime_default_format_matches() {
        assert_eq!(
            mktime_default("10/18/2003 20:07:13"),
            Some(1_066_507_633.0)
        );
        assert_eq!(
            mktime_default("01/01/2000 00:00:00"),
            Some(946_684_800.0)
        );
        assert_eq!(mktime_default("1066473433"), Some(1_066_473_433.0));
    }

    #[test]
    fn mktime_matches() {
        assert_eq!(
            parse_mktime(
                "18/10/2003 20:07:13",
                &Some("%d/%m/%Y %H:%M:%S".to_string())
            ),
            Some(1_066_507_633.0)
        );
        assert_eq!(
            parse_mktime(
                "2003-10-18 20:07:13",
                &Some("%Y-%m-%d %H:%M:%S".to_string())
            ),
            Some(1_066_507_633.0)
        );
        assert_eq!(
            parse_mktime(
                "01/01/2000 00:00:00",
                &Some("%d/%m/%Y %H:%M:%S".to_string())
            ),
            Some(946_684_800.0)
        );
        assert_eq!(
            parse_mktime(
                "2003-10-18 20:07:13",
                &Some("invalid format".to_string())
            ),
            None
        );
        assert_eq!(
            parse_mktime("10/18/2003 20:07:13", &Some(String::new())),
            None
        );
    }

    #[test]
    fn mktime_parses_formatted_string() {
        let out = parse_mktime(
            "11/14/2023 22:13:20",
            &Some("%m/%d/%Y %H:%M:%S".to_string()),
        );
        assert_eq!(out, Some(1_700_000_000.0));
    }

    #[test]
    fn mktime_rejects_date_only_format() {
        assert_eq!(parse_mktime("2023-11-14", &Some("%Y-%m-%d".to_string())), None);
    }

    #[test]
    fn mktime_accepts_pre_parsed_number() {
        let out = parse_mktime("3600", &Some("%H:%M:%S".to_string()));
        assert_eq!(out, Some(3600.0));
    }

    #[test]
    fn mktime_rejects_unparseable() {
        assert_eq!(
            parse_mktime("not a date", &Some("%Y-%m-%d".to_string())),
            None
        );
        assert_eq!(mktime_default("invalid"), None);
        assert_eq!(mktime_default(""), None);
        assert_eq!(mktime_default("not-a-date"), None);
    }

    #[test]
    fn mktime_rejects_empty_format() {
        // Non-numeric input with empty format → no way to parse → None.
        assert_eq!(parse_mktime("11/14/2023", &Some(String::new())), None);
        assert_eq!(parse_mktime("11/14/2023", &None), None);
    }

    #[test]
    fn mktime_roundtrips_through_ctime() {
        let rendered = format_ctime(
            "1700000000",
            &Some("%Y-%m-%d %H:%M:%S".to_string()),
        )
        .unwrap();
        let back = parse_mktime(&rendered, &Some("%Y-%m-%d %H:%M:%S".to_string()));
        assert_eq!(back, Some(1_700_000_000.0));
    }
}
