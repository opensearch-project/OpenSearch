/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `date_format(datetime, format)` — render a timestamp via MySQL-style tokens
//! ([`mysql_format`](super::mysql_format)). Returns Utf8; null input → null.

use std::any::Any;
use std::sync::Arc;

use super::udf_identity;

use chrono::{TimeZone, Utc};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::mysql_format::{format_datetime, FormatMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(DateFormatUdf::new()));
}

#[derive(Debug)]
pub struct DateFormatUdf {
    signature: Signature,
}

impl DateFormatUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(DateFormatUdf, "date_format");

impl ScalarUDFImpl for DateFormatUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "date_format"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("date_format expects 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!("date_format expects 2 arguments, got {}", arg_types.len());
        }
        let ts = match &arg_types[0] {
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            other => return plan_err!("date_format: arg 0 expected timestamp/date/string, got {other:?}"),
        };
        let fmt = match &arg_types[1] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            other => return plan_err!("date_format: arg 1 expected string, got {other:?}"),
        };
        Ok(vec![ts, fmt])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        format_dispatch("date_format", FormatMode::Date, args)
    }
}

pub(crate) fn format_dispatch(
    udf: &str,
    mode: FormatMode,
    args: ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    if args.args.len() != 2 {
        return exec_err!("{udf} expects 2 arguments, got {}", args.args.len());
    }
    let n = args.number_rows;

    if let (ColumnarValue::Scalar(v), ColumnarValue::Scalar(fmt)) = (&args.args[0], &args.args[1]) {
        let micros = match v {
            ScalarValue::TimestampMicrosecond(v, _) => *v,
            other => return exec_err!("{udf}: unsupported ts scalar: {other:?}"),
        };
        let fmt_str = match fmt {
            ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) => opt.clone(),
            other => return exec_err!("{udf}: unsupported format scalar: {other:?}"),
        };
        let out = match (micros, fmt_str) {
            (Some(m), Some(f)) => render_at(m, &f, mode),
            _ => None,
        };
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)));
    }

    let v_arr = args.args[0].clone().into_array(n)?;
    let f_arr = args.args[1].clone().into_array(n)?;
    let ts = v_arr
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(format!(
                "{udf}: expected Timestamp(Microsecond, None) after coercion, got {:?}",
                v_arr.data_type()
            ))
        })?;
    let mut builder = StringBuilder::with_capacity(n, 0);
    for i in 0..n {
        if ts.is_null(i) {
            builder.append_null();
            continue;
        }
        let fmt_opt = match f_arr.data_type() {
            DataType::Utf8 => {
                let a = f_arr.as_string::<i32>();
                if a.is_null(i) { None } else { Some(a.value(i).to_string()) }
            }
            DataType::LargeUtf8 => {
                let a = f_arr.as_string::<i64>();
                if a.is_null(i) { None } else { Some(a.value(i).to_string()) }
            }
            other => return exec_err!("{udf}: format array has unexpected type {other:?}"),
        };
        match fmt_opt.and_then(|f| render_at(ts.value(i), &f, mode)) {
            Some(s) => builder.append_value(&s),
            None => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
}

fn render_at(micros: i64, format: &str, mode: FormatMode) -> Option<String> {
    let seconds = micros.div_euclid(1_000_000);
    let micros_rem = micros.rem_euclid(1_000_000) as u32;
    let dt = Utc.timestamp_opt(seconds, micros_rem * 1_000).single()?;
    format_datetime(dt, format, mode)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_scalar_matches_mysql_format() {
        let out = render_at(1_584_268_245_123_456, "%Y-%m-%d %H:%i:%S", FormatMode::Date).unwrap();
        assert_eq!(out, "2020-03-15 10:30:45");
    }

    #[test]
    fn null_format_returns_none() {
        assert!(render_at(0, "", FormatMode::Date).is_some());
    }
}
