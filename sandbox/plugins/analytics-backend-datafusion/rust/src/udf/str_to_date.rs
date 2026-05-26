/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `str_to_date(input, format)` — parse with MySQL tokens → `Timestamp(us)`. Missing date fields
//! default to 2000-01-01, missing time → 00:00:00. Unparseable → NULL; trailing input tolerated.

use std::any::Any;
use std::sync::Arc;

use super::udf_identity;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondBuilder};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::mysql_format::parse_mysql_format;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(StrToDateUdf::new()));
}

#[derive(Debug)]
pub struct StrToDateUdf {
    signature: Signature,
}

impl StrToDateUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(StrToDateUdf, "str_to_date");

impl ScalarUDFImpl for StrToDateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "str_to_date"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("str_to_date expects 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!("str_to_date expects 2 arguments, got {}", arg_types.len());
        }
        for (i, t) in arg_types.iter().enumerate() {
            match t {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
                other => return plan_err!("str_to_date: arg {i} expected string, got {other:?}"),
            }
        }
        Ok(vec![DataType::Utf8, DataType::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("str_to_date expects 2 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        if let (ColumnarValue::Scalar(input), ColumnarValue::Scalar(fmt)) =
            (&args.args[0], &args.args[1])
        {
            let out = match (utf8_of(input)?, utf8_of(fmt)?) {
                (Some(i), Some(f)) => parse_to_micros(&i, &f),
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                out, None,
            )));
        }

        let in_arr = args.args[0].clone().into_array(n)?;
        let f_arr = args.args[1].clone().into_array(n)?;
        let mut builder = TimestampMicrosecondBuilder::with_capacity(n);
        for i in 0..n {
            let in_opt = utf8_at(&in_arr, i)?;
            let f_opt = utf8_at(&f_arr, i)?;
            match (in_opt, f_opt) {
                (Some(s), Some(f)) => match parse_to_micros(&s, &f) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                },
                _ => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn utf8_of(s: &ScalarValue) -> Result<Option<String>> {
    match s {
        ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) => Ok(opt.clone()),
        other => exec_err!("str_to_date: expected string scalar, got {other:?}"),
    }
}

fn utf8_at(array: &ArrayRef, i: usize) -> Result<Option<String>> {
    let (is_null, value) = match array.data_type() {
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            (a.is_null(i), a.value(i).to_string())
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            (a.is_null(i), a.value(i).to_string())
        }
        other => return exec_err!("str_to_date: expected string array, got {other:?}"),
    };
    Ok(if is_null { None } else { Some(value) })
}

fn parse_to_micros(input: &str, format: &str) -> Option<i64> {
    let parsed = parse_mysql_format(input, format)?;
    let ndt = parsed.to_naive()?;
    Some(ndt.and_utc().timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_well_formed_inputs() {
        // (input, format, expected micros). Date-only defaults 00:00:00.
        for (i, f, want) in [
            ("2020-03-15 10:30:45", "%Y-%m-%d %H:%i:%S", 1_584_268_245_000_000_i64),
            ("2020-03-15", "%Y-%m-%d", 1_584_230_400_000_000),
            ("2020-03-15 10:30:45.123456", "%Y-%m-%d %H:%i:%S.%f", 1_584_268_245_123_456),
        ] {
            assert_eq!(parse_to_micros(i, f), Some(want), "input={i}");
        }
        assert!(parse_to_micros("2020-03-15 extra", "%Y-%m-%d").is_some());
    }

    #[test]
    fn unparseable_input_returns_none() {
        assert!(parse_to_micros("not-a-date", "%Y-%m-%d").is_none());
        assert!(parse_to_micros("2020-13-01", "%Y-%m-%d").is_none());
    }
}
