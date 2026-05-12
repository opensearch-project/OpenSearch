/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `time_format(time_or_datetime, format)` — time-mode sibling of date_format.
//! MySQL time-format rules: date-only numeric tokens render with literal zeros
//! (%d→"00", %Y→"0000"); date-only name tokens (%W, %a, %M, %D, %j, %w, %U/%u,
//! %V/%v, %X/%x, %b) cause the whole render to collapse to NULL.

use std::any::Any;

use super::udf_identity;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{plan_err, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::date_format::format_dispatch;
use super::mysql_format::FormatMode;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(TimeFormatUdf::new()));
}

#[derive(Debug)]
pub struct TimeFormatUdf {
    signature: Signature,
}

impl TimeFormatUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(TimeFormatUdf, "time_format");

impl ScalarUDFImpl for TimeFormatUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "time_format"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("time_format expects 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!("time_format expects 2 arguments, got {}", arg_types.len());
        }
        let ts = match &arg_types[0] {
            DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            other => {
                return plan_err!(
                    "time_format: arg 0 expected time/timestamp/date/string, got {other:?}"
                )
            }
        };
        let fmt = match &arg_types[1] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            other => return plan_err!("time_format: arg 1 expected string, got {other:?}"),
        };
        Ok(vec![ts, fmt])
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        format_dispatch("time_format", FormatMode::Time, args)
    }
}
