/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `makedate(year, day_of_year)` → `Date32`. MySQL quirks: `year==0` remaps to 2000; `doy<=0` /
//! `year<0` → NULL; `doy` past year-end cascades into the next year.

use std::any::Any;
use std::sync::Arc;

use super::udf_identity;

use chrono::{Datelike, NaiveDate};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Date32Builder};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(MakedateUdf::new()));
}

/// Offset between CE (0001-01-01) and Unix epoch (1970-01-01) used for Arrow Date32 conversion.
const CE_TO_UNIX_EPOCH_DAYS: i32 = 719_163;

#[derive(Debug)]
pub struct MakedateUdf {
    signature: Signature,
}

impl MakedateUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(MakedateUdf, "makedate");

impl ScalarUDFImpl for MakedateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "makedate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("makedate expects 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Date32)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "makedate",
            arg_types,
            &[CoerceMode::Float64, CoerceMode::Float64],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("makedate expects 2 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        if let (
            ColumnarValue::Scalar(ScalarValue::Float64(y)),
            ColumnarValue::Scalar(ScalarValue::Float64(d)),
        ) = (&args.args[0], &args.args[1])
        {
            let days = match (y, d) {
                (Some(y), Some(d)) => days_since_epoch(*y, *d),
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Date32(days)));
        }

        let y = args.args[0].clone().into_array(n)?;
        let d = args.args[1].clone().into_array(n)?;
        let y = y.as_primitive::<Float64Type>();
        let d = d.as_primitive::<Float64Type>();
        let mut builder = Date32Builder::with_capacity(n);
        for i in 0..n {
            if y.is_null(i) || d.is_null(i) {
                builder.append_null();
                continue;
            }
            match days_since_epoch(y.value(i), d.value(i)) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn days_since_epoch(year: f64, day_of_year: f64) -> Option<i32> {
    if !year.is_finite() || !day_of_year.is_finite() {
        return None;
    }
    let year = year.round() as i64;
    let doy = day_of_year.round() as i64;
    if doy <= 0 || year < 0 {
        return None;
    }
    let year = if year == 0 { 2000 } else { year };
    let year: i32 = year.try_into().ok()?;
    let start = NaiveDate::from_yo_opt(year, 1)?;
    let date = start.checked_add_days(chrono::Days::new((doy - 1) as u64))?;
    Some(date.num_days_from_ce() - CE_TO_UNIX_EPOCH_DAYS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn days(y: i32, m: u32, d: u32) -> i32 {
        NaiveDate::from_ymd_opt(y, m, d).unwrap().num_days_from_ce() - CE_TO_UNIX_EPOCH_DAYS
    }

    #[test]
    fn well_formed_and_overflow_cases() {
        // (year, doy) → (y, m, d) expected.
        for (y, doy, ey, em, ed) in [
            (2024.0, 1.0, 2024, 1, 1),
            (2024.0, 60.0, 2024, 2, 29),   // 2024 leap; doy 60 = Feb 29
            (2024.0, 366.0, 2024, 12, 31),
            (0.0, 1.0, 2000, 1, 1),        // year 0 remaps to 2000
            (2023.0, 366.0, 2024, 1, 1),   // non-leap overflow cascades
            (2024.4, 60.6, 2024, 3, 1),    // fractional operands round: 2024, 61
        ] {
            assert_eq!(days_since_epoch(y, doy), Some(days(ey, em, ed)), "y={y} doy={doy}");
        }
    }

    #[test]
    fn rejects_out_of_range_and_non_finite() {
        assert_eq!(days_since_epoch(2024.0, 0.0), None);
        assert_eq!(days_since_epoch(2024.0, -1.0), None);
        assert_eq!(days_since_epoch(-1.0, 100.0), None);
        assert_eq!(days_since_epoch(f64::NAN, 1.0), None);
        assert_eq!(days_since_epoch(2024.0, f64::INFINITY), None);
    }
}
