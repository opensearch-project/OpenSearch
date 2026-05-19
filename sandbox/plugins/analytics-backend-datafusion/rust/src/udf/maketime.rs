/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `maketime(hour, minute, second)` → `Time64(us)`. Hour/minute rounded; second passes with fraction.
//! Negative / non-finite / out-of-range (after rounding) / null → NULL.

use std::any::Any;
use std::sync::Arc;

use super::udf_identity;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Time64MicrosecondBuilder};
use datafusion::arrow::datatypes::{DataType, Float64Type, TimeUnit};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(MaketimeUdf::new()));
}

#[derive(Debug)]
pub struct MaketimeUdf {
    signature: Signature,
}

impl MaketimeUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(MaketimeUdf, "maketime");

impl ScalarUDFImpl for MaketimeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "maketime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("maketime expects 3 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Time64(TimeUnit::Microsecond))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "maketime",
            arg_types,
            &[CoerceMode::Float64, CoerceMode::Float64, CoerceMode::Float64],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return exec_err!("maketime expects 3 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        if let (
            ColumnarValue::Scalar(ScalarValue::Float64(h)),
            ColumnarValue::Scalar(ScalarValue::Float64(m)),
            ColumnarValue::Scalar(ScalarValue::Float64(s)),
        ) = (&args.args[0], &args.args[1], &args.args[2])
        {
            let micros = match (h, m, s) {
                (Some(h), Some(m), Some(s)) => micros_of_day(*h, *m, *s),
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(
                micros,
            )));
        }

        let h = args.args[0].clone().into_array(n)?;
        let m = args.args[1].clone().into_array(n)?;
        let s = args.args[2].clone().into_array(n)?;
        let h = h.as_primitive::<Float64Type>();
        let m = m.as_primitive::<Float64Type>();
        let s = s.as_primitive::<Float64Type>();
        let mut builder = Time64MicrosecondBuilder::with_capacity(n);
        for i in 0..n {
            if h.is_null(i) || m.is_null(i) || s.is_null(i) {
                builder.append_null();
                continue;
            }
            match micros_of_day(h.value(i), m.value(i), s.value(i)) {
                Some(us) => builder.append_value(us),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn micros_of_day(hour: f64, minute: f64, second: f64) -> Option<i64> {
    if !hour.is_finite() || !minute.is_finite() || !second.is_finite() {
        return None;
    }
    if hour < 0.0 || minute < 0.0 || second < 0.0 {
        return None;
    }
    // Java's Math.round(d) = (long)floor(d+0.5); Rust's f64::round is half-away-from-zero which
    // matches for non-negative inputs. PPL throws on out-of-range; we return None → NULL.
    let hour = hour.round() as i64;
    let minute = minute.round() as i64;
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || second >= 60.0 {
        return None;
    }
    let second_micros = (second * 1_000_000.0).trunc() as i64;
    Some(hour * 3_600_000_000 + minute * 60_000_000 + second_micros)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_formed_and_rounding_semantics() {
        // (1, 2, 3.5) → 1h2m3.5s.
        assert_eq!(
            micros_of_day(1.0, 2.0, 3.5),
            Some(3_600_000_000 + 2 * 60_000_000 + 3_500_000)
        );
        // 1.6→2h, 2.4→2m (half-away-from-zero), 3.999999s truncated.
        assert_eq!(
            micros_of_day(1.6, 2.4, 3.999_999),
            Some(2 * 3_600_000_000 + 2 * 60_000_000 + 3_999_999)
        );
    }

    #[test]
    fn rejects_invalid_operands() {
        // Negative components.
        for args in [(-0.1, 0.0, 0.0), (0.0, -0.1, 0.0), (0.0, 0.0, -0.1)] {
            assert_eq!(micros_of_day(args.0, args.1, args.2), None, "{args:?}");
        }
        // Out of range after rounding: 23.5→24h, 59.5→60m, 60s (no rounding).
        assert_eq!(micros_of_day(23.5, 0.0, 0.0), None);
        assert_eq!(micros_of_day(0.0, 59.5, 0.0), None);
        assert_eq!(micros_of_day(0.0, 0.0, 60.0), None);
        // Non-finite.
        assert_eq!(micros_of_day(f64::NAN, 0.0, 0.0), None);
        assert_eq!(micros_of_day(f64::INFINITY, 0.0, 0.0), None);
    }
}
