/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `from_unixtime(seconds)` — UNIX seconds (fractional ok) → `Timestamp(us)`. Negative / ≥ MySQL
//! upper bound / non-finite → NULL. 2-arg `(seconds, format)` overload deferred to date_format.

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

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(FromUnixtimeUdf::new()));
}

/// MySQL docs: values at/above 32536771199.999999 return 0; first failing second is 32_536_771_200.
const MAX_UNIX_SECONDS_EXCLUSIVE: f64 = 32_536_771_200.0;

#[derive(Debug)]
pub struct FromUnixtimeUdf {
    signature: Signature,
}

impl FromUnixtimeUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(FromUnixtimeUdf, "from_unixtime");

impl ScalarUDFImpl for FromUnixtimeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "from_unixtime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return plan_err!("from_unixtime expects 1 argument, got {}", arg_types.len());
        }
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args("from_unixtime", arg_types, &[CoerceMode::Float64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!(
                "from_unixtime expects 1 argument, got {}",
                args.args.len()
            );
        }
        let n = args.number_rows;

        if let ColumnarValue::Scalar(ScalarValue::Float64(v)) = &args.args[0] {
            let micros = v.and_then(to_micros);
            return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                micros, None,
            )));
        }

        let input = args.args[0].clone().into_array(n)?;
        let input = input.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
        let mut builder = TimestampMicrosecondBuilder::with_capacity(n);
        for i in 0..input.len() {
            if input.is_null(i) {
                builder.append_null();
                continue;
            }
            match to_micros(input.value(i)) {
                Some(us) => builder.append_value(us),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn to_micros(seconds: f64) -> Option<i64> {
    if !seconds.is_finite() || !(0.0..MAX_UNIX_SECONDS_EXCLUSIVE).contains(&seconds) {
        return None;
    }
    // PPL truncates nanos via int cast after `(v%1)*1e9`; trunc ≡ floor for non-negative seconds.
    Some((seconds * 1_000_000.0).trunc() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_out_of_range_and_non_finite() {
        for v in [-0.1, MAX_UNIX_SECONDS_EXCLUSIVE, MAX_UNIX_SECONDS_EXCLUSIVE + 1.0, f64::NAN, f64::INFINITY] {
            assert_eq!(to_micros(v), None, "v={v}");
        }
    }

    #[test]
    fn converts_epoch_and_fractional_seconds() {
        assert_eq!(to_micros(0.0), Some(0));
        assert_eq!(to_micros(1.5), Some(1_500_000));
        // Sub-microsecond truncates (not rounds) to match PPL's int cast.
        assert_eq!(to_micros(0.000_000_9), Some(0));
        assert_eq!(to_micros(0.000_001_9), Some(1));
    }
}
