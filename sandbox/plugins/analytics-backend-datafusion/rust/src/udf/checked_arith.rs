/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Overflow-checked BIGINT arithmetic UDFs: `checked_add_i64`, `checked_sub_i64`,
//! `checked_mul_i64`.
//!
//! DataFusion's default integer arithmetic (`Operator::Plus/Minus/Multiply`) uses arrow's
//! *wrapping* kernels, so `9223372036854775807 * 2` silently wraps into a negative i64 instead
//! of erroring. PPL callers expect an error, not a wrapped value. [`crate::checked_arith_rewrite`]
//! rewrites every `Int64 {+,-,*} Int64` logical `BinaryExpr` into a call to one of these UDFs,
//! which invoke arrow's *checked* numeric kernels (`add`/`sub`/`mul`) and surface a stable,
//! self-authored error on overflow.
//!
//! The overflow message carries [`OVERFLOW_KEYPHRASE`] verbatim; `NativeErrorConverter` (Java)
//! matches on that substring and maps it to an `IllegalArgumentException` (HTTP 400).

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::kernels::numeric::{add, mul, sub};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Stable Rust→Java contract phrase. `NativeErrorConverter.PATTERNS` matches on this exact
/// substring to classify the error as a client (400) error. Changing it requires a coordinated
/// edit in `NativeErrorConverter.java`.
pub const OVERFLOW_KEYPHRASE: &str = "BIGINT arithmetic overflow";

#[derive(Copy, Clone, Debug)]
enum Op {
    Add,
    Sub,
    Mul,
}

impl Op {
    fn verb(self) -> &'static str {
        match self {
            Op::Add => "addition",
            Op::Sub => "subtraction",
            Op::Mul => "multiplication",
        }
    }
}

/// `checked_{add,sub,mul}_i64(bigint, bigint) -> bigint`. Errors on 64-bit overflow instead of
/// wrapping. The return type is `Int64`, identical to the `BinaryExpr` it replaces, so the
/// rewrite leaves plan schemas byte-identical.
#[derive(Debug)]
pub struct CheckedArithUdf {
    op: Op,
    name: &'static str,
    signature: Signature,
}

impl CheckedArithUdf {
    fn new(op: Op, name: &'static str) -> Self {
        Self {
            op,
            name,
            // Exact (Int64, Int64) — the rewrite only ever emits this shape. Immutable (not
            // Volatile) so the UDF still participates in predicate pushdown and common-subexpression
            // elimination. Const-folding an *overflowing* literal pair does not lose the error:
            // DataFusion's ConstEvaluator preserves the original expr on a UDF runtime error
            // (it only fails-fast for CAST), so overflow still surfaces at execution with our
            // stable message; a non-overflowing constant simply folds to its exact value.
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

// `ScalarUDFImpl` requires DynEq + DynHash. Instances are distinguished only by name.
impl PartialEq for CheckedArithUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for CheckedArithUdf {}
impl Hash for CheckedArithUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for CheckedArithUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Must equal the BinaryExpr output type (Int64) so plan schemas are unchanged by the
        // rewrite (see checked_arith_rewrite: no recompute_schema).
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(exec_datafusion_err!(
                "{} expects exactly 2 arguments, got {}",
                self.name,
                args.args.len()
            ));
        }
        let n = args.number_rows;
        // Normalize both operands to arrays; scalar-scalar calls arrive with number_rows == 1.
        let lhs: ArrayRef = args.args[0].to_array(n)?;
        let rhs: ArrayRef = args.args[1].to_array(n)?;

        // arrow's checked kernels null-propagate (null in either operand -> null out) and return
        // Err(ArithmeticOverflow) only for non-null overflowing pairs. `&dyn Array` is a `Datum`.
        let result = match self.op {
            Op::Add => add(&lhs, &rhs),
            Op::Sub => sub(&lhs, &rhs),
            Op::Mul => mul(&lhs, &rhs),
        };
        match result {
            Ok(arr) => Ok(ColumnarValue::Array(arr)),
            // Re-wrap arrow's ArithmeticOverflow into a stable, self-authored message so the
            // Java-side keyphrase does not depend on arrow's wording across version bumps.
            Err(e) => Err(DataFusionError::Execution(format!(
                "{OVERFLOW_KEYPHRASE}: {} of two BIGINT values exceeded the 64-bit range \
                 (underlying: {e})",
                self.op.verb()
            ))),
        }
    }
}

fn make_udf(op: Op, name: &'static str) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(CheckedArithUdf::new(op, name)))
}

/// `checked_add_i64(bigint, bigint) -> bigint`.
pub fn checked_add_udf() -> Arc<ScalarUDF> {
    make_udf(Op::Add, "checked_add_i64")
}

/// `checked_sub_i64(bigint, bigint) -> bigint`.
pub fn checked_sub_udf() -> Arc<ScalarUDF> {
    make_udf(Op::Sub, "checked_sub_i64")
}

/// `checked_mul_i64(bigint, bigint) -> bigint`.
pub fn checked_mul_udf() -> Arc<ScalarUDF> {
    make_udf(Op::Mul, "checked_mul_i64")
}

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::clone(&checked_add_udf()));
    ctx.register_udf(ScalarUDF::clone(&checked_sub_udf()));
    ctx.register_udf(ScalarUDF::clone(&checked_mul_udf()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, AsArray, Int64Array};
    use datafusion::arrow::datatypes::{Field, Int64Type};
    use datafusion::logical_expr::ColumnarValue;

    fn call(udf: Arc<ScalarUDF>, a: &[Option<i64>], b: &[Option<i64>]) -> Result<ArrayRef> {
        let la: ArrayRef = Arc::new(Int64Array::from(a.to_vec()));
        let lb: ArrayRef = Arc::new(Int64Array::from(b.to_vec()));
        let return_field = Arc::new(Field::new("r", DataType::Int64, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(la), ColumnarValue::Array(lb)],
            arg_fields: vec![
                Arc::new(Field::new("a", DataType::Int64, true)),
                Arc::new(Field::new("b", DataType::Int64, true)),
            ],
            number_rows: a.len(),
            return_field,
            config_options: Arc::new(Default::default()),
        };
        match udf.inner().invoke_with_args(args)? {
            ColumnarValue::Array(arr) => Ok(arr),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn mul_overflow_errors_with_stable_phrase() {
        let err = call(checked_mul_udf(), &[Some(i64::MAX)], &[Some(2)]).unwrap_err();
        assert!(err.to_string().contains(OVERFLOW_KEYPHRASE), "got: {err}");
    }

    #[test]
    fn add_overflow_errors() {
        let err = call(checked_add_udf(), &[Some(i64::MAX)], &[Some(1)]).unwrap_err();
        assert!(err.to_string().contains(OVERFLOW_KEYPHRASE), "got: {err}");
    }

    #[test]
    fn sub_overflow_errors() {
        let err = call(checked_sub_udf(), &[Some(i64::MIN)], &[Some(1)]).unwrap_err();
        assert!(err.to_string().contains(OVERFLOW_KEYPHRASE), "got: {err}");
    }

    #[test]
    fn no_overflow_returns_exact_value() {
        let out = call(checked_mul_udf(), &[Some(3)], &[Some(4)]).unwrap();
        assert_eq!(out.as_primitive::<Int64Type>().value(0), 12);
    }

    #[test]
    fn negative_product_is_exact_not_overflow() {
        // -3 * 20000 = -60000, well within i64 range; must not error.
        let out = call(checked_mul_udf(), &[Some(-3)], &[Some(20000)]).unwrap();
        assert_eq!(out.as_primitive::<Int64Type>().value(0), -60000);
    }

    #[test]
    fn null_operand_propagates_null_not_error() {
        let out = call(checked_mul_udf(), &[None], &[Some(2)]).unwrap();
        assert!(out.as_primitive::<Int64Type>().is_null(0));
    }

    #[test]
    fn one_overflow_in_batch_fails_whole_call() {
        // arrow's checked kernel errors on the first overflowing pair, failing the batch.
        let err = call(
            checked_mul_udf(),
            &[Some(1), Some(i64::MAX)],
            &[Some(1), Some(2)],
        )
        .unwrap_err();
        assert!(err.to_string().contains(OVERFLOW_KEYPHRASE), "got: {err}");
    }
}
