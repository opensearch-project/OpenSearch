/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `reduce_eval(agg_name, state)` — evaluates an opaque aggregate's partial state
//! to produce a sortable scalar. Used by the TopK rewriter's reduce Project.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Column;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(ReduceEvalUdf));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ReduceEvalUdf;

impl ScalarUDFImpl for ReduceEvalUdf {
    fn name(&self) -> &str {
        "reduce_eval"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let agg_name = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => {
                return Err(DataFusionError::Execution(
                    "reduce_eval: first arg must be literal agg name".into(),
                ))
            }
        };
        let state_col = match &args.args[1] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };

        match agg_name.as_str() {
            "approx_distinct" => eval_approx_distinct(&state_col),
            other => Err(DataFusionError::Execution(format!(
                "reduce_eval: unsupported aggregate '{other}'"
            ))),
        }
    }
}

fn eval_approx_distinct(state_col: &ArrayRef) -> Result<ColumnarValue> {
    let binary = state_col
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("reduce_eval(approx_distinct): expected Binary state".into())
        })?;

    let field: Arc<Field> = Arc::new(Field::new("x", DataType::Int64, true));
    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![field
        .as_ref()
        .clone()]));
    let expr: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("x", 0));
    let ret_field: Arc<Field> = Arc::new(Field::new("r", DataType::UInt64, true));

    let mut results = Vec::with_capacity(binary.len());
    for i in 0..binary.len() {
        if binary.is_null(i) {
            results.push(0u64);
            continue;
        }
        let mut acc = approx_distinct_udaf().accumulator(AccumulatorArgs {
            return_field: ret_field.clone(),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            name: "x",
            is_distinct: false,
            exprs: &[expr.clone()],
            expr_fields: &[field.clone()],
            is_reversed: false,
        })?;
        let state_array: ArrayRef = Arc::new(BinaryArray::from(vec![binary.value(i)]));
        acc.merge_batch(&[state_array])?;
        match acc.evaluate()? {
            ScalarValue::UInt64(Some(v)) => results.push(v),
            _ => results.push(0),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(results))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::BinaryArray;
    use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
    use datafusion::logical_expr::Accumulator;

    #[test]
    fn test_reduce_eval_approx_distinct() {
        // Build an HLL state by updating an accumulator
        let field: Arc<Field> = Arc::new(Field::new("x", DataType::Int64, true));
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![field
            .as_ref()
            .clone()]));
        let expr: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(Column::new("x", 0));
        let ret_field: Arc<Field> = Arc::new(Field::new("r", DataType::UInt64, true));
        let mut acc = approx_distinct_udaf()
            .accumulator(AccumulatorArgs {
                return_field: ret_field.clone(),
                schema: &schema,
                ignore_nulls: false,
                order_bys: &[],
                name: "x",
                is_distinct: false,
                exprs: &[expr.clone()],
                expr_fields: &[field.clone()],
                is_reversed: false,
            })
            .unwrap();

        // Feed some values
        let values: ArrayRef = Arc::new(datafusion::arrow::array::Int64Array::from(vec![
            1, 2, 3, 4, 5,
        ]));
        acc.update_batch(&[values]).unwrap();
        let state = acc.state().unwrap();

        // Extract the Binary state
        let state_array = state[0].to_array_of_size(1).unwrap();
        let binary = state_array.as_any().downcast_ref::<BinaryArray>().unwrap();

        // Run reduce_eval
        let result = eval_approx_distinct(&(Arc::new(binary.clone()) as ArrayRef)).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let uint_arr = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
                assert_eq!(uint_arr.value(0), 5); // 5 distinct values
            }
            _ => panic!("expected Array"),
        }
    }
}
