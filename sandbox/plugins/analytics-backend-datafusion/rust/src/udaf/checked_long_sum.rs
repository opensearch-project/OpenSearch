/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Distinctly named analytics binding for SQL's `CHECKED_LONG_SUM`.
//!
//! Analytics routing intentionally keeps DataFusion's native SUM semantics. This wrapper delegates
//! every SUM execution path while retaining the `checked_long_sum` name, preventing collisions when
//! a distributed intermediate schema contains both SUM and CHECKED_LONG_SUM over the same field.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, Expr, GroupsAccumulator, Operator,
    ReversedUDAF, SetMonotonicity, Signature,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(CheckedLongSum::new()));
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CheckedLongSum {
    native_sum: Arc<AggregateUDF>,
}

impl CheckedLongSum {
    fn new() -> Self {
        Self {
            native_sum: sum_udaf(),
        }
    }
}

impl AggregateUDFImpl for CheckedLongSum {
    fn name(&self) -> &str {
        "checked_long_sum"
    }

    fn signature(&self) -> &Signature {
        self.native_sum.inner().signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.native_sum.inner().return_type(arg_types)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.native_sum.inner().accumulator(args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.native_sum.inner().state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.native_sum.inner().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        self.native_sum.inner().create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.native_sum.inner().create_sliding_accumulator(args)
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.native_sum.inner().reverse_expr()
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.native_sum.inner().order_sensitivity()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.native_sum.inner().documentation()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.native_sum.inner().set_monotonicity(data_type)
    }

    fn simplify_expr_op_literal(
        &self,
        aggregate: &AggregateFunction,
        arg: &Expr,
        op: Operator,
        literal: &Expr,
        arg_is_left: bool,
    ) -> Result<Option<Expr>> {
        self.native_sum
            .inner()
            .simplify_expr_op_literal(aggregate, arg, op, literal, arg_is_left)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema};

    #[tokio::test]
    async fn native_and_checked_sum_keep_distinct_names() {
        let ctx = SessionContext::new();
        register_all(&ctx);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();

        let batches = ctx
            .sql("SELECT SUM(x), CHECKED_LONG_SUM(x) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let result = &batches[0];

        assert_eq!(result.schema().field(0).name(), "sum(t.x)");
        assert_eq!(result.schema().field(1).name(), "checked_long_sum(t.x)");
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            6
        );
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            6
        );
    }
}
