/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Custom `approx_distinct` UDAF bringing unreleased fixes from DataFusion main
//! as an intermediate solution for two issues in DF 54:
//!
//! 1. Utf8View over-counting: https://github.com/apache/datafusion/pull/21064 introduced
//!    inconsistent hashing (inline u128 vs &str) causing ~2x overcount on keyword fields.
//!    Fixed upstream in https://github.com/apache/datafusion/pull/22815.
//!
//! 2. Boolean not supported: `approx_distinct(boolean)` errors with "not implemented".
//!    Added upstream in https://github.com/apache/datafusion/pull/22707 with short-circuit.
//!
//! This UDAF overrides DF54's built-in `approx_distinct` for these two types and delegates
//! all others to the built-in.
//! TODO: Remove this file when upgrading to DataFusion 55+.

use std::hash::BuildHasher;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{downcast_value, internal_datafusion_err, Result};
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::expressions::format_state_name;
use datafusion::scalar::ScalarValue;

use super::hll::{HyperLogLog, HLL_HASH_STATE};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(datafusion::logical_expr::AggregateUDF::from(
        SafeApproxDistinct::new(),
    ));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SafeApproxDistinct {
    signature: Signature,
}

impl SafeApproxDistinct {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SafeApproxDistinct {
    fn name(&self) -> &str {
        "approx_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        approx_distinct_udaf().inner().state_fields(args)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = acc_args.expr_fields[0].data_type();
        match data_type {
            DataType::Utf8View => Ok(Box::new(Utf8ViewHLLAccumulator::new())),
            _ => approx_distinct_udaf().inner().accumulator(acc_args),
        }
    }

    fn documentation(&self) -> Option<&datafusion::logical_expr::Documentation> {
        None
    }
}

// ── Utf8View HLL: consistent per-string-length hashing (matches DF main #22815) ──

#[derive(Debug)]
struct Utf8ViewHLLAccumulator {
    hll: HyperLogLog<u8>,
}

impl Utf8ViewHLLAccumulator {
    fn new() -> Self {
        Self { hll: HyperLogLog::new() }
    }
}

impl Accumulator for Utf8ViewHLLAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array: &StringViewArray = downcast_value!(values[0], StringViewArray);
        if array.data_buffers().is_empty() {
            for (i, &view) in array.views().iter().enumerate() {
                if !array.is_null(i) {
                    self.hll.add_hashed(HLL_HASH_STATE.hash_one(view));
                }
            }
        } else {
            for (i, &view) in array.views().iter().enumerate() {
                if array.is_null(i) {
                    continue;
                }
                if (view as u32) <= 12 {
                    self.hll.add_hashed(HLL_HASH_STATE.hash_one(view));
                } else {
                    self.hll.add_hashed(HLL_HASH_STATE.hash_one(array.value(i)));
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let binary_array = downcast_value!(states[0], BinaryArray);
        for v in binary_array.iter() {
            let v = v.ok_or_else(|| {
                internal_datafusion_err!("Invalid HLL binary state")
            })?;
            let other: HyperLogLog<u8> = v.try_into()?;
            self.hll.merge(&other);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.hll.to_scalar()])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.hll.count() as u64)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

// ── Boolean accumulator with short-circuit ──────────────────────────────────

#[derive(Debug)]
struct BooleanDistinctAccumulator {
    seen_true: bool,
    seen_false: bool,
}

impl BooleanDistinctAccumulator {
    fn new() -> Self {
        Self {
            seen_true: false,
            seen_false: false,
        }
    }
}

impl Accumulator for BooleanDistinctAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.seen_true && self.seen_false {
            return Ok(());
        }
        let array: &BooleanArray = downcast_value!(values[0], BooleanArray);
        for i in 0..array.len() {
            if !array.is_null(i) {
                if array.value(i) {
                    self.seen_true = true;
                } else {
                    self.seen_false = true;
                }
                if self.seen_true && self.seen_false {
                    break;
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if self.seen_true && self.seen_false {
            return Ok(());
        }
        let binary_array: &BinaryArray = downcast_value!(states[0], BinaryArray);
        for v in binary_array.iter().flatten() {
            if v.len() >= 2 {
                self.seen_true |= v[0] != 0;
                self.seen_false |= v[1] != 0;
            }
            if self.seen_true && self.seen_false {
                break;
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(vec![
            self.seen_true as u8,
            self.seen_false as u8,
        ]))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(
            self.seen_true as u64 + self.seen_false as u64,
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::BooleanArray;

    #[test]
    fn boolean_acc_both_values() {
        let mut acc = BooleanDistinctAccumulator::new();
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true, false]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::UInt64(Some(2)));
    }

    #[test]
    fn boolean_acc_only_true() {
        let mut acc = BooleanDistinctAccumulator::new();
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, true]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::UInt64(Some(1)));
    }

    #[test]
    fn boolean_acc_only_false() {
        let mut acc = BooleanDistinctAccumulator::new();
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![false, false]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::UInt64(Some(1)));
    }

    #[test]
    fn boolean_acc_with_nulls() {
        let mut acc = BooleanDistinctAccumulator::new();
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true), None, Some(false), None,
        ]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::UInt64(Some(2)));
    }

    #[test]
    fn boolean_acc_short_circuits() {
        let mut acc = BooleanDistinctAccumulator::new();
        let arr1: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        acc.update_batch(&[arr1]).unwrap();
        // Second batch should be a no-op
        let arr2: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, true]));
        acc.update_batch(&[arr2]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::UInt64(Some(2)));
    }

    #[test]
    fn boolean_acc_empty() {
        let mut acc = BooleanDistinctAccumulator::new();
        let arr: ArrayRef = Arc::new(BooleanArray::from(Vec::<bool>::new()));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::UInt64(Some(0)));
    }

    #[test]
    fn utf8view_consistency() {
        use datafusion::arrow::array::StringViewArray;
        let udaf = SafeApproxDistinct::new();
        let field = Arc::new(Field::new("x", DataType::Utf8View, true));
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![field.as_ref().clone()]));
        let expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("x", 0));
        let acc_args = AccumulatorArgs {
            return_field: Arc::new(Field::new("r", DataType::UInt64, true)),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            name: "x",
            is_distinct: false,
            exprs: &[expr],
            expr_fields: &[Arc::new(Field::new("x", DataType::Utf8View, true))],
            is_reversed: false,
        };
        let mut acc = udaf.accumulator(acc_args).unwrap();

        // Two batches with same values — should produce same result as one batch
        let arr1: ArrayRef = Arc::new(StringViewArray::from(vec!["hello", "world", "hello"]));
        let arr2: ArrayRef = Arc::new(StringViewArray::from(vec!["world", "hello", "world"]));
        acc.update_batch(&[arr1]).unwrap();
        acc.update_batch(&[arr2]).unwrap();

        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::UInt64(Some(2)));
    }

    #[test]
    fn utf8view_state_roundtrip() {
        let mut acc = Utf8ViewHLLAccumulator::new();
        let arr: ArrayRef = Arc::new(StringViewArray::from(vec!["alpha", "beta", "gamma"]));
        acc.update_batch(&[arr]).unwrap();

        let state = acc.state().unwrap();
        assert_eq!(state.len(), 1);
        let ScalarValue::Binary(Some(bytes)) = &state[0] else { panic!("expected Binary state") };

        let mut acc2 = Utf8ViewHLLAccumulator::new();
        let binary_arr: ArrayRef = Arc::new(
            datafusion::arrow::array::BinaryArray::from(vec![bytes.as_slice()])
        );
        acc2.merge_batch(&[binary_arr]).unwrap();
        assert_eq!(acc2.evaluate().unwrap(), ScalarValue::UInt64(Some(3)));
    }

    #[test]
    fn utf8view_mixed_batches_consistent() {
        // Batch 1: has a long string (forces data_buffers non-empty)
        let mut builder = arrow::array::StringViewBuilder::new();
        builder.append_value("this_is_a_long_string_over_12_bytes");
        builder.append_value("short");
        builder.append_value("tiny");
        let batch1: ArrayRef = Arc::new(builder.finish());

        // Batch 2: only short strings (data_buffers empty, all inline)
        let batch2: ArrayRef = Arc::new(StringViewArray::from(vec!["short", "tiny", "new"]));

        let mut acc = Utf8ViewHLLAccumulator::new();
        acc.update_batch(&[batch1]).unwrap();
        acc.update_batch(&[batch2]).unwrap();

        // 4 distinct: "this_is_a_long...", "short", "tiny", "new"
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::UInt64(Some(4)));
    }

    #[test]
    fn utf8view_merge_two_shards() {
        let mut acc1 = Utf8ViewHLLAccumulator::new();
        let mut acc2 = Utf8ViewHLLAccumulator::new();

        let arr1: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b", "c"]));
        let arr2: ArrayRef = Arc::new(StringViewArray::from(vec!["b", "c", "d"]));
        acc1.update_batch(&[arr1]).unwrap();
        acc2.update_batch(&[arr2]).unwrap();

        let state1 = acc1.state().unwrap();
        let state2 = acc2.state().unwrap();
        let ScalarValue::Binary(Some(b1)) = &state1[0] else { panic!() };
        let ScalarValue::Binary(Some(b2)) = &state2[0] else { panic!() };

        let mut merged = Utf8ViewHLLAccumulator::new();
        let binary_arr: ArrayRef = Arc::new(
            datafusion::arrow::array::BinaryArray::from(vec![b1.as_slice(), b2.as_slice()])
        );
        merged.merge_batch(&[binary_arr]).unwrap();
        // Union of {a,b,c} and {b,c,d} = {a,b,c,d} = 4
        assert_eq!(merged.evaluate().unwrap(), ScalarValue::UInt64(Some(4)));
    }

    #[test]
    fn delegates_int32_to_builtin() {
        let udaf = SafeApproxDistinct::new();
        let field = Arc::new(Field::new("x", DataType::Int32, true));
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![field.as_ref().clone()]));
        let expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new("x", 0));
        let acc_args = AccumulatorArgs {
            return_field: Arc::new(Field::new("r", DataType::UInt64, true)),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            name: "x",
            is_distinct: false,
            exprs: &[expr],
            expr_fields: &[field.clone()],
            is_reversed: false,
        };
        let mut acc = udaf.accumulator(acc_args).unwrap();
        let arr: ArrayRef = Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3, 2, 1]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::UInt64(Some(3)));
    }
}
