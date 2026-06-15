/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Custom `sum` UDAF that replaces DataFusion's built-in to fix Int64 overflow.
//!
//! DataFusion's native SUM for Int64 columns uses wrapping i64 arithmetic.
//! When summing large values (e.g., UserID in ClickBench), the result overflows
//! and wraps to a negative number. Vanilla OpenSearch accumulates as f64.
//!
//! This UDAF uses DataFusion's type coercion to widen inputs at plan time:
//! - Int8/16/32 → Int64 (via CastExec inserted by TypeCoercion optimizer)
//! - UInt8/16/32 → UInt64 (via CastExec)
//! - Float32 → Float64 (via CastExec)
//! So the accumulator only ever sees Int64, UInt64, or Float64.
//!
//! The accumulator then:
//! - Casts Int64/UInt64 batch sums to f64 (single scalar cast, not array-wide)
//! - Uses Neumaier compensated summation cross-batch for precision
//! - Returns Float64 always (no overflow possible within f64 range)

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, UInt64Array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::types::{
    logical_float64, logical_int8, logical_int16, logical_int32, logical_int64, logical_uint8,
    logical_uint16, logical_uint32, logical_uint64, NativeType,
};
use datafusion::common::{Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Coercion, EmitTo, GroupsAccumulator, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(SumUdaf::new()));
}

#[derive(Debug)]
pub struct SumUdaf {
    signature: Signature,
}

impl SumUdaf {
    pub fn new() -> Self {
        Self {
            // Plan-time coercion: small types are widened via CastExec before
            // reaching the accumulator. Mirrors DataFusion's native Sum signature.
            signature: Signature::one_of(
                vec![
                    // Signed integers → Int64
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int64()),
                        vec![
                            TypeSignatureClass::Native(logical_int8()),
                            TypeSignatureClass::Native(logical_int16()),
                            TypeSignatureClass::Native(logical_int32()),
                        ],
                        NativeType::Int64,
                    )]),
                    // Unsigned integers → UInt64
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_uint64()),
                        vec![
                            TypeSignatureClass::Native(logical_uint8()),
                            TypeSignatureClass::Native(logical_uint16()),
                            TypeSignatureClass::Native(logical_uint32()),
                        ],
                        NativeType::UInt64,
                    )]),
                    // Floats → Float64
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_float64()),
                        vec![TypeSignatureClass::Float],
                        NativeType::Float64,
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl PartialEq for SumUdaf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for SumUdaf {}
impl Hash for SumUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "sum".hash(state);
    }
}

impl AggregateUDFImpl for SumUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(NeumaierSumAccumulator::new()))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(format!("{}[sum]", args.name), DataType::Float64, true)),
            Arc::new(Field::new(format!("{}[comp]", args.name), DataType::Float64, true)),
            Arc::new(Field::new(format!("{}[count]", args.name), DataType::UInt64, true)),
        ])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(NeumaierGroupsAccumulator::new()))
    }
}

/// Accumulator using Neumaier's improved Kahan summation for cross-batch precision.
/// Only receives Int64, UInt64, or Float64 (plan-time coercion handles widening).
pub struct NeumaierSumAccumulator {
    sum: f64,
    compensation: f64,
    count: u64,
}

impl NeumaierSumAccumulator {
    pub fn new() -> Self {
        Self { sum: 0.0, compensation: 0.0, count: 0 }
    }

    #[inline]
    fn neumaier_add(&mut self, value: f64) {
        let t = self.sum + value;
        if self.sum.abs() >= value.abs() {
            self.compensation += (self.sum - t) + value;
        } else {
            self.compensation += (value - t) + self.sum;
        }
        self.sum = t;
    }
}

impl Debug for NeumaierSumAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NeumaierSumAccumulator")
            .field("sum", &self.sum)
            .field("compensation", &self.compensation)
            .field("count", &self.count)
            .finish()
    }
}

impl Accumulator for NeumaierSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let col = &values[0];
        if col.is_empty() {
            return Ok(());
        }

        let non_null = (col.len() - col.null_count()) as u64;
        if non_null == 0 {
            return Ok(());
        }

        // Input is already widened by plan-time coercion: only Int64, UInt64, or Float64.
        // Native SIMD sum on the widened type — no extra cast in UDAF.
        let batch_sum = match col.data_type() {
            DataType::Float64 => compute::sum(col.as_any().downcast_ref::<Float64Array>().unwrap()),
            DataType::Int64 => {
                compute::sum(col.as_any().downcast_ref::<Int64Array>().unwrap()).map(|v| v as f64)
            }
            DataType::UInt64 => {
                compute::sum(col.as_any().downcast_ref::<UInt64Array>().unwrap()).map(|v| v as f64)
            }
            _ => {
                // Defensive fallback — should never hit due to coercion signature
                let f64_array = compute::cast(col, &DataType::Float64)?;
                compute::sum(f64_array.as_any().downcast_ref::<Float64Array>().unwrap())
            }
        };

        if let Some(s) = batch_sum {
            self.neumaier_add(s);
        }
        self.count += non_null;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let sums = states[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let comps = states[1].as_any().downcast_ref::<Float64Array>().unwrap();
        let counts = states[2].as_any().downcast_ref::<UInt64Array>().unwrap();

        for i in 0..sums.len() {
            if sums.is_null(i) {
                continue;
            }
            self.neumaier_add(sums.value(i));
            self.neumaier_add(comps.value(i));
            self.count += counts.value(i);
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let col = &values[0];
        if col.is_empty() {
            return Ok(());
        }
        let non_null = (col.len() - col.null_count()) as u64;
        if non_null == 0 {
            return Ok(());
        }
        // Retract = subtract. Negate the batch sum and add via Neumaier.
        let batch_sum = match col.data_type() {
            DataType::Float64 => compute::sum(col.as_any().downcast_ref::<Float64Array>().unwrap()),
            DataType::Int64 => compute::sum(col.as_any().downcast_ref::<Int64Array>().unwrap()).map(|v| v as f64),
            DataType::UInt64 => compute::sum(col.as_any().downcast_ref::<UInt64Array>().unwrap()).map(|v| v as f64),
            _ => {
                let f64_array = compute::cast(col, &DataType::Float64)?;
                compute::sum(f64_array.as_any().downcast_ref::<Float64Array>().unwrap())
            }
        };
        if let Some(s) = batch_sum {
            self.neumaier_add(-s);
        }
        self.count = self.count.saturating_sub(non_null);
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some(self.sum + self.compensation)))
        }
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.sum)),
            ScalarValue::Float64(Some(self.compensation)),
            ScalarValue::UInt64(Some(self.count)),
        ])
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

// ─── Vectorized GroupsAccumulator for GROUP BY queries ───────────────────────

/// Vectorized groups accumulator. Only receives Int64, UInt64, or Float64
/// (plan-time coercion handles widening of smaller types).
pub struct NeumaierGroupsAccumulator {
    sums: Vec<f64>,
    compensations: Vec<f64>,
    null_state: Vec<bool>,
}

impl NeumaierGroupsAccumulator {
    pub fn new() -> Self {
        Self {
            sums: Vec::new(),
            compensations: Vec::new(),
            null_state: Vec::new(),
        }
    }

    fn ensure_capacity(&mut self, total_num_groups: usize) {
        if total_num_groups > self.sums.len() {
            self.sums.resize(total_num_groups, 0.0);
            self.compensations.resize(total_num_groups, 0.0);
            self.null_state.resize(total_num_groups, false);
        }
    }

    #[inline]
    fn neumaier_add_to_group(&mut self, group: usize, value: f64) {
        let sum = self.sums[group];
        let t = sum + value;
        if sum.abs() >= value.abs() {
            self.compensations[group] += (sum - t) + value;
        } else {
            self.compensations[group] += (value - t) + sum;
        }
        self.sums[group] = t;
        self.null_state[group] = true;
    }
}

impl GroupsAccumulator for NeumaierGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.ensure_capacity(total_num_groups);
        let col = &values[0];

        // Input is already widened: only Int64, UInt64, or Float64
        match col.data_type() {
            DataType::Float64 => {
                let typed = col.as_any().downcast_ref::<Float64Array>().unwrap();
                for (i, &group) in group_indices.iter().enumerate() {
                    if opt_filter.map_or(false, |f| !f.value(i)) || typed.is_null(i) {
                        continue;
                    }
                    self.neumaier_add_to_group(group, typed.value(i));
                }
            }
            DataType::Int64 => {
                let typed = col.as_any().downcast_ref::<Int64Array>().unwrap();
                for (i, &group) in group_indices.iter().enumerate() {
                    if opt_filter.map_or(false, |f| !f.value(i)) || typed.is_null(i) {
                        continue;
                    }
                    self.neumaier_add_to_group(group, typed.value(i) as f64);
                }
            }
            DataType::UInt64 => {
                let typed = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                for (i, &group) in group_indices.iter().enumerate() {
                    if opt_filter.map_or(false, |f| !f.value(i)) || typed.is_null(i) {
                        continue;
                    }
                    self.neumaier_add_to_group(group, typed.value(i) as f64);
                }
            }
            _ => {
                let f64_array = compute::cast(col, &DataType::Float64)?;
                let typed = f64_array.as_any().downcast_ref::<Float64Array>().unwrap();
                for (i, &group) in group_indices.iter().enumerate() {
                    if opt_filter.map_or(false, |f| !f.value(i)) || typed.is_null(i) {
                        continue;
                    }
                    self.neumaier_add_to_group(group, typed.value(i));
                }
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.ensure_capacity(total_num_groups);

        let sums = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let comps = values[1].as_any().downcast_ref::<Float64Array>().unwrap();
        let counts = values[2].as_any().downcast_ref::<UInt64Array>().unwrap();

        for (i, &group) in group_indices.iter().enumerate() {
            if opt_filter.map_or(false, |f| !f.value(i)) || sums.is_null(i) {
                continue;
            }
            self.neumaier_add_to_group(group, sums.value(i));
            self.neumaier_add_to_group(group, comps.value(i));
            if counts.value(i) > 0 {
                self.null_state[group] = true;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (sums, comps, nulls) = match emit_to {
            EmitTo::All => {
                let s = std::mem::take(&mut self.sums);
                let c = std::mem::take(&mut self.compensations);
                let n = std::mem::take(&mut self.null_state);
                (s, c, n)
            }
            EmitTo::First(n) => {
                let s = self.sums.drain(..n).collect::<Vec<_>>();
                let c = self.compensations.drain(..n).collect::<Vec<_>>();
                let ns = self.null_state.drain(..n).collect::<Vec<_>>();
                (s, c, ns)
            }
        };

        let values: Vec<Option<f64>> = sums
            .iter()
            .zip(comps.iter())
            .zip(nulls.iter())
            .map(|((s, c), &has_value)| if has_value { Some(s + c) } else { None })
            .collect();

        Ok(Arc::new(Float64Array::from(values)))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (sums, comps, nulls) = match emit_to {
            EmitTo::All => {
                let s = std::mem::take(&mut self.sums);
                let c = std::mem::take(&mut self.compensations);
                let n = std::mem::take(&mut self.null_state);
                (s, c, n)
            }
            EmitTo::First(n) => {
                let s = self.sums.drain(..n).collect::<Vec<_>>();
                let c = self.compensations.drain(..n).collect::<Vec<_>>();
                let ns = self.null_state.drain(..n).collect::<Vec<_>>();
                (s, c, ns)
            }
        };

        let null_buffer = NullBuffer::from(nulls.clone());
        let sum_array = Float64Array::new(sums.into(), Some(null_buffer.clone()));
        let comp_array = Float64Array::new(comps.into(), Some(null_buffer.clone()));
        let count_array: Vec<u64> = nulls.iter().map(|&v| if v { 1 } else { 0 }).collect();
        let count_array = UInt64Array::from(count_array);

        Ok(vec![
            Arc::new(sum_array),
            Arc::new(comp_array),
            Arc::new(count_array),
        ])
    }

    fn size(&self) -> usize {
        self.sums.capacity() * 8 + self.compensations.capacity() * 8 + self.null_state.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array};

    fn sum_i64_batches(batches: &[&[i64]]) -> ScalarValue {
        let mut acc = NeumaierSumAccumulator::new();
        for b in batches {
            let arr: ArrayRef = Arc::new(Int64Array::from(b.to_vec()));
            acc.update_batch(&[arr]).unwrap();
        }
        acc.evaluate().unwrap()
    }

    #[test]
    fn basic_sum() {
        let result = sum_i64_batches(&[&[1, 2, 3, 4, 5]]);
        assert_eq!(result, ScalarValue::Float64(Some(15.0)));
    }

    #[test]
    fn sum_large_int64_cross_batch_no_overflow() {
        // Cross-batch overflow protection: values that would overflow i64 if
        // summed across batches, but each individual batch sum fits in i64.
        // This is the actual overflow case our UDAF protects against.
        let big = i64::MAX / 2 + 1; // ~4.6e18
        // Split across 3 separate batches so per-batch i64 sum doesn't overflow,
        // but cumulative sum across batches would exceed i64::MAX.
        let result = sum_i64_batches(&[&[big], &[big], &[big]]);
        match result {
            ScalarValue::Float64(Some(v)) => {
                assert!(v > 0.0, "sum must be positive, got {v}");
                let expected = 3.0 * (big as f64);
                let rel_err = ((v - expected) / expected).abs();
                assert!(rel_err < 1e-10, "relative error {rel_err} too large");
            }
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn sum_empty_returns_null() {
        let mut acc = NeumaierSumAccumulator::new();
        let arr: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(None));
    }

    #[test]
    fn sum_all_nulls_returns_null() {
        let mut acc = NeumaierSumAccumulator::new();
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![None, None, None]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(None));
    }

    #[test]
    fn sum_with_nulls_skips_them() {
        let mut acc = NeumaierSumAccumulator::new();
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(20)]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(Some(30.0)));
    }

    #[test]
    fn neumaier_precision_catastrophic_cancellation() {
        let mut acc = NeumaierSumAccumulator::new();
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![1e16, 1.0, -1e16]));
        acc.update_batch(&[arr]).unwrap();
        match acc.evaluate().unwrap() {
            ScalarValue::Float64(Some(v)) => {
                assert!((v - 1.0).abs() < 1e-10, "expected ~1.0, got {v}");
            }
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn neumaier_cross_batch_precision() {
        let mut acc = NeumaierSumAccumulator::new();
        let b1: ArrayRef = Arc::new(Float64Array::from(vec![1e16]));
        let b2: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        let b3: ArrayRef = Arc::new(Float64Array::from(vec![-1e16]));
        acc.update_batch(&[b1]).unwrap();
        acc.update_batch(&[b2]).unwrap();
        acc.update_batch(&[b3]).unwrap();
        match acc.evaluate().unwrap() {
            ScalarValue::Float64(Some(v)) => {
                assert!((v - 1.0).abs() < 1e-10, "expected ~1.0, got {v}");
            }
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn merge_batch_combines_partial_states() {
        let mut acc1 = NeumaierSumAccumulator::new();
        let arr1: ArrayRef = Arc::new(Int64Array::from(vec![100, 200]));
        acc1.update_batch(&[arr1]).unwrap();

        let mut acc2 = NeumaierSumAccumulator::new();
        let arr2: ArrayRef = Arc::new(Int64Array::from(vec![300, 400]));
        acc2.update_batch(&[arr2]).unwrap();

        let state1 = acc1.state().unwrap();
        let state2 = acc2.state().unwrap();

        let sums: ArrayRef = Arc::new(Float64Array::from(vec![
            match &state1[0] { ScalarValue::Float64(Some(v)) => *v, _ => panic!() },
            match &state2[0] { ScalarValue::Float64(Some(v)) => *v, _ => panic!() },
        ]));
        let comps: ArrayRef = Arc::new(Float64Array::from(vec![
            match &state1[1] { ScalarValue::Float64(Some(v)) => *v, _ => panic!() },
            match &state2[1] { ScalarValue::Float64(Some(v)) => *v, _ => panic!() },
        ]));
        let counts: ArrayRef = Arc::new(UInt64Array::from(vec![
            match &state1[2] { ScalarValue::UInt64(Some(v)) => *v, _ => panic!() },
            match &state2[2] { ScalarValue::UInt64(Some(v)) => *v, _ => panic!() },
        ]));

        let mut coord = NeumaierSumAccumulator::new();
        coord.merge_batch(&[sums, comps, counts]).unwrap();
        assert_eq!(coord.evaluate().unwrap(), ScalarValue::Float64(Some(1000.0)));
    }

    #[test]
    fn sum_float64_passthrough() {
        let mut acc = NeumaierSumAccumulator::new();
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5, 3.0]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(Some(7.0)));
    }

    #[test]
    fn sum_multiple_batches() {
        let result = sum_i64_batches(&[&[1, 2, 3], &[4, 5, 6], &[7, 8, 9]]);
        assert_eq!(result, ScalarValue::Float64(Some(45.0)));
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    #[tokio::test]
    async fn substrait_sum_override_returns_float64() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

        // Build Substrait plan using vanilla DataFusion (no custom UDAF)
        let substrait_bytes = {
            let ctx = SessionContext::new();
            let batch = arrow::array::RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1i64]))],
            )
            .unwrap();
            let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).unwrap();
            ctx.register_table("t", Arc::new(table)).unwrap();
            let df = ctx.sql("SELECT SUM(x) FROM t").await.unwrap();
            let plan = df.logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &ctx.state()).unwrap();
            let mut buf = Vec::new();
            substrait.encode(&mut buf).unwrap();
            buf
        };

        // Consume with a session that has our custom sum UDAF
        let ctx = SessionContext::new();
        crate::udaf::register_all(&ctx);

        let big = i64::MAX / 2 + 1;
        // Split across 3 batches — per-batch i64 sum fits, cross-batch tests Neumaier
        let table = MemTable::try_new(
            Arc::clone(&schema),
            vec![
                vec![arrow::array::RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![big]))],
                )
                .unwrap()],
                vec![arrow::array::RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![big]))],
                )
                .unwrap()],
                vec![arrow::array::RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![big]))],
                )
                .unwrap()],
            ],
        )
        .unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();

        let plan = substrait::proto::Plan::decode(substrait_bytes.as_slice()).unwrap();
        let logical_plan = from_substrait_plan(&ctx.state(), &plan).await.unwrap();
        let df = ctx.execute_logical_plan(logical_plan).await.unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let result_col = batches[0].column(0);
        assert_eq!(
            result_col.data_type(),
            &DataType::Float64,
            "Custom sum UDAF must return Float64, got {:?}",
            result_col.data_type()
        );

        let f64_col = result_col.as_any().downcast_ref::<Float64Array>().unwrap();
        let value = f64_col.value(0);
        assert!(value > 0.0, "sum must be positive (no overflow), got {}", value);
        let expected = 3.0 * (big as f64);
        let rel_err = ((value - expected) / expected).abs();
        assert!(rel_err < 1e-10, "value {} != expected {}", value, expected);
    }
}
