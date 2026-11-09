/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `take(field, [n])` — bounded array of the first `n` values per group in
//! arrival order.
//!
//! Mirrors PPL's [`TakeAggFunction`] semantics:
//!
//! * `n > 0`: append values in scan order until the buffer holds `n` items.
//! * `n <= 0`: never append — returns an empty list.
//! * Default `n` (when only one argument supplied): `10`.
//! * NULL elements are appended like any other value.
//!
//! Distributed correctness: each per-shard accumulator emits its bounded buffer
//! as a `List<element>` scalar via `state()`, the coordinator's final
//! accumulator concatenates incoming lists in `merge_batch()` and trims back to
//! `n`. Cross-shard ordering is non-deterministic — same property as the Java
//! implementation, which assumes single-accumulator scan order.
//!
//! Calcite often materialises the literal `n` as a `Project` column rather than
//! passing it as a Substrait literal. When `accumulator()` only sees a column
//! reference (not a `Literal`), `limit_from_args` returns `None` and the
//! accumulator resolves the limit on the first `update_batch` call from row 0
//! of that column (every row carries the same constant value).
//!
//! [`TakeAggFunction`]: https://github.com/opensearch-project/sql/blob/main/core/src/main/java/org/opensearch/sql/calcite/udf/udaf/TakeAggFunction.java

use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;

const DEFAULT_LIMIT: i64 = 10;

/// If `dt` is `List<E>` / `LargeList<E>` / `FixedSizeList<E>`, return `E`;
/// otherwise return `dt` unchanged. Used to normalize PARTIAL (element column)
/// and FINAL (list-state column) inputs to the same element type.
fn inner_element_type(dt: &DataType) -> DataType {
    match dt {
        DataType::List(field) | DataType::LargeList(field) => field.data_type().clone(),
        DataType::FixedSizeList(field, _) => field.data_type().clone(),
        other => other.clone(),
    }
}

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(TakeUdaf::new()));
}

/// `take(value, [n])` aggregate UDF.
#[derive(Debug)]
pub struct TakeUdaf {
    signature: Signature,
}

impl TakeUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for TakeUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for TakeUdaf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for TakeUdaf {}
impl Hash for TakeUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "take".hash(state);
    }
}

impl AggregateUDFImpl for TakeUdaf {
    fn name(&self) -> &str {
        "take"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("take() requires at least one argument");
        }
        // Two call shapes:
        //   PARTIAL: take(value, n)     — arg 0 is the element type
        //   FINAL  : take(list_state)   — arg 0 is List<element>
        // Both return List<element>, so peel one level of List off arg 0 if present.
        let element = inner_element_type(&arg_types[0]);
        Ok(DataType::List(Arc::new(Field::new("item", element, true))))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let raw_arg0 = acc_args
            .exprs
            .first()
            .map(|e| e.data_type(acc_args.schema))
            .transpose()?
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "take(): missing first argument".to_string(),
                )
            })?;
        // The accumulator stores individual elements. For PARTIAL `take(value, n)` arg 0
        // is the raw element column and we accumulate values directly. For FINAL
        // `take(state)` arg 0 is itself a `List<elem>` carrying each shard's bounded
        // buffer — `update_batch` must un-nest each list-row into elements rather than
        // pushing the whole list as a single value.
        let arg0_is_list = matches!(
            raw_arg0,
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _),
        );
        let element_type = inner_element_type(&raw_arg0);
        let limit = limit_from_args(&acc_args)?;
        Ok(Box::new(TakeAccumulator::new(
            element_type,
            limit,
            arg0_is_list,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Use the function's own return_field — it is `List<elem>` in both
        // the PARTIAL (`take(value, n) -> List<elem>`) and FINAL
        // (`take(state) -> List<elem>`) call shapes, so the per-shard state
        // schema matches what the coordinator's `merge_batch` consumes.
        Ok(vec![Arc::new(Field::new(
            format!("{}[buf]", args.name),
            args.return_field.data_type().clone(),
            true,
        ))])
    }
}

/// Try to extract the integer limit from the aggregate's second physical
/// expression. Returns:
/// * `Some(DEFAULT_LIMIT)` when the aggregate has only one argument.
/// * `Some(n)` when the second argument is a Substrait literal.
/// * `None` when the second argument is a column reference (Calcite often
///   materializes the literal `n` as a `Project` column instead of inlining it
///   into the aggregate). The accumulator resolves the limit on the first
///   `update_batch` call by reading row 0 of that column.
fn limit_from_args(acc_args: &AccumulatorArgs) -> Result<Option<i64>> {
    let Some(expr) = acc_args.exprs.get(1) else {
        return Ok(Some(DEFAULT_LIMIT));
    };
    if let Some(lit) = expr.downcast_ref::<Literal>() {
        return scalar_to_i64(lit.value()).map(Some);
    }
    Ok(None)
}

fn scalar_to_i64(scalar: &ScalarValue) -> Result<i64> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Ok(*v as i64),
        ScalarValue::Int16(Some(v)) => Ok(*v as i64),
        ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::UInt8(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt16(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt64(Some(v)) => Ok(*v as i64),
        ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None) => Ok(DEFAULT_LIMIT),
        other => exec_err!("take(): n must be an integer, got {other:?}"),
    }
}

/// Bounded buffer of `ScalarValue`s. Capped at `limit`; further appends are
/// no-ops. `limit <= 0` → never accept anything → empty result list.
///
/// `limit` is `Option` because Calcite often materializes the literal `n` as a
/// `Project` column rather than passing it as a Substrait literal — at
/// accumulator construction time we may only see a column reference, not a
/// literal value. In that case we resolve the limit on the first
/// `update_batch` call from row 0 of the second column (every row carries the
/// same constant value).
pub struct TakeAccumulator {
    element_type: DataType,
    limit: Option<i64>,
    buf: Vec<ScalarValue>,
    /// True when arg 0 is a `List<elem>` column (the FINAL `take(state)` call shape) —
    /// `update_batch` un-nests each list-row into individual elements before
    /// appending. False for the PARTIAL `take(value, n)` shape where arg 0 is the
    /// element column directly.
    arg0_is_list: bool,
}

impl TakeAccumulator {
    pub fn new(element_type: DataType, limit: Option<i64>, arg0_is_list: bool) -> Self {
        let cap = limit.filter(|&l| l > 0).map(|l| l as usize).unwrap_or(0);
        Self {
            element_type,
            limit,
            buf: Vec::with_capacity(cap.min(1024)),
            arg0_is_list,
        }
    }

    fn cap(&self) -> usize {
        match self.limit {
            Some(l) if l > 0 => l as usize,
            _ => 0,
        }
    }

    /// If the limit is still unknown, try to read it from row 0 of `n_col`.
    /// The Calcite plan materializes the literal `n` as a constant column —
    /// every row carries the same value, so reading row 0 is sufficient.
    fn resolve_limit_from(&mut self, n_col: &ArrayRef) -> Result<()> {
        if self.limit.is_some() || n_col.len() == 0 {
            return Ok(());
        }
        let scalar = ScalarValue::try_from_array(n_col, 0)?;
        self.limit = Some(scalar_to_i64(&scalar)?);
        Ok(())
    }

    fn current_list(&self) -> ScalarValue {
        let arr = ScalarValue::new_list_nullable(&self.buf, &self.element_type);
        ScalarValue::List(arr)
    }
}

impl Debug for TakeAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TakeAccumulator")
            .field("limit", &self.limit)
            .field("len", &self.buf.len())
            .finish()
    }
}

impl Accumulator for TakeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(n_col) = values.get(1) {
            self.resolve_limit_from(n_col)?;
        }
        let cap = self.cap();
        if cap == 0 {
            return Ok(());
        }
        let col = &values[0];
        if self.arg0_is_list {
            // FINAL form: each row of `col` is itself a List of pre-capped elements
            // emitted by a per-shard PARTIAL. Un-nest into the buffer up to `cap`,
            // identical semantics to `merge_batch` — DataFusion's substrait consumer
            // ignores aggregation phase, so coordinator-side reduction shows up here.
            let lists: &ListArray = col.as_list();
            for i in 0..lists.len() {
                if self.buf.len() >= cap {
                    return Ok(());
                }
                if lists.is_null(i) {
                    continue;
                }
                let inner = lists.value(i);
                for j in 0..inner.len() {
                    if self.buf.len() >= cap {
                        return Ok(());
                    }
                    self.buf.push(ScalarValue::try_from_array(&inner, j)?);
                }
            }
        } else {
            let mut i = 0;
            while self.buf.len() < cap && i < col.len() {
                self.buf.push(ScalarValue::try_from_array(col, i)?);
                i += 1;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.current_list())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.buf.iter().map(|s| s.size()).sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.current_list()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let cap = self.cap();
        if cap == 0 {
            return Ok(());
        }
        let lists: &ListArray = states[0].as_list();
        for i in 0..lists.len() {
            if self.buf.len() >= cap {
                return Ok(());
            }
            if lists.is_null(i) {
                continue;
            }
            let inner = lists.value(i);
            for j in 0..inner.len() {
                if self.buf.len() >= cap {
                    return Ok(());
                }
                self.buf.push(ScalarValue::try_from_array(&inner, j)?);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};

    fn run_int_batches(limit: Option<i64>, batches: &[&[Option<i32>]]) -> Vec<Option<i32>> {
        let mut acc = TakeAccumulator::new(DataType::Int32, limit, false);
        for b in batches {
            let arr: ArrayRef = Arc::new(Int32Array::from_iter(b.iter().copied()));
            acc.update_batch(&[arr]).unwrap();
        }
        match acc.evaluate().unwrap() {
            ScalarValue::List(l) => {
                let inner = l.value(0);
                let typed = inner.as_any().downcast_ref::<Int32Array>().unwrap();
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            None
                        } else {
                            Some(typed.value(i))
                        }
                    })
                    .collect()
            }
            other => panic!("expected list scalar, got {other:?}"),
        }
    }

    #[test]
    fn returns_first_n_values_in_arrival_order() {
        let out = run_int_batches(Some(3), &[&[Some(10), Some(20), Some(30), Some(40)]]);
        assert_eq!(out, vec![Some(10), Some(20), Some(30)]);
    }

    #[test]
    fn caps_across_multiple_update_batches() {
        let out = run_int_batches(
            Some(3),
            &[&[Some(1), Some(2)], &[Some(3), Some(4), Some(5)]],
        );
        assert_eq!(out, vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn null_elements_are_preserved_in_arrival_order() {
        let out = run_int_batches(Some(3), &[&[None, Some(2), None, Some(4)]]);
        assert_eq!(out, vec![None, Some(2), None]);
    }

    #[test]
    fn fewer_than_n_values_returns_what_was_seen() {
        let out = run_int_batches(Some(5), &[&[Some(1), Some(2)]]);
        assert_eq!(out, vec![Some(1), Some(2)]);
    }

    #[test]
    fn limit_resolved_from_column_when_not_a_literal() {
        // Simulate Calcite's "literal materialised as a Project column" case:
        // limit is None at construction, accumulator reads row 0 of arg 1.
        let mut acc = TakeAccumulator::new(DataType::Int32, None, false);
        let values: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(40),
        ]));
        let n_col: ArrayRef = Arc::new(Int32Array::from(vec![3, 3, 3, 3]));
        acc.update_batch(&[values, n_col]).unwrap();
        let out = match acc.evaluate().unwrap() {
            ScalarValue::List(l) => {
                let inner = l.value(0);
                let typed = inner.as_any().downcast_ref::<Int32Array>().unwrap();
                (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>()
            }
            other => panic!("expected list scalar, got {other:?}"),
        };
        assert_eq!(out, vec![10, 20, 30]);
    }

    #[test]
    fn merge_batch_concatenates_partial_states_and_caps() {
        let mut acc1 = TakeAccumulator::new(DataType::Int32, Some(4), false);
        let arr1: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        acc1.update_batch(&[arr1]).unwrap();
        let mut acc2 = TakeAccumulator::new(DataType::Int32, Some(4), false);
        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), Some(4), Some(5)]));
        acc2.update_batch(&[arr2]).unwrap();

        let state1: ArrayRef = match acc1.state().unwrap().pop().unwrap() {
            ScalarValue::List(l) => l,
            o => panic!("expected list state, got {o:?}"),
        };
        let state2: ArrayRef = match acc2.state().unwrap().pop().unwrap() {
            ScalarValue::List(l) => l,
            o => panic!("expected list state, got {o:?}"),
        };
        let combined: ArrayRef =
            datafusion::arrow::compute::concat(&[state1.as_ref(), state2.as_ref()]).unwrap();

        let mut coord = TakeAccumulator::new(DataType::Int32, Some(4), false);
        coord.merge_batch(&[combined]).unwrap();
        let result = match coord.evaluate().unwrap() {
            ScalarValue::List(l) => {
                let inner = l.value(0);
                let typed = inner.as_any().downcast_ref::<Int32Array>().unwrap();
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            None
                        } else {
                            Some(typed.value(i))
                        }
                    })
                    .collect::<Vec<_>>()
            }
            o => panic!("expected list result, got {o:?}"),
        };
        assert_eq!(result, vec![Some(1), Some(2), Some(3), Some(4)]);
    }

    /// FINAL-stage shape: arg 0 is `List<elem>` carrying each shard's bounded
    /// buffer. `update_batch` must un-nest the list rows into elements and cap
    /// at `n`, the same semantics as `merge_batch`. Mirrors the cross-shard
    /// {@code testTakeAcrossShards} IT.
    #[test]
    fn final_shape_un_nests_list_input_rows_and_caps() {
        // Two shards' partial outputs as List<Int32> rows:
        //   row 0 = [1, 2]
        //   row 1 = [3, 4, 5]
        // FINAL with n=4 must take [1, 2, 3, 4].
        let mut shard1 = TakeAccumulator::new(DataType::Int32, Some(4), false);
        let arr1: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        shard1.update_batch(&[arr1]).unwrap();
        let mut shard2 = TakeAccumulator::new(DataType::Int32, Some(4), false);
        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), Some(4), Some(5)]));
        shard2.update_batch(&[arr2]).unwrap();

        // Build the wire-shape `List<Int32>` column carrying both partial results.
        let s1: ArrayRef = match shard1.state().unwrap().pop().unwrap() {
            ScalarValue::List(l) => l,
            o => panic!("expected list state, got {o:?}"),
        };
        let s2: ArrayRef = match shard2.state().unwrap().pop().unwrap() {
            ScalarValue::List(l) => l,
            o => panic!("expected list state, got {o:?}"),
        };
        let combined: ArrayRef =
            datafusion::arrow::compute::concat(&[s1.as_ref(), s2.as_ref()]).unwrap();

        // FINAL accumulator: arg0_is_list=true, drives `update_batch` (not merge_batch).
        let mut coord = TakeAccumulator::new(DataType::Int32, Some(4), true);
        coord.update_batch(&[combined]).unwrap();
        let result = match coord.evaluate().unwrap() {
            ScalarValue::List(l) => {
                let inner = l.value(0);
                let typed = inner.as_any().downcast_ref::<Int32Array>().unwrap();
                (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>()
            }
            o => panic!("expected list result, got {o:?}"),
        };
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[test]
    fn string_take_round_trips_through_state() {
        let mut acc = TakeAccumulator::new(DataType::Utf8, Some(2), false);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]));
        acc.update_batch(&[arr]).unwrap();
        let state: ArrayRef = match acc.state().unwrap().pop().unwrap() {
            ScalarValue::List(l) => l,
            o => panic!("expected list state, got {o:?}"),
        };
        let mut coord = TakeAccumulator::new(DataType::Utf8, Some(2), false);
        coord.merge_batch(&[state]).unwrap();
        let result = match coord.evaluate().unwrap() {
            ScalarValue::List(l) => {
                let inner = l.value(0);
                let typed = inner.as_any().downcast_ref::<StringArray>().unwrap();
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            None
                        } else {
                            Some(typed.value(i).to_string())
                        }
                    })
                    .collect::<Vec<_>>()
            }
            o => panic!("expected list result, got {o:?}"),
        };
        assert_eq!(result, vec![Some("a".into()), Some("b".into())]);
    }
}
