/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `os_count_distinct(x)` — exact distinct count for use in a window context.
//!
//! Why this UDAF exists: DataFusion 54.x's substrait crate drops the
//! `AggregationInvocation::DISTINCT` bit on window aggregates (the consumer
//! hardcodes `distinct: false` and the producer writes `invocation: 0`). A
//! plain `count(DISTINCT x) OVER(...)` therefore reaches the executor as a
//! plain `count(x)`. By encoding DISTINCT in the *operator name* instead of
//! the invocation enum, we sidestep the bug without vendoring a fork of
//! `datafusion-substrait`.
//!
//! Semantics: emits the count of distinct, non-null values per group / per
//! frame. NULL inputs are skipped — matches `count(DISTINCT x)` SQL semantics
//! and the running cumulative behavior expected by `OVER(ORDER BY)`.
//!
//! Distributed correctness: state is `List<value>` (the dedup'd set), so
//! `merge_batch` unions per-shard sets. PARTIAL emits the dedup'd values,
//! FINAL re-unions and counts.

use std::collections::HashSet;
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

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(OsCountDistinctUdaf::new()));
}

#[derive(Debug)]
pub struct OsCountDistinctUdaf {
    signature: Signature,
}

impl OsCountDistinctUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Default for OsCountDistinctUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for OsCountDistinctUdaf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for OsCountDistinctUdaf {}
impl Hash for OsCountDistinctUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "os_count_distinct".hash(state);
    }
}

/// If `dt` is a list type, return its element type; otherwise return `dt`.
/// PARTIAL sees the raw element column; FINAL sees a `List<element>` carrying
/// each shard's dedup'd state. Both paths land in the same accumulator.
fn inner_element_type(dt: &DataType) -> DataType {
    match dt {
        DataType::List(field) | DataType::LargeList(field) => field.data_type().clone(),
        DataType::FixedSizeList(field, _) => field.data_type().clone(),
        other => other.clone(),
    }
}

impl AggregateUDFImpl for OsCountDistinctUdaf {
    fn name(&self) -> &str {
        "os_count_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let raw_arg0 = acc_args
            .exprs
            .first()
            .map(|e| e.data_type(acc_args.schema))
            .transpose()?
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "os_count_distinct: missing argument".to_string(),
                )
            })?;
        let arg0_is_list = matches!(
            raw_arg0,
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _),
        );
        let element_type = inner_element_type(&raw_arg0);
        Ok(Box::new(OsCountDistinctAccumulator::new(
            element_type,
            arg0_is_list,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // State is the dedup'd value set so merge_batch can union shard sets
        // and recount on the coordinator. Reusing the input's element type
        // avoids re-introducing a list of i64 counts (which would lose
        // distinct semantics across shards).
        let element_type = match args.input_fields.first() {
            Some(f) => inner_element_type(f.data_type()),
            None => {
                return exec_err!("os_count_distinct: state_fields needs at least one input field")
            }
        };
        Ok(vec![Arc::new(Field::new(
            format!("{}[set]", args.name),
            DataType::List(Arc::new(Field::new("item", element_type, true))),
            true,
        ))])
    }
}

pub struct OsCountDistinctAccumulator {
    element_type: DataType,
    seen: HashSet<ScalarValue>,
    /// True when arg 0 is a list column (FINAL stage) — `update_batch` must
    /// un-nest each list-row into individual elements rather than treating the
    /// whole list as a single value.
    arg0_is_list: bool,
}

impl OsCountDistinctAccumulator {
    pub fn new(element_type: DataType, arg0_is_list: bool) -> Self {
        Self {
            element_type,
            seen: HashSet::new(),
            arg0_is_list,
        }
    }

    fn add_from_array(&mut self, col: &ArrayRef) -> Result<()> {
        for i in 0..col.len() {
            if col.is_null(i) {
                continue;
            }
            self.seen.insert(ScalarValue::try_from_array(col, i)?);
        }
        Ok(())
    }

    fn add_from_list(&mut self, col: &ArrayRef) -> Result<()> {
        let lists: &ListArray = col.as_list();
        for i in 0..lists.len() {
            if lists.is_null(i) {
                continue;
            }
            let inner = lists.value(i);
            for j in 0..inner.len() {
                if inner.is_null(j) {
                    continue;
                }
                self.seen.insert(ScalarValue::try_from_array(&inner, j)?);
            }
        }
        Ok(())
    }
}

impl Debug for OsCountDistinctAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OsCountDistinctAccumulator")
            .field("len", &self.seen.len())
            .finish()
    }
}

impl Accumulator for OsCountDistinctAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let col = &values[0];
        if self.arg0_is_list {
            self.add_from_list(col)
        } else {
            self.add_from_array(col)
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.seen.len() as i64)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.seen.iter().map(|s| s.size()).sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<ScalarValue> = self.seen.iter().cloned().collect();
        let arr = ScalarValue::new_list_nullable(&values, &self.element_type);
        Ok(vec![ScalarValue::List(arr)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        self.add_from_list(&states[0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};

    fn run_int_batches(batches: &[&[Option<i32>]]) -> i64 {
        let mut acc = OsCountDistinctAccumulator::new(DataType::Int32, false);
        for b in batches {
            let arr: ArrayRef = Arc::new(Int32Array::from_iter(b.iter().copied()));
            acc.update_batch(&[arr]).unwrap();
        }
        match acc.evaluate().unwrap() {
            ScalarValue::Int64(Some(n)) => n,
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn counts_distinct_non_null_values() {
        // {1, 2, 3} → 3
        let n = run_int_batches(&[&[Some(1), Some(2), Some(2), Some(3), Some(1)]]);
        assert_eq!(n, 3);
    }

    #[test]
    fn skips_nulls() {
        // {1, 2} (None ignored) → 2
        let n = run_int_batches(&[&[Some(1), None, Some(2), None, Some(1)]]);
        assert_eq!(n, 2);
    }

    #[test]
    fn dedupes_across_batches() {
        // {1, 2, 3} → 3
        let n = run_int_batches(&[&[Some(1), Some(2)], &[Some(2), Some(3)]]);
        assert_eq!(n, 3);
    }

    #[test]
    fn empty_input_returns_zero() {
        let n = run_int_batches(&[&[]]);
        assert_eq!(n, 0);
    }

    #[test]
    fn all_nulls_return_zero() {
        let n = run_int_batches(&[&[None, None, None]]);
        assert_eq!(n, 0);
    }

    #[test]
    fn merge_unions_per_shard_states() {
        let mut s1 = OsCountDistinctAccumulator::new(DataType::Int32, false);
        s1.update_batch(&[Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef])
            .unwrap();
        let mut s2 = OsCountDistinctAccumulator::new(DataType::Int32, false);
        s2.update_batch(&[Arc::new(Int32Array::from(vec![Some(2), Some(3), Some(4)])) as ArrayRef])
            .unwrap();

        let st1: ArrayRef = match s1.state().unwrap().pop().unwrap() {
            ScalarValue::List(l) => l,
            o => panic!("expected list state, got {o:?}"),
        };
        let st2: ArrayRef = match s2.state().unwrap().pop().unwrap() {
            ScalarValue::List(l) => l,
            o => panic!("expected list state, got {o:?}"),
        };
        let combined: ArrayRef =
            datafusion::arrow::compute::concat(&[st1.as_ref(), st2.as_ref()]).unwrap();

        let mut coord = OsCountDistinctAccumulator::new(DataType::Int32, false);
        coord.merge_batch(&[combined]).unwrap();
        match coord.evaluate().unwrap() {
            // {1, 2, 3, 4} → 4
            ScalarValue::Int64(Some(n)) => assert_eq!(n, 4),
            o => panic!("expected Int64, got {o:?}"),
        }
    }

    #[test]
    fn final_shape_un_nests_list_input_rows() {
        let mut shard1 = OsCountDistinctAccumulator::new(DataType::Utf8, false);
        shard1
            .update_batch(&[
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])) as ArrayRef,
            ])
            .unwrap();
        let mut shard2 = OsCountDistinctAccumulator::new(DataType::Utf8, false);
        shard2
            .update_batch(&[Arc::new(StringArray::from(vec![Some("b"), Some("c")])) as ArrayRef])
            .unwrap();
        let s1: ArrayRef = match shard1.state().unwrap().pop().unwrap() {
            ScalarValue::List(l) => l,
            o => panic!("expected list, got {o:?}"),
        };
        let s2: ArrayRef = match shard2.state().unwrap().pop().unwrap() {
            ScalarValue::List(l) => l,
            o => panic!("expected list, got {o:?}"),
        };
        let combined: ArrayRef =
            datafusion::arrow::compute::concat(&[s1.as_ref(), s2.as_ref()]).unwrap();

        // FINAL stage: arg0_is_list=true, drives update_batch on the
        // List-typed column rather than merge_batch.
        let mut coord = OsCountDistinctAccumulator::new(DataType::Utf8, true);
        coord.update_batch(&[combined]).unwrap();
        match coord.evaluate().unwrap() {
            // {a, b, c} → 3
            ScalarValue::Int64(Some(n)) => assert_eq!(n, 3),
            o => panic!("expected Int64, got {o:?}"),
        }
    }
}
