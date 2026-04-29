/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `take(x, n)` — bounded aggregate that collects the first `n` values of `x`
//! into a list. Mirrors PPL's existing Java [`TakeAggFunction`] semantics:
//!
//! * `n > 0`: append values in scan order until the buffer holds `n` items.
//! * `n <= 0`: never append — returns an empty list.
//! * Default `n` (when only one argument supplied): `10`.
//!
//! Distributed correctness: each per-shard accumulator emits its bounded
//! buffer as a `List<element>` scalar via `state()`, the coordinator's final
//! accumulator concatenates incoming lists in `merge_batch()` and trims back
//! to `n`. Cross-shard ordering is non-deterministic — same property as the
//! Java implementation, which assumes single-accumulator scan order.
//!
//! [`TakeAggFunction`]: https://github.com/opensearch-project/sql/blob/main/core/src/main/java/org/opensearch/sql/calcite/udf/udaf/TakeAggFunction.java

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::expressions::Literal;

const DEFAULT_LIMIT: i64 = 10;

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

// AggregateUDFImpl requires `DynEq + DynHash`. There's no meaningful "equality"
// between two TakeUdaf instances (they're effectively a singleton), so all
// instances compare equal and hash identically.
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            arg_types[0].clone(),
            true,
        ))))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let element_type = acc_args
            .expr_fields
            .get(0)
            .map(|f| f.data_type().clone())
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "take(): missing first argument".to_string(),
                )
            })?;
        let limit = limit_from_args(&acc_args)?;
        Ok(Box::new(TakeAccumulator::new(element_type, limit)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let element = args
            .input_fields
            .get(0)
            .map(|f| f.data_type().clone())
            .unwrap_or(DataType::Null);
        let list_type = DataType::List(Arc::new(Field::new("item", element, true)));
        Ok(vec![Arc::new(Field::new(
            format!("{}[buf]", args.name),
            list_type,
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
    if let Some(lit) = expr.as_any().downcast_ref::<Literal>() {
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
struct TakeAccumulator {
    element_type: DataType,
    limit: Option<i64>,
    buf: Vec<ScalarValue>,
}

impl TakeAccumulator {
    fn new(element_type: DataType, limit: Option<i64>) -> Self {
        let cap = limit.filter(|&l| l > 0).map(|l| l as usize).unwrap_or(0);
        Self {
            element_type,
            limit,
            buf: Vec::with_capacity(cap),
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
        let mut i = 0;
        while self.buf.len() < cap && i < col.len() {
            self.buf.push(ScalarValue::try_from_array(col, i)?);
            i += 1;
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

    fn varchar_acc(limit: Option<i64>) -> TakeAccumulator {
        TakeAccumulator::new(DataType::Utf8, limit)
    }

    fn strings(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec())) as ArrayRef
    }

    fn const_int32(value: i32, len: usize) -> ArrayRef {
        Arc::new(Int32Array::from(vec![value; len])) as ArrayRef
    }

    fn list_len(scalar: &ScalarValue) -> usize {
        match scalar {
            ScalarValue::List(arr) => arr.value(0).len(),
            other => panic!("expected List scalar, got {other:?}"),
        }
    }

    fn list_strings(scalar: &ScalarValue) -> Vec<String> {
        let inner = match scalar {
            ScalarValue::List(arr) => arr.value(0),
            other => panic!("expected List scalar, got {other:?}"),
        };
        let s = inner.as_any().downcast_ref::<StringArray>().expect("string array");
        (0..s.len()).map(|i| s.value(i).to_string()).collect()
    }

    /// `take(col, lit(2))` — limit known at construction time. Should cap at 2.
    #[test]
    fn limit_from_literal_caps_buffer() {
        let mut acc = varchar_acc(Some(2));
        let col = strings(&["a", "b", "c", "d", "e"]);
        // No second column — caller passed a Substrait literal, so DF doesn't
        // materialize an n-column. We model this by passing a single arg.
        acc.update_batch(&[col]).expect("update_batch");
        let out = acc.evaluate().expect("evaluate");
        assert_eq!(list_len(&out), 2);
        assert_eq!(list_strings(&out), vec!["a", "b"]);
    }

    /// `take(col, $f1=2)` — Calcite materialized `2` as a project column. The
    /// accumulator gets `Option::None` at construction; the limit is resolved
    /// from row 0 of the second column on the first update_batch. Regression
    /// test for the bug where the limit was hardcoded to DEFAULT_LIMIT (10)
    /// because we only handled the literal path.
    #[test]
    fn limit_from_column_resolved_on_first_update() {
        let mut acc = varchar_acc(None);
        let col = strings(&["a", "b", "c", "d", "e"]);
        let n_col = const_int32(2, 5); // every row carries the constant n=2
        acc.update_batch(&[col, n_col]).expect("update_batch");
        let out = acc.evaluate().expect("evaluate");
        assert_eq!(list_len(&out), 2);
        assert_eq!(list_strings(&out), vec!["a", "b"]);
    }

    /// `take(col)` — no second arg, limit defaults to 10.
    #[test]
    fn missing_limit_defaults_to_ten() {
        let mut acc = varchar_acc(Some(DEFAULT_LIMIT));
        let col = strings(&["a", "b", "c"]);
        acc.update_batch(&[col]).expect("update_batch");
        let out = acc.evaluate().expect("evaluate");
        assert_eq!(list_len(&out), 3, "fewer rows than DEFAULT_LIMIT — collect all");
    }

    /// `take(col, 0)` and `take(col, -1)` produce empty lists. Mirrors the
    /// PPL TakeAggFunction behavior — no rows accepted when n <= 0.
    #[test]
    fn non_positive_limit_yields_empty_list() {
        for n in [Some(0), Some(-3)] {
            let mut acc = varchar_acc(n);
            let col = strings(&["a", "b", "c"]);
            acc.update_batch(&[col]).expect("update_batch");
            let out = acc.evaluate().expect("evaluate");
            assert_eq!(list_len(&out), 0, "n={n:?} should produce empty list");
        }
    }

    /// Multiple update_batch calls past the cap are no-ops.
    #[test]
    fn cap_holds_across_multiple_batches() {
        let mut acc = varchar_acc(Some(2));
        acc.update_batch(&[strings(&["a"])]).expect("first batch");
        acc.update_batch(&[strings(&["b", "c", "d"])]).expect("second batch");
        acc.update_batch(&[strings(&["e", "f"])]).expect("third batch");
        let out = acc.evaluate().expect("evaluate");
        assert_eq!(list_strings(&out), vec!["a", "b"]);
    }

    /// merge_batch concatenates partial-state lists and trims to cap. This is
    /// the coordinator-reduce path for the multi-shard case.
    #[test]
    fn merge_batch_trims_concatenated_partials() {
        let mut acc = varchar_acc(Some(3));
        let mut shard1 = varchar_acc(Some(3));
        shard1.update_batch(&[strings(&["a", "b"])]).expect("shard1 update");
        let mut shard2 = varchar_acc(Some(3));
        shard2.update_batch(&[strings(&["c", "d", "e"])]).expect("shard2 update");

        // Build a ListArray containing both shard states as rows. ScalarValue
        // doesn't easily concatenate two single-element list scalars, so build
        // the ListArray manually via the same helper the partials use.
        let s1 = shard1.evaluate().expect("shard1 eval");
        let s2 = shard2.evaluate().expect("shard2 eval");
        let combined = ScalarValue::iter_to_array(vec![s1, s2]).expect("iter_to_array");

        acc.merge_batch(&[combined]).expect("merge_batch");
        let out = acc.evaluate().expect("evaluate");
        assert_eq!(list_strings(&out), vec!["a", "b", "c"]);
    }
}
