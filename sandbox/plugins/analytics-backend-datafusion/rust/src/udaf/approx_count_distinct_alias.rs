/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `approx_count_distinct` — DataFusion-substrait-facing alias for DF's native
//! `approx_distinct` UDAF.
//!
//! Context: PPL's `distinct_count` / `dc` aliases map to Calcite's
//! `SqlStdOperatorTable.APPROX_COUNT_DISTINCT`. Isthmus's built-in
//! `AGGREGATE_SIGS` emits substrait with function name `"approx_count_distinct"`
//! (matching the core `functions_aggregate_approx.yaml` declaration). But
//! DataFusion's UDAF registry keys by primary name only, and DF registers its
//! HyperLogLog-backed impl under `"approx_distinct"` — no alias entry for
//! `approx_count_distinct`. The substrait consumer's name lookup
//! (`FunctionRegistry::udaf(name)`) therefore misses.
//!
//! This module wraps `datafusion::functions_aggregate::approx_distinct::ApproxDistinct`
//! and overrides only `name()` to return `"approx_count_distinct"`. Everything
//! else (signature, accumulator, state fields, aliases) delegates to the inner
//! impl via a fresh `Arc<AggregateUDF>` we construct at call time. Registered
//! alongside DF's existing `approx_distinct` UDAF — both keys resolve to the
//! same HLL implementation, differing only in the registry key.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{
    AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs,
};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, ReversedUDAF, Signature,
    StatisticsArgs,
};

/// Wrapper around DataFusion's native `ApproxDistinct` UDAF that reports its
/// `name()` as `"approx_count_distinct"`. Every other trait method delegates to
/// the inner `approx_distinct` impl.
///
/// `AggregateUDFImpl` requires `DynEq + DynHash`; `Arc<AggregateUDF>` implements
/// `PartialEq + Eq + Hash`, so the derives propagate to the single field.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ApproxCountDistinctAlias {
    inner: Arc<AggregateUDF>,
}

impl ApproxCountDistinctAlias {
    /// Wraps DataFusion's native `approx_distinct` UDAF (from the
    /// `datafusion-functions-aggregate` crate, re-exported via
    /// `datafusion::functions_aggregate`).
    pub fn new() -> Self {
        Self {
            inner: datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf(),
        }
    }
}

impl Default for ApproxCountDistinctAlias {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for ApproxCountDistinctAlias {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Only override — the reason this wrapper exists.
    fn name(&self) -> &str {
        "approx_count_distinct"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        self.inner.create_groups_accumulator(args)
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        // AggregateUDF exposes `reverse_udf()` (not `reverse_expr()` of the
        // underlying impl). It returns `ReversedUDAF`; delegate.
        self.inner.reverse_udf()
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        self.inner.simplify()
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn is_descending(&self) -> Option<bool> {
        self.inner.is_descending()
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner.value_from_stats(statistics_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::Result;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::AggregateUDF;
    use std::sync::Arc;

    /// Register the alias and verify the substrait-consumer-facing lookup
    /// `ctx.udaf("approx_count_distinct")` resolves. Guards against DataFusion
    /// ever adding the alias itself (in which case `with_default_features()`
    /// would already wire it up and this wrapper would become redundant).
    #[tokio::test]
    async fn alias_resolves_by_name() -> Result<()> {
        let ctx = SessionContext::new();

        // Pre-condition: DF's default features DO register `approx_distinct`,
        // but NOT `approx_count_distinct`.
        assert!(
            ctx.state().aggregate_functions().contains_key("approx_distinct"),
            "approx_distinct must be registered by default"
        );
        assert!(
            !ctx.state().aggregate_functions().contains_key("approx_count_distinct"),
            "approx_count_distinct must NOT be registered by default — if this flips, the alias wrapper is redundant"
        );

        // Register the alias.
        ctx.register_udaf(AggregateUDF::from(ApproxCountDistinctAlias::new()));

        assert!(
            ctx.state().aggregate_functions().contains_key("approx_count_distinct"),
            "approx_count_distinct must be registered after alias registration"
        );
        Ok(())
    }

    /// End-to-end evaluation via SQL: `SELECT approx_count_distinct(col) FROM t`
    /// must return the expected distinct count on a small enough input that HLL
    /// is exact. Exercises the full accumulator + groups_accumulator path via
    /// delegation.
    #[tokio::test]
    async fn alias_executes_via_sql() -> Result<()> {
        use datafusion::arrow::datatypes::{Field, Schema};

        let ctx = SessionContext::new();
        ctx.register_udaf(AggregateUDF::from(ApproxCountDistinctAlias::new()));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tag", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "a", "b", "c", "d"])),
            ],
        )?;
        ctx.register_batch("t", batch)?;

        let rows = ctx
            .sql("SELECT approx_count_distinct(id) AS n_ids, approx_count_distinct(tag) AS n_tags FROM t")
            .await?
            .collect()
            .await?;
        assert_eq!(rows.len(), 1);
        let batch = &rows[0];
        // HLL is exact for <= 100-ish distinct values in DF's 52.x impl.
        let n_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .expect("n_ids column")
            .value(0);
        let n_tags = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .expect("n_tags column")
            .value(0);
        assert_eq!(n_ids, 4, "expected 4 distinct id values");
        assert_eq!(n_tags, 4, "expected 4 distinct tag values");
        Ok(())
    }
}
