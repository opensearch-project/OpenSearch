/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `approx_count_distinct` — alias for DataFusion's built-in `approx_distinct`
//! (HyperLogLog). isthmus emits PPL `dc()` / `distinct_count()` /
//! `distinct_count_approx()` (after `BackendPlanAdapter` rewrites them) as the
//! Substrait window function name `approx_count_distinct`, which matches the
//! Substrait extension binding for Calcite's `SqlStdOperatorTable.APPROX_COUNT_DISTINCT`.
//! DataFusion 53.1's built-in is named `approx_distinct` and has no alias, so
//! the substrait consumer can't resolve `approx_count_distinct` against the
//! function registry. This module registers a thin wrapper UDAF whose primary
//! `name()` is `approx_count_distinct` and which delegates everything else to
//! the built-in `approx_distinct_udaf()`. SessionState.register_udaf inserts
//! the wrapper under both `approx_count_distinct` and any aliases, leaving the
//! original `approx_distinct` registration untouched.
//!
//! PPL semantics: V3 path computes `dc()` via OpenSearch's cardinality
//! aggregation (HyperLogLog++), so it has always been approximate even when
//! users wrote `distinct_count(x)`. Routing to `approx_distinct` matches that
//! existing behavior — there is no semantic regression versus the V3 path.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(ApproxCountDistinctUdaf::new()));
}

/// Wrapper UDAF that exposes datafusion's built-in `approx_distinct` UDAF
/// under the name `approx_count_distinct`.
pub struct ApproxCountDistinctUdaf {
    inner: Arc<AggregateUDF>,
}

impl std::fmt::Debug for ApproxCountDistinctUdaf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApproxCountDistinctUdaf").finish()
    }
}

impl ApproxCountDistinctUdaf {
    pub fn new() -> Self {
        Self {
            inner: approx_distinct_udaf(),
        }
    }
}

impl Default for ApproxCountDistinctUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for ApproxCountDistinctUdaf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for ApproxCountDistinctUdaf {}
impl std::hash::Hash for ApproxCountDistinctUdaf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        "approx_count_distinct".hash(state);
    }
}

impl AggregateUDFImpl for ApproxCountDistinctUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "approx_count_distinct"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        self.inner.return_field(arg_fields)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
}
