/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! OpenSearch-specific user-defined aggregate functions registered on every
//! DataFusion `SessionContext` used by this plugin (per-shard scan + coordinator
//! reduce). The substrait consumer resolves aggregate references by name against
//! the session's registry, so it's enough to register here and ship matching
//! YAML extension entries (see `extensions/opensearch_aggregate.yaml`) on the
//! Java side.

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::AggregateUDF;

pub mod approx_count_distinct_alias;
pub mod take;

/// Register every OpenSearch UDAF on `ctx`. Call once at session construction.
pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(take::TakeUdaf::new()));
    // Alias DataFusion's native `approx_distinct` UDAF under the name
    // `approx_count_distinct` so the substrait consumer's name-lookup resolves
    // the core-substrait-YAML signature emitted by isthmus for PPL's
    // distinct_count / dc. See approx_count_distinct_alias.rs for rationale.
    ctx.register_udaf(AggregateUDF::from(
        approx_count_distinct_alias::ApproxCountDistinctAlias::new(),
    ));
    log::info!(
        "OpenSearch UDAF register_all: take, approx_count_distinct (alias for approx_distinct) registered"
    );
}

/// Same as [`register_all`] but builds an `Arc<AggregateUDF>` for callers that
/// only have a `SessionStateBuilder`.
pub fn all_udafs() -> Vec<Arc<AggregateUDF>> {
    vec![
        Arc::new(AggregateUDF::from(take::TakeUdaf::new())),
        Arc::new(AggregateUDF::from(
            approx_count_distinct_alias::ApproxCountDistinctAlias::new(),
        )),
    ]
}
