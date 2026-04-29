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

pub mod take;

/// Register every OpenSearch UDAF on `ctx`. Call once at session construction.
pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(take::TakeUdaf::new()));
    log::info!("OpenSearch UDAF register_all: take registered");
}

/// Same as [`register_all`] but builds an `Arc<AggregateUDF>` for callers that
/// only have a `SessionStateBuilder`.
pub fn all_udafs() -> Vec<Arc<AggregateUDF>> {
    vec![Arc::new(AggregateUDF::from(take::TakeUdaf::new()))]
}
