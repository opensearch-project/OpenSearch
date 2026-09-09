/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! OpenSearch aggregate UDFs not in DataFusion's builtins. Each registered UDAF
//! must have a matching YAML entry in `opensearch_aggregate_functions.yaml` so
//! that isthmus's substrait emitter can bind the operator to the extension URN.

use datafusion::execution::context::SessionContext;

pub mod approx_distinct_safe;
pub mod internal_pattern;
pub mod list_merge;
pub mod os_count_distinct;
pub mod take;

pub fn register_all(ctx: &SessionContext) {
    take::register_all(ctx);
    list_merge::register_all(ctx);
    internal_pattern::register_all(ctx);
    os_count_distinct::register_all(ctx);
    approx_distinct_safe::register_all(ctx);
}
