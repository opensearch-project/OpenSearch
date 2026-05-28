/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! OpenSearch window UDFs. Each registered UDWF must have a matching YAML entry
//! in `opensearch_window_functions.yaml` so isthmus's substrait emitter can
//! bind the operator to the extension URN.

use datafusion::execution::context::SessionContext;

pub mod internal_pattern;

pub fn register_all(ctx: &SessionContext) {
    internal_pattern::register_all(ctx);
}
