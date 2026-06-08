/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Conversion-family UDFs grouped under one module.

use datafusion::execution::context::SessionContext;

pub mod numeric_conversion;
pub mod time_conversion;
pub mod tonumber;
pub mod tostring;

/// Register every conversion-family UDF on the given session context.
pub fn register_all(ctx: &SessionContext) {
    numeric_conversion::register_all(ctx);
    time_conversion::register_all(ctx);
    tonumber::register_all(ctx);
    tostring::register_all(ctx);
}
