/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Liquid Cache runtime + control surface, compiled into the analytics engine
//! native library when the `liquid_cache` cargo feature is enabled.
//!
//! [`runtime::LiquidOnlyRuntime`] owns the process-global cache and optimizer;
//! [`ffi`] exposes the `lc_*` C functions the Java plugin calls to init the
//! cache and drive the enable/memory/stats/clear settings surface.

pub mod ffi;
pub mod runtime;

pub use liquid_cache_datafusion::LiquidParquetSource;
pub use runtime::LiquidOnlyRuntime;

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::physical_expr::PhysicalExpr;

/// Whether an indexed row-group scan should engage liquid cache: every projected
/// column is numeric/date/timestamp/boolean, the projection is non-empty and
/// within the indexed-path column budget, and no predicate column is string or
/// binary (those were never cacheable in-process). This is the engagement policy
/// for the indexed path (the listing path is gated by the optimizer instead).
pub fn indexed_scan_eligible(
    schema: &SchemaRef,
    projection: Option<&[usize]>,
    predicate: Option<&Arc<dyn PhysicalExpr>>,
) -> bool {
    let max_columns = runtime::lc_indexed_max_columns();
    let projection_ok = projection.is_some_and(|proj| {
        !proj.is_empty()
            && proj.len() <= max_columns
            && proj
                .iter()
                .all(|&idx| schema.fields().get(idx).is_some_and(|f| is_cacheable(f.data_type())))
    });
    if !projection_ok {
        return false;
    }
    let predicate_has_binary = predicate.is_some_and(|pred| {
        datafusion::physical_expr::utils::collect_columns(pred)
            .iter()
            .any(|col| {
                schema
                    .fields()
                    .get(col.index())
                    .is_some_and(|f| is_string_or_binary(f.data_type()))
            })
    });
    !predicate_has_binary
}

fn is_cacheable(dt: &DataType) -> bool {
    dt.is_numeric()
        || matches!(
            dt,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Boolean
        )
}

fn is_string_or_binary(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::BinaryView
            | DataType::LargeBinary
    )
}
