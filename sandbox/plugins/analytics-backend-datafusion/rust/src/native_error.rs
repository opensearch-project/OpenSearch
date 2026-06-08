/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Standardized error constructors for errors that cross the Rust→Java FFM boundary.
//!
//! Errors produced by these functions are returned as `Err(String)` through `#[ffm_safe]`
//! FFM exports, arriving in Java as `RuntimeException(message)`. The Java-side
//! `NativeErrorConverter` (in analytics-engine plugin) parses these messages to convert
//! them into appropriate OpenSearch exception types.
//!
//! # Contract
//!
//! The message formats below are a **stable contract** between Rust and Java.
//! Any changes to prefixes, field ordering, or key phrases MUST be coordinated
//! with `NativeErrorConverter.java`. The Java parser uses substring matching on
//! these messages to classify errors.
//!
//! # Adding new error types
//!
//! 1. Add a factory function here with a distinctive, stable prefix/phrase.
//! 2. Add a matching branch in `NativeErrorConverter.findAndConvert()`.
//! 3. Document the Java exception type and HTTP status in the doc comment.

use datafusion::common::DataFusionError;

/// Pool limit exceeded during operator execution.
///
/// Produced when `DynamicLimitPool.try_grow` fails and jemalloc confirms pressure.
/// Triggers operator spill; if spill also fails, the query is terminated.
///
/// Java conversion: `CircuitBreakingException` → HTTP 429
/// Key phrase: "Failed to allocate"
pub fn pool_limit_error(
    bytes_requested: usize,
    consumer_name: &str,
    consumer_reserved: usize,
    available: usize,
    limit: usize,
) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!(
        "Failed to allocate {} bytes for {} ({} already reserved) \
         — {} available out of {} limit",
        bytes_requested, consumer_name, consumer_reserved, available, limit,
    ))
}

/// Admission rejection: not enough pool capacity to start a new query.
///
/// Produced when `acquire_budget` cannot reserve the phantom even at minimum
/// parallelism. The query is rejected before any execution begins.
///
/// Java conversion: `OpenSearchRejectedExecutionException` → HTTP 429
/// Key phrase: "Cannot reserve untracked memory budget"
pub fn admission_rejected_error(
    bytes_required: usize,
    partitions: usize,
    batch_size: usize,
    avg_row_bytes: usize,
) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!(
        "Cannot reserve untracked memory budget: {} bytes required at \
         minimum parallelism (partitions={}, batch_size={}, avg_row_bytes={}). \
         Pool capacity exhausted.",
        bytes_required, partitions, batch_size, avg_row_bytes,
    ))
}
