/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Logger module that re-exports from the shared opensearch-vectorized-spi crate.
//!
//! This module provides logging functions that call back to Java's logging framework
//! through the RustLoggerBridge class.

// Re-export everything from the shared crate's logger module
pub use opensearch_vectorized_spi::logger::*;

// Re-export macros from the shared crate
pub use opensearch_vectorized_spi::{rust_log_info, rust_log_warn, rust_log_error, rust_log_debug, rust_log_trace};
