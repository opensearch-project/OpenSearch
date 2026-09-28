/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared Rust utilities for OpenSearch sandbox native plugins.

pub mod allocator;
pub mod error;
pub mod io_runtime;
pub mod logger;
pub mod memory_pool;

// Re-export the proc macro so plugins use `#[native_bridge_common::ffm_safe]`
pub use native_bridge_macros::ffm_safe;
