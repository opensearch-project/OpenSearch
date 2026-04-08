/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use mimalloc::MiMalloc;
use std::sync::LazyLock;
use tokio::runtime::Runtime;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Shared tokio runtime for driving async object-store IO from synchronous JNI calls.
/// Uses a multi-thread scheduler with default worker count (num CPUs).
pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Runtime::new().expect("Failed to create tokio runtime")
});

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(test)]
mod tests;

pub mod object_store_handle;
pub mod writer;
mod jni;

pub use native_bridge_spi::{log_info, log_error, log_debug};
