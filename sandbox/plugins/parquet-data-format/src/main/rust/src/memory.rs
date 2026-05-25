/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Global memory pool instances for write and merge operations.

use std::sync::{Arc, OnceLock};
use native_bridge_common::memory_pool::MemoryPool;

static WRITE_POOL: OnceLock<Arc<MemoryPool>> = OnceLock::new();
static MERGE_POOL: OnceLock<Arc<MemoryPool>> = OnceLock::new();

/// Default: 0 (unlimited). Set via FFI at runtime.
/// For testing limit enforcement, build with: cargo build --features test-limits
#[cfg(not(feature = "test-limits"))]
const DEFAULT_WRITE_LIMIT: usize = 0;
#[cfg(feature = "test-limits")]
const DEFAULT_WRITE_LIMIT: usize = 35 * 1024 * 1024;

/// Default: 0 (unlimited). Set via FFI at runtime.
#[cfg(not(feature = "test-limits"))]
const DEFAULT_MERGE_LIMIT: usize = 0;
#[cfg(feature = "test-limits")]
const DEFAULT_MERGE_LIMIT: usize = 25 * 1024 * 1024;

/// Returns the node-level write memory pool.
pub fn write_pool() -> &'static Arc<MemoryPool> {
    WRITE_POOL.get_or_init(|| Arc::new(MemoryPool::new("WRITE", DEFAULT_WRITE_LIMIT)))
}

/// Returns the node-level merge memory pool.
pub fn merge_pool() -> &'static Arc<MemoryPool> {
    MERGE_POOL.get_or_init(|| Arc::new(MemoryPool::new("MERGE", DEFAULT_MERGE_LIMIT)))
}

/// Initialize both pools with limits. Called from FFI at node startup.
pub fn init_pools(write_limit: usize, merge_limit: usize) {
    write_pool().set_limit(write_limit);
    merge_pool().set_limit(merge_limit);
}
