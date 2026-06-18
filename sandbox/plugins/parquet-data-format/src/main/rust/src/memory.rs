/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Write and merge memory pools backed by `native_bridge_common::memory_pool::MemoryPool`.

use std::sync::{Arc, OnceLock};

use native_bridge_common::memory_pool::MemoryPool;

static WRITE_POOL: OnceLock<Arc<MemoryPool>> = OnceLock::new();
static MERGE_POOL: OnceLock<Arc<MemoryPool>> = OnceLock::new();

/// Initialize write and merge pools. Called once from Java.
pub fn init_pools(write_limit: usize, merge_limit: usize) {
    WRITE_POOL.get_or_init(|| Arc::new(MemoryPool::new("write", write_limit)));
    MERGE_POOL.get_or_init(|| Arc::new(MemoryPool::new("merge", merge_limit)));
}

/// Returns the write pool, or panics if not initialized.
pub fn write_pool() -> &'static Arc<MemoryPool> {
    WRITE_POOL.get().expect("write pool not initialized")
}

/// Returns the merge pool, or panics if not initialized.
pub fn merge_pool() -> &'static Arc<MemoryPool> {
    MERGE_POOL.get().expect("merge pool not initialized")
}

pub fn set_write_limit(v: usize) {
    if let Some(p) = WRITE_POOL.get() {
        p.set_limit(v);
    }
}

pub fn set_merge_limit(v: usize) {
    if let Some(p) = MERGE_POOL.get() {
        p.set_limit(v);
    }
}

/// Returns [write_limit, write_used, write_peak, merge_limit, merge_used, merge_peak].
pub fn get_stats() -> [usize; 6] {
    let w = WRITE_POOL
        .get()
        .map(|p| (p.limit(), p.used(), p.peak()))
        .unwrap_or((0, 0, 0));
    let m = MERGE_POOL
        .get()
        .map(|p| (p.limit(), p.used(), p.peak()))
        .unwrap_or((0, 0, 0));
    [w.0, w.1, w.2, m.0, m.1, m.2]
}
