/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared tokio runtime for async ObjectStore operations.
//!
//! All FFM calls that need async (writer flush, ObjectStore put) use
//! `RUNTIME.block_on(...)` to bridge the sync FFM boundary.

use lazy_static::lazy_static;
use tokio::runtime::Runtime;

lazy_static! {
    pub static ref RUNTIME: Runtime =
        Runtime::new().expect("Failed to create tokio runtime for parquet ObjectStore I/O");
}
