/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! DataFusion native execution engine for OpenSearch.
//!
//! The bridge-agnostic API lives in [`api`]. The FFM bridge (`ffm.rs`) exports
//! `extern "C"` functions for JDK FFM.

pub mod api;
pub mod cross_rt_stream;
pub mod executor;
pub mod ffm;
pub mod io;
pub mod local_executor;
pub mod partition_stream;
pub mod query_executor;
pub mod query_memory_pool_tracker;
pub mod runtime_manager;
