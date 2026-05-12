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

pub(crate) mod agg_mode;
pub mod api;
pub mod cache;
pub mod cancellation;
pub mod cross_rt_stream;
pub mod custom_cache_manager;
pub mod datafusion_query_config;
pub mod eviction_policy;
pub mod executor;
pub mod ffm;
pub mod indexed_executor;
pub mod indexed_table;
pub mod io;
pub mod local_executor;
pub mod memory;
pub mod memory_guard;
pub mod partition_stream;
pub mod phantom_corrector;
pub mod query_budget;
pub mod query_executor;
pub mod query_tracker;
pub mod runtime_manager;
pub mod schema_coerce;
pub mod session_context;
pub mod statistics_cache;
pub mod udf;
pub mod stats;
pub mod task_monitors;