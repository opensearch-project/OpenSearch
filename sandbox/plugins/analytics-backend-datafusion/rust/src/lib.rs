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

/// Column name for the shard-global row identifier used by Query-Then-Fetch.
/// Stored as Int64 in parquet, computed from position in the indexed path.
///
/// TODO: source this from Java — the canonical column name should be defined
/// once on the coordinator side and passed across FFM rather than hardcoded
/// here. Today both sides hardcode "__row_id__" and rely on the strings
/// matching by convention.
pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

pub(crate) mod agg_mode;
pub mod api;
pub mod cache;
pub mod can_match;
pub mod cancellation;
pub mod cross_rt_stream;
pub mod datafusion_query_config;
pub mod executor;
pub mod ffm;
pub mod helper;
pub mod indexed_executor;
pub mod indexed_table;
pub mod local_executor;
pub mod memory;
pub mod memory_guard;
pub mod native_error;
pub mod partition_stream;
pub mod patterns;
pub mod phantom_corrector;
pub mod project_row_id_analyzer;
pub mod project_row_id_optimizer;
pub mod query_budget;
pub mod query_executor;
pub mod query_tracker;
pub mod relabel_exec;
pub mod runtime_manager;
pub mod schema_coerce;
pub mod session_context;
pub mod shard_table_provider;

pub mod native_node_stats;
pub mod scoped_index_optimizer;
pub mod scoped_page_index_reader;
pub mod search_stats;
pub mod stats;
pub mod task_monitors;
pub mod udaf;
pub mod udf;
pub mod udwf;

// Path aliases — old module names still resolve unchanged.
pub use cache::custom_cache_manager;
pub use cache::eviction_policy;
pub use cache::page_index as parquet_page_cache;
pub use cache::statistics_cache;

/// jemalloc as the unit-test harness's global allocator.
///
/// In production this crate is linked into `opensearch-native-lib`, which installs
/// jemalloc globally (`libs/dataformat-native/rust/lib/src/lib.rs`), so
/// `memory_guard`'s resident readings cover every Rust allocation on the node. The
/// `cargo test --lib` harness links no such crate. Without this declaration the
/// test binary allocates through the system allocator while jemalloc sits linked
/// but unused, `native_bridge_common::allocator::resident_bytes()` returns only
/// jemalloc's own arena metadata (~4.6 MB), and every RSS-gate test that compares
/// resident against a pool fraction silently early-returns instead of asserting.
#[cfg(test)]
#[global_allocator]
static TEST_GLOBAL_ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(test)]
mod memory_guard_signal_test;

#[cfg(test)]
mod spill_e2e_test;

// End-to-end TieredObjectStore + TieredBlockCache integration tests. Located here
// (not in the lower-level `opensearch-tiered-storage` crate) because they drive a
// real DataFusion session + Parquet I/O, and DataFusion/Parquet/Arrow are already
// normal dependencies of this crate — keeping the storage-primitive crate's test
// build free of the DataFusion stack.
#[cfg(test)]
mod tiered_storage_integration_tests;
