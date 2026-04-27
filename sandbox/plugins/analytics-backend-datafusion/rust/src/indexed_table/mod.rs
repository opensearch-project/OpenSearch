/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Index-accelerated parquet queries for DataFusion.
//!
//! # Execution paths
//!
//! Three shapes of query land in this module depending on the filter tree
//! produced by `substrait_to_tree::classify_filter`:
//!
//! - **No-index path** — no `index_filter(...)` calls in the plan. The
//!   query never reaches this module; [`crate::query_executor`] runs it
//!   as a plain `ListingTable` scan.
//! - **Single-collector path** — exactly one `index_filter(...)` AND'd
//!   with zero or more parquet-native predicates. The collector produces
//!   the candidate bitset; DataFusion's
//!   `ParquetSource.with_predicate(...).with_pushdown_filters(true)`
//!   applies the residual predicates during decode. See
//!   [`eval::single_collector::SingleCollectorEvaluator`].
//! - **Multi-filter tree path** — multiple `index_filter(...)` calls,
//!   or OR/NOT mixing them with predicates, or any shape that isn't a
//!   flat AND-of-one-collector-plus-predicates. Uses
//!   [`eval::bitmap_tree::BitmapTreeEvaluator`], which runs a two-stage
//!   evaluation per row group: a candidate stage in the RoaringBitmap
//!   domain (to decide which parquet pages to read) plus a refinement
//!   stage on the decoded record batches using Arrow kernels (to produce
//!   the exact per-row answer).
//!
//! All three share the same [`stream::IndexedExec`] / [`stream::IndexedStream`]
//! / [`table_provider::IndexedTableProvider`]. The evaluator choice is the
//! only thing that varies.
//!
//! # Row-group-by-row-group streaming
//!
//! **Invariant.** Tree evaluation always happens per row group. Bitsets stay
//! bounded (~512 bytes per RG), prefetch overlap hides index-side latency.
//! The [`eval::RowGroupBitsetSource`] trait enforces this structurally —
//! there is no shard-wide bitset API.
//!
//! # Modules
//!
//! - `index` — `ShardSearcher` + `RowGroupDocsCollector` trait
//! - `bool_tree` — `BoolNode`, `ResolvedNode`, De Morgan's normalize
//! - `substrait_to_tree` — `index_filter` UDF, `expr_to_bool_tree`, `classify_filter`
//! - `eval` — `RowGroupBitsetSource` trait + concrete evaluators
//! - `stream` — unified `IndexedExec` + `IndexedStream`
//! - `table_provider` — unified `IndexedTableProvider`
//! - `ffm_callbacks` — FFM upcall surface for the filter-tree callbacks
//!   into Java (provider/collector lifetime wrappers + registration)
//! - `page_pruner`, `partitioning`, `parquet_bridge`, `metrics`, `segment_info` — support

pub mod bool_tree;
pub mod eval;
pub mod ffm_callbacks;
pub mod index;
pub mod metrics;
pub mod page_pruner;
pub mod parquet_bridge;
pub mod partitioning;
pub mod row_selection;
pub mod segment_info;
pub mod stream;
pub mod substrait_to_tree;
pub mod table_provider;

#[cfg(test)]
mod tests_e2e;
