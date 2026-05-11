/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Benchmark support for row ID emission strategies.
//!
//! All three `RowIdStrategy` variants are exercisable from the existing
//! entry points:
//!
//! - `execute_query` / `execute_with_context` respects `row_id_strategy`
//!   from `DatafusionQueryConfig` for Approaches 1 and 3.
//! - `execute_indexed_query` / `execute_indexed_with_context` works for
//!   Approach 2 when `emit_row_ids=true` and `FilterClass::None`.
//!
//! ## How to benchmark
//!
//! Set `WireDatafusionQueryConfig.row_id_strategy` to:
//! - `0` for ListingTable (Approach 1)
//! - `1` for IndexedPredicateOnly (Approach 2)
//!
//! ### Baseline
//!
//! For a baseline comparison (plain vanilla query projecting `___row_id`
//! as a regular column without any optimizer rewrite or row_base addition),
//! simply execute a query that projects `___row_id` without the
//! `_global_row_id()` UDF marker. The vanilla path will read `___row_id`
//! from parquet as a normal column.
//!
//! ### Metrics
//!
//! - Wall-clock time: measure start-to-last-batch externally
//! - Rows/second: total_rows / wall_clock_seconds
//! - Peak memory: from `QueryTrackingContext` memory pool
//! - Per-RG latency: from `StreamMetrics` (Approach 2 only)

use crate::datafusion_query_config::{DatafusionQueryConfig, RowIdStrategy};

/// Create a `DatafusionQueryConfig` configured for a specific row ID strategy.
/// Useful for benchmark harnesses that want to compare strategies.
pub fn config_for_strategy(strategy: RowIdStrategy) -> DatafusionQueryConfig {
    let mut config = DatafusionQueryConfig::test_default();
    config.row_id_strategy = strategy;
    config
}

/// Benchmark modes for row ID emission comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkMode {
    /// Baseline: project ___row_id as a regular column (no optimizer, no row_base).
    Baseline,
    /// Approach 1: ShardTableProvider + ProjectRowIdOptimizer.
    ListingTable,
    /// Approach 2: Full indexed executor with select-all evaluator.
    IndexedPredicateOnly,
}

impl BenchmarkMode {
    /// Convert to the corresponding `RowIdStrategy` (Baseline has no strategy).
    pub fn to_strategy(&self) -> Option<RowIdStrategy> {
        match self {
            BenchmarkMode::Baseline => None,
            BenchmarkMode::ListingTable => Some(RowIdStrategy::ListingTable),
            BenchmarkMode::IndexedPredicateOnly => Some(RowIdStrategy::IndexedPredicateOnly),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_for_each_strategy() {
        let c1 = config_for_strategy(RowIdStrategy::ListingTable);
        assert_eq!(c1.row_id_strategy, RowIdStrategy::ListingTable);

        let c2 = config_for_strategy(RowIdStrategy::IndexedPredicateOnly);
        assert_eq!(c2.row_id_strategy, RowIdStrategy::IndexedPredicateOnly);

    }

    #[test]
    fn benchmark_mode_to_strategy() {
        assert_eq!(BenchmarkMode::Baseline.to_strategy(), None);
        assert_eq!(
            BenchmarkMode::ListingTable.to_strategy(),
            Some(RowIdStrategy::ListingTable)
        );
        assert_eq!(
            BenchmarkMode::IndexedPredicateOnly.to_strategy(),
            Some(RowIdStrategy::IndexedPredicateOnly)
        );
    }
}
