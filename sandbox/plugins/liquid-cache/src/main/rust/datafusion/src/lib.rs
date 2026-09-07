// SPDX-License-Identifier: Apache-2.0
// Vendored in-memory subset of liquid-cache DataFusion integration. See ../README.md for provenance.

//! DataFusion integration for the vendored in-memory liquid cache:
//! a Parquet [`FileSource`](datafusion::datasource::physical_plan::FileSource)
//! replacement that serves decoded batches from the cache, plus a physical
//! optimizer rule that rewrites `ParquetSource` scans to use it.
#![warn(missing_docs)]

pub mod optimizers;
mod reader;
mod sync;
pub(crate) mod utils;

pub mod cache;
pub use cache::{LiquidCacheParquet, LiquidCacheParquetRef};
pub use optimizers::{LocalModeOptimizer, rewrite_data_source_plan};
pub use reader::plantime::engagement_policy::{
    AlwaysEngagePolicy, CacheEngagementPolicy, DEFAULT_SELECTIVITY_THRESHOLD, EngagementContext,
    EngagementDecision, NeverEngagePolicy, SelectivityThresholdPolicy, default_engagement_policy,
};
pub use reader::plantime::source::pre_seed_metadata_cache;
pub use reader::{FilterCandidateBuilder, LiquidParquetSource, LiquidPredicate, LiquidRowFilter};
pub use utils::boolean_buffer_and_then;
