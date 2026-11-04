//! Cache layer for liquid cache.

mod budget;
mod builders;
mod cached_batch;
mod core;
mod index;
mod liquid_expr;
mod observer;
pub mod policies;
mod transcode;
mod utils;

pub use builders::{EvaluatePredicate, Get, Insert, LiquidCacheBuilder};
pub use cached_batch::{CacheEntry, CachedBatchType};
pub use core::{CacheFull, LiquidCache};
pub use liquid_expr::LiquidExpr;
pub use observer::Observer;
pub use observer::{CacheStats, RuntimeStats, RuntimeStatsSnapshot};
pub use policies::{
    CachePolicy, Evict, LiquidPolicy, LruPolicy, SqueezeOutcome, SqueezePolicy, TranscodeEvict,
};
pub use transcode::transcode_liquid_inner;
pub use utils::EntryID;

// Backwards-compatible module paths for existing imports.
/// Legacy path: re-export cache policy types under `cache::cache_policies`.
pub mod cache_policies {
    pub use super::policies::cache::*;
}

/// Legacy path: re-export squeeze policy types under `cache::squeeze_policies`.
pub mod squeeze_policies {
    pub use super::policies::squeeze::*;
}
