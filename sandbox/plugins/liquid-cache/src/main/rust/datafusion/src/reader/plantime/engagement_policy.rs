//! Cache engagement policy — decides per-file whether LC should serve
//! decoded batches from cache or delegate to plain parquet.

use std::fmt::Debug;
use std::sync::Arc;

/// Context passed to the policy at file-open time.
#[derive(Debug, Clone)]
pub struct EngagementContext {
    /// Fraction of rows surviving RG + page pruning (0.0–1.0).
    pub estimated_selectivity: f64,
    /// Whether the query has a pushdown predicate on this file.
    pub has_predicate: bool,
    /// Total rows across selected row groups.
    pub total_rows: usize,
    /// Rows that survived pruning.
    pub selected_rows: usize,
    /// File path for logging.
    pub file_path: String,
}

/// Decision returned by the policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngagementDecision {
    /// Serve decoded batches from cache, fill on miss.
    UseLiquidCache,
    /// Bypass LC, delegate to plain parquet reader.
    DelegateToParquet,
}

/// Decides per-file whether Liquid Cache should engage or delegate.
pub trait CacheEngagementPolicy: Send + Sync + Debug {
    /// Decide whether to use LC or delegate to plain parquet for this file.
    fn decide(&self, ctx: &EngagementContext) -> EngagementDecision;
}

/// Default selectivity threshold below which LC delegates to parquet.
pub const DEFAULT_SELECTIVITY_THRESHOLD: f64 = 0.5;

/// Delegate when selectivity < threshold AND a predicate exists.
#[derive(Debug, Clone)]
pub struct SelectivityThresholdPolicy {
    /// Selectivity below which LC delegates to plain parquet.
    pub threshold: f64,
}

impl Default for SelectivityThresholdPolicy {
    fn default() -> Self {
        Self {
            threshold: DEFAULT_SELECTIVITY_THRESHOLD,
        }
    }
}

impl SelectivityThresholdPolicy {
    /// Create a policy with the given selectivity threshold.
    pub fn new(threshold: f64) -> Self {
        Self { threshold }
    }
}

impl CacheEngagementPolicy for SelectivityThresholdPolicy {
    fn decide(&self, ctx: &EngagementContext) -> EngagementDecision {
        if ctx.estimated_selectivity < self.threshold && ctx.has_predicate {
            EngagementDecision::DelegateToParquet
        } else {
            EngagementDecision::UseLiquidCache
        }
    }
}

/// Always use LC regardless of selectivity.
#[derive(Debug, Clone)]
pub struct AlwaysEngagePolicy;

impl CacheEngagementPolicy for AlwaysEngagePolicy {
    fn decide(&self, _ctx: &EngagementContext) -> EngagementDecision {
        EngagementDecision::UseLiquidCache
    }
}

/// Never use LC — always delegate to plain parquet.
#[derive(Debug, Clone)]
pub struct NeverEngagePolicy;

impl CacheEngagementPolicy for NeverEngagePolicy {
    fn decide(&self, _ctx: &EngagementContext) -> EngagementDecision {
        EngagementDecision::DelegateToParquet
    }
}

/// Returns the default engagement policy.
pub fn default_engagement_policy() -> Arc<dyn CacheEngagementPolicy> {
    Arc::new(SelectivityThresholdPolicy::default())
}
