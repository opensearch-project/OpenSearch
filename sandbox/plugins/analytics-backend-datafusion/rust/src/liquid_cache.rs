/* SPDX-License-Identifier: Apache-2.0 */

//! Liquid Cache integration — in-memory decoded-batch cache for Parquet scans.
//!
//! Backed by the vendored in-memory liquid-cache subset
//! (`sandbox/libs/dataformat-native/rust/liquid-cache`). There is no disk
//! tier: entries are transcoded to the Liquid format under memory pressure
//! and evicted when the budget is exhausted (`TranscodeEvict`).

use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc, OnceLock,
};

use datafusion::{common::DataFusionError, physical_optimizer::PhysicalOptimizerRule};

use liquid_cache::cache::{CachePolicy, LiquidCache, TranscodeEvict};
use liquid_cache::cache_policies::{LiquidPolicy, LruPolicy};
use liquid_cache_datafusion::{LiquidCacheParquet, LiquidCacheParquetRef, LocalModeOptimizer};
use native_bridge_common::log_debug;

const EVICTION_POLICY_LRU: &str = "lru";

/// Liquid cache batch size — must be a power of two (upstream default).
const LIQUID_CACHE_BATCH_SIZE: usize = 8192;

static INSTANCE: OnceLock<Result<LiquidOnlyRuntime, String>> = OnceLock::new();

// Dynamic tuning knobs — updated via cluster settings without restart.
// Selectivity threshold stored as permille (800 = 0.800) to avoid floating-point atomics.
static LC_SELECTIVITY_THRESHOLD_PERMILLE: AtomicU32 = AtomicU32::new(800);
static LC_MAX_COLUMNS: AtomicU32 = AtomicU32::new(10);

pub fn lc_selectivity_threshold() -> f64 {
    LC_SELECTIVITY_THRESHOLD_PERMILLE.load(Ordering::Relaxed) as f64 / 1000.0
}

pub fn lc_max_columns() -> usize {
    LC_MAX_COLUMNS.load(Ordering::Relaxed) as usize
}

pub fn set_lc_selectivity_threshold(value: f64) {
    let permille = (value * 1000.0) as u32;
    LC_SELECTIVITY_THRESHOLD_PERMILLE.store(permille, Ordering::Relaxed);
}

pub fn set_lc_max_columns(value: usize) {
    LC_MAX_COLUMNS.store(value as u32, Ordering::Relaxed);
}

pub struct LiquidOnlyRuntime {
    optimizer: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    cache_ref: LiquidCacheParquetRef,
    storage: Arc<LiquidCache>,
    enabled: AtomicBool,
}

impl LiquidOnlyRuntime {
    pub fn init(
        max_cache_bytes: u64,
        eviction_policy: &str,
    ) -> Result<&'static Self, DataFusionError> {
        INSTANCE
            .get_or_init(|| Self::build(max_cache_bytes, eviction_policy))
            .as_ref()
            .map_err(|e| DataFusionError::Execution(e.clone()))
    }

    fn build(max_cache_bytes: u64, eviction_policy: &str) -> Result<Self, String> {
        let policy: Box<dyn CachePolicy> = match eviction_policy {
            EVICTION_POLICY_LRU => Box::new(LruPolicy::new()),
            _ => Box::new(LiquidPolicy::new()),
        };

        let cache_ref: LiquidCacheParquetRef = Arc::new(LiquidCacheParquet::new(
            LIQUID_CACHE_BATCH_SIZE,
            max_cache_bytes as usize,
            policy,
            Box::new(TranscodeEvict),
        ));
        let optimizer = Arc::new(LocalModeOptimizer::new(cache_ref.clone()));

        Ok(Self {
            optimizer,
            storage: cache_ref.storage().clone(),
            cache_ref,
            enabled: AtomicBool::new(true),
        })
    }

    pub fn optimizer(&self) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
        self.optimizer.clone()
    }

    pub fn cache_ref(&self) -> &LiquidCacheParquetRef {
        &self.cache_ref
    }

    pub fn cache_ref_globally() -> Option<LiquidCacheParquetRef> {
        Self::get().map(|rt| rt.cache_ref.clone())
    }

    pub fn optimizer_globally() -> Option<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        Self::get().map(|rt| rt.optimizer())
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn set_max_memory_bytes(&self, bytes: usize) {
        self.storage.budget().set_max_memory_bytes(bytes);
    }

    pub fn reset_cache(&self) {
        // Safety: callers only reset via the explicit REST clear action;
        // concurrent queries may observe a cold cache, which is benign for
        // a read-through cache (they fall back to Parquet decode).
        unsafe { self.cache_ref.reset() };
        let stats = self.storage.stats();
        log_debug!(
            "[LiquidCache] cache cleared: entries={}, mem_usage={} bytes",
            stats.total_entries,
            stats.memory_usage_bytes
        );
    }

    pub fn log_stats(&self) {
        let s = self.storage.stats();
        log_debug!(
            "[LiquidCache] entries={}, mem={}/{}, arrow={}({} B), liquid={}({} B)",
            s.total_entries,
            s.memory_usage_bytes,
            s.max_memory_bytes,
            s.memory_arrow_entries,
            s.memory_arrow_bytes,
            s.memory_liquid_entries,
            s.memory_liquid_bytes,
        );
        let mem_pct = if s.max_memory_bytes > 0 {
            (s.memory_usage_bytes as f64 / s.max_memory_bytes as f64 * 100.0) as u64
        } else {
            0
        };
        log_debug!(
            "[LiquidCache] hits={}, misses={}, predicate_evals={}, mem_evictions={}, transcodes={}, mem_pressure={}%",
            s.runtime.cache_hit,
            s.runtime.cache_miss,
            s.runtime.eval_predicate,
            s.runtime.memory_evictions,
            s.runtime.transcodes,
            mem_pct,
        );
    }

    fn get() -> Option<&'static Self> {
        INSTANCE.get().and_then(|r| r.as_ref().ok())
    }

    pub fn is_enabled_globally() -> bool {
        Self::get().map(|rt| rt.is_enabled()).unwrap_or(false)
    }

    pub fn set_enabled_globally(enabled: bool) {
        if let Some(rt) = Self::get() {
            rt.set_enabled(enabled);
        }
    }

    pub fn set_max_memory_bytes_globally(bytes: usize) {
        if let Some(rt) = Self::get() {
            rt.set_max_memory_bytes(bytes);
        }
    }

    pub fn log_stats_if_initialized() {
        if let Some(rt) = Self::get() {
            rt.log_stats();
        }
    }

    pub fn reset_cache_if_initialized() {
        if let Some(rt) = Self::get() {
            rt.reset_cache();
        }
    }
}
