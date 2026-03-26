/* SPDX-License-Identifier: Apache-2.0 */

use std::{
    fs,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use datafusion::{
    common::DataFusionError,
    execution::runtime_env::RuntimeEnv,
    optimizer::OptimizerRule,
    physical_optimizer::PhysicalOptimizerRule,
    prelude::SessionConfig,
};

use liquid_cache_datafusion_local::LiquidCacheLocalBuilder;
use liquid_cache_datafusion_local::storage::cache::LiquidCache;
use liquid_cache_datafusion_local::storage::cache_policies::LiquidPolicy;
use vectorized_exec_spi::{log_info, log_error};

pub const LIQUID_CACHE_DIR: &str = "/tmp/opensearch/liquid_cache";

/// Holds global Liquid Cache runtime and optimizer.
/// `_cache_ref` MUST be kept alive for the lifetime of the process.
pub struct LiquidOnlyRuntime {
    runtime_env: Arc<RuntimeEnv>,
    /// The physical optimizer rule that rewrites Parquet scans to use Liquid Cache.
    optimizer: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    /// The logical optimizer rule for lineage tracking.
    lineage_optimizer: Arc<dyn OptimizerRule + Send + Sync>,
    /// Keep the cache ref alive for the lifetime of the process.
    _cache_ref: Box<dyn std::any::Any + Send + Sync>,
    /// Direct handle to the underlying cache storage for stats.
    cache_storage: Arc<LiquidCache>,
}

static LIQUID_ONLY: OnceLock<Result<LiquidOnlyRuntime, String>> = OnceLock::new();

impl LiquidOnlyRuntime {
    /// Initialize Liquid Cache once per process and return the global runtime.
    pub fn init(
        max_cache_bytes: u64,
    ) -> Result<&'static LiquidOnlyRuntime, DataFusionError> {
        let result = LIQUID_ONLY.get_or_init(|| {
            let cache_dir = PathBuf::from(LIQUID_CACHE_DIR);
            if let Err(e) = fs::create_dir_all(&cache_dir) {
                return Err(format!(
                    "Failed to create liquid cache dir {:?}: {}",
                    cache_dir, e
                ));
            }

            let bootstrap_cfg = SessionConfig::new();

            let (liquid_ctx, liquid_cache_ref) = match LiquidCacheLocalBuilder::new()
                .with_max_cache_bytes(max_cache_bytes as usize)
                .with_cache_dir(cache_dir)
                .with_cache_policy(Box::new(LiquidPolicy::new()))
                .build(bootstrap_cfg)
            {
                Ok(result) => result,
                Err(e) => return Err(format!("Liquid cache build failed: {}", e)),
            };

            // Grab a handle to the underlying cache storage for stats before boxing
            let cache_storage = liquid_cache_ref.storage().clone();

            // Extract the optimizer rules from the Liquid Cache SessionState
            // so we can apply them to per-query SessionStates
            let state = liquid_ctx.state();

            let physical_rules = state.physical_optimizers();
            let liquid_optimizer = physical_rules
                .iter()
                .find(|r| r.name() == "LocalModeLiquidCacheOptimizer")
                .cloned()
                .ok_or_else(|| "LocalModeLiquidCacheOptimizer not found in Liquid Cache session state".to_string())?;

            let optimizer_rules = state.optimizers();
            let lineage_optimizer = optimizer_rules
                .iter()
                .find(|r| r.name() == "LineageOptimizer")
                .cloned()
                .ok_or_else(|| "LineageOptimizer not found in Liquid Cache session state".to_string())?;

            Ok(LiquidOnlyRuntime {
                runtime_env: liquid_ctx.runtime_env(),
                optimizer: liquid_optimizer,
                lineage_optimizer,
                _cache_ref: Box::new(liquid_cache_ref),
                cache_storage,
            })
        });

        result
            .as_ref()
            .map_err(|e| DataFusionError::Execution(e.clone()))
    }

    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime_env.clone()
    }

    pub fn optimizer(&self) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
        self.optimizer.clone()
    }

    pub fn lineage_optimizer(&self) -> Arc<dyn OptimizerRule + Send + Sync> {
        self.lineage_optimizer.clone()
    }

    /// Log current cache statistics.
    pub fn log_stats(&self) {
        let stats = self.cache_storage.stats();
        log_info!(
            "[LiquidCache] Stats: entries={}, memory={}/{} bytes, disk={} bytes, \
             arrow_mem={}, liquid_mem={}, squeezed_mem={}, disk_liquid={}, disk_arrow={}",
            stats.total_entries,
            stats.memory_usage_bytes,
            stats.max_cache_bytes,
            stats.disk_usage_bytes,
            stats.memory_arrow_entries,
            stats.memory_liquid_entries,
            stats.memory_squeezed_liquid_entries,
            stats.disk_liquid_entries,
            stats.disk_arrow_entries,
        );
    }

    /// Log cache stats if Liquid Cache has been initialized.
    pub fn log_stats_if_initialized() {
        if let Some(Ok(runtime)) = LIQUID_ONLY.get() {
            runtime.log_stats();
        }
    }

    /// Reset the cache, clearing all in-memory entries and disk files.
    pub fn reset_cache(&self) {
        self.cache_storage.reset();

        let cache_dir = PathBuf::from(LIQUID_CACHE_DIR);
        if cache_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&cache_dir) {
                log_error!("[LiquidCache] Failed to clean cache dir: {}", e);
            }
        }
        if let Err(e) = fs::create_dir_all(&cache_dir) {
            log_error!("[LiquidCache] Failed to recreate cache dir: {}", e);
        }

        log_info!("[LiquidCache] Cache cleared");
        self.log_stats();
    }

    /// Reset cache if Liquid Cache has been initialized.
    pub fn reset_cache_if_initialized() {
        if let Some(Ok(runtime)) = LIQUID_ONLY.get() {
            runtime.reset_cache();
        }
    }
}
