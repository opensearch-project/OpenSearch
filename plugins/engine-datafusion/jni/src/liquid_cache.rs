/* SPDX-License-Identifier: Apache-2.0 */

use std::{
    fs,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use datafusion::{
    common::DataFusionError,
    execution::runtime_env::RuntimeEnv,
    prelude::SessionConfig,
};

use liquid_cache_datafusion_local::LiquidCacheLocalBuilder;

pub const LIQUID_CACHE_DIR: &str = "/var/lib/opensearch/liquid_cache";

/// Holds global Liquid Cache runtime.
/// `_cache_ref` MUST be kept alive for the lifetime of the process.
pub struct LiquidOnlyRuntime {
    runtime_env: Arc<RuntimeEnv>,
    _cache_ref: Box<dyn std::any::Any + Send + Sync>,
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

            // Only used to build the liquid cache ctx.
            // Per-query config is still used when building SessionState.
            let bootstrap_cfg = SessionConfig::new();

            let (liquid_ctx, liquid_cache_ref) = match LiquidCacheLocalBuilder::new()
                .with_max_cache_bytes(max_cache_bytes as usize)
                .with_cache_dir(cache_dir)
                .build(bootstrap_cfg)
            {
                Ok(result) => result,
                Err(e) => return Err(format!("Liquid cache build failed: {}", e)),
            };

            Ok(LiquidOnlyRuntime {
                runtime_env: liquid_ctx.runtime_env(),
                _cache_ref: Box::new(liquid_cache_ref),
            })
        });

        result
            .as_ref()
            .map_err(|e| DataFusionError::Execution(e.clone()))
    }

    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime_env.clone()
    }
}
