/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;
use std::sync::OnceLock;

use datafusion::datasource::physical_plan::{FileSource, ParquetSource};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use opensearch_query_spi::{ParquetEngagementCtx, SessionOptimizerProviderRef};

static PROVIDERS: OnceLock<Vec<SessionOptimizerProviderRef>> = OnceLock::new();

/// # Safety
/// Every non-zero entry must be a live handle from `into_handle` that outlives
/// the registry's use.
pub unsafe fn install(handles: &[i64]) {
    let providers: Vec<SessionOptimizerProviderRef> = handles
        .iter()
        .copied()
        .filter(|&h| h != 0)
        .map(|h| opensearch_query_spi::borrow_handle(h))
        .collect();
    let _ = PROVIDERS.set(providers);
}

pub fn enabled_optimizer_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    match PROVIDERS.get() {
        Some(providers) => providers
            .iter()
            .filter(|p| p.enabled())
            .map(|p| p.physical_optimizer_rule())
            .collect(),
        None => Vec::new(),
    }
}

pub fn wrap_parquet_source(
    mut source: ParquetSource,
    ctx: &ParquetEngagementCtx<'_>,
) -> Arc<dyn FileSource> {
    if let Some(providers) = PROVIDERS.get() {
        for p in providers {
            match p.wrap_parquet_source(source, ctx) {
                Ok(wrapped) => return wrapped,
                Err(returned) => source = returned,
            }
        }
    }
    Arc::new(source)
}
