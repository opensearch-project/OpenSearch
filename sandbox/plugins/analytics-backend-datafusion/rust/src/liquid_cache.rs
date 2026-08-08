/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Liquid Cache hook for the indexed scan path; gated behind the `liquid_cache` feature (off by default = inlined no-op). Policy and cache live in the `opensearch-liquid-cache` crate.

use std::sync::Arc;

use datafusion::datasource::physical_plan::{FileSource, ParquetSource};

use crate::indexed_table::parquet_bridge::RowGroupStreamConfig;

/// Wrap the source with LiquidParquetSource when the `liquid_cache` feature is
/// compiled in and the cache is enabled for an eligible scan; otherwise (and
/// always in the default build) return the plain source unchanged.
pub(crate) fn maybe_wrap_parquet_source(
    parquet_source: ParquetSource,
    config: &RowGroupStreamConfig,
) -> Arc<dyn FileSource> {
    #[cfg(feature = "liquid_cache")]
    {
        use opensearch_liquid_cache::{
            indexed_scan_eligible, LiquidOnlyRuntime, LiquidParquetSource,
        };

        if LiquidOnlyRuntime::is_enabled_globally() {
            if let Some(cache_ref) = LiquidOnlyRuntime::cache_ref_globally() {
                if indexed_scan_eligible(
                    &config.full_schema,
                    config.projection.as_deref(),
                    config.predicate.as_ref(),
                ) {
                    return Arc::new(LiquidParquetSource::from_parquet_source(
                        parquet_source,
                        cache_ref,
                    ));
                }
            }
        }
    }
    #[cfg(not(feature = "liquid_cache"))]
    let _ = config;

    Arc::new(parquet_source)
}
