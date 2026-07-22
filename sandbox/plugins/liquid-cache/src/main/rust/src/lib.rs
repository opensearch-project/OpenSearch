/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

pub mod ffi;
pub mod runtime;

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::physical_plan::{FileSource, ParquetSource};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use opensearch_query_spi::{ParquetEngagementCtx, SessionOptimizerProvider};

pub use runtime::LiquidOnlyRuntime;

fn engages(ctx: &ParquetEngagementCtx<'_>) -> bool {
    let max_cols = runtime::lc_indexed_max_columns();
    let all_numeric_projection = ctx.projection.map_or(false, |proj| {
        !proj.is_empty()
            && proj.len() <= max_cols
            && proj.iter().all(|&idx| {
                ctx.full_schema.fields().get(idx).map_or(false, |f| {
                    f.data_type().is_numeric()
                        || matches!(
                            f.data_type(),
                            DataType::Date32
                                | DataType::Date64
                                | DataType::Timestamp(_, _)
                                | DataType::Boolean
                        )
                })
            })
    });
    let predicate_has_string = ctx.predicate.map_or(false, |pred| {
        let referenced = datafusion::physical_expr::utils::collect_columns(pred);
        referenced.iter().any(|col| {
            ctx.full_schema
                .fields()
                .get(col.index())
                .map_or(false, |f| {
                    matches!(
                        f.data_type(),
                        DataType::Utf8
                            | DataType::Utf8View
                            | DataType::LargeUtf8
                            | DataType::Binary
                            | DataType::BinaryView
                            | DataType::LargeBinary
                    )
                })
        })
    });
    all_numeric_projection && !predicate_has_string
}

/// [`SessionOptimizerProvider`] backed by the process-global [`LiquidOnlyRuntime`].
pub struct LiquidOptimizerProvider {
    optimizer: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
}

impl LiquidOptimizerProvider {
    fn new(optimizer: Arc<dyn PhysicalOptimizerRule + Send + Sync>) -> Self {
        Self { optimizer }
    }
}

impl SessionOptimizerProvider for LiquidOptimizerProvider {
    fn physical_optimizer_rule(&self) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
        self.optimizer.clone()
    }

    fn enabled(&self) -> bool {
        LiquidOnlyRuntime::is_enabled_globally()
    }

    fn wrap_parquet_source(
        &self,
        source: ParquetSource,
        ctx: &ParquetEngagementCtx<'_>,
    ) -> Result<Arc<dyn FileSource>, ParquetSource> {
        if !LiquidOnlyRuntime::is_enabled_globally() || !engages(ctx) {
            return Err(source);
        }
        match LiquidOnlyRuntime::cache_ref_globally() {
            Some(cache_ref) => {
                let liquid = liquid_cache_datafusion::LiquidParquetSource::from_parquet_source(
                    source, cache_ref,
                );
                Ok(Arc::new(liquid))
            }
            None => Err(source),
        }
    }
}
