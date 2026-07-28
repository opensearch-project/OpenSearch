/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Optional Liquid Cache integration — in-memory decoded-batch cache for
//! Parquet scans.
//!
//! This whole module is gated behind the `liquid_cache` cargo feature. When the
//! feature is off (the default), [`maybe_wrap_parquet_source`] is an inlined
//! no-op that hands the plain `ParquetSource` back, and none of the liquid cache
//! crates are compiled or even resolved. When the feature is on, an eligible
//! indexed scan (all projected columns numeric/date/timestamp/boolean, within a
//! column budget, and no string/binary predicate column) is wrapped with
//! `LiquidParquetSource`, which serves decoded batches from / populates them into
//! the process-global cache.

use std::sync::Arc;

use datafusion::datasource::physical_plan::{FileSource, ParquetSource};

use crate::indexed_table::parquet_bridge::RowGroupStreamConfig;

/// Wrap `parquet_source` with liquid cache when the feature is enabled and the
/// scan is eligible; otherwise return the source unchanged. The return type is
/// the erased `Arc<dyn FileSource>` accepted by `FileScanConfigBuilder`, so both
/// branches are interchangeable at the call site.
#[cfg(not(feature = "liquid_cache"))]
#[inline]
pub(crate) fn maybe_wrap_parquet_source(
    parquet_source: ParquetSource,
    _config: &RowGroupStreamConfig,
) -> Arc<dyn FileSource> {
    Arc::new(parquet_source)
}

#[cfg(feature = "liquid_cache")]
pub(crate) fn maybe_wrap_parquet_source(
    parquet_source: ParquetSource,
    config: &RowGroupStreamConfig,
) -> Arc<dyn FileSource> {
    match runtime::LiquidRuntime::cache_ref_if_enabled() {
        Some(cache_ref) if runtime::scan_is_eligible(config) => {
            let liquid_source = liquid_cache_datafusion::LiquidParquetSource::from_parquet_source(
                parquet_source,
                cache_ref,
            );
            Arc::new(liquid_source)
        }
        _ => Arc::new(parquet_source),
    }
}

#[cfg(feature = "liquid_cache")]
mod runtime {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::OnceLock;

    use datafusion::arrow::datatypes::DataType;
    use liquid_cache_datafusion::LiquidCacheParquetRef;

    use crate::indexed_table::parquet_bridge::RowGroupStreamConfig;

    /// Liquid cache batch size — must be a power of two (upstream default).
    const LIQUID_CACHE_BATCH_SIZE: usize = 8192;

    static INSTANCE: OnceLock<LiquidCacheParquetRef> = OnceLock::new();
    static MAX_COLUMNS: AtomicU32 = AtomicU32::new(10);

    /// Column budget above which a scan is not delegated to liquid cache.
    pub(super) fn max_columns() -> usize {
        MAX_COLUMNS.load(Ordering::Relaxed) as usize
    }

    pub(super) fn set_max_columns(value: usize) {
        MAX_COLUMNS.store(value as u32, Ordering::Relaxed);
    }

    /// The process-global cache reference, if liquid cache has been initialized.
    pub(super) fn cache_ref_if_enabled() -> Option<LiquidCacheParquetRef> {
        INSTANCE.get().cloned()
    }

    /// Install the process-global cache reference. First call wins.
    pub(super) fn set_cache_ref(cache_ref: LiquidCacheParquetRef) {
        let _ = INSTANCE.set(cache_ref);
    }

    /// A scan is eligible when every projected column is numeric/date/timestamp/
    /// boolean, the projection is non-empty and within the column budget, and no
    /// predicate column is string/binary (parity with the original engagement
    /// gate — strings were never cacheable in-process).
    pub(super) fn scan_is_eligible(config: &RowGroupStreamConfig) -> bool {
        let all_numeric_projection = config.projection.as_ref().is_some_and(|proj| {
            !proj.is_empty()
                && proj.len() <= max_columns()
                && proj.iter().all(|&idx| {
                    config
                        .full_schema
                        .fields()
                        .get(idx)
                        .is_some_and(|f| is_cacheable(f.data_type()))
                })
        });
        if all_numeric_projection == false {
            return false;
        }
        let predicate_has_string = config.predicate.as_ref().is_some_and(|pred| {
            datafusion::physical_expr::utils::collect_columns(pred)
                .iter()
                .any(|col| {
                    config
                        .full_schema
                        .fields()
                        .get(col.index())
                        .is_some_and(|f| is_string_or_binary(f.data_type()))
                })
        });
        predicate_has_string == false
    }

    fn is_cacheable(dt: &DataType) -> bool {
        dt.is_numeric()
            || matches!(
                dt,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Boolean
            )
    }

    fn is_string_or_binary(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Utf8
                | DataType::Utf8View
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::BinaryView
                | DataType::LargeBinary
        )
    }
}

/// Re-exports for the initialization path (used by the runtime bootstrap when
/// the feature is enabled). No-ops are not provided for the feature-off build
/// because the initializer is itself feature-gated.
#[cfg(feature = "liquid_cache")]
pub(crate) use runtime::{set_cache_ref, set_max_columns};
