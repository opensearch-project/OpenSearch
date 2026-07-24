/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::{FileSource, ParquetSource};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;

pub struct ParquetEngagementCtx<'a> {
    pub full_schema: &'a SchemaRef,
    pub projection: Option<&'a [usize]>,
    pub predicate: Option<&'a Arc<dyn PhysicalExpr>>,
}

pub trait SessionOptimizerProvider: Send + Sync {
    fn physical_optimizer_rule(&self) -> Arc<dyn PhysicalOptimizerRule + Send + Sync>;

    fn enabled(&self) -> bool;

    fn wrap_parquet_source(
        &self,
        source: ParquetSource,
        _ctx: &ParquetEngagementCtx<'_>,
    ) -> Result<Arc<dyn FileSource>, ParquetSource> {
        Err(source)
    }
}

pub type SessionOptimizerProviderRef = Arc<dyn SessionOptimizerProvider>;

pub fn into_handle(provider: SessionOptimizerProviderRef) -> i64 {
    Box::into_raw(Box::new(provider)) as i64
}

/// # Safety
/// `handle` must be non-zero from [`into_handle`] and not yet freed.
pub unsafe fn borrow_handle(handle: i64) -> SessionOptimizerProviderRef {
    debug_assert!(handle != 0, "borrow_handle called with null handle");
    let boxed = &*(handle as *const SessionOptimizerProviderRef);
    boxed.clone()
}

/// # Safety
/// `handle` must be non-zero from [`into_handle`] and not previously freed;
/// it is dangling afterwards.
pub unsafe fn from_handle(handle: i64) -> SessionOptimizerProviderRef {
    debug_assert!(handle != 0, "from_handle called with null handle");
    *Box::from_raw(handle as *mut SessionOptimizerProviderRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Result as DFResult;
    use datafusion::config::ConfigOptions;
    use datafusion::physical_plan::ExecutionPlan;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug)]
    struct NoopRule;

    impl PhysicalOptimizerRule for NoopRule {
        fn optimize(
            &self,
            plan: Arc<dyn ExecutionPlan>,
            _config: &ConfigOptions,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            Ok(plan)
        }
        fn name(&self) -> &str {
            "noop"
        }
        fn schema_check(&self) -> bool {
            true
        }
    }

    struct TestProvider {
        enabled: AtomicBool,
    }

    impl SessionOptimizerProvider for TestProvider {
        fn physical_optimizer_rule(&self) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
            Arc::new(NoopRule)
        }
        fn enabled(&self) -> bool {
            self.enabled.load(Ordering::Relaxed)
        }
    }

    #[test]
    fn handle_round_trip_borrow_then_free() {
        let provider: SessionOptimizerProviderRef = Arc::new(TestProvider {
            enabled: AtomicBool::new(true),
        });
        let handle = into_handle(provider);
        assert_ne!(handle, 0);

        unsafe {
            let borrowed = borrow_handle(handle);
            assert!(borrowed.enabled());
            assert_eq!(borrowed.physical_optimizer_rule().name(), "noop");
            let borrowed2 = borrow_handle(handle);
            assert!(borrowed2.enabled());
        }

        unsafe {
            let owned = from_handle(handle);
            assert!(owned.enabled());
        }
    }
}
