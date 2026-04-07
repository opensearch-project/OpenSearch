/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Central cancellation utilities for DataFusion query tasks.
use std::future::Future;
use datafusion::common::DataFusionError;
use tokio_util::sync::CancellationToken;

/// Run a future with cancellation support. If the token fires before the
/// future completes, returns a cancellation error immediately.
///
/// Pass `None` for non-cancellable queries — the future runs directly.
pub async fn cancellable<F, T>(
    token: Option<&CancellationToken>,
    task_id: i64,
    fut: F,
) -> Result<T, DataFusionError>
where
    F: Future<Output = Result<T, DataFusionError>>,
{
    match token {
        Some(token) => {
            tokio::select! {
                result = fut => result,
                _ = token.cancelled() => {
                    Err(DataFusionError::Execution(
                        format!("Query {} cancelled", task_id)
                    ))
                }
            }
        }
        None => fut.await,
    }
}

/// Run a future with cancellation support. If the token fires before the
/// future completes, returns a sentinel value immediately.
///
/// Pass `None` for non-cancellable queries — the future runs directly.
pub async fn cancellable_or<F, T>(
    token: Option<&CancellationToken>,
    sentinel: T,
    fut: F,
) -> Result<T, DataFusionError>
where
    F: Future<Output = Result<T, DataFusionError>>,
{
    match token {
        Some(token) => {
            tokio::select! {
                result = fut => result,
                _ = token.cancelled() => Ok(sentinel),
            }
        }
        None => fut.await,
    }
}
