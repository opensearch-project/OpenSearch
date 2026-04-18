/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Central cancellation utilities for DataFusion query tasks.

use std::future::Future;
use tokio_util::sync::CancellationToken;

/// Per-query cancellation state stored in the ACTIVE_QUERIES registry.
pub struct QueryCancellationContext {
    pub cancellation_token: CancellationToken,
}

impl QueryCancellationContext {
    pub fn new() -> Self {
        Self { cancellation_token: CancellationToken::new() }
    }
}

/// Race a future against a cancellation token. Returns a cancellation error string
/// if the token fires first. Pass `None` for non-cancellable queries.
pub async fn cancellable<F, T, E>(
    token: Option<&CancellationToken>,
    context_id: i64,
    fut: F,
) -> Result<T, String>
where
    F: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match token {
        Some(token) => {
            tokio::select! {
                result = fut => result.map_err(|e| e.to_string()),
                _ = token.cancelled() => Err(format!("Query {} cancelled", context_id)),
            }
        }
        None => fut.await.map_err(|e| e.to_string()),
    }
}

/// Variant that returns a sentinel value on cancellation instead of an error.
/// Used by `stream_next` where `None` signals cancellation/EOF.
pub async fn cancellable_or<F, T, E>(
    token: Option<&CancellationToken>,
    sentinel: T,
    fut: F,
) -> Result<T, String>
where
    F: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match token {
        Some(token) => {
            tokio::select! {
                result = fut => result.map_err(|e| e.to_string()),
                _ = token.cancelled() => Ok(sentinel),
            }
        }
        None => fut.await.map_err(|e| e.to_string()),
    }
}
