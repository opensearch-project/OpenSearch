/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cancellation helpers for DataFusion query tasks.
//!
//! The cancellation token itself lives in [`crate::query_tracker::QueryTracker`].
//! This module provides `select!`-based helpers that race a future against a token.

use std::future::Future;
use tokio_util::sync::CancellationToken;

/// Race a future against a cancellation token. Returns a cancellation error string
/// if the token fires first. Pass `None` for non-cancellable queries.
///
/// The select is `biased` toward the token branch so that once cancellation is
/// signalled the cancellation result wins even when the inner future is also
/// ready (e.g. it produced an error from an aborted CPU task). Without this,
/// `cancel_query` calls that abort a CPU task can race the resulting "Worker
/// gone" error to the inner branch and surface it instead of the cancellation.
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
                biased;
                _ = token.cancelled() => Err(format!("Query {} cancelled", context_id)),
                result = fut => result.map_err(|e| e.to_string()),
            }
        }
        None => fut.await.map_err(|e| e.to_string()),
    }
}

/// Variant that returns a sentinel value on cancellation instead of an error.
/// Used by `stream_next` where `None` signals cancellation/EOF.
///
/// `biased` toward the token branch — see [`cancellable`] for the rationale.
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
                biased;
                _ = token.cancelled() => Ok(sentinel),
                result = fut => result.map_err(|e| e.to_string()),
            }
        }
        None => fut.await.map_err(|e| e.to_string()),
    }
}
