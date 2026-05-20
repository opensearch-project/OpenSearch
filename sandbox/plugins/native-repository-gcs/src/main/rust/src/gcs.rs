/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Google Cloud Storage backend configuration and builder.
//!
//! Credentials are NOT accepted via config JSON — use Application Default
//! Credentials (ADC) or pass a custom `GcpCredentialProvider`.

use std::sync::Arc;
use std::time::Duration;

use object_store::gcp::{GcpCredentialProvider, GoogleCloudStorageBuilder};
use object_store::{ObjectStore, RetryConfig};
use serde::Deserialize;

/// Configuration for a Google Cloud Storage remote store.
///
/// Credentials are intentionally excluded — authentication is handled
/// via Application Default Credentials (ADC) or a custom `GcpCredentialProvider`
/// passed to [`build`].
#[derive(Debug, Deserialize)]
pub struct GcsConfig {
    /// GCS bucket name (required).
    pub bucket: String,
    /// Maximum number of retries for failed requests.
    pub max_retries: Option<usize>,
    /// Retry timeout in milliseconds.
    pub retry_timeout_ms: Option<u64>,
}

/// Build a GCS [`ObjectStore`] from JSON config.
///
/// If `credentials` is `Some`, it is used for authentication.
/// If `None`, Application Default Credentials (ADC) are used.
pub fn build(
    config_json: &str,
    credentials: Option<GcpCredentialProvider>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let config: GcsConfig = serde_json::from_str(config_json)?;
    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&config.bucket);

    if let Some(creds) = credentials {
        builder = builder.with_credentials(creds);
    }

    if config.max_retries.is_some() || config.retry_timeout_ms.is_some() {
        let mut retry = RetryConfig::default();
        if let Some(m) = config.max_retries { retry.max_retries = m; }
        if let Some(ms) = config.retry_timeout_ms { retry.retry_timeout = Duration::from_millis(ms); }
        builder = builder.with_retry(retry);
    }

    Ok(Arc::new(builder.build()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_missing_bucket_returns_error() {
        assert!(build(r#"{}"#, None).is_err());
    }

    #[test]
    fn test_build_invalid_json_returns_error() {
        assert!(build("not json", None).is_err());
    }

    #[test]
    fn test_build_with_retry_config() {
        let config = r#"{"bucket":"b","max_retries":3,"retry_timeout_ms":10000}"#;
        let result = build(config, None);
        if let Err(e) = &result {
            assert!(!e.to_string().contains("parse"), "should not be a parse error: {}", e);
        }
    }
}
