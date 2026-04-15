/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Azure Blob Storage backend configuration and builder.
//!
//! Credentials are NOT accepted via config JSON — use the default Azure
//! credential chain (managed identity, CLI, env vars) or pass a custom
//! `AzureCredentialProvider`.

use std::sync::Arc;
use std::time::Duration;

use object_store::azure::{AzureCredentialProvider, MicrosoftAzureBuilder};
use object_store::{ObjectStore, RetryConfig};
use serde::Deserialize;

/// Configuration for an Azure Blob Storage remote store.
///
/// Credentials are intentionally excluded — authentication is handled
/// via the default Azure credential chain or a custom `AzureCredentialProvider`
/// passed to [`build`].
#[derive(Debug, Deserialize)]
pub struct AzureConfig {
    /// Azure storage account name (required).
    pub account: String,
    /// Azure container name (required).
    pub container: String,
    /// Maximum number of retries for failed requests.
    pub max_retries: Option<usize>,
    /// Retry timeout in milliseconds.
    pub retry_timeout_ms: Option<u64>,
}

/// Build an Azure [`ObjectStore`] from JSON config.
///
/// If `credentials` is `Some`, it is used for authentication.
/// If `None`, the default Azure credential chain is used.
pub fn build(
    config_json: &str,
    credentials: Option<AzureCredentialProvider>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let config: AzureConfig = serde_json::from_str(config_json)?;
    let mut builder = MicrosoftAzureBuilder::new()
        .with_account(&config.account)
        .with_container_name(&config.container);

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
    fn test_build_missing_account_returns_error() {
        assert!(build(r#"{"container":"c"}"#, None).is_err());
    }

    #[test]
    fn test_build_minimal_config() {
        // Default credential chain — will fail at runtime without real Azure creds,
        // but the builder construction should succeed.
        let config = r#"{"account":"a","container":"c"}"#;
        assert!(build(config, None).is_ok());
    }

    #[test]
    fn test_build_with_retry_config() {
        let config = r#"{"account":"a","container":"c","max_retries":5,"retry_timeout_ms":20000}"#;
        assert!(build(config, None).is_ok());
    }
}
