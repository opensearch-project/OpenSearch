/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Azure Blob Storage backend configuration and builder.

use std::sync::Arc;
use std::time::Duration;

use object_store::azure::{AzureCredentialProvider, MicrosoftAzureBuilder};
use object_store::{ObjectStore, RetryConfig};
use serde::Deserialize;

/// Configuration for an Azure Blob Storage remote store.
#[derive(Debug, Deserialize)]
pub struct AzureConfig {
    /// Azure storage account name (required).
    pub account: String,
    /// Azure container name (required).
    pub container: String,
    /// Access key for authentication.
    pub access_key: Option<String>,
    /// SAS token for authentication (query string format: "sv=...&sig=...").
    pub sas_token: Option<String>,
    /// Use Azure CLI credentials.
    pub use_azure_cli: Option<bool>,
    /// Maximum number of retries for failed requests.
    pub max_retries: Option<usize>,
    /// Retry timeout in milliseconds.
    pub retry_timeout_ms: Option<u64>,
}

/// Build an Azure [`ObjectStore`] from JSON config.
///
/// If `credentials` is `Some`, it overrides any static credentials in the config.
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
    } else {
        if let Some(ref k) = config.access_key { builder = builder.with_access_key(k); }
        if let Some(ref sas) = config.sas_token {
            let pairs: Vec<(String, String)> = sas
                .trim_start_matches('?')
                .split('&')
                .filter_map(|p| {
                    let mut parts = p.splitn(2, '=');
                    match (parts.next(), parts.next()) {
                        (Some(k), Some(v)) => Some((k.to_string(), v.to_string())),
                        _ => None,
                    }
                })
                .collect();
            builder = builder.with_sas_authorization(pairs);
        }
        if config.use_azure_cli == Some(true) { builder = builder.with_use_azure_cli(true); }
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
    fn test_build_with_access_key() {
        let config = r#"{"account":"a","container":"c","access_key":"dGVzdGtleQ=="}"#;
        assert!(build(config, None).is_ok());
    }

    #[test]
    fn test_build_with_sas_token() {
        let config = r#"{"account":"a","container":"c","sas_token":"sv=2020-08-04&ss=b&sig=abc"}"#;
        assert!(build(config, None).is_ok());
    }

    #[test]
    fn test_build_with_retry_config() {
        let config = r#"{"account":"a","container":"c","access_key":"dGVzdGtleQ==","max_retries":5,"retry_timeout_ms":20000}"#;
        assert!(build(config, None).is_ok());
    }
}
