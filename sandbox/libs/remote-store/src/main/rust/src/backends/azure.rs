/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Azure Blob Storage backend configuration and builder.

use std::sync::Arc;

use object_store::azure::MicrosoftAzureBuilder;
use object_store::ObjectStore;
use serde::Deserialize;

use crate::factory::{build_retry_config, StoreFactoryError};

/// Configuration for an Azure Blob Storage remote store.
#[derive(Debug, Deserialize)]
pub struct AzureConfig {
    /// Azure storage account name.
    pub account: String,
    /// Azure container name.
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

/// Build an Azure [`ObjectStore`] from config.
pub fn build(config_json: &str) -> Result<Arc<dyn ObjectStore>, StoreFactoryError> {
    let config: AzureConfig =
        serde_json::from_str(config_json).map_err(|e| StoreFactoryError::ConfigParse {
            store_type: "azure".to_string(),
            reason: e.to_string(),
        })?;

    let mut builder = MicrosoftAzureBuilder::new()
        .with_account(&config.account)
        .with_container_name(&config.container);

    if let Some(ref key) = config.access_key {
        builder = builder.with_access_key(key);
    }
    if let Some(ref sas) = config.sas_token {
        // Parse SAS token string into key-value pairs for the builder
        let pairs: Vec<(String, String)> = sas
            .trim_start_matches('?')
            .split('&')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                match (parts.next(), parts.next()) {
                    (Some(k), Some(v)) => Some((k.to_string(), v.to_string())),
                    _ => None,
                }
            })
            .collect();
        builder = builder.with_sas_authorization(pairs);
    }
    if config.use_azure_cli == Some(true) {
        builder = builder.with_use_azure_cli(true);
    }
    if let Some(retry) = build_retry_config(config.max_retries, config.retry_timeout_ms) {
        builder = builder.with_retry(retry);
    }

    let store = builder
        .build()
        .map_err(|e| StoreFactoryError::BuildFailed {
            store_type: "azure".to_string(),
            reason: e.to_string(),
        })?;
    Ok(Arc::new(store))
}
