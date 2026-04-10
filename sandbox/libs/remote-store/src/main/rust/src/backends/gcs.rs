/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Google Cloud Storage backend configuration and builder.

use std::sync::Arc;

use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::ObjectStore;
use serde::Deserialize;

use crate::factory::{build_retry_config, StoreFactoryError};

/// Configuration for a Google Cloud Storage remote store.
#[derive(Debug, Deserialize)]
pub struct GcsConfig {
    /// GCS bucket name.
    pub bucket: String,
    /// Path to service account JSON key file.
    pub service_account_key: Option<String>,
    /// Maximum number of retries for failed requests.
    pub max_retries: Option<usize>,
    /// Retry timeout in milliseconds.
    pub retry_timeout_ms: Option<u64>,
}

/// Build a GCS [`ObjectStore`] from config.
pub fn build(config_json: &str) -> Result<Arc<dyn ObjectStore>, StoreFactoryError> {
    let config: GcsConfig =
        serde_json::from_str(config_json).map_err(|e| StoreFactoryError::ConfigParse {
            store_type: "gcs".to_string(),
            reason: e.to_string(),
        })?;

    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&config.bucket);

    if let Some(ref key_path) = config.service_account_key {
        builder = builder.with_service_account_key(key_path);
    }
    if let Some(retry) = build_retry_config(config.max_retries, config.retry_timeout_ms) {
        builder = builder.with_retry(retry);
    }

    let store = builder
        .build()
        .map_err(|e| StoreFactoryError::BuildFailed {
            store_type: "gcs".to_string(),
            reason: e.to_string(),
        })?;
    Ok(Arc::new(store))
}
