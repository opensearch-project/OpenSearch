/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Local filesystem backend configuration and builder.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde::Deserialize;

use crate::factory::StoreFactoryError;

/// Configuration for a local filesystem remote store.
#[derive(Debug, Deserialize)]
pub struct FsConfig {
    /// Base directory path for the local filesystem store.
    pub base_path: String,
}

/// Build a local filesystem [`ObjectStore`] from config.
pub fn build(config_json: &str) -> Result<Arc<dyn ObjectStore>, StoreFactoryError> {
    let config: FsConfig =
        serde_json::from_str(config_json).map_err(|e| StoreFactoryError::ConfigParse {
            store_type: "fs".to_string(),
            reason: e.to_string(),
        })?;
    let store = LocalFileSystem::new_with_prefix(&config.base_path).map_err(|e| {
        StoreFactoryError::BuildFailed {
            store_type: "fs".to_string(),
            reason: e.to_string(),
        }
    })?;
    Ok(Arc::new(store))
}
