/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Local filesystem backend configuration and builder (for testing).

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde::Deserialize;

/// Configuration for a local filesystem remote store.
#[derive(Debug, Deserialize)]
pub struct FsConfig {
    /// Base directory path for the local filesystem store (required).
    pub base_path: String,
}

/// Build a local filesystem [`ObjectStore`] from JSON config.
pub fn build(
    config_json: &str,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let config: FsConfig = serde_json::from_str(config_json)?;
    let store = LocalFileSystem::new_with_prefix(&config.base_path)?;
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_with_valid_path() {
        let dir = tempfile::tempdir().unwrap();
        let config = format!(r#"{{"base_path":"{}"}}"#, dir.path().display());
        assert!(build(&config).is_ok());
    }

    #[test]
    fn test_build_missing_base_path_returns_error() {
        assert!(build(r#"{"other":"v"}"#).is_err());
    }

    #[test]
    fn test_build_invalid_json_returns_error() {
        assert!(build("not json").is_err());
    }
}
