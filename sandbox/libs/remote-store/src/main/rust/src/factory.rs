/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Factory for creating remote [`ObjectStore`] backends from a type string
//! and JSON configuration.
//!
//! Uses a builder pattern so optional features (like custom credential
//! providers) flow through the same creation path.
//!
//! # Usage
//!
//! ```ignore
//! // Simple:
//! let store = StoreFactory::new("s3", config_json, "repo-1").build()?;
//!
//! // With custom credentials:
//! let store = StoreFactory::new("s3", config_json, "repo-1")
//!     .with_credentials(CloudCredentials::Aws(provider))
//!     .build()?;
//! ```
//!
//! # Extensibility
//!
//! New store types can be added by adding a match arm in
//! [`StoreFactory::build`].

use std::sync::Arc;

use object_store::ObjectStore;

use crate::backends::{azure, fs, gcs, s3};
use crate::remote_object_store::RemoteObjectStore;

/// Errors from store creation.
#[derive(Debug, thiserror::Error)]
pub enum StoreFactoryError {
    /// The store type is not recognised.
    #[error("StoreFactory: unknown store type '{0}' — supported: fs, s3, gcs, azure")]
    UnknownType(String),

    /// JSON config parsing failed.
    #[error("StoreFactory: failed to parse config for type '{store_type}': {reason}")]
    ConfigParse { store_type: String, reason: String },

    /// The underlying object_store builder failed.
    #[error("StoreFactory: failed to build '{store_type}' store: {reason}")]
    BuildFailed { store_type: String, reason: String },
}

/// Custom credential provider for cloud backends.
///
/// Each variant wraps the provider type for its respective cloud. The factory
/// matches the variant against the store type and passes it to the builder.
/// Mismatched variants (e.g. `Aws` for a `"gcs"` store) are silently ignored.
pub enum CloudCredentials {
    /// AWS credential provider (for `"s3"` stores).
    Aws(object_store::aws::AwsCredentialProvider),
    /// GCP credential provider (for `"gcs"` stores).
    Gcp(object_store::gcp::GcpCredentialProvider),
    /// Azure credential provider (for `"azure"` stores).
    Azure(object_store::azure::AzureCredentialProvider),
}

/// Builder for creating remote [`ObjectStore`] backends.
pub struct StoreFactory<'a> {
    store_type: &'a str,
    config_json: &'a str,
    repo_key: &'a str,
    credentials: Option<CloudCredentials>,
}

impl<'a> StoreFactory<'a> {
    /// Start building a store of the given type.
    #[must_use]
    pub fn new(store_type: &'a str, config_json: &'a str, repo_key: &'a str) -> Self {
        Self {
            store_type,
            config_json,
            repo_key,
            credentials: None,
        }
    }

    /// Set a custom credential provider (overrides static creds in config).
    ///
    /// The variant must match the store type. Mismatched variants are ignored.
    #[must_use]
    pub fn with_credentials(mut self, credentials: CloudCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Build the store. Returns it wrapped in [`RemoteObjectStore`] for logging.
    pub fn build(self) -> Result<Arc<dyn ObjectStore>, StoreFactoryError> {
        native_bridge_common::log_info!(
            "StoreFactory: creating store type='{}', repo_key='{}'",
            self.store_type,
            self.repo_key
        );

        let raw: Arc<dyn ObjectStore> = match self.store_type {
            "fs" => fs::build(self.config_json)?,
            "s3" => {
                let creds = self.credentials.and_then(|c| match c {
                    CloudCredentials::Aws(p) => Some(p),
                    _ => None,
                });
                s3::build(self.config_json, creds)?
            }
            "gcs" => {
                let creds = self.credentials.and_then(|c| match c {
                    CloudCredentials::Gcp(p) => Some(p),
                    _ => None,
                });
                gcs::build(self.config_json, creds)?
            }
            "azure" => {
                let creds = self.credentials.and_then(|c| match c {
                    CloudCredentials::Azure(p) => Some(p),
                    _ => None,
                });
                azure::build(self.config_json, creds)?
            }
            other => return Err(StoreFactoryError::UnknownType(other.to_string())),
        };

        Ok(Arc::new(RemoteObjectStore::new(
            raw,
            self.repo_key.to_string(),
        )))
    }
}

/// Convenience: create a store without any optional features.
pub fn create(
    store_type: &str,
    config_json: &str,
    repo_key: &str,
) -> Result<Arc<dyn ObjectStore>, StoreFactoryError> {
    StoreFactory::new(store_type, config_json, repo_key).build()
}

/// Build a [`RetryConfig`] from optional max_retries and timeout_ms values.
///
/// Returns `None` if both are `None` (use the crate default).
#[must_use]
pub fn build_retry_config(
    max_retries: Option<usize>,
    retry_timeout_ms: Option<u64>,
) -> Option<object_store::RetryConfig> {
    if max_retries.is_none() && retry_timeout_ms.is_none() {
        return None;
    }
    let mut retry = object_store::RetryConfig::default();
    if let Some(max) = max_retries {
        retry.max_retries = max;
    }
    if let Some(ms) = retry_timeout_ms {
        retry.retry_timeout = std::time::Duration::from_millis(ms);
    }
    Some(retry)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_fs_store_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let config = format!(r#"{{"base_path":"{}"}}"#, dir.path().display());
        assert!(create("fs", &config, "test-repo").is_ok());
    }

    #[test]
    fn test_unknown_store_type_returns_error() {
        let result = create("cassandra", "{}", "test-repo");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cassandra"));
    }

    #[test]
    fn test_invalid_json_returns_error() {
        assert!(create("fs", "not json", "r").is_err());
        assert!(create("s3", "not json", "r").is_err());
        assert!(create("gcs", "not json", "r").is_err());
        assert!(create("azure", "not json", "r").is_err());
    }

    #[test]
    fn test_missing_required_fields_returns_error() {
        assert!(create("fs", r#"{"other":"v"}"#, "r").is_err());
        assert!(create("s3", r#"{"region":"us-east-1"}"#, "r").is_err());
        assert!(create("gcs", r#"{}"#, "r").is_err());
        assert!(create("azure", r#"{"container":"c"}"#, "r").is_err());
    }

    #[test]
    fn test_error_messages_include_store_type() {
        assert!(create("s3", "bad", "r").unwrap_err().to_string().contains("s3"));
        assert!(create("gcs", "bad", "r").unwrap_err().to_string().contains("gcs"));
        assert!(create("azure", "bad", "r").unwrap_err().to_string().contains("azure"));
        assert!(create("fs", "bad", "r").unwrap_err().to_string().contains("fs"));
    }

    #[test]
    fn test_fs_store_is_wrapped_in_remote_object_store() {
        let dir = tempfile::tempdir().unwrap();
        let config = format!(r#"{{"base_path":"{}"}}"#, dir.path().display());
        let store = create("fs", &config, "my-repo").unwrap();
        assert!(format!("{}", store).contains("my-repo"));
    }

    fn s3_base_config() -> String {
        r#"{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"http://localhost:9000","access_key_id":"x","secret_access_key":"y"}"#.to_string()
    }

    #[test]
    fn test_s3_minimal_config_builds() {
        assert!(create("s3", &s3_base_config(), "r").is_ok());
    }

    #[test]
    fn test_s3_with_all_optional_fields() {
        let config = r#"{
            "bucket": "test-bucket",
            "region": "us-west-2",
            "endpoint": "http://localhost:9000",
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
            "session_token": "TOKEN",
            "virtual_hosted_style": false,
            "unsigned_payload": true,
            "skip_signature": false,
            "allow_http": true,
            "proxy_url": "http://proxy:8080",
            "imdsv1_fallback": false,
            "s3_express": false,
            "sse_kms_key_id": "arn:aws:kms:us-east-1:123:key/abc",
            "bucket_key": true,
            "checksum_algorithm": "sha256",
            "max_retries": 5,
            "retry_timeout_ms": 30000
        }"#;
        assert!(create("s3", config, "repo-full").is_ok());
    }

    #[test]
    fn test_s3_unknown_checksum_returns_error() {
        let config = r#"{"bucket":"b","region":"us-east-1","checksum_algorithm":"md5","allow_http":true,"endpoint":"http://localhost:9000","access_key_id":"x","secret_access_key":"y"}"#;
        let err = create("s3", config, "r").unwrap_err();
        assert!(err.to_string().contains("checksum"));
    }

    #[test]
    fn test_s3_extra_unknown_fields_ignored() {
        let config = r#"{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"http://localhost:9000","access_key_id":"x","secret_access_key":"y","unknown_field":"value"}"#;
        assert!(create("s3", config, "r").is_ok());
    }

    #[test]
    fn test_s3_with_custom_credentials_via_builder() {
        use object_store::aws::AwsCredential;
        use object_store::client::StaticCredentialProvider;

        let cred = AwsCredential {
            key_id: "AKID".to_string(),
            secret_key: "SECRET".to_string(),
            token: None,
        };
        let provider = Arc::new(StaticCredentialProvider::new(cred));
        let config = r#"{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"http://localhost:9000"}"#;

        let result = StoreFactory::new("s3", config, "r")
            .with_credentials(CloudCredentials::Aws(provider))
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_s3_custom_credentials_override_static() {
        use object_store::aws::AwsCredential;
        use object_store::client::StaticCredentialProvider;

        let cred = AwsCredential {
            key_id: "OVERRIDE".to_string(),
            secret_key: "SECRET".to_string(),
            token: None,
        };
        let provider = Arc::new(StaticCredentialProvider::new(cred));
        let config = r#"{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"http://localhost:9000","access_key_id":"WRONG","secret_access_key":"WRONG"}"#;

        let result = StoreFactory::new("s3", config, "r")
            .with_credentials(CloudCredentials::Aws(provider))
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_mismatched_credentials_ignored() {
        use object_store::aws::AwsCredential;
        use object_store::client::StaticCredentialProvider;

        let cred = AwsCredential {
            key_id: "AKID".to_string(),
            secret_key: "SECRET".to_string(),
            token: None,
        };
        let provider = Arc::new(StaticCredentialProvider::new(cred));
        let dir = tempfile::tempdir().unwrap();
        let config = format!(r#"{{"base_path":"{}"}}"#, dir.path().display());

        // Aws credentials on an "fs" store — silently ignored.
        let result = StoreFactory::new("fs", &config, "r")
            .with_credentials(CloudCredentials::Aws(provider))
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_gcs_with_retry_config() {
        let config = r#"{"bucket":"b","max_retries":3,"retry_timeout_ms":10000}"#;
        let result = create("gcs", config, "r");
        if let Err(e) = &result {
            assert!(!e.to_string().contains("parse"), "should not be a parse error: {}", e);
        }
    }

    #[test]
    fn test_azure_with_retry_config() {
        let config = r#"{"account":"a","container":"c","access_key":"dGVzdGtleQ==","max_retries":5,"retry_timeout_ms":20000}"#;
        assert!(create("azure", config, "r").is_ok());
    }

    #[test]
    fn test_azure_with_sas_token() {
        let config = r#"{"account":"a","container":"c","sas_token":"sv=2020-08-04&ss=b&sig=abc"}"#;
        assert!(create("azure", config, "r").is_ok());
    }
}
