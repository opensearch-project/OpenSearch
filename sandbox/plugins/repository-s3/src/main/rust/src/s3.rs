/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Amazon S3 backend configuration and builder.
//!
//! Supports: bucket, region, endpoint, static credentials, custom credential
//! provider, virtual-hosted-style, unsigned payload, skip signature, allow HTTP,
//! proxy (with CA cert), IMDSv1 fallback, S3 Express One Zone, SSE-KMS,
//! DSSE-KMS, bucket key, checksum algorithm, and retry config.

use std::sync::Arc;
use std::time::Duration;

use object_store::aws::{AmazonS3Builder, AwsCredentialProvider};
use object_store::{ObjectStore, RetryConfig};
use serde::Deserialize;

/// Configuration for an S3-compatible remote store.
///
/// All fields except `bucket` are optional. Unknown JSON fields are
/// silently ignored by serde's default behavior.
#[derive(Debug, Deserialize)]
pub struct S3Config {
    /// S3 bucket name (required).
    pub bucket: String,
    /// AWS region (e.g. "us-east-1").
    pub region: Option<String>,
    /// Custom endpoint URL (for S3-compatible stores like MinIO).
    pub endpoint: Option<String>,
    /// Access key ID. If not set, uses default credential chain.
    pub access_key_id: Option<String>,
    /// Secret access key.
    pub secret_access_key: Option<String>,
    /// Session token for temporary credentials.
    pub session_token: Option<String>,
    /// Enable virtual-hosted-style requests.
    pub virtual_hosted_style: Option<bool>,
    /// Allow unsigned payload.
    pub unsigned_payload: Option<bool>,
    /// Skip request signing entirely (for public buckets).
    pub skip_signature: Option<bool>,
    /// Allow HTTP (non-TLS) connections.
    pub allow_http: Option<bool>,
    /// Proxy URL.
    pub proxy_url: Option<String>,
    /// PEM-encoded CA certificate for proxy TLS verification.
    pub proxy_ca_certificate: Option<String>,
    /// Enable IMDSv1 fallback for EC2 metadata credentials.
    pub imdsv1_fallback: Option<bool>,
    /// Enable S3 Express One Zone support.
    pub s3_express: Option<bool>,
    /// SSE-KMS key ID. When set, enables SSE-KMS encryption.
    pub sse_kms_key_id: Option<String>,
    /// DSSE-KMS key ID. When set, enables DSSE-KMS encryption.
    pub dsse_kms_key_id: Option<String>,
    /// Enable S3 bucket key for SSE-KMS (reduces KMS costs).
    pub bucket_key: Option<bool>,
    /// Checksum algorithm (e.g. "sha256").
    pub checksum_algorithm: Option<String>,
    /// Maximum number of retries for failed requests.
    pub max_retries: Option<usize>,
    /// Retry timeout in milliseconds.
    pub retry_timeout_ms: Option<u64>,
}

/// Build an S3 [`ObjectStore`] from JSON config.
///
/// If `credentials` is `Some`, it overrides any static credentials in the config.
/// Pass `None` to use static creds from config or the default credential chain.
pub fn build(
    config_json: &str,
    credentials: Option<AwsCredentialProvider>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let config: S3Config = serde_json::from_str(config_json)?;
    let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);

    if let Some(ref r) = config.region { builder = builder.with_region(r); }
    if let Some(ref e) = config.endpoint { builder = builder.with_endpoint(e); }

    if let Some(creds) = credentials {
        // Custom credential provider overrides all static creds
        builder = builder.with_credentials(creds);
    } else {
        if let Some(ref k) = config.access_key_id { builder = builder.with_access_key_id(k); }
        if let Some(ref s) = config.secret_access_key { builder = builder.with_secret_access_key(s); }
        if let Some(ref t) = config.session_token { builder = builder.with_token(t); }
    }

    if config.virtual_hosted_style == Some(true) { builder = builder.with_virtual_hosted_style_request(true); }
    if config.unsigned_payload == Some(true) { builder = builder.with_unsigned_payload(true); }
    if config.skip_signature == Some(true) { builder = builder.with_skip_signature(true); }
    if config.allow_http == Some(true) { builder = builder.with_allow_http(true); }
    if let Some(ref p) = config.proxy_url { builder = builder.with_proxy_url(p); }
    if let Some(ref c) = config.proxy_ca_certificate { builder = builder.with_proxy_ca_certificate(c); }
    if config.imdsv1_fallback == Some(true) { builder = builder.with_imdsv1_fallback(); }
    if config.s3_express == Some(true) { builder = builder.with_s3_express(true); }
    if let Some(ref k) = config.sse_kms_key_id { builder = builder.with_sse_kms_encryption(k); }
    if let Some(ref d) = config.dsse_kms_key_id { builder = builder.with_dsse_kms_encryption(d); }
    if config.bucket_key == Some(true) { builder = builder.with_bucket_key(true); }
    if let Some(ref a) = config.checksum_algorithm {
        builder = builder.with_checksum_algorithm(
            a.parse().map_err(|_| format!("unknown checksum algorithm: '{}'", a))?
        );
    }
    if config.max_retries.is_some() || config.retry_timeout_ms.is_some() {
        let mut retry = RetryConfig::default();
        if let Some(m) = config.max_retries { retry.max_retries = m; }
        if let Some(ms) = config.retry_timeout_ms { retry.retry_timeout = Duration::from_millis(ms); }
        builder = builder.with_retry(retry);
    }

    Ok(Arc::new(builder.build()?))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_with_valid_config() {
        let config = r#"{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"http://localhost:9000","access_key_id":"x","secret_access_key":"y"}"#;
        assert!(build(config, None).is_ok());
    }

    #[test]
    fn test_build_missing_bucket_returns_error() {
        assert!(build(r#"{"region":"us-east-1"}"#, None).is_err());
    }

    #[test]
    fn test_build_invalid_json_returns_error() {
        assert!(build("not json", None).is_err());
    }

    #[test]
    fn test_build_with_all_optional_fields() {
        let config = r#"{
            "bucket":"b","region":"us-west-2","endpoint":"http://localhost:9000",
            "access_key_id":"AKID","secret_access_key":"SECRET","session_token":"TOKEN",
            "virtual_hosted_style":false,"unsigned_payload":true,"skip_signature":false,
            "allow_http":true,"proxy_url":"http://proxy:8080","imdsv1_fallback":false,
            "s3_express":false,"sse_kms_key_id":"arn:aws:kms:us-east-1:123:key/abc",
            "bucket_key":true,"checksum_algorithm":"sha256","max_retries":5,"retry_timeout_ms":30000
        }"#;
        assert!(build(config, None).is_ok());
    }

    #[test]
    fn test_build_unknown_checksum_returns_error() {
        let config = r#"{"bucket":"b","region":"us-east-1","checksum_algorithm":"md5","allow_http":true,"endpoint":"http://localhost:9000","access_key_id":"x","secret_access_key":"y"}"#;
        assert!(build(config, None).is_err());
    }

    #[test]
    fn test_build_with_custom_credentials() {
        use object_store::aws::AwsCredential;
        use object_store::client::StaticCredentialProvider;

        let cred = AwsCredential {
            key_id: "AKID".to_string(),
            secret_key: "SECRET".to_string(),
            token: None,
        };
        let provider = Arc::new(StaticCredentialProvider::new(cred));
        let config = r#"{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"http://localhost:9000"}"#;
        assert!(build(config, Some(provider)).is_ok());
    }

    #[test]
    fn test_extra_unknown_fields_ignored() {
        let config = r#"{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"http://localhost:9000","access_key_id":"x","secret_access_key":"y","unknown_field":"value"}"#;
        assert!(build(config, None).is_ok());
    }
}
