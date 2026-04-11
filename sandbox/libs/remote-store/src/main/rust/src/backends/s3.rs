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

use object_store::aws::{AmazonS3Builder, AwsCredentialProvider};
use object_store::ObjectStore;
use serde::Deserialize;

use crate::factory::{build_retry_config, StoreFactoryError};

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
pub fn build(
    config_json: &str,
    credentials: Option<AwsCredentialProvider>,
) -> Result<Arc<dyn ObjectStore>, StoreFactoryError> {
    let config: S3Config =
        serde_json::from_str(config_json).map_err(|e| StoreFactoryError::ConfigParse {
            store_type: "s3".to_string(),
            reason: e.to_string(),
        })?;

    let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);

    if let Some(ref region) = config.region {
        builder = builder.with_region(region);
    }
    if let Some(ref endpoint) = config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(creds) = credentials {
        builder = builder.with_credentials(creds);
    } else {
        if let Some(ref key) = config.access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(ref secret) = config.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(ref token) = config.session_token {
            builder = builder.with_token(token);
        }
    }
    if config.virtual_hosted_style == Some(true) {
        builder = builder.with_virtual_hosted_style_request(true);
    }
    if config.unsigned_payload == Some(true) {
        builder = builder.with_unsigned_payload(true);
    }
    if config.skip_signature == Some(true) {
        builder = builder.with_skip_signature(true);
    }
    if config.allow_http == Some(true) {
        builder = builder.with_allow_http(true);
    }
    if let Some(ref proxy) = config.proxy_url {
        builder = builder.with_proxy_url(proxy);
    }
    if let Some(ref ca_cert) = config.proxy_ca_certificate {
        builder = builder.with_proxy_ca_certificate(ca_cert);
    }
    if config.imdsv1_fallback == Some(true) {
        builder = builder.with_imdsv1_fallback();
    }
    if config.s3_express == Some(true) {
        builder = builder.with_s3_express(true);
    }
    if let Some(ref kms_key_id) = config.sse_kms_key_id {
        builder = builder.with_sse_kms_encryption(kms_key_id);
    }
    if let Some(ref dsse_key_id) = config.dsse_kms_key_id {
        builder = builder.with_dsse_kms_encryption(dsse_key_id);
    }
    if config.bucket_key == Some(true) {
        builder = builder.with_bucket_key(true);
    }
    if let Some(ref algo) = config.checksum_algorithm {
        let checksum = algo.parse().map_err(|_| StoreFactoryError::ConfigParse {
            store_type: "s3".to_string(),
            reason: format!("unknown checksum algorithm: '{}'", algo),
        })?;
        builder = builder.with_checksum_algorithm(checksum);
    }
    if let Some(retry) = build_retry_config(config.max_retries, config.retry_timeout_ms) {
        builder = builder.with_retry(retry);
    }

    let store = builder
        .build()
        .map_err(|e| StoreFactoryError::BuildFailed {
            store_type: "s3".to_string(),
            reason: e.to_string(),
        })?;
    Ok(Arc::new(store))
}
