/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Amazon S3 backend configuration and builder.
//!
//! Supports: bucket, region, endpoint, custom credential provider,
//! virtual-hosted-style, unsigned payload, allow HTTP, proxy (with CA cert),
//! IMDSv1 fallback, S3 Express One Zone, SSE-KMS, DSSE-KMS, bucket key,
//! checksum algorithm, and retry config.
//!
//! Credentials are NOT accepted via config JSON — use the default credential
//! chain (IAM roles, env vars, IMDS) or pass a custom `AwsCredentialProvider`.

use std::sync::Arc;
use std::time::Duration;

use object_store::aws::{AmazonS3Builder, AwsCredentialProvider};
use object_store::{ObjectStore, RetryConfig};
use serde::Deserialize;

/// Configuration for an S3-compatible remote store.
///
/// All fields except `bucket` are optional. Unknown JSON fields are
/// silently ignored by serde's default behavior.
///
/// Credentials are intentionally excluded — authentication is handled
/// via the default credential chain or a custom `AwsCredentialProvider`
/// passed to [`build`].
#[derive(Debug, Deserialize)]
pub struct S3Config {
    /// S3 bucket name (required).
    pub bucket: String,
    /// AWS region (e.g. "us-east-1").
    pub region: Option<String>,
    /// Custom endpoint URL (for S3-compatible stores like MinIO).
    pub endpoint: Option<String>,
    /// Enable virtual-hosted-style requests.
    pub virtual_hosted_style: Option<bool>,
    /// Allow unsigned payload.
    pub unsigned_payload: Option<bool>,
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
/// If `credentials` is `Some`, it is used for authentication.
/// If `None`, the default credential chain is used (env vars, IMDS, etc.).
pub fn build(
    config_json: &str,
    credentials: Option<AwsCredentialProvider>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let config: S3Config = serde_json::from_str(config_json)?;
    let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);

    if let Some(ref r) = config.region { builder = builder.with_region(r); }
    if let Some(ref e) = config.endpoint { builder = builder.with_endpoint(e); }

    if let Some(creds) = credentials {
        builder = builder.with_credentials(creds);
    }

    if config.virtual_hosted_style == Some(true) { builder = builder.with_virtual_hosted_style_request(true); }
    if config.unsigned_payload == Some(true) { builder = builder.with_unsigned_payload(true); }
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

    // If the analytics engine has installed a dedicated IO runtime, route HTTP
    // requests (and response-body streaming) onto it via SpawnedReqwestConnector
    // — DataFusion's `thread_pools` example mechanism. This keeps network IO and
    // its completion work (TLS, body assembly) off the CPU runtime that drives
    // query decode. No-op when no IO runtime is installed (e.g. unit tests).
    if let Some(io) = native_bridge_common::io_runtime::io_handle() {
        builder = builder
            .with_http_connector(object_store::client::SpawnedReqwestConnector::new(io));
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
        let config = r#"{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"http://localhost:9000"}"#;
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
            "virtual_hosted_style":false,"unsigned_payload":true,
            "allow_http":true,"proxy_url":"http://proxy:8080","imdsv1_fallback":false,
            "s3_express":false,"sse_kms_key_id":"arn:aws:kms:us-east-1:123:key/abc",
            "bucket_key":true,"checksum_algorithm":"sha256","max_retries":5,"retry_timeout_ms":30000
        }"#;
        assert!(build(config, None).is_ok());
    }

    #[test]
    fn test_build_unknown_checksum_returns_error() {
        let config = r#"{"bucket":"b","region":"us-east-1","checksum_algorithm":"md5","allow_http":true,"endpoint":"http://localhost:9000"}"#;
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
        let config = r#"{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"http://localhost:9000","unknown_field":"value"}"#;
        assert!(build(config, None).is_ok());
    }

    // ── IO-runtime dispatch (SpawnedReqwestConnector) ────────────────────────
    //
    // These assert the warm path: an S3 store built while an IO runtime handle is
    // installed routes its HTTP requests onto that runtime (the
    // `SpawnedReqwestConnector` wiring in `build`), so the IO runtime's worker
    // poll counter advances. With no handle installed, the store falls back to the
    // default connector and the IO runtime stays idle. `worker_poll_count` is the
    // exact counter the DataFusion `_stats` API exports as `total_polls_count`.

    use std::io::Write as _;
    use std::net::TcpListener as StdTcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Minimal localhost HTTP server: accepts connections and replies with a fixed
    /// `200 OK` + tiny body to ANY request, until `stop` is set. Lets us exercise
    /// the real `AmazonS3` HTTP client end-to-end without a container or real S3.
    /// Runs on its own std thread so it is independent of any tokio runtime.
    fn spawn_mock_http() -> (String, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            use std::io::Read as _;
            while !stop_c.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut sock, _)) => {
                        // Drain the request headers (best-effort) then reply.
                        let mut buf = [0u8; 1024];
                        let _ = sock.read(&mut buf);
                        let body = b"hello-from-mock-s3";
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
                            body.len()
                        );
                        let _ = sock.write_all(resp.as_bytes());
                        let _ = sock.write_all(body);
                        let _ = sock.flush();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        (format!("http://{}", addr), stop, handle)
    }

    /// Sum of per-worker poll counts for a runtime — the same metric the
    /// production `_stats` API reports as `io_runtime.total_polls_count`.
    fn worker_polls(handle: &tokio::runtime::Handle) -> u64 {
        let m = handle.metrics();
        (0..m.num_workers()).map(|i| m.worker_poll_count(i)).sum()
    }

    fn s3_config(endpoint: &str) -> String {
        format!(
            r#"{{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"{}"}}"#,
            endpoint
        )
    }

    /// Drive a single `get` against `endpoint` on a throwaway driver runtime
    /// (separate from `io_rt`, so any polls observed on `io_rt` can only come from
    /// the connector), returning the IO runtime's worker-poll delta across it.
    fn poll_delta_for_get(io_rt: &tokio::runtime::Runtime, endpoint: &str) -> u64 {
        let driver_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let store = build(&s3_config(endpoint), None).expect("build s3 store");
        let before = worker_polls(io_rt.handle());
        let _ = driver_rt.block_on(async move {
            store
                .get_opts(
                    &object_store::path::Path::from("some-object"),
                    object_store::GetOptions::default(),
                )
                .await
        });
        worker_polls(io_rt.handle()) - before
    }

    /// Warm-path assertion + its control, in ONE test because the IO handle is a
    /// process-global that parallel tests would race on.
    ///
    /// * WITH a handle installed → the S3 store's HTTP request is polled on that
    ///   IO runtime (the `SpawnedReqwestConnector` wiring), so its
    ///   `worker_poll_count` advances.
    /// * WITHOUT a handle (a runtime never installed) → that runtime sees zero
    ///   polls, proving the warm-case polls come from the connector, not ambient
    ///   activity.
    #[test]
    fn io_runtime_services_s3_reads_only_when_handle_installed() {
        let (endpoint, stop, server) = spawn_mock_http();

        let io_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("test-io")
            .enable_all()
            .build()
            .unwrap();
        // A second runtime that we observe but NEVER install as the IO handle.
        let uninstalled_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("test-uninstalled")
            .enable_all()
            .build()
            .unwrap();

        // WARM: handle installed → io_rt must poll the request.
        native_bridge_common::io_runtime::set_io_handle(io_rt.handle().clone());
        let installed_delta = poll_delta_for_get(&io_rt, &endpoint);

        // CONTROL: a runtime that was never installed must see nothing from the read.
        let uninstalled_delta = poll_delta_for_get(&uninstalled_rt, &endpoint);

        native_bridge_common::io_runtime::clear_io_handle();
        stop.store(true, Ordering::Relaxed);
        let _ = server.join();

        assert!(
            installed_delta > 0,
            "IO runtime worker_poll_count must advance across an S3 read \
             (delta={}) — SpawnedReqwestConnector is not dispatching HTTP onto the \
             installed IO runtime",
            installed_delta
        );
        assert_eq!(
            uninstalled_delta, 0,
            "a runtime never installed as the IO handle must see no polls from an \
             S3 read (delta={}) — the warm-case polls would then be ambient noise",
            uninstalled_delta
        );
    }
}
