/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Amazon S3 native backend for OpenSearch tiered storage.
//!
//! - [`s3`] — S3 config, builder, and credential provider support
//! - FFM exports: `s3_create_store`, `s3_destroy_store`

pub mod s3;

pub use s3::build;
pub use s3::S3Config;

use std::sync::Arc;
use object_store::ObjectStore;

// ---------------------------------------------------------------------------
// FFM entry points
// ---------------------------------------------------------------------------

/// Create an S3 [`ObjectStore`] from JSON config and return a boxed Arc pointer.
///
/// Returns `>= 0` (pointer) on success, `< 0` on error.
/// The returned pointer must be passed to `ts_add_remote_store_ptr` or freed
/// via [`s3_destroy_store`].
///
/// # Credential provider
///
/// `cred_provider_ptr` is an optional pointer to a `Box<AwsCredentialProvider>`.
/// - `0` → use the default credential chain (env vars, IMDS, IRSA, static creds from config)
/// - `> 0` → use the custom credential provider (takes ownership, pointer is consumed)
#[native_bridge_common::ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn s3_create_store(
    config_ptr: *const u8,
    config_len: i64,
    cred_provider_ptr: i64,
) -> i64 {
    if config_ptr.is_null() {
        return Err("s3_create_store: null config pointer".to_string());
    }
    let bytes = std::slice::from_raw_parts(config_ptr, config_len as usize);
    let config_json = std::str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))?;
    let credentials = if cred_provider_ptr != 0 {
        // SAFETY: ptr was produced by Box::into_raw(Box::new(Arc<dyn CredentialProvider<Credential = AwsCredential>>))
        let provider = Box::from_raw(
            cred_provider_ptr as *mut Arc<dyn object_store::CredentialProvider<Credential = object_store::aws::AwsCredential>>,
        );
        Some(*provider)
    } else {
        None
    };
    let store = s3::build(config_json, credentials).map_err(|e| e.to_string())?;
    let ptr = Box::into_raw(Box::new(store)) as i64;
    native_bridge_common::log_info!("ffm: s3_create_store ptr={}", ptr);
    Ok(ptr)
}

/// Free an S3 store created by [`s3_create_store`].
#[native_bridge_common::ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn s3_destroy_store(ptr: i64) -> i64 {
    if ptr == 0 {
        return Err("s3_destroy_store: null pointer".to_string());
    }
    // SAFETY: ptr was produced by Box::into_raw(Box::new(Arc<dyn ObjectStore>))
    let _store: Box<Arc<dyn ObjectStore>> = Box::from_raw(ptr as *mut Arc<dyn ObjectStore>);
    Ok(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_destroy_no_leak() {
        let config = r#"{"bucket":"b","region":"us-east-1","allow_http":true,"endpoint":"http://localhost:9000","access_key_id":"x","secret_access_key":"y"}"#;
        let config_bytes = config.as_bytes();
        let ptr = unsafe { s3_create_store(config_bytes.as_ptr(), config_bytes.len() as i64, 0) };
        assert!(ptr > 0, "create should return positive pointer");
        let result = unsafe { s3_destroy_store(ptr) };
        assert_eq!(result, 0, "destroy should return 0");
    }

    #[test]
    fn test_destroy_null_returns_error() {
        let result = unsafe { s3_destroy_store(0) };
        assert!(result < 0);
    }

    #[test]
    fn test_create_null_config_returns_error() {
        let result = unsafe { s3_create_store(std::ptr::null(), 0, 0) };
        assert!(result < 0);
    }

    #[test]
    fn test_create_invalid_json_returns_error() {
        let bad = b"not json";
        let result = unsafe { s3_create_store(bad.as_ptr(), bad.len() as i64, 0) };
        assert!(result < 0);
    }
}
