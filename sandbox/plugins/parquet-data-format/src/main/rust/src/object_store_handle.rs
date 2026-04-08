/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;

use crate::{log_debug, log_error};

/// Supported object store backend types.
/// Passed from Java as a string identifier.
const STORE_TYPE_LOCAL: &str = "local";
const STORE_TYPE_S3: &str = "s3";
const STORE_TYPE_GCS: &str = "gcs";
const STORE_TYPE_AZURE: &str = "azure";

/// Creates an `Arc<dyn ObjectStore>` from a type identifier and JSON config.
///
/// Config format varies by store type:
/// - `local`:  `{"root": "/path/to/dir"}`
/// - `s3`:     `{"bucket": "...", "region": "...", "endpoint": "...", "access_key_id": "...", "secret_access_key": "..."}`
/// - `gcs`:    `{"bucket": "...", "service_account_key": "..."}`
/// - `azure`:  `{"container": "...", "account": "...", "access_key": "..."}`
///
/// All config fields are optional — builders use environment defaults when omitted.
pub fn create_object_store(
    store_type: &str,
    config_json: &str,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error>> {
    log_debug!("Creating object store: type={}, config_len={}", store_type, config_json.len());

    let config: serde_json::Value = if config_json.is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str(config_json)?
    };

    let store: Arc<dyn ObjectStore> = match store_type {
        STORE_TYPE_LOCAL => {
            let root = config.get("root").and_then(|v| v.as_str()).unwrap_or("/");
            Arc::new(LocalFileSystem::new_with_prefix(root)?)
        }
        STORE_TYPE_S3 => {
            let mut builder = AmazonS3Builder::from_env();
            if let Some(bucket) = config.get("bucket").and_then(|v| v.as_str()) {
                builder = builder.with_bucket_name(bucket);
            }
            if let Some(region) = config.get("region").and_then(|v| v.as_str()) {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = config.get("endpoint").and_then(|v| v.as_str()) {
                builder = builder.with_endpoint(endpoint);
            }
            if let Some(key) = config.get("access_key_id").and_then(|v| v.as_str()) {
                builder = builder.with_access_key_id(key);
            }
            if let Some(secret) = config.get("secret_access_key").and_then(|v| v.as_str()) {
                builder = builder.with_secret_access_key(secret);
            }
            Arc::new(builder.build()?)
        }
        STORE_TYPE_GCS => {
            let mut builder = GoogleCloudStorageBuilder::from_env();
            if let Some(bucket) = config.get("bucket").and_then(|v| v.as_str()) {
                builder = builder.with_bucket_name(bucket);
            }
            if let Some(key) = config.get("service_account_key").and_then(|v| v.as_str()) {
                builder = builder.with_service_account_key(key);
            }
            Arc::new(builder.build()?)
        }
        STORE_TYPE_AZURE => {
            let mut builder = MicrosoftAzureBuilder::from_env();
            if let Some(container) = config.get("container").and_then(|v| v.as_str()) {
                builder = builder.with_container_name(container);
            }
            if let Some(account) = config.get("account").and_then(|v| v.as_str()) {
                builder = builder.with_account(account);
            }
            if let Some(key) = config.get("access_key").and_then(|v| v.as_str()) {
                builder = builder.with_access_key(key);
            }
            Arc::new(builder.build()?)
        }
        other => {
            log_error!("Unknown object store type: {}", other);
            return Err(format!("Unknown object store type: {}", other).into());
        }
    };

    log_debug!("Object store created successfully: type={}", store_type);
    Ok(store)
}

/// Wraps an `Arc<dyn ObjectStore>` so we can pass a thin pointer across JNI.
struct ObjectStoreBox(Arc<dyn ObjectStore>);

/// Converts an `Arc<dyn ObjectStore>` into an opaque handle for JNI.
pub fn store_to_handle(store: Arc<dyn ObjectStore>) -> i64 {
    Box::into_raw(Box::new(ObjectStoreBox(store))) as i64
}

/// Recovers a cloned `Arc<dyn ObjectStore>` from an opaque handle without consuming it.
/// # Safety
/// The handle must have been created by `store_to_handle` and not yet destroyed.
pub unsafe fn store_from_handle(handle: i64) -> Arc<dyn ObjectStore> {
    assert!(handle != 0, "Null object store handle");
    let boxed = unsafe { &*(handle as *const ObjectStoreBox) };
    Arc::clone(&boxed.0)
}

/// Drops the `Arc<dyn ObjectStore>` associated with the handle.
/// # Safety
/// The handle must have been created by `store_to_handle` and must not be used after this call.
pub unsafe fn drop_store(handle: i64) {
    if handle != 0 {
        unsafe {
            let _ = Box::from_raw(handle as *mut ObjectStoreBox);
        }
    }
}
