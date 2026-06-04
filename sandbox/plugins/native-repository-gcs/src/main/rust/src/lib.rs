/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Google Cloud Storage native backend for OpenSearch tiered storage.
//!
//! - [`gcs`] — GCS config, builder, and credential provider support
//! - FFM exports: `gcs_create_store`, `gcs_destroy_store`

pub mod gcs;

pub use gcs::build;
pub use gcs::GcsConfig;

use std::sync::Arc;
use object_store::ObjectStore;

/// Create a GCS [`ObjectStore`] from JSON config and return a boxed Arc pointer.
///
/// # Credential provider
///
/// `cred_provider_ptr` is an optional pointer to a `Box<GcpCredentialProvider>`.
/// - `0` → use the default credential chain
/// - `> 0` → use the custom credential provider (takes ownership)
#[native_bridge_common::ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn gcs_create_store(
    config_ptr: *const u8,
    config_len: i64,
    cred_provider_ptr: i64,
) -> i64 {
    if config_ptr.is_null() {
        return Err("gcs_create_store: null config pointer".to_string());
    }
    let bytes = std::slice::from_raw_parts(config_ptr, config_len as usize);
    let config_json = std::str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))?;
    let credentials = if cred_provider_ptr != 0 {
        let provider = Box::from_raw(
            cred_provider_ptr as *mut Arc<dyn object_store::CredentialProvider<Credential = object_store::gcp::GcpCredential>>,
        );
        Some(*provider)
    } else {
        None
    };
    let store = gcs::build(config_json, credentials).map_err(|e| e.to_string())?;
    let ptr = Box::into_raw(Box::new(store)) as i64;
    native_bridge_common::log_info!("ffm: gcs_create_store ptr={}", ptr);
    Ok(ptr)
}

/// Free a GCS store created by [`gcs_create_store`].
#[native_bridge_common::ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn gcs_destroy_store(ptr: i64) -> i64 {
    if ptr == 0 { return Err("gcs_destroy_store: null pointer".to_string()); }
    let _: Box<Arc<dyn ObjectStore>> = Box::from_raw(ptr as *mut Arc<dyn ObjectStore>);
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_destroy_null_returns_error() {
        assert!(unsafe { gcs_destroy_store(0) } < 0);
    }

    #[test]
    fn test_create_null_config_returns_error() {
        assert!(unsafe { gcs_create_store(std::ptr::null(), 0, 0) } < 0);
    }
}
