/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Azure Blob Storage native backend for OpenSearch tiered storage.
//!
//! - [`azure`] — Azure config, builder, and credential provider support
//! - FFM exports: `azure_create_store`, `azure_destroy_store`

pub mod azure;

pub use azure::build;
pub use azure::AzureConfig;

use std::sync::Arc;
use object_store::ObjectStore;

/// Create an Azure [`ObjectStore`] from JSON config and return a boxed Arc pointer.
///
/// # Credential provider
///
/// `cred_provider_ptr` is an optional pointer to a `Box<AzureCredentialProvider>`.
/// - `0` → use the default credential chain
/// - `> 0` → use the custom credential provider (takes ownership)
#[native_bridge_common::ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn azure_create_store(
    config_ptr: *const u8,
    config_len: i64,
    cred_provider_ptr: i64,
) -> i64 {
    if config_ptr.is_null() {
        return Err("azure_create_store: null config pointer".to_string());
    }
    let bytes = std::slice::from_raw_parts(config_ptr, config_len as usize);
    let config_json = std::str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))?;
    let credentials = if cred_provider_ptr != 0 {
        let provider = Box::from_raw(
            cred_provider_ptr as *mut Arc<dyn object_store::CredentialProvider<Credential = object_store::azure::AzureCredential>>,
        );
        Some(*provider)
    } else {
        None
    };
    let store = azure::build(config_json, credentials).map_err(|e| e.to_string())?;
    let ptr = Box::into_raw(Box::new(store)) as i64;
    native_bridge_common::log_info!("ffm: azure_create_store ptr={}", ptr);
    Ok(ptr)
}

/// Free an Azure store created by [`azure_create_store`].
#[native_bridge_common::ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn azure_destroy_store(ptr: i64) -> i64 {
    if ptr == 0 { return Err("azure_destroy_store: null pointer".to_string()); }
    let _: Box<Arc<dyn ObjectStore>> = Box::from_raw(ptr as *mut Arc<dyn ObjectStore>);
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_destroy_null_returns_error() {
        assert!(unsafe { azure_destroy_store(0) } < 0);
    }

    #[test]
    fn test_create_null_config_returns_error() {
        assert!(unsafe { azure_create_store(std::ptr::null(), 0, 0) } < 0);
    }
}
