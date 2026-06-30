/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Local filesystem native backend for OpenSearch tiered storage (testing).
//!
//! - [`fs`] — FS config and builder
//! - FFM exports: `fs_create_store`, `fs_destroy_store`

pub mod fs;

pub use fs::build;
pub use fs::FsConfig;

use std::sync::Arc;
use object_store::ObjectStore;

/// Create a local filesystem [`ObjectStore`] from JSON config.
///
/// FS backend does not use credentials.
#[native_bridge_common::ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn fs_create_store(
    config_ptr: *const u8,
    config_len: i64,
) -> i64 {
    if config_ptr.is_null() {
        return Err("fs_create_store: null config pointer".to_string());
    }
    let bytes = std::slice::from_raw_parts(config_ptr, config_len as usize);
    let config_json = std::str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))?;
    let store = fs::build(config_json).map_err(|e| e.to_string())?;
    let ptr = Box::into_raw(Box::new(store)) as i64;
    native_bridge_common::log_info!("ffm: fs_create_store ptr={}", ptr);
    Ok(ptr)
}

/// Free a FS store created by [`fs_create_store`].
#[native_bridge_common::ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn fs_destroy_store(ptr: i64) -> i64 {
    if ptr == 0 { return Err("fs_destroy_store: null pointer".to_string()); }
    let _: Box<Arc<dyn ObjectStore>> = Box::from_raw(ptr as *mut Arc<dyn ObjectStore>);
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_destroy_no_leak() {
        let dir = tempfile::tempdir().unwrap();
        let config = format!(r#"{{"base_path":"{}"}}"#, dir.path().display());
        let config_bytes = config.as_bytes();
        let ptr = unsafe { fs_create_store(config_bytes.as_ptr(), config_bytes.len() as i64) };
        assert!(ptr > 0);
        assert_eq!(unsafe { fs_destroy_store(ptr) }, 0);
    }

    #[test]
    fn test_destroy_null_returns_error() {
        assert!(unsafe { fs_destroy_store(0) } < 0);
    }
}
