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
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};
    use futures::TryStreamExt;

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

    #[tokio::test]
    async fn test_write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = format!(r#"{{"base_path":"{}"}}"#, dir.path().display());
        let store = build(&config).unwrap();

        let path = Path::from("test_file.parquet");
        let data = b"hello parquet world";

        // Write
        store.put(&path, PutPayload::from_static(data)).await.unwrap();

        // Read
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), data);
    }

    #[tokio::test]
    async fn test_head_returns_correct_size() {
        let dir = tempfile::tempdir().unwrap();
        let config = format!(r#"{{"base_path":"{}"}}"#, dir.path().display());
        let store = build(&config).unwrap();

        let path = Path::from("sized_file.dat");
        let data = b"exactly 26 bytes of data!!";

        store.put(&path, PutPayload::from_static(data)).await.unwrap();

        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size as usize, data.len());
    }

    #[tokio::test]
    async fn test_list_returns_written_files() {
        let dir = tempfile::tempdir().unwrap();
        let config = format!(r#"{{"base_path":"{}"}}"#, dir.path().display());
        let store = build(&config).unwrap();

        store.put(&Path::from("a.parquet"), PutPayload::from_static(b"aaa")).await.unwrap();
        store.put(&Path::from("b.parquet"), PutPayload::from_static(b"bbb")).await.unwrap();

        let list: Vec<_> = store.list(None).try_collect().await.unwrap();
        let names: Vec<String> = list.iter().map(|m| m.location.to_string()).collect();
        assert!(names.contains(&"a.parquet".to_string()));
        assert!(names.contains(&"b.parquet".to_string()));
    }

    #[tokio::test]
    async fn test_delete_removes_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = format!(r#"{{"base_path":"{}"}}"#, dir.path().display());
        let store = build(&config).unwrap();

        let path = Path::from("to_delete.dat");
        store.put(&path, PutPayload::from_static(b"delete me")).await.unwrap();
        assert!(store.head(&path).await.is_ok());

        store.delete(&path).await.unwrap();
        assert!(store.head(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_get_range_reads_subset() {
        let dir = tempfile::tempdir().unwrap();
        let config = format!(r#"{{"base_path":"{}"}}"#, dir.path().display());
        let store = build(&config).unwrap();

        let path = Path::from("range_file.dat");
        store.put(&path, PutPayload::from_static(b"0123456789")).await.unwrap();

        let bytes = store.get_range(&path, 3..7).await.unwrap();
        assert_eq!(bytes.as_ref(), b"3456");
    }
}
