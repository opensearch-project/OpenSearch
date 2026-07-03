/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Object-store-aware Parquet PME footer key-metadata reader.
//!
//! Reads `FileCryptoMetaData.key_metadata` from the tail of a Parquet file via
//! range reads — never downloads the full object.
//!
//! # Algorithm
//! 1. `ObjectStore::head` → get `file_size`.
//! 2. Range-read last 8 bytes `[file_size-8 .. file_size]`.
//! 3. Bytes `[4..8]` are the magic:
//!    - `b"PAR1"` → unencrypted, return `Ok(None)`.
//!    - `b"PARE"` → encrypted, continue.
//!    - Anything else → fail closed.
//! 4. Bytes `[0..4]` = 4-byte LE length of `FileCryptoMetaData` Thrift payload.
//! 5. Range-read `FileCryptoMetaData` bytes.
//! 6. Deserialise the payload with `parquet::format::FileCryptoMetaData`
//!    via `parquet::thrift::TSerializable` and `thrift::protocol::TCompactInputProtocol`.

use datafusion::common::DataFusionError;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::format::FileCryptoMetaData;
use parquet::thrift::TSerializable;

/// Encrypted Parquet file magic.
const PARE_MAGIC: &[u8] = b"PARE";
/// Plain-text Parquet file magic.
const PAR1_MAGIC: &[u8] = b"PAR1";

/// Read `FileCryptoMetaData.key_metadata` from the tail of a Parquet file via
/// range reads against `store`.
///
/// The file path is resolved exactly as `create_object_metas` in `api.rs`:
/// absolute `filename` is used as-is; otherwise
/// `<table_path.trim_end_matches('/')>/<filename>` is used.
///
/// Returns:
/// - `Ok(Some(bytes))` — encrypted file with `key_metadata` present.
/// - `Ok(None)`        — plain-text (`PAR1`) file, or `PARE` file with no
///   `key_metadata` field in `FileCryptoMetaData`.
/// - `Err(_)`          — I/O error, file too small, unsupported magic, or
///   malformed Thrift payload.
pub async fn read_footer_key_metadata(
    store: &dyn ObjectStore,
    table_path: &str,
    filename: &str,
) -> Result<Option<Vec<u8>>, DataFusionError> {
    // Path resolution mirrors `create_object_metas` in `api.rs`.
    let full_path = if filename.starts_with('/') || filename.contains(table_path) {
        filename.to_string()
    } else {
        format!("{}/{}", table_path.trim_end_matches('/'), filename)
    };
    let path = object_store::path::Path::from(full_path.as_str());

    // 1. Get file size via head().
    let meta = store.head(&path).await.map_err(|e| {
        DataFusionError::Execution(format!("pme_reader: head({}) failed: {}", full_path, e))
    })?;
    let file_size = meta.size as u64;

    if file_size < 8 {
        return Err(DataFusionError::Execution(format!(
            "pme_reader: file {} is too small ({} bytes) to be a valid Parquet file",
            full_path, file_size
        )));
    }

    // 2. Range-read last 8 bytes: [FileCryptoMetaData_len: 4 LE][magic: 4].
    let tail = store
        .get_range(&path, (file_size - 8)..(file_size))
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "pme_reader: get_range(tail 8 bytes) for {} failed: {}",
                full_path, e
            ))
        })?;

    if tail.len() < 8 {
        return Err(DataFusionError::Execution(format!(
            "pme_reader: expected 8 tail bytes from {}, got {}",
            full_path,
            tail.len()
        )));
    }

    // 3. Check magic at tail[4..8].
    let magic = &tail[4..8];
    if magic == PAR1_MAGIC {
        return Ok(None);
    }
    if magic != PARE_MAGIC {
        return Err(DataFusionError::Execution(format!(
            "pme_reader: unexpected Parquet magic [{:#x},{:#x},{:#x},{:#x}] in {} — \
             expected PAR1 (plain-text) or PARE (encrypted)",
            magic[0], magic[1], magic[2], magic[3], full_path
        )));
    }

    // 4. Parse 4-byte LE FileCryptoMetaData length from tail[0..4].
    let crypto_meta_len = i32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]);
    if crypto_meta_len <= 0 {
        return Err(DataFusionError::Execution(format!(
            "pme_reader: invalid FileCryptoMetaData length {} in {}",
            crypto_meta_len, full_path
        )));
    }
    let crypto_meta_len = crypto_meta_len as u64;
    if file_size < 8 + crypto_meta_len {
        return Err(DataFusionError::Execution(format!(
            "pme_reader: FileCryptoMetaData length {} exceeds file size {} in {}",
            crypto_meta_len, file_size, full_path
        )));
    }

    // 5. Range-read FileCryptoMetaData Thrift bytes.
    let meta_start = file_size - 8 - crypto_meta_len;
    let meta_end = file_size - 8;
    let crypto_meta = store
        .get_range(&path, meta_start..meta_end)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "pme_reader: get_range(FileCryptoMetaData) for {} failed: {}",
                full_path, e
            ))
        })?;

    // 6. Deserialise FileCryptoMetaData using parquet-rs generated Thrift types.
    //    TCompactInputProtocol wraps an std::io::Read, so we use a Cursor over
    //    the fetched bytes. TSerializable::read_from_in_protocol drives the
    //    generated field-by-field parser; any structural mismatch is returned
    //    as a Thrift error rather than a panic.
    let mut cursor = std::io::Cursor::new(&crypto_meta[..]);
    let mut protocol = thrift::protocol::TCompactInputProtocol::new(&mut cursor);
    let file_crypto_meta = FileCryptoMetaData::read_from_in_protocol(&mut protocol)
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "pme_reader: Thrift parse error for {}: {}",
                full_path, e
            ))
        })?;

    Ok(file_crypto_meta.key_metadata)
}

// ---- Tests -------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::PutPayload;

    // ---- helpers ----

    /// Serialise a `FileCryptoMetaData` to a Thrift compact-protocol byte buffer
    /// using the parquet-rs generated codec (the same codec the reader uses).
    fn serialize_file_crypto_meta(meta: &FileCryptoMetaData) -> Vec<u8> {
        use parquet::thrift::TCompactOutputProtocol;
        let mut buf = Vec::new();
        let mut protocol = TCompactOutputProtocol::new(&mut buf);
        meta.write_to_out_protocol(&mut protocol).unwrap();
        buf
    }

    /// Build a valid `FileCryptoMetaData` Thrift compact buffer.
    ///
    /// `key_meta` is `Some` → field 2 is present; `None` → field 2 absent.
    fn build_crypto_meta_thrift(key_meta: Option<&[u8]>) -> Vec<u8> {
        use parquet::format::{AesGcmV1, EncryptionAlgorithm, FileCryptoMetaData};
        let algo = EncryptionAlgorithm::AESGCMV1(AesGcmV1::new(None, None, None));
        let meta = FileCryptoMetaData::new(algo, key_meta.map(|b| b.to_vec()));
        serialize_file_crypto_meta(&meta)
    }

    /// Build a synthetic encrypted Parquet file with `key_meta` in the footer.
    fn make_encrypted_parquet(key_meta: &[u8]) -> Vec<u8> {
        let thrift = build_crypto_meta_thrift(Some(key_meta));
        let crypto_meta_len = thrift.len() as i32;
        let mut file = vec![0u8; 8]; // fake row-group data prefix
        file.extend_from_slice(&thrift);
        file.extend_from_slice(&crypto_meta_len.to_le_bytes());
        file.extend_from_slice(b"PARE");
        file
    }

    /// Build a synthetic encrypted PARE file with no `key_metadata` field.
    fn make_encrypted_parquet_no_km() -> Vec<u8> {
        let thrift = build_crypto_meta_thrift(None);
        let crypto_meta_len = thrift.len() as i32;
        let mut file = vec![0u8; 8];
        file.extend_from_slice(&thrift);
        file.extend_from_slice(&crypto_meta_len.to_le_bytes());
        file.extend_from_slice(b"PARE");
        file
    }

    /// Build a synthetic plain-text Parquet file (PAR1 magic, no FileCryptoMetaData).
    fn make_plaintext_parquet() -> Vec<u8> {
        let mut file = vec![0u8; 8];
        file.extend_from_slice(b"PAR1");
        file
    }

    /// Put `data` into `store` at `path`.
    async fn put(store: &dyn ObjectStore, path: &str, data: Vec<u8>) {
        store
            .put(&Path::from(path), PutPayload::from(Bytes::from(data)))
            .await
            .unwrap();
    }

    // ---- object-store integration tests ----

    #[tokio::test]
    async fn encrypted_file_returns_key_metadata() {
        let store = InMemory::new();
        let key_meta = b"{\"version\":1,\"data_key_id\":\"default\",\"message_id\":\"AAAAAAAAAAAAAAAA\"}";
        let data = make_encrypted_parquet(key_meta);
        put(&store, "shard/seg.parquet", data).await;

        let result = read_footer_key_metadata(&store, "shard", "seg.parquet").await.unwrap();
        assert_eq!(result, Some(key_meta.to_vec()));
    }

    #[tokio::test]
    async fn plaintext_file_returns_none() {
        let store = InMemory::new();
        let data = make_plaintext_parquet();
        put(&store, "shard/plain.parquet", data).await;

        let result = read_footer_key_metadata(&store, "shard", "plain.parquet").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn pare_without_key_metadata_returns_none() {
        let store = InMemory::new();
        let data = make_encrypted_parquet_no_km();
        put(&store, "shard/nokm.parquet", data).await;

        let result = read_footer_key_metadata(&store, "shard", "nokm.parquet").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn file_too_small_fails() {
        let store = InMemory::new();
        put(&store, "shard/short.parquet", vec![0u8; 4]).await;

        let result = read_footer_key_metadata(&store, "shard", "short.parquet").await;
        assert!(result.is_err(), "file with < 8 bytes must fail");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("too small"), "error message should mention too small: {}", msg);
    }

    #[tokio::test]
    async fn bad_magic_fails_closed() {
        let store = InMemory::new();
        let mut data = vec![0u8; 8];
        data.extend_from_slice(b"XXXX"); // unsupported magic
        put(&store, "shard/bad.parquet", data).await;

        let result = read_footer_key_metadata(&store, "shard", "bad.parquet").await;
        assert!(result.is_err(), "unknown magic must fail closed");
    }

    #[tokio::test]
    async fn negative_crypto_meta_len_fails() {
        let store = InMemory::new();
        let mut data = vec![0u8; 8];
        data.extend_from_slice(&(-1i32).to_le_bytes());
        data.extend_from_slice(b"PARE");
        put(&store, "shard/neglen.parquet", data).await;

        let result = read_footer_key_metadata(&store, "shard", "neglen.parquet").await;
        assert!(result.is_err(), "negative crypto_meta_len must fail");
    }

    #[tokio::test]
    async fn corrupt_thrift_fails() {
        // Truncated / garbage bytes after PARE magic with a plausible length.
        let store = InMemory::new();
        let junk = vec![0xFF, 0xFE, 0xFD]; // not a valid Thrift compact struct
        let len = junk.len() as i32;
        let mut data = vec![0u8; 8];
        data.extend_from_slice(&junk);
        data.extend_from_slice(&len.to_le_bytes());
        data.extend_from_slice(b"PARE");
        put(&store, "shard/corrupt.parquet", data).await;

        let result = read_footer_key_metadata(&store, "shard", "corrupt.parquet").await;
        assert!(result.is_err(), "corrupt Thrift payload must fail");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("Thrift parse error"),
            "error should mention Thrift parse: {}",
            msg
        );
    }
}

