/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`TieredChunkReader`] — a parquet `ChunkReader` over either the local
//! filesystem or an `ObjectStore` (the per-shard `TieredObjectStore` on warm
//! nodes, which routes LOCAL files to disk and REMOTE files to the remote
//! store through the block cache layer).
//!
//! Store-mode reads bridge to async via the shared merge IO runtime
//! (`merge::io_task::IO_RUNTIME`). Merge threads run on the rayon pool, so
//! blocking on the dedicated IO runtime cannot deadlock either pool.

use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};

use super::io_task::io_runtime_handle;

/// Chunk size for streaming `get_read` tail reads in store mode. Bounded so a
/// column-chunk stream never buffers more than this per fetch.
const STORE_READ_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

/// A `ChunkReader` over a local file or an object store entry.
pub enum TieredChunkReader {
    /// Direct local file (hot shards; identical to the previous behavior).
    Local(File),
    /// Object-store-backed (warm shards). The store routes per file location.
    Store {
        store: Arc<dyn ObjectStore>,
        path: StorePath,
        len: u64,
    },
}

impl TieredChunkReader {
    /// Opens `path` through `store` when present, else as a local file.
    pub fn open(path: &str, store: Option<&Arc<dyn ObjectStore>>) -> ParquetResult<Self> {
        match store {
            None => {
                let file = File::open(path)?;
                Ok(TieredChunkReader::Local(file))
            }
            Some(store) => {
                let store_path = StorePath::from(path);
                let meta = io_runtime_handle()
                    .block_on(store.head(&store_path))
                    .map_err(|e| {
                        ParquetError::General(format!(
                            "TieredChunkReader: head failed for '{}': {}",
                            path, e
                        ))
                    })?;
                Ok(TieredChunkReader::Store {
                    store: Arc::clone(store),
                    path: store_path,
                    len: meta.size,
                })
            }
        }
    }

    fn store_get_range(
        store: &Arc<dyn ObjectStore>,
        path: &StorePath,
        start: u64,
        end: u64,
    ) -> ParquetResult<Bytes> {
        io_runtime_handle()
            .block_on(store.get_range(path, start..end))
            .map_err(|e| {
                ParquetError::General(format!(
                    "TieredChunkReader: get_range [{}, {}) failed for '{}': {}",
                    start, end, path, e
                ))
            })
    }
}

impl Length for TieredChunkReader {
    fn len(&self) -> u64 {
        match self {
            TieredChunkReader::Local(file) => file.len(),
            TieredChunkReader::Store { len, .. } => *len,
        }
    }
}

impl ChunkReader for TieredChunkReader {
    type T = TieredRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        match self {
            TieredChunkReader::Local(file) => Ok(TieredRead::Local(file.get_read(start)?)),
            TieredChunkReader::Store { store, path, len } => Ok(TieredRead::Store(StoreTailRead {
                store: Arc::clone(store),
                path: path.clone(),
                pos: start,
                end: *len,
                current: Bytes::new(),
            })),
        }
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        match self {
            TieredChunkReader::Local(file) => file.get_bytes(start, length),
            TieredChunkReader::Store { store, path, .. } => {
                Self::store_get_range(store, path, start, start + length as u64)
            }
        }
    }
}

/// `Read` implementation returned by [`TieredChunkReader::get_read`].
pub enum TieredRead {
    Local(<File as ChunkReader>::T),
    Store(StoreTailRead),
}

impl Read for TieredRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            TieredRead::Local(r) => r.read(buf),
            TieredRead::Store(r) => r.read(buf),
        }
    }
}

/// Streams `[pos, end)` from the store in bounded chunks so tail reads never
/// buffer more than [`STORE_READ_CHUNK_BYTES`] at a time.
pub struct StoreTailRead {
    store: Arc<dyn ObjectStore>,
    path: StorePath,
    pos: u64,
    end: u64,
    current: Bytes,
}

impl Read for StoreTailRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.current.is_empty() {
            if self.pos >= self.end {
                return Ok(0);
            }
            let fetch_end = (self.pos + STORE_READ_CHUNK_BYTES).min(self.end);
            let bytes = TieredChunkReader::store_get_range(&self.store, &self.path, self.pos, fetch_end)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            self.pos = fetch_end;
            self.current = bytes;
        }
        let n = self.current.len().min(buf.len());
        self.current.copy_to_slice(&mut buf[..n]);
        Ok(n)
    }
}
