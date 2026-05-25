/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::io::{Result, Write};
use std::sync::{Arc, Mutex};

/// Shared CRC32 handle that can be cloned and read independently of the writer.
#[derive(Clone)]
pub struct CrcHandle {
    hasher: Arc<Mutex<crc32fast::Hasher>>,
}

impl CrcHandle {
    pub fn crc32(&self) -> u32 {
        self.hasher.lock().unwrap().clone().finalize()
    }
}

/// A writer wrapper that computes CRC32 incrementally on every write.
/// The CRC can be read via a `CrcHandle` without consuming the writer.
pub struct CrcWriter<W: Write> {
    inner: W,
    hasher: Arc<Mutex<crc32fast::Hasher>>,
}

impl<W: Write> CrcWriter<W> {
    pub fn new(inner: W) -> (Self, CrcHandle) {
        let hasher = Arc::new(Mutex::new(crc32fast::Hasher::new()));
        let handle = CrcHandle { hasher: hasher.clone() };
        (Self { inner, hasher }, handle)
    }
}

impl<W: Write> Write for CrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.lock().unwrap().update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}
