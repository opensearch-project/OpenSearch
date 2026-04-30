/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fs::File;
use std::sync::OnceLock;

use parquet::file::metadata::ParquetMetaData;
use parquet::file::writer::SerializedFileWriter;

use rayon::ThreadPool;

use tokio::runtime::Runtime;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::crc_writer::CrcWriter;
use crate::rate_limited_writer::RateLimitedWriter;
use crate::log_error;

use super::error::{MergeError, MergeResult};
// =============================================================================
// Constants
// =============================================================================

/// Disk write rate limit in MB/s.
pub const RATE_LIMIT_MB_PER_SEC: f64 = 20.0;

/// Default thread count for merge pools: max(1, num_cpus / 8).
fn default_merge_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() / 8)
        .unwrap_or(1)
        .max(1)
}

/// Bounded channel capacity between the merge loop and the IO task.
const IO_CHANNEL_BUFFER: usize = 2;

// =============================================================================
// Process-wide shared Rayon thread pool
// =============================================================================

static MERGE_POOL: OnceLock<ThreadPool> = OnceLock::new();

pub fn get_merge_pool(num_threads: Option<usize>) -> &'static ThreadPool {
    MERGE_POOL.get_or_init(|| {
        let n = num_threads.unwrap_or_else(default_merge_threads);
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .thread_name(|idx| format!("parquet-merge-{}", idx))
            .build()
            .expect("Failed to build parquet-merge Rayon thread pool")
    })
}

// =============================================================================
// Process-wide shared Tokio runtime for async IO
// =============================================================================

static IO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_io_runtime(num_threads: Option<usize>) -> &'static Runtime {
    IO_RUNTIME.get_or_init(|| {
        let n = num_threads.unwrap_or_else(default_merge_threads);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n)
            .thread_name("parquet-io")
            .enable_all()
            .build()
            .expect("Failed to build tokio IO runtime")
    })
}

// =============================================================================
// IO task protocol
// =============================================================================

/// Writer type used by the IO task: CRC → rate-limit → file.
pub type MergeWriter = CrcWriter<RateLimitedWriter<File>>;

/// Commands sent from the merge loop to the background IO task.
pub enum IoCommand {
    WriteRowGroup(Vec<parquet::arrow::arrow_writer::ArrowColumnChunk>),
    Close(oneshot::Sender<MergeResult<(ParquetMetaData, u32)>>),
}

async fn drain_on_error(rx: &mut tokio_mpsc::Receiver<IoCommand>, msg: &str) {
    while let Some(cmd) = rx.recv().await {
        if let IoCommand::Close(reply) = cmd {
            let _ = reply.send(Err(MergeError::Logic(
                format!("Prior IO write failed: {msg}"),
            )));
        }
    }
}

/// Spawns the background IO task on the shared Tokio runtime.
///
/// The IO task owns the `SerializedFileWriter` and receives encoded row groups
/// over a bounded channel. Each disk write is dispatched to `spawn_blocking`
/// but is **not** awaited immediately — this allows the merge loop to prepare
/// the next row group while the current one is still being flushed to disk.
pub fn spawn_io_task(
    writer: SerializedFileWriter<MergeWriter>,
    crc_handle: crate::crc_writer::CrcHandle,
    io_threads: Option<usize>,
) -> tokio_mpsc::Sender<IoCommand> {
    let (tx, mut rx) = tokio_mpsc::channel::<IoCommand>(IO_CHANNEL_BUFFER);

    get_io_runtime(io_threads).spawn(async move {
        let mut writer: Option<SerializedFileWriter<MergeWriter>> = Some(writer);
        let mut in_flight: Option<
            JoinHandle<MergeResult<SerializedFileWriter<MergeWriter>>>,
        > = None;

        while let Some(cmd) = rx.recv().await {
            match cmd {
                IoCommand::WriteRowGroup(chunks) => {
                    if let Some(handle) = in_flight.take() {
                        match handle.await {
                            Ok(Ok(w)) => writer = Some(w),
                            Ok(Err(e)) => {
                                let msg = format!("{e}");
                                log_error!("[RUST] IO write error during merge: {}", e);
                                drain_on_error(&mut rx, &msg).await;
                                return;
                            }
                            Err(e) => {
                                let msg = format!("{e}");
                                log_error!("[RUST] IO spawn_blocking panicked during merge: {}", e);
                                drain_on_error(&mut rx, &msg).await;
                                return;
                            }
                        }
                    }

                    let w = writer.take().unwrap();
                    in_flight = Some(tokio::task::spawn_blocking(move || {
                        let mut w = w;
                        let mut rg_writer = w.next_row_group()?;
                        for chunk in chunks {
                            chunk.append_to_row_group(&mut rg_writer)?;
                        }
                        rg_writer.close()?;
                        Ok(w)
                    }));
                }

                IoCommand::Close(reply) => {
                    if let Some(handle) = in_flight.take() {
                        match handle.await {
                            Ok(Ok(w)) => writer = Some(w),
                            Ok(Err(e)) => {
                                let _ = reply.send(Err(e));
                                return;
                            }
                            Err(e) => {
                                let _ = reply.send(Err(MergeError::Logic(
                                    format!("IO panic during final write: {e}"),
                                )));
                                return;
                            }
                        }
                    }

                    let w = writer.take().unwrap();
                    let crc = crc_handle.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        let metadata = w.close().map_err(MergeError::from)?;
                        Ok((metadata, crc.crc32()))
                    })
                        .await;

                    let _ = match result {
                        Ok(r) => reply.send(r),
                        Err(e) => reply.send(Err(MergeError::Logic(
                            format!("Close panicked: {e}"),
                        ))),
                    };
                    return;
                }
            }
        }
    });

    tx
}
