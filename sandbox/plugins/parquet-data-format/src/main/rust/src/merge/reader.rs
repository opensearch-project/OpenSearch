/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared input-reader abstraction for the merge paths (sorted `FileCursor` and unsorted merge).
//!
//! A merge input is either a local Parquet file (segment merges when no store is configured, and
//! native unit tests) or a store object read via the async `ParquetObjectReader`. The async stream
//! is driven to completion with `block_on` on the store runtime — safe here because merges run on
//! rayon / finalize / `spawn_blocking` threads, never Tokio workers.

use std::fs::File;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema as ArrowSchema;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use super::error::{MergeError, MergeResult};
use crate::store_io::os_store_runtime;

/// A source of `RecordBatch`es for one merge input — a synchronous local Parquet reader, or an
/// async store-backed Parquet stream driven to completion via `block_on` on the store runtime.
pub(crate) enum BatchSource {
    Local(ParquetRecordBatchReader),
    Store(ParquetRecordBatchStream<ParquetObjectReader>),
}

impl BatchSource {
    /// Pull the next batch, or `None` at end of input.
    pub(crate) fn next_batch(&mut self) -> Option<MergeResult<RecordBatch>> {
        match self {
            BatchSource::Local(reader) => match reader.next() {
                Some(Ok(b)) => Some(Ok(b)),
                Some(Err(e)) => Some(Err(MergeError::Arrow(e))),
                None => None,
            },
            BatchSource::Store(stream) => os_store_runtime().block_on(async {
                match stream.next().await {
                    Some(Ok(b)) => Some(Ok(b)),
                    Some(Err(e)) => Some(Err(MergeError::from(e))),
                    None => None,
                }
            }),
        }
    }
}

/// Read just the metadata/schema of an input (local or store) to drive projection/mode decisions
/// before building the batch readers.
pub(crate) fn read_source_meta(
    store: &Option<Arc<dyn ObjectStore>>,
    path: &str,
    file_id: usize,
) -> MergeResult<(Arc<ArrowSchema>, SchemaDescriptor, i64, usize)> {
    match store {
        Some(s) => {
            let reader = ParquetObjectReader::new(s.clone(), ObjectPath::from(path));
            os_store_runtime()
                .block_on(async move {
                    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
                    let schema = builder.schema().clone();
                    let wg = crate::writer_properties_builder::read_writer_generation(
                        builder.metadata().file_metadata(),
                        file_id,
                    );
                    let trc = builder.metadata().file_metadata().num_rows() as usize;
                    let descr = builder.parquet_schema().clone();
                    Ok::<_, parquet::errors::ParquetError>((schema, descr, wg, trc))
                })
                .map_err(MergeError::from)
        }
        None => {
            let file = File::open(path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let schema = builder.schema().clone();
            let wg = crate::writer_properties_builder::read_writer_generation(
                builder.metadata().file_metadata(),
                file_id,
            );
            let trc = builder.metadata().file_metadata().num_rows() as usize;
            let descr = builder.parquet_schema().clone();
            Ok((schema, descr, wg, trc))
        }
    }
}

/// Build a batch source (local reader or store stream) for `path` with the given projection.
pub(crate) fn build_source(
    store: &Option<Arc<dyn ObjectStore>>,
    path: &str,
    batch_size: usize,
    projection: ProjectionMask,
) -> MergeResult<BatchSource> {
    match store {
        Some(s) => {
            let reader = ParquetObjectReader::new(s.clone(), ObjectPath::from(path));
            let stream = os_store_runtime()
                .block_on(async move {
                    ParquetRecordBatchStreamBuilder::new(reader)
                        .await?
                        .with_batch_size(batch_size)
                        .with_projection(projection)
                        .build()
                })
                .map_err(MergeError::from)?;
            Ok(BatchSource::Store(stream))
        }
        None => {
            let file = File::open(path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
                .with_batch_size(batch_size)
                .with_projection(projection)
                .build()?;
            Ok(BatchSource::Local(reader))
        }
    }
}
