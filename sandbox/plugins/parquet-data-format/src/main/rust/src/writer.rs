/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::compute::{concat_batches, take};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use arrow_ipc::reader::FileReader as IpcFileReader;
use arrow_ipc::writer::FileWriter as IpcFileWriter;
use dashmap::DashMap;
use lazy_static::lazy_static;
use parquet::arrow::ArrowWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::crc_writer::{CrcHandle, CrcWriter};
use crate::memory::write_pool;
use crate::merge::{merge_sorted_with_pool, schema::ROW_ID_COLUMN_NAME};
use crate::native_settings::NativeSettings;
use crate::writer_properties_builder::WriterPropertiesBuilder;
use crate::{log_debug, log_error, log_info};
use native_bridge_common::memory_pool::{MemoryReservation, PoolBehavior};

use object_store::ObjectStore;
use parquet::arrow::async_writer::{AsyncFileWriter, ParquetObjectWriter};
use parquet::arrow::AsyncArrowWriter;
use bytes::Bytes;
use futures::future::BoxFuture;
use crate::store_io::os_store_runtime;

/// An [`AsyncFileWriter`] that tees the Parquet byte stream into a CRC32 hasher before handing it
/// to the underlying [`ParquetObjectWriter`] sink. This is the async counterpart to
/// [`crate::crc_writer::CrcWriter`]: the same whole-file CRC32 that the local path computes, but
/// over the exact bytes uploaded to the `ObjectStore` (multipart, streamed — never staged to a
/// local temp file). The CRC is read out-of-band via a [`CrcHandle`] sharing `hasher`.
struct CrcObjectWriter {
    inner: ParquetObjectWriter,
    hasher: Arc<Mutex<crc32fast::Hasher>>,
}

impl AsyncFileWriter for CrcObjectWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        // Hash synchronously (in write order) before delegating the async upload.
        self.hasher.lock().unwrap().update(&bs);
        self.inner.write(bs)
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        self.inner.complete()
    }
}

/// Build a store-backed streaming Parquet sink (`AsyncArrowWriter` over `ParquetObjectWriter`),
/// teeing every uploaded byte into a returned CRC32 handle. Shared by the non-sorted writer and
/// by sorted-chunk writes.
fn new_store_parquet_sink(
    store: Arc<dyn ObjectStore>,
    object_path: &str,
    schema: Arc<arrow::datatypes::Schema>,
    props: parquet::file::properties::WriterProperties,
) -> Result<(AsyncArrowWriter<CrcObjectWriter>, CrcHandle), Box<dyn std::error::Error>> {
    let object_writer =
        ParquetObjectWriter::new(store, object_store::path::Path::from(object_path));
    let (crc_handle, hasher) = CrcHandle::new_shared();
    let crc_writer = CrcObjectWriter {
        inner: object_writer,
        hasher,
    };
    let writer = AsyncArrowWriter::try_new(crc_writer, schema, Some(props))?;
    Ok((writer, crc_handle))
}

/// Write a single `RecordBatch` as a complete Parquet object into the store (used for sorted
/// chunks). Streams via `AsyncArrowWriter` — no local temp file — and returns the whole-object
/// CRC32. Mirrors [`NativeParquetWriter::write_final_file`] for the store path.
fn write_batch_to_store(
    store: Arc<dyn ObjectStore>,
    object_path: &str,
    index_name: &str,
    batch: &RecordBatch,
    schema: Arc<arrow::datatypes::Schema>,
    writer_generation: Option<i64>,
) -> Result<u32, Box<dyn std::error::Error>> {
    let config = SETTINGS_STORE
        .get(index_name)
        .map(|r| r.clone())
        .unwrap_or_default();
    let props = WriterPropertiesBuilder::build_with_generation(&config, writer_generation, &schema)
        .map_err(|e| format!("Invalid encoding/compression config: {}", e))?;
    let (mut writer, crc_handle) = new_store_parquet_sink(store, object_path, schema, props)?;
    os_store_runtime().block_on(writer.write(batch))?;
    os_store_runtime().block_on(writer.close())?;
    Ok(crc_handle.crc32())
}

/// Collect all non-empty batches from an Arrow IPC file reader (local `File` or in-memory
/// `Cursor` over a store object — both are `Read + Seek`).
fn collect_ipc_batches<R: std::io::Read + std::io::Seek>(
    reader: IpcFileReader<R>,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in reader {
        let batch = batch_result?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok(batches)
}

/// Result from finalizing a writer: Parquet metadata + whole-file CRC32 + optional sort permutation.
#[derive(Debug)]
pub struct FinalizeResult {
    pub metadata: parquet::file::metadata::ParquetMetaData,
    pub crc32: u32,
    /// Flat row ID mapping where mapping[original_row_id] = new_row_id.
    /// Present only when sort columns were configured.
    pub row_id_mapping: Option<Vec<i64>>,
}

/// The underlying writer — either direct Parquet or Arrow IPC staging.
/// When sort columns are configured, the IPC variant is used so that
/// batches can be cheaply read back for sorting — Arrow IPC is a raw
/// dump of in-memory Arrow buffers with minimal framing overhead.
enum WriterVariant {
    /// Direct Parquet writer — used when no sort columns are configured.
    Parquet(Arc<Mutex<ArrowWriter<CrcWriter<File>>>>),
    /// Arrow IPC staging writer — used when sort columns are configured.
    /// Batches are written as raw Arrow IPC; on close they are read back,
    /// sorted, and written as a final Parquet file.
    /// Uses eager sort-and-write: writes batches to IPC staging, and when
    /// the chunk row limit is reached, reads back the IPC, sorts, writes as
    /// sorted Parquet chunk, then starts a new IPC file. At finalize, only
    /// a k-way merge is needed.
    Ipc(Arc<Mutex<SortingChunkedWriter>>),
    /// Store-backed Parquet writer — used for the non-sorted path when a shard `ObjectStore`
    /// handle is present (the production path). The Parquet output is streamed directly into the
    /// `ObjectStore` (multipart upload) via `AsyncArrowWriter`; there is no local temp file and no
    /// post-hoc copy. For hot indices the store is a `LocalFileSystem` rooted at the shard dir, so
    /// the object lands at `filename`.
    ParquetStore(Arc<Mutex<AsyncArrowWriter<CrcObjectWriter>>>),
}

/// IPC staging sink — local file or a store-backed multipart upload. Arrow IPC batches are written
/// incrementally as they arrive; on flush the object/file is read back fully, sorted, and written
/// out as a sorted Parquet chunk.
enum IpcStaging {
    Local(IpcFileWriter<File>),
    Store(IpcFileWriter<crate::store_io::StoreSyncWriter>),
}

impl IpcStaging {
    fn write(&mut self, batch: &RecordBatch) -> Result<(), arrow::error::ArrowError> {
        match self {
            IpcStaging::Local(w) => w.write(batch),
            IpcStaging::Store(w) => w.write(batch),
        }
    }

    /// Write the IPC footer and, for the store sink, `shutdown()` the bridge to finalize the
    /// multipart upload (a plain drop would NOT complete it).
    fn finish_and_close(self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            IpcStaging::Local(mut w) => {
                w.finish()?;
            }
            IpcStaging::Store(mut w) => {
                w.finish()?;
                let mut bridge = w.into_inner()?;
                bridge.shutdown()?;
            }
        }
        Ok(())
    }
}

/// Hybrid IPC staging + eager sort writer.
/// - Writes incoming batches to an IPC file (cheap, no memory accumulation).
/// - When the accumulated chunk byte size plus the incoming batch size would
///   exceed `memory_threshold_bytes`, reads the IPC file back, sorts in memory,
///   writes as a sorted Parquet chunk, deletes the IPC file, and starts a new
///   IPC file.
/// - At finalize: flushes remaining IPC data (sort + write), returns sorted
///   Parquet chunk paths for k-way merge.
struct SortingChunkedWriter {
    /// Base path for local staging/chunk files (used when `store` is `None`).
    base_path: String,
    /// Arrow schema shared across all chunks.
    schema: Arc<arrow::datatypes::Schema>,
    /// Memory budget (bytes) for in-memory sort. When the IPC staging file size
    /// plus the incoming batch size would exceed this threshold, the current chunk
    /// is flushed (sorted and written as Parquet) before accepting the new batch.
    memory_threshold_bytes: u64,
    /// Index name for writer properties.
    index_name: String,
    /// Sort configuration.
    sort_columns: Vec<String>,
    reverse_sorts: Vec<bool>,
    nulls_first: Vec<bool>,
    /// Current IPC writer for staging incoming batches.
    current_ipc_writer: Option<IpcStaging>,
    /// Tracked byte size of the current IPC staging file (approximated from
    /// the Arrow array memory sizes of batches written so far).
    current_chunk_bytes: u64,
    /// Row count in the current IPC staging file.
    current_rows: usize,
    /// Index of the next chunk (0-based).
    chunk_idx: usize,
    /// Paths of all completed sorted Parquet chunk files (local paths or store object paths).
    completed_chunks: Vec<String>,
    /// Row IDs captured from each sorted chunk (for permutation building).
    chunk_row_ids: Vec<Vec<i64>>,
    /// CRC32 values for each completed sorted Parquet chunk file.
    chunk_crcs: Vec<u32>,
    /// Total rows written across all chunks.
    total_rows: usize,
    /// Writer generation propagated into Parquet file metadata for each chunk.
    writer_generation: i64,
    /// Shard store, or `None` for the local path. When set, IPC staging and sorted chunks are
    /// written to / read from / deleted in the store instead of the local filesystem.
    store: Option<Arc<dyn ObjectStore>>,
    /// Basename of the final output; IPC + chunk object paths are derived from it.
    object_base: String,
}

impl SortingChunkedWriter {
    fn new(
        base_path: String,
        schema: Arc<arrow::datatypes::Schema>,
        memory_threshold_bytes: u64,
        index_name: String,
        sort_columns: Vec<String>,
        reverse_sorts: Vec<bool>,
        nulls_first: Vec<bool>,
        writer_generation: i64,
        store: Option<Arc<dyn ObjectStore>>,
        object_base: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut writer = Self {
            base_path,
            schema,
            memory_threshold_bytes,
            index_name,
            sort_columns,
            reverse_sorts,
            nulls_first,
            current_ipc_writer: None,
            current_chunk_bytes: 0,
            current_rows: 0,
            chunk_idx: 0,
            completed_chunks: Vec::new(),
            chunk_row_ids: Vec::new(),
            chunk_crcs: Vec::new(),
            total_rows: 0,
            writer_generation,
            store,
            object_base,
        };
        writer.open_new_ipc()?;
        Ok(writer)
    }

    /// IPC staging path (local temp file when `store` is `None`).
    fn ipc_staging_path(&self) -> String {
        self.base_path.clone()
    }

    /// IPC staging object path within the store root.
    fn ipc_object_path(&self) -> String {
        format!("{}{}", self.object_base, IPC_STAGING_SUFFIX)
    }

    fn sorted_chunk_path(&self, idx: usize) -> String {
        if self.store.is_some() {
            format!("{}.sorted_chunk_{}.parquet", self.object_base, idx)
        } else {
            format!("{}.sorted_chunk_{}.parquet", self.base_path, idx)
        }
    }

    fn open_new_ipc(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let ipc_writer = match &self.store {
            Some(store) => {
                let sink = crate::store_io::store_sync_writer(
                    store.clone(),
                    object_store::path::Path::from(self.ipc_object_path()),
                );
                IpcStaging::Store(IpcFileWriter::try_new(sink, &self.schema)?)
            }
            None => {
                let file = File::create(self.ipc_staging_path())?;
                IpcStaging::Local(IpcFileWriter::try_new(file, &self.schema)?)
            }
        };
        self.current_ipc_writer = Some(ipc_writer);
        self.current_chunk_bytes = 0;
        self.current_rows = 0;
        Ok(())
    }

    fn write(
        &mut self,
        batch: &RecordBatch,
        reservation: &mut MemoryReservation,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.current_ipc_writer.is_none() {
            return Ok(());
        }

        let incoming_batch_bytes = batch.get_array_memory_size() as u64;

        // Check if adding this batch would breach the memory threshold.
        // If the current chunk already has data and the combined size exceeds
        // the budget, flush (sort + write) the current chunk first, then
        // write the new batch into a fresh IPC staging file.
        if self.current_chunk_bytes > 0
            && self.current_chunk_bytes + incoming_batch_bytes > self.memory_threshold_bytes
        {
            self.flush_and_sort_chunk(reservation)?;
        }

        // If the batch itself fits within the threshold, write it directly.
        if incoming_batch_bytes <= self.memory_threshold_bytes {
            if let Some(ref mut w) = self.current_ipc_writer {
                w.write(batch)?;
            }
            self.current_chunk_bytes += incoming_batch_bytes;
            self.current_rows += batch.num_rows();
            self.total_rows += batch.num_rows();
        } else {
            // The batch alone exceeds the memory budget — slice it into pieces
            // that each fit within the threshold, flushing after each piece.
            let num_rows = batch.num_rows();
            let bytes_per_row = incoming_batch_bytes / num_rows as u64;
            // Compute how many rows fit within the threshold (at least 1 to make progress).
            let rows_per_slice =
                std::cmp::max(1, (self.memory_threshold_bytes / bytes_per_row) as usize);

            let mut offset = 0;
            while offset < num_rows {
                let len = std::cmp::min(rows_per_slice, num_rows - offset);
                let slice = batch.slice(offset, len);
                let slice_bytes = slice.get_array_memory_size() as u64;

                if let Some(ref mut w) = self.current_ipc_writer {
                    w.write(&slice)?;
                }
                self.current_chunk_bytes += slice_bytes;
                self.current_rows += len;
                self.total_rows += len;
                offset += len;

                // Flush after each slice that fills the budget.
                if self.current_chunk_bytes >= self.memory_threshold_bytes {
                    self.flush_and_sort_chunk(reservation)?;
                }
            }
        }

        // Safety net: flush if we ended up at or above the threshold.
        if self.current_chunk_bytes >= self.memory_threshold_bytes {
            self.flush_and_sort_chunk(reservation)?;
        }

        Ok(())
    }

    /// Close the current IPC file, read it back, sort, write as sorted Parquet chunk.
    fn flush_and_sort_chunk(
        &mut self,
        reservation: &mut MemoryReservation,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use arrow::array::Int64Array;

        log_debug!(
            "flush_and_sort_chunk: chunk_idx={}, current_chunk_bytes={}, current_rows={}, row_id_memory_size={}",
            self.chunk_idx, self.current_chunk_bytes, self.current_rows, self.memory_size()
        );

        // Reserve memory for sort (read-back + sorted copy coexist = 2× chunk)
        let sort_reserve = self.current_chunk_bytes as usize * 2;
        reservation.request(sort_reserve)?;

        // Close the IPC writer (finalizes the multipart upload for the store sink).
        if let Some(writer) = self.current_ipc_writer.take() {
            writer.finish_and_close()?;
        }

        // Read the staged IPC back fully, then sort. The local path also loads the whole staging
        // file into memory here, so reading a store object into memory adds no extra peak.
        let batches: Vec<RecordBatch> = match &self.store {
            Some(store) => {
                let data = crate::store_io::read_object(
                    store,
                    &object_store::path::Path::from(self.ipc_object_path()),
                )?;
                collect_ipc_batches(IpcFileReader::try_new(std::io::Cursor::new(data), None)?)?
            }
            None => {
                let file = File::open(self.ipc_staging_path())?;
                collect_ipc_batches(IpcFileReader::try_new(file, None)?)?
            }
        };

        if batches.is_empty() {
            // Nothing to sort, just reopen
            reservation.shrink(sort_reserve);
            self.delete_ipc_staging();
            self.open_new_ipc()?;
            return Ok(());
        }

        // Concat and sort
        let combined = concat_batches(&self.schema, &batches)?;
        drop(batches); // free memory before sort allocates
        let sorted_batch = NativeParquetWriter::sort_batch(
            &combined,
            &self.sort_columns,
            &self.reverse_sorts,
            &self.nulls_first,
        )?;
        drop(combined); // free unsorted data

        // Capture original row IDs for permutation building, then rewrite to sequential 0..N
        let row_id_col_idx = self
            .schema
            .fields()
            .iter()
            .position(|f| f.name() == ROW_ID_COLUMN_NAME);
        let final_batch = if let Some(idx) = row_id_col_idx {
            let row_id_array = sorted_batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("___row_id column must be Int64");
            let ids: Vec<i64> = (0..row_id_array.len())
                .map(|i| row_id_array.value(i))
                .collect();
            self.chunk_row_ids.push(ids);

            // Rewrite ___row_id to sequential 0..N so the chunk file is self-consistent
            let sequential_ids =
                Int64Array::from_iter_values((0..sorted_batch.num_rows() as i64).map(|x| x));
            let mut columns = sorted_batch.columns().to_vec();
            columns[idx] = Arc::new(sequential_ids);
            RecordBatch::try_new(self.schema.clone(), columns)?
        } else {
            sorted_batch
        };

        // Write sorted chunk as Parquet (to the store, or a local file when store is None)
        let chunk_path = self.sorted_chunk_path(self.chunk_idx);
        let crc32 = match &self.store {
            Some(store) => write_batch_to_store(
                store.clone(),
                &chunk_path,
                &self.index_name,
                &final_batch,
                self.schema.clone(),
                Some(self.writer_generation),
            )?,
            None => NativeParquetWriter::write_final_file(
                &chunk_path,
                &self.index_name,
                &final_batch,
                self.schema.clone(),
                Some(self.writer_generation),
            )?,
        };

        self.completed_chunks.push(chunk_path);
        self.chunk_crcs.push(crc32);
        self.chunk_idx += 1;

        // Release sort memory
        // Release sort working memory (read-back + sorted copy no longer alive)
        reservation.shrink(sort_reserve);
        // Track accumulated row_ids for this chunk — Vec<i64> per chunk persists until finish()
        let row_ids_bytes = self.current_rows * std::mem::size_of::<i64>();
        reservation.grow(row_ids_bytes);

        // Delete the IPC staging object/file and open a fresh one
        self.delete_ipc_staging();
        self.open_new_ipc()?;
        Ok(())
    }

    /// Delete the current IPC staging artifact (store object or local file); best-effort.
    fn delete_ipc_staging(&self) {
        match &self.store {
            Some(store) => {
                let _ = crate::store_io::delete_object(
                    store,
                    &object_store::path::Path::from(self.ipc_object_path()),
                );
            }
            None => {
                let _ = std::fs::remove_file(self.ipc_staging_path());
            }
        }
    }

    /// Finalize: flush remaining IPC data (sort + write) and return chunk paths + row IDs + CRCs.
    fn finish(
        mut self,
        reservation: &mut MemoryReservation,
    ) -> Result<(Vec<String>, Vec<Vec<i64>>, Vec<u32>), Box<dyn std::error::Error>> {
        if self.current_rows > 0 {
            self.flush_and_sort_chunk(reservation)?;
        }
        // Close and remove the trailing IPC staging artifact
        if let Some(writer) = self.current_ipc_writer.take() {
            writer.finish_and_close()?;
        }
        self.delete_ipc_staging();
        Ok((self.completed_chunks, self.chunk_row_ids, self.chunk_crcs))
    }

    fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// Returns the actual in-memory footprint of this writer's heap allocations.
    ///
    /// The IPC staging approach means record batch data lives on disk, not in memory.
    /// What *is* held in memory:
    /// - `chunk_row_ids`: accumulated row ID vectors from all completed chunks
    ///   (retained until `finish()` for permutation building).
    ///
    /// This does NOT include the transient peak during `flush_and_sort_chunk()`
    /// where the IPC file is read back and sorted — that's short-lived and freed
    /// before the method returns.
    fn memory_size(&self) -> usize {
        self.chunk_row_ids
            .iter()
            .map(|ids| ids.len() * std::mem::size_of::<i64>())
            .sum()
    }
}

/// Bundles all per-writer resources. The writer's lifetime is owned by Java via an opaque
/// handle (`Box::into_raw`); dropping the box (on finalize/free) closes the file handle,
/// releases the memory reservation, and cleans up sort config. Stores its own file paths
/// (previously the registry key) so the handle-based FFI entry points don't need them passed in.
pub struct WriterState {
    variant: WriterVariant,
    settings: NativeSettings,
    crc_handle: Option<crate::crc_writer::CrcHandle>,
    writer_generation: i64,
    reservation: MemoryReservation,
    /// Final output path (temp file is renamed to this on finalize).
    filename: String,
    /// Temporary path written to before finalize (`temp-<basename>`); used only by the local
    /// (`store == None`) non-sorted path. Store-backed variants stream to the `ObjectStore`.
    temp_filename: String,
    /// Shard-scoped store (cloned from the engine handle), or `None` for the local path (native
    /// unit tests). Used by the sorted (IPC) variant at finalize to run the k-way merge through
    /// the store, and carried so the store outlives all in-flight writer ops.
    store: Option<Arc<dyn ObjectStore>>,
    /// Object path (basename of `filename`) for the final output within the store root.
    object_path: String,
}

/// Path suffix for the intermediate Arrow IPC file used during sort-on-close.
const IPC_STAGING_SUFFIX: &str = ".arrow_ipc_staging";

lazy_static! {
    pub static ref SETTINGS_STORE: DashMap<String, NativeSettings> = DashMap::new();
}

pub struct NativeParquetWriter;

impl NativeParquetWriter {
    /// Build the temp filename by prepending "temp-" to the basename.
    fn temp_filename(filename: &str) -> String {
        let path = Path::new(filename);
        path.parent()
            .unwrap_or_else(|| Path::new(""))
            .join(format!(
                "temp-{}",
                path.file_name().unwrap().to_str().unwrap()
            ))
            .to_string_lossy()
            .to_string()
    }

    pub fn create_writer(
        filename: String,
        index_name: String,
        schema_address: i64,
        sort_columns: Vec<String>,
        reverse_sorts: Vec<bool>,
        nulls_first: Vec<bool>,
        writer_generation: i64,
        store_handle: i64,
    ) -> Result<*mut WriterState, Box<dyn std::error::Error>> {
        log_debug!(
            "create_writer called for file: {}, index: {}, schema_address: {}, sort_columns: {:?}, reverse_sorts: {:?}, nulls_first: {:?}, writer_generation: {}, store_handle: {}",
            filename, index_name, schema_address, sort_columns, reverse_sorts, nulls_first, writer_generation, store_handle
        );

        if (schema_address as *mut u8).is_null() {
            log_error!(
                "ERROR: Invalid schema address (null pointer) for file: {}",
                filename
            );
            return Err("Invalid schema address".into());
        }

        let temp_filename = Self::temp_filename(&filename);
        let object_path = Path::new(&filename)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(&filename)
            .to_string();
        // Clone the engine-owned store Arc once (never consume the box), or None for the local
        // path (native unit tests). Shared by all store-backed sinks and the sorted finalize.
        let store: Option<Arc<dyn ObjectStore>> = if store_handle > 0 {
            Some(unsafe { (*(store_handle as *const Arc<dyn ObjectStore>)).clone() })
        } else {
            None
        };
        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);
        log_debug!("Schema created with {} fields", schema.fields().len());

        let mut settings: NativeSettings = SETTINGS_STORE
            .get(&index_name)
            .map(|r| r.clone())
            .unwrap_or_default();
        settings.index_name = Some(index_name.clone());
        settings.sort_columns = sort_columns;
        settings.reverse_sorts = reverse_sorts;
        settings.nulls_first = nulls_first;

        SETTINGS_STORE.insert(index_name.clone(), settings.clone());

        // If sort columns are configured, use eager sort-and-write path:
        // accumulate batches in memory, sort each chunk, write as Parquet.
        // At finalize, only a k-way merge is needed (no IPC re-read).
        let (variant, crc_handle) = if !settings.sort_columns.is_empty() {
            let base_path = format!("{}{}", temp_filename, IPC_STAGING_SUFFIX);
            let memory_threshold_bytes = settings.get_sort_in_memory_threshold_bytes();
            let chunked_writer = SortingChunkedWriter::new(
                base_path,
                schema,
                memory_threshold_bytes,
                index_name.clone(),
                settings.sort_columns.clone(),
                settings.reverse_sorts.clone(),
                settings.nulls_first.clone(),
                writer_generation,
                store.clone(),
                object_path.clone(),
            )?;
            (
                WriterVariant::Ipc(Arc::new(Mutex::new(chunked_writer))),
                None,
            )
        } else {
            let props = WriterPropertiesBuilder::build_with_generation(
                &settings,
                Some(writer_generation),
                &schema,
            )
            .map_err(|e| format!("Invalid encoding/compression config: {}", e))?;

            if let Some(store_ref) = store.clone() {
                // Production path: stream the Parquet output straight into the shard ObjectStore.
                let (writer, crc_handle) =
                    new_store_parquet_sink(store_ref, &object_path, schema, props)?;
                (
                    WriterVariant::ParquetStore(Arc::new(Mutex::new(writer))),
                    Some(crc_handle),
                )
            } else {
                // Legacy local path (native unit tests / no store): synchronous ArrowWriter to a
                // local temp file, renamed into place at finalize.
                let file = File::create(&temp_filename)?;
                let (crc_file, crc_handle) = CrcWriter::new(file);
                let writer = ArrowWriter::try_new(crc_file, schema, Some(props))?;
                (
                    WriterVariant::Parquet(Arc::new(Mutex::new(writer))),
                    Some(crc_handle),
                )
            }
        };

        let state = Box::new(WriterState {
            variant,
            settings,
            crc_handle,
            writer_generation,
            reservation: MemoryReservation::new(
                write_pool(),
                "parquet_writer",
                PoolBehavior::IgnoreLimit,
            ),
            filename,
            temp_filename,
            store,
            object_path,
        });
        let handle = Box::into_raw(state);
        Ok(handle)
    }

    pub fn write_data(
        handle: *mut WriterState,
        array_address: i64,
        schema_address: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if handle.is_null() {
            return Err("Invalid writer handle (null)".into());
        }
        // Safety: `handle` is a live pointer minted by `create_writer` and owned by Java, which
        // serializes all handle-touching native calls (write/finalize/free/memory) under a
        // per-writer lock, so there is no concurrent `&mut`/`&` aliasing on this allocation.
        // Reborrow only (not Box::from_raw): does NOT take ownership, so the writer is not dropped here and lives on for subsequent writes.
        let state = unsafe { &mut *handle };
        log_debug!("write_data called for temp file: {}", state.temp_filename);

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            log_error!(
                "ERROR: Invalid FFI addresses for file: {}",
                state.temp_filename
            );
            return Err("Invalid FFI addresses (null pointers)".into());
        }

        unsafe {
            let arrow_schema = FFI_ArrowSchema::from_raw(schema_address as *mut _);
            let arrow_array = FFI_ArrowArray::from_raw(array_address as *mut _);
            let array_data = arrow::ffi::from_ffi(arrow_array, &arrow_schema)?;
            let array: Arc<dyn arrow::array::Array> = arrow::array::make_array(array_data);

            if let Some(struct_array) = array.as_any().downcast_ref::<arrow::array::StructArray>() {
                let schema = Arc::new(arrow::datatypes::Schema::new(struct_array.fields().clone()));
                let record_batch = RecordBatch::try_new(schema, struct_array.columns().to_vec())?;
                log_debug!(
                    "Created RecordBatch with {} rows and {} columns",
                    record_batch.num_rows(),
                    record_batch.num_columns()
                );

                match &state.variant {
                    WriterVariant::Ipc(writer_arc) => {
                        log_debug!("Writing RecordBatch to IPC staging file");
                        let writer_arc = writer_arc.clone();
                        let mut writer = writer_arc.lock().unwrap();
                        writer.write(&record_batch, &mut state.reservation)?;
                    }
                    WriterVariant::Parquet(writer_arc) => {
                        log_debug!("Writing RecordBatch to Parquet file");
                        let batch_bytes = record_batch.get_array_memory_size();
                        // Reserve 3× batch as estimate — ArrowWriter encoding may temporarily
                        // hold dictionary, compressed pages, and data page buffers.
                        let estimated = batch_bytes * 3;
                        let writer_arc = writer_arc.clone();
                        state.reservation.reserve_estimated(estimated)?;
                        let mut writer = writer_arc.lock().unwrap();
                        let before = writer.memory_size();
                        writer.write(&record_batch)?;
                        // Reconcile: adjust reservation to actual delta reported by ArrowWriter
                        let actual = writer.memory_size().saturating_sub(before);
                        drop(writer);
                        state.reservation.reconcile(estimated, actual);
                    }
                    WriterVariant::ParquetStore(writer_arc) => {
                        log_debug!("Writing RecordBatch to Parquet ObjectStore sink");
                        let batch_bytes = record_batch.get_array_memory_size();
                        // Same 3× estimate as the local path; AsyncArrowWriter exposes the same
                        // `memory_size()` for the in-progress buffered encoding.
                        let estimated = batch_bytes * 3;
                        let writer_arc = writer_arc.clone();
                        state.reservation.reserve_estimated(estimated)?;
                        let mut writer = writer_arc.lock().unwrap();
                        let before = writer.memory_size();
                        // Drive the async write to completion. Java serializes handle-touching
                        // calls per writer, so no other thread contends this lock/runtime slot.
                        os_store_runtime().block_on(writer.write(&record_batch))?;
                        let actual = writer.memory_size().saturating_sub(before);
                        drop(writer);
                        state.reservation.reconcile(estimated, actual);
                    }
                }
                Ok(())
            } else {
                log_error!(
                    "ERROR: Array is not a StructArray, type: {:?}",
                    array.data_type()
                );
                Err("Expected struct array from VectorSchemaRoot".into())
            }
        }
    }

    pub fn finalize_writer(
        handle: *mut WriterState,
    ) -> Result<Option<FinalizeResult>, Box<dyn std::error::Error>> {
        if handle.is_null() {
            return Ok(None);
        }
        // Safety: Java hands back a live handle exactly once for finalize; reclaim ownership so the
        // Box (and its reservation + open file handles) is dropped when this function returns.
        // Box::from_raw TAKES ownership: the writer is dropped exactly once here (success path).
        let WriterState {
            variant,
            settings,
            crc_handle,
            writer_generation,
            mut reservation,
            filename,
            temp_filename,
            store,
            object_path,
        } = unsafe { *Box::from_raw(handle) };
        log_debug!(
            "finalize_writer called for file: {} (temp: {})",
            filename,
            temp_filename
        );
        let index_name = settings.index_name.as_deref().unwrap_or("");

        match variant {
            WriterVariant::Ipc(writer_arc) => {
                match Arc::try_unwrap(writer_arc) {
                    Ok(mutex) => {
                        let chunked_writer = mutex.into_inner().unwrap();
                        let total_rows = chunked_writer.total_rows();
                        let schema = chunked_writer.schema.clone();
                        let (chunk_paths, chunk_row_ids, chunk_crcs) =
                            chunked_writer.finish(&mut reservation)?;
                        log_info!(
                                "Successfully closed sorting chunked writer for: {}, total_rows={}, chunks={}",
                                temp_filename, total_rows, chunk_paths.len()
                            );

                        let (crc32, row_id_mapping, merged_metadata) =
                            Self::finalize_sorted_chunks(
                                &chunk_paths,
                                &chunk_row_ids,
                                &chunk_crcs,
                                &filename,
                                &object_path,
                                store.as_ref(),
                                index_name,
                                &settings.sort_columns,
                                &settings.reverse_sorts,
                                &settings.nulls_first,
                                writer_generation,
                                schema.clone(),
                                &mut reservation,
                            )?;

                        // Clean up sorted chunk artifacts only after successful finalization.
                        // On failure, chunks are preserved as they may be the only copy of the data.
                        for path in &chunk_paths {
                            match &store {
                                Some(s) => {
                                    let _ = crate::store_io::delete_object(
                                        s,
                                        &object_store::path::Path::from(path.as_str()),
                                    );
                                }
                                None => {
                                    let _ = std::fs::remove_file(path);
                                }
                            }
                        }

                        log_debug!("CRC32 for file {}: {:#010x}", filename, crc32);

                        // Prefer the metadata produced by the merge (backend-agnostic). Only the
                        // single-chunk/empty store cases and the local path fall back to reading
                        // the final file (for hot indices it is a real local file at `filename`).
                        let parquet_metadata = match merged_metadata {
                            Some(md) => md,
                            None => {
                                let file = File::open(&filename)?;
                                let reader = SerializedFileReader::new(file)?;
                                reader.metadata().clone()
                            }
                        };

                        // Detach mapping from reservation before handing to FFI/Java.
                        // FFI layer will track it via write_pool().grow/shrink.
                        if let Some(ref mapping) = row_id_mapping {
                            reservation.shrink(mapping.len() * std::mem::size_of::<i64>());
                        }

                        Ok(Some(FinalizeResult {
                            metadata: parquet_metadata,
                            crc32,
                            row_id_mapping,
                        }))
                    }
                    Err(_) => {
                        log_error!(
                            "ERROR: IPC Writer still in use for temp file: {}",
                            temp_filename
                        );
                        Err("IPC Writer still in use".into())
                    }
                }
            }
            WriterVariant::Parquet(writer_arc) => {
                match Arc::try_unwrap(writer_arc) {
                    Ok(mutex) => {
                        let writer = mutex.into_inner().unwrap();
                        match writer.close() {
                            Ok(_) => {
                                let crc32 = crc_handle.map(|h| h.crc32()).unwrap_or(0);
                                log_info!("Successfully closed temp writer for: {}", temp_filename);

                                // Legacy local path (no ObjectStore): atomically move the finished
                                // temp file into place.
                                std::fs::rename(&temp_filename, &filename)?;

                                log_debug!("CRC32 for file {}: {:#010x}", filename, crc32);

                                let file = File::open(&filename)?;
                                let reader = SerializedFileReader::new(file)?;
                                let parquet_metadata = reader.metadata().clone();

                                Ok(Some(FinalizeResult {
                                    metadata: parquet_metadata,
                                    crc32,
                                    row_id_mapping: None,
                                }))
                            }
                            Err(e) => {
                                log_error!(
                                    "ERROR: Failed to close writer for temp file: {}",
                                    temp_filename
                                );
                                Err(e.into())
                            }
                        }
                    }
                    Err(_) => {
                        log_error!(
                            "ERROR: Writer still in use for temp file: {}",
                            temp_filename
                        );
                        Err("Writer still in use".into())
                    }
                }
            }
            WriterVariant::ParquetStore(writer_arc) => {
                match Arc::try_unwrap(writer_arc) {
                    Ok(mutex) => {
                        let writer = mutex.into_inner().unwrap();
                        // `close()` force-flushes the buffered Parquet, completes the ObjectStore
                        // multipart upload, and returns the full ParquetMetaData — so, unlike the
                        // local path, there is no file re-open (works for any store backend). The
                        // CrcObjectWriter has hashed every uploaded byte into `crc_handle`.
                        let parquet_metadata =
                            os_store_runtime().block_on(writer.close())?;
                        let crc32 = crc_handle.map(|h| h.crc32()).unwrap_or(0);
                        log_info!(
                            "Successfully closed store-backed writer for: {} (crc32={:#010x})",
                            filename,
                            crc32
                        );
                        Ok(Some(FinalizeResult {
                            metadata: parquet_metadata,
                            crc32,
                            row_id_mapping: None,
                        }))
                    }
                    Err(_) => {
                        log_error!(
                            "ERROR: Store-backed writer still in use for file: {}",
                            filename
                        );
                        Err("Store-backed writer still in use".into())
                    }
                }
            }
        }
    }

    /// Finalize pre-sorted Parquet chunks: k-way merge them into the final file.
    /// Chunks are already sorted (done eagerly during write), so no sort needed here.
    /// For single chunk, just rename. For empty, write empty Parquet.
    fn finalize_sorted_chunks(
        chunk_paths: &[String],
        chunk_row_ids: &[Vec<i64>],
        chunk_crcs: &[u32],
        output_filename: &str,
        output_object_path: &str,
        store: Option<&Arc<dyn ObjectStore>>,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
        writer_generation: i64,
        schema: Arc<arrow::datatypes::Schema>,
        reservation: &mut MemoryReservation,
    ) -> Result<(u32, Option<Vec<i64>>, Option<parquet::file::metadata::ParquetMetaData>), Box<dyn std::error::Error>>
    {
        if chunk_paths.is_empty() {
            log_info!("finalize_sorted_chunks: no chunks, writing empty Parquet file");
            let config = SETTINGS_STORE
                .get(index_name)
                .map(|r| r.clone())
                .unwrap_or_default();
            let props = WriterPropertiesBuilder::build_with_generation(
                &config,
                Some(writer_generation),
                &schema,
            )
            .map_err(|e| format!("Invalid encoding/compression config: {}", e))?;
            match store {
                Some(s) => {
                    // Empty Parquet straight to the store.
                    let (writer, _crc) =
                        new_store_parquet_sink(s.clone(), output_object_path, schema, props)?;
                    os_store_runtime().block_on(writer.close())?;
                }
                None => {
                    let file = File::create(output_filename)?;
                    let writer = ArrowWriter::try_new(file, schema, Some(props))?;
                    writer.close()?;
                }
            }
            return Ok((0, None, None));
        }

        if chunk_paths.len() == 1 {
            // Single chunk is already sorted Parquet — move it to the final output.
            // Store: metadata-only rename within the store. Local: filesystem rename.
            log_info!("finalize_sorted_chunks: single chunk, renaming to final output");
            match store {
                Some(s) => {
                    crate::store_io::rename_object(
                        s,
                        &object_store::path::Path::from(chunk_paths[0].as_str()),
                        &object_store::path::Path::from(output_object_path),
                    )?;
                }
                None => {
                    std::fs::rename(&chunk_paths[0], output_filename)?;
                }
            }

            // Use the CRC computed when the chunk was written
            let crc32 = chunk_crcs.first().copied().unwrap_or(0);

            // Build permutation from the single chunk's row IDs
            let row_id_mapping = if !chunk_row_ids.is_empty() && !chunk_row_ids[0].is_empty() {
                let ids = &chunk_row_ids[0];
                let total = ids.len();
                let mapping_bytes = total * std::mem::size_of::<i64>();
                // Reserve for permutation Vec<i64> before allocating
                reservation.request(mapping_bytes)?;
                let mut mapping = vec![0i64; total];
                for (new_pos, &old_row_id) in ids.iter().enumerate() {
                    let orig_idx = old_row_id as usize;
                    if orig_idx < total {
                        mapping[orig_idx] = new_pos as i64;
                    }
                }
                Some(mapping)
            } else {
                None
            };

            return Ok((crc32, row_id_mapping, None));
        }

        // Multiple chunks: k-way merge
        let overall_start = std::time::Instant::now();
        log_info!(
            "finalize_sorted_chunks: merging {} pre-sorted chunks for {}",
            chunk_paths.len(),
            output_filename
        );

        let mut merge_reservation = MemoryReservation::new(
            write_pool(),
            "writer:k_way_merge",
            PoolBehavior::IgnoreLimit,
        );
        let merge_output = merge_sorted_with_pool(
            chunk_paths,
            if store.is_some() {
                output_object_path
            } else {
                output_filename
            },
            index_name,
            sort_columns,
            reverse_sorts,
            nulls_first,
            writer_generation,
            &mut merge_reservation,
            store.cloned(),
        )
        .map_err(|e| -> Box<dyn std::error::Error> {
            format!("Streaming merge failed: {}", e).into()
        })?;
        let merge_duration = overall_start.elapsed();
        log_info!(
            "finalize_sorted_chunks: k-way merge complete: {} chunks merged, duration={:?}",
            chunk_paths.len(),
            merge_duration
        );

        // Take the merged output's metadata (backend-agnostic) and mapping; destructure so we can
        // free the merge mapping without holding the whole struct.
        let crc32 = merge_output.crc32;
        let merged_metadata = merge_output.metadata;
        let merge_mapping = merge_output.mapping;
        let row_id_mapping = if !merge_mapping.is_empty() && !chunk_row_ids.is_empty() {
            let total = merge_mapping.len();
            let mapping_bytes = total * std::mem::size_of::<i64>();
            // Reserve 2× mapping: merge mapping (alive) + flat_mapping (about to allocate)
            reservation.request(mapping_bytes * 2)?;
            let mut flat_mapping = vec![0i64; total];
            for i in 0..total {
                flat_mapping[i] = i as i64;
            }
            let mut pos = 0usize;
            for chunk_ids in chunk_row_ids {
                for &original_row_id in chunk_ids {
                    let orig_idx = original_row_id as usize;
                    if orig_idx < total && pos < total {
                        flat_mapping[orig_idx] = merge_mapping[pos];
                    }
                    pos += 1;
                }
            }
            drop(merge_mapping);
            // merge mapping freed — release its share, flat_mapping remains tracked
            reservation.shrink(mapping_bytes);
            log_info!(
                "finalize_sorted_chunks: produced {} permutation entries for {}",
                flat_mapping.len(),
                output_filename
            );
            Some(flat_mapping)
        } else {
            None
        };

        log_info!(
            "finalize_sorted_chunks: DONE file={}, chunks={}, merge_duration={:?}",
            output_filename,
            chunk_paths.len(),
            merge_duration
        );
        Ok((crc32, row_id_mapping, Some(merged_metadata)))
    }

    /// Sort a batch using RowConverter: converts sort columns into compact
    /// byte-comparable rows, sorts indices by comparing those rows, then
    /// reorders all columns via take.
    fn sort_batch(
        batch: &RecordBatch,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let sort_fields: Vec<SortField> = sort_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| {
                let col_index = batch
                    .schema()
                    .index_of(col_name)
                    .map_err(|_| format!("Sort column '{}' not found in schema", col_name))?;
                let data_type = batch.schema().field(col_index).data_type().clone();
                let options = arrow::compute::SortOptions {
                    descending: reverse_sorts.get(i).copied().unwrap_or(false),
                    nulls_first: nulls_first.get(i).copied().unwrap_or(false),
                };
                Ok(SortField::new_with_options(data_type, options))
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

        let converter = RowConverter::new(sort_fields)?;

        let sort_arrays: Vec<Arc<dyn arrow::array::Array>> = sort_columns
            .iter()
            .map(|col_name| {
                let col_index = batch.schema().index_of(col_name).unwrap();
                batch.column(col_index).clone()
            })
            .collect();

        let rows = converter.convert_columns(&sort_arrays)?;
        let mut sort_indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
        sort_indices.sort_unstable_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));

        let indices = arrow::array::UInt32Array::from(sort_indices);
        let sorted_columns: Result<Vec<_>, _> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect();

        Ok(RecordBatch::try_new(batch.schema(), sorted_columns?)?)
    }

    fn write_final_file(
        output_filename: &str,
        index_name: &str,
        batch: &RecordBatch,
        schema: Arc<arrow::datatypes::Schema>,
        writer_generation: Option<i64>,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let config = SETTINGS_STORE
            .get(index_name)
            .map(|r| r.clone())
            .unwrap_or_default();
        let props =
            WriterPropertiesBuilder::build_with_generation(&config, writer_generation, &schema)
                .map_err(|e| format!("Invalid encoding/compression config: {}", e))?;
        let file = File::create(output_filename)?;
        let (crc_file, crc_handle) = CrcWriter::new(file);
        let mut writer = ArrowWriter::try_new(crc_file, schema, Some(props))?;
        writer.write(batch)?;
        writer.close()?;
        let crc32 = crc_handle.crc32();
        log_debug!(
            "Successfully wrote final file: {} (crc32={:#010x})",
            output_filename,
            crc32
        );
        Ok(crc32)
    }

    /// Returns the native memory reserved by the writer identified by `handle`, or 0 if the handle
    /// is null. Access is serialized by the Java-side monitor, so the shared borrow never aliases a
    /// concurrent `&mut`/reclaim. Never panics: any unexpected panic is caught and reported as 0, so
    /// this can never unwind across the FFI boundary or fail the Java caller.
    pub fn get_writer_memory_usage(handle: *const WriterState) -> usize {
        if handle.is_null() {
            return 0;
        }
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Safety: live handle, access serialized by the Java-side monitor.
            // Shared reborrow only (not Box::from_raw): does NOT take ownership, so the writer is not dropped here.
            let state = unsafe { &*handle };
            state.reservation.size()
        }))
        .unwrap_or_else(|_| {
            log_error!("get_writer_memory_usage: swallowed panic; reporting 0");
            0
        })
    }

    /// Best-effort teardown of a writer that Java is abandoning without finalizing (idempotent on
    /// the Java side via a released-flag). Reclaims the Box (dropping the reservation and closing
    /// file handles) and deletes any leftover temp/IPC/sorted-chunk artifacts. Never fails and
    /// never panics — filesystem errors are ignored and any unexpected panic is caught, so this can
    /// never unwind across the FFI boundary or fail the Java caller.
    pub fn free_writer(handle: *mut WriterState) {
        if handle.is_null() {
            return;
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Safety: Java guarantees free is called at most once per handle (released CAS + Cleaner).
            // Box::from_raw TAKES ownership: the writer is dropped exactly once here (abandonment path).
            let state = unsafe { *Box::from_raw(handle) };
            let temp_filename = state.temp_filename.clone();
            let ipc_base = format!("{}{}", temp_filename, IPC_STAGING_SUFFIX);
            // Drop first so the underlying File handles are closed before we unlink.
            drop(state);
            let _ = std::fs::remove_file(&temp_filename);
            let _ = std::fs::remove_file(&ipc_base);
            // Sorted-chunk files are named "{ipc_base}.sorted_chunk_{i}.parquet"; remove any remaining.
            let mut idx = 0usize;
            loop {
                let chunk = format!("{}.sorted_chunk_{}.parquet", ipc_base, idx);
                if std::fs::remove_file(&chunk).is_err() {
                    break;
                }
                idx += 1;
            }
        }));
        if result.is_err() {
            log_error!("free_writer: swallowed panic while freeing writer handle");
        }
    }

    pub fn get_file_metadata(
        filename: String,
    ) -> Result<parquet::file::metadata::ParquetMetaData, Box<dyn std::error::Error>> {
        let file = File::open(&filename)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata().clone();
        log_debug!(
            "Metadata for {}: version={}, num_rows={}, num_row_groups={}",
            filename,
            metadata.file_metadata().version(),
            metadata.file_metadata().num_rows(),
            metadata.num_row_groups()
        );
        Ok(metadata)
    }
}
