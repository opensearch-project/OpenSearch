/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow::compute::{concat_batches, take};
use arrow::row::{RowConverter, SortField};
use arrow_ipc::writer::FileWriter as IpcFileWriter;
use arrow_ipc::reader::FileReader as IpcFileReader;
use dashmap::DashMap;
use lazy_static::lazy_static;
use parquet::arrow::ArrowWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::{log_error, log_debug, log_info};
use crate::crc_writer::CrcWriter;
use crate::merge::{merge_sorted, schema::ROW_ID_COLUMN_NAME};
use crate::native_settings::NativeSettings;
use crate::writer_properties_builder::WriterPropertiesBuilder;

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
    /// Base path for staging/chunk files.
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
    current_ipc_writer: Option<IpcFileWriter<File>>,
    /// Tracked byte size of the current IPC staging file (approximated from
    /// the Arrow array memory sizes of batches written so far).
    current_chunk_bytes: u64,
    /// Row count in the current IPC staging file.
    current_rows: usize,
    /// Index of the next chunk (0-based).
    chunk_idx: usize,
    /// Paths of all completed sorted Parquet chunk files.
    completed_chunks: Vec<String>,
    /// Row IDs captured from each sorted chunk (for permutation building).
    chunk_row_ids: Vec<Vec<i64>>,
    /// Total rows written across all chunks.
    total_rows: usize,
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
            total_rows: 0,
        };
        writer.open_new_ipc()?;
        Ok(writer)
    }

    fn ipc_staging_path(&self) -> String {
        self.base_path.clone()
    }

    fn sorted_chunk_path(&self, idx: usize) -> String {
        format!("{}.sorted_chunk_{}.parquet", self.base_path, idx)
    }

    fn open_new_ipc(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let path = self.ipc_staging_path();
        let file = File::create(&path)?;
        let ipc_writer = IpcFileWriter::try_new(file, &self.schema)?;
        self.current_ipc_writer = Some(ipc_writer);
        self.current_chunk_bytes = 0;
        self.current_rows = 0;
        Ok(())
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
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
            self.flush_and_sort_chunk()?;
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
            let rows_per_slice = std::cmp::max(
                1,
                (self.memory_threshold_bytes / bytes_per_row) as usize,
            );

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
                    self.flush_and_sort_chunk()?;
                }
            }
        }

        // Safety net: flush if we ended up at or above the threshold.
        if self.current_chunk_bytes >= self.memory_threshold_bytes {
            self.flush_and_sort_chunk()?;
        }

        Ok(())
    }

    /// Close the current IPC file, read it back, sort, write as sorted Parquet chunk.
    fn flush_and_sort_chunk(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        use arrow::array::Int64Array;

        log_debug!(
            "flush_and_sort_chunk: chunk_idx={}, current_chunk_bytes={}, current_rows={}, row_id_memory_size={}",
            self.chunk_idx, self.current_chunk_bytes, self.current_rows, self.memory_size()
        );

        // Close the IPC writer
        if let Some(mut writer) = self.current_ipc_writer.take() {
            writer.finish()?;
        }

        let ipc_path = self.ipc_staging_path();

        // Read back the IPC file (still hot in page cache since we just wrote it)
        let file = File::open(&ipc_path)?;
        let reader = IpcFileReader::try_new(file, None)?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        for batch_result in reader {
            let batch = batch_result?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        if batches.is_empty() {
            // Nothing to sort, just reopen
            let _ = std::fs::remove_file(&ipc_path);
            self.open_new_ipc()?;
            return Ok(());
        }

        // Concat and sort
        let combined = concat_batches(&self.schema, &batches)?;
        drop(batches); // free memory before sort allocates
        let sorted_batch = NativeParquetWriter::sort_batch(
            &combined, &self.sort_columns, &self.reverse_sorts, &self.nulls_first,
        )?;
        drop(combined); // free unsorted data

        // Capture row IDs for permutation building
        let row_id_col_idx = self.schema.fields().iter().position(|f| f.name() == ROW_ID_COLUMN_NAME);
        if let Some(idx) = row_id_col_idx {
            let row_id_array = sorted_batch.column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("___row_id column must be Int64");
            let ids: Vec<i64> = (0..row_id_array.len())
                .map(|i| row_id_array.value(i))
                .collect();
            self.chunk_row_ids.push(ids);
        }

        // Write sorted chunk as Parquet
        let chunk_path = self.sorted_chunk_path(self.chunk_idx);
        NativeParquetWriter::write_final_file(
            &chunk_path, &self.index_name, &sorted_batch, self.schema.clone(), Some(self.chunk_idx as i64),
        )?;

        self.completed_chunks.push(chunk_path);
        self.chunk_idx += 1;

        // Delete the IPC staging file and open a fresh one
        let _ = std::fs::remove_file(&ipc_path);
        self.open_new_ipc()?;
        Ok(())
    }

    /// Finalize: flush remaining IPC data (sort + write) and return chunk paths + row IDs.
    fn finish(mut self) -> Result<(Vec<String>, Vec<Vec<i64>>), Box<dyn std::error::Error>> {
        if self.current_rows > 0 {
            self.flush_and_sort_chunk()?;
        }
        // Close and remove the trailing IPC staging file
        if let Some(mut writer) = self.current_ipc_writer.take() {
            writer.finish()?;
        }
        let _ = std::fs::remove_file(&self.ipc_staging_path());
        Ok((self.completed_chunks, self.chunk_row_ids))
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
        self.chunk_row_ids.iter()
            .map(|ids| ids.len() * std::mem::size_of::<i64>())
            .sum()
    }
}

/// Bundles all per-writer resources so a single `DashMap::remove` atomically
/// drops the writer, closes the file handle, and cleans up sort config.
struct WriterState {
    variant: WriterVariant,
    settings: NativeSettings,
    crc_handle: Option<crate::crc_writer::CrcHandle>,
    writer_generation: i64,
}

/// Path suffix for the intermediate Arrow IPC file used during sort-on-close.
const IPC_STAGING_SUFFIX: &str = ".arrow_ipc_staging";

lazy_static! {
    /// Unified per-writer registry. Keyed by temp filename.
    /// Holds both Parquet and IPC writers via the `WriterVariant` enum.
    static ref WRITERS: DashMap<String, WriterState> = DashMap::new();
    pub static ref SETTINGS_STORE: DashMap<String, NativeSettings> = DashMap::new();
    /// Holds file handles for finalized files pending fsync. Removed after sync.
    static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
}

pub struct NativeParquetWriter;

impl NativeParquetWriter {
    /// Returns true if a writer is currently open for the given filename.
    pub fn has_writer(filename: &str) -> bool {
        let temp_filename = Self::temp_filename(filename);
        WRITERS.contains_key(&temp_filename)
    }
    /// Build the temp filename by prepending "temp-" to the basename.
    fn temp_filename(filename: &str) -> String {
        let path = Path::new(filename);
        path.parent()
            .unwrap_or_else(|| Path::new(""))
            .join(format!("temp-{}", path.file_name().unwrap().to_str().unwrap()))
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
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!(
            "create_writer called for file: {}, index: {}, schema_address: {}, sort_columns: {:?}, reverse_sorts: {:?}, nulls_first: {:?}, writer_generation: {}",
            filename, index_name, schema_address, sort_columns, reverse_sorts, nulls_first, writer_generation
        );

        if (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid schema address (null pointer) for file: {}", filename);
            return Err("Invalid schema address".into());
        }

        let temp_filename = Self::temp_filename(&filename);

        if WRITERS.contains_key(&temp_filename) {
            log_error!("ERROR: Writer already exists for file: {}", temp_filename);
            return Err("Writer already exists for this file".into());
        }

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
            )?;
            (WriterVariant::Ipc(Arc::new(Mutex::new(chunked_writer))), None)
        } else {
            let file = File::create(&temp_filename)?;
            let (crc_file, crc_handle) = CrcWriter::new(file);
            let props = WriterPropertiesBuilder::build_with_generation(&settings, Some(writer_generation));
            let writer = ArrowWriter::try_new(crc_file, schema, Some(props))?;
            (WriterVariant::Parquet(Arc::new(Mutex::new(writer))), Some(crc_handle))
        };

        WRITERS.insert(temp_filename, WriterState {
            variant,
            settings,
            crc_handle,
            writer_generation,
        });

        Ok(())
    }

    pub fn write_data(filename: String, array_address: i64, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        let temp_filename = Self::temp_filename(&filename);
        log_debug!("write_data called for file: {} (temp: {})", filename, temp_filename);

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid FFI addresses for file: {}", temp_filename);
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
                log_debug!("Created RecordBatch with {} rows and {} columns", record_batch.num_rows(), record_batch.num_columns());

                if let Some(mut state) = WRITERS.get_mut(&temp_filename) {
                    match &state.variant {
                        WriterVariant::Ipc(writer_arc) => {
                            log_debug!("Writing RecordBatch to IPC staging file");
                            let mut writer = writer_arc.lock().unwrap();
                            writer.write(&record_batch)?;
                        }
                        WriterVariant::Parquet(writer_arc) => {
                            log_debug!("Writing RecordBatch to Parquet file");
                            let mut writer = writer_arc.lock().unwrap();
                            writer.write(&record_batch)?;
                        }
                    }
                    Ok(())
                } else {
                    log_error!("ERROR: No writer found for temp file: {}", temp_filename);
                    Err("Writer not found".into())
                }
            } else {
                log_error!("ERROR: Array is not a StructArray, type: {:?}", array.data_type());
                Err("Expected struct array from VectorSchemaRoot".into())
            }
        }
    }

    pub fn finalize_writer(filename: String) -> Result<Option<FinalizeResult>, Box<dyn std::error::Error>> {
        let temp_filename = Self::temp_filename(&filename);
        log_debug!("finalize_writer called for file: {} (temp: {})", filename, temp_filename);

        if let Some((_, state)) = WRITERS.remove(&temp_filename) {
            let WriterState { variant, settings, crc_handle, writer_generation } = state;
            let index_name = settings.index_name.as_deref().unwrap_or("");

            match variant {
                WriterVariant::Ipc(writer_arc) => {
                    match Arc::try_unwrap(writer_arc) {
                        Ok(mutex) => {
                            let chunked_writer = mutex.into_inner().unwrap();
                            let total_rows = chunked_writer.total_rows();
                            let schema = chunked_writer.schema.clone();
                            let (chunk_paths, chunk_row_ids) = chunked_writer.finish()?;
                            log_info!(
                                "Successfully closed sorting chunked writer for: {}, total_rows={}, chunks={}",
                                temp_filename, total_rows, chunk_paths.len()
                            );

                            let (crc32, row_id_mapping) = Self::finalize_sorted_chunks(
                                &chunk_paths, &chunk_row_ids, &filename, index_name,
                                &settings.sort_columns, &settings.reverse_sorts, &settings.nulls_first,
                                writer_generation, schema.clone(),
                            )?;

                            // Clean up sorted chunk files
                            for path in &chunk_paths {
                                let _ = std::fs::remove_file(path);
                            }

                            log_debug!("CRC32 for file {}: {:#010x}", filename, crc32);

                            let file_for_sync = File::open(&filename)?;
                            FILE_MANAGER.insert(filename.clone(), file_for_sync);

                            let file = File::open(&filename)?;
                            let reader = SerializedFileReader::new(file)?;
                            let parquet_metadata = reader.metadata().clone();

                            Ok(Some(FinalizeResult { metadata: parquet_metadata, crc32, row_id_mapping }))
                        }
                        Err(_) => {
                            log_error!("ERROR: IPC Writer still in use for temp file: {}", temp_filename);
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

                                    // Parquet variant is used for non-sorted data; just rename.
                                    std::fs::rename(&temp_filename, &filename)?;

                                    log_debug!("CRC32 for file {}: {:#010x}", filename, crc32);

                                    let file_for_sync = File::open(&filename)?;
                                    FILE_MANAGER.insert(filename.clone(), file_for_sync);

                                    let file = File::open(&filename)?;
                                    let reader = SerializedFileReader::new(file)?;
                                    let parquet_metadata = reader.metadata().clone();

                                    Ok(Some(FinalizeResult { metadata: parquet_metadata, crc32, row_id_mapping: None }))
                                }
                                Err(e) => {
                                    log_error!("ERROR: Failed to close writer for temp file: {}", temp_filename);
                                    Err(e.into())
                                }
                            }
                        }
                        Err(_) => {
                            log_error!("ERROR: Writer still in use for temp file: {}", temp_filename);
                            Err("Writer still in use".into())
                        }
                    }
                }
            }
        } else {
            log_error!("ERROR: Writer not found for temp file: {}", temp_filename);
            Err("Writer not found".into())
        }
    }

    /// Finalize pre-sorted Parquet chunks: k-way merge them into the final file.
    /// Chunks are already sorted (done eagerly during write), so no sort needed here.
    /// For single chunk, just rename. For empty, write empty Parquet.
    fn finalize_sorted_chunks(
        chunk_paths: &[String],
        chunk_row_ids: &[Vec<i64>],
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
        writer_generation: i64,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(u32, Option<Vec<i64>>), Box<dyn std::error::Error>> {
        if chunk_paths.is_empty() {
            log_info!("finalize_sorted_chunks: no chunks, writing empty Parquet file");
            let config = SETTINGS_STORE
                .get(index_name)
                .map(|r| r.clone())
                .unwrap_or_default();
            let props = WriterPropertiesBuilder::build_with_generation(&config, Some(writer_generation));
            let file = File::create(output_filename)?;
            let writer = ArrowWriter::try_new(file, schema, Some(props))?;
            writer.close()?;
            return Ok((0, None));
        }

        if chunk_paths.len() == 1 {
            // Single chunk: just rename to final output (already sorted Parquet)
            log_info!("finalize_sorted_chunks: single chunk, renaming to final output");
            std::fs::rename(&chunk_paths[0], output_filename)?;

            // Build permutation from the single chunk's row IDs
            let row_id_mapping = if !chunk_row_ids.is_empty() && !chunk_row_ids[0].is_empty() {
                let ids = &chunk_row_ids[0];
                let total = ids.len();
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

            return Ok((0, row_id_mapping));
        }

        // Multiple chunks: k-way merge
        let overall_start = std::time::Instant::now();
        log_info!(
            "finalize_sorted_chunks: merging {} pre-sorted chunks for {}",
            chunk_paths.len(), output_filename
        );

        let merge_output = merge_sorted(
            chunk_paths,
            output_filename,
            index_name,
            sort_columns,
            reverse_sorts,
            nulls_first,
        )
        .map_err(|e| -> Box<dyn std::error::Error> {
            format!("Streaming merge failed: {}", e).into()
        })?;
        let merge_duration = overall_start.elapsed();
        log_info!(
            "finalize_sorted_chunks: k-way merge complete: {} chunks merged, duration={:?}",
            chunk_paths.len(), merge_duration
        );

        // Build the flat permutation: result[original_row_id] = new_row_id
        let row_id_mapping = if !merge_output.mapping.is_empty() && !chunk_row_ids.is_empty() {
            let total = merge_output.mapping.len();
            let mut flat_mapping = vec![0i64; total];
            for i in 0..total {
                flat_mapping[i] = i as i64;
            }
            let mut pos = 0usize;
            for chunk_ids in chunk_row_ids {
                for &original_row_id in chunk_ids {
                    let orig_idx = original_row_id as usize;
                    if orig_idx < total && pos < total {
                        flat_mapping[orig_idx] = merge_output.mapping[pos];
                    }
                    pos += 1;
                }
            }
            log_info!("finalize_sorted_chunks: produced {} permutation entries for {}", flat_mapping.len(), output_filename);
            Some(flat_mapping)
        } else {
            None
        };

        log_info!(
            "finalize_sorted_chunks: DONE file={}, chunks={}, merge_duration={:?}",
            output_filename, chunk_paths.len(), merge_duration
        );
        Ok((0, row_id_mapping))
    }

    fn sort_and_rewrite_parquet(
        temp_filename: &str,
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
        writer_generation: i64,
    ) -> Result<(u32, Option<Vec<i64>>), Box<dyn std::error::Error>> {
        log_debug!(
            "sort_and_rewrite_parquet: temp={}, output={}, sort_columns={:?}, reverse_sorts={:?}, nulls_first={:?}",
            temp_filename, output_filename, sort_columns, reverse_sorts, nulls_first
        );

        let config = SETTINGS_STORE
            .get(index_name)
            .map(|r| r.clone())
            .unwrap_or_default();

        let file_size = std::fs::metadata(temp_filename)?.len();

        if file_size <= config.get_sort_in_memory_threshold_bytes() {
            Self::sort_small_file(temp_filename, output_filename, index_name, sort_columns, reverse_sorts, nulls_first, writer_generation)
        } else {
            Self::sort_large_file(temp_filename, output_filename, index_name, sort_columns, reverse_sorts, nulls_first, config.get_sort_batch_size())
        }
    }

    /// Sort and rewrite Parquet from pre-chunked IPC files.
    /// Each chunk file already contains at most sort_batch_size rows (chunked during write).
    /// - If there are no chunks (empty data), write an empty Parquet file.
    /// - If there's only 1 chunk (small data), use in-memory sort.
    /// - If there are multiple chunks, sort each chunk individually and k-way merge.
    fn sort_and_rewrite_parquet_from_chunks(
        chunk_paths: &[String],
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
        writer_generation: i64,
        settings: &NativeSettings,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(u32, Option<Vec<i64>>), Box<dyn std::error::Error>> {
        use arrow::array::Int64Array;

        if chunk_paths.is_empty() {
            log_info!("sort_and_rewrite_parquet_from_chunks: no chunks, writing empty Parquet file");
            let config = SETTINGS_STORE
                .get(index_name)
                .map(|r| r.clone())
                .unwrap_or_default();
            let props = WriterPropertiesBuilder::build_with_generation(&config, Some(writer_generation));
            let file = File::create(output_filename)?;
            let writer = ArrowWriter::try_new(file, schema, Some(props))?;
            writer.close()?;
            return Ok((0, None));
        }

        // If only 1 chunk, use the small-file (in-memory) path
        if chunk_paths.len() == 1 {
            log_info!("sort_and_rewrite_parquet_from_chunks: single chunk, using in-memory sort");
            return Self::sort_small_file(
                &chunk_paths[0], output_filename, index_name,
                sort_columns, reverse_sorts, nulls_first, writer_generation,
            );
        }

        // Multiple chunks: sort each chunk, write as Parquet, then k-way merge
        let overall_start = std::time::Instant::now();
        log_info!(
            "sort_and_rewrite_parquet_from_chunks: {} chunks, sorting each and merging",
            chunk_paths.len()
        );

        let row_id_col_idx = schema.fields().iter().position(|f| f.name() == ROW_ID_COLUMN_NAME);
        let mut sorted_chunk_paths: Vec<String> = Vec::new();
        let mut chunk_row_ids: Vec<Vec<i64>> = Vec::new();
        let mut total_rows: usize = 0;
        let mut total_sort_duration = std::time::Duration::ZERO;
        let mut total_chunk_write_duration = std::time::Duration::ZERO;
        let mut total_ipc_read_duration = std::time::Duration::ZERO;

        let output_stem = Path::new(output_filename)
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let chunk_dir = Path::new(output_filename).parent().unwrap_or_else(|| Path::new("."));

        for (chunk_idx, ipc_chunk_path) in chunk_paths.iter().enumerate() {
            // Read the IPC chunk
            let ipc_read_start = std::time::Instant::now();
            let file = File::open(ipc_chunk_path)?;
            let reader = IpcFileReader::try_new(file, None)?;
            let mut batches: Vec<RecordBatch> = Vec::new();
            for batch_result in reader {
                let batch = batch_result?;
                if batch.num_rows() > 0 {
                    batches.push(batch);
                }
            }
            total_ipc_read_duration += ipc_read_start.elapsed();

            if batches.is_empty() {
                continue;
            }

            let combined = concat_batches(&schema, &batches)?;
            total_rows += combined.num_rows();

            // Sort the chunk
            let sort_start = std::time::Instant::now();
            let sorted_batch = Self::sort_batch(&combined, sort_columns, reverse_sorts, nulls_first)?;
            total_sort_duration += sort_start.elapsed();

            // Capture row IDs
            if let Some(idx) = row_id_col_idx {
                let row_id_array = sorted_batch.column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("___row_id column must be Int64");
                let ids: Vec<i64> = (0..row_id_array.len())
                    .map(|i| row_id_array.value(i))
                    .collect();
                chunk_row_ids.push(ids);
            }

            // Write sorted chunk as Parquet
            let sorted_chunk_filename = chunk_dir
                .join(format!("temp_sort_chunk_{}_{}_{}.parquet", output_stem, chunk_idx, std::process::id()))
                .to_string_lossy()
                .to_string();

            let write_start = std::time::Instant::now();
            Self::write_final_file(&sorted_chunk_filename, index_name, &sorted_batch, schema.clone(), Some(chunk_idx as i64))?;
            total_chunk_write_duration += write_start.elapsed();

            sorted_chunk_paths.push(sorted_chunk_filename);
        }

        let chunking_duration = overall_start.elapsed();
        log_info!(
            "sort_and_rewrite_parquet_from_chunks: chunking phase complete: total_rows={}, chunks_sorted={}, \
             duration={:?} (ipc_read={:?}, sort={:?}, chunk_write={:?})",
            total_rows, sorted_chunk_paths.len(),
            chunking_duration, total_ipc_read_duration, total_sort_duration, total_chunk_write_duration
        );

        if sorted_chunk_paths.is_empty() {
            return Ok((0, None));
        }

        // K-way merge
        let merge_start = std::time::Instant::now();
        let merge_output = merge_sorted(
            &sorted_chunk_paths,
            output_filename,
            index_name,
            sort_columns,
            reverse_sorts,
            nulls_first,
        )
        .map_err(|e| -> Box<dyn std::error::Error> {
            format!("Streaming merge failed: {}", e).into()
        })?;
        let merge_duration = merge_start.elapsed();
        log_info!(
            "sort_and_rewrite_parquet_from_chunks: k-way merge complete: {} chunks merged, duration={:?}",
            sorted_chunk_paths.len(), merge_duration
        );

        // Build the flat permutation
        let row_id_mapping = if !merge_output.mapping.is_empty() && !chunk_row_ids.is_empty() {
            let total = merge_output.mapping.len();
            let mut flat_mapping = vec![0i64; total];
            for i in 0..total {
                flat_mapping[i] = i as i64;
            }
            let mut pos = 0usize;
            for chunk_ids in &chunk_row_ids {
                for &original_row_id in chunk_ids {
                    let orig_idx = original_row_id as usize;
                    if orig_idx < total && pos < total {
                        flat_mapping[orig_idx] = merge_output.mapping[pos];
                    }
                    pos += 1;
                }
            }
            log_info!("sort_and_rewrite_parquet_from_chunks: produced {} permutation entries", flat_mapping.len());
            Some(flat_mapping)
        } else {
            None
        };

        // Clean up sorted Parquet chunk files
        for path in &sorted_chunk_paths {
            let _ = std::fs::remove_file(path);
        }

        let overall_duration = overall_start.elapsed();
        log_info!(
            "sort_and_rewrite_parquet_from_chunks: DONE file={}, total_rows={}, chunks={}, total_duration={:?} \
             (chunking={:?}, merge={:?})",
            output_filename, total_rows, chunk_paths.len(), overall_duration,
            chunking_duration, merge_duration
        );
        Ok((0, row_id_mapping))
    }


    /// In-memory sort for small files: read all batches, concat, sort, rewrite row IDs, write.
    pub(crate) fn sort_small_file(
        temp_filename: &str,
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
        writer_generation: i64,
    ) -> Result<(u32, Option<Vec<i64>>), Box<dyn std::error::Error>> {
        let overall_start = std::time::Instant::now();
        log_info!(
            "sort_small_file: START file={}, sort_columns={:?}, writer_generation={}",
            temp_filename, sort_columns, writer_generation
        );

        let read_start = std::time::Instant::now();
        let file = File::open(temp_filename)?;
        let reader = IpcFileReader::try_new(file, None)?;
        let schema = reader.schema();

        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows_read: usize = 0;
        let mut batch_count: usize = 0;
        for batch_result in reader {
            let batch = batch_result?;
            if batch.num_rows() > 0 {
                total_rows_read += batch.num_rows();
                batch_count += 1;
                all_batches.push(batch);
            }
        }
        let read_duration = read_start.elapsed();
        log_info!(
            "sort_small_file: IPC read complete: {} batches, {} total rows, duration={:?}",
            batch_count, total_rows_read, read_duration
        );

        if all_batches.is_empty() {
            log_info!("No data in temp file: {}", temp_filename);
            let props = WriterPropertiesBuilder::build_with_generation(
                &SETTINGS_STORE.get(index_name).map(|r| r.clone()).unwrap_or_default(),
                Some(writer_generation),
            );
            let file = File::create(output_filename)?;
            let writer = ArrowWriter::try_new(file, schema, Some(props))?;
            writer.close()?;
            return Ok((0, None));
        }

        let concat_start = std::time::Instant::now();
        let combined_batch = concat_batches(&schema, &all_batches)?;
        let concat_duration = concat_start.elapsed();
        log_info!(
            "sort_small_file: concat complete: {} rows, duration={:?}",
            combined_batch.num_rows(), concat_duration
        );

        let sort_start = std::time::Instant::now();
        let sorted_batch = Self::sort_batch(&combined_batch, sort_columns, reverse_sorts, nulls_first)?;
        let sort_duration = sort_start.elapsed();
        log_info!("sort_small_file: sort complete: {} rows, duration={:?}", sorted_batch.num_rows(), sort_duration);

        let rewrite_start = std::time::Instant::now();
        let (final_batch, permutation) = Self::rewrite_row_ids(&sorted_batch, &schema)?;
        let rewrite_duration = rewrite_start.elapsed();
        log_info!("sort_small_file: rewrite_row_ids complete: duration={:?}", rewrite_duration);

        let write_start = std::time::Instant::now();
        let crc32 = Self::write_final_file(output_filename, index_name, &final_batch, schema, Some(writer_generation))?;
        let write_duration = write_start.elapsed();
        log_info!("sort_small_file: Parquet write complete: duration={:?}", write_duration);

        let row_id_mapping = if !permutation.is_empty() {
            log_info!("sort_small_file: produced {} permutation entries for {}", permutation.len(), output_filename);
            Some(permutation)
        } else {
            None
        };

        let overall_duration = overall_start.elapsed();
        log_info!(
            "sort_small_file: DONE file={}, total_rows={}, total_duration={:?} (read={:?}, concat={:?}, sort={:?}, rewrite_ids={:?}, write={:?})",
            output_filename, final_batch.num_rows(), overall_duration,
            read_duration, concat_duration, sort_duration, rewrite_duration, write_duration
        );
        Ok((crc32, row_id_mapping))
    }

    pub(crate) fn sort_large_file(
        temp_filename: &str,
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
        batch_size: usize,
    ) -> Result<(u32, Option<Vec<i64>>), Box<dyn std::error::Error>> {
        use arrow::array::Int64Array;

        let overall_start = std::time::Instant::now();
        let file_size = std::fs::metadata(temp_filename).map(|m| m.len()).unwrap_or(0);
        log_info!(
            "sort_large_file: START file={}, file_size={} bytes, batch_size={}, sort_columns={:?}",
            temp_filename, file_size, batch_size, sort_columns
        );

        let read_start = std::time::Instant::now();
        let file = File::open(temp_filename)?;
        let reader = IpcFileReader::try_new(file, None)?;
        let schema = reader.schema();

        let mut chunk_paths: Vec<String> = Vec::new();
        let mut batch_count: i64 = 0;
        let mut total_rows_read: usize = 0;
        let mut total_ipc_batches: usize = 0;
        let output_stem = Path::new(output_filename)
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let chunk_dir = Path::new(output_filename).parent().unwrap_or_else(|| Path::new("."));

        // Capture the ___row_id values from each sorted chunk (in sorted order).
        // After merge, we use these to map sorted-positions back to original row IDs.
        let mut chunk_row_ids: Vec<Vec<i64>> = Vec::new();

        // Find the ___row_id column index in the schema
        let row_id_col_idx = schema.fields().iter().position(|f| f.name() == ROW_ID_COLUMN_NAME);

        let mut total_sort_duration = std::time::Duration::ZERO;
        let mut total_chunk_write_duration = std::time::Duration::ZERO;
        let mut total_ipc_read_duration = std::time::Duration::ZERO;
        let mut total_rowid_capture_duration = std::time::Duration::ZERO;

        let mut reader_iter = reader.into_iter();
        loop {
            let ipc_read_start = std::time::Instant::now();
            let batch_opt = reader_iter.next();
            total_ipc_read_duration += ipc_read_start.elapsed();

            let batch = match batch_opt {
                Some(Ok(b)) => b,
                Some(Err(e)) => return Err(e.into()),
                None => break,
            };
            if batch.num_rows() == 0 {
                continue;
            }
            total_ipc_batches += 1;
            total_rows_read += batch.num_rows();

            // IpcFileReader returns batches at whatever size they were written.
            // Slice into batch_size chunks to bound memory during sort.
            let mut offset = 0;
            while offset < batch.num_rows() {
                let len = std::cmp::min(batch_size, batch.num_rows() - offset);
                let slice = batch.slice(offset, len);
                offset += len;

                let sort_start = std::time::Instant::now();
                let sorted_batch = Self::sort_batch(&slice, sort_columns, reverse_sorts, nulls_first)?;
                total_sort_duration += sort_start.elapsed();

                // Capture ___row_id values from the sorted batch (these are the original row IDs
                // in the order they appear after per-chunk sort)
                if let Some(idx) = row_id_col_idx {
                    let rowid_start = std::time::Instant::now();
                    let row_id_array = sorted_batch.column(idx)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("___row_id column must be Int64");
                    let ids: Vec<i64> = (0..row_id_array.len())
                        .map(|i| row_id_array.value(i))
                        .collect();
                    chunk_row_ids.push(ids);
                    total_rowid_capture_duration += rowid_start.elapsed();
                }

                let chunk_filename = chunk_dir
                    .join(format!("temp_sort_chunk_{}_{}_{}.parquet", output_stem, batch_count, std::process::id()))
                    .to_string_lossy()
                    .to_string();

                let write_start = std::time::Instant::now();
                // Write chunk with batch_count as writer_generation so merge_sorted can track it
                Self::write_final_file(&chunk_filename, index_name, &sorted_batch, schema.clone(), Some(batch_count))?;
                total_chunk_write_duration += write_start.elapsed();

                chunk_paths.push(chunk_filename);
                batch_count += 1;
            }
        }
        let chunking_duration = read_start.elapsed();
        log_info!(
            "sort_large_file: chunking phase complete: ipc_batches_read={}, total_rows={}, chunks_created={}, \
             chunking_duration={:?} (ipc_read={:?}, sort={:?}, rowid_capture={:?}, chunk_write={:?})",
            total_ipc_batches, total_rows_read, batch_count,
            chunking_duration, total_ipc_read_duration, total_sort_duration,
            total_rowid_capture_duration, total_chunk_write_duration
        );

        if chunk_paths.is_empty() {
            log_info!("sort_large_file: no data to sort in file: {}", temp_filename);
            return Ok((0, None));
        }

        let merge_start = std::time::Instant::now();
        let merge_output = merge_sorted(
            &chunk_paths,
            output_filename,
            index_name,
            sort_columns,
            reverse_sorts,
            nulls_first,
        )
        .map_err(|e| -> Box<dyn std::error::Error> {
            format!("Streaming merge failed: {}", e).into()
        })?;
        let merge_duration = merge_start.elapsed();
        log_info!(
            "sort_large_file: k-way merge complete: {} chunks merged, duration={:?}",
            batch_count, merge_duration
        );

        // Build the flat permutation: result[original_row_id] = new_row_id
        let permutation_start = std::time::Instant::now();
        let row_id_mapping = if !merge_output.mapping.is_empty() && !chunk_row_ids.is_empty() {
            let total_rows = merge_output.mapping.len();
            let mut flat_mapping = vec![0i64; total_rows];
            for i in 0..total_rows {
                flat_mapping[i] = i as i64;
            }

            let mut pos = 0usize;
            for chunk_ids in &chunk_row_ids {
                for &original_row_id in chunk_ids {
                    let orig_idx = original_row_id as usize;
                    if orig_idx < total_rows && pos < total_rows {
                        flat_mapping[orig_idx] = merge_output.mapping[pos];
                    }
                    pos += 1;
                }
            }

            log_info!("sort_large_file: produced {} permutation entries for {}", flat_mapping.len(), output_filename);
            Some(flat_mapping)
        } else {
            None
        };
        let permutation_duration = permutation_start.elapsed();

        // Clean up temp chunk files
        for path in &chunk_paths {
            let _ = std::fs::remove_file(path);
        }

        let overall_duration = overall_start.elapsed();
        log_info!(
            "sort_large_file: DONE file={}, total_rows={}, chunks={}, total_duration={:?} \
             (chunking={:?}, merge={:?}, permutation_build={:?})",
            output_filename, total_rows_read, batch_count, overall_duration,
            chunking_duration, merge_duration, permutation_duration
        );
        Ok((0, row_id_mapping))
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
                let col_index = batch.schema().index_of(col_name)
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

    /// If a ___row_id column exists, rewrite it with sequential values 0..N.
    /// Also returns the sort permutation as a flat mapping Vec<i64> where mapping[old_row_id] = new_row_id.
    fn rewrite_row_ids(
        batch: &RecordBatch,
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> Result<(RecordBatch, Vec<i64>), Box<dyn std::error::Error>> {
        use arrow::array::Int64Array;

        // Log all column names present in the schema
        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        log_info!("[RUST] rewrite_row_ids: schema has {} columns: {:?}", column_names.len(), column_names);
        log_info!("[RUST] rewrite_row_ids: looking for ROW_ID_COLUMN_NAME = '{}'", ROW_ID_COLUMN_NAME);
        log_info!("[RUST] rewrite_row_ids: batch num_rows = {}, num_columns = {}", batch.num_rows(), batch.num_columns());

        if let Some(row_id_idx) = schema.fields().iter().position(|f| f.name() == ROW_ID_COLUMN_NAME) {
            log_info!("rewrite_row_ids: FOUND ___row_id at column index {}", row_id_idx);
            log_debug!("rewrite_row_ids: rewriting ___row_id with sequential values 0..{}", batch.num_rows());

            // Capture the sort permutation as flat mapping: mapping[old_row_id] = new_row_id
            let old_row_ids = batch.column(row_id_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("___row_id column is not Int64")?;

            let num_rows = batch.num_rows();
            let mut mapping = vec![0i64; num_rows];
            // Initialize with identity
            for i in 0..num_rows {
                mapping[i] = i as i64;
            }
            for new_pos in 0..num_rows {
                let old_row_id = old_row_ids.value(new_pos) as usize;
                if old_row_id < num_rows {
                    mapping[old_row_id] = new_pos as i64;
                }
            }

            log_debug!("rewrite_row_ids: captured flat mapping with {} entries", mapping.len());

            let sequential_ids = Int64Array::from_iter_values(
                (0..batch.num_rows() as u64).map(|x| x as i64)
            );
            let mut new_columns = batch.columns().to_vec();
            new_columns[row_id_idx] = Arc::new(sequential_ids);
            log_info!("[RUST] rewrite_row_ids: successfully rewrote ___row_id with sequential 0..{}", batch.num_rows());
            Ok((RecordBatch::try_new(schema.clone(), new_columns)?, mapping))
        } else {
            log_info!("[RUST] rewrite_row_ids: ___row_id column NOT FOUND in schema — returning empty permutation");
            log_info!("[RUST] rewrite_row_ids: available columns are: {:?}", column_names);
            Ok((batch.clone(), Vec::new()))
        }
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
        let props = WriterPropertiesBuilder::build_with_generation(&config, writer_generation);
        let file = File::create(output_filename)?;
        let (crc_file, crc_handle) = CrcWriter::new(file);
        let mut writer = ArrowWriter::try_new(crc_file, schema, Some(props))?;
        writer.write(batch)?;
        writer.close()?;
        let crc32 = crc_handle.crc32();
        log_debug!("Successfully wrote final file: {} (crc32={:#010x})", output_filename, crc32);
        Ok(crc32)
    }

    pub fn sync_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!("sync_to_disk called for file: {}", filename);

        if let Some(file) = FILE_MANAGER.get_mut(&filename) {
            file.sync_all()?;
            log_debug!("Successfully fsynced file: {}", filename);
            drop(file);
            FILE_MANAGER.remove(&filename);
            Ok(())
        } else {
            log_error!("ERROR: File not found for fsync: {}", filename);
            Err("File not found".into())
        }
    }

    pub fn get_filtered_writer_memory_usage(path_prefix: String) -> Result<usize, Box<dyn std::error::Error>> {
        let mut total_memory = 0;
        for entry in WRITERS.iter() {
            if entry.key().starts_with(&path_prefix) {
                match &entry.value().variant {
                    WriterVariant::Parquet(writer_arc) => {
                        if let Ok(writer) = writer_arc.lock() {
                            total_memory += writer.memory_size();
                        }
                    }
                    WriterVariant::Ipc(writer_arc) => {
                        if let Ok(writer) = writer_arc.lock() {
                            total_memory += writer.memory_size();
                        }
                    }
                }
            }
        }
        Ok(total_memory)
    }

    pub fn get_file_metadata(filename: String) -> Result<parquet::file::metadata::FileMetaData, Box<dyn std::error::Error>> {
        let file = File::open(&filename)?;
        let reader = SerializedFileReader::new(file)?;
        let file_metadata = reader.metadata().file_metadata().clone();
        log_debug!("Metadata for {}: version={}, num_rows={}", filename, file_metadata.version(), file_metadata.num_rows());
        Ok(file_metadata)
    }
}
