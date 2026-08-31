/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Forward-only Parquet doc-values cursor over DataFusion I/O and Arrow decoding.
//!
//! DataFusion supplies metadata, page indexes, and remote object-store/cache I/O.
//! Local shard files use Arrow's synchronous `ChunkReader` directly. Arrow owns
//! page traversal, skipping, decompression, definition-level expansion, and batch
//! construction. This module selects the byte source and exposes the FFM lifecycle.
//!
//! v1 scope: numeric fixed-width leaf columns only (INT32/INT64/FLOAT/DOUBLE).
//! Values are exported to Java zero-copy by borrowing the Arrow buffers.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Weak};

use arrow::array::{Array, ArrayRef};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::execution::cache::DefaultFilesMetadataCache;
use datafusion_datasource::PartitionedFile;
use native_bridge_common::ffm_safe;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use parquet::arrow::ProjectionMask;
use parquet::basic::Type as PhysicalType;
use tokio::runtime::{Builder, Runtime};

use crate::cache::metadata_cache::MutexFileMetadataCache;
use crate::cache::page_index::load_scoped_page_index_cols;
use crate::forward_reader::{ParquetForwardBatchReader, ParquetForwardBatchReaderFactory};
use crate::indexed_table::parquet_bridge::{
    load_parquet_metadata_with_meta, CachedMetadataReaderFactory, ReadIoStats,
};
use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;

/// Hard ceiling for the adaptive decode window. Mirrored by
/// `DataFusionColumnReader.MAX_BATCH_ROWS` and the upper bound of the
/// `parquet.docvalues.initial_batch_size` setting on the Java side; keep the
/// three in sync.
const MAX_BATCH_SIZE: usize = 8192;
const RC_OK: i64 = 0;
const RC_EOF: i64 = 2;

static NEXT_HANDLE: AtomicI64 = AtomicI64::new(0);
static CURSORS: Lazy<DashMap<i64, Arc<Mutex<DocValuesCursor>>>> = Lazy::new(DashMap::new);

/// File path to the DataFusion shard store that owns it. Weak references avoid
/// extending a shard view's lifetime.
static STORES: Lazy<DashMap<String, Weak<dyn ObjectStore>>> = Lazy::new(DashMap::new);

/// The node's DataFusion file-metadata cache, registered with the global runtime.
static METADATA_CACHE: RwLock<Option<Weak<dyn FileMetadataCache>>> = RwLock::new(None);

static FALLBACK_METADATA_CACHE: Lazy<Arc<MutexFileMetadataCache>> = Lazy::new(|| {
    Arc::new(MutexFileMetadataCache::new(DefaultFilesMetadataCache::new(
        64 * 1024 * 1024,
    )))
});

static FALLBACK_RUNTIME: Lazy<Arc<Runtime>> = Lazy::new(|| {
    Arc::new(
        Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("df-docvalues-io")
            .enable_all()
            .build()
            .expect("failed to build doc-values fallback runtime"),
    )
});

pub fn register_metadata_cache(cache: Arc<dyn FileMetadataCache>) {
    *METADATA_CACHE.write() = Some(Arc::downgrade(&cache));
}

pub fn register_store(object_metas: &[ObjectMeta], store: Arc<dyn ObjectStore>) {
    STORES.retain(|_, weak| weak.strong_count() > 0);
    let weak = Arc::downgrade(&store);
    for meta in object_metas {
        STORES.insert(normalize_path(meta.location.as_ref()), Weak::clone(&weak));
    }
}

fn normalize_path(path: &str) -> String {
    path.strip_prefix("file://")
        .unwrap_or(path)
        .trim_start_matches('/')
        .to_string()
}

fn registered_store(filename: &str, location: &ObjectPath) -> Option<Arc<dyn ObjectStore>> {
    for key in [normalize_path(filename), normalize_path(location.as_ref())] {
        if let Some(entry) = STORES.get(&key) {
            if let Some(store) = entry.value().upgrade() {
                return Some(store);
            }
            drop(entry);
            STORES.remove(&key);
        }
    }
    None
}

fn metadata_cache() -> Arc<dyn FileMetadataCache> {
    METADATA_CACHE
        .read()
        .as_ref()
        .and_then(Weak::upgrade)
        .unwrap_or_else(|| Arc::clone(&FALLBACK_METADATA_CACHE) as Arc<dyn FileMetadataCache>)
}

fn io_runtime() -> Arc<Runtime> {
    crate::ffm::try_get_rt_manager()
        .map(|manager| Arc::clone(&manager.io_runtime))
        .unwrap_or_else(|| Arc::clone(&FALLBACK_RUNTIME))
}

struct DocValuesCursor {
    reader: ParquetForwardBatchReader,
    /// Retained to reopen the reader on reset without re-resolving metadata,
    /// page index, or store registrations.
    factory: ParquetForwardBatchReaderFactory,
    row_count: i64,
    initial_batch_size: usize,
    batch_size: usize,
    has_decoded_batch: bool,
    /// Keeps the most recently borrowed-out batch's buffers alive. Java reads
    /// the exported pointers until its next call on this cursor, so the array
    /// must outlive exactly one call cycle; each export replaces the previous.
    borrowed_batch: Option<ArrayRef>,
    /// Retained so tests can assert on the number of object-store range reads;
    /// also handed to the reader factory to attribute I/O.
    stats: Arc<ReadIoStats>,
}

impl DocValuesCursor {
    async fn open(
        filename: &str,
        column: &str,
        batch_size: usize,
        store_override: Option<Arc<dyn ObjectStore>>,
        location_override: Option<ObjectPath>,
        runtime: Arc<Runtime>,
    ) -> Result<Self, String> {
        let location = location_override.unwrap_or_else(|| ObjectPath::from(filename));
        let store = store_override
            .or_else(|| registered_store(filename, &location))
            .unwrap_or_else(|| Arc::new(LocalFileSystem::new())); // local filesystem

        let object_meta = store
            .head(&location)
            .await
            .map_err(|e| format!("df_docvalues: object-store head {location}: {e}"))?; // stat the file (size, etc.)
        let (_arrow_schema, file_size, footer) = load_parquet_metadata_with_meta(
            Arc::clone(&store),
            &location,
            object_meta,
            metadata_cache(),
        )
        .await?;

        let schema = footer.file_metadata().schema_descr(); // list of all leaf columns
        // Collect every candidate rather than taking the first: two leaves in different groups can
        // share a name, and a group root can cover several leaves. Serving whichever happened to be
        // first would silently read the wrong column, so ambiguity is an error.
        //
        // Unreachable for files this plugin writes today: every mapped field produces a flat Arrow
        // type, so each column is a top-level leaf whose path equals its name. The guard is here for
        // files written with a nested schema - by a future mapping type, or by another writer - where
        // first-match-wins would return wrong values with no error. The tests build such a schema
        // directly to keep this covered.
        let matches = (0..schema.num_columns())
            .filter(|&idx| {
                let descriptor = schema.column(idx);
                descriptor.name() == column   // match by leaf name
                    || descriptor.path().string() == column
                    || descriptor
                        .path()
                        .parts()
                        .first()
                        .is_some_and(|root| root == column)
            })
            .collect::<Vec<_>>();
        let leaf_idx = match matches.as_slice() {
            [only] => *only,
            [] => {
                return Err(format!(
                    "df_docvalues: column '{column}' not found in {filename}"
                ))
            }
            several => {
                let paths = several
                    .iter()
                    .map(|&idx| schema.column(idx).path().string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(format!(
                    "df_docvalues: column '{column}' is ambiguous in {filename}; it matches {paths}"
                ));
            }
        };
        let descriptor = schema.column(leaf_idx);
        let physical_type = descriptor.physical_type();
        if !matches!(
            physical_type,
            PhysicalType::INT32 | PhysicalType::INT64 | PhysicalType::FLOAT | PhysicalType::DOUBLE
        ) {
            return Err(format!(
                "df_docvalues: unsupported physical type {physical_type:?} for column '{column}'"
            ));
        }

        let metadata =
            load_scoped_page_index_cols(&store, &location, &footer, &[leaf_idx], &[leaf_idx])
                .await
                .ok_or_else(|| {
                    format!(
                        "df_docvalues: no page index available for column '{column}' in {filename}"
                    )
                })?; // OffsetIndex + ColumnIndex scoped to just this column
        let stats = Arc::new(ReadIoStats::default());
        let projection =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [leaf_idx]);
        let batch_size = batch_size.clamp(1, MAX_BATCH_SIZE);
        let reader_factory: Arc<dyn ParquetFileReaderFactory> = Arc::new(
            CachedMetadataReaderFactory::new(store, Arc::clone(&metadata), Arc::clone(&stats)),
        );
        let factory = ParquetForwardBatchReaderFactory::new(
            reader_factory,
            PartitionedFile::new(location.to_string(), file_size),
            metadata,
            projection,
            batch_size,
            Arc::clone(&runtime),
        )
        .with_local_file_if_exists(filename);
        let reader = factory
            .open()
            .map_err(|e| format!("df_docvalues: build forward reader for {filename}: {e}"))?;
        let row_count = reader.row_count() as i64;

        Ok(Self {
            reader,
            factory,
            row_count,
            initial_batch_size: batch_size,
            batch_size,
            has_decoded_batch: false,
            borrowed_batch: None,
            stats,
        })
    }

    /// Chooses the decode window for `target_row` and returns `(window, rows)`:
    /// `window` is the adaptive size to remember for next time, `rows` is how much
    /// to decode now, never past the end of `target_row`'s page.
    fn planned_batch(&self, target_row: i64) -> Result<(usize, usize), String> {
        if target_row < 0 || target_row >= self.row_count {
            return Err(format!("df_docvalues: row {target_row} out of range"));
        }
        let target_row = target_row as usize;
        let position = self.reader.position();
        // Adaptive decode window: geometric growth with multiplicative backoff. A forward
        // skip no larger than the current window still indicates page-scale dense access
        // (e.g. a 10-30% selective filter that touches every page), so the window doubles.
        // A jump beyond the window halves it (rather than resetting), so mixed access
        // patterns don't pay a full re-ramp after every jump.
        let dense = self.has_decoded_batch
            && target_row >= position
            && target_row - position <= self.batch_size; //the forward jump is no bigger than one current window.
        let window = if dense {
            self.batch_size.saturating_mul(2).min(MAX_BATCH_SIZE) //  double, cap at 8192
        } else if self.has_decoded_batch {
            (self.batch_size / 2).max(self.initial_batch_size) // DECAY: halve, floor at initial
        } else {
            self.batch_size // CALL: use current (=initial)
        };
        let page_rows = self
            .reader
            .page_row_count(target_row) // how many rows total are in the page that contains row
            .map_err(|e| format!("df_docvalues: find page at row {target_row}: {e}"))?;
        let page_remaining = self
            .reader
            .rows_remaining_in_page(target_row) //from row target_row to the end of that page
            .map_err(|e| format!("df_docvalues: find page end at row {target_row}: {e}"))?;
        let window = window.min(page_rows); // never plan a window bigger than a whole page
        Ok((window, window.min(page_remaining))) // return window (next time batch size saved) , rows remaining
    }

    fn next_batch(&mut self, target_row: i64) -> Result<RecordBatch, String> {
        // `window` is the size to remember for next time; `rows` is what we
        // actually decode now.
        let (window, rows) = self.planned_batch(target_row)?;
        let batch = self
            .reader
            .read_batch_at(target_row as usize, rows)
            .map_err(|e| format!("df_docvalues: read row {target_row}: {e}"))?
            .ok_or_else(|| format!("df_docvalues: reader exhausted before row {target_row}"))?;
        self.batch_size = window;
        self.has_decoded_batch = true;
        Ok(batch)
    }
}

/// Shared entry-point prologue: resolves a live cursor handle.
fn cursor_for(handle: i64, fn_name: &str) -> Result<Arc<Mutex<DocValuesCursor>>, String> {
    CURSORS
        .get(&handle)
        .map(|entry| Arc::clone(entry.value()))
        .ok_or_else(|| format!("{fn_name}: unknown handle {handle}"))
}

/// Validates `target_row`, returning `true` when the cursor is exactly at end-of-column.
fn at_eof(cursor: &DocValuesCursor, target_row: i64, fn_name: &str) -> Result<bool, String> {
    if target_row == cursor.row_count {
        return Ok(true);
    }
    if target_row < 0 || target_row > cursor.row_count {
        return Err(format!("{fn_name}: row {target_row} out of range"));
    }
    Ok(false)
}

/// Writes `value` through a nullable out-parameter.
unsafe fn write_out(ptr: *mut i64, value: i64) {
    if !ptr.is_null() {
        *ptr = value;
    }
}

/// Java-side interpretation of a borrowed values buffer. Mirrors the
/// `KIND_*` constants in `DecodedBatch.java`; keep in sync.
const BORROW_KIND_LONG: i64 = 1; // i64 / u64 raw bits, 8 bytes per row
const BORROW_KIND_INT: i64 = 2; // i32 / date32, sign-extended, 4 bytes per row
const BORROW_KIND_UINT_BITS: i64 = 3; // u32 raw bits, zero-extended, 4 bytes per row
const BORROW_KIND_SHORT: i64 = 4; // i16, sign-extended, 2 bytes per row
const BORROW_KIND_USHORT: i64 = 5; // u16, zero-extended, 2 bytes per row
const BORROW_KIND_BYTE: i64 = 6; // i8, sign-extended, 1 byte per row
const BORROW_KIND_UBYTE: i64 = 7; // u8, zero-extended, 1 byte per row
const BORROW_KIND_DOUBLE: i64 = 8; // f64 raw bits; Java re-encodes to a Lucene sortable long, 8 bytes per row
const BORROW_KIND_FLOAT: i64 = 9; // f32 raw bits; Java re-encodes to a sign-extended sortable int, 4 bytes per row

struct BorrowedBuffers {
    values_addr: usize,
    validity_addr: usize,
    validity_bit_offset: usize,
    kind: i64,
}

/// Exposes an Arrow primitive array's buffers for zero-copy reads from Java.
///
/// Java reads the Arrow values buffer and validity bitmap in place, widening per
/// accessed row, so a sparse reader pays O(rows accessed) rather than O(rows
/// served) and avoids a per-batch copy or narrow-integer cast. Keyed on the
/// Arrow type (the decode output), not the Parquet physical type: Parquet INT32
/// arrives as Int8/Int16/Int32/Date32 depending on the logical type.
///
/// TODO: extend to boolean (bit-packed values buffer + a value bit offset,
/// mirroring the validity bitmap), then binary/variable-width columns. Until
/// then non-numeric arrays return `None` and the caller rejects them.
///
/// TODO: `Float16` (OpenSearch `half_float`) has no arm, so it is rejected both
/// here and by the physical-type guard in `open`. Adding it needs a new borrow
/// kind whose Java side re-encodes to a sortable short, not a sortable int.
fn borrowable_buffers(array: &dyn Array) -> Option<BorrowedBuffers> {
    use arrow::datatypes::DataType as DT;
    let (kind, width) = match array.data_type() {
        DT::Int64 | DT::UInt64 | DT::Date64 | DT::Timestamp(_, _) => (BORROW_KIND_LONG, 8usize),
        DT::Float64 => (BORROW_KIND_DOUBLE, 8),
        DT::Int32 | DT::Date32 | DT::Time32(_) => (BORROW_KIND_INT, 4),
        DT::UInt32 => (BORROW_KIND_UINT_BITS, 4),
        DT::Float32 => (BORROW_KIND_FLOAT, 4),
        DT::Int16 => (BORROW_KIND_SHORT, 2),
        DT::UInt16 => (BORROW_KIND_USHORT, 2),
        DT::Int8 => (BORROW_KIND_BYTE, 1),
        DT::UInt8 => (BORROW_KIND_UBYTE, 1),
        _ => return None,
    };
    debug_assert_eq!(array.data_type().primitive_width(), Some(width));
    let data = array.to_data();
    let buffer = data.buffers().first()?; // buffer 0 holds the values for a primitive array
    // Fold the array's logical offset into the pointer so it addresses row 0.
    let values_addr = buffer.as_ptr() as usize + data.offset() * width;
    let (validity_addr, validity_bit_offset) = match data.nulls() {
        None => (0, 0),
        Some(nulls) => (nulls.buffer().as_ptr() as usize, nulls.offset()),
    };
    Some(BorrowedBuffers {
        values_addr,
        validity_addr, // start of the null-bitmap buffer; the bit offset is applied separately
        validity_bit_offset, // bit index of row 0 within that bitmap (bitmaps are bit-addressable)
        kind,
    })
}

fn open(filename: &str, column: &str, initial_batch_size: usize) -> Result<i64, String> {
    let runtime = io_runtime(); // the shared Tokio runtime
    let cursor = runtime.block_on(DocValuesCursor::open(
        filename,
        column,
        initial_batch_size,
        None,
        None,
        Arc::clone(&runtime),
    ))?;
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::SeqCst); // allocate a fresh handle number
    CURSORS.insert(handle, Arc::new(Mutex::new(cursor))); // register cursor in the global map
    Ok(handle)
}

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null string pointer".to_string());
    }
    if len < 0 {
        return Err(format!("negative string length: {len}"));
    }
    std::str::from_utf8(std::slice::from_raw_parts(ptr, len as usize))
        .map_err(|e| format!("invalid UTF-8: {e}"))
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_df_open_iter(
    file_ptr: *const u8, // filepath (location in machine) as raw bytes + length to share across FFM
    file_len: i64,
    column_ptr: *const u8,
    column_len: i64,
    initial_batch_size: i64,
) -> i64 {
    let filename =
        str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_df_open_iter file: {e}"))?; // reconstruct &str from raw bytes
    let column = str_from_raw(column_ptr, column_len)
        .map_err(|e| format!("parquet_df_open_iter column: {e}"))?;
    if initial_batch_size <= 0 || initial_batch_size > MAX_BATCH_SIZE as i64 {
        return Err(format!(
            "parquet_df_open_iter: initial batch size {initial_batch_size} outside 1..={MAX_BATCH_SIZE}"
        ));
    }
    open(filename, column, initial_batch_size as usize)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_df_close_iter(handle: i64) -> i64 {
    CURSORS.remove(&handle);
    Ok(RC_OK)
}

/// Rewinds a cursor to row zero. The forward reader only moves forward, so
/// serving a target below the current position (a backward seek) requires
/// rebuilding the inner Parquet decoder; the factory reopens it while reusing
/// the already-resolved metadata, page index, and store registration.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_df_reset_iter(handle: i64) -> i64 {
    let cursor = cursor_for(handle, "parquet_df_reset_iter")?;
    let mut cursor = cursor.lock();
    cursor.reader = cursor
        .factory
        .open()
        .map_err(|e| format!("parquet_df_reset_iter: {e}"))?;
    cursor.batch_size = cursor.initial_batch_size;
    cursor.has_decoded_batch = false;
    cursor.borrowed_batch = None;
    Ok(RC_OK)
}

/// Advance to `target_row`, skip intermediate pages through Arrow's retained
/// reader, and hand Java the decoded Arrow batch's buffers zero-copy.
#[ffm_safe]
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn parquet_df_next_batch(
    handle: i64,
    target_row: i64,
    out_first_row: *mut i64,
    out_last_row: *mut i64,
    out_values_addr: *mut i64,
    out_validity_addr: *mut i64,
    out_validity_bit_offset: *mut i64,
    out_value_kind: *mut i64,
) -> i64 {
    const FN: &str = "parquet_df_next_batch";
    let cursor = cursor_for(handle, FN)?;
    let mut cursor = cursor.lock();
    if at_eof(&cursor, target_row, FN)? {
        return Ok(RC_EOF); // target is past the last row (e.g. a scan running off the end)
    }

    let batch = cursor.next_batch(target_row)?;
    let rows = batch.num_rows();
    if rows == 0 || rows > MAX_BATCH_SIZE {
        return Err(format!("{FN}: Arrow returned {rows} rows"));
    }
    write_out(out_first_row, target_row);
    write_out(out_last_row, target_row + rows as i64 - 1); // inclusive last row of this batch

    // Zero-copy: hand Java the Arrow buffers directly. The array is retained on
    // the cursor so the pointers stay valid until Java's next call on this
    // handle (Java swaps its resident batch before that call).
    let array = batch.column(0); // single projected column
    let borrow = borrowable_buffers(array.as_ref()).ok_or_else(|| {
        format!(
            "{FN}: unsupported non-numeric array {}",
            array.data_type()
        )
    })?;
    write_out(out_values_addr, borrow.values_addr as i64);
    write_out(out_validity_addr, borrow.validity_addr as i64);
    write_out(out_validity_bit_offset, borrow.validity_bit_offset as i64);
    write_out(out_value_kind, borrow.kind);
    cursor.borrowed_batch = Some(Arc::clone(array)); // keep the buffers alive for one call cycle
    Ok(RC_OK)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use tempfile::NamedTempFile;

    use super::*;

    const ROWS_PER_PAGE: usize = 64;

    fn parquet_fixture_with_page_rows(row_groups: usize, rows_per_page: usize) -> Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(rows_per_page)
            .set_write_batch_size(rows_per_page)
            .set_max_row_group_row_count(Some(rows_per_page * 8))
            .build();
        let mut writer =
            ArrowWriter::try_new(Cursor::new(Vec::new()), Arc::clone(&schema), Some(props))
                .unwrap();
        let row_count = row_groups * rows_per_page * 8;
        let values = (0..row_count).map(|value| value as i64).collect::<Vec<_>>();
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef])
                .unwrap();
        writer.write(&batch).unwrap();
        Bytes::from(writer.into_inner().unwrap().into_inner())
    }

    fn parquet_fixture_with_all_null_page(rows_per_page: usize) -> Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(rows_per_page)
            .set_write_batch_size(rows_per_page)
            .set_max_row_group_row_count(Some(rows_per_page * 4))
            .build();
        let mut writer =
            ArrowWriter::try_new(Cursor::new(Vec::new()), Arc::clone(&schema), Some(props))
                .unwrap();
        let values = (0..rows_per_page * 2)
            .map(|row| (row >= rows_per_page).then_some(row as i64))
            .collect::<Vec<_>>();
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef])
                .unwrap();
        writer.write(&batch).unwrap();
        Bytes::from(writer.into_inner().unwrap().into_inner())
    }

    /// Two leaves under one struct, so the group root matches both by first path part.
    fn parquet_fixture_with_two_leaves_under_one_group() -> Bytes {
        let children = vec![
            Arc::new(Field::new("left", DataType::Int64, false)),
            Arc::new(Field::new("right", DataType::Int64, false)),
        ];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group",
            DataType::Struct(children.clone().into()),
            false,
        )]));
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut writer =
            ArrowWriter::try_new(Cursor::new(Vec::new()), Arc::clone(&schema), Some(props))
                .unwrap();
        let left = Arc::new(Int64Array::from((0..8_i64).collect::<Vec<_>>())) as ArrayRef;
        let right = Arc::new(Int64Array::from((8..16_i64).collect::<Vec<_>>())) as ArrayRef;
        let group = arrow::array::StructArray::new(children.into(), vec![left, right], None);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(group) as ArrayRef]).unwrap();
        writer.write(&batch).unwrap();
        Bytes::from(writer.into_inner().unwrap().into_inner())
    }

    fn open_named_column(bytes: Bytes, column: &str) -> Result<DocValuesCursor, String> {
        let runtime = Arc::new(Builder::new_current_thread().enable_all().build().unwrap());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = ObjectPath::from(format!(
            "doc-values-cursor-{}.parquet",
            NEXT_HANDLE.fetch_add(1, Ordering::Relaxed)
        ));
        runtime
            .block_on(store.put(&location, bytes.into()))
            .unwrap();
        runtime.block_on(DocValuesCursor::open(
            location.as_ref(),
            column,
            8,
            Some(store),
            Some(location.clone()),
            Arc::clone(&runtime),
        ))
    }

    #[test]
    fn an_ambiguous_column_name_is_rejected_rather_than_guessed() {
        let bytes = parquet_fixture_with_two_leaves_under_one_group();

        // "group" is the root of two leaves. Binding it to whichever came first would silently
        // serve doc values for the wrong column.
        let error = open_named_column(bytes.clone(), "group")
            .err()
            .expect("an ambiguous column must not open");
        assert!(error.contains("ambiguous"), "{error}");
        assert!(error.contains("group.left") && error.contains("group.right"), "{error}");

        // An unambiguous leaf under the same group still resolves.
        assert!(open_named_column(bytes, "left").is_ok());
    }

    fn open_parquet_fixture(bytes: Bytes, batch_size: usize) -> (DocValuesCursor, Arc<Runtime>) {
        let runtime = Arc::new(Builder::new_current_thread().enable_all().build().unwrap());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = ObjectPath::from(format!(
            "doc-values-cursor-{}.parquet",
            NEXT_HANDLE.fetch_add(1, Ordering::Relaxed)
        ));
        runtime
            .block_on(store.put(&location, bytes.into()))
            .unwrap();
        let cursor = runtime
            .block_on(DocValuesCursor::open(
                location.as_ref(),
                "value",
                batch_size,
                Some(store),
                Some(location.clone()),
                Arc::clone(&runtime),
            ))
            .unwrap();
        (cursor, runtime)
    }

    fn open_fixture_with_page_rows(
        row_groups: usize,
        rows_per_page: usize,
        batch_size: usize,
    ) -> (DocValuesCursor, Arc<Runtime>) {
        open_parquet_fixture(
            parquet_fixture_with_page_rows(row_groups, rows_per_page),
            batch_size,
        )
    }

    fn open_fixture(row_groups: usize, batch_size: usize) -> (DocValuesCursor, Arc<Runtime>) {
        open_fixture_with_page_rows(row_groups, ROWS_PER_PAGE, batch_size)
    }

    fn int64_values(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn forward_jump_uses_arrow_skip_without_fetching_intermediate_pages() {
        let (mut cursor, _runtime) = open_fixture(1, 8);
        let stats = Arc::clone(&cursor.stats);

        let first = cursor.next_batch(0).unwrap();
        assert_eq!(int64_values(&first), (0..8).collect::<Vec<_>>());
        assert_eq!(stats.count.load(Ordering::Relaxed), 1);

        let target = (ROWS_PER_PAGE * 5 + 7) as i64;
        let jumped = cursor.next_batch(target).unwrap();
        assert_eq!(
            int64_values(&jumped),
            (target..target + 8).collect::<Vec<_>>()
        );
        assert_eq!(
            stats.count.load(Ordering::Relaxed),
            2,
            "Arrow should fetch only the first and target data pages"
        );
    }

    #[test]
    fn all_null_page_is_skipped_without_fetch_or_decode() {
        let (mut cursor, _runtime) =
            open_parquet_fixture(parquet_fixture_with_all_null_page(ROWS_PER_PAGE), 8);

        let reads_before = cursor.stats.count.load(Ordering::Relaxed);
        let first = cursor.next_batch(17).unwrap();
        assert_eq!(first.num_rows(), 8);
        assert_eq!(first.column(0).null_count(), 8);
        assert_eq!(cursor.reader.position(), 25);
        assert_eq!(
            cursor.stats.count.load(Ordering::Relaxed),
            reads_before,
            "all-null page must be skipped from OffsetIndex metadata"
        );

        let second = cursor.next_batch(25).unwrap();
        assert_eq!(second.num_rows(), 16);
        assert_eq!(second.column(0).null_count(), 16);
        assert_eq!(
            cursor.stats.count.load(Ordering::Relaxed),
            reads_before,
            "bounded synthetic batches must not revisit the skipped page"
        );

        let non_null = cursor.next_batch(ROWS_PER_PAGE as i64).unwrap();
        assert_eq!(non_null.column(0).null_count(), 0);
        assert!(
            cursor.stats.count.load(Ordering::Relaxed) > reads_before,
            "the following non-null page should be fetched"
        );
    }

    #[test]
    fn local_file_cursor_reuses_retained_descriptor_for_page_reads() {
        let runtime = Arc::new(Builder::new_current_thread().enable_all().build().unwrap());
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&parquet_fixture_with_page_rows(1, ROWS_PER_PAGE))
            .unwrap();
        file.flush().unwrap();
        let filename = file.path().to_str().unwrap();
        let mut cursor = runtime
            .block_on(DocValuesCursor::open(
                filename,
                "value",
                8,
                None,
                None,
                Arc::clone(&runtime),
            ))
            .unwrap();
        let stats = Arc::clone(&cursor.stats);

        assert_eq!(
            int64_values(&cursor.next_batch(0).unwrap()),
            (0..8).collect::<Vec<_>>()
        );
        let target = (ROWS_PER_PAGE * 5 + 7) as i64;
        assert_eq!(
            int64_values(&cursor.next_batch(target).unwrap()),
            (target..target + 8).collect::<Vec<_>>()
        );
        assert_eq!(
            stats.count.load(Ordering::Relaxed),
            0,
            "local page reads must use the retained synchronous file descriptor"
        );
    }

    #[test]
    fn retained_arrow_reader_crosses_row_groups_and_rejects_backward_seeks() {
        let (mut cursor, _runtime) = open_fixture(2, 8);
        let second_rg = (ROWS_PER_PAGE * 8) as i64;

        let batch = cursor.next_batch(second_rg + 11).unwrap();
        assert_eq!(
            int64_values(&batch),
            (second_rg + 11..second_rg + 19).collect::<Vec<_>>()
        );
        let error = cursor.next_batch(second_rg - 1).unwrap_err();
        assert!(error.contains("backward seek"), "{error}");
    }

    #[test]
    fn adaptive_batches_grow_and_stop_at_page_boundaries() {
        let (mut cursor, _runtime) = open_fixture(1, 8);

        assert_eq!(cursor.next_batch(0).unwrap().num_rows(), 8);
        assert_eq!(cursor.next_batch(8).unwrap().num_rows(), 16);
        assert_eq!(cursor.next_batch(24).unwrap().num_rows(), 32);
        assert_eq!(
            cursor.next_batch(56).unwrap().num_rows(),
            8,
            "a batch must not decode across a data-page boundary"
        );
    }

    #[test]
    fn dense_access_retains_the_grown_window_across_pages() {
        let (mut cursor, _runtime) = open_fixture(1, 8);

        assert_eq!(cursor.next_batch(0).unwrap().num_rows(), 8);
        assert_eq!(cursor.next_batch(8).unwrap().num_rows(), 16);
        assert_eq!(cursor.next_batch(24).unwrap().num_rows(), 32);
        assert_eq!(cursor.next_batch(56).unwrap().num_rows(), 8);
        assert_eq!(
            cursor.next_batch(64).unwrap().num_rows(),
            ROWS_PER_PAGE,
            "a boundary-clamped read must not reset dense-scan growth"
        );
    }

    #[test]
    fn small_forward_skip_keeps_growing_the_window() {
        let (mut cursor, _runtime) = open_fixture(1, 8);

        assert_eq!(cursor.next_batch(0).unwrap().num_rows(), 8);
        // Skips no larger than the current window are page-scale dense access
        // (e.g. a moderately selective filter) and must not stall the ramp-up.
        assert_eq!(cursor.next_batch(11).unwrap().num_rows(), 16);
        assert_eq!(cursor.next_batch(27).unwrap().num_rows(), 32);
    }

    #[test]
    fn large_jump_halves_the_window_instead_of_resetting() {
        let (mut cursor, _runtime) = open_fixture(2, 8);

        assert_eq!(cursor.next_batch(0).unwrap().num_rows(), 8);
        assert_eq!(cursor.next_batch(8).unwrap().num_rows(), 16);
        assert_eq!(cursor.next_batch(24).unwrap().num_rows(), 32);

        // Decay is multiplicative (32 -> 16), not a reset to the initial window.
        let jump = (ROWS_PER_PAGE * 3 + 5) as i64;
        assert_eq!(cursor.next_batch(jump).unwrap().num_rows(), 16);

        // Repeated large jumps keep halving and bottom out at the initial window.
        let jump = (ROWS_PER_PAGE * 6 + 3) as i64;
        assert_eq!(cursor.next_batch(jump).unwrap().num_rows(), 8);
        let jump = (ROWS_PER_PAGE * 9 + 1) as i64;
        assert_eq!(cursor.next_batch(jump).unwrap().num_rows(), 8);
    }

    #[test]
    fn adaptive_window_is_capped_at_max_batch_size() {
        let rows_per_page = MAX_BATCH_SIZE * 2;
        let (mut cursor, _runtime) =
            open_fixture_with_page_rows(1, rows_per_page, MAX_BATCH_SIZE / 2);

        assert_eq!(cursor.next_batch(0).unwrap().num_rows(), MAX_BATCH_SIZE / 2);
        assert_eq!(
            cursor
                .next_batch((MAX_BATCH_SIZE / 2) as i64)
                .unwrap()
                .num_rows(),
            MAX_BATCH_SIZE
        );
        assert_eq!(
            cursor
                .next_batch((MAX_BATCH_SIZE + MAX_BATCH_SIZE / 2) as i64)
                .unwrap()
                .num_rows(),
            MAX_BATCH_SIZE / 2,
            "the max-sized window is clamped at the page boundary"
        );
        assert_eq!(
            cursor.next_batch(rows_per_page as i64).unwrap().num_rows(),
            MAX_BATCH_SIZE,
            "the adaptive window must not grow beyond the configured maximum"
        );
    }
}
