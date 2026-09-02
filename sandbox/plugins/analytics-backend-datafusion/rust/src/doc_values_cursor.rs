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
//!
//! # Handles
//!
//! `parquet_df_open_iter` returns an opaque cursor handle, not a pointer: it keys a
//! process-wide registry, so an unknown or stale handle is reported as an error rather
//! than dereferenced. `0` is never a live handle. Java owns the lifecycle and calls
//! `parquet_df_close_iter` exactly once.

use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use datafusion::error::DataFusionError;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion_datasource::PartitionedFile;
use native_bridge_common::ffm_safe;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use parquet::arrow::{parquet_to_arrow_schema_by_columns, ProjectionMask};
#[cfg(test)]
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

use crate::cache::page_index::load_scoped_page_index_cols;
use crate::forward_reader::{ParquetForwardBatchReader, ParquetForwardBatchReaderFactory};
use crate::indexed_table::parquet_bridge::{
    load_parquet_metadata_with_meta, CachedMetadataReaderFactory, ReadIoStats,
};
use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;

/// Largest decode window this reader will accept, whatever the caller asks for.
/// `index.parquet.docvalues.max_batch_size` can only lower a cursor's ceiling, never raise it past
/// this. Mirrored by `ParquetSettings.DEFAULT_DOCVALUES_MAX_BATCH_SIZE` on the Java side.
const BATCH_SIZE_HARD_LIMIT: usize = 8_192;

/// Default ceiling, as a cursor opened through the FFM boundary would receive it. Tests that care
/// about a different cap pass their own, since the value is per cursor.
#[cfg(test)]
const TEST_MAX_BATCH_SIZE: usize = BATCH_SIZE_HARD_LIMIT;

/// Status codes returned to Java. A negative return is an error-message pointer produced by
/// `ffm_safe`, so only non-negative values are status; 1 is unused. Mirrors `ParquetCodecBridge`.
const RC_OK: i64 = 0;
const RC_EOF: i64 = 2;

static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1); // 0 is never a live handle
static CURSORS: Lazy<DashMap<i64, Arc<Mutex<DocValuesCursor>>>> = Lazy::new(DashMap::new);

/// The global runtime's cache, so reopening a cursor does not re-read the Parquet footer. Required
/// rather than defaulted: a cache created here would hold a second copy of every footer, outside the
/// node's configured budget.
fn metadata_cache() -> Result<Arc<dyn FileMetadataCache>, DataFusionError> {
    crate::cache::global_metadata_cache().ok_or_else(|| {
        DataFusionError::Configuration(
            "no global file-metadata cache; the analytics-backend-datafusion global runtime must be \
             created before Parquet doc-values reads"
                .to_string(),
        )
    })
}

/// The shared IO runtime, used only to block on object-store fetches; decode is synchronous and runs
/// on the calling thread. Required rather than defaulted: `RuntimeManager` sizes this pool from the
/// core count and monitors it, and a pool created here would be neither sized nor monitored.
fn io_runtime() -> Result<Arc<Runtime>, DataFusionError> {
    if let Some(manager) = crate::ffm::try_get_rt_manager() {
        return Ok(Arc::clone(&manager.io_runtime));
    }
    #[cfg(test)]
    {
        // Tests reach the entry points without DataFusionService. Current-thread: no worker threads.
        Ok(Arc::new(
            Builder::new_current_thread().enable_all().build()?,
        ))
    }
    #[cfg(not(test))]
    Err(DataFusionError::Configuration(
        "DataFusion runtime manager is not initialized; the analytics-backend-datafusion plugin \
         must start before Parquet doc-values reads"
            .to_string(),
    ))
}

struct DocValuesCursor {
    reader: ParquetForwardBatchReader,
    /// Retained so a reset can rebuild the reader without re-resolving metadata or the page index.
    factory: ParquetForwardBatchReaderFactory,
    row_count: i64,
    initial_batch_size: usize,
    /// Ceiling the window grows to, captured at open from
    /// `index.parquet.docvalues.max_batch_size` so a later setting update cannot disagree with
    /// `initial_batch_size` mid-cursor.
    max_batch_size: usize,
    batch_size: usize,
    has_decoded_batch: bool,
    /// The batch Java last borrowed from, held so the exported pointers stay valid. Released on the
    /// next batch call, on reset, or when close drops the cursor.
    borrowed_batch: Option<RecordBatch>,
    /// Retained so tests can assert on the number of object-store range reads;
    /// also handed to the reader factory to attribute I/O.
    stats: Arc<ReadIoStats>,
}

impl DocValuesCursor {
    /// Opens a cursor over one column. `store_override` is the store that owns `filename`; `None`
    /// uses the local filesystem, which is what local-tier shard files need. A non-local store is
    /// supplied by the caller along with its own `location_override`.
    async fn open(
        filename: &str,
        column: &str,
        batch_size: usize,
        max_batch_size: usize,
        store_override: Option<Arc<dyn ObjectStore>>,
        location_override: Option<ObjectPath>,
        runtime: Arc<Runtime>,
    ) -> Result<Self, DataFusionError> {
        let location = location_override.unwrap_or_else(|| ObjectPath::from(filename));
        // Ask the store where the object lives rather than probing the filesystem for `filename`:
        // only a local-filesystem store can name a descriptor, and only it knows how an object path
        // maps onto one. A caller-supplied store reads through its own IO.
        let (store, local_path): (Arc<dyn ObjectStore>, Option<PathBuf>) = match store_override {
            Some(store) => (store, None),
            None => {
                let local = LocalFileSystem::new();
                let path = local.path_to_filesystem(&location).ok();
                (Arc::new(local), path)
            }
        };

        let object_meta = store.head(&location).await?;
        let (_arrow_schema, file_size, footer) = load_parquet_metadata_with_meta(
            Arc::clone(&store),
            &location,
            object_meta,
            metadata_cache()?,
        )
        .await
        .map_err(DataFusionError::Execution)?;

        let schema = footer.file_metadata().schema_descr();

        // Collect every candidate rather than taking the first: two leaves in different row groups
        // can share a name, and a group root can cover several leaves. Serving whichever came first
        // would read the wrong column silently, so an ambiguous name is an error.
        //
        // Flat schemas cannot produce an ambiguity, so this only applies to files written with a
        // nested schema, by a future mapping type or another writer.
        let matches = (0..schema.num_columns())
            .filter(|&idx| {
                let descriptor = schema.column(idx);
                descriptor.name() == column
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
                return Err(DataFusionError::Plan(format!(
                    "column '{column}' not found in {filename}"
                )))
            }
            several => {
                let paths = several
                    .iter()
                    .map(|&idx| schema.column(idx).path().string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(DataFusionError::Plan(format!(
                    "column '{column}' is ambiguous in {filename}; it matches {paths}"
                )));
            }
        };
        // Reject on the Arrow type the reader will produce, not the Parquet physical type: an INT64
        // decimal is a valid physical type but decodes to `Decimal128`, which cannot be borrowed.
        // Converted with the same projection and key-value metadata the reader uses, so the two
        // cannot disagree.
        let leaf_schema = parquet_to_arrow_schema_by_columns(
            schema,
            ProjectionMask::leaves(schema, [leaf_idx]),
            footer.file_metadata().key_value_metadata(),
        )?;
        let data_type = leaf_schema.field(0).data_type();
        if BorrowKind::for_arrow(data_type).is_none() {
            return Err(DataFusionError::NotImplemented(format!(
                "unsupported type {data_type} for column '{column}'"
            )));
        }

        let metadata =
            load_scoped_page_index_cols(&store, &location, &footer, &[leaf_idx], &[leaf_idx])
                .await
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "no page index available for column '{column}' in {filename}"
                    ))
                })?; // OffsetIndex + ColumnIndex scoped to just this column
        let stats = Arc::new(ReadIoStats::default());
        let projection =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [leaf_idx]);
        // Clamp the ceiling first, then the starting window against it, so the decay floor
        // (`initial_batch_size`) can never exceed the growth cap.
        let max_batch_size = max_batch_size.clamp(1, BATCH_SIZE_HARD_LIMIT);
        let batch_size = batch_size.clamp(1, max_batch_size);
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
        // Local files read through a synchronous `ChunkReader`; anything else goes via the store.
        .with_local_file(local_path);
        let reader = factory.open()?;
        let row_count = reader.row_count() as i64;

        Ok(Self {
            reader,
            factory,
            row_count,
            initial_batch_size: batch_size,
            max_batch_size,
            batch_size,
            has_decoded_batch: false,
            borrowed_batch: None,
            stats,
        })
    }

    /// Chooses the decode window for `target_row` and returns `(window, rows)`:
    /// `window` is the adaptive size to remember for next time, `rows` is how much
    /// to decode now, never past the end of `target_row`'s page.
    fn planned_batch(&self, target_row: i64) -> Result<(usize, usize), DataFusionError> {
        if target_row < 0 || target_row >= self.row_count {
            return Err(DataFusionError::Internal(format!(
                "row {target_row} out of range (0..{})",
                self.row_count
            )));
        }
        let target_row = target_row as usize;
        let position = self.reader.position();
        // Adaptive decode window: geometric growth with multiplicative backoff. A forward skip no
        // larger than the current window is treated as dense access, so the window doubles; a jump
        // beyond it halves the window rather than resetting, so mixed access does not re-ramp from
        // the initial size after every jump.
        //
        // TODO: the growth and backoff factors are not benchmarked. Tune them, and the default
        // ceiling, against representative query shapes.
        let dense = self.has_decoded_batch
            && target_row >= position
            && target_row - position <= self.batch_size;
        let window = if dense {
            self.batch_size.saturating_mul(2).min(self.max_batch_size)
        } else if self.has_decoded_batch {
            (self.batch_size / 2).max(self.initial_batch_size)
        } else {
            self.batch_size
        };
        // A read never crosses a page boundary, so a window larger than one page cannot be
        // satisfied. The remembered window is capped at the whole page; only the rows decoded now
        // are additionally limited to what is left of it.
        let page_rows = self.reader.page_row_count(target_row)?;
        let page_remaining = self.reader.rows_remaining_in_page(target_row)?;
        let window = window.min(page_rows);
        Ok((window, window.min(page_remaining)))
    }

    fn next_batch(&mut self, target_row: i64) -> Result<RecordBatch, DataFusionError> {
        // `window` is the size to remember for next time; `rows` is what we
        // actually decode now.
        let (window, rows) = self.planned_batch(target_row)?;
        let batch = self
            .reader
            .read_batch_at(target_row as usize, rows)?
            .ok_or_else(|| {
                DataFusionError::Internal(format!("reader exhausted before row {target_row}"))
            })?;
        self.batch_size = window;
        self.has_decoded_batch = true;
        Ok(batch)
    }
}

/// Shared entry-point prologue: resolves a live cursor handle.
fn cursor_for(handle: i64, fn_name: &str) -> Result<Arc<Mutex<DocValuesCursor>>, DataFusionError> {
    CURSORS
        .get(&handle)
        .map(|entry| Arc::clone(entry.value()))
        .ok_or_else(|| DataFusionError::Internal(format!("{fn_name}: unknown handle {handle}")))
}

/// Validates `target_row`, returning `true` when the cursor is exactly at end-of-column.
fn at_eof(
    cursor: &DocValuesCursor,
    target_row: i64,
    fn_name: &str,
) -> Result<bool, DataFusionError> {
    if target_row == cursor.row_count {
        return Ok(true);
    }
    if target_row < 0 || target_row > cursor.row_count {
        return Err(DataFusionError::Internal(format!(
            "{fn_name}: row {target_row} out of range (0..={})",
            cursor.row_count
        )));
    }
    Ok(false)
}

/// Writes `value` through a nullable out-parameter.
unsafe fn write_out(ptr: *mut i64, value: i64) {
    if !ptr.is_null() {
        *ptr = value;
    }
}

/// Java-side interpretation of a borrowed values buffer, and the single place a supported column
/// type is defined. Discriminants are the wire values; mirrors `DecodedBatch.KIND_*`.
///
/// Adding a type means adding a variant here plus arms in [`BorrowKind::for_arrow`] and
/// [`BorrowKind::width`], and a matching constant on the Java side.
#[repr(i64)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum BorrowKind {
    Long = 1,     // i64 / u64 raw bits
    Int = 2,      // i32 / date32, sign-extended
    UintBits = 3, // u32 raw bits, zero-extended
    Short = 4,    // i16, sign-extended
    Ushort = 5,   // u16, zero-extended
    Byte = 6,     // i8, sign-extended
    Ubyte = 7,    // u8, zero-extended
    Double = 8,   // f64 raw bits; Java re-encodes to a Lucene sortable long
    Float = 9,    // f32 raw bits; Java re-encodes to a sign-extended sortable int
}

impl BorrowKind {
    /// Keyed on the Arrow type, which is the decode output: Parquet INT32 arrives as
    /// Int8/Int16/Int32/Date32 depending on its logical type.
    ///
    /// TODO: boolean needs a bit-packed values buffer with a value bit offset, mirroring the
    /// validity bitmap. Binary and variable-width columns come after that. `Float16`
    /// (OpenSearch `half_float`) needs a kind whose Java side re-encodes to a sortable short.
    fn for_arrow(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Int64 | DataType::UInt64 | DataType::Date64 | DataType::Timestamp(_, _) => {
                Some(Self::Long)
            }
            DataType::Float64 => Some(Self::Double),
            DataType::Int32 | DataType::Date32 | DataType::Time32(_) => Some(Self::Int),
            DataType::UInt32 => Some(Self::UintBits),
            DataType::Float32 => Some(Self::Float),
            DataType::Int16 => Some(Self::Short),
            DataType::UInt16 => Some(Self::Ushort),
            DataType::Int8 => Some(Self::Byte),
            DataType::UInt8 => Some(Self::Ubyte),
            _ => None,
        }
    }

    /// Bytes per row. Mirrors `ParquetColumnReader.widthForKind`.
    fn width(self) -> usize {
        match self {
            Self::Long | Self::Double => 8,
            Self::Int | Self::UintBits | Self::Float => 4,
            Self::Short | Self::Ushort => 2,
            Self::Byte | Self::Ubyte => 1,
        }
    }
}

struct BorrowedBuffers {
    values_addr: usize,
    validity_addr: usize,
    validity_bit_offset: usize,
    kind: i64,
}

/// Exposes an Arrow primitive array's buffers for zero-copy reads from Java.
///
/// Java reads the values buffer and validity bitmap in place, widening per accessed row, so a sparse
/// reader pays O(rows accessed) rather than O(rows served) and avoids a per-batch copy.
///
/// `open` rejects an unsupported column, so `None` here means the column's Arrow type changed
/// between open and read.
fn borrowable_buffers(array: &dyn Array) -> Option<BorrowedBuffers> {
    let kind = BorrowKind::for_arrow(array.data_type())?;
    let width = kind.width();
    debug_assert_eq!(array.data_type().primitive_width(), Some(width));
    let data = array.to_data();
    let buffer = data.buffers().first()?; // buffer 0 holds the values for a primitive array
                                          // Fold the array offset into the pointer so it addresses row 0. Zero for primitive arrays, whose
                                          // window Arrow folds into the buffer pointer, but not for `BooleanArray`.
    let values_addr = buffer.as_ptr() as usize + data.offset() * width;
    let (validity_addr, validity_bit_offset) = match data.nulls() {
        None => (0, 0),
        Some(nulls) => (nulls.buffer().as_ptr() as usize, nulls.offset()),
    };
    Some(BorrowedBuffers {
        values_addr,
        validity_addr, // start of the null-bitmap buffer; the bit offset is applied separately
        validity_bit_offset, // bit index of row 0 within that bitmap (bitmaps are bit-addressable)
        kind: kind as i64,
    })
}

/// Opens a cursor and registers it, returning the handle Java holds.
fn open_and_register(
    filename: &str,
    column: &str,
    initial_batch_size: usize,
    max_batch_size: usize,
) -> Result<i64, DataFusionError> {
    let runtime = io_runtime()?;
    let cursor = runtime.block_on(DocValuesCursor::open(
        filename,
        column,
        initial_batch_size,
        max_batch_size,
        None,
        None,
        Arc::clone(&runtime),
    ))?;
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::SeqCst);
    CURSORS.insert(handle, Arc::new(Mutex::new(cursor)));
    Ok(handle)
}

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, DataFusionError> {
    if ptr.is_null() {
        return Err(DataFusionError::Internal("null string pointer".to_string()));
    }
    if len < 0 {
        return Err(DataFusionError::Internal(format!(
            "negative string length: {len}"
        )));
    }
    std::str::from_utf8(std::slice::from_raw_parts(ptr, len as usize))
        .map_err(|e| DataFusionError::Internal(format!("invalid UTF-8: {e}")))
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_df_open_iter(
    file_ptr: *const u8, // path bytes and length, since FFM cannot pass a Rust &str
    file_len: i64,
    column_ptr: *const u8,
    column_len: i64,
    initial_batch_size: i64,
    max_batch_size: i64,
) -> i64 {
    static FN: &str = "parquet_df_open_iter";
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("{FN} file: {e}"))?;
    let column = str_from_raw(column_ptr, column_len).map_err(|e| format!("{FN} column: {e}"))?;
    // Re-checked here because these arrive as untrusted `i64`: a negative would wrap to an enormous
    // `usize` on cast, and the Java reader exposes an int overload that bypasses settings resolution.
    if max_batch_size <= 0 || max_batch_size > BATCH_SIZE_HARD_LIMIT as i64 {
        return Err(format!(
            "{FN}: max batch size {max_batch_size} outside 1..={BATCH_SIZE_HARD_LIMIT}"
        ));
    }
    if initial_batch_size <= 0 || initial_batch_size > max_batch_size {
        return Err(format!(
            "{FN}: initial batch size {initial_batch_size} outside 1..={max_batch_size}"
        ));
    }
    // The FFM contract carries only a message (see native_bridge_common::error), so the typed
    // error is flattened here at the boundary.
    open_and_register(
        filename,
        column,
        initial_batch_size as usize,
        max_batch_size as usize,
    )
    .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_df_close_iter(handle: i64) -> i64 {
    CURSORS.remove(&handle);
    Ok(RC_OK)
}

/// Rewinds a cursor to row zero, rebuilding the Parquet decoder while reusing the resolved metadata
/// and page index, so the rewind costs no IO.
///
/// Only reached when a request lands below the resident batch, which a caller honouring Lucene's
/// non-decreasing `advanceExact` contract never does. Kept because Lucene's own dense numeric
/// doc values tolerate a backward target (their layout is seekable), so a non-compliant caller
/// would fail here and nowhere else; this turns that into a slow read rather than a shard failure.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_df_reset_iter(handle: i64) -> i64 {
    static FN: &str = "parquet_df_reset_iter";
    let cursor = cursor_for(handle, FN).map_err(|e| e.to_string())?;
    let mut cursor = cursor.lock();
    cursor.reader = cursor.factory.open().map_err(|e| format!("{FN}: {e}"))?;
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
    static FN: &str = "parquet_df_next_batch";
    let cursor = cursor_for(handle, FN).map_err(|e| e.to_string())?;
    let mut cursor = cursor.lock();

    // Released here rather than on success, so no early return below leaves buffers held. Java
    // clears its resident batch before calling.
    cursor.borrowed_batch = None;

    if at_eof(&cursor, target_row, FN).map_err(|e| e.to_string())? {
        return Ok(RC_EOF); // target is past the last row (e.g. a scan running off the end)
    }

    let batch = cursor.next_batch(target_row).map_err(|e| e.to_string())?;
    let rows = batch.num_rows();
    if rows == 0 || rows > cursor.max_batch_size {
        return Err(format!(
            "{FN}: Arrow returned {rows} rows, expected 1..={}",
            cursor.max_batch_size
        ));
    }

    // Scoped so the borrow ends before `batch` moves onto the cursor; `BorrowedBuffers` holds
    // plain addresses.
    let borrow = {
        let array = batch.column(0); // single projected column
        borrowable_buffers(array.as_ref())
            .ok_or_else(|| format!("{FN}: unsupported non-numeric array {}", array.data_type()))?
    };

    // Written only once the export is known good, so a failed call leaves them untouched.
    write_out(out_first_row, target_row);
    write_out(out_last_row, target_row + rows as i64 - 1); // inclusive last row of this batch
    write_out(out_values_addr, borrow.values_addr as i64);
    write_out(out_validity_addr, borrow.validity_addr as i64);
    write_out(out_validity_bit_offset, borrow.validity_bit_offset as i64);
    write_out(out_value_kind, borrow.kind);
    cursor.borrowed_batch = Some(batch);
    Ok(RC_OK)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use datafusion::execution::cache::DefaultFilesMetadataCache;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use tempfile::NamedTempFile;

    use super::*;
    use crate::cache::metadata_cache::MutexFileMetadataCache;

    const ROWS_PER_PAGE: usize = 64;

    /// Stands in for the cache that production registers from `create_global_runtime`.
    /// Held in a static because the registry keeps only a `Weak`: a cache dropped at the end of a
    /// test would leave later opens with nothing registered.
    pub(super) fn register_test_metadata_cache() {
        static CACHE: Lazy<Arc<MutexFileMetadataCache>> = Lazy::new(|| {
            Arc::new(MutexFileMetadataCache::new(DefaultFilesMetadataCache::new(
                64 * 1024 * 1024,
            )))
        });
        crate::cache::register_global_metadata_cache(
            Arc::clone(&CACHE) as Arc<dyn FileMetadataCache>
        );
    }

    pub(super) fn parquet_fixture_with_page_rows(row_groups: usize, rows_per_page: usize) -> Bytes {
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

    /// A decimal column: Parquet stores precision 18 as INT64, and Arrow decodes it back to
    /// `Decimal128`, which has no [`BorrowKind`].
    pub(super) fn parquet_fixture_with_decimal_column(rows_per_page: usize) -> Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Decimal128(18, 2),
            false,
        )]));
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(rows_per_page)
            .set_write_batch_size(rows_per_page)
            .build();
        let mut writer =
            ArrowWriter::try_new(Cursor::new(Vec::new()), Arc::clone(&schema), Some(props))
                .unwrap();
        let values = (0..rows_per_page * 2)
            .map(|value| value as i128)
            .collect::<Vec<_>>();
        let array = arrow::array::Decimal128Array::from(values)
            .with_precision_and_scale(18, 2)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).unwrap();
        writer.write(&batch).unwrap();
        Bytes::from(writer.into_inner().unwrap().into_inner())
    }

    fn open_named_column(bytes: Bytes, column: &str) -> Result<DocValuesCursor, DataFusionError> {
        register_test_metadata_cache();
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
            TEST_MAX_BATCH_SIZE,
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
            .expect("an ambiguous column must not open")
            .to_string();
        assert!(error.contains("ambiguous"), "{error}");
        assert!(
            error.contains("group.left") && error.contains("group.right"),
            "{error}"
        );

        // An unambiguous leaf resolves to a single column, but a nested leaf projects as a struct
        // rather than a flat primitive, so it is refused at open instead of on the first read.
        let error = open_named_column(bytes, "left")
            .err()
            .expect("a nested leaf is not a supported column type")
            .to_string();
        assert!(error.contains("unsupported type Struct"), "{error}");
        assert!(
            !error.contains("ambiguous"),
            "resolution must still pick a single leaf: {error}"
        );
    }

    fn open_parquet_fixture(bytes: Bytes, batch_size: usize) -> (DocValuesCursor, Arc<Runtime>) {
        open_parquet_fixture_with_max(bytes, batch_size, TEST_MAX_BATCH_SIZE)
    }

    fn open_parquet_fixture_with_max(
        bytes: Bytes,
        batch_size: usize,
        max_batch_size: usize,
    ) -> (DocValuesCursor, Arc<Runtime>) {
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
                max_batch_size,
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
                TEST_MAX_BATCH_SIZE,
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
        let error = cursor.next_batch(second_rg - 1).unwrap_err().to_string();
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
        let rows_per_page = BATCH_SIZE_HARD_LIMIT * 2;
        let (mut cursor, _runtime) =
            open_fixture_with_page_rows(1, rows_per_page, BATCH_SIZE_HARD_LIMIT / 2);

        assert_eq!(
            cursor.next_batch(0).unwrap().num_rows(),
            BATCH_SIZE_HARD_LIMIT / 2
        );
        assert_eq!(
            cursor
                .next_batch((BATCH_SIZE_HARD_LIMIT / 2) as i64)
                .unwrap()
                .num_rows(),
            BATCH_SIZE_HARD_LIMIT
        );
        assert_eq!(
            cursor
                .next_batch((BATCH_SIZE_HARD_LIMIT + BATCH_SIZE_HARD_LIMIT / 2) as i64)
                .unwrap()
                .num_rows(),
            BATCH_SIZE_HARD_LIMIT / 2,
            "the max-sized window is clamped at the page boundary"
        );
        assert_eq!(
            cursor.next_batch(rows_per_page as i64).unwrap().num_rows(),
            BATCH_SIZE_HARD_LIMIT,
            "the adaptive window must not grow beyond the configured maximum"
        );
    }

    /// `index.parquet.docvalues.max_batch_size` lowers the ceiling per cursor, so growth has to stop
    /// at the configured value rather than the compile-time limit.
    #[test]
    fn a_lowered_maximum_caps_growth_below_the_hard_limit() {
        let configured_max = 64;
        let (mut cursor, _runtime) = open_parquet_fixture_with_max(
            parquet_fixture_with_page_rows(1, 1024),
            8,
            configured_max,
        );

        // 8 -> 16 -> 32 -> 64, then held: doubling would pass the configured ceiling.
        let mut row = 0i64;
        for expected in [8, 16, 32, 64, 64, 64] {
            let rows = cursor.next_batch(row).unwrap().num_rows();
            assert_eq!(rows, expected, "window at row {row}");
            row += rows as i64;
        }
        assert!(
            configured_max < BATCH_SIZE_HARD_LIMIT,
            "the test is only meaningful below the hard limit"
        );
    }

    /// The starting window is clamped to the ceiling, so the decay floor can never sit above the
    /// growth cap however the two settings are combined.
    #[test]
    fn a_starting_window_above_the_maximum_is_lowered_to_it() {
        let configured_max = 32;
        let (cursor, _runtime) = open_parquet_fixture_with_max(
            parquet_fixture_with_page_rows(1, 1024),
            4096,
            configured_max,
        );

        assert_eq!(cursor.initial_batch_size, configured_max);
        assert_eq!(cursor.batch_size, configured_max);
        assert_eq!(cursor.max_batch_size, configured_max);
    }

    /// The kind is the whole of the Rust-to-Java type contract: Java picks a byte width and a
    /// sign-extension rule from it alone, so a wrong mapping here reads the right bytes as the
    /// wrong numbers. Mirrors the `KIND_*` constants in `DecodedBatch.java` and the widths in
    /// `ParquetColumnReader.widthForKind`.
    #[test]
    fn each_borrowable_arrow_type_maps_to_the_kind_java_expects() {
        use arrow::array::{
            Float32Array, Float64Array, Int16Array, Int32Array, Int8Array, UInt16Array,
            UInt32Array, UInt64Array, UInt8Array,
        };

        let cases: Vec<(ArrayRef, BorrowKind)> = vec![
            (Arc::new(Int64Array::from(vec![1])), BorrowKind::Long),
            (Arc::new(UInt64Array::from(vec![1])), BorrowKind::Long),
            (Arc::new(Float64Array::from(vec![1.0])), BorrowKind::Double),
            (Arc::new(Int32Array::from(vec![1])), BorrowKind::Int),
            (Arc::new(UInt32Array::from(vec![1])), BorrowKind::UintBits),
            (Arc::new(Float32Array::from(vec![1.0])), BorrowKind::Float),
            (Arc::new(Int16Array::from(vec![1])), BorrowKind::Short),
            (Arc::new(UInt16Array::from(vec![1])), BorrowKind::Ushort),
            (Arc::new(Int8Array::from(vec![1])), BorrowKind::Byte),
            (Arc::new(UInt8Array::from(vec![1])), BorrowKind::Ubyte),
        ];

        for (array, expected_kind) in cases {
            let borrow = borrowable_buffers(array.as_ref())
                .unwrap_or_else(|| panic!("{} must be borrowable", array.data_type()));
            assert_eq!(
                borrow.kind,
                expected_kind as i64,
                "kind for {}",
                array.data_type()
            );
            assert_eq!(
                expected_kind.width(),
                array.data_type().primitive_width().unwrap(),
                "width for {}",
                array.data_type()
            );
        }
    }

    /// An array can be a window onto a longer buffer, so row 0 need not sit at the buffer's start
    /// and Java is handed exactly one address per buffer.
    ///
    /// The two buffers reach that address differently. Arrow folds a primitive array's window into
    /// the values buffer's own pointer (`ScalarBuffer::new(buffer, offset, len)` when a
    /// `PrimitiveArray` is built from `ArrayData`, and `ArrayData::from` never writes the offset
    /// back), so `to_data().offset()` reads as 0 here and the `data.offset()` term in the export is
    /// defensive. A validity bitmap keeps its offset, because a bit position cannot be folded into
    /// a byte pointer, which is why the bit offset is a separate out-parameter.
    #[test]
    fn a_borrowed_window_exports_row_zero_and_its_first_validity_bit() {
        use arrow::array::{make_array, ArrayData};
        use arrow::buffer::Buffer;

        let values_buffer = Buffer::from_slice_ref((0..8_i64).collect::<Vec<_>>());
        let values_base = values_buffer.as_ptr() as usize;
        // Bitmaps are indexed from the least significant bit, so this clears bit 3: row 3 is null.
        // A window with no nulls in it is dropped by `ArrayDataBuilder::build`, which would leave
        // no bitmap to assert on.
        let validity_buffer = Buffer::from_slice_ref([0b1111_0111_u8]);
        let validity_base = validity_buffer.as_ptr() as usize;

        // Rows 3..8 of an eight-row buffer.
        let array = make_array(
            ArrayData::builder(DataType::Int64)
                .len(5)
                .offset(3)
                .add_buffer(values_buffer)
                .null_bit_buffer(Some(validity_buffer))
                .build()
                .unwrap(),
        );
        assert_eq!(
            array.to_data().offset(),
            0,
            "the values window is carried by the buffer pointer, not by the offset"
        );

        let borrow = borrowable_buffers(array.as_ref()).expect("Int64 must be borrowable");
        assert_eq!(
            borrow.values_addr,
            values_base + 3 * std::mem::size_of::<i64>(),
            "the exported pointer must address the window's first row, not the buffer start"
        );
        assert_eq!(
            unsafe { *(borrow.values_addr as *const i64) },
            3,
            "reading through the exported pointer must yield the window's first value"
        );
        assert_eq!(
            borrow.validity_addr, validity_base,
            "the bitmap is exported unshifted, because bit 3 is not at a byte boundary"
        );
        assert_eq!(
            borrow.validity_bit_offset, 3,
            "so Java is handed the bit offset to apply itself"
        );
    }

    /// Rejected rather than exported as raw bytes Java would silently misread.
    #[test]
    fn a_type_with_no_borrow_kind_is_not_borrowable() {
        let decimal = arrow::array::Decimal128Array::from(vec![1_i128, 2])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert!(borrowable_buffers(&decimal).is_none(), "Decimal128");

        let strings = arrow::array::StringArray::from(vec!["a", "b"]);
        assert!(borrowable_buffers(&strings).is_none(), "Utf8");

        let booleans = arrow::array::BooleanArray::from(vec![true, false]);
        assert!(
            borrowable_buffers(&booleans).is_none(),
            "Boolean is bit-packed, so it needs a value bit offset before it can be exported"
        );
    }
}

/// Tests driving the `extern "C"` entry points: handle registry, status codes, and `borrowed_batch`,
/// none of which the inherent-method tests above touch.
#[cfg(test)]
mod ffm_tests {
    use std::ffi::{c_char, CString};
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::tests::{parquet_fixture_with_page_rows, register_test_metadata_cache};
    use super::*;

    const ROWS_PER_PAGE: usize = 64;
    /// `parquet_fixture_with_page_rows` writes eight pages per row group.
    const FIXTURE_ROWS: i64 = (ROWS_PER_PAGE * 8) as i64;

    /// Reads the message an error return points at, taking ownership so it is freed rather than
    /// leaked into the test binary.
    fn error_message(rc: i64) -> String {
        assert!(rc < 0, "expected an error pointer, got {rc}");
        unsafe {
            CString::from_raw((-rc) as *mut c_char)
                .into_string()
                .expect("error message must be valid UTF-8")
        }
    }

    fn fixture_file() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&parquet_fixture_with_page_rows(1, ROWS_PER_PAGE))
            .unwrap();
        file.flush().unwrap();
        file
    }

    fn open_iter(path: &str, initial: i64) -> i64 {
        open_iter_with_max(path, initial, TEST_MAX_BATCH_SIZE as i64)
    }

    fn open_iter_with_max(path: &str, initial: i64, max: i64) -> i64 {
        register_test_metadata_cache();
        let column = "value";
        unsafe {
            parquet_df_open_iter(
                path.as_ptr(),
                path.len() as i64,
                column.as_ptr(),
                column.len() as i64,
                initial,
                max,
            )
        }
    }

    fn open_fixture(file: &NamedTempFile) -> i64 {
        let handle = open_iter(file.path().to_str().unwrap(), 8);
        assert!(handle >= 0, "{}", error_message(handle));
        handle
    }

    /// True when the cursor behind `handle` is holding Arrow buffers on Java's behalf.
    fn holds_borrow(handle: i64) -> bool {
        CURSORS
            .get(&handle)
            .expect("cursor must still be registered")
            .value()
            .lock()
            .borrowed_batch
            .is_some()
    }

    struct Batch {
        rc: i64,
        first_row: i64,
        last_row: i64,
        values_addr: i64,
        validity_addr: i64,
        value_kind: i64,
    }

    /// Reads values back out of an exported buffer the way Java does, via
    /// `MemorySegment.ofAddress(addr).reinterpret(len)`: an unchecked view over memory Rust owns.
    ///
    /// # Safety
    /// Only valid while the cursor still holds the batch these addresses were borrowed from, so
    /// call this before the next entry-point call on the same handle.
    unsafe fn exported_i64s(addr: i64, rows: usize) -> Vec<i64> {
        std::slice::from_raw_parts(addr as *const i64, rows).to_vec()
    }

    fn next_batch(handle: i64, target_row: i64) -> Batch {
        let mut first_row = -1i64;
        let mut last_row = -1i64;
        let mut values_addr = 0i64;
        let mut validity_addr = 0i64;
        let mut validity_bit_offset = -1i64;
        let mut value_kind = -1i64;
        let rc = unsafe {
            parquet_df_next_batch(
                handle,
                target_row,
                &mut first_row,
                &mut last_row,
                &mut values_addr,
                &mut validity_addr,
                &mut validity_bit_offset,
                &mut value_kind,
            )
        };
        Batch {
            rc,
            first_row,
            last_row,
            values_addr,
            validity_addr,
            value_kind,
        }
    }

    #[test]
    fn a_served_batch_is_retained_so_javas_pointers_stay_valid() {
        let file = fixture_file();
        let handle = open_fixture(&file);

        assert!(
            !holds_borrow(handle),
            "nothing is borrowed before the first read"
        );
        let batch = next_batch(handle, 0);
        assert_eq!(batch.rc, RC_OK);
        assert_eq!(batch.first_row, 0);
        assert_eq!(batch.last_row, 7, "the initial window is eight rows");
        assert_ne!(batch.values_addr, 0, "a borrowed batch must expose values");
        assert_eq!(batch.value_kind, BorrowKind::Long as i64);
        assert!(holds_borrow(handle), "the served batch must be retained");

        assert_eq!(unsafe { parquet_df_close_iter(handle) }, RC_OK);
    }

    /// Reads back through the exported addresses across a whole multi-page scan, which is the only
    /// check on what Java actually sees. The tests around this one assert the *lifetime* of the
    /// borrow; a wrong address, a wrong kind, or an unfolded array offset keeps the batch alive
    /// just as well and still serves wrong doc values.
    #[test]
    fn exported_addresses_read_back_as_the_rows_the_batch_reports() {
        let file = fixture_file();
        let handle = open_fixture(&file);

        let mut row = 0;
        while row < FIXTURE_ROWS {
            let batch = next_batch(handle, row);
            assert_eq!(batch.rc, RC_OK, "row {row}");
            assert_eq!(batch.first_row, row);
            assert_eq!(batch.value_kind, BorrowKind::Long as i64);
            assert_eq!(
                batch.validity_addr, 0,
                "the fixture column is required, so no bitmap should be exported"
            );

            // The fixture writes value == row index, so the buffer must read back as exactly the
            // rows the batch claims to cover.
            let rows = (batch.last_row - batch.first_row + 1) as usize;
            let expected = (batch.first_row..=batch.last_row).collect::<Vec<_>>();
            assert_eq!(
                unsafe { exported_i64s(batch.values_addr, rows) },
                expected,
                "batch [{}, {}]",
                batch.first_row,
                batch.last_row
            );

            row = batch.last_row + 1;
        }

        assert_eq!(unsafe { parquet_df_close_iter(handle) }, RC_OK);
    }

    #[test]
    fn only_one_batch_is_retained_across_a_long_scan() {
        let file = fixture_file();
        let handle = open_fixture(&file);

        // Each call releases the previous batch before decoding, so retention never accumulates.
        let mut row = 0;
        while row < FIXTURE_ROWS {
            let batch = next_batch(handle, row);
            assert_eq!(batch.rc, RC_OK, "row {row}: {}", batch.last_row);
            assert!(holds_borrow(handle));
            row = batch.last_row + 1;
        }

        assert_eq!(unsafe { parquet_df_close_iter(handle) }, RC_OK);
    }

    #[test]
    fn reaching_end_of_column_releases_the_borrow() {
        let file = fixture_file();
        let handle = open_fixture(&file);

        assert_eq!(next_batch(handle, 0).rc, RC_OK);
        assert!(holds_borrow(handle));

        // EOF returns before decoding, so it must not leave the previous batch held.
        assert_eq!(next_batch(handle, FIXTURE_ROWS).rc, RC_EOF);
        assert!(
            !holds_borrow(handle),
            "end of column must release the retained batch"
        );

        assert_eq!(unsafe { parquet_df_close_iter(handle) }, RC_OK);
    }

    #[test]
    fn a_failed_read_releases_the_borrow() {
        let file = fixture_file();
        let handle = open_fixture(&file);

        assert_eq!(next_batch(handle, 0).rc, RC_OK);
        assert!(holds_borrow(handle));

        // Out of range: fails after the release, so nothing stays held.
        let message = error_message(next_batch(handle, FIXTURE_ROWS + 1).rc);
        assert!(message.contains("out of range"), "{message}");
        assert!(
            !holds_borrow(handle),
            "a failed read must release the retained batch"
        );

        // A backward seek fails inside the reader, after the release, for the same reason.
        assert_eq!(next_batch(handle, 40).rc, RC_OK);
        let message = error_message(next_batch(handle, 0).rc);
        assert!(message.contains("backward seek"), "{message}");
        assert!(!holds_borrow(handle));

        assert_eq!(unsafe { parquet_df_close_iter(handle) }, RC_OK);
    }

    /// A decimal is a supported Parquet physical type (precision 18 is stored as INT64) that decodes
    /// to Arrow `Decimal128`, which cannot be borrowed. `open` gates on the decoded Arrow type, so it
    /// is refused before a cursor exists rather than on the first read.
    #[test]
    fn a_column_that_cannot_be_borrowed_is_rejected_at_open() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&super::tests::parquet_fixture_with_decimal_column(
            ROWS_PER_PAGE,
        ))
        .unwrap();
        file.flush().unwrap();

        let before = CURSORS.len();
        let message = error_message(open_iter(file.path().to_str().unwrap(), 8));
        assert!(message.contains("unsupported type Decimal128"), "{message}");
        assert_eq!(
            CURSORS.len(),
            before,
            "a rejected open must not register a cursor"
        );
    }

    #[test]
    fn resetting_releases_the_borrow_and_allows_rereading_row_zero() {
        let file = fixture_file();
        let handle = open_fixture(&file);

        assert_eq!(next_batch(handle, 0).rc, RC_OK);
        let forward = next_batch(handle, 32);
        assert_eq!(forward.rc, RC_OK);
        assert_eq!(forward.first_row, 32);
        assert!(holds_borrow(handle));

        assert_eq!(unsafe { parquet_df_reset_iter(handle) }, RC_OK);
        assert!(
            !holds_borrow(handle),
            "reset must release the retained batch"
        );

        // Without the reset this would be a rejected backward seek.
        let rewound = next_batch(handle, 0);
        assert_eq!(rewound.rc, RC_OK);
        assert_eq!(rewound.first_row, 0);

        assert_eq!(unsafe { parquet_df_close_iter(handle) }, RC_OK);
    }

    #[test]
    fn closing_while_holding_a_borrow_drops_the_cursor() {
        let file = fixture_file();
        let handle = open_fixture(&file);

        assert_eq!(next_batch(handle, 0).rc, RC_OK);
        assert!(
            holds_borrow(handle),
            "close is exercised with a batch still held"
        );

        assert_eq!(unsafe { parquet_df_close_iter(handle) }, RC_OK);
        assert!(
            CURSORS.get(&handle).is_none(),
            "close must drop the cursor, releasing the retained Arrow buffers"
        );
        // Closing twice is a no-op, so a Java close in a finally block is safe.
        assert_eq!(unsafe { parquet_df_close_iter(handle) }, RC_OK);
    }

    #[test]
    fn every_entry_point_rejects_an_unknown_handle() {
        let unknown = i64::MAX;
        for message in [
            error_message(next_batch(unknown, 0).rc),
            error_message(unsafe { parquet_df_reset_iter(unknown) }),
        ] {
            assert!(message.contains("unknown handle"), "{message}");
        }
    }

    #[test]
    fn a_closed_handle_is_no_longer_usable() {
        let file = fixture_file();
        let handle = open_fixture(&file);
        assert_eq!(unsafe { parquet_df_close_iter(handle) }, RC_OK);

        let message = error_message(next_batch(handle, 0).rc);
        assert!(message.contains("unknown handle"), "{message}");
    }

    #[test]
    fn an_out_of_range_initial_window_is_rejected() {
        for initial in [0, -1, BATCH_SIZE_HARD_LIMIT as i64 + 1] {
            // Rejected before the file is touched, so the path is irrelevant.
            let message = error_message(open_iter("/nonexistent/never-opened.parquet", initial));
            assert!(
                message.contains("initial batch size"),
                "initial {initial} produced {message}"
            );
        }
    }

    #[test]
    fn a_missing_column_is_reported_without_leaving_a_handle_behind() {
        register_test_metadata_cache();
        let file = fixture_file();
        let path = file.path().to_str().unwrap();
        let column = "absent";
        let before = CURSORS.len();
        let rc = unsafe {
            parquet_df_open_iter(
                path.as_ptr(),
                path.len() as i64,
                column.as_ptr(),
                column.len() as i64,
                8,
                TEST_MAX_BATCH_SIZE as i64,
            )
        };
        let message = error_message(rc);
        assert!(message.contains("not found"), "{message}");
        assert_eq!(
            CURSORS.len(),
            before,
            "a failed open must not register a cursor"
        );
    }
}
