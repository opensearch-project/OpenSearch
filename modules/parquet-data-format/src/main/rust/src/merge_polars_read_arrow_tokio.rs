use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use polars::prelude::*;
use polars::datatypes::DataType;
use polars_core::utils::concat_df;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

// arrow-rs imports
use arrow::array::{ArrayRef, RecordBatch, make_array};
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};

// parquet low-level parallel writer imports
use parquet::arrow::arrow_writer::{ArrowRowGroupWriterFactory, compute_leaves};
use parquet::basic::{Compression, Repetition};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;

// tokio for parallel column encoding
use tokio::runtime::Runtime;

// polars-arrow (arrow2 fork)
use polars_arrow;

const ROW_ID_COLUMN_NAME: &str = "___row_id";
const BATCH_SIZE: usize = 50_000;
const OUTPUT_FLUSH_ROWS: usize = 100_000;

// =============================================================================
// Timing accumulator
// =============================================================================

struct MergeTimings {
    parquet_read: std::time::Duration,
    take_slice: std::time::Duration,
    rechunk_slice: std::time::Duration,
    concat_df: std::time::Duration,
    rechunk_flush: std::time::Duration,
    add_row_id: std::time::Duration,
    ffi_convert: std::time::Duration,
    parallel_encode: std::time::Duration,
    rg_append_close: std::time::Duration,
    heap_ops: std::time::Duration,
    sort_value_lookups: std::time::Duration,
    parquet_read_count: usize,
    take_slice_count: usize,
    flush_count: usize,
    ffi_convert_count: usize,
    encode_count: usize,
    sort_lookup_count: usize,
    total_rows_read: usize,
    total_rows_sliced: usize,
    total_rows_written: usize,
}

impl MergeTimings {
    fn new() -> Self {
        Self {
            parquet_read: std::time::Duration::ZERO,
            take_slice: std::time::Duration::ZERO,
            rechunk_slice: std::time::Duration::ZERO,
            concat_df: std::time::Duration::ZERO,
            rechunk_flush: std::time::Duration::ZERO,
            add_row_id: std::time::Duration::ZERO,
            ffi_convert: std::time::Duration::ZERO,
            parallel_encode: std::time::Duration::ZERO,
            rg_append_close: std::time::Duration::ZERO,
            heap_ops: std::time::Duration::ZERO,
            sort_value_lookups: std::time::Duration::ZERO,
            parquet_read_count: 0,
            take_slice_count: 0,
            flush_count: 0,
            ffi_convert_count: 0,
            encode_count: 0,
            sort_lookup_count: 0,
            total_rows_read: 0,
            total_rows_sliced: 0,
            total_rows_written: 0,
        }
    }

    fn print_summary(&self, total_elapsed: std::time::Duration) {
        println!("\n{}", "=".repeat(70));
        println!("MERGE TIMING BREAKDOWN");
        println!("{}", "=".repeat(70));
        println!("Total wall time:        {:>10.3}s", total_elapsed.as_secs_f64());
        println!();
        println!("--- READ (Polars ParquetReader) ---");
        println!("  parquet_read:         {:>10.3}s  ({} calls, {} rows)",
                 self.parquet_read.as_secs_f64(), self.parquet_read_count, self.total_rows_read);
        if self.total_rows_read > 0 && self.parquet_read.as_secs_f64() > 0.0 {
            println!("  read throughput:      {:>10.0} rows/sec",
                     self.total_rows_read as f64 / self.parquet_read.as_secs_f64());
        }
        println!();
        println!("--- SLICE ---");
        println!("  take_slice (offset):  {:>10.3}s  ({} calls, {} rows)",
                 self.take_slice.as_secs_f64(), self.take_slice_count, self.total_rows_sliced);
        println!("  rechunk_slice:        {:>10.3}s",
                 self.rechunk_slice.as_secs_f64());
        println!("  slice total:          {:>10.3}s",
                 (self.take_slice + self.rechunk_slice).as_secs_f64());
        println!();
        println!("--- FLUSH (concat + rechunk + row_id) ---");
        println!("  concat_df:            {:>10.3}s  ({} flushes)",
                 self.concat_df.as_secs_f64(), self.flush_count);
        println!("  rechunk_flush:        {:>10.3}s",
                 self.rechunk_flush.as_secs_f64());
        println!("  add_row_id:           {:>10.3}s",
                 self.add_row_id.as_secs_f64());
        println!("  flush total:          {:>10.3}s",
                 (self.concat_df + self.rechunk_flush + self.add_row_id).as_secs_f64());
        println!();
        println!("--- FFI CONVERSION (Polars → arrow-rs) ---");
        println!("  ffi_convert:          {:>10.3}s  ({} calls)",
                 self.ffi_convert.as_secs_f64(), self.ffi_convert_count);
        println!();
        println!("--- WRITE (Parallel Column Encoding via tokio) ---");
        println!("  parallel_encode:      {:>10.3}s  ({} row groups, {} rows)",
                 self.parallel_encode.as_secs_f64(), self.encode_count, self.total_rows_written);
        println!("  rg_append_close:      {:>10.3}s",
                 self.rg_append_close.as_secs_f64());
        if self.total_rows_written > 0 && self.parallel_encode.as_secs_f64() > 0.0 {
            println!("  encode throughput:    {:>10.0} rows/sec",
                     self.total_rows_written as f64 / self.parallel_encode.as_secs_f64());
        }
        println!();
        println!("--- MERGE LOGIC ---");
        println!("  heap_ops:             {:>10.3}s",
                 self.heap_ops.as_secs_f64());
        println!("  sort_value_lookups:   {:>10.3}s  ({} lookups)",
                 self.sort_value_lookups.as_secs_f64(), self.sort_lookup_count);
        println!();

        let accounted = self.parquet_read + self.take_slice + self.rechunk_slice
            + self.concat_df + self.rechunk_flush + self.add_row_id
            + self.ffi_convert + self.parallel_encode + self.rg_append_close
            + self.heap_ops + self.sort_value_lookups;
        let unaccounted = if total_elapsed > accounted {
            total_elapsed - accounted
        } else {
            std::time::Duration::ZERO
        };

        println!("--- SUMMARY ---");
        println!("  accounted:            {:>10.3}s  ({:.1}%)",
                 accounted.as_secs_f64(),
                 accounted.as_secs_f64() / total_elapsed.as_secs_f64() * 100.0);
        println!("  unaccounted:          {:>10.3}s  ({:.1}%)",
                 unaccounted.as_secs_f64(),
                 unaccounted.as_secs_f64() / total_elapsed.as_secs_f64() * 100.0);

        println!();
        println!("--- PERCENTAGE BREAKDOWN ---");
        let t = total_elapsed.as_secs_f64();
        println!("  parquet_read:         {:>6.1}%", self.parquet_read.as_secs_f64() / t * 100.0);
        println!("  take_slice+rechunk:   {:>6.1}%", (self.take_slice + self.rechunk_slice).as_secs_f64() / t * 100.0);
        println!("  concat_df:            {:>6.1}%", self.concat_df.as_secs_f64() / t * 100.0);
        println!("  rechunk_flush:        {:>6.1}%", self.rechunk_flush.as_secs_f64() / t * 100.0);
        println!("  add_row_id:           {:>6.1}%", self.add_row_id.as_secs_f64() / t * 100.0);
        println!("  ffi_convert:          {:>6.1}%", self.ffi_convert.as_secs_f64() / t * 100.0);
        println!("  parallel_encode:      {:>6.1}%", self.parallel_encode.as_secs_f64() / t * 100.0);
        println!("  rg_append_close:      {:>6.1}%", self.rg_append_close.as_secs_f64() / t * 100.0);
        println!("  heap_ops:             {:>6.1}%", self.heap_ops.as_secs_f64() / t * 100.0);
        println!("  sort_lookups:         {:>6.1}%", self.sort_value_lookups.as_secs_f64() / t * 100.0);
        println!("  unaccounted:          {:>6.1}%", unaccounted.as_secs_f64() / t * 100.0);
        println!("{}\n", "=".repeat(70));
    }
}

// =============================================================================
// Error conversion helpers
// =============================================================================

fn parquet_to_polars_err(e: parquet::errors::ParquetError) -> PolarsError {
    PolarsError::IO {
        error: Arc::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())),
        msg: None,
    }
}

fn arrow_to_polars_err(e: arrow::error::ArrowError) -> PolarsError {
    PolarsError::ComputeError(format!("Arrow error: {e}").into())
}

// =============================================================================
// Build output parquet schema from input file + add ___row_id
// =============================================================================

fn build_output_schemas(
    first_input_path: &str,
    first_batch: &DataFrame,
) -> PolarsResult<(Arc<Type>, Arc<ArrowSchema>)> {
    let input_file = File::open(first_input_path).map_err(|e| PolarsError::IO {
        error: e.into(),
        msg: None,
    })?;
    let reader =
        SerializedFileReader::new(input_file).map_err(parquet_to_polars_err)?;
    let input_schema_descr = reader.metadata().file_metadata().schema_descr_ptr();
    let input_root = input_schema_descr.root_schema();

    let mut parquet_fields: Vec<Arc<Type>> = Vec::new();
    for field in input_root.get_fields() {
        if field.name() != ROW_ID_COLUMN_NAME {
            parquet_fields.push(Arc::new(field.as_ref().clone()));
        }
    }

    let row_id_type = Type::primitive_type_builder(ROW_ID_COLUMN_NAME, parquet::basic::Type::INT64)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .map_err(parquet_to_polars_err)?;
    parquet_fields.push(Arc::new(row_id_type));

    let parquet_root = Type::group_type_builder("schema")
        .with_fields(parquet_fields)
        .build()
        .map_err(parquet_to_polars_err)?;
    let parquet_root_ptr = Arc::new(parquet_root);

    let base_arrow = build_arrow_schema_from_df(first_batch)?;
    let mut arrow_fields: Vec<ArrowField> = base_arrow
        .fields()
        .iter()
        .filter(|f| f.name() != ROW_ID_COLUMN_NAME)
        .map(|f| f.as_ref().clone())
        .collect();
    arrow_fields.push(ArrowField::new(
        ROW_ID_COLUMN_NAME,
        arrow::datatypes::DataType::Int64,
        false,
    ));
    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

    Ok((parquet_root_ptr, arrow_schema))
}

// =============================================================================
// FileCursor — reads BATCH_SIZE rows at a time via with_slice
// =============================================================================

struct FileCursor {
    path: String,
    rows_read: usize,
    current_batch: Option<DataFrame>,
    row_idx: usize,
    file_id: usize,
    sort_column: String,
    batch_size: usize,
}

impl FileCursor {
    fn new(
        path: String,
        file_id: usize,
        sort_column: String,
        batch_size: usize,
    ) -> PolarsResult<Option<Self>> {
        let mut cursor = Self {
            path,
            rows_read: 0,
            current_batch: None,
            row_idx: 0,
            file_id,
            sort_column,
            batch_size,
        };

        if !cursor.load_next_batch_timed(&mut None)? {
            return Ok(None);
        }
        Ok(Some(cursor))
    }

    fn load_next_batch_timed(
        &mut self,
        timings: &mut Option<&mut MergeTimings>,
    ) -> PolarsResult<bool> {
        self.current_batch = None;

        let t0 = Instant::now();

        let file = File::open(&self.path).map_err(|e| PolarsError::IO {
            error: e.into(),
            msg: None,
        })?;

        let mut df = ParquetReader::new(file)
            .with_slice(Some((self.rows_read, self.batch_size)))
            .finish()?;

        let _ = df.drop_in_place(ROW_ID_COLUMN_NAME);

        let elapsed = t0.elapsed();

        if df.height() == 0 {
            if let Some(t) = timings {
                t.parquet_read += elapsed;
                t.parquet_read_count += 1;
            }
            return Ok(false);
        }

        let rows = df.height();
        self.rows_read += rows;
        self.current_batch = Some(df);
        self.row_idx = 0;

        if let Some(t) = timings {
            t.parquet_read += elapsed;
            t.parquet_read_count += 1;
            t.total_rows_read += rows;
        }

        Ok(true)
    }

    fn load_next_batch(&mut self) -> PolarsResult<bool> {
        self.load_next_batch_timed(&mut None)
    }

    fn current_sort_value(&self) -> PolarsResult<i64> {
        match &self.current_batch {
            Some(batch) => get_sort_value(batch, self.row_idx, &self.sort_column),
            None => Err(PolarsError::NoData("Cursor exhausted".into())),
        }
    }

    fn last_sort_value(&self) -> PolarsResult<i64> {
        match &self.current_batch {
            Some(batch) => get_sort_value(batch, batch.height() - 1, &self.sort_column),
            None => Err(PolarsError::NoData("Cursor exhausted".into())),
        }
    }

    fn batch_height(&self) -> usize {
        self.current_batch.as_ref().map_or(0, |b| b.height())
    }

    fn take_slice_timed(
        &self,
        start: usize,
        len: usize,
        timings: &mut MergeTimings,
    ) -> DataFrame {
        let t0 = Instant::now();
        let mut slice = self
            .current_batch
            .as_ref()
            .unwrap()
            .slice(start as i64, len);
        let t1 = Instant::now();
        timings.take_slice += t1 - t0;

        slice.rechunk_mut();
        let t2 = Instant::now();
        timings.rechunk_slice += t2 - t1;

        timings.take_slice_count += 1;
        timings.total_rows_sliced += len;

        slice
    }

    fn take_slice(&self, start: usize, len: usize) -> DataFrame {
        let mut slice = self
            .current_batch
            .as_ref()
            .unwrap()
            .slice(start as i64, len);
        slice.rechunk_mut();
        slice
    }

    fn advance_timed(&mut self, timings: &mut MergeTimings) -> PolarsResult<bool> {
        if self.current_batch.is_none() {
            return Ok(false);
        }

        self.row_idx += 1;

        let batch_height = self.current_batch.as_ref().unwrap().height();
        if self.row_idx >= batch_height {
            self.current_batch = None;
            return self.load_next_batch_timed(&mut Some(timings));
        }
        Ok(true)
    }

    fn advance(&mut self) -> PolarsResult<bool> {
        if self.current_batch.is_none() {
            return Ok(false);
        }

        self.row_idx += 1;

        let batch_height = self.current_batch.as_ref().unwrap().height();
        if self.row_idx >= batch_height {
            self.current_batch = None;
            return self.load_next_batch();
        }
        Ok(true)
    }

    fn advance_past_batch_timed(
        &mut self,
        timings: &mut MergeTimings,
    ) -> PolarsResult<bool> {
        self.current_batch = None;
        self.load_next_batch_timed(&mut Some(timings))
    }

    fn advance_past_batch(&mut self) -> PolarsResult<bool> {
        self.current_batch = None;
        self.load_next_batch()
    }

    fn is_exhausted(&self) -> bool {
        self.current_batch.is_none()
    }
}

// =============================================================================
// HeapItem for min-heap (ascending sort)
// =============================================================================

#[derive(Debug)]
struct HeapItem {
    sort_value: i64,
    file_id: usize,
}

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_value == other.sort_value
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other.sort_value.cmp(&self.sort_value)
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// =============================================================================
// Polars → arrow-rs conversion via Arrow C Data Interface (FFI)
// =============================================================================

fn polars_array_to_arrow_rs(
    polars_arr: Box<dyn polars_arrow::array::Array>,
    field: &polars_arrow::datatypes::Field,
) -> PolarsResult<ArrayRef> {
    let pa_c_schema = polars_arrow::ffi::export_field_to_c(field);
    let pa_c_array = polars_arrow::ffi::export_array_to_c(polars_arr);

    assert_eq!(
        std::mem::size_of::<polars_arrow::ffi::ArrowSchema>(),
        std::mem::size_of::<FFI_ArrowSchema>(),
        "ArrowSchema FFI struct size mismatch between polars-arrow and arrow-rs"
    );
    assert_eq!(
        std::mem::size_of::<polars_arrow::ffi::ArrowArray>(),
        std::mem::size_of::<FFI_ArrowArray>(),
        "ArrowArray FFI struct size mismatch between polars-arrow and arrow-rs"
    );

    let ffi_schema: FFI_ArrowSchema = unsafe { std::mem::transmute(pa_c_schema) };
    let ffi_array: FFI_ArrowArray = unsafe { std::mem::transmute(pa_c_array) };

    let array_data =
        unsafe { from_ffi(ffi_array, &ffi_schema) }.map_err(arrow_to_polars_err)?;

    Ok(make_array(array_data))
}

fn polars_field_to_arrow_field(
    field: &polars_arrow::datatypes::Field,
) -> PolarsResult<ArrowField> {
    let pa_c_schema = polars_arrow::ffi::export_field_to_c(field);
    let ffi_schema: FFI_ArrowSchema = unsafe { std::mem::transmute(pa_c_schema) };
    ArrowField::try_from(&ffi_schema).map_err(arrow_to_polars_err)
}

fn build_arrow_schema_from_df(df: &DataFrame) -> PolarsResult<Arc<ArrowSchema>> {
    let mut fields: Vec<ArrowField> = Vec::with_capacity(df.width());

    for col in df.get_columns() {
        let arrow_dtype = col.dtype().to_arrow(CompatLevel::oldest());
        let pa_field = polars_arrow::datatypes::Field::new(
            col.name().clone(),
            arrow_dtype,
            true,
        );
        fields.push(polars_field_to_arrow_field(&pa_field)?);
    }

    Ok(Arc::new(ArrowSchema::new(fields)))
}

fn polars_df_to_record_batch(
    df: &DataFrame,
    schema: &Arc<ArrowSchema>,
) -> PolarsResult<RecordBatch> {
    let pa_fields: Vec<polars_arrow::datatypes::Field> = df
        .get_columns()
        .iter()
        .map(|col| {
            let arrow_dtype = col.dtype().to_arrow(CompatLevel::oldest());
            polars_arrow::datatypes::Field::new(col.name().clone(), arrow_dtype, true)
        })
        .collect();

    let chunk = df
        .iter_chunks(CompatLevel::oldest(), false)
        .next()
        .ok_or_else(|| PolarsError::NoData("Empty DataFrame".into()))?;

    let arrays = chunk.into_arrays();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrays.len());

    for (arr, field) in arrays.into_iter().zip(pa_fields.iter()) {
        columns.push(polars_array_to_arrow_rs(arr, field)?);
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(arrow_to_polars_err)
}

// =============================================================================
// JNI Entry Point
// =============================================================================

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesInRust(
    mut env: JNIEnv,
    _class: JClass,
    input_files: JObject,
    output_file: JString,
    sort_column: JString,
    is_reverse: jint,
) -> jint {
    let input_files_vec = match convert_java_list_to_vec(&mut env, input_files) {
        Ok(v) => v,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };
    let output_path: String = match env.get_string(&output_file) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };
    let sort_col: String = match env.get_string(&sort_column) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };
    let _reverse = is_reverse != 0;

    match merge_streaming(&input_files_vec, &output_path, &sort_col) {
        Ok(_) => 0,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", format!("{:?}", e));
            -1
        }
    }
}

// =============================================================================
// STREAMING K-WAY MERGE (with parallel column encoding via tokio)
// =============================================================================

pub fn merge_streaming(
    input_files: &[String],
    output_path: &str,
    sort_column: &str,
) -> PolarsResult<()> {
    merge_streaming_with_config(
        input_files,
        output_path,
        sort_column,
        BATCH_SIZE,
        OUTPUT_FLUSH_ROWS,
    )
}

pub fn merge_streaming_with_config(
    input_files: &[String],
    output_path: &str,
    sort_column: &str,
    batch_size: usize,
    output_flush_rows: usize,
) -> PolarsResult<()> {
    if input_files.is_empty() {
        return Ok(());
    }

    let total_start = Instant::now();
    let mut timings = MergeTimings::new();

    println!("Starting streaming merge of {} files", input_files.len());

    if let Some(parent) = Path::new(output_path).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| PolarsError::IO {
                error: e.into(),
                msg: None,
            })?;
        }
    }

    // ── Build tokio runtime (reused across all flushes) ─────────────────
    let rt = Runtime::new().map_err(|e| PolarsError::ComputeError(
        format!("Failed to create tokio runtime: {e}").into(),
    ))?;

    // ── Initialise cursors (indexed by file_id) ─────────────────────────
    let mut cursors: Vec<Option<FileCursor>> = Vec::with_capacity(input_files.len());

    for (file_id, path) in input_files.iter().enumerate() {
        println!("Initializing cursor for file {}: {}", file_id, path);

        let mut cursor_partial = FileCursor {
            path: path.clone(),
            rows_read: 0,
            current_batch: None,
            row_idx: 0,
            file_id,
            sort_column: sort_column.to_string(),
            batch_size,
        };
        let loaded = cursor_partial.load_next_batch_timed(&mut Some(&mut timings))?;

        if loaded {
            cursors.push(Some(cursor_partial));
        } else {
            cursors.push(None);
            eprintln!("Skipping empty file: {}", path);
        }
    }

    let active_count = cursors.iter().filter(|c| c.is_some()).count();
    if active_count == 0 {
        println!("All files were empty");
        timings.print_summary(total_start.elapsed());
        return Ok(());
    }

    // Validate sort column exists in every active cursor
    for (i, cursor_opt) in cursors.iter().enumerate() {
        if let Some(cursor) = cursor_opt {
            let batch = cursor.current_batch.as_ref().unwrap();
            if !batch.schema().contains(sort_column) {
                let cols: Vec<_> =
                    batch.schema().iter().map(|(n, _)| n.to_string()).collect();
                return Err(PolarsError::ColumnNotFound(
                    format!(
                        "Sort column '{}' not found in file {} (cursor {}). Available: {:?}",
                        sort_column,
                        input_files.get(i).unwrap_or(&"unknown".to_string()),
                        i,
                        cols
                    )
                        .into(),
                ));
            }
        }
    }

    // ── Build output schemas ────────────────────────────────────────────
    let first_active_idx = cursors
        .iter()
        .position(|c| c.is_some())
        .unwrap();
    let first_batch = cursors[first_active_idx]
        .as_ref()
        .unwrap()
        .current_batch
        .as_ref()
        .unwrap();
    let first_input_path = &input_files[first_active_idx];

    let (parquet_root, output_arrow_schema) =
        build_output_schemas(first_input_path, first_batch)?;

    println!(
        "Initialized {} active cursors, starting merge",
        active_count
    );

    // ── Open SerializedFileWriter (low-level) ───────────────────────────
    let output_file = File::create(output_path).map_err(|e| PolarsError::IO {
        error: e.into(),
        msg: None,
    })?;

    let writer_props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_row_count(Some(1_00_000))
            .set_write_batch_size(50_000)
            .build(),
    );

    let mut writer =
        SerializedFileWriter::new(output_file, parquet_root, writer_props.clone())
            .map_err(parquet_to_polars_err)?;

    let mut row_group_index: usize = 0;

    // ── Seed min-heap ───────────────────────────────────────────────────
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(active_count);
    for cursor_opt in &cursors {
        if let Some(cursor) = cursor_opt {
            let t0 = Instant::now();
            let sv = cursor.current_sort_value()?;
            timings.sort_value_lookups += t0.elapsed();
            timings.sort_lookup_count += 1;

            let t0 = Instant::now();
            heap.push(HeapItem {
                sort_value: sv,
                file_id: cursor.file_id,
            });
            timings.heap_ops += t0.elapsed();
        }
    }

    // ── Output buffer ───────────────────────────────────────────────────
    let mut output_chunks: Vec<DataFrame> = Vec::new();
    let mut output_row_count = 0usize;
    let mut next_row_id = 0i64;
    let sort_col_owned = sort_column.to_string();

    // ── Flush helper (parallel column encoding via tokio) ───────────────
    macro_rules! flush {
        () => {
            if !output_chunks.is_empty() {
                // 1. Concat accumulated slices
                let t_concat = Instant::now();
                let mut df = concat_df(output_chunks.as_slice())?;
                output_chunks.clear();
                timings.concat_df += t_concat.elapsed();

                // 2. Rechunk into contiguous memory
                let t_rechunk = Instant::now();
                df.rechunk_mut();
                timings.rechunk_flush += t_rechunk.elapsed();

                let n = df.height();

                // 3. Append ___row_id column
                let t_rowid = Instant::now();
                let row_ids: Vec<i64> = (next_row_id..next_row_id + n as i64).collect();
                let row_id_series = Series::new(ROW_ID_COLUMN_NAME.into(), row_ids);
                df.with_column(row_id_series)?;
                timings.add_row_id += t_rowid.elapsed();

                // 4. FFI convert Polars DF → arrow-rs RecordBatch
                let t_ffi = Instant::now();
                let record_batch =
                    polars_df_to_record_batch(&df, &output_arrow_schema)?;
                drop(df);
                timings.ffi_convert += t_ffi.elapsed();
                timings.ffi_convert_count += 1;

                // 5. Create per-column writers for this row group
                let col_writers = {
                    let factory = ArrowRowGroupWriterFactory::new(
                        &writer,
                        output_arrow_schema.clone(),
                    );
                    factory
                        .create_column_writers(row_group_index)
                        .map_err(parquet_to_polars_err)?
                };

                // 6. Compute leaf columns and pair with writers
                let mut leaves_and_writers = Vec::new();
                {
                    let mut writer_iter = col_writers.into_iter();
                    for (arr, field) in record_batch
                        .columns()
                        .iter()
                        .zip(output_arrow_schema.fields())
                    {
                        for leaf in
                            compute_leaves(field, arr).map_err(parquet_to_polars_err)?
                        {
                            let col_writer = writer_iter.next().ok_or_else(|| {
                                PolarsError::ComputeError(
                                    "Fewer column writers than leaf columns".into(),
                                )
                            })?;
                            leaves_and_writers.push((leaf, col_writer));
                        }
                    }
                }

                // 7. Encode ALL columns in parallel using tokio spawn_blocking.
                //    Each task is CPU-bound so spawn_blocking is the right fit.
                //    We spawn all tasks first, then await handles in order to
                //    preserve the original column ordering.
                let t_encode = Instant::now();
                let chunk_results: Vec<
                    Result<
                        parquet::arrow::arrow_writer::ArrowColumnChunk,
                        parquet::errors::ParquetError,
                    >,
                > = rt.block_on(async {
                    let mut handles = Vec::with_capacity(leaves_and_writers.len());

                    for (leaf, mut col_writer) in leaves_and_writers {
                        let handle = tokio::task::spawn_blocking(move || {
                            col_writer.write(&leaf)?;
                            col_writer.close()
                        });
                        handles.push(handle);
                    }

                    let mut results = Vec::with_capacity(handles.len());
                    for handle in handles {
                        match handle.await {
                            Ok(result) => results.push(result),
                            Err(join_err) => results.push(Err(
                                parquet::errors::ParquetError::General(
                                    format!("Tokio task join error: {join_err}")
                                )
                            )),
                        }
                    }
                    results
                });
                timings.parallel_encode += t_encode.elapsed();
                timings.encode_count += 1;

                // 8. Open row group, append encoded chunks, close
                let t_rg = Instant::now();
                let mut rg_writer =
                    writer.next_row_group().map_err(parquet_to_polars_err)?;

                for chunk_result in chunk_results {
                    let chunk = chunk_result.map_err(parquet_to_polars_err)?;
                    chunk
                        .append_to_row_group(&mut rg_writer)
                        .map_err(parquet_to_polars_err)?;
                }
                rg_writer.close().map_err(parquet_to_polars_err)?;
                timings.rg_append_close += t_rg.elapsed();

                row_group_index += 1;
                next_row_id += n as i64;
                timings.flush_count += 1;
                timings.total_rows_written += n;
                output_row_count = 0;
            }
        };
    }

    // =====================================================================
    // K-way merge loop — three-tier cascade
    // =====================================================================
    while let Some(item) = {
        let t0 = Instant::now();
        let item = heap.pop();
        timings.heap_ops += t0.elapsed();
        item
    } {
        let file_id = item.file_id;

        // TIER 1: only one cursor left → dump everything
        if heap.is_empty() {
            let cursor = cursors[file_id].as_mut().unwrap();
            loop {
                let remaining = cursor.batch_height() - cursor.row_idx;
                if remaining > 0 {
                    let slice =
                        cursor.take_slice_timed(cursor.row_idx, remaining, &mut timings);
                    output_row_count += remaining;
                    output_chunks.push(slice);

                    if output_row_count >= output_flush_rows {
                        flush!();
                    }
                }
                if !cursor.advance_past_batch_timed(&mut timings)? {
                    break;
                }
            }
            break;
        }

        // TIER 2 & 3: multiple cursors active
        let cursor = cursors[file_id].as_mut().unwrap();

        loop {
            let t0 = Instant::now();
            let heap_top = heap.peek().unwrap().sort_value;
            timings.heap_ops += t0.elapsed();

            // TIER 2: entire remaining batch fits
            let t0 = Instant::now();
            let last_val = cursor.last_sort_value()?;
            timings.sort_value_lookups += t0.elapsed();
            timings.sort_lookup_count += 1;

            if last_val <= heap_top {
                let remaining = cursor.batch_height() - cursor.row_idx;
                let slice =
                    cursor.take_slice_timed(cursor.row_idx, remaining, &mut timings);
                output_chunks.push(slice);
                output_row_count += remaining;

                if output_row_count >= output_flush_rows {
                    flush!();
                }

                if !cursor.advance_past_batch_timed(&mut timings)? {
                    break;
                }
                continue;
            }

            // TIER 3: binary search for cut point
            let run_start = cursor.row_idx;
            let batch_h = cursor.batch_height();
            let batch = cursor.current_batch.as_ref().unwrap();

            let mut lo = run_start;
            let mut hi = batch_h - 1;

            while lo + 1 < hi {
                let mid = lo + (hi - lo) / 2;
                let t0 = Instant::now();
                let mid_val = get_sort_value(batch, mid, &sort_col_owned)?;
                timings.sort_value_lookups += t0.elapsed();
                timings.sort_lookup_count += 1;

                if mid_val <= heap_top {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            let run_end = lo;

            let run_len = run_end - run_start + 1;
            let slice = cursor.take_slice_timed(run_start, run_len, &mut timings);
            output_chunks.push(slice);
            output_row_count += run_len;

            if output_row_count >= output_flush_rows {
                flush!();
            }

            cursor.row_idx = run_end;
            if !cursor.advance_timed(&mut timings)? {
                break;
            }

            let t0 = Instant::now();
            let next_val = cursor.current_sort_value()?;
            timings.sort_value_lookups += t0.elapsed();
            timings.sort_lookup_count += 1;

            if next_val > heap_top {
                let t0 = Instant::now();
                heap.push(HeapItem {
                    sort_value: next_val,
                    file_id,
                });
                timings.heap_ops += t0.elapsed();
                break;
            }
        }
    }

    // Final flush
    flush!();

    // Shut down the tokio runtime cleanly
    rt.shutdown_background();

    let t_close = Instant::now();
    let _metadata = writer.close().map_err(parquet_to_polars_err)?;
    let close_elapsed = t_close.elapsed();
    println!("Writer.close():         {:>10.3}s", close_elapsed.as_secs_f64());

    let total_elapsed = total_start.elapsed();
    timings.print_summary(total_elapsed);

    println!(
        "Merge complete: {} total rows written",
        timings.total_rows_written
    );
    Ok(())
}

// =============================================================================
// Helpers
// =============================================================================

fn get_sort_value(df: &DataFrame, row: usize, col: &str) -> PolarsResult<i64> {
    let series = df.column(col)?;
    match series.dtype() {
        DataType::Int64 => series
            .i64()?
            .get(row)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::Int32 => series
            .i32()?
            .get(row)
            .map(|v| v as i64)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::UInt64 => series
            .u64()?
            .get(row)
            .map(|v| v as i64)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::UInt32 => series
            .u32()?
            .get(row)
            .map(|v| v as i64)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::Datetime(_, _) => series
            .datetime()?
            .phys
            .get(row)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::Date => series
            .date()?
            .phys
            .get(row)
            .map(|v| v as i64)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::Duration(_) => series
            .duration()?
            .phys
            .get(row)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        other => Err(PolarsError::InvalidOperation(
            format!("Unsupported sort type: {:?}", other).into(),
        )),
    }
}

fn convert_java_list_to_vec(
    env: &mut JNIEnv,
    list: JObject,
) -> Result<Vec<String>, Box<dyn Error>> {
    let size = env.call_method(&list, "size", "()I", &[])?.i()? as usize;
    let mut result = Vec::with_capacity(size);
    for i in 0..size {
        env.push_local_frame(4)?;
        let obj = env
            .call_method(
                &list,
                "get",
                "(I)Ljava/lang/Object;",
                &[(i as i32).into()],
            )?
            .l()?;
        let jstring = env
            .call_method(&obj, "toString", "()Ljava/lang/String;", &[])?
            .l()?;
        let rust_string: String = env.get_string(&jstring.into())?.into();
        result.push(rust_string);
        unsafe {
            env.pop_local_frame(&JObject::null())?;
        }
    }
    Ok(result)
}
