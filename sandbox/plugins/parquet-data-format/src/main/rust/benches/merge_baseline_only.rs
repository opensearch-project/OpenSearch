use std::fs;
use std::sync::Arc;
use std::time::Instant;
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use opensearch_parquet_format::merge::merge_sorted;
use opensearch_parquet_format::native_settings::NativeSettings;
use opensearch_parquet_format::writer::SETTINGS_STORE;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

// Realistic OTel workload: time-partitioned files with overlapping tails.
// Each file covers a 10-minute window with ~80% sequential, ~20% overlap with neighbors.
const ROWS_PER_FILE: usize = 1_000_000;
const NUM_FILES: usize = 10;
const NUM_STRING_COLUMNS: usize = 30;
const STRING_VALUE_LEN: usize = 150;
const ROW_GROUP_ROWS: usize = 500_000; // 2 RGs per file

fn generate_parquet_file(path: &str, num_rows: usize, file_id: usize) {
    let mut fields = vec![Field::new("@timestamp", DataType::Int64, false)];
    for i in 0..NUM_STRING_COLUMNS {
        fields.push(Field::new(format!("field_{}", i), DataType::Utf8, true));
    }
    let schema = Arc::new(ArrowSchema::new(fields));
    let file = fs::File::create(path).unwrap();
    let props = WriterProperties::builder().set_max_row_group_row_count(Some(ROW_GROUP_ROWS)).build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let batch_size = 8192;
    let mut rows_written = 0;

    // Each file covers a time range with 20% overlap with the next file.
    // File 0: timestamps 0..1_000_000
    // File 1: timestamps 800_000..1_800_000 (200K overlap with file 0)
    // etc.
    let file_start = (file_id as i64) * (num_rows as i64 * 80 / 100);

    while rows_written < num_rows {
        let batch_len = batch_size.min(num_rows - rows_written);
        // Sorted within each file (realistic: ingestion order = time order)
        let timestamps: Vec<i64> = (0..batch_len)
            .map(|i| file_start + (rows_written + i) as i64)
            .collect();
        let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![Arc::new(Int64Array::from(timestamps))];
        for col_idx in 0..NUM_STRING_COLUMNS {
            let values: Vec<String> = (0..batch_len)
                .map(|row| format!("{:0>width$}", (rows_written + row) * 31 + col_idx * 7, width = STRING_VALUE_LEN))
                .collect();
            columns.push(Arc::new(StringArray::from(values)));
        }
        writer.write(&RecordBatch::try_new(schema.clone(), columns).unwrap()).unwrap();
        rows_written += batch_len;
    }
    writer.close().unwrap();
}

fn main() {
    let tmp_dir = tempfile::tempdir().unwrap();
    eprintln!("Generating {} files × {} rows × {} string cols ({}B)...",
        NUM_FILES, ROWS_PER_FILE, NUM_STRING_COLUMNS, STRING_VALUE_LEN);
    let mut input_paths = Vec::new();
    for i in 0..NUM_FILES {
        let path = tmp_dir.path().join(format!("input_{}.parquet", i));
        generate_parquet_file(path.to_str().unwrap(), ROWS_PER_FILE, i);
        input_paths.push(path.to_str().unwrap().to_string());
    }
    let input_size: u64 = input_paths.iter().map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0)).sum();
    eprintln!("Input ready: {:.1} MB", input_size as f64 / 1024.0 / 1024.0);

    // BASELINE: threshold=999 forces eager mode (all columns decoded upfront)
    SETTINGS_STORE.insert("bench".to_string(), NativeSettings {
        merge_batch_size: Some(8192),
        merge_deferred_column_threshold: Some(999),
        ..Default::default()
    });

    let output = tmp_dir.path().join("out.parquet");
    let start = Instant::now();
    let result = merge_sorted(&input_paths, output.to_str().unwrap(), "bench",
        &["@timestamp".to_string()], &[false], &[false], 1).unwrap();
    let elapsed = start.elapsed();

    let total_rows = ROWS_PER_FILE * NUM_FILES;
    eprintln!("BASELINE (eager): {:.2?}, {} rows/sec, {} output rows, {} RGs",
        elapsed,
        (total_rows as f64 / elapsed.as_secs_f64()) as u64,
        result.metadata.file_metadata().num_rows(),
        result.metadata.num_row_groups());
}
