//! Benchmark for merge cursor projection optimization.
//!
//! Measures memory and throughput for sorted merge with:
//! - Wide schema (many string columns) → deferred mode should activate
//! - Narrow schema (few columns) → eager mode, no regression
//!
//! Run:
//!   cargo bench --bench merge_projection_bench
//!
//! Requires the crate to compile (including metrics.rs fix in CI).

use std::fs;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const ROWS_PER_FILE: usize = 100_000;
const NUM_FILES: usize = 5;
const BATCH_SIZE: usize = 8192;
const WIDE_STRING_COLUMNS: usize = 20;
const NARROW_STRING_COLUMNS: usize = 2;
const STRING_VALUE_LEN: usize = 100;

fn generate_parquet_file(
    path: &str,
    num_rows: usize,
    num_string_cols: usize,
    file_id: usize,
) {
    let mut fields = vec![
        Field::new("@timestamp", DataType::Int64, false),
    ];
    for i in 0..num_string_cols {
        fields.push(Field::new(format!("field_{}", i), DataType::Utf8, true));
    }
    let schema = Arc::new(ArrowSchema::new(fields));

    let file = fs::File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(num_rows))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let batch_rows = BATCH_SIZE.min(num_rows);
    let mut rows_written = 0;

    while rows_written < num_rows {
        let batch_len = batch_rows.min(num_rows - rows_written);

        // Timestamps: sorted, unique per file with offset to ensure interleaving
        let timestamps: Vec<i64> = (0..batch_len)
            .map(|i| ((rows_written + i) * NUM_FILES + file_id) as i64)
            .collect();
        let ts_array = Int64Array::from(timestamps);

        let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![Arc::new(ts_array)];

        // String columns: random-ish values of fixed length
        for col_idx in 0..num_string_cols {
            let values: Vec<String> = (0..batch_len)
                .map(|row| {
                    format!(
                        "{:0>width$}",
                        (rows_written + row) * 31 + col_idx * 7,
                        width = STRING_VALUE_LEN
                    )
                })
                .collect();
            columns.push(Arc::new(StringArray::from(values)));
        }

        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        writer.write(&batch).unwrap();
        rows_written += batch_len;
    }

    writer.close().unwrap();
}

fn measure_resident_bytes() -> usize {
    // Approximate: read from /proc/self/statm on Linux, or use jemalloc stats
    #[cfg(target_os = "linux")]
    {
        let statm = fs::read_to_string("/proc/self/statm").unwrap_or_default();
        let pages: usize = statm.split_whitespace().nth(1).unwrap_or("0").parse().unwrap_or(0);
        pages * 4096 // RSS in bytes (page size = 4KB)
    }
    #[cfg(not(target_os = "linux"))]
    {
        // macOS: use mach APIs or approximate with allocated bytes
        0
    }
}

fn run_merge_bench(label: &str, num_string_cols: usize) {
    let tmp_dir = tempfile::tempdir().unwrap();
    let tmp_path = tmp_dir.path();

    // Generate input files
    let mut input_paths = Vec::new();
    for i in 0..NUM_FILES {
        let path = tmp_path.join(format!("input_{}.parquet", i));
        let path_str = path.to_str().unwrap().to_string();
        generate_parquet_file(&path_str, ROWS_PER_FILE, num_string_cols, i);
        input_paths.push(path_str);
    }

    let output_path = tmp_path.join("merged_output.parquet");
    let output_str = output_path.to_str().unwrap();

    // Measure memory before
    let mem_before = measure_resident_bytes();

    // Run merge
    let start = Instant::now();

    let sort_columns = vec!["@timestamp".to_string()];
    let reverse_sorts = vec![false];
    let nulls_first = vec![false];

    let result = opensearch_parquet_format::merge::merge_sorted(
        &input_paths,
        output_str,
        "bench_index",
        &sort_columns,
        &reverse_sorts,
        &nulls_first,
        1, // output_writer_generation
    );

    let elapsed = start.elapsed();

    // Measure memory after (peak would require sampling during merge)
    let mem_after = measure_resident_bytes();

    let total_rows = ROWS_PER_FILE * NUM_FILES;
    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    let mb_per_sec = {
        let input_size: u64 = input_paths.iter()
            .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .sum();
        (input_size as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64()
    };

    let mem_delta_mb = (mem_after.saturating_sub(mem_before)) as f64 / 1024.0 / 1024.0;

    match result {
        Ok(output) => {
            println!("┌─────────────────────────────────────────────────────────────");
            println!("│ {} ", label);
            println!("├─────────────────────────────────────────────────────────────");
            println!("│ Files:         {} × {} rows = {} total rows", NUM_FILES, ROWS_PER_FILE, total_rows);
            println!("│ Columns:       1 sort (Int64) + {} string ({}B each)", num_string_cols, STRING_VALUE_LEN);
            println!("│ Batch size:    {}", BATCH_SIZE);
            println!("│ Elapsed:       {:.2?}", elapsed);
            println!("│ Throughput:    {:.0} rows/sec, {:.1} MB/sec (input)", rows_per_sec, mb_per_sec);
            println!("│ RSS delta:     {:.1} MB", mem_delta_mb);
            println!("│ Output rows:   {}", output.metadata.file_metadata().num_rows());
            println!("│ Output RGs:    {}", output.metadata.num_row_groups());
            println!("│ Deferred mode: {}", if num_string_cols >= 3 { "YES (expected)" } else { "NO (eager)" });
            println!("└─────────────────────────────────────────────────────────────");
            println!();
        }
        Err(e) => {
            println!("│ {} FAILED: {:?}", label, e);
        }
    }
}

fn main() {
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!(" Merge Projection Benchmark");
    println!(" Compares deferred (wide schema) vs eager (narrow schema)");
    println!("═══════════════════════════════════════════════════════════════");
    println!();

    // Wide schema: should trigger deferred mode (≥3 string non-sort columns)
    run_merge_bench(
        "WIDE SCHEMA (deferred mode — 20 string columns)",
        WIDE_STRING_COLUMNS,
    );

    // Narrow schema: should stay in eager mode (< 3 string non-sort columns)
    run_merge_bench(
        "NARROW SCHEMA (eager mode — 2 string columns)",
        NARROW_STRING_COLUMNS,
    );

    println!("═══════════════════════════════════════════════════════════════");
    println!(" Expected results:");
    println!("   Wide:   Lower RSS, slightly lower throughput vs old code");
    println!("   Narrow: Same RSS & throughput as old code (no regression)");
    println!("═══════════════════════════════════════════════════════════════");
}
