//! Benchmark: sorted write with IPC vs Parquet chunk staging.
//!
//! Tests three scenarios:
//! 1. Standard (1.5M rows, 32MB threshold, 1M row groups) — multi-chunk finalize
//! 2. Many row groups (1.5M rows, 32MB threshold, 100K row groups) — exercises double-buffer pipeline
//! 3. Distribution comparison (near-sorted vs random)
//!
//! Run from workspace root:
//!   cargo bench --bench sorted_write_bench -p opensearch-parquet-format

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use tempfile::TempDir;

use opensearch_parquet_format::writer::NativeParquetWriter;
use opensearch_parquet_format::native_settings::NativeSettings;
use opensearch_parquet_format::SETTINGS_STORE;

fn batch_to_ffi(batch: &RecordBatch) -> (i64, i64) {
    let struct_array: StructArray = batch.clone().into();
    let array_data = struct_array.into_data();
    let ffi_array = FFI_ArrowArray::new(&array_data);
    let ffi_schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).unwrap();
    (Box::into_raw(Box::new(ffi_array)) as i64, Box::into_raw(Box::new(ffi_schema)) as i64)
}

fn schema_to_ffi(schema: &Schema) -> i64 {
    Box::into_raw(Box::new(FFI_ArrowSchema::try_from(schema).unwrap())) as i64
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("duration_ns", DataType::Int64, false),
        Field::new("http_url", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("__row_id__", DataType::Int64, false),
    ]))
}

/// Deterministic pseudo-random number from seed
fn pseudo_random(seed: u64) -> u64 {
    let mut x = seed;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// Generate batch with timestamps based on the specified distribution.
fn generate_batch(batch_idx: usize, rows: usize, distribution: &str) -> RecordBatch {
    let services = ["auth", "gateway", "worker", "scheduler", "metrics", "billing", "notify", "search"];
    let base_ts: i64 = 1700000000000;

    let timestamps: Vec<i64> = (0..rows).map(|i| {
        let global_idx = batch_idx * rows + i;
        match distribution {
            "sorted" => {
                base_ts + global_idx as i64 * 1000
            }
            "near_sorted" => {
                let is_displaced = pseudo_random((global_idx as u64) * 7 + 3) % 10 == 0;
                if is_displaced {
                    let offset = (pseudo_random((global_idx as u64) * 13 + 7) % 100000) as i64 - 50000;
                    base_ts + (global_idx as i64 + offset).max(0) * 1000
                } else {
                    base_ts + global_idx as i64 * 1000
                }
            }
            "random" => {
                let random_offset = pseudo_random((global_idx as u64) * 31 + 11) % 86_400_000;
                base_ts + random_offset as i64
            }
            _ => base_ts + global_idx as i64 * 1000,
        }
    }).collect();

    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(StringArray::from((0..rows).map(|i| {
                let idx = batch_idx * rows + i;
                format!("{:016x}{:016x}", idx as u64 * 7919, i as u64 * 6271)
            }).collect::<Vec<_>>())),
            Arc::new(StringArray::from((0..rows).map(|i| services[i % services.len()]).collect::<Vec<_>>())),
            Arc::new(Int64Array::from((0..rows).map(|i| ((i * 137 + batch_idx * 31) % 50000) as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from((0..rows).map(|i| {
                format!("https://api.example.com/v2/{}/res/{}", services[i % services.len()], i % 10000)
            }).collect::<Vec<_>>())),
            Arc::new(StringArray::from((0..rows).map(|i| {
                format!("usr_{:012x}", (batch_idx * rows + i) as u64 * 4219)
            }).collect::<Vec<_>>())),
            Arc::new(Int64Array::from((0..rows).map(|i| (batch_idx * rows + i) as i64).collect::<Vec<_>>())),
        ],
    )
    .unwrap()
}

struct BenchResult {
    write_phase_ms: f64,
    finalize_ms: f64,
    total_ms: f64,
}

fn run_sorted_write_with_rg(batches: &[RecordBatch], threshold: u64, use_ipc: bool, row_group_max_rows: Option<usize>) -> BenchResult {
    let tmp = TempDir::new().unwrap();
    let output_path = tmp.path().join("output.parquet").to_str().unwrap().to_string();
    let index_name = format!("bench_{}_{}_{}_{}", use_ipc, std::process::id(),
        pseudo_random(use_ipc as u64 * 99 + 1),
        row_group_max_rows.unwrap_or(0));

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["timestamp".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(threshold);
    settings.ipc_sorted_chunks = Some(use_ipc);
    if let Some(rg) = row_group_max_rows {
        settings.row_group_max_rows = Some(rg);
    }
    SETTINGS_STORE.insert(index_name.clone(), settings);

    let schema = test_schema();
    let schema_ptr = schema_to_ffi(&schema);

    NativeParquetWriter::create_writer(
        output_path.clone(),
        index_name.clone(),
        schema_ptr,
        vec!["timestamp".to_string()],
        vec![false],
        vec![false],
        0,
    ).expect("create writer");

    let write_start = Instant::now();
    for batch in batches {
        let (array_ptr, schema_ptr) = batch_to_ffi(batch);
        NativeParquetWriter::write_data(output_path.clone(), array_ptr, schema_ptr)
            .expect("write batch");
    }
    let write_phase = write_start.elapsed();

    let finalize_start = Instant::now();
    let result = NativeParquetWriter::finalize_writer(output_path).expect("finalize");
    let finalize_phase = finalize_start.elapsed();

    assert!(result.is_some());
    SETTINGS_STORE.remove(&index_name);

    BenchResult {
        write_phase_ms: write_phase.as_secs_f64() * 1000.0,
        finalize_ms: finalize_phase.as_secs_f64() * 1000.0,
        total_ms: (write_phase + finalize_phase).as_secs_f64() * 1000.0,
    }
}

fn run_sorted_write(batches: &[RecordBatch], threshold: u64, use_ipc: bool) -> BenchResult {
    run_sorted_write_with_rg(batches, threshold, use_ipc, None)
}

fn bench_by_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("sorted_write_by_distribution");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(30));

    let num_batches = 30;
    let rows_per_batch = 50_000;
    let total_rows = num_batches * rows_per_batch;
    let threshold: u64 = 32 * 1024 * 1024;

    group.throughput(Throughput::Elements(total_rows as u64));

    for distribution in ["near_sorted", "random"] {
        let batches: Vec<RecordBatch> = (0..num_batches)
            .map(|i| generate_batch(i, rows_per_batch, distribution))
            .collect();

        // Parquet chunks (baseline)
        group.bench_function(
            BenchmarkId::new(format!("parquet_{}", distribution), total_rows),
            |b| { b.iter(|| run_sorted_write(&batches, threshold, false)); },
        );

        // IPC chunks (optimization)
        group.bench_function(
            BenchmarkId::new(format!("ipc_{}", distribution), total_rows),
            |b| { b.iter(|| run_sorted_write(&batches, threshold, true)); },
        );
    }

    group.finish();

    // Print phase breakdowns — standard (1M row groups)
    for distribution in ["near_sorted", "random"] {
        let batches: Vec<RecordBatch> = (0..num_batches)
            .map(|i| generate_batch(i, rows_per_batch, distribution))
            .collect();

        let parquet_result = run_sorted_write(&batches, threshold, false);
        let ipc_result = run_sorted_write(&batches, threshold, true);

        eprintln!("\n=== {} data ({} rows, {}MB threshold, 1M row groups) ===",
            distribution, total_rows, threshold / 1024 / 1024);
        eprintln!("                    Parquet Chunks    IPC Chunks    Improvement");
        eprintln!("Write phase:        {:>8.1} ms      {:>8.1} ms      {:>+.1}%",
            parquet_result.write_phase_ms, ipc_result.write_phase_ms,
            (1.0 - ipc_result.write_phase_ms / parquet_result.write_phase_ms) * 100.0);
        eprintln!("Finalize phase:     {:>8.1} ms      {:>8.1} ms      {:>+.1}%",
            parquet_result.finalize_ms, ipc_result.finalize_ms,
            (1.0 - ipc_result.finalize_ms / parquet_result.finalize_ms) * 100.0);
        eprintln!("Total:              {:>8.1} ms      {:>8.1} ms      {:>+.1}%",
            parquet_result.total_ms, ipc_result.total_ms,
            (1.0 - ipc_result.total_ms / parquet_result.total_ms) * 100.0);
        eprintln!("Write throughput:   {:>8.1} MB/s    {:>8.1} MB/s",
            90.0 / (parquet_result.write_phase_ms / 1000.0),
            90.0 / (ipc_result.write_phase_ms / 1000.0));
    }

    // Print phase breakdowns — smaller row groups (100K) to exercise double-buffer pipeline
    eprintln!("\n\n========== DOUBLE-BUFFER PIPELINE (100K row groups) ==========");
    for distribution in ["near_sorted", "random"] {
        let batches: Vec<RecordBatch> = (0..num_batches)
            .map(|i| generate_batch(i, rows_per_batch, distribution))
            .collect();

        let parquet_result = run_sorted_write_with_rg(&batches, threshold, false, Some(100_000));
        let ipc_result = run_sorted_write_with_rg(&batches, threshold, true, Some(100_000));

        eprintln!("\n=== {} data ({} rows, {}MB threshold, 100K row groups = ~15 flushes) ===",
            distribution, total_rows, threshold / 1024 / 1024);
        eprintln!("                    Parquet Chunks    IPC Chunks    Improvement");
        eprintln!("Write phase:        {:>8.1} ms      {:>8.1} ms      {:>+.1}%",
            parquet_result.write_phase_ms, ipc_result.write_phase_ms,
            (1.0 - ipc_result.write_phase_ms / parquet_result.write_phase_ms) * 100.0);
        eprintln!("Finalize phase:     {:>8.1} ms      {:>8.1} ms      {:>+.1}%",
            parquet_result.finalize_ms, ipc_result.finalize_ms,
            (1.0 - ipc_result.finalize_ms / parquet_result.finalize_ms) * 100.0);
        eprintln!("Total:              {:>8.1} ms      {:>8.1} ms      {:>+.1}%",
            parquet_result.total_ms, ipc_result.total_ms,
            (1.0 - ipc_result.total_ms / parquet_result.total_ms) * 100.0);
        eprintln!("Write throughput:   {:>8.1} MB/s    {:>8.1} MB/s",
            90.0 / (parquet_result.write_phase_ms / 1000.0),
            90.0 / (ipc_result.write_phase_ms / 1000.0));
    }
}

criterion_group!(benches, bench_by_distribution);
criterion_main!(benches);
