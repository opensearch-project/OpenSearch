//! Throughput benchmark for CrossRtStream.
//!
//! Measures how fast batches flow from the CPU executor through the mpsc channel
//! to the consumer. This directly measures the impact of channel capacity on
//! pipeline throughput.
//!
//! Run: cargo bench --bench cross_rt_throughput_bench

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{Int64Array, RecordBatch};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{stream, StreamExt};
use opensearch_datafusion::cross_rt_stream::CrossRtStream;
use opensearch_datafusion::executor::DedicatedExecutor;

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
        Field::new("d", DataType::Int64, false),
    ]))
}

fn test_batch(rows: usize) -> RecordBatch {
    let schema = test_schema();
    let col: Vec<i64> = (0..rows as i64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(col.clone())),
            Arc::new(Int64Array::from(col.clone())),
            Arc::new(Int64Array::from(col.clone())),
            Arc::new(Int64Array::from(col)),
        ],
    )
    .unwrap()
}

fn make_executor() -> DedicatedExecutor {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(4).enable_all();
    DedicatedExecutor::new("bench-cpu", builder)
}

/// Benchmark: push N batches through CrossRtStream and consume them.
/// This measures the channel overhead per batch — the difference between
/// channel(1) and channel(2) shows up as reduced stall time.
fn bench_cross_rt_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("cross_rt_throughput");

    for num_batches in [100, 1000, 5000] {
        let batch_size = 8192;
        let rows_total = num_batches * batch_size;
        group.throughput(Throughput::Elements(rows_total as u64));

        group.bench_with_input(
            BenchmarkId::new("batches", num_batches),
            &num_batches,
            |b, &n| {
                let exec = make_executor();
                let schema = test_schema();
                let batch = test_batch(batch_size);

                b.to_async(
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                        .unwrap(),
                )
                .iter(|| {
                    let exec = exec.clone();
                    let schema = schema.clone();
                    let batch = batch.clone();
                    async move {
                        let batches: Vec<Result<RecordBatch, DataFusionError>> =
                            (0..n).map(|_| Ok(batch.clone())).collect();

                        let inner = Box::pin(RecordBatchStreamAdapter::new(
                            schema.clone(),
                            stream::iter(batches),
                        ));

                        let cross =
                            CrossRtStream::new_with_df_error_stream(inner, exec.clone());
                        let wrapped = RecordBatchStreamAdapter::new(cross.schema(), cross);
                        tokio::pin!(wrapped);

                        let mut total_rows = 0u64;
                        while let Some(batch) = wrapped.next().await {
                            total_rows += batch.unwrap().num_rows() as u64;
                        }
                        total_rows
                    }
                });

                exec.join_blocking();
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_cross_rt_throughput);
criterion_main!(benches);
