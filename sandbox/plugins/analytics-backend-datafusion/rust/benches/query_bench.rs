use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use opensearch_datafusion_jni::query_executor;
use opensearch_datafusion_jni::runtime_manager::RuntimeManager;
use std::sync::Arc;
use opensearch_datafusion_jni::api::DataFusionRuntime;

fn create_test_parquet(dir: &std::path::Path, rows: usize) {
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int64Array, RecordBatch};
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let ids: Vec<i64> = (0..rows as i64).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(Int64Array::from(vals))],
    ).unwrap();

    let path = dir.join("bench.parquet");
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn setup() -> (RuntimeManager, DataFusionRuntime, tempfile::TempDir) {
    let mgr = RuntimeManager::new(4);
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(256 * 1024 * 1024)))
        .with_disk_manager_builder(DiskManagerBuilder::default())
        .build().unwrap();
    let df_runtime = DataFusionRuntime { runtime_env };
    let tmp = tempfile::tempdir().unwrap();
    (mgr, df_runtime, tmp)
}

fn get_substrait(mgr: &RuntimeManager, df: &DataFusionRuntime, dir: &str, sql: &str) -> Vec<u8> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    mgr.io_runtime.block_on(async {
        let ctx = datafusion::prelude::SessionContext::new();
        let url = ListingTableUrl::parse(dir).unwrap();
        let opts = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet").with_collect_stat(true);
        let schema = opts.infer_schema(&ctx.state(), &url).await.unwrap();
        let cfg = ListingTableConfig::new(url).with_listing_options(opts).with_schema(schema);
        ctx.register_table("t", Arc::new(ListingTable::try_new(cfg).unwrap())).unwrap();
        let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
        let sub = to_substrait_plan(&plan, &ctx.state()).unwrap();
        let mut buf = Vec::new();
        sub.encode(&mut buf).unwrap();
        buf
    })
}

fn get_metas(mgr: &RuntimeManager, dir: &str) -> Arc<Vec<object_store::ObjectMeta>> {
    let store = Arc::new(LocalFileSystem::new());
    let path = object_store::path::Path::from(format!("{}/bench.parquet", dir));
    let meta = mgr.io_runtime.block_on(store.head(&path)).unwrap();
    Arc::new(vec![meta])
}

fn bench_execute_query(c: &mut Criterion) {
    let (mgr, df_runtime, tmp) = setup();
    for rows in [100, 10_000, 100_000] {
        create_test_parquet(tmp.path(), rows);
        let dir = tmp.path().to_str().unwrap();
        let metas = get_metas(&mgr, dir);
        let plan = get_substrait(&mgr, &df_runtime, dir, "SELECT id, value FROM t");
        let url = ListingTableUrl::parse(dir).unwrap();

        c.bench_with_input(
            BenchmarkId::new("execute_query", rows),
            &rows,
            |b, _| {
                b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                    let url = url.clone();
                    let metas = metas.clone();
                    let plan = plan.clone();
                    let exec = mgr.cpu_executor();
                    async {
                        let ptr = query_executor::execute_query(
                            url, metas, "t".into(), plan, &df_runtime, exec,
                        ).await.unwrap();
                        // Consume and free the stream
                        let mut stream = unsafe {
                            Box::from_raw(ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                                opensearch_datafusion_jni::cross_rt_stream::CrossRtStream,
                            >)
                        };
                        let mut count = 0u64;
                        while let Some(batch) = stream.try_next().await.unwrap() {
                            count += batch.num_rows() as u64;
                        }
                        count
                    }
                });
            },
        );
    }
    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

fn bench_stream_next(c: &mut Criterion) {
    let (mgr, df_runtime, tmp) = setup();
    create_test_parquet(tmp.path(), 100_000);
    let dir = tmp.path().to_str().unwrap();
    let metas = get_metas(&mgr, dir);
    let plan = get_substrait(&mgr, &df_runtime, dir, "SELECT id, value FROM t");
    let url = ListingTableUrl::parse(dir).unwrap();

    c.bench_function("stream_next_100k", |b| {
        b.to_async(mgr.io_runtime.as_ref()).iter(|| {
            let url = url.clone();
            let metas = metas.clone();
            let plan = plan.clone();
            let exec = mgr.cpu_executor();
            async {
                let ptr = query_executor::execute_query(
                    url, metas, "t".into(), plan, &df_runtime, exec,
                ).await.unwrap();
                let mut stream = unsafe {
                    Box::from_raw(ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                        opensearch_datafusion_jni::cross_rt_stream::CrossRtStream,
                    >)
                };
                let mut batches = 0u64;
                while let Some(_) = stream.try_next().await.unwrap() {
                    batches += 1;
                }
                batches
            }
        });
    });

    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

fn bench_aggregation(c: &mut Criterion) {
    let (mgr, df_runtime, tmp) = setup();
    create_test_parquet(tmp.path(), 100_000);
    let dir = tmp.path().to_str().unwrap();
    let metas = get_metas(&mgr, dir);
    let plan = get_substrait(&mgr, &df_runtime, dir, "SELECT SUM(value), COUNT(*) FROM t");
    let url = ListingTableUrl::parse(dir).unwrap();

    c.bench_function("aggregation_100k", |b| {
        b.to_async(mgr.io_runtime.as_ref()).iter(|| {
            let url = url.clone();
            let metas = metas.clone();
            let plan = plan.clone();
            let exec = mgr.cpu_executor();
            async {
                let ptr = query_executor::execute_query(
                    url, metas, "t".into(), plan, &df_runtime, exec,
                ).await.unwrap();
                let mut stream = unsafe {
                    Box::from_raw(ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                        opensearch_datafusion_jni::cross_rt_stream::CrossRtStream,
                    >)
                };
                while let Some(_) = stream.try_next().await.unwrap() {}
            }
        });
    });

    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

criterion_group!(benches, bench_execute_query, bench_stream_next, bench_aggregation);
criterion_main!(benches);
