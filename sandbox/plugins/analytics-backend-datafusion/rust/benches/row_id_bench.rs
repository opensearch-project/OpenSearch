//! Benchmark: ClickBench queries with and without row ID emission.
//!
//! Queries (from ClickBench q25–q27):
//!   - q25: WHERE SearchPhrase != '' ORDER BY EventTime LIMIT 10
//!   - q26: WHERE SearchPhrase != '' ORDER BY SearchPhrase LIMIT 10
//!   - q27: WHERE SearchPhrase != '' ORDER BY EventTime, SearchPhrase LIMIT 10
//!
//! Each query is run in two modes:
//!   - data_fetch: normal execution returning data columns (via ListingTable)
//!   - row_id_emit: query phase only, returning shard-global row IDs (via indexed path)
//!
//! Usage:
//!   ROW_ID_BENCH_FILE=/Users/abandeji/Downloads/hits_1.parquet cargo bench --bench row_id_bench

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, ObjectStoreExt};
use opensearch_datafusion::api::DataFusionRuntime;
use opensearch_datafusion::datafusion_query_config::DatafusionQueryConfig;
use opensearch_datafusion::memory::DynamicLimitPool;
use opensearch_datafusion::query_executor;
use opensearch_datafusion::runtime_manager::RuntimeManager;
use std::sync::Arc;

fn bench_file() -> String {
    let path = std::env::var("ROW_ID_BENCH_FILE").unwrap_or_else(|_| {
        panic!(
            "ROW_ID_BENCH_FILE not set.\n\
             Pass path to a ClickBench hits parquet file.\n\n\
             Example:\n  \
             ROW_ID_BENCH_FILE=/Users/abandeji/Downloads/hits_1.parquet \\\n    \
             cargo bench --bench row_id_bench"
        );
    });
    assert!(
        std::path::Path::new(&path).exists(),
        "Benchmark file not found: {}",
        path
    );
    path
}

fn setup() -> (RuntimeManager, DataFusionRuntime) {
    let mgr = RuntimeManager::new(4);
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(2 * 1024 * 1024 * 1024)))
        .build()
        .unwrap();
    let (_, handle) = DynamicLimitPool::new(2 * 1024 * 1024 * 1024);
    let df_runtime = DataFusionRuntime {
        runtime_env,
        custom_cache_manager: None,
        dynamic_limit_handle: handle,
    };
    (mgr, df_runtime)
}

fn get_metas(mgr: &RuntimeManager, file: &str) -> Arc<Vec<object_store::ObjectMeta>> {
    let store = Arc::new(LocalFileSystem::new());
    let path = object_store::path::Path::from(file);
    let meta = mgr.io_runtime.block_on(store.head(&path)).unwrap();
    Arc::new(vec![meta])
}

fn get_substrait(mgr: &RuntimeManager, file_path: &str, sql: &str) -> Vec<u8> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    mgr.io_runtime.block_on(async {
        let ctx = datafusion::prelude::SessionContext::new();
        let url = ListingTableUrl::parse(file_path).unwrap();
        let opts = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let schema = opts.infer_schema(&ctx.state(), &url).await.unwrap();
        let cfg = ListingTableConfig::new(url)
            .with_listing_options(opts)
            .with_schema(schema);
        ctx.register_table("t", Arc::new(ListingTable::try_new(cfg).unwrap()))
            .unwrap();
        let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
        let sub = to_substrait_plan(&plan, &ctx.state()).unwrap();
        let mut buf = Vec::new();
        sub.encode(&mut buf).unwrap();
        buf
    })
}

struct QueryDef {
    name: &'static str,
    sql_data: &'static str,
    sql_rowid: &'static str,
}

const QUERIES: &[QueryDef] = &[
    QueryDef {
        name: "q25-sort-eventtime",
        sql_data: "SELECT \"SearchPhrase\" FROM t WHERE \"SearchPhrase\" != '' ORDER BY \"EventTime\" LIMIT 10",
        sql_rowid: "SELECT \"SearchPhrase\" FROM t WHERE \"SearchPhrase\" != ''",
    },
    QueryDef {
        name: "q26-sort-searchphrase",
        sql_data: "SELECT \"SearchPhrase\" FROM t WHERE \"SearchPhrase\" != '' ORDER BY \"SearchPhrase\" LIMIT 10",
        sql_rowid: "SELECT \"SearchPhrase\" FROM t WHERE \"SearchPhrase\" != ''",
    },
    QueryDef {
        name: "q27-sort-multi",
        sql_data: "SELECT \"SearchPhrase\" FROM t WHERE \"SearchPhrase\" != '' ORDER BY \"EventTime\", \"SearchPhrase\" LIMIT 10",
        sql_rowid: "SELECT \"SearchPhrase\" FROM t WHERE \"SearchPhrase\" != ''",
    },
];

fn bench_clickbench(c: &mut Criterion) {
    let (mgr, df_runtime) = setup();
    let file = bench_file();
    let metas = get_metas(&mgr, &file);
    let url = ListingTableUrl::parse(&file).unwrap();

    let mut group = c.benchmark_group("clickbench_qtf");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_secs(3));
    group.measurement_time(std::time::Duration::from_secs(10));

    for q in QUERIES {
        // --- Mode 1: data_fetch (normal query, no row ID emission) ---
        let plan_data = get_substrait(&mgr, &file, q.sql_data);
        let id_data = BenchmarkId::new(format!("{}/data_fetch", q.name), "");
        group.bench_with_input(id_data, &plan_data, |b, plan| {
            let df_rt = &df_runtime;
            b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                let url = url.clone();
                let metas = metas.clone();
                let plan = plan.clone();
                let exec = mgr.cpu_executor();
                async move {
                    let mut config = DatafusionQueryConfig::test_default();
                    config.target_partitions = 4;
                    let ptr = query_executor::execute_query(
                        url, metas, "t".into(), plan, df_rt, exec, None, &config,
                    )
                    .await
                    .unwrap();
                    let mut stream = unsafe {
                        Box::from_raw(
                            ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                                opensearch_datafusion::cross_rt_stream::CrossRtStream,
                            >,
                        )
                    };
                    let mut rows = 0u64;
                    while let Some(batch) = stream.try_next().await.unwrap() {
                        rows += batch.num_rows() as u64;
                    }
                    rows
                }
            });
        });

        // --- Mode 2: row_id_emit (indexed path, emit_row_ids=true, filter only) ---
        {
            use datafusion::common::ScalarValue;
            use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
            use opensearch_datafusion::indexed_table::bool_tree::BoolNode;
            use opensearch_datafusion::indexed_table::eval::bitmap_tree::{
                BitmapTreeEvaluator, CollectorLeafBitmaps,
            };
            use opensearch_datafusion::indexed_table::eval::{RowGroupBitsetSource, TreeBitsetSource};
            use opensearch_datafusion::indexed_table::page_pruner::PagePruner;
            use opensearch_datafusion::indexed_table::stream::RowGroupInfo;
            use opensearch_datafusion::indexed_table::table_provider::{
                IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
            };

            let path = std::path::Path::new(&file);
            let size = std::fs::metadata(path).unwrap().len();
            let fh = std::fs::File::open(path).unwrap();
            let meta =
                ArrowReaderMetadata::load(&fh, ArrowReaderOptions::new().with_page_index(true))
                    .unwrap();
            let schema = meta.schema().clone();
            let parquet_meta = meta.metadata().clone();
            let mut rgs = Vec::new();
            let mut offset = 0i64;
            for i in 0..parquet_meta.num_row_groups() {
                let n = parquet_meta.row_group(i).num_rows();
                rgs.push(RowGroupInfo {
                    index: i,
                    first_row: offset,
                    num_rows: n,
                });
                offset += n;
            }
            let total_rows = offset;
            let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
            let segment = SegmentFileInfo {
                segment_ord: 0,
                max_doc: total_rows,
                object_path,
                parquet_size: size,
                row_groups: rgs,
                metadata: Arc::clone(&parquet_meta),
                global_base: 0,
            };

            let col_idx = schema.index_of("SearchPhrase").unwrap();

            let id_rowid = BenchmarkId::new(format!("{}/row_id_emit", q.name), "");
            let segment_c = segment.clone();
            let schema_c = schema.clone();
            group.bench_function(id_rowid, |b| {
                let segment = segment_c.clone();
                let schema = schema_c.clone();
                b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                    let segment = segment.clone();
                    let schema = schema.clone();
                    async move {
                        // Predicate: SearchPhrase != '' (binary != empty bytes)
                        let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Column::new(
                                "SearchPhrase",
                                col_idx,
                            ),
                        );
                        let lit_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Literal::new(
                                ScalarValue::Binary(Some(vec![])),
                            ),
                        );
                        let pred = BoolNode::Predicate(Arc::new(
                            datafusion::physical_expr::expressions::BinaryExpr::new(
                                col_expr,
                                datafusion::logical_expr::Operator::NotEq,
                                lit_expr,
                            ),
                        ));
                        let tree = Arc::new(BoolNode::And(vec![pred]).push_not_down());

                        let factory: opensearch_datafusion::indexed_table::table_provider::EvaluatorFactory = {
                            let tree = Arc::clone(&tree);
                            let schema = schema.clone();
                            Arc::new(move |seg, _chunk, _sm| {
                                let resolved = tree.resolve(&[])?;
                                let pruner = Arc::new(PagePruner::new(
                                    &schema,
                                    Arc::clone(&seg.metadata),
                                ));
                                let eval: Arc<dyn RowGroupBitsetSource> =
                                    Arc::new(TreeBitsetSource {
                                        tree: Arc::new(resolved),
                                        evaluator: Arc::new(BitmapTreeEvaluator),
                                        leaves: Arc::new(CollectorLeafBitmaps {
                                            ffm_collector_calls: _sm.ffm_collector_calls.clone(),
                                        }),
                                        page_pruner: pruner,
                                        cost_predicate: 1,
                                        cost_collector: 10,
                                        max_collector_parallelism: 1,
                                        pruning_predicates: Arc::new(
                                            std::collections::HashMap::new(),
                                        ),
                                        page_prune_metrics: Some(
                                            opensearch_datafusion::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(_sm),
                                        ),
                                        collector_strategy: opensearch_datafusion::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
                                    });
                                Ok(eval)
                            })
                        };

                        let store: Arc<dyn object_store::ObjectStore> =
                            Arc::new(LocalFileSystem::new());
                        let store_url =
                            datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
                        let provider =
                            Arc::new(IndexedTableProvider::new(IndexedTableConfig {
                                schema,
                                segments: vec![segment],
                                store,
                                store_url,
                                evaluator_factory: factory,
                                pushdown_predicate: None,
                                query_config: Arc::new({
                                    let mut qc = DatafusionQueryConfig::test_default();
                                    qc.target_partitions = 4;
                                    qc
                                }),
                                predicate_columns: vec![col_idx],
                                emit_row_ids: true,
                            }));

                        let ctx = datafusion::prelude::SessionContext::new();
                        ctx.register_table("t", provider).unwrap();
                        let df = ctx.sql("SELECT * FROM t").await.unwrap();
                        let mut stream = df.execute_stream().await.unwrap();
                        let mut rows = 0u64;
                        while let Some(batch) = stream.try_next().await.unwrap() {
                            rows += batch.num_rows() as u64;
                        }
                        rows
                    }
                });
            });
        }

        // --- Mode 3: row_id_emit with NO emit (indexed path, same filter, returns data) ---
        {
            use datafusion::common::ScalarValue;
            use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
            use opensearch_datafusion::indexed_table::bool_tree::BoolNode;
            use opensearch_datafusion::indexed_table::eval::bitmap_tree::{
                BitmapTreeEvaluator, CollectorLeafBitmaps,
            };
            use opensearch_datafusion::indexed_table::eval::{RowGroupBitsetSource, TreeBitsetSource};
            use opensearch_datafusion::indexed_table::page_pruner::PagePruner;
            use opensearch_datafusion::indexed_table::stream::RowGroupInfo;
            use opensearch_datafusion::indexed_table::table_provider::{
                IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
            };

            let path = std::path::Path::new(&file);
            let size = std::fs::metadata(path).unwrap().len();
            let fh = std::fs::File::open(path).unwrap();
            let meta =
                ArrowReaderMetadata::load(&fh, ArrowReaderOptions::new().with_page_index(true))
                    .unwrap();
            let schema = meta.schema().clone();
            let parquet_meta = meta.metadata().clone();
            let mut rgs = Vec::new();
            let mut offset = 0i64;
            for i in 0..parquet_meta.num_row_groups() {
                let n = parquet_meta.row_group(i).num_rows();
                rgs.push(RowGroupInfo {
                    index: i,
                    first_row: offset,
                    num_rows: n,
                });
                offset += n;
            }
            let total_rows = offset;
            let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
            let segment = SegmentFileInfo {
                segment_ord: 0,
                max_doc: total_rows,
                object_path,
                parquet_size: size,
                row_groups: rgs,
                metadata: Arc::clone(&parquet_meta),
                global_base: 0,
            };

            let col_idx = schema.index_of("SearchPhrase").unwrap();

            let id_no_rowid = BenchmarkId::new(format!("{}/indexed_no_emit", q.name), "");
            let segment_c = segment.clone();
            let schema_c = schema.clone();
            group.bench_function(id_no_rowid, |b| {
                let segment = segment_c.clone();
                let schema = schema_c.clone();
                b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                    let segment = segment.clone();
                    let schema = schema.clone();
                    async move {
                        let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Column::new(
                                "SearchPhrase",
                                col_idx,
                            ),
                        );
                        let lit_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Literal::new(
                                ScalarValue::Binary(Some(vec![])),
                            ),
                        );
                        let pred = BoolNode::Predicate(Arc::new(
                            datafusion::physical_expr::expressions::BinaryExpr::new(
                                col_expr,
                                datafusion::logical_expr::Operator::NotEq,
                                lit_expr,
                            ),
                        ));
                        let tree = Arc::new(BoolNode::And(vec![pred]).push_not_down());

                        let factory: opensearch_datafusion::indexed_table::table_provider::EvaluatorFactory = {
                            let tree = Arc::clone(&tree);
                            let schema = schema.clone();
                            Arc::new(move |seg, _chunk, _sm| {
                                let resolved = tree.resolve(&[])?;
                                let pruner = Arc::new(PagePruner::new(
                                    &schema,
                                    Arc::clone(&seg.metadata),
                                ));
                                let eval: Arc<dyn RowGroupBitsetSource> =
                                    Arc::new(TreeBitsetSource {
                                        tree: Arc::new(resolved),
                                        evaluator: Arc::new(BitmapTreeEvaluator),
                                        leaves: Arc::new(CollectorLeafBitmaps {
                                            ffm_collector_calls: _sm.ffm_collector_calls.clone(),
                                        }),
                                        page_pruner: pruner,
                                        cost_predicate: 1,
                                        cost_collector: 10,
                                        max_collector_parallelism: 1,
                                        pruning_predicates: Arc::new(
                                            std::collections::HashMap::new(),
                                        ),
                                        page_prune_metrics: Some(
                                            opensearch_datafusion::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(_sm),
                                        ),
                                        collector_strategy: opensearch_datafusion::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
                                    });
                                Ok(eval)
                            })
                        };

                        let store: Arc<dyn object_store::ObjectStore> =
                            Arc::new(LocalFileSystem::new());
                        let store_url =
                            datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
                        let provider =
                            Arc::new(IndexedTableProvider::new(IndexedTableConfig {
                                schema,
                                segments: vec![segment],
                                store,
                                store_url,
                                evaluator_factory: factory,
                                pushdown_predicate: None,
                                query_config: Arc::new({
                                    let mut qc = DatafusionQueryConfig::test_default();
                                    qc.target_partitions = 4;
                                    qc
                                }),
                                predicate_columns: vec![col_idx],
                                emit_row_ids: false,
                            }));

                        let ctx = datafusion::prelude::SessionContext::new();
                        ctx.register_table("t", provider).unwrap();
                        let df = ctx.sql("SELECT * FROM t").await.unwrap();
                        let mut stream = df.execute_stream().await.unwrap();
                        let mut rows = 0u64;
                        while let Some(batch) = stream.try_next().await.unwrap() {
                            rows += batch.num_rows() as u64;
                        }
                        rows
                    }
                });
            });
        }
    }

    group.finish();
    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

criterion_group!(benches, bench_clickbench);
criterion_main!(benches);
