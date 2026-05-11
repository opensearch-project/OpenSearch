//! Benchmark: row ID fetch across all three approaches.
//!
//! Approaches:
//!   - baseline: ListingTable, reads ___row_id as plain column (local per-file IDs)
//!   - listing_table: ShardTableProvider + ProjectRowIdOptimizer (___row_id + row_base = absolute IDs)
//!   - indexed_pred: IndexedTableProvider + SingleCollectorEvaluator::predicate_only() (position math, zero ___row_id I/O)
//!
//! Usage:
//!   ROW_ID_BENCH_FILES=/path/seg1.parquet,/path/seg2.parquet cargo bench --bench row_id_bench

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use opensearch_datafusion::api::DataFusionRuntime;
use opensearch_datafusion::datafusion_query_config::{DatafusionQueryConfig, RowIdStrategy};
use opensearch_datafusion::memory::DynamicLimitPool;
use opensearch_datafusion::query_executor;
use opensearch_datafusion::runtime_manager::RuntimeManager;
use std::sync::Arc;

/// Parquet files for benchmarking.
///
/// Pass comma-separated file paths via env var:
///   ROW_ID_BENCH_FILES=/path/seg1.parquet,/path/seg2.parquet
///
/// Single file → single-segment bench only.
/// Two+ files → both single-segment (first file) and multi-segment (all files).
///
/// Files MUST contain a `___row_id` Int32 column and `target_status_code` Int32 column.
///
/// Examples:
///   ROW_ID_BENCH_FILES=/data/generation-1.parquet,/data/generation-2.parquet \
///     cargo bench --bench row_id_bench
///
///   # Use repo test resources (only 2 rows — for smoke test, not real benchmarking)
///   ROW_ID_BENCH_FILES=src/test/resources/test.parquet cargo bench --bench row_id_bench
fn bench_files() -> Vec<String> {
    let files: Vec<String> = std::env::var("ROW_ID_BENCH_FILES")
        .unwrap_or_else(|_| {
            panic!(
                "ROW_ID_BENCH_FILES not set.\n\
                 Pass comma-separated paths to parquet files with ___row_id and target_status_code columns.\n\n\
                 Example:\n  \
                 ROW_ID_BENCH_FILES=/data/generation-1.parquet,/data/generation-2.parquet \\\n    \
                 cargo bench --bench row_id_bench"
            );
        })
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Verify files exist
    for f in &files {
        assert!(
            std::path::Path::new(f).exists(),
            "Benchmark file not found: {}",
            f
        );
    }
    assert!(!files.is_empty(), "ROW_ID_BENCH_FILES must contain at least one file path");
    files
}

/// Derive the directory (common prefix) from the file list for ListingTable URL.
fn bench_dir(files: &[String]) -> String {
    if let Some(first) = files.first() {
        let path = std::path::Path::new(first);
        path.parent()
            .map(|p| format!("{}/", p.display()))
            .unwrap_or_else(|| "/".to_string())
    } else {
        "/".to_string()
    }
}

fn setup() -> (RuntimeManager, DataFusionRuntime) {
    let mgr = RuntimeManager::new(4);
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024)))
        .build()
        .unwrap();
    let (_, handle) = DynamicLimitPool::new(1024 * 1024 * 1024);
    let df_runtime = DataFusionRuntime {
        runtime_env,
        custom_cache_manager: None,
        dynamic_limit_handle: handle,
    };
    (mgr, df_runtime)
}

fn get_metas(mgr: &RuntimeManager, files: &[&str]) -> Arc<Vec<object_store::ObjectMeta>> {
    let store = Arc::new(LocalFileSystem::new());
    let metas: Vec<object_store::ObjectMeta> = files
        .iter()
        .map(|f| {
            let path = object_store::path::Path::from(*f);
            mgr.io_runtime.block_on(store.head(&path)).unwrap()
        })
        .collect();
    Arc::new(metas)
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

fn bench_all_approaches(c: &mut Criterion) {
    let (mgr, df_runtime) = setup();

    let files = bench_files();
    assert!(!files.is_empty(), "ROW_ID_BENCH_FILES must contain at least one file");
    let f1 = &files[0];
    let d = bench_dir(&files);

    let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();
    let metas_single = get_metas(&mgr, &[file_refs[0]]);
    let metas_multi = get_metas(&mgr, &file_refs);
    let url_single = ListingTableUrl::parse(f1).unwrap();
    let url_multi = ListingTableUrl::parse(&d).unwrap();

    // Substrait plans for three selectivities
    let plan_200 = get_substrait(&mgr, f1, "SELECT \"___row_id\" FROM t WHERE target_status_code = 200");
    let plan_404 = get_substrait(&mgr, f1, "SELECT \"___row_id\" FROM t WHERE target_status_code = 404");
    let plan_500 = get_substrait(&mgr, f1, "SELECT \"___row_id\" FROM t WHERE target_status_code = 500");

    let selectivities = vec![
        ("200_70pct", &plan_200),
        ("404_5pct", &plan_404),
        ("500_2pct", &plan_500),
    ];

    let approaches: Vec<(&str, DatafusionQueryConfig)> = vec![
        ("baseline", DatafusionQueryConfig { target_partitions: 10, ..Default::default() }),
        ("listing_table", DatafusionQueryConfig { target_partitions: 10, row_id_strategy: RowIdStrategy::ListingTable, ..Default::default() }),
    ];

    let mut group = c.benchmark_group("row_id");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.measurement_time(std::time::Duration::from_secs(5));

    // === Single segment ===
    for (sel_label, plan) in &selectivities {
        for (approach_label, config) in &approaches {
            let id = BenchmarkId::new(
                format!("1seg/{}", approach_label),
                sel_label,
            );
            group.bench_with_input(id, plan, |b, plan| {
                let config = config.clone();
                let df_rt = &df_runtime;
                b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                    let url = url_single.clone();
                    let metas = metas_single.clone();
                    let plan = (*plan).clone();
                    let exec = mgr.cpu_executor();
                    let config = config.clone();
                    async move {
                        let ptr = query_executor::execute_query(
                            url, metas, "t".into(), plan, df_rt, exec, None, &config,
                        ).await.unwrap();
                        let mut stream = unsafe {
                            Box::from_raw(ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                                opensearch_datafusion::cross_rt_stream::CrossRtStream,
                            >)
                        };
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

    // === Multi segment ===
    for (sel_label, plan) in &selectivities {
        for (approach_label, config) in &approaches {
            let id = BenchmarkId::new(
                format!("2seg/{}", approach_label),
                sel_label,
            );
            group.bench_with_input(id, plan, |b, plan| {
                let config = config.clone();
                let df_rt = &df_runtime;
                b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                    let url = url_multi.clone();
                    let metas = metas_multi.clone();
                    let plan = (*plan).clone();
                    let exec = mgr.cpu_executor();
                    let config = config.clone();
                    async move {
                        let ptr = query_executor::execute_query(
                            url, metas, "t".into(), plan, df_rt, exec, None, &config,
                        ).await.unwrap();
                        let mut stream = unsafe {
                            Box::from_raw(ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                                opensearch_datafusion::cross_rt_stream::CrossRtStream,
                            >)
                        };
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

    // === IndexedPredicateOnly (single segment): BoolTree with Predicate filter ===
    // Uses the same target_status_code filter as other approaches but through
    // the indexed pipeline with position-based row ID computation.
    {
        use opensearch_datafusion::indexed_table::table_provider::{
            IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
        };
        use opensearch_datafusion::indexed_table::eval::{RowGroupBitsetSource, TreeBitsetSource};
        use opensearch_datafusion::indexed_table::eval::bitmap_tree::{BitmapTreeEvaluator, CollectorLeafBitmaps};
        use opensearch_datafusion::indexed_table::bool_tree::BoolNode;
        use opensearch_datafusion::indexed_table::page_pruner::PagePruner;
        use opensearch_datafusion::indexed_table::stream::{RowGroupInfo, FilterStrategy};
        use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
        use datafusion::common::ScalarValue;

        let path = std::path::Path::new(&f1);
        let size = std::fs::metadata(path).unwrap().len();
        let file = std::fs::File::open(path).unwrap();
        let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
        let schema = meta.schema().clone();
        let parquet_meta = meta.metadata().clone();
        let mut rgs = Vec::new();
        let mut offset = 0i64;
        for i in 0..parquet_meta.num_row_groups() {
            let n = parquet_meta.row_group(i).num_rows();
            rgs.push(RowGroupInfo { index: i, first_row: offset, num_rows: n });
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

        // target_status_code is column index 21 in the file
        let col_idx = schema.index_of("target_status_code").unwrap();

        for &(sel_label, filter_value) in &[("200_70pct", 200i32), ("404_5pct", 404), ("500_2pct", 500)] {
            let segment = segment.clone();
            let schema = schema.clone();
            let id = BenchmarkId::new("1seg/indexed_pred", sel_label);
            group.bench_function(id, |b| {
                let segment = segment.clone();
                let schema = schema.clone();
                b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                    let segment = segment.clone();
                    let schema = schema.clone();
                    async move {
                        // Build predicate: target_status_code == filter_value
                        let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Column::new("target_status_code", col_idx),
                        );
                        let lit_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Literal::new(ScalarValue::Int32(Some(filter_value))),
                        );
                        let pred = BoolNode::Predicate(Arc::new(
                            datafusion::physical_expr::expressions::BinaryExpr::new(
                                col_expr, datafusion::logical_expr::Operator::Eq, lit_expr,
                            ),
                        ));
                        let tree = Arc::new(BoolNode::And(vec![pred]).push_not_down());

                        let factory: opensearch_datafusion::indexed_table::table_provider::EvaluatorFactory = {
                            let tree = Arc::clone(&tree);
                            let schema = schema.clone();
                            Arc::new(move |seg, _chunk, _sm| {
                                let resolved = tree.resolve(&[])?;
                                let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&seg.metadata)));
                                let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                                    tree: Arc::new(resolved),
                                    evaluator: Arc::new(BitmapTreeEvaluator),
                                    leaves: Arc::new(CollectorLeafBitmaps {
                                        ffm_collector_calls: _sm.ffm_collector_calls.clone(),
                                    }),
                                    page_pruner: pruner,
                                    cost_predicate: 1,
                                    cost_collector: 10,
                                    max_collector_parallelism: 1,
                                    pruning_predicates: Arc::new(std::collections::HashMap::new()),
                                    page_prune_metrics: Some(
                                        opensearch_datafusion::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(_sm),
                                    ),
                                    collector_strategy: opensearch_datafusion::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
                                });
                                Ok(eval)
                            })
                        };

                        let store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
                        let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
                        let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
                            schema,
                            segments: vec![segment],
                            store,
                            store_url,
                            evaluator_factory: factory,
                            target_partitions: 10,
                            force_strategy: Some(FilterStrategy::BooleanMask),
                            force_pushdown: Some(false),
                            pushdown_predicate: None,
                            query_config: Arc::new(opensearch_datafusion::datafusion_query_config::DatafusionQueryConfig {
                                target_partitions: 10,
                                ..Default::default()
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

        // Multi-segment: all files (skip if only one file provided)
        if files.len() >= 2 {
            // Load all additional segments
            let mut all_segments = vec![segment.clone()];
            let mut cumulative_rows = total_rows as u64;
            for (ord, f) in files[1..].iter().enumerate() {
                let p = std::path::Path::new(f.as_str());
                let sz = std::fs::metadata(p).unwrap().len();
                let fh = std::fs::File::open(p).unwrap();
                let m = ArrowReaderMetadata::load(&fh, ArrowReaderOptions::new().with_page_index(true)).unwrap();
                let pm = m.metadata().clone();
                let mut rg_list = Vec::new();
                let mut off = 0i64;
                for i in 0..pm.num_row_groups() {
                    let n = pm.row_group(i).num_rows();
                    rg_list.push(RowGroupInfo { index: i, first_row: off, num_rows: n });
                    off += n;
                }
                let op = object_store::path::Path::from(p.to_string_lossy().as_ref());
                all_segments.push(SegmentFileInfo {
                    segment_ord: (ord + 1) as i32,
                    max_doc: off,
                    object_path: op,
                    parquet_size: sz,
                    row_groups: rg_list,
                    metadata: Arc::clone(&pm),
                    global_base: cumulative_rows,
                });
                cumulative_rows += off as u64;
            }

            let seg_label = format!("{}seg/indexed_pred", files.len());
            for &(sel_label, filter_value) in &[("200_70pct", 200i32), ("404_5pct", 404), ("500_2pct", 500)] {
                let all_segments = all_segments.clone();
                let schema = schema.clone();
                let id = BenchmarkId::new(&seg_label, sel_label);
                group.bench_function(id, |b| {
                    let all_segments = all_segments.clone();
                    let schema = schema.clone();
                    b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                        let all_segments = all_segments.clone();
                        let schema = schema.clone();
                        async move {
                            let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                                datafusion::physical_expr::expressions::Column::new("target_status_code", col_idx),
                            );
                            let lit_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                                datafusion::physical_expr::expressions::Literal::new(ScalarValue::Int32(Some(filter_value))),
                            );
                            let pred = BoolNode::Predicate(Arc::new(
                                datafusion::physical_expr::expressions::BinaryExpr::new(
                                    col_expr, datafusion::logical_expr::Operator::Eq, lit_expr,
                                ),
                            ));
                            let tree = Arc::new(BoolNode::And(vec![pred]).push_not_down());

                            let factory: opensearch_datafusion::indexed_table::table_provider::EvaluatorFactory = {
                                let tree = Arc::clone(&tree);
                                let schema = schema.clone();
                                Arc::new(move |seg, _chunk, _sm| {
                                    let resolved = tree.resolve(&[])?;
                                    let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&seg.metadata)));
                                    let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                                        tree: Arc::new(resolved),
                                        evaluator: Arc::new(BitmapTreeEvaluator),
                                        leaves: Arc::new(CollectorLeafBitmaps {
                                            ffm_collector_calls: _sm.ffm_collector_calls.clone(),
                                        }),
                                        page_pruner: pruner,
                                        cost_predicate: 1, cost_collector: 10, max_collector_parallelism: 1,
                                        pruning_predicates: Arc::new(std::collections::HashMap::new()),
                                        page_prune_metrics: Some(
                                            opensearch_datafusion::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(_sm),
                                        ),
                                        collector_strategy: opensearch_datafusion::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
                                    });
                                    Ok(eval)
                                })
                            };

                            let store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
                            let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
                            let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
                                schema,
                                segments: all_segments,
                                store,
                                store_url,
                                evaluator_factory: factory,
                                target_partitions: 10,
                                force_strategy: Some(FilterStrategy::BooleanMask),
                                force_pushdown: Some(false),
                                pushdown_predicate: None,
                                query_config: Arc::new(opensearch_datafusion::datafusion_query_config::DatafusionQueryConfig {
                                    target_partitions: 10,
                                    ..Default::default()
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
        }
    }

    group.finish();
    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

/// Correctness check: all three approaches must produce the same sorted row IDs.
/// Collects actual values (not just counts) and compares element-by-element.
fn verify_correctness(c: &mut Criterion) {
    use opensearch_datafusion::indexed_table::table_provider::{
        IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
    };
    use opensearch_datafusion::indexed_table::eval::{RowGroupBitsetSource, TreeBitsetSource};
    use opensearch_datafusion::indexed_table::eval::bitmap_tree::{BitmapTreeEvaluator, CollectorLeafBitmaps};
    use opensearch_datafusion::indexed_table::bool_tree::BoolNode;
    use opensearch_datafusion::indexed_table::page_pruner::PagePruner;
    use opensearch_datafusion::indexed_table::stream::{RowGroupInfo, FilterStrategy};
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::common::ScalarValue;
    use datafusion::arrow::array::{Int32Array, Int64Array, UInt64Array, Array};

    let (mgr, df_runtime) = setup();
    let files = bench_files();
    let f1 = &files[0];
    let metas = get_metas(&mgr, &[f1.as_str()]);
    let url = ListingTableUrl::parse(f1).unwrap();

    // Load segment info for indexed_pred
    let path = std::path::Path::new(&f1);
    let size = std::fs::metadata(path).unwrap().len();
    let file = std::fs::File::open(path).unwrap();
    let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();
    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo { index: i, first_row: offset, num_rows: n });
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
    let col_idx = schema.index_of("target_status_code").unwrap();

    println!("\n=== Correctness verification (actual row ID values) ===");

    for &(label, filter_value) in &[("200", 200i32), ("404", 404), ("500", 500)] {
        let plan = get_substrait(&mgr, &f1,
            &format!("SELECT \"___row_id\" FROM t WHERE target_status_code = {}", filter_value));

        // --- Baseline: collect row IDs (local, not absolute) ---
        let baseline_ids: Vec<i64> = mgr.io_runtime.block_on(async {
            let config = DatafusionQueryConfig { target_partitions: 10, ..Default::default() };
            let ptr = query_executor::execute_query(
                url.clone(), metas.clone(), "t".into(), plan.clone(),
                &df_runtime, mgr.cpu_executor(), None, &config,
            ).await.unwrap();
            let mut stream = unsafe {
                Box::from_raw(ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                    opensearch_datafusion::cross_rt_stream::CrossRtStream,
                >)
            };
            let mut ids = Vec::new();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let col = batch.column(0);
                if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    for i in 0..batch.num_rows() { ids.push(arr.value(i) as i64); }
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..batch.num_rows() { ids.push(arr.value(i)); }
                }
            }
            ids
        });

        // --- ListingTable: collect absolute row IDs (___row_id + row_base) ---
        let listing_ids: Vec<i64> = mgr.io_runtime.block_on(async {
            let config = DatafusionQueryConfig {
                target_partitions: 10,
                row_id_strategy: RowIdStrategy::ListingTable,
                ..Default::default()
            };
            let ptr = query_executor::execute_query(
                url.clone(), metas.clone(), "t".into(), plan.clone(),
                &df_runtime, mgr.cpu_executor(), None, &config,
            ).await.unwrap();
            let mut stream = unsafe {
                Box::from_raw(ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                    opensearch_datafusion::cross_rt_stream::CrossRtStream,
                >)
            };
            let mut ids = Vec::new();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let col = batch.column(0);
                if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    for i in 0..batch.num_rows() { ids.push(arr.value(i) as i64); }
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..batch.num_rows() { ids.push(arr.value(i)); }
                }
            }
            ids
        });

        // --- IndexedPred: collect computed row IDs ---
        let indexed_ids: Vec<i64> = mgr.io_runtime.block_on(async {
            let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                datafusion::physical_expr::expressions::Column::new("target_status_code", col_idx),
            );
            let lit_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                datafusion::physical_expr::expressions::Literal::new(ScalarValue::Int32(Some(filter_value))),
            );
            let pred = BoolNode::Predicate(Arc::new(
                datafusion::physical_expr::expressions::BinaryExpr::new(
                    col_expr, datafusion::logical_expr::Operator::Eq, lit_expr,
                ),
            ));
            let tree = Arc::new(BoolNode::And(vec![pred]).push_not_down());
            let schema_c = schema.clone();
            let factory: opensearch_datafusion::indexed_table::table_provider::EvaluatorFactory = {
                let tree = Arc::clone(&tree);
                Arc::new(move |seg, _chunk, _sm| {
                    let resolved = tree.resolve(&[])?;
                    let pruner = Arc::new(PagePruner::new(&schema_c, Arc::clone(&seg.metadata)));
                    let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                        tree: Arc::new(resolved),
                        evaluator: Arc::new(BitmapTreeEvaluator),
                        leaves: Arc::new(CollectorLeafBitmaps { ffm_collector_calls: _sm.ffm_collector_calls.clone() }),
                        page_pruner: pruner,
                        cost_predicate: 1, cost_collector: 10, max_collector_parallelism: 1,
                        pruning_predicates: Arc::new(std::collections::HashMap::new()),
                        page_prune_metrics: Some(opensearch_datafusion::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(_sm)),
                        collector_strategy: opensearch_datafusion::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
                    });
                    Ok(eval)
                })
            };
            let store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
            let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
            let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
                schema: schema.clone(),
                segments: vec![segment.clone()],
                store, store_url,
                evaluator_factory: factory,
                target_partitions: 10,
                force_strategy: Some(FilterStrategy::BooleanMask),
                force_pushdown: Some(false),
                pushdown_predicate: None,
                query_config: Arc::new(opensearch_datafusion::datafusion_query_config::DatafusionQueryConfig {
                    target_partitions: 10, ..Default::default()
                }),
                predicate_columns: vec![col_idx],
                emit_row_ids: true,
            }));
            let ctx = datafusion::prelude::SessionContext::new();
            ctx.register_table("t", provider).unwrap();
            let df = ctx.sql("SELECT * FROM t").await.unwrap();
            let mut stream = df.execute_stream().await.unwrap();
            let mut ids = Vec::new();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let col = batch.column(0);
                if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    for i in 0..batch.num_rows() { ids.push(arr.value(i) as i64); }
                }
            }
            ids
        });

        // Sort all for comparison (execution order may differ across partitions)
        let mut baseline_sorted = baseline_ids.clone();
        let mut listing_sorted = listing_ids.clone();
        let mut indexed_sorted = indexed_ids.clone();
        baseline_sorted.sort();
        listing_sorted.sort();
        indexed_sorted.sort();

        // Compare
        let counts_match = baseline_sorted.len() == listing_sorted.len()
            && listing_sorted.len() == indexed_sorted.len();

        // For single file with row_base=0, listing_table IDs should equal baseline IDs
        // (since ___row_id + 0 = ___row_id). Both should equal indexed_pred IDs
        // (since global_base=0 and position === ___row_id for sequential data).
        let baseline_eq_listing = baseline_sorted == listing_sorted;
        let baseline_eq_indexed = baseline_sorted == indexed_sorted;

        println!("  filter={}: count={} | baseline==listing: {} | baseline==indexed: {}",
            label, baseline_sorted.len(), baseline_eq_listing, baseline_eq_indexed);

        if !baseline_eq_listing {
            println!("    MISMATCH baseline vs listing_table!");
            println!("    first 5 baseline: {:?}", &baseline_sorted[..5.min(baseline_sorted.len())]);
            println!("    first 5 listing:  {:?}", &listing_sorted[..5.min(listing_sorted.len())]);
        }
        if !baseline_eq_indexed {
            println!("    MISMATCH baseline vs indexed_pred!");
            println!("    first 5 baseline: {:?}", &baseline_sorted[..5.min(baseline_sorted.len())]);
            println!("    first 5 indexed:  {:?}", &indexed_sorted[..5.min(indexed_sorted.len())]);
        }

        assert!(counts_match, "Row counts differ for filter={}", label);
        assert!(baseline_eq_listing, "Row ID values differ: baseline vs listing_table for filter={}", label);
        assert!(baseline_eq_indexed, "Row ID values differ: baseline vs indexed_pred for filter={}", label);
    }
    println!("  ✓ All approaches return identical row ID values\n");

    // Dummy bench so criterion doesn't complain
    let mut group = c.benchmark_group("correctness");
    group.sample_size(10);
    group.bench_function("verify", |b| { b.iter(|| 1 + 1); });
    group.finish();

    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

criterion_group!(benches, bench_all_approaches, verify_correctness);
criterion_main!(benches);
