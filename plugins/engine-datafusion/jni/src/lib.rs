use std::cell::RefCell;
use std::num::NonZeroUsize;
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::ptr::addr_of_mut;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use jni::objects::{JByteArray, JClass, JObject};
use std::collections::{BTreeSet, HashMap};
use jni::objects::JLongArray;
use jni::sys::{jbyteArray, jint, jlong, jstring};
use jni::{JNIEnv, JavaVM};
use std::sync::{Arc, OnceLock};
use arrow_array::{Array, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::DataType;
use arrow_schema::ffi::FFI_ArrowSchema;
use datafusion::{
    common::DataFusionError
    ,
    datasource::file_format::parquet::ParquetFormat,
    datasource::listing::ListingTableUrl,
    datasource::object_store::ObjectStoreUrl,
    datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess},
    datasource::physical_plan::ParquetSource,
    execution::cache::cache_manager::CacheManagerConfig,
    execution::cache::cache_unit::{DefaultListFilesCache,DefaultFilesMetadataCache},
    execution::cache::CacheAccessor,
    execution::context::SessionContext,
    execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    execution::TaskContext,
    parquet::arrow::arrow_reader::RowSelector,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
    prelude::*,
    DATAFUSION_VERSION,
};
use std::default::Default;
use std::time::{Duration, Instant};

mod util;
mod row_id_optimizer;
mod listing_table;
mod cache;
mod custom_cache_manager;
mod memory;
mod cross_rt_stream;
mod executor;
mod io;
mod runtime_manager;
mod cache_jni;
mod partial_agg_optimizer;

use crate::custom_cache_manager::CustomCacheManager;
use crate::row_id_optimizer::ProjectRowIdOptimizer;
use crate::util::{create_file_meta_from_filenames, parse_string_arr, set_object_result_error, set_object_result_error_global, set_object_result_ok, set_object_result_ok_global};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};
use crate::partial_agg_optimizer::PartialAggregationOptimizer;

use datafusion::catalog::TableProvider;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    from_substrait_plan, from_substrait_plan_with_consumer, DefaultSubstraitConsumer,
    SubstraitConsumer,
};
use datafusion_substrait::substrait::proto::{
    Expression, ExtensionLeafRel, ExtensionMultiRel, ExtensionSingleRel, Plan, PlanRel, ProjectRel,
};
use datafusion_substrait::substrait::proto::extensions::simple_extension_declaration::MappingType;

use object_store::ObjectMeta;
use prost::Message;
use tokio::runtime::Runtime;
use std::result;
use std::sync::atomic::AtomicU64;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;

// NativeBridge JNI implementations
use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use jni::objects::{JObjectArray, JString};
use log::{error, info};
use once_cell::sync::Lazy;
use tokio_metrics::{RuntimeMonitor, TaskMonitor};
use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::memory::{CustomMemoryPool, Monitor, MonitoredMemoryPool};
use crate::runtime_manager::RuntimeManager;

struct DataFusionRuntime {
    runtime_env: RuntimeEnv,
    custom_cache_manager: Option<CustomCacheManager>,
    monitor: Arc<Monitor>,
}

// TASK monitorint metrics
static QUERY_EXECUTION_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(100)).clone()
});

static STREAM_NEXT_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(50)).clone()
});

// Global runtime manager
static TOKIO_RUNTIME_MANAGER: OnceLock<Arc<RuntimeManager>> = OnceLock::new();

// Global JavaVM reference
static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

thread_local! {
    static THREAD_JNIENV: RefCell<Option<JNIEnv<'static>>> = RefCell::new(None);
}

// Helper function to get or attach JNI env
fn with_jni_env<F, R>(f: F) -> R
where
    F: FnOnce(&mut JNIEnv) -> R,
{
    THREAD_JNIENV.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            let jvm = JAVA_VM.get().expect("JavaVM not initialized");
            let env = jvm.attach_current_thread_permanently()
                .expect("Failed to attach thread to JVM");
            *opt = Some(env);
        }

        // Safe because we're the only one with access to this thread-local
        let env_ref = opt.as_mut().unwrap();
        f(env_ref)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_initTokioRuntimeManager(
    env: JNIEnv,
    _class: JClass,
    cpu_threads: jint,
) {
    // Initialize JavaVM once
    JAVA_VM.get_or_init(|| {
        env.get_java_vm().expect("Failed to get JavaVM")
    });

    TOKIO_RUNTIME_MANAGER.get_or_init(|| {
        println!("Runtime manager initialized with {} CPU threads", cpu_threads);
        Arc::new(RuntimeManager::new(cpu_threads as usize))
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_shutdownTokioRuntimeManager(
    _env: JNIEnv,
    _class: JClass,
) {
    if let Some(_mgr) = TOKIO_RUNTIME_MANAGER.get() {
        // Runtimes will be dropped and shut down when RUNTIME_MANAGER is dropped
        info!("Runtime manager shut down");
    }
}


#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_startTokioRuntimeMonitoring(
    _env: JNIEnv,
    _class: JClass,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            error!("Tokio runtime manager not initialized");
            return;
        }
    };

    // Uncomment this to monitor tokio metrics

    // let io_runtime = manager.io_runtime.clone();
    // io_runtime.spawn(async move {
    //     let handle = tokio::runtime::Handle::current();
    //     let runtime_monitor = RuntimeMonitor::new(&handle);
    //
    //     // Monitor at 120-second intervals
    //     for metrics in runtime_monitor.intervals() {
    //         log_runtime_metrics(&metrics);
    //         tokio::time::sleep(Duration::from_secs(120)).await;
    //     }
    // });
    //
    // println!("Runtime monitoring started");
}

/// Log runtime metrics with performance analysis
fn log_runtime_metrics(metrics: &tokio_metrics::RuntimeMetrics) {
    println!("=== Runtime Metrics ===");
    println!("  Workers: {}", metrics.workers_count);
    println!("  Global queue depth: {}", metrics.global_queue_depth);
    /**
    //unstable tokio causes build failures, uncomment this when monitoring

    println!("  Worker overflow: {}", metrics.total_overflow_count);
    println!("  Remote schedule: {}", metrics.max_local_schedule_count);
    println!("  Worker steal ops: {}", metrics.total_steal_operations);
    println!("  Blocking queue depth: {}", metrics.blocking_queue_depth);
    println!("  Max local queue depth: {}", metrics.max_local_queue_depth);
    println!("  Min local queue depth: {}", metrics.min_local_queue_depth);
    println!("  Max local schedule count: {}", metrics.max_local_schedule_count);
    println!("  Min local schedule count: {}", metrics.min_local_schedule_count);
    println!("  Queue depth: {}", metrics.total_local_queue_depth);
    println!("  Total schedule count: {}", metrics.total_local_schedule_count);
    **/
    let query_metrics = QUERY_EXECUTION_MONITOR.cumulative();
    log_task_metrics("Query exec (via CrossRtStream)", &query_metrics);
    let stream_metrics = STREAM_NEXT_MONITOR.cumulative();
    log_task_metrics("Stream Next (via CrossRtStream)", &stream_metrics);
    println!("======================");
}

/// Log task metrics with performance analysis
fn log_task_metrics(operation: &str, metrics: &tokio_metrics::TaskMetrics) {
    println!("=== Task Metrics: {} ===", operation);
    println!("  Scheduled duration: {:?}", metrics.total_scheduled_duration);
    println!("  Poll duration: {:?}", metrics.total_poll_duration);
    println!("  Idle duration: {:?}", metrics.total_idle_duration);
    println!("  Mean poll duration: {:?}", metrics.mean_poll_duration());
    println!("  Slow poll ratio: {:.2}%", metrics.slow_poll_ratio() * 100.0);
    println!("  Mean first poll delay: {:?}", metrics.mean_first_poll_delay());
    println!("  Total slow polls: {}", metrics.total_slow_poll_count);
    println!("  Total long delays: {}", metrics.total_long_delay_count);
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
    memory_pool_limit: jlong,
    cache_manager_ptr: jlong
) -> jlong {
    let monitor = Arc::new(Monitor::default());
    let memory_pool = Arc::new(MonitoredMemoryPool::new(
        Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(memory_pool_limit as usize),
            NonZeroUsize::new(5).unwrap(),
        )),
        monitor.clone(),
    ));

    if cache_manager_ptr != 0 {
        // Take ownership of the CustomCacheManager
        let custom_cache_manager = unsafe { *Box::from_raw(cache_manager_ptr as *mut CustomCacheManager) };
        let cache_manager_config = custom_cache_manager.build_cache_manager_config();

        let runtime_env = RuntimeEnvBuilder::new().with_cache_manager(cache_manager_config)
            .with_memory_pool(memory_pool.clone())
            .build().unwrap();

        let runtime = DataFusionRuntime {
            runtime_env,
            custom_cache_manager: Some(custom_cache_manager),
            monitor,
        };

        Box::into_raw(Box::new(runtime)) as jlong
    } else {
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build().unwrap();

        let runtime = DataFusionRuntime {
            runtime_env,
            custom_cache_manager: None,
            monitor,
        };

        Box::into_raw(Box::new(runtime)) as jlong
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        let _ = unsafe { Box::from_raw(ptr as *mut DataFusionRuntime) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
) -> jlong {
    if runtime_id == 0 {
        return 0;
    }
    let runtime_env = unsafe { &*(runtime_id as *const RuntimeEnv) };
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config_rt(config, Arc::new(runtime_env.clone()));
    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeSessionContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    if context_id != 0 {
        let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_getVersionInfo(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version_info = format!(
        r#"{{"version": "{}", "codecs": ["CsvDataSourceCodec"]}}"#,
        DATAFUSION_VERSION
    );
    env.new_string(version_info)
        .expect("Couldn't create Java string")
        .as_raw()
}



#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    files: JObjectArray,
) -> jlong {
    let table_path: String = match env.get_string(&table_path) {
        Ok(path) => path.into(),
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid table path: {:?}", e),
            );
            return 0;
        }
    };

    let files: Vec<String> = match parse_string_arr(&mut env, files) {
        Ok(files) => files,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid file list: {}", e),
            );
            return 0;
        }
    };

    let files_metadata = match create_file_meta_from_filenames(&table_path, files.clone()) {
        Ok(metadata) => metadata,
        Err(err) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create metadata: {}", err),
            );
            return 0;
        }
    };

    let table_url = match ListingTableUrl::parse(&table_path) {
        Ok(url) => url,
        Err(err) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Invalid table path: {}", err),
            );
            return 0;
        }
    };

    let shard_view = ShardView::new(table_url, files_metadata);

    Box::into_raw(Box::new(shard_view)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeDatafusionReader(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        let _ = unsafe { Box::from_raw(ptr as *mut ShardView) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_destroyTokioRuntime(
    mut env: JNIEnv,
    _class: JClass,
    tokio_runtime_ptr: jlong
)  {
    let _ = unsafe { Box::from_raw(tokio_runtime_ptr as *mut Runtime) };
}

pub struct ShardView {
    table_path: ListingTableUrl,
    files_metadata: Arc<Vec<CustomFileMeta>>,
}

impl ShardView {
    pub fn new(table_path: ListingTableUrl, files_metadata: Vec<CustomFileMeta>) -> Self {
        let files_metadata = Arc::new(files_metadata);
        ShardView {
            table_path,
            files_metadata,
        }
    }

    pub fn table_path(&self) -> ListingTableUrl {
        self.table_path.clone()
    }

    pub fn files_metadata(&self) -> Arc<Vec<CustomFileMeta>> {
        self.files_metadata.clone()
    }
}

#[derive(Debug, Clone)]
struct CustomFileMeta {
    row_group_row_counts: Arc<Vec<i64>>,
    row_base: Arc<i64>,
    object_meta: Arc<ObjectMeta>,
}

impl CustomFileMeta {
    pub fn new(row_group_row_counts: Vec<i64>, row_base: i64, object_meta: ObjectMeta) -> Self {
        let row_group_row_counts = Arc::new(row_group_row_counts);
        let row_base = Arc::new(row_base);
        let object_meta = Arc::new(object_meta);
        CustomFileMeta {
            row_group_row_counts,
            row_base,
            object_meta,
        }
    }

    pub fn row_group_row_counts(&self) -> Arc<Vec<i64>> {
        self.row_group_row_counts.clone()
    }

    pub fn row_base(&self) -> Arc<i64> {
        self.row_base.clone()
    }

    pub fn object_meta(&self) -> Arc<ObjectMeta> {
        self.object_meta.clone()
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeQueryPhaseAsync(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: jbyteArray,
    runtime_ptr: jlong,
    callback: JObject,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            error!("Runtime manager not initialized");
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return;
        }
    };

    // ===== EXTRACT ALL JAVA DATA BEFORE ASYNC BLOCK =====
    let table_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            error!("Failed to get table name: {}", e);
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution(format!("Failed to get table name: {}", e)));
            return;
        }
    };

    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Failed to convert plan bytes: {}", e);
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)));
            return;
        }
    };

    // Convert callback to GlobalRef (thread-safe)
    let callback_ref = match env.new_global_ref(&callback) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to create global ref: {}", e);
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };
    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();

    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_metadata();

    // Spawn async task - TRULY NON-BLOCKING!
    io_runtime.block_on(async move {

        let result = execute_query_with_cross_rt_stream(
            table_path,
            files_meta,
            table_name,
            plan_bytes_vec,
            runtime,
            cpu_executor,
        ).await;

        match result {
            Ok(stream_ptr) => {
                with_jni_env(|env| {
                    set_object_result_ok_global(env, &callback_ref, stream_ptr as *mut u8);
                });
            }
            Err(e) => {
                with_jni_env(|env| {
                    error!("Query execution failed: {}", e);
                    set_object_result_error_global(env, &callback_ref, &e);
                });
            }
        }
    });
}

async fn execute_query_with_cross_rt_stream(
    table_path: ListingTableUrl,
    files_meta: Arc<Vec<CustomFileMeta>>,
    table_name: String,
    plan_bytes_vec: Vec<u8>,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<jlong, DataFusionError> {
    let object_meta: Arc<Vec<ObjectMeta>> = Arc::new(
        files_meta
            .iter()
            .map(|metadata| (*metadata.object_meta).clone())
            .collect(),
    );

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), object_meta);

    let runtimeEnv = &runtime.runtime_env;

    let runtime_env = match RuntimeEnvBuilder::from_runtime_env(runtimeEnv)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache.clone()))
                .with_file_metadata_cache(Some(runtimeEnv.cache_manager.get_file_metadata_cache())),
        )
        .build() {
        Ok(env) => env,
        Err(e) => {
            error!("Failed to build runtime env: {}", e);
            return Err(e);
        }
    };

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = false;
    config.options_mut().execution.target_partitions = 1;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        //.with_physical_optimizer_rule(Arc::new(ProjectRowIdOptimizer)) // TODO : uncomment this after fix
        .with_physical_optimizer_rule(Arc::new(PartialAggregationOptimizer))
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Register table
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet") // TODO: take this as parameter
        .with_files_metadata(files_meta)
        .with_table_partition_cols(vec![("row_base".to_string(), DataType::Int64)]);

    let resolved_schema = match listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await {
        Ok(schema) => schema,
        Err(e) => {
            error!("Failed to infer schema: {}", e);
            return Err(e);
        }
    };

    let table_config = ListingTableConfig::new(table_path.clone())
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let provider = match ListingTable::try_new(table_config) {
        Ok(table) => Arc::new(table),
        Err(e) => {
            error!("Failed to create listing table: {}", e);
            return Err(e);
        }
    };

    if let Err(e) = ctx.register_table(&table_name, provider) {
        error!("Failed to register table: {}", e);
        return Err(e);
    }

    // Decode substrait
    let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
        Ok(plan) => plan,
        Err(e) => {
            error!("Failed to decode Substrait plan: {}", e);
            return Err(DataFusionError::Execution(format!("Failed to decode Substrait: {}", e)));
        }
    };

    let mut modified_plan = substrait_plan.clone();
        for ext in modified_plan.extensions.iter_mut() {
            if let Some(mapping_type) = &mut ext.mapping_type {
                if let MappingType::ExtensionFunction(func) = mapping_type {
                    if func.name == "approx_count_distinct:any" {
                        func.name = "approx_distinct:any".to_string();
                    }
                }
            }
        }

        let logical_plan = match from_substrait_plan(&ctx.state(), &modified_plan).await {
        Ok(plan) => plan,
        Err(e) => {
            error!("Failed to convert Substrait plan: {}", e);
            return Err(e);
        }
    };

    let dataframe = match ctx.execute_logical_plan(logical_plan).await {
        Ok(df) => df,
        Err(e) => {
            error!("Failed to execute logical plan: {}", e);
            return Err(e);
        }
    };

    // Get and display the physical plan
//     let physical_plan = dataframe.clone().create_physical_plan().await?;
//     let displayable_plan = displayable(physical_plan.as_ref()).indent(true);
//     println!("Physical Plan:\n{}", displayable_plan);

    let df_stream = match dataframe.execute_stream().await {
        Ok(stream) => stream,
        Err(e) => {
            error!("Failed to create execution stream: {}", e);
            return Err(e);
        }
    };

    Ok(get_cross_rt_stream(cpu_executor, df_stream))
}

fn get_cross_rt_stream(cpu_executor: DedicatedExecutor, df_stream: SendableRecordBatchStream) -> jlong {
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(
        df_stream,
        cpu_executor,
    );

    let wrapped_stream = RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    Box::into_raw(Box::new(wrapped_stream)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamNext(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    stream: jlong,
    callback: JObject,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            set_object_result_error(
                &mut env,
                callback,
                &DataFusionError::Execution("Runtime manager not initialized".to_string())
            );
            return;
        }
    };

    // Convert callback to GlobalRef
    let callback_ref = match env.new_global_ref(&callback) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to create global ref: {}", e);
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    let stream_ptr = stream;
    let io_runtime = manager.io_runtime.clone();

    // TODO : this can be 'io_runtime.block_on' if we see rust workers getting overloaded
    // benchmarks so far are good with spawn
    io_runtime.spawn(async move {

        let stream = unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
        // Poll the stream with monitoring
        let result = stream.try_next().await;

        // Uncomment for monitoring stream next
        // let result = STREAM_NEXT_MONITOR.instrument(async {
        //         stream.try_next().await
        // }).await;

        // Use thread-local JNI env - auto-attaches!
        with_jni_env(|env| {
            match result {
                Ok(Some(batch)) => {
                    // Convert to FFI
                    let struct_array: StructArray = batch.into();
                    let array_data = struct_array.into_data();
                    let ffi_array = FFI_ArrowArray::new(&array_data);
                    let ffi_array_ptr = Box::into_raw(Box::new(ffi_array));
                    set_object_result_ok_global(env, &callback_ref, ffi_array_ptr);
                }
                Ok(None) => {
                    // End of stream
                    set_object_result_ok_global(env, &callback_ref, std::ptr::null_mut::<FFI_ArrowSchema>());
                }
                Err(err) => {
                    error!("Stream next failed: {}", err);
                    set_object_result_error_global(env, &callback_ref, &err);
                }
            }
        });
    });
    // Function returns immediately to java - async rust work continues in background
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamGetSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
    callback: JObject,
) {
    if stream_ptr == 0 {
        set_object_result_error(
            &mut env,
            callback,
            &DataFusionError::Execution("Invalid stream pointer".to_string())
        );
        return;
    }
    // Schema access is synchronous and fast - no need for runtime
    let stream = unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
    //let stream = unsafe { &mut *(stream_ptr as *mut SendableRecordBatchStream) };

    let schema = stream.schema();
    match FFI_ArrowSchema::try_from(schema.as_ref()) {
        Ok(mut ffi_schema) => {
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
        }
        Err(err) => {
            set_object_result_error(&mut env, callback, &DataFusionError::Execution(
                format!("Schema conversion failed: {}", err)
            ));
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeFetchPhase(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    values: JLongArray,
    projections: JObjectArray,
    runtime_ptr: jlong,
    callback: JObject,
) -> jlong {
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    let table_path = shard_view.table_path();
    let files_metadata = shard_view.files_metadata();

    let projections: Vec<String> =
        parse_string_arr(&mut env, projections).expect("Expected list of files");

    // Safety checks first
    if values.is_null() {
        let _ = env.throw_new("java/lang/NullPointerException", "values array is null");
        return 0;
    }

    // 2. Get array length
    let array_length = match env.get_array_length(&values) {
        Ok(len) => len,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to get array length: {:?}", e),
            );
            return 0;
        }
    };

    // 3. Allocate Rust buffer
    let mut row_ids: Vec<jlong> = vec![0; array_length as usize];

    // 4. Copy Java array into Rust buffer
    match env.get_long_array_region(values, 0, &mut row_ids[..]) {
        Ok(_) => {
            println!("Received array: {:?}", row_ids);
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to get array data: {:?}", e),
            );
            return 0;
        }
    }

    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            error!("Runtime manager not initialized");
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return 0;
        }
    };

    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();

    let access_plans = create_access_plans(row_ids, files_metadata.clone());

    let object_meta: Arc<Vec<ObjectMeta>> = Arc::new(
        files_metadata
            .iter()
            .map(|metadata| (*metadata.object_meta).clone())
            .collect(),
    );

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), object_meta);

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(
            CacheManagerConfig::default().with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(runtime.runtime_env.cache_manager.get_file_metadata_cache())),
        )
        .build()
        .unwrap();
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime_env));

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet"); // TODO: take this as parameter
                                                                                    // .with_table_partition_cols(vec![("row_base".to_string(), DataType::Int32)]); // TODO: enable only for query phase

    // Ideally the executor will give this

    io_runtime.block_on(async {
        let parquet_schema = match listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await
        {
            Ok(schema) => schema,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to infer schema during fetch: {}", e),
                );
                return 0;
            }
        };

        // let total_groups = files_metadata[0].row_group_row_counts.len();
        // let mut access_plan = ParquetAccessPlan::new_all(total_groups);
        // for i in 0..total_groups {
        //     access_plan.skip(i);
        // }

        // let partitioned_files: Vec<PartitionedFile> = files_metadata
        //     .iter()
        //     .zip(access_plans.await.iter())
        //     .map(|(meta, access_plan)| {
        //         PartitionedFile::new(
        //             format!("{}/{}",
        //                     table_path.prefix().to_string().trim_end_matches('/'),
        //                     meta.object_meta().location.to_string().trim_start_matches('/')
        //             ),
        //             meta.object_meta.size
        //         ).with_extensions(Arc::new(access_plan.clone()))
        //     })
        //     .collect();

        let access_plans = match access_plans.await {
            Ok(plans) => plans,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to create access plans during fetch: {}", e),
                );
                return 0;
            }
        };

        let partitioned_files: Vec<PartitionedFile> = files_metadata
            .iter()
            .zip(access_plans.iter())
            .map(|(meta, access_plan)| {
                PartitionedFile::new(
                    meta.object_meta().location.to_string(),
                    meta.object_meta.size,
                )
                .with_extensions(Arc::new(access_plan.clone()))
            })
            .collect();

        let file_group = FileGroup::new(partitioned_files);

        let file_source = Arc::new(
            ParquetSource::default(), // provide the factory to create parquet reader without re-reading metadata
                                      //.with_parquet_file_reader_factory(Arc::new(reader_factory)),
        );

        let mut projection_index = vec![];

        for field_name in projections.iter() {
            projection_index.push(
                parquet_schema
                    .index_of(field_name)
                    .expect(format!("Projected field {} not found in Schema", field_name).as_str()),
            );
        }

        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            parquet_schema.clone(),
            file_source,
        )
        //.with_limit(limit)
        .with_projection(Option::from(projection_index.clone()))
        .with_file_group(file_group)
        .build();

        let parquet_exec = DataSourceExec::from_data_source(file_scan_config);

        // IMPORTANT: Only get one reference to each pointer
        // let liquid_ctx = unsafe { &mut *(context_ptr as *mut SessionContext) };
        // let session_ctx = unsafe { Box::from_raw(context_ptr as *mut SessionContext) };
        let optimized_plan: Arc<dyn ExecutionPlan> = parquet_exec.clone();

        let task_ctx = Arc::new(TaskContext::default());

        let stream = match optimized_plan.execute(0, task_ctx) {
            Ok(s) => s,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to execute plan during fetch: {}", e),
                );
                return 0;
            }
        };

        // Convert the stream to cross rt stream to execute in CPU executor while streaming
        get_cross_rt_stream(cpu_executor, stream)
    })
}

async fn create_access_plans(
    row_ids: Vec<jlong>,
    files_metadata: Arc<Vec<CustomFileMeta>>,
) -> Result<Vec<ParquetAccessPlan>, DataFusionError> {
    let mut access_plans = Vec::new();

    // Sort row_ids for better processing
    let mut sorted_row_ids: Vec<i64> = row_ids.iter().map(|&id| id as i64).collect();
    sorted_row_ids.sort_unstable();

    // Process each file
    for file_meta in files_metadata.iter() {
        let row_base = *file_meta.row_base;
        let total_row_groups = file_meta.row_group_row_counts.len();
        let mut access_plan = ParquetAccessPlan::new_all(total_row_groups);

        // Calculate file's row range
        let file_total_rows: i64 = file_meta.row_group_row_counts.iter().map(|&x| x).sum();
        let file_end_row: i64 = row_base + file_total_rows;
        // Filter row IDs that belong to this file
        let file_row_ids: Vec<i64> = sorted_row_ids
            .iter()
            .copied() // or .cloned() if it's not Copy
            .filter(|&id| id >= row_base && id < file_end_row)
            .map(|id| id - row_base)
            .collect();

        if file_row_ids.is_empty() {
            // If no rows belong to this file, skip all row groups
            for group_id in 0..total_row_groups {
                access_plan.skip(group_id);
            }
        } else {
            // Create cumulative row counts for row groups
            let mut cumulative_group_rows: Vec<i64> = Vec::with_capacity(total_row_groups + 1);
            cumulative_group_rows.push(0);
            let mut current_sum = 0;
            for &count in file_meta.row_group_row_counts.iter() {
                current_sum += count;
                cumulative_group_rows.push(current_sum);
            }
            // Group local row IDs by row group
            let mut group_map: HashMap<usize, BTreeSet<i32>> = HashMap::new();
            for &row_id in &file_row_ids {
                // Find the appropriate row group using binary search
                let group_id = cumulative_group_rows
                    .windows(2)
                    .position(|window| row_id >= window[0] as i64 && row_id < window[1] as i64)
                    .unwrap();

                // Calculate relative position within the row group
                let relative_pos = row_id - cumulative_group_rows[group_id];
                group_map
                    .entry(group_id)
                    .or_default()
                    .insert(relative_pos as i32);
            }

            // Process each row group
            for group_id in 0..total_row_groups {
                let row_group_size = file_meta.row_group_row_counts[group_id] as usize;

                if let Some(group_row_ids) = group_map.get(&group_id) {
                    let mut relative_row_ids: Vec<usize> =
                        group_row_ids.iter().map(|&x| x as usize).collect();
                    relative_row_ids.sort_unstable();

                    if relative_row_ids.is_empty() {
                        access_plan.skip(group_id);
                    } else if relative_row_ids.len() == row_group_size {
                        access_plan.scan(group_id);
                    } else {
                        // Create selectors
                        let mut selectors = Vec::new();
                        let mut current_pos = 0;
                        let mut i = 0;
                        while i < relative_row_ids.len() {
                            let mut target_pos = relative_row_ids[i];
                            if target_pos > current_pos {
                                selectors.push(RowSelector::skip(target_pos - current_pos));
                            }
                            let mut select_count = 1;
                            while i + 1 < relative_row_ids.len()
                                && relative_row_ids[i + 1] == relative_row_ids[i] + 1
                            {
                                select_count += 1;
                                i += 1;
                                target_pos = relative_row_ids[i];
                            }
                            selectors.push(RowSelector::select(select_count));
                            current_pos = relative_row_ids[i] + 1;
                            i += 1;
                        }
                        if current_pos < row_group_size {
                            selectors.push(RowSelector::skip(row_group_size - current_pos));
                        }
                        access_plan.set(group_id, RowGroupAccess::Selection(selectors.into()));
                    }
                } else {
                    access_plan.skip(group_id);
                }
            }
        }

        access_plans.push(access_plan);
    }

    Ok(access_plans)
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamClose(
    _env: JNIEnv,
    _class: JClass,
    stream: jlong,
) {
    let _ = unsafe { Box::from_raw(stream as *mut RecordBatchStreamAdapter<CrossRtStream>) };
}
