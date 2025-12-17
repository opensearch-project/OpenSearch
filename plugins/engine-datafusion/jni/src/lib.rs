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
use jni::objects::{JByteArray, JClass, JObject};
use jni::objects::JLongArray;
use jni::sys::{jboolean, jbyteArray, jint, jlong, jstring};
use jni::{JNIEnv, JavaVM};
use std::sync::{Arc, OnceLock};
use arrow_array::{Array, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use datafusion::{
    common::DataFusionError,
    datasource::listing::ListingTableUrl,
    execution::context::SessionContext,
    execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    execution::RecordBatchStream,
    prelude::*,
    DATAFUSION_VERSION,
};


use std::default::Default;
use std::path::PathBuf;
use std::time::{Duration, Instant};

mod util;
mod absolute_row_id_optimizer;
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
mod query_executor;
mod project_row_id_analyzer;
pub mod logger;

// Import logger macros from shared crate
use vectorized_exec_spi::{log_info, log_error, log_debug};

use crate::custom_cache_manager::CustomCacheManager;
use crate::util::{create_file_meta_from_filenames, parse_string_arr, set_action_listener_error, set_action_listener_error_global, set_action_listener_ok, set_action_listener_ok_global};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};

use crate::statistics_cache::CustomStatisticsCache;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use object_store::ObjectMeta;
use tokio::runtime::Runtime;
use std::result;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;

// NativeBridge JNI implementations
use jni::objects::{JObjectArray, JString};
use log::error;
use once_cell::sync::Lazy;
use tokio_metrics::TaskMonitor;
use crate::cross_rt_stream::CrossRtStream;
use crate::memory::{Monitor, MonitoredMemoryPool};
use crate::runtime_manager::RuntimeManager;

mod statistics_cache;
mod eviction_policy;

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

/// Initialize the logger for Rust->Java logging bridge.
/// This should be called once when the native library is loaded.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_initLogger(
    env: JNIEnv,
    _class: JClass,
) {
    // Initialize the logger with the JVM for Rust->Java logging bridge
    // This uses the shared logger from vectorized_exec_spi
    // The logger stores its own JVM reference internally
    vectorized_exec_spi::logger::init_logger_from_env(&env);
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_initTokioRuntimeManager(
    env: JNIEnv,
    _class: JClass,
    cpu_threads: jint,
) {
    // Initialize JavaVM for async callbacks from Tokio worker threads
    // This is needed so worker threads can attach to JVM and call ActionListener methods
    JAVA_VM.get_or_init(|| {
        env.get_java_vm().expect("Failed to get JavaVM")
    });

    TOKIO_RUNTIME_MANAGER.get_or_init(|| {
        log_info!("Runtime manager initialized with {} CPU threads", cpu_threads);
        Arc::new(RuntimeManager::new(cpu_threads as usize))
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_shutdownTokioRuntimeManager(
    _env: JNIEnv,
    _class: JClass,
) {
    log_info!("Runtime manager shut down started");
    if let Some(mgr) = TOKIO_RUNTIME_MANAGER.get() {
        mgr.shutdown();
        log_info!("Runtime manager shut down successfully");
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
            log_info!("Tokio runtime manager not initialized");
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
    log_info!("=== Runtime Metrics ===");
    log_info!("  Workers: {}", metrics.workers_count);
    log_info!("  Global queue depth: {}", metrics.global_queue_depth);
    /*
    //unstable tokio causes build failures, uncomment this when monitoring

    log_info!("  Worker overflow: {}", metrics.total_overflow_count);
    log_info!("  Remote schedule: {}", metrics.max_local_schedule_count);
    log_info!("  Worker steal ops: {}", metrics.total_steal_operations);
    log_info!("  Blocking queue depth: {}", metrics.blocking_queue_depth);
    log_info!("  Max local queue depth: {}", metrics.max_local_queue_depth);
    log_info!("  Min local queue depth: {}", metrics.min_local_queue_depth);
    log_info!("  Max local schedule count: {}", metrics.max_local_schedule_count);
    log_info!("  Min local schedule count: {}", metrics.min_local_schedule_count);
    log_info!("  Queue depth: {}", metrics.total_local_queue_depth);
    log_info!("  Total schedule count: {}", metrics.total_local_schedule_count);
    */
    let query_metrics = QUERY_EXECUTION_MONITOR.cumulative();
    log_task_metrics("Query exec (via CrossRtStream)", &query_metrics);
    let stream_metrics = STREAM_NEXT_MONITOR.cumulative();
    log_task_metrics("Stream Next (via CrossRtStream)", &stream_metrics);
    log_info!("======================");
}

/// Log task metrics with performance analysis
fn log_task_metrics(operation: &str, metrics: &tokio_metrics::TaskMetrics) {
    log_info!("=== Task Metrics: {} ===", operation);
    log_info!("  Scheduled duration: {:?}", metrics.total_scheduled_duration);
    log_info!("  Poll duration: {:?}", metrics.total_poll_duration);
    log_info!("  Idle duration: {:?}", metrics.total_idle_duration);
    log_info!("  Mean poll duration: {:?}", metrics.mean_poll_duration());
    log_info!("  Slow poll ratio: {:.2}%", metrics.slow_poll_ratio() * 100.0);
    log_info!("  Mean first poll delay: {:?}", metrics.mean_first_poll_delay());
    log_info!("  Total slow polls: {}", metrics.total_slow_poll_count);
    log_info!("  Total long delays: {}", metrics.total_long_delay_count);
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createGlobalRuntime(
    mut env: JNIEnv,
    _class: JClass,
    memory_pool_limit: jlong,
    cache_manager_ptr: jlong,
    spill_dir: JString,
    spill_limit: jlong
) -> jlong {
    let spill_dir: String = match env.get_string(&spill_dir) {
        Ok(path) => path.into(),
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid table path: {:?}", e),
            );
            return 0;
        }
    };

    let mut builder = DiskManagerBuilder::default()
        .with_max_temp_directory_size(spill_limit as u64);
    log_info!("Spill Limit is being set to : {}", spill_limit);
    let builder = builder.with_mode(DiskManagerMode::Directories(vec![PathBuf::from(spill_dir)]));

    let monitor = Arc::new(Monitor::default());
    let memory_pool = Arc::new(MonitoredMemoryPool::new(
        Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(memory_pool_limit as usize),
            NonZeroUsize::new(5).unwrap(),
        )),
        monitor.clone(),
    ));

    let (cache_manager_config, custom_cache_manager) = match cache_manager_ptr {
        0 => {
            (CacheManagerConfig::default(), None)
        }
        _ => {
            let custom_cache_manager = unsafe { *Box::from_raw(cache_manager_ptr as *mut CustomCacheManager) };
            (custom_cache_manager.build_cache_manager_config(), Some(custom_cache_manager))
        }
    };

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(cache_manager_config)
        .with_memory_pool(memory_pool.clone())
        .with_disk_manager_builder(builder)
        .build().unwrap();

    let runtime = DataFusionRuntime {
        runtime_env,
        custom_cache_manager,
        monitor,
    };

    Box::into_raw(Box::new(runtime)) as jlong
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

    let mut files: Vec<String> = match parse_string_arr(&mut env, files) {
        Ok(files) => files,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid file list: {}", e),
            );
            return 0;
        }
    };

    // TODO: This works since files are named similarly ending with incremental generation count, preferably move this up to DatafusionReaderManager to keep file order
    files.sort();
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
    listener: JObject,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_info!("Runtime manager not initialized");
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return;
        }
    };

    // ===== EXTRACT ALL JAVA DATA BEFORE ASYNC BLOCK =====
    let table_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("Failed to get table name: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to get table name: {}", e)));
            return;
        }
    };

    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            log_error!("Failed to convert plan bytes: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)));
            return;
        }
    };

    // Convert listener to GlobalRef (thread-safe)
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
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

    io_runtime.block_on(async move {

        let result = query_executor::execute_query_with_cross_rt_stream(
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
                    set_action_listener_ok_global(env, &listener_ref, stream_ptr);
                });
            }
            Err(e) => {
                with_jni_env(|env| {
                    log_error!("Query execution failed: {}", e);
                    set_action_listener_error_global(env, &listener_ref, &e);
                });
            }
        }
    });
}



#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamNext(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    stream: jlong,
    listener: JObject,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            set_action_listener_error(
                &mut env,
                listener,
                &DataFusionError::Execution("Runtime manager not initialized".to_string())
            );
            return;
        }
    };

    // Convert listener to GlobalRef
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    let stream_ptr = stream;
    let io_runtime = manager.io_runtime.clone();

    // TODO : this can be 'io_runtime.block_on' if we see rust workers getting overloaded
    // benchmarks so far are good with spawn
    // TODO : Thread leaks in tests if its spawn
    io_runtime.block_on(async move {

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
                    set_action_listener_ok_global(env, &listener_ref, ffi_array_ptr as jlong);
                }
                Ok(None) => {
                    // End of stream
                    set_action_listener_ok_global(env, &listener_ref, 0);
                }
                Err(err) => {
                    log_error!("Stream next failed: {}", err);
                    set_action_listener_error_global(env, &listener_ref, &err);
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
    listener: JObject,
) {
    if stream_ptr == 0 {
        set_action_listener_error(
            &mut env,
            listener,
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
            set_action_listener_ok(&mut env, listener, addr_of_mut!(ffi_schema) as jlong);
        }
        Err(err) => {
            set_action_listener_error(&mut env, listener, &DataFusionError::Execution(
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
    include_fields: JObjectArray,
    exclude_fields: JObjectArray,
    runtime_ptr: jlong,
    callback: JObject,
) -> jlong {
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    let table_path = shard_view.table_path();
    let files_metadata = shard_view.files_metadata();

    let include_fields: Vec<String> =
        parse_string_arr(&mut env, include_fields).expect("Expected list of files");
    let exclude_fields: Vec<String> =
        parse_string_arr(&mut env, exclude_fields).expect("Expected list of files");

    // Safety checks first
    if values.is_null() {
        let _ = env.throw_new("java/lang/NullPointerException", "values array is null");
        return 0;
    }

    // Get array length
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

    // Allocate Rust buffer
    let mut row_ids: Vec<jlong> = vec![0; array_length as usize];

    // Copy Java array into Rust buffer
    match env.get_long_array_region(values, 0, &mut row_ids[..]) {
        Ok(_) => {
            log_debug!("Received array: {:?}", row_ids);
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
            log_error!("Runtime manager not initialized");
            set_action_listener_error(&mut env, callback,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return 0;
        }
    };

    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();

    io_runtime.block_on(async {
        match query_executor::execute_fetch_phase(
            table_path,
            files_metadata,
            row_ids,
            include_fields,
            exclude_fields,
            runtime,
            cpu_executor,
        ).await {
            Ok(stream_ptr) => stream_ptr,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to execute fetch phase: {}", e),
                );
                0 // return 0
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamClose(
    _env: JNIEnv,
    _class: JClass,
    stream: jlong,
) {
    let _ = unsafe { Box::from_raw(stream as *mut RecordBatchStreamAdapter<CrossRtStream>) };
}
