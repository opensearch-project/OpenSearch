use std::cell::RefCell;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::num::NonZeroUsize;
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::ptr::addr_of_mut;
use jni::objects::{GlobalRef, JByteArray, JClass, JMap, JObject};
use jni::objects::JLongArray;
use jni::sys::{jboolean, jbyteArray, jint, jlong, jlongArray, jstring};
use jni::{JNIEnv, JavaVM};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::SchemaRef;
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
mod metrics_collector;
mod metrics_layout;
mod cache_jni;
mod partial_agg_optimizer;
mod query_executor;
mod indexed_query_executor;
mod indexed_table;
mod project_row_id_analyzer;
pub mod logger;

// Import logger macros from shared crate
use vectorized_exec_spi::{log_info, log_error, log_debug};

use crate::custom_cache_manager::CustomCacheManager;
use crate::util::{create_file_meta_from_filenames, parse_string_arr, set_action_listener_error, set_action_listener_error_global, set_action_listener_ok, set_action_listener_ok_global, set_action_listener_ok_global_with_map};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};

use crate::statistics_cache::CustomStatisticsCache;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use object_store::ObjectMeta;
use tokio::runtime::Runtime;
use std::result;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{TryStreamExt, FutureExt};

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

static FETCH_PHASE_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(100)).clone()
});

static SEGMENT_STATS_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(100)).clone()
});

static INDEXED_QUERY_EXECUTION_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(100)).clone()
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

/// Extract a human-readable message from a panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Spawn an async task on `runtime` that calls an ActionListener exactly once.
///
/// The entire `task` future runs inside `catch_unwind`. Any panic is converted
/// to a `DataFusionError` and surfaced to the Java caller via `listener_ref`.
/// This ensures the `CompletableFuture` on the Java side is always completed,
/// never left hanging.
///
/// `on_ok` receives the success value and is responsible for calling the
/// appropriate `set_action_listener_ok_*` variant. `T` is inferred from
/// the closure, which in turn pins the `Output` type of `task`.
fn spawn_jni_task<Fut, T, FOk>(
    runtime: &tokio::runtime::Handle,
    task_name: &'static str,
    listener_ref: GlobalRef,
    task: Fut,
    on_ok: FOk,
)
where
    Fut: Future<Output = Result<T, DataFusionError>> + Send + 'static,
    T: Send + 'static,
    FOk: FnOnce(&mut JNIEnv, &GlobalRef, T) + Send + 'static,
{
    let _ = runtime.spawn(async move {
        let result = std::panic::AssertUnwindSafe(task)
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| {
                let msg = panic_message(&panic);
                log_error!("{} panicked: {}", task_name, msg);
                Err(DataFusionError::Execution(format!("{} panicked: {}", task_name, msg)))
            });

        with_jni_env(|env| match result {
            Ok(value) => on_ok(env, &listener_ref, value),
            Err(e) => {
                log_error!("{} failed: {}", task_name, e);
                set_action_listener_error_global(env, &listener_ref, &e);
            }
        });
    });
}

/// Helper: TaskMonitor → `[i64; 3]` flat array (from cumulative metrics)
fn task_monitor_to_longs(monitor: &TaskMonitor) -> [i64; metrics_layout::TASK_MONITOR_SIZE] {
    let m = monitor.cumulative();
    let mut buf = [0i64; metrics_layout::TASK_MONITOR_SIZE];
    buf[metrics_layout::TASK_MONITOR_TOTAL_POLL_DURATION_MS] = m.total_poll_duration.as_millis() as i64;
    buf[metrics_layout::TASK_MONITOR_TOTAL_SCHEDULED_DURATION_MS] = m.total_scheduled_duration.as_millis() as i64;
    buf[metrics_layout::TASK_MONITOR_TOTAL_IDLE_DURATION_MS] = m.total_idle_duration.as_millis() as i64;
    buf
}

// ── Flat long[] stats JNI function ──

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_stats<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlongArray {
    let mut buf = [0i64; metrics_layout::TOTAL_SIZE];

    // IO runtime [0..6] + CPU runtime [6..12]
    if let Some(rm) = TOKIO_RUNTIME_MANAGER.get() {
        let io_snap = rm.io_metrics.snapshot();
        buf[0..metrics_layout::RUNTIME_SIZE].copy_from_slice(&io_snap);

        if let Some(cpu) = rm.cpu_metrics.as_ref() {
            let cpu_snap = cpu.snapshot();
            buf[metrics_layout::RUNTIME_SIZE..metrics_layout::RUNTIME_SIZE * 2].copy_from_slice(&cpu_snap);
        }
    }

    // Task monitors [12..27]
    let base = metrics_layout::RUNTIME_SIZE * 2;
    let qe = task_monitor_to_longs(&QUERY_EXECUTION_MONITOR);
    buf[base..base + metrics_layout::TASK_MONITOR_SIZE].copy_from_slice(&qe);

    let sn = task_monitor_to_longs(&STREAM_NEXT_MONITOR);
    buf[base + metrics_layout::TASK_MONITOR_SIZE..base + metrics_layout::TASK_MONITOR_SIZE * 2].copy_from_slice(&sn);

    let fp = task_monitor_to_longs(&FETCH_PHASE_MONITOR);
    buf[base + metrics_layout::TASK_MONITOR_SIZE * 2..base + metrics_layout::TASK_MONITOR_SIZE * 3].copy_from_slice(&fp);

    let ss = task_monitor_to_longs(&SEGMENT_STATS_MONITOR);
    buf[base + metrics_layout::TASK_MONITOR_SIZE * 3..base + metrics_layout::TASK_MONITOR_SIZE * 4].copy_from_slice(&ss);

    let iq = task_monitor_to_longs(&INDEXED_QUERY_EXECUTION_MONITOR);
    buf[base + metrics_layout::TASK_MONITOR_SIZE * 4..base + metrics_layout::TASK_MONITOR_SIZE * 5].copy_from_slice(&iq);

    match env.new_long_array(metrics_layout::TOTAL_SIZE as i32) {
        Ok(arr) => {
            if let Err(e) = env.set_long_array_region(&arr, 0, &buf) {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to set stats long array region: {:?}", e),
                );
                return std::ptr::null_mut();
            }
            arr.into_raw()
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create stats long array: {:?}", e),
            );
            std::ptr::null_mut()
        }
    }
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
        let manager = Arc::new(RuntimeManager::new(cpu_threads as usize));

        manager
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


/// Log runtime metrics with performance analysis
#[allow(dead_code)]
fn log_runtime_metrics(metrics: &tokio_metrics::RuntimeMetrics) {
    log_info!("=== Runtime Metrics ===");
    log_info!("  Workers: {}", metrics.workers_count);
    log_info!("  Total polls: {}", metrics.total_polls_count);
    log_info!("  Total busy duration ms: {}", metrics.total_busy_duration.as_millis());
    log_info!("  Total overflow: {}", metrics.total_overflow_count);
    log_info!("  Global queue depth: {}", metrics.global_queue_depth);
    log_info!("  Blocking queue depth: {}", metrics.blocking_queue_depth);
    let query_metrics = QUERY_EXECUTION_MONITOR.cumulative();
    log_task_metrics("Query exec (via CrossRtStream)", &query_metrics);
    let stream_metrics = STREAM_NEXT_MONITOR.cumulative();
    log_task_metrics("Stream Next (via CrossRtStream)", &stream_metrics);
    log_info!("======================");
}

/// Log task metrics with performance analysis
#[allow(dead_code)]
fn log_task_metrics(operation: &str, metrics: &tokio_metrics::TaskMetrics) {
    log_info!("=== Task Metrics: {} ===", operation);
    log_info!("  Poll duration: {:?}", metrics.total_poll_duration);
    log_info!("  Scheduled duration: {:?}", metrics.total_scheduled_duration);
    log_info!("  Idle duration: {:?}", metrics.total_idle_duration);
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

/// Test JNI method to verify FFI boundary handling of sliced arrays.
/// Creates a sliced StringArray (simulating `head X from Y`) and returns FFI pointers.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createTestSlicedArray(
    mut env: JNIEnv,
    _class: JClass,
    offset: jint,
    length: jint,
    listener: JObject,
) {
    use arrow_schema::{Schema, Field, DataType};
    use arrow_array::StringArray;

    let original = StringArray::from(vec!["zero", "one", "two", "three", "four"]);
    let sliced = original.slice(offset as usize, length as usize);

    let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sliced)]).unwrap();

    let struct_array: StructArray = batch.into();
    let array_data = struct_array.to_data();

    let ffi_schema = FFI_ArrowSchema::try_from(array_data.data_type()).unwrap();
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;

    let ffi_array = FFI_ArrowArray::new(&array_data);
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;

    let result = env.new_long_array(2).unwrap();
    env.set_long_array_region(&result, 0, &[schema_ptr, array_ptr]).unwrap();

    let listener_class = env.get_object_class(&listener).unwrap();
    let on_response = env.get_method_id(&listener_class, "onResponse", "(Ljava/lang/Object;)V").unwrap();

    unsafe {
        env.call_method_unchecked(
            &listener,
            on_response,
            jni::signature::ReturnType::Primitive(jni::signature::Primitive::Void),
            &[jni::objects::JValue::Object(&result).as_jni()]
        ).unwrap();
    }
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

    let mut shard_view = ShardView::new(table_url, files_metadata);

    if let Some(first_meta) = shard_view.files_metadata().first() {
        let file_path = format!("/{}", first_meta.object_meta.location.as_ref().trim_start_matches('/'));
        if let Ok(file) = std::fs::File::open(&file_path) {
            if let Ok(builder) = datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file) {
                shard_view.set_cached_schema(builder.schema().clone());
            }
        }
    }

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
    cached_schema: Option<SchemaRef>,
}

impl ShardView {
    pub fn new(table_path: ListingTableUrl, files_metadata: Vec<CustomFileMeta>) -> Self {
        let files_metadata = Arc::new(files_metadata);
        ShardView {
            table_path,
            files_metadata,
            cached_schema: None,
        }
    }

    pub fn table_path(&self) -> ListingTableUrl {
        self.table_path.clone()
    }

    pub fn files_metadata(&self) -> Arc<Vec<CustomFileMeta>> {
        self.files_metadata.clone()
    }

    pub fn cached_schema(&self) -> Option<SchemaRef> {
        self.cached_schema.clone()
    }

    pub fn set_cached_schema(&mut self, schema: SchemaRef) {
        self.cached_schema = Some(schema);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStats {
    /// Total file size in bytes
    pub size: u64,

    /// Total number of rows in the file
    pub num_rows: i64,
}

impl FileStats {
    pub fn new(size: u64, num_rows: i64) -> Self {
        Self { size, num_rows }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn num_rows(&self) -> i64 {
        self.num_rows
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeQueryPhaseAsync(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: jbyteArray,
    is_query_plan_explain_enabled: jboolean,
    target_partitions: jint,
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

    let is_query_plan_explain_enabled: bool = is_query_plan_explain_enabled !=0;
    let target_partitions: usize = target_partitions as usize;

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
    let cached_schema = shard_view.cached_schema();

    spawn_jni_task(
        &io_runtime,
        "executeQueryPhaseAsync",
        listener_ref,
        QUERY_EXECUTION_MONITOR.instrument(query_executor::execute_query_with_cross_rt_stream(
            table_path,
            files_meta,
            table_name,
            plan_bytes_vec,
            is_query_plan_explain_enabled,
            target_partitions,
            runtime,
            cpu_executor,
            cached_schema,
        )),
        |env, listener_ref, stream_pointer| set_action_listener_ok_global(env, listener_ref, stream_pointer),
    );
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_fetchSegmentStats(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
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

    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let files_meta = shard_view.files_metadata();

    spawn_jni_task(
        &io_runtime,
        "fetchSegmentStats",
        listener_ref,
        SEGMENT_STATS_MONITOR.instrument(async move { util::fetch_segment_statistics(files_meta).await }),
        |env, listener_ref, stats_map| set_action_listener_ok_global_with_map(env, listener_ref, &stats_map),
    );
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

    // Ensure stream_ptr lifetime is guaranteed beyond the spawn boundary
    // (e.g., wrap in Arc<Mutex<...>> or ensure sequential access contract)
    spawn_jni_task(
        &io_runtime,
        "streamNext",
        listener_ref,
        STREAM_NEXT_MONITOR.instrument(async move {
            let stream = unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
            // Poll the stream with monitoring
            let result = stream.try_next().await?;

            match result {
                Some(batch) => {
                    log_info!("[RUST streamNext] Batch produced: {} rows, {} columns, schema: {:?}",
                        batch.num_rows(), batch.num_columns(), batch.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>());
                    // Convert to FFI
                    let struct_array: StructArray = batch.into();
                    let array_data = struct_array.into_data();
                    let ffi_array = FFI_ArrowArray::new(&array_data);
                    Ok(Box::into_raw(Box::new(ffi_array)) as jlong)
                }
                None => {
                    log_info!("[RUST streamNext] End of stream reached");
                    // End of stream
                    Ok(0)
                }
            }
        }),
        |env, listener_ref, data_pointer| set_action_listener_ok_global(env, listener_ref, data_pointer),
    );
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
    let cached_schema = shard_view.cached_schema();

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

    io_runtime.block_on(FETCH_PHASE_MONITOR.instrument(async {
        match query_executor::execute_fetch_phase(
            table_path,
            files_metadata,
            row_ids,
            include_fields,
            exclude_fields,
            runtime,
            cpu_executor,
            cached_schema,
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
    }))
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamClose(
    _env: JNIEnv,
    _class: JClass,
    stream: jlong,
) {
    let _ = unsafe { Box::from_raw(stream as *mut RecordBatchStreamAdapter<CrossRtStream>) };
}



/// Execute an indexed query asynchronously.
///
/// Registers an IndexedTableProvider under `tableName`, then executes the
/// substrait plan against it — same response path as executeQueryPhaseAsync.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeIndexedQueryAsync(
    mut env: JNIEnv,
    _class: JClass,
    weight_ptr: jlong,
    segment_max_docs: JLongArray,
    parquet_paths: JObjectArray,
    table_name: JString,
    substrait_bytes: jbyteArray,
    num_partitions: jint,
    bitset_mode: jint,
    is_query_plan_explain_enabled: jboolean,
    runtime_ptr: jlong,
    listener: JObject,
) {
    use crate::indexed_table::index::BitsetMode;

    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_error!("Runtime manager not initialized");
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return;
        }
    };

    // Extract all Java data before async block
    let seg_max_docs = {
        let len = match env.get_array_length(&segment_max_docs) {
            Ok(l) => l as usize,
            Err(e) => {
                set_action_listener_error(&mut env, listener,
                    &DataFusionError::Execution(format!("get_array_length: {}", e)));
                return;
            }
        };
        let mut buf = vec![0i64; len];
        if let Err(e) = env.get_long_array_region(segment_max_docs, 0, &mut buf) {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("get_long_array_region: {}", e)));
            return;
        }
        buf
    };

    let pq_paths = match parse_string_arr(&mut env, parquet_paths) {
        Ok(paths) => paths,
        Err(e) => {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("parse parquet paths: {}", e)));
            return;
        }
    };

    let tbl_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("Failed to get table name: {}", e)));
            return;
        }
    };

    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)));
            return;
        }
    };

    let n = (num_partitions as usize).max(1);
    let mode = match bitset_mode {
        1 => BitsetMode::Or,
        _ => BitsetMode::And,
    };

    let jvm = match JAVA_VM.get() {
        Some(vm) => Arc::new(unsafe {
            JavaVM::from_raw(vm.get_java_vm_pointer())
                .expect("Failed to create JavaVM from pointer")
        }),
        None => {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution("JavaVM not initialized".to_string()));
            return;
        }
    };

    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    // Pre-resolve the LuceneIndexSearcher class on the calling thread (which has the plugin classloader).
    // Tokio worker threads use the system classloader and can't find plugin classes.
    let searcher_class_ref = match env.find_class("org/opensearch/datafusion/search/LuceneIndexSearcher") {
        Ok(cls) => match env.new_global_ref(cls) {
            Ok(r) => r,
            Err(e) => {
                set_action_listener_error(&mut env, listener,
                    &DataFusionError::Execution(format!("Failed to create global ref for LuceneIndexSearcher: {}", e)));
                return;
            }
        },
        Err(e) => {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("Failed to find LuceneIndexSearcher class: {}", e)));
            return;
        }
    };

    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    let is_explain: bool = is_query_plan_explain_enabled != 0;

    // Use spawn + blocking channel instead of block_on.
    // block_on occupies the calling thread as a worker, causing RepartitionExec's
    // spawned tasks to serialize on that thread. spawn() lets the io_runtime's
    // worker threads handle the tasks concurrently.
    let (tx, rx) = std::sync::mpsc::channel();

    io_runtime.spawn(INDEXED_QUERY_EXECUTION_MONITOR.instrument(async move {
        let result = indexed_query_executor::execute_indexed_query_stream(
            weight_ptr,
            seg_max_docs,
            pq_paths,
            tbl_name,
            plan_bytes_vec,
            n,
            mode,
            is_explain,
            jvm,
            searcher_class_ref,
            runtime,
            cpu_executor,
        ).await;
        let _ = tx.send(result);
    }));

    let result = rx.recv().unwrap_or_else(|_| Err(DataFusionError::Execution("Channel closed".to_string())));

    match result {
        Ok(stream_ptr) => {
            set_action_listener_ok(&mut env, listener, stream_ptr);
        }
        Err(e) => {
            log_error!("Indexed query execution failed: {}", e);
            set_action_listener_error(&mut env, listener, &e);
        }
    }
}
