/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use arrow_array::StructArray;
use arrow_array::Array;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_array::ffi::FFI_ArrowArray;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::{RecordBatchStream, SessionState, SessionStateBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionConfig;
use futures::TryStreamExt;
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString};
use jni::sys::{jint, jlong};
use jni::{JNIEnv, JavaVM};
use log::error;
use object_store::ObjectMeta;

pub mod cross_rt_stream;
pub mod executor;
pub mod io;
pub mod query_executor;
pub mod runtime_manager;
pub mod util;

use crate::cross_rt_stream::CrossRtStream;
use crate::runtime_manager::RuntimeManager;
use crate::util::*;

// Global state
static TOKIO_RUNTIME_MANAGER: OnceLock<Arc<RuntimeManager>> = OnceLock::new();
static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

thread_local! {
    static THREAD_JNIENV: RefCell<Option<JNIEnv<'static>>> = RefCell::new(None);
}

fn with_jni_env<F, R>(f: F) -> R
where
    F: FnOnce(&mut JNIEnv) -> R,
{
    THREAD_JNIENV.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            let jvm = JAVA_VM.get().expect("JavaVM not initialized");
            let env = jvm
                .attach_current_thread_permanently()
                .expect("Failed to attach thread to JVM");
            *opt = Some(env);
        }
        f(opt.as_mut().unwrap())
    })
}

// Native types for runtime and reader

pub struct DataFusionRuntime {
    pub runtime_env: RuntimeEnv,
    /// Pre-built session state with all default features registered.
    /// Cloned per query to avoid re-registering functions and optimizer rules.
    pub session_state_template: SessionState,
}

struct ShardView {
    table_path: ListingTableUrl,
    object_metas: Arc<Vec<ObjectMeta>>,
}

// Tokio runtime management

#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_initTokioRuntimeManager(
    mut env: JNIEnv,
    _class: JClass,
    cpu_threads: jint,
) {
    jni_safe(&mut env, (), |env| {
        JAVA_VM.get_or_init(|| env.get_java_vm().expect("Failed to get JavaVM"));
        TOKIO_RUNTIME_MANAGER.get_or_init(|| Arc::new(RuntimeManager::new(cpu_threads as usize)));
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_shutdownTokioRuntimeManager(
    mut env: JNIEnv,
    _class: JClass,
) {
    jni_safe(&mut env, (), |_env| {
        if let Some(mgr) = TOKIO_RUNTIME_MANAGER.get() {
            mgr.shutdown();
        }
    });
}

// Create DataFusion global runtime with user defined configuration
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_createGlobalRuntime(
    mut env: JNIEnv,
    _class: JClass,
    memory_pool_limit: jlong,
    _cache_manager_ptr: jlong,
    spill_dir: JString,
    spill_limit: jlong,
) -> jlong {
    jni_safe(&mut env, 0, |env| {
        let spill_dir: String = match env.get_string(&spill_dir) {
            Ok(s) => s.into(),
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/IllegalArgumentException",
                    format!("Invalid spill dir: {:?}", e),
                );
                return 0;
            }
        };

        let disk_manager = DiskManagerBuilder::default()
            .with_max_temp_directory_size(spill_limit as u64)
            .with_mode(DiskManagerMode::Directories(vec![PathBuf::from(spill_dir)]));

        let memory_pool = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(memory_pool_limit as usize),
            NonZeroUsize::new(5).unwrap(),
        ));

        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .with_disk_manager_builder(disk_manager)
            .build()
            .unwrap();

        let mut config = SessionConfig::new();
        config.options_mut().execution.parquet.pushdown_filters = false;
        config.options_mut().execution.target_partitions = 4;
        config.options_mut().execution.batch_size = 8192;

        let session_state_template = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::from(runtime_env.clone()))
            .with_default_features()
            .build();

        let runtime = DataFusionRuntime { runtime_env, session_state_template };
        Box::into_raw(Box::new(runtime)) as jlong
    })
}

// Close DataFusion global runtime
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_closeGlobalRuntime(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    jni_safe(&mut env, (), |_env| {
        if ptr != 0 {
            let _ = unsafe { Box::from_raw(ptr as *mut DataFusionRuntime) };
        }
    });
}

// Create datafusion reader by representing it in shard view
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_createDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    files: JObjectArray,
) -> jlong {
    jni_safe(&mut env, 0, |env| {
        let table_path: String = match env.get_string(&table_path) {
            Ok(s) => s.into(),
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/IllegalArgumentException",
                    format!("Invalid table path: {:?}", e),
                );
                return 0;
            }
        };

        let mut filenames = match parse_string_arr(env, files) {
            Ok(f) => f,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/IllegalArgumentException",
                    format!("Invalid file list: {}", e),
                );
                return 0;
            }
        };
        filenames.sort();

        let manager = match TOKIO_RUNTIME_MANAGER.get() {
            Some(m) => m,
            None => {
                let _ = env.throw_new(
                    "java/lang/IllegalStateException",
                    "Runtime manager not initialized",
                );
                return 0;
            }
        };

        let table_url = match ListingTableUrl::parse(&table_path) {
            Ok(u) => u,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Invalid table path: {}", e),
                );
                return 0;
            }
        };

        let default_rt = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .build()
            .unwrap();

        let store = match default_rt.object_store(&table_url) {
            Ok(s) => s,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to resolve object store: {}", e),
                );
                return 0;
            }
        };

        let tp = table_path.clone();
        let object_metas = match manager.io_runtime.block_on(
            create_object_metas(store.as_ref(), &tp, filenames),
        ) {
            Ok(m) => m,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to create metadata: {}", e),
                );
                return 0;
            }
        };

        let shard_view = ShardView {
            table_path: table_url,
            object_metas: Arc::new(object_metas),
        };
        Box::into_raw(Box::new(shard_view)) as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_closeDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    jni_safe(&mut env, (), |_env| {
        if ptr != 0 {
            let _ = unsafe { Box::from_raw(ptr as *mut ShardView) };
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_executeQueryAsync(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: JObject, // byte[]
    runtime_ptr: jlong,
    listener: JObject,
) {
    jni_safe(&mut env, (), |env| {
        let manager = match TOKIO_RUNTIME_MANAGER.get() {
            Some(m) => m,
            None => {
                set_action_listener_error(
                    env,
                    listener,
                    &DataFusionError::Execution("Runtime manager not initialized".to_string()),
                );
                return;
            }
        };

        let table_name: String = match env.get_string(&JString::from(table_name)) {
            Ok(s) => s.into(),
            Err(e) => {
                set_action_listener_error(
                    env,
                    listener,
                    &DataFusionError::Execution(format!("Invalid table name: {}", e)),
                );
                return;
            }
        };

        let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes.as_raw()) };
        let plan_bytes = match env.convert_byte_array(plan_bytes_obj) {
            Ok(b) => b,
            Err(e) => {
                set_action_listener_error(
                    env,
                    listener,
                    &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)),
                );
                return;
            }
        };

        let listener_ref = match env.new_global_ref(&listener) {
            Ok(r) => r,
            Err(e) => {
                set_action_listener_error(
                    env,
                    listener,
                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)),
                );
                return;
            }
        };

        let io_runtime = manager.io_runtime.clone();
        let cpu_executor = manager.cpu_executor();

        let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
        let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

        let table_path = shard_view.table_path.clone();
        let object_metas = shard_view.object_metas.clone();

        io_runtime.block_on(async move {
            let result = query_executor::execute_query(
                table_path,
                object_metas,
                table_name,
                plan_bytes,
                runtime,
                cpu_executor,
            )
            .await;

            with_jni_env(|env| match result {
                Ok(stream_ptr) => set_action_listener_ok_global(env, &listener_ref, stream_ptr),
                Err(e) => {
                    error!("Query execution failed: {}", e);
                    set_action_listener_error_global(env, &listener_ref, &e);
                }
            });
        });
    });
}

// Stream operations (async)
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_streamGetSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
    listener: JObject,
) {
    jni_safe(&mut env, (), |env| {
        if stream_ptr == 0 {
            set_action_listener_error(
                env,
                listener,
                &DataFusionError::Execution("Invalid stream pointer".to_string()),
            );
            return;
        }
        let stream =
            unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
        let schema = stream.schema();
        match FFI_ArrowSchema::try_from(schema.as_ref()) {
            Ok(ffi_schema) => {
                let schema_ptr = Box::into_raw(Box::new(ffi_schema));
                set_action_listener_ok(env, listener, schema_ptr as jlong);
            }
            Err(e) => {
                set_action_listener_error(
                    env,
                    listener,
                    &DataFusionError::Execution(format!("Schema conversion failed: {}", e)),
                );
            }
        }
    });
}

// This method pulls the next batch of the stream
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_streamNext(
    mut env: JNIEnv,
    _class: JClass,
    _runtime_ptr: jlong,
    stream_ptr: jlong,
    listener: JObject,
) {
    jni_safe(&mut env, (), |env| {
        let manager = match TOKIO_RUNTIME_MANAGER.get() {
            Some(m) => m,
            None => {
                set_action_listener_error(
                    env,
                    listener,
                    &DataFusionError::Execution("Runtime manager not initialized".to_string()),
                );
                return;
            }
        };

        let listener_ref = match env.new_global_ref(&listener) {
            Ok(r) => r,
            Err(e) => {
                set_action_listener_error(
                    env,
                    listener,
                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)),
                );
                return;
            }
        };

        let io_runtime = manager.io_runtime.clone();

        io_runtime.block_on(async move {
            let stream =
                unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
            let result = stream.try_next().await;

            with_jni_env(|env| match result {
                Ok(Some(batch)) => {
                    let struct_array: StructArray = batch.into();
                    let array_data = struct_array.into_data();
                    let ffi_array = FFI_ArrowArray::new(&array_data);
                    let ffi_array_ptr = Box::into_raw(Box::new(ffi_array));
                    set_action_listener_ok_global(env, &listener_ref, ffi_array_ptr as jlong);
                }
                Ok(None) => {
                    set_action_listener_ok_global(env, &listener_ref, 0);
                }
                Err(e) => {
                    error!("Stream next failed: {}", e);
                    set_action_listener_error_global(env, &listener_ref, &e);
                }
            });
        });
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_streamClose(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
) {
    jni_safe(&mut env, (), |_env| {
        if stream_ptr != 0 {
            let _ =
                unsafe { Box::from_raw(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
        }
    });
}

// JNI: SQL → Substrait (test helper)
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_sqlToSubstrait(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    sql: JString,
    runtime_ptr: jlong,
) -> jni::sys::jbyteArray {
    jni_safe(&mut env, std::ptr::null_mut(), |env| {
        use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
        use datafusion::execution::cache::cache_manager::CacheManagerConfig;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion_substrait::logical_plan::producer::to_substrait_plan;
        use prost::Message;

        let manager = TOKIO_RUNTIME_MANAGER.get().expect("Runtime manager not initialized");
        let table_name: String = env.get_string(&table_name).expect("Invalid table name").into();
        let sql: String = env.get_string(&sql).expect("Invalid SQL").into();

        let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
        let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };
        let table_path = shard_view.table_path.clone();
        let object_metas = shard_view.object_metas.clone();

        let plan_bytes = manager.io_runtime.block_on(async {
            let list_file_cache = Arc::new(DefaultListFilesCache::default());
            list_file_cache.put(
                &datafusion::execution::cache::TableScopedPath { table: None, path: table_path.prefix().clone() },
                object_metas,
            );
            let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
                .with_cache_manager(
                    CacheManagerConfig::default()
                        .with_list_files_cache(Some(list_file_cache))
                        .with_file_metadata_cache(Some(runtime.runtime_env.cache_manager.get_file_metadata_cache()))
                        .with_files_statistics_cache(runtime.runtime_env.cache_manager.get_file_statistic_cache()),
                )
                .build().expect("runtime env");

            let state = runtime.session_state_template.clone()
                .with_runtime_env(Arc::from(runtime_env));
            let ctx = datafusion::prelude::SessionContext::new_with_state(state);

            let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
                .with_file_extension(".parquet").with_collect_stat(true);
            let schema = listing_options.infer_schema(&ctx.state(), &table_path).await.expect("schema");
            let config = ListingTableConfig::new(table_path).with_listing_options(listing_options).with_schema(schema);
            ctx.register_table(&table_name, Arc::new(ListingTable::try_new(config).expect("table"))).expect("register");

            let plan = ctx.sql(&sql).await.expect("sql").logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &ctx.state()).expect("substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        });

        env.byte_array_from_slice(&plan_bytes).expect("byte array").into_raw()
    })
}

// Cache management

#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_cacheManagerAddFiles(
    mut env: JNIEnv,
    _class: JClass,
    _runtime_ptr: jlong,
    _file_paths: JObjectArray,
) {
    jni_safe(&mut env, (), |_env| {
        // TODO: wire to native cache manager when cache config is passed to createGlobalRuntime
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_cacheManagerRemoveFiles(
    mut env: JNIEnv,
    _class: JClass,
    _runtime_ptr: jlong,
    _file_paths: JObjectArray,
) {
    jni_safe(&mut env, (), |_env| {
        // TODO: wire to native cache manager when cache config is passed to createGlobalRuntime
    });
}

// JNI: Logger

#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_initLogger(
    mut env: JNIEnv,
    _class: JClass,
) {
    jni_safe(&mut env, (), |_env| {
        // TODO: wire Rust→Java logging bridge
    });
}
