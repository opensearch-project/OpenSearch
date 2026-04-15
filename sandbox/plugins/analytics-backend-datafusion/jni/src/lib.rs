/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! JNI bridge layer.
//!
//! This module is a thin adapter between Java's JNI types and the bridge-agnostic
//! API in [`api`]. All core logic lives in `api.rs` and `query_executor.rs`.
//! When migrating to JDK FFM, replace this file with an `extern "C"` bridge
//! that calls the same `api::*` functions.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::cell::RefCell;
use std::sync::{Arc, OnceLock};

use datafusion::common::DataFusionError;
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString, JValue};
use jni::sys::{jint, jlong};
use jni::{JNIEnv, JavaVM};
use log::error;

pub mod api;
pub mod cross_rt_stream;
pub mod executor;
pub mod indexed_table;
pub mod io;
pub mod query_executor;
pub mod runtime_manager;
pub mod util;

use jni_macros::jni_safe;

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

fn get_tokio_rt_manager() -> Result<&'static Arc<RuntimeManager>, DataFusionError> {
    TOKIO_RUNTIME_MANAGER
        .get()
        .ok_or_else(|| DataFusionError::Execution("Runtime manager not initialized".to_string()))
}

// Tokio runtime management
#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_initTokioRuntimeManager(
    mut env: JNIEnv,
    _class: JClass,
    cpu_threads: jint,
) {
    JAVA_VM.get_or_init(|| env.get_java_vm().expect("Failed to get JavaVM"));
    TOKIO_RUNTIME_MANAGER.get_or_init(|| Arc::new(RuntimeManager::new(cpu_threads as usize)));
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_shutdownTokioRuntimeManager(
    mut env: JNIEnv,
    _class: JClass,
) {
    if let Some(mgr) = TOKIO_RUNTIME_MANAGER.get() {
        mgr.shutdown();
    }
}

// Create DataFusion global runtime with user defined configuration
#[jni_safe(default = 0)]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_createGlobalRuntime(
    mut env: JNIEnv,
    _class: JClass,
    memory_pool_limit: jlong,
    _cache_manager_ptr: jlong,
    spill_dir: JString,
    spill_limit: jlong,
) -> jlong {
    let spill_dir: String = match env.get_string(&spill_dir) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", format!("Invalid spill dir: {:?}", e));
            return 0;
        }
    };

    match api::create_global_runtime(memory_pool_limit, &spill_dir, spill_limit) {
        Ok(ptr) => ptr as jlong,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            0
        }
    }
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_closeGlobalRuntime(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    unsafe { api::close_global_runtime(ptr as i64) };
}

// Create datafusion reader backed by shard view/catalog snapshot associated files
#[jni_safe(default = 0)]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_createDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    files: JObjectArray,
) -> jlong {
    let table_path: String = match env.get_string(&table_path) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", format!("Invalid table path: {:?}", e));
            return 0;
        }
    };
    let filenames = match parse_string_arr(env, files) {
        Ok(f) => f,
        Err(e) => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", format!("Invalid file list: {}", e));
            return 0;
        }
    };
    let tokio_rt_mgr = match get_tokio_rt_manager() {
        Ok(m) => m,
        Err(e) => {
            let _ = env.throw_new("java/lang/IllegalStateException", e.to_string());
            return 0;
        }
    };

    match api::create_reader(&table_path, filenames, tokio_rt_mgr) {
        Ok(ptr) => ptr as jlong,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            0
        }
    }
}


#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_closeDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    unsafe { api::close_reader(ptr as i64) };
}

// Executes the query for the substrait plan and returns a stream handle to listener
#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_executeQueryAsync(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: JObject,
    runtime_ptr: jlong,
    listener: JObject,
) {
    let tokio_rt_mgr = match get_tokio_rt_manager() {
        Ok(m) => m,
        Err(e) => {
            set_action_listener_error(env, listener, &e);
            return;
        }
    };

    let table_name: String = match env.get_string(&JString::from(table_name)) {
        Ok(s) => s.into(),
        Err(e) => {
            set_action_listener_error(env, listener, &DataFusionError::Execution(format!("Invalid table name: {}", e)));
            return;
        }
    };
    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes.as_raw()) };
    let plan_bytes = match env.convert_byte_array(plan_bytes_obj) {
        Ok(b) => b,
        Err(e) => {
            set_action_listener_error(env, listener, &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)));
            return;
        }
    };
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            set_action_listener_error(env, listener, &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    // Delegate to bridge-agnostic API — bridge does the block_on
    let result = tokio_rt_mgr.io_runtime.block_on(unsafe {
        api::execute_query(shard_view_ptr as i64, &table_name, &plan_bytes, runtime_ptr as i64, tokio_rt_mgr)
    });

    with_jni_env(|env| match result {
        Ok(stream_ptr) => set_action_listener_ok_global(env, &listener_ref, stream_ptr as jlong),
        Err(e) => {
            error!("Query execution failed: {}", e);
            set_action_listener_error_global(env, &listener_ref, &e);
        }
    });
}

// Get schema for the stream
#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_streamGetSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
    listener: JObject,
) {
    if stream_ptr == 0 {
        set_action_listener_error(env, listener, &DataFusionError::Execution("Invalid stream pointer".to_string()));
        return;
    }
    match unsafe { api::stream_get_schema(stream_ptr as i64) } {
        Ok(schema_ptr) => set_action_listener_ok(env, listener, schema_ptr as jlong),
        Err(e) => set_action_listener_error(env, listener, &e),
    }
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_streamNext(
    mut env: JNIEnv,
    _class: JClass,
    _runtime_ptr: jlong,
    stream_ptr: jlong,
    listener: JObject,
) {
    let manager = match get_tokio_rt_manager() {
        Ok(m) => m,
        Err(e) => {
            set_action_listener_error(env, listener, &e);
            return;
        }
    };

    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            set_action_listener_error(env, listener, &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    let result = manager.io_runtime.block_on(unsafe { api::stream_next(stream_ptr as i64) });

    with_jni_env(|env| match result {
        Ok(array_ptr) => set_action_listener_ok_global(env, &listener_ref, array_ptr as jlong),
        Err(e) => {
            error!("Stream next failed: {}", e);
            set_action_listener_error_global(env, &listener_ref, &e);
        }
    });
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_streamClose(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
) {
    unsafe { api::stream_close(stream_ptr as i64) };
}

// Only used for tests
#[jni_safe(default = std::ptr::null_mut())]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_sqlToSubstrait(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    sql: JString,
    runtime_ptr: jlong,
) -> jni::sys::jbyteArray {
    let manager = TOKIO_RUNTIME_MANAGER.get().expect("Runtime manager not initialized");
    let table_name: String = env.get_string(&table_name).expect("Invalid table name").into();
    let sql: String = env.get_string(&sql).expect("Invalid SQL").into();

    let result = unsafe {
        api::sql_to_substrait(shard_view_ptr as i64, &table_name, &sql, runtime_ptr as i64, manager)
    };

    match result {
        Ok(bytes) => env.byte_array_from_slice(&bytes).expect("byte array").into_raw(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            std::ptr::null_mut()
        }
    }
}

// Tests panic, only used for testing
#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_testPanic(
    mut env: JNIEnv,
    _class: JClass,
    message: JString,
) {
    let msg: String = env.get_string(&message).expect("Invalid message").into();
    panic!("{}", msg);
}

// Cache manager stubs
#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_cacheManagerAddFiles(
    mut env: JNIEnv,
    _class: JClass,
    _runtime_ptr: jlong,
    _file_paths: JObjectArray,
) {
    // TODO: wire to native cache manager
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_cacheManagerRemoveFiles(
    mut env: JNIEnv,
    _class: JClass,
    _runtime_ptr: jlong,
    _file_paths: JObjectArray,
) {
    // TODO: wire to native cache manager
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_initLogger(
    mut env: JNIEnv,
    _class: JClass,
) {
    // TODO: wire Rust→Java logging bridge
}

// ── Tree Query Execution ───────────────────────────────────────────────

/// JNI entry point for boolean tree query execution.
///
/// Deserializes the tree from bytes, creates JniTreeShardSearcher per Collector leaf,
/// resolves predicates from the Substrait plan, builds TreeIndexedTableProvider,
/// and executes via DataFusion. The result stream pointer is delivered via ActionListener.
#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_executeTreeQueryAsync(
    mut env: JNIEnv,
    _class: JClass,
    tree_bytes: JObject,        // byte[]
    bridge_context_id: jlong,
    _segment_max_docs: JObject, // long[]
    _parquet_paths: JObject,    // String[]
    table_name: JString,
    substrait_bytes: JObject,   // byte[]
    _num_partitions: jint,
    _index_leaf_count: jint,
    _is_explain_enabled: u8,
    _runtime_ptr: jlong,
    listener: JObject,
) {
    let tokio_rt_mgr = match get_tokio_rt_manager() {
        Ok(m) => m,
        Err(e) => {
            set_action_listener_error(env, listener, &e);
            return;
        }
    };

    // Parse tree bytes
    let tree_bytes_arr = unsafe { JByteArray::from_raw(tree_bytes.as_raw()) };
    let tree_data = match env.convert_byte_array(tree_bytes_arr) {
        Ok(b) => b,
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Failed to convert tree bytes: {}", e)),
            );
            return;
        }
    };

    // Deserialize the boolean tree
    let bool_node = match indexed_table::BoolNode::deserialize(&tree_data) {
        Ok(n) => n,
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Failed to deserialize tree: {}", e)),
            );
            return;
        }
    };

    // Parse table name
    let table_name_str: String = match env.get_string(&JString::from(table_name)) {
        Ok(s) => s.into(),
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Invalid table name: {}", e)),
            );
            return;
        }
    };

    // Parse substrait plan bytes
    let plan_bytes_arr = unsafe { JByteArray::from_raw(substrait_bytes.as_raw()) };
    let plan_bytes = match env.convert_byte_array(plan_bytes_arr) {
        Ok(b) => b,
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Failed to convert substrait bytes: {}", e)),
            );
            return;
        }
    };

    // Get JVM reference for JNI callbacks
    let jvm = match JAVA_VM.get() {
        Some(vm) => Arc::new(unsafe {
            // Safety: we need an owned JavaVM for the searcher, but JAVA_VM is a global static.
            // We create a new Arc wrapping the raw pointer.
            JavaVM::from_raw(vm.get_java_vm_pointer()).expect("Failed to get JavaVM")
        }),
        None => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution("JavaVM not initialized".to_string()),
            );
            return;
        }
    };

    // Find the FilterTreeCallbackBridge class for JNI callbacks
    let bridge_class = match env.find_class("org/opensearch/index/engine/exec/FilterTreeCallbackBridge") {
        Ok(c) => match env.new_global_ref(c) {
            Ok(r) => r,
            Err(e) => {
                set_action_listener_error(
                    env, listener,
                    &DataFusionError::Execution(format!("Failed to create global ref for bridge class: {}", e)),
                );
                return;
            }
        },
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Failed to find FilterTreeCallbackBridge class: {}", e)),
            );
            return;
        }
    };

    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Failed to create listener global ref: {}", e)),
            );
            return;
        }
    };

    let io_runtime = tokio_rt_mgr.io_runtime.clone();
    let context_id = bridge_context_id;
    let tree = Arc::new(bool_node);
    let jvm_ref = jvm;
    let bridge_class_ref = bridge_class;

    // Parse segment_max_docs and parquet_paths from JNI arrays
    let seg_max_docs_arr = unsafe { jni::objects::JLongArray::from_raw(_segment_max_docs.as_raw()) };
    let seg_max_docs_len = env.get_array_length(&seg_max_docs_arr).unwrap_or(0) as usize;
    let mut seg_max_docs_buf = vec![0i64; seg_max_docs_len];
    if seg_max_docs_len > 0 {
        let _ = env.get_long_array_region(&seg_max_docs_arr, 0, &mut seg_max_docs_buf);
    }

    let pq_paths = {
        let arr = unsafe { JObjectArray::from_raw(_parquet_paths.as_raw()) };
        match parse_string_arr(env, arr) {
            Ok(p) => p,
            Err(e) => {
                set_action_listener_error(
                    env, listener,
                    &DataFusionError::Execution(format!("Failed to parse parquet paths: {}", e)),
                );
                return;
            }
        }
    };

    let num_parts = _num_partitions.max(1) as usize;

    // Create one JniTreeShardSearcher per unique collector leaf
    let collector_leaves = tree.collector_leaves();
    let mut searchers: Vec<Arc<dyn indexed_table::ShardSearcher>> = Vec::with_capacity(collector_leaves.len());

    for (idx, &(provider_id, _collector_idx)) in collector_leaves.iter().enumerate() {
        // Get segment count and max docs via JNI callbacks
        let bridge_class_for_searcher = match env.new_global_ref(bridge_class_ref.as_obj()) {
            Ok(r) => r,
            Err(e) => {
                set_action_listener_error(
                    env, listener,
                    &DataFusionError::Execution(format!("Failed to create global ref for searcher: {}", e)),
                );
                return;
            }
        };

        let class: &JClass = bridge_class_ref.as_obj().into();

        // Call FilterTreeCallbackBridge.getSegmentCount(contextId, providerId, leafIndex)
        let seg_count = match env.call_static_method(
            class, "getSegmentCount", "(JII)I",
            &[jni::objects::JValue::Long(context_id),
              jni::objects::JValue::Int(provider_id as i32),
              jni::objects::JValue::Int(idx as i32)],
        ) {
            Ok(v) => match v.i() {
                Ok(c) if c > 0 => c as usize,
                _ => 0,
            },
            Err(_) => 0,
        };

        // Get max doc per segment
        let mut searcher_max_docs = Vec::with_capacity(seg_count);
        for seg_ord in 0..seg_count {
            let max_doc = match env.call_static_method(
                class, "getSegmentMaxDoc", "(JIII)I",
                &[jni::objects::JValue::Long(context_id),
                  jni::objects::JValue::Int(provider_id as i32),
                  jni::objects::JValue::Int(idx as i32),
                  jni::objects::JValue::Int(seg_ord as i32)],
            ) {
                Ok(v) => v.i().unwrap_or(0) as i64,
                Err(_) => 0,
            };
            searcher_max_docs.push(max_doc);
        }

        searchers.push(Arc::new(indexed_table::JniTreeShardSearcher::new(
            Arc::clone(&jvm_ref),
            context_id,
            provider_id as i32,
            idx as i32,
            bridge_class_for_searcher,
            seg_count,
            searcher_max_docs,
        )));
    }

    // Build segments from parquet paths (if provided) or from segment_max_docs
    let segments_and_schema = if !pq_paths.is_empty() {
        let pq_strs: Vec<String> = pq_paths;
        indexed_table::build_segments(&pq_strs, &seg_max_docs_buf)
    } else {
        Err("No parquet paths provided for tree query".to_string())
    };

    let (segments, schema) = match segments_and_schema {
        Ok((s, sch)) => (s, sch),
        Err(e) => {
            // If no segments, report error
            io_runtime.block_on(async move {
                with_jni_env(|env| {
                    set_action_listener_error_global(
                        env, &listener_ref,
                        &DataFusionError::Execution(format!("Failed to build segments: {}", e)),
                    );
                });
            });
            return;
        }
    };

    // Build TreeIndexedTableProvider
    let predicates: Vec<indexed_table::bool_tree::ResolvedPredicate> = Vec::new(); // TODO: resolve from substrait
    let provider = match indexed_table::TreeIndexedTableProvider::try_new(
        indexed_table::tree_provider::TreeIndexedTableConfig::new(
            Arc::clone(&tree), searchers, predicates, segments, schema,
        ).with_partitions(num_parts),
    ) {
        Ok(p) => p,
        Err(e) => {
            io_runtime.block_on(async move {
                with_jni_env(|env| {
                    set_action_listener_error_global(
                        env, &listener_ref,
                        &DataFusionError::Execution(format!("TreeIndexedTableProvider: {}", e)),
                    );
                });
            });
            return;
        }
    };

    // Execute via DataFusion: register table, decode substrait, execute plan, return stream
    let result = io_runtime.block_on(async {
        use datafusion::prelude::*;
        use datafusion::physical_plan::execute_stream;
        use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
        use prost::Message;

        // Build session context
        let runtime_ptr_val = _runtime_ptr;
        let runtime = if runtime_ptr_val != 0 {
            unsafe { &*(runtime_ptr_val as *const crate::api::DataFusionRuntime) }
        } else {
            return Err(DataFusionError::Execution("Invalid runtime pointer".to_string()));
        };

        let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::from_runtime_env(
            &runtime.runtime_env,
        ).build()?;

        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = num_parts;
        config.options_mut().execution.batch_size = 8192;

        let state = datafusion::execution::SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::from(runtime_env))
            .with_default_features()
            .build();

        let ctx = SessionContext::new_with_state(state);

        // Register the tree-indexed table
        ctx.register_table(&table_name_str, Arc::new(provider))
            .map_err(|e| DataFusionError::Execution(format!("register_table: {}", e)))?;

        // Decode substrait → logical plan → physical plan → stream
        let substrait_plan = substrait::proto::Plan::decode(plan_bytes.as_slice())
            .map_err(|e| DataFusionError::Execution(format!("Substrait decode: {}", e)))?;

        let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;
        let dataframe = ctx.execute_logical_plan(logical_plan).await?;
        let physical_plan = dataframe.create_physical_plan().await?;

        let df_stream = execute_stream(physical_plan, ctx.task_ctx())?;

        // Wrap in CrossRtStream for safe cross-runtime consumption
        let cpu_executor = tokio_rt_mgr.cpu_executor();
        let cross_rt_stream = crate::cross_rt_stream::CrossRtStream::new_with_df_error_stream(
            df_stream, cpu_executor,
        );
        let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            cross_rt_stream.schema(),
            cross_rt_stream,
        );

        Ok(Box::into_raw(Box::new(wrapped)) as jni::sys::jlong)
    });

    // Deliver result via ActionListener
    with_jni_env(|env| {
        match result {
            Ok(stream_ptr) => set_action_listener_ok_global(env, &listener_ref, stream_ptr),
            Err(e) => {
                error!("Tree query execution failed: {}", e);
                set_action_listener_error_global(env, &listener_ref, &e);
            }
        }
    });
}


// ── Substrait-Driven Tree Query Execution ──────────────────────────────

/// JNI-backed RowGroupDocsCollector that calls IndexFilterBridge for doc collection.
///
/// Similar to JniTreeCollector but uses IndexFilterBridge instead of FilterTreeCallbackBridge.
struct JniBridgeCollector {
    jvm: Arc<JavaVM>,
    context_id: i64,
    collector_key: i32,
    class_ref: jni::objects::GlobalRef,
}

impl std::fmt::Debug for JniBridgeCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JniBridgeCollector")
            .field("context_id", &self.context_id)
            .field("collector_key", &self.collector_key)
            .finish()
    }
}

impl Drop for JniBridgeCollector {
    fn drop(&mut self) {
        if let Ok(mut env) = self.jvm.attach_current_thread() {
            let class: &jni::objects::JClass = self.class_ref.as_obj().into();
            let _ = env.call_static_method(
                class,
                "releaseCollector",
                "(JI)V",
                &[
                    JValue::Long(self.context_id),
                    JValue::Int(self.collector_key),
                ],
            );
        }
    }
}

impl indexed_table::index::RowGroupDocsCollector for JniBridgeCollector {
    fn collect(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        let mut env = self.jvm.attach_current_thread()
            .map_err(|e| format!("Failed to attach thread: {}", e))?;

        let class: &jni::objects::JClass = self.class_ref.as_obj().into();

        // Call IndexFilterBridge.collectDocs(contextId, collectorKey, minDoc, maxDoc)
        let result = env.call_static_method(
            class,
            "collectDocs",
            "(JIII)[J",
            &[
                JValue::Long(self.context_id),
                JValue::Int(self.collector_key),
                JValue::Int(min_doc),
                JValue::Int(max_doc),
            ],
        )
        .map_err(|e| format!("collectDocs failed: {}", e))?;

        let array_obj = result.l()
            .map_err(|e| format!("Failed to get array: {}", e))?;

        let long_array = unsafe { jni::objects::JLongArray::from_raw(array_obj.as_raw()) };
        let len = env.get_array_length(&long_array)
            .map_err(|e| format!("Failed to get array length: {}", e))? as usize;

        if len == 0 {
            return Ok(Vec::new());
        }

        let mut buf = vec![0i64; len];
        env.get_long_array_region(&long_array, 0, &mut buf)
            .map_err(|e| format!("Failed to get array region: {}", e))?;

        Ok(buf.iter().map(|&v| v as u64).collect())
    }
}

/// Substrait-driven tree query execution.
///
/// Takes Substrait bytes (no tree bytes), extracts the filter from the plan,
/// builds the SubstraitBoolNode tree, resolves predicates upfront, creates
/// JNI-backed collectors per segment via IndexFilterBridge callbacks, and
/// executes via the existing TreeIndexedTableProvider pipeline.
#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_executeSubstraitTreeQueryAsync(
    mut env: JNIEnv,
    _class: JClass,
    context_id: jlong,          // IndexFilterBridge contextId
    parquet_paths: JObject,     // String[]
    table_name: JString,
    substrait_bytes: JObject,   // byte[]
    num_partitions: jint,
    runtime_ptr: jlong,
    listener: JObject,
) {
    let tokio_rt_mgr = match get_tokio_rt_manager() {
        Ok(m) => m,
        Err(e) => {
            set_action_listener_error(env, listener, &e);
            return;
        }
    };

    // Parse parquet paths (String[])
    let pq_paths = {
        let arr = unsafe { JObjectArray::from_raw(parquet_paths.as_raw()) };
        match parse_string_arr(env, arr) {
            Ok(p) => p,
            Err(e) => {
                set_action_listener_error(
                    env, listener,
                    &DataFusionError::Execution(format!("Failed to parse parquet paths: {}", e)),
                );
                return;
            }
        }
    };

    // Parse table name
    let table_name_str: String = match env.get_string(&JString::from(table_name)) {
        Ok(s) => s.into(),
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Invalid table name: {}", e)),
            );
            return;
        }
    };

    // Parse substrait plan bytes
    let plan_bytes_arr = unsafe { JByteArray::from_raw(substrait_bytes.as_raw()) };
    let plan_bytes = match env.convert_byte_array(plan_bytes_arr) {
        Ok(b) => b,
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Failed to convert substrait bytes: {}", e)),
            );
            return;
        }
    };

    // Get JVM reference for JNI callbacks
    let jvm = match JAVA_VM.get() {
        Some(vm) => Arc::new(unsafe {
            JavaVM::from_raw(vm.get_java_vm_pointer()).expect("Failed to get JavaVM")
        }),
        None => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution("JavaVM not initialized".to_string()),
            );
            return;
        }
    };

    // Find the IndexFilterBridge class for JNI callbacks
    let bridge_class = match env.find_class("org/opensearch/index/engine/exec/IndexFilterBridge") {
        Ok(c) => match env.new_global_ref(c) {
            Ok(r) => r,
            Err(e) => {
                set_action_listener_error(
                    env, listener,
                    &DataFusionError::Execution(format!("Failed to create global ref for IndexFilterBridge: {}", e)),
                );
                return;
            }
        },
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Failed to find IndexFilterBridge class: {}", e)),
            );
            return;
        }
    };

    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Failed to create listener global ref: {}", e)),
            );
            return;
        }
    };

    let num_parts = num_partitions.max(1) as usize;
    let io_runtime = tokio_rt_mgr.io_runtime.clone();

    // Build segments from parquet paths
    // For the Substrait-driven path, segment_max_docs are derived from parquet metadata.
    // We use the row count from parquet as max_doc for each segment.
    let seg_max_docs: Vec<i64> = {
        let mut max_docs = Vec::with_capacity(pq_paths.len());
        for path in &pq_paths {
            let file = match std::fs::File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    set_action_listener_error(
                        env, listener,
                        &DataFusionError::Execution(format!("Failed to open parquet file {}: {}", path, e)),
                    );
                    return;
                }
            };
            let (_, pq_meta) = match indexed_table::parquet_bridge::load_parquet_metadata(&file) {
                Ok(m) => m,
                Err(e) => {
                    set_action_listener_error(
                        env, listener,
                        &DataFusionError::Execution(format!("Failed to read parquet metadata {}: {}", path, e)),
                    );
                    return;
                }
            };
            let total_rows: i64 = (0..pq_meta.num_row_groups())
                .map(|i| pq_meta.row_group(i).num_rows())
                .sum();
            max_docs.push(total_rows);
        }
        max_docs
    };

    let (segments, _coerced_schema) = match indexed_table::build_segments(&pq_paths, &seg_max_docs) {
        Ok((s, sch)) => (s, sch),
        Err(e) => {
            set_action_listener_error(
                env, listener,
                &DataFusionError::Execution(format!("Failed to build segments: {}", e)),
            );
            return;
        }
    };

    // Decode Substrait → LogicalPlan, extract filter, build tree
    let result = io_runtime.block_on(async {
        use datafusion::prelude::*;
        use datafusion::physical_plan::execute_stream;
        use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
        use prost::Message;

        // Build session context with index_filter UDF registered
        let runtime = if runtime_ptr != 0 {
            unsafe { &*(runtime_ptr as *const crate::api::DataFusionRuntime) }
        } else {
            return Err(DataFusionError::Execution("Invalid runtime pointer".to_string()));
        };

        let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::from_runtime_env(
            &runtime.runtime_env,
        ).build()?;

        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = num_parts;
        config.options_mut().execution.batch_size = 8192;

        let state = datafusion::execution::SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::from(runtime_env))
            .with_default_features()
            .build();

        let ctx = SessionContext::new_with_state(state);

        // Register index_filter UDF so Substrait consumer can decode it
        ctx.register_udf(indexed_table::substrait_to_tree::create_index_filter_udf());

        // Register a temporary ListingTable with the parquet files so Substrait consumer
        // can resolve column references during plan decoding. We use ListingTable (same as
        // sqlToSubstraitWithIndexFilter) to ensure the schema matches exactly.
        // We capture the inferred schema so the TreeIndexedTableProvider uses the same
        // schema as the Substrait plan (avoiding BinaryView vs Utf8 mismatches from
        // coerce_binary_to_string in build_segments).
        let listing_schema;
        {
            use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
            use datafusion::datasource::file_format::parquet::ParquetFormat;
            let listing_url = datafusion::datasource::listing::ListingTableUrl::parse(
                pq_paths[0].rsplit_once('/').map(|(dir, _)| dir).unwrap_or(".")
            ).map_err(|e| DataFusionError::Execution(format!("ListingTableUrl: {}", e)))?;
            let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
                .with_file_extension(".parquet")
                .with_collect_stat(true);
            let inferred_schema = listing_options.infer_schema(&ctx.state(), &listing_url).await
                .map_err(|e| DataFusionError::Execution(format!("infer_schema: {}", e)))?;
            listing_schema = inferred_schema.clone();
            let config = ListingTableConfig::new(listing_url)
                .with_listing_options(listing_options)
                .with_schema(inferred_schema);
            let listing_table = ListingTable::try_new(config)
                .map_err(|e| DataFusionError::Execution(format!("ListingTable: {}", e)))?;
            ctx.register_table(&table_name_str, Arc::new(listing_table))
                .map_err(|e| DataFusionError::Execution(format!("register temp table: {}", e)))?;
        }

        // Decode Substrait → LogicalPlan
        let substrait_plan = substrait::proto::Plan::decode(plan_bytes.as_slice())
            .map_err(|e| DataFusionError::Execution(format!("Substrait decode: {}", e)))?;

        let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;

        // Extract filter expression from the LogicalPlan
        let filter_expr = indexed_table::substrait_to_tree::extract_filter_expr(&logical_plan)
            .map_err(|e| DataFusionError::Execution(format!("extract_filter_expr: {}", e)))?;

        // Build SubstraitBoolNode tree from the filter expression
        let (substrait_bool_node, predicates) =
            indexed_table::substrait_to_tree::expr_to_bool_tree(&filter_expr)
                .map_err(|e| DataFusionError::Execution(format!("expr_to_bool_tree: {}", e)))?;

        // Normalize: push NOT down
        let normalized = substrait_bool_node.push_not_down();

        // Partially resolve: predicates resolved upfront, collectors carry column+value
        let partially_resolved = normalized.partially_resolve(&predicates)
            .map_err(|e| DataFusionError::Execution(format!("partially_resolve: {}", e)))?;

        // Collect all (column, value) pairs from Collector leaves in DFS order
        let collector_leaves = partially_resolved.collector_leaves();

        // For each Collector leaf, call IndexFilterBridge.createProvider(contextId, column, value)
        // Then for each segment, call createCollector and build a JniBridgeCollector
        let mut provider_keys: Vec<i32> = Vec::with_capacity(collector_leaves.len());
        for (column, value) in &collector_leaves {
            let provider_key = with_jni_env(|env| {
                let bridge_cls: &jni::objects::JClass = bridge_class.as_obj().into();
                let j_column = env.new_string(column)
                    .map_err(|e| format!("Failed to create column string: {}", e))?;
                let j_value = env.new_string(value)
                    .map_err(|e| format!("Failed to create value string: {}", e))?;

                let result = env.call_static_method(
                    bridge_cls,
                    "createProvider",
                    "(JLjava/lang/String;Ljava/lang/String;)I",
                    &[
                        JValue::Long(context_id),
                        JValue::Object(&j_column),
                        JValue::Object(&j_value),
                    ],
                )
                .map_err(|e| format!("createProvider failed: {}", e))?;

                result.i().map_err(|e| format!("Failed to get int: {}", e))
            });

            match provider_key {
                Ok(key) => provider_keys.push(key),
                Err(e) => {
                    return Err(DataFusionError::Execution(format!("createProvider failed: {}", e)));
                }
            }
        }

        // Build collectors for each segment. The TreeIndexedTableProvider expects
        // one ShardSearcher per collector leaf. We create a simple searcher that
        // wraps the IndexFilterBridge callbacks.
        //
        // For the Substrait-driven path, we build collectors per-segment inline
        // and resolve the PartiallyResolvedNode to a ResolvedNode per segment chunk.
        // However, the existing TreeIndexedTableProvider expects a BoolNode + ShardSearchers.
        //
        // We convert the PartiallyResolvedNode to a BoolNode (old-style with collector indices)
        // and create ShardSearchers that wrap the IndexFilterBridge callbacks.

        // Build a BoolNode (old-style) from the PartiallyResolvedNode for the TreeIndexedTableProvider
        let old_tree = build_old_bool_node_from_partially_resolved(&partially_resolved);
        let old_tree = Arc::new(old_tree);

        // Create one IndexFilterBridgeSearcher per collector leaf
        let mut searchers: Vec<Arc<dyn indexed_table::ShardSearcher>> = Vec::with_capacity(collector_leaves.len());
        for (_leaf_idx, provider_key) in provider_keys.iter().enumerate() {
            let bridge_class_for_searcher = with_jni_env(|env| {
                env.new_global_ref(bridge_class.as_obj())
                    .map_err(|e| format!("Failed to create global ref: {}", e))
            }).map_err(|e| DataFusionError::Execution(e))?;

            searchers.push(Arc::new(IndexFilterBridgeSearcher {
                jvm: Arc::clone(&jvm),
                context_id,
                provider_key: *provider_key,
                class_ref: bridge_class_for_searcher,
                segment_count: segments.len(),
                segment_max_docs: segments.iter().map(|s| s.max_doc).collect(),
            }));
        }

        // Build TreeIndexedTableProvider using the ListingTable's inferred schema
        // so it matches the Substrait plan's schema exactly (avoids BinaryView vs Utf8 mismatch).
        let resolved_predicates = predicates;
        let provider = indexed_table::TreeIndexedTableProvider::try_new(
            indexed_table::tree_provider::TreeIndexedTableConfig::new(
                old_tree, searchers, resolved_predicates, segments, listing_schema,
            ).with_partitions(num_parts),
        )?;

        // Replace the temporary table with the real TreeIndexedTableProvider
        ctx.deregister_table(&table_name_str)
            .map_err(|e| DataFusionError::Execution(format!("deregister temp table: {}", e)))?;
        ctx.register_table(&table_name_str, Arc::new(provider))
            .map_err(|e| DataFusionError::Execution(format!("register_table: {}", e)))?;

        // Execute substrait plan → physical plan → stream
        // Re-decode the substrait plan now that the real table is registered
        let logical_plan2 = from_substrait_plan(&ctx.state(), &substrait_plan).await?;
        let dataframe = ctx.execute_logical_plan(logical_plan2).await?;
        let physical_plan = dataframe.create_physical_plan().await?;

        let df_stream = execute_stream(physical_plan, ctx.task_ctx())?;

        // Wrap in CrossRtStream for safe cross-runtime consumption
        let cpu_executor = tokio_rt_mgr.cpu_executor();
        let cross_rt_stream = crate::cross_rt_stream::CrossRtStream::new_with_df_error_stream(
            df_stream, cpu_executor,
        );
        let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            cross_rt_stream.schema(),
            cross_rt_stream,
        );

        Ok(Box::into_raw(Box::new(wrapped)) as jni::sys::jlong)
    });

    // Deliver result via ActionListener
    with_jni_env(|env| {
        match result {
            Ok(stream_ptr) => set_action_listener_ok_global(env, &listener_ref, stream_ptr),
            Err(e) => {
                error!("Substrait tree query execution failed: {}", e);
                set_action_listener_error_global(env, &listener_ref, &e);
            }
        }
    });
}

/// Converts a PartiallyResolvedNode back to a BoolNode (old-style) for the
/// TreeIndexedTableProvider. Collector leaves get sequential indices.
/// Predicate leaves get sequential predicate_id values matching the predicates vec order.
fn build_old_bool_node_from_partially_resolved(
    node: &indexed_table::bool_tree::PartiallyResolvedNode,
) -> indexed_table::bool_tree::BoolNode {
    let mut collector_idx = 0;
    let mut predicate_idx: u16 = 0;
    build_old_bool_node_inner(node, &mut collector_idx, &mut predicate_idx)
}

fn build_old_bool_node_inner(
    node: &indexed_table::bool_tree::PartiallyResolvedNode,
    collector_idx: &mut usize,
    predicate_idx: &mut u16,
) -> indexed_table::bool_tree::BoolNode {
    match node {
        indexed_table::bool_tree::PartiallyResolvedNode::And(children) => {
            indexed_table::bool_tree::BoolNode::And(
                children.iter().map(|c| build_old_bool_node_inner(c, collector_idx, predicate_idx)).collect()
            )
        }
        indexed_table::bool_tree::PartiallyResolvedNode::Or(children) => {
            indexed_table::bool_tree::BoolNode::Or(
                children.iter().map(|c| build_old_bool_node_inner(c, collector_idx, predicate_idx)).collect()
            )
        }
        indexed_table::bool_tree::PartiallyResolvedNode::Not(child) => {
            indexed_table::bool_tree::BoolNode::Not(Box::new(build_old_bool_node_inner(child, collector_idx, predicate_idx)))
        }
        indexed_table::bool_tree::PartiallyResolvedNode::Collector { .. } => {
            let idx = *collector_idx;
            *collector_idx += 1;
            indexed_table::bool_tree::BoolNode::Collector {
                provider_id: 0,
                collector_idx: idx,
            }
        }
        indexed_table::bool_tree::PartiallyResolvedNode::Predicate { column: _, op: _, value: _ } => {
            let id = *predicate_idx;
            *predicate_idx += 1;
            indexed_table::bool_tree::BoolNode::Predicate { predicate_id: id }
        }
    }
}

/// A ShardSearcher that wraps IndexFilterBridge JNI callbacks for a single
/// collector leaf (identified by provider_key).
struct IndexFilterBridgeSearcher {
    jvm: Arc<JavaVM>,
    context_id: i64,
    provider_key: i32,
    class_ref: jni::objects::GlobalRef,
    segment_count: usize,
    segment_max_docs: Vec<i64>,
}

impl std::fmt::Debug for IndexFilterBridgeSearcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexFilterBridgeSearcher")
            .field("context_id", &self.context_id)
            .field("provider_key", &self.provider_key)
            .field("segment_count", &self.segment_count)
            .finish()
    }
}

impl Drop for IndexFilterBridgeSearcher {
    fn drop(&mut self) {
        // Release the provider when the searcher is dropped
        if let Ok(mut env) = self.jvm.attach_current_thread() {
            let class: &jni::objects::JClass = self.class_ref.as_obj().into();
            let _ = env.call_static_method(
                class,
                "releaseProvider",
                "(JI)V",
                &[
                    JValue::Long(self.context_id),
                    JValue::Int(self.provider_key),
                ],
            );
        }
    }
}

impl indexed_table::index::ShardSearcher for IndexFilterBridgeSearcher {
    fn segment_count(&self) -> usize {
        self.segment_count
    }

    fn segment_max_doc(&self, segment_ord: usize) -> Result<i64, String> {
        self.segment_max_docs.get(segment_ord).copied()
            .ok_or_else(|| format!("segment_ord {} out of range ({})", segment_ord, self.segment_count))
    }

    fn collector(
        &self,
        segment_ord: usize,
        doc_min: i32,
        doc_max: i32,
    ) -> Result<Arc<dyn indexed_table::index::RowGroupDocsCollector>, String> {
        let mut env = self.jvm.attach_current_thread()
            .map_err(|e| format!("Failed to attach thread: {}", e))?;

        let class: &jni::objects::JClass = self.class_ref.as_obj().into();

        // Call IndexFilterBridge.createCollector(contextId, providerKey, segmentOrd, minDoc, maxDoc)
        let collector_key = env.call_static_method(
            class,
            "createCollector",
            "(JIIII)I",
            &[
                JValue::Long(self.context_id),
                JValue::Int(self.provider_key),
                JValue::Int(segment_ord as i32),
                JValue::Int(doc_min),
                JValue::Int(doc_max),
            ],
        )
        .map_err(|e| format!("createCollector failed: {}", e))?
        .i()
        .map_err(|e| format!("Failed to get int: {}", e))?;

        if collector_key < 0 {
            // Return empty collector for failed creation
            return Ok(Arc::new(EmptyBridgeCollector));
        }

        let collector_class_ref = env.new_global_ref(class)
            .map_err(|e| format!("Failed to create global ref: {}", e))?;

        Ok(Arc::new(JniBridgeCollector {
            jvm: Arc::clone(&self.jvm),
            context_id: self.context_id,
            collector_key,
            class_ref: collector_class_ref,
        }))
    }
}

/// Empty collector that returns no matches (used when createCollector fails).
#[derive(Debug)]
struct EmptyBridgeCollector;

impl indexed_table::index::RowGroupDocsCollector for EmptyBridgeCollector {
    fn collect(&self, _min_doc: i32, _max_doc: i32) -> Result<Vec<u64>, String> {
        Ok(Vec::new())
    }
}

// ── Substrait-Driven Test Helper ───────────────────────────────────────

/// Converts SQL to Substrait bytes with the index_filter UDF registered.
/// Same as sqlToSubstrait but registers the index_filter UDF before parsing SQL.
/// Used for test support.
#[jni_safe(default = std::ptr::null_mut())]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_sqlToSubstraitWithIndexFilter(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    sql: JString,
    runtime_ptr: jlong,
) -> jni::sys::jbyteArray {
    let manager = TOKIO_RUNTIME_MANAGER.get().expect("Runtime manager not initialized");
    let table_name: String = env.get_string(&table_name).expect("Invalid table name").into();
    let sql: String = env.get_string(&sql).expect("Invalid SQL").into();

    let result = sql_to_substrait_with_index_filter(
        shard_view_ptr as i64, &table_name, &sql, runtime_ptr as i64, manager,
    );

    match result {
        Ok(bytes) => env.byte_array_from_slice(&bytes).expect("byte array").into_raw(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Internal: converts SQL to Substrait bytes with index_filter UDF registered.
///
/// # Safety
/// `shard_view_ptr` and `runtime_ptr` must be valid, non-zero pointers.
fn sql_to_substrait_with_index_filter(
    shard_view_ptr: i64,
    table_name: &str,
    sql: &str,
    runtime_ptr: i64,
    manager: &RuntimeManager,
) -> Result<Vec<u8>, DataFusionError> {
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
    use datafusion::execution::cache::cache_manager::CacheManagerConfig;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    let shard_view = unsafe { &*(shard_view_ptr as *const crate::api::ShardView) };
    let runtime = unsafe { &*(runtime_ptr as *const crate::api::DataFusionRuntime) };
    let table_path = shard_view.table_path.clone();
    let object_metas = shard_view.object_metas.clone();
    let table_name = table_name.to_string();

    manager.io_runtime.block_on(async {
        let list_file_cache = Arc::new(DefaultListFilesCache::default());
        list_file_cache.put(
            &datafusion::execution::cache::TableScopedPath {
                table: None,
                path: table_path.prefix().clone(),
            },
            object_metas,
        );
        let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::from_runtime_env(
            &runtime.runtime_env,
        )
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_files_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        )
        .build()?;

        let state = datafusion::execution::SessionStateBuilder::new()
            .with_config(datafusion::prelude::SessionConfig::new())
            .with_runtime_env(Arc::from(runtime_env))
            .with_default_features()
            .build();
        let ctx = datafusion::prelude::SessionContext::new_with_state(state);

        // Register index_filter UDF before parsing SQL
        ctx.register_udf(indexed_table::substrait_to_tree::create_index_filter_udf());

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let schema = listing_options.infer_schema(&ctx.state(), &table_path).await?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);
        ctx.register_table(&table_name, Arc::new(ListingTable::try_new(config)?))?;

        let plan = ctx.sql(sql).await?.logical_plan().clone();
        let substrait = to_substrait_plan(&plan, &ctx.state())?;
        let mut buf = Vec::new();
        substrait.encode(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("Substrait encode failed: {}", e)))?;
        Ok(buf)
    })
}
