/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::ptr::addr_of_mut;
use datafusion_expr::expr_rewriter::unalias;
use jni::objects::{JByteArray, JClass, JObject};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use std::sync::{Arc, Mutex};
use arrow_array::{Array, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::DataType;
use arrow_schema::ffi::FFI_ArrowSchema;

mod util;
mod row_id_optimizer;
mod listing_table;
mod metadata_cache;

use datafusion::execution::context::SessionContext;

use datafusion::prelude::SessionConfig;
use datafusion::DATAFUSION_VERSION;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use futures::TryStreamExt;
use jni::objects::{JObjectArray, JString};
use object_store::ObjectMeta;
use prost::Message;
use tokio::runtime::Runtime;
use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::row_id_optimizer::FilterRowIdOptimizer;
use crate::metadata_cache::{MutexFileMetadataCache};
use crate::util::{construct_file_metadata, create_object_meta_from_file, create_object_meta_from_filenames, parse_string_arr, set_object_result_error, set_object_result_ok};

use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{ListingTableUrl};
use datafusion::execution::cache::cache_manager::{self, CacheManagerConfig, FileMetadataCache};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileMeta;
use datafusion::execution::cache::cache_unit::{DefaultFilesMetadataCache, DefaultListFilesCache};
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

/// Create a new DataFusion session context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createContext(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config(config);
    let ctx = Box::into_raw(Box::new(context)) as jlong;
    ctx
}

/// Close and cleanup a DataFusion context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
}

/// Get version information
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getVersionInfo(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version_info = format!(r#"{{"version": "{}", "codecs": ["CsvDataSourceCodec"]}}"#, DATAFUSION_VERSION);
    env.new_string(version_info).expect("Couldn't create Java string").as_raw()
}

/// Get version information (legacy method name)
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    env.new_string(DATAFUSION_VERSION).expect("Couldn't create Java string").as_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createTokioRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let rt = Runtime::new().unwrap();
    let ctx = Box::into_raw(Box::new(rt)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let runtime_env = RuntimeEnvBuilder::default().build().unwrap();
    /**
    // We can copy global runtime to local runtime - file statistics cache, and most of the things
    // will be shared across session contexts. But list files cache will be specific to session
    // context

    let fsCache = runtimeEnv.clone().cache_manager.get_file_statistic_cache().unwrap();
    let localCacheManagerConfig = CacheManagerConfig::default().with_files_statistics_cache(Option::from(fsCache));
    let localCacheManager = CacheManager::try_new(&localCacheManagerConfig);
    let localRuntimeEnv = RuntimeEnvBuilder::new()
        .with_cache_manager(localCacheManagerConfig)
        .with_disk_manager(DiskManagerConfig::new_existing(runtimeEnv.disk_manager))
        .with_memory_pool(runtimeEnv.memory_pool)
        .with_object_store_registry(runtimeEnv.object_store_registry)
        .build();
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config(config);
    **/

    let ctx = Box::into_raw(Box::new(runtime_env)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createGlobalRuntimev1(
    _env: JNIEnv,
    _class: JClass,
    cache_config_ptr: jlong,
) -> jlong {
        // Take ownership of the CacheManagerConfig
    let cache_manager_config = unsafe { Box::from_raw(cache_config_ptr as *mut CacheManagerConfig) };

    // Create RuntimeEnv with the configured cache manager
    let runtime_env = RuntimeEnvBuilder::default()
        .with_cache_manager(*cache_manager_config)
        .build()
        .unwrap();

    let ptr =Box::into_raw(Box::new(runtime_env)) as jlong;
    ptr
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
) -> jlong {
    let runtimeEnv = unsafe { &mut *(runtime_id as *mut RuntimeEnv) };
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config_rt(config, Arc::new(runtimeEnv.clone()));
    let ctx = Box::into_raw(Box::new(context)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeSessionContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
}


#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    files: JObjectArray
) -> jlong {

    let table_path: String = env.get_string(&table_path).expect("Couldn't get java string!").into();
    let files: Vec<String> = parse_string_arr(&mut env, files).expect("Expected list of files");
    let files_meta = create_object_meta_from_filenames(&table_path, files);

    let table_path = ListingTableUrl::parse(table_path).unwrap();
    let shard_view = ShardView::new(table_path, files_meta);
    Box::into_raw(Box::new(shard_view)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_destroyReader(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong
)  {
    let _ = unsafe { Box::from_raw(ptr as *mut ShardView) };
}

pub struct ShardView {
    table_path: ListingTableUrl,
    files_meta: Arc<Vec<ObjectMeta>>
}

impl ShardView {
    pub fn new(table_path: ListingTableUrl, files_meta: Vec<ObjectMeta>) -> Self {
        let files_meta = Arc::new(files_meta);
        ShardView {
            table_path,
            files_meta
        }
    }

    pub fn table_path(&self) -> ListingTableUrl {
        self.table_path.clone()
    }

    pub fn files_meta(&self) -> Arc<Vec<ObjectMeta>> {
        self.files_meta.clone()
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_executeSubstraitQuery(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    substrait_bytes: jbyteArray,
    tokio_runtime_env_ptr: jlong,
    // callback: JObject,
) -> jlong {
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime_ptr = unsafe { &*(tokio_runtime_env_ptr as *const Runtime)};

    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_meta();

    println!("Table path: {}", table_path);
    println!("Files: {:?}", files_meta);

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), files_meta);

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default()
                                .with_list_files_cache(Some(list_file_cache.clone()))
        ).build().unwrap();

    // TODO: get config from CSV DataFormat
    let mut config = SessionConfig::new();
    // config.options_mut().execution.parquet.pushdown_filters = true;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        // .with_optimizer_rule(Arc::new(OptimizeRowId))
        // .with_physical_optimizer_rule(Arc::new(FilterRowIdOptimizer)) // TODO: enable only for query phase
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet"); // TODO: take this as parameter
        // .with_table_partition_cols(vec![("row_base".to_string(), DataType::Int32)]); // TODO: enable only for query phase

    // Ideally the executor will give this
    runtime_ptr.block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();


        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let shard_id = table_path.prefix().filename().expect("error in fetching Path");
        ctx.register_table("index-7", provider)
            .expect("Failed to attach the Table");

    });

    // TODO : how to close ctx ?
    // Convert Java byte array to Rust Vec<u8>
    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            let error_msg = format!("Failed to convert plan bytes: {}", e);
            env.throw_new("java/lang/Exception", error_msg);
            return 0;
        }
    };

    let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
        Ok(plan) => {
            println!("SUBSTRAIT rust: Decoding is successful, Plan has {} relations", plan.relations.len());
            plan
        },
        Err(e) => {
            return 0;
        }
    };

    //let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    runtime_ptr.block_on(async {

        let logical_plan = match from_substrait_plan(&ctx.state(), &substrait_plan).await {
            Ok(plan) => {
                println!("SUBSTRAIT Rust: LogicalPlan: {:?}", plan);
                plan
            },
            Err(e) => {
                println!("SUBSTRAIT Rust: Failed to convert Substrait plan: {}", e);
                return 0;
            }
        };

        let dataframe = ctx.execute_logical_plan(logical_plan).await.unwrap();
        let stream = dataframe.execute_stream().await.unwrap();
        let stream_ptr = Box::into_raw(Box::new(stream)) as jlong;

        stream_ptr

    })
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_executeSubstraitQueryv1(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    substrait_bytes: jbyteArray,
    tokio_runtime_env_ptr: jlong,
    global_runtime_env_ptr: jlong,
    // callback: JObject,
) -> jlong {
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime_ptr = unsafe { &*(tokio_runtime_env_ptr as *const Runtime)};
    let runtime_env = unsafe { &*(global_runtime_env_ptr as *const RuntimeEnv) };

    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_meta();

    println!("Table path: {}", table_path);
    println!("Files: {:?}", files_meta);

    // TODO: get config from CSV DataFormat
    let mut config = SessionConfig::new();
    // config.options_mut().execution.parquet.pushdown_filters = true;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::new(runtime_env.clone()))
        .with_default_features()
        // .with_optimizer_rule(Arc::new(OptimizeRowId))
        // .with_physical_optimizer_rule(Arc::new(FilterRowIdOptimizer)) // TODO: enable only for query phase
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet"); // TODO: take this as parameter
        // .with_table_partition_cols(vec![("row_base".to_string(), DataType::Int32)]); // TODO: enable only for query phase

    // Ideally the executor will give this
    runtime_ptr.block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();


        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let shard_id = table_path.prefix().filename().expect("error in fetching Path");
        ctx.register_table("index-7", provider)
            .expect("Failed to attach the Table");

    });

    // TODO : how to close ctx ?
    // Convert Java byte array to Rust Vec<u8>
    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            let error_msg = format!("Failed to convert plan bytes: {}", e);
            env.throw_new("java/lang/Exception", error_msg);
            return 0;
        }
    };

    let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
        Ok(plan) => {
            println!("SUBSTRAIT rust: Decoding is successful, Plan has {} relations", plan.relations.len());
            plan
        },
        Err(e) => {
            return 0;
        }
    };

    //let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    runtime_ptr.block_on(async {

        let logical_plan = match from_substrait_plan(&ctx.state(), &substrait_plan).await {
            Ok(plan) => {
                println!("SUBSTRAIT Rust: LogicalPlan: {:?}", plan);
                plan
            },
            Err(e) => {
                println!("SUBSTRAIT Rust: Failed to convert Substrait plan: {}", e);
                return 0;
            }
        };

        let dataframe = ctx.execute_logical_plan(logical_plan).await.unwrap();
        let stream = dataframe.execute_stream().await.unwrap();
        let stream_ptr = Box::into_raw(Box::new(stream)) as jlong;

        stream_ptr

    })
}



// If we need to create session context separately
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_nativeCreateSessionContext(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    shard_view_ptr: jlong,
    global_runtime_env_ptr: jlong,
) -> jlong {
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_meta();

    // Will use it once the global RunTime is defined
    // let runtime_arc = unsafe {
    //     let boxed = &*(runtime_env_ptr as *const Pin<Arc<RuntimeEnv>>);
    //     (**boxed).clone()
    // };

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), files_meta);

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default()
            .with_list_files_cache(Some(list_file_cache))).build().unwrap();



    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime_env));


    // Create default parquet options
    let file_format = CsvFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".csv");


    // let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    let mut session_context_ptr = 0;

    // Ideally the executor will give this
    Runtime::new().expect("Failed to create Tokio Runtime").block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();


        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let shard_id = table_path.prefix().filename().expect("error in fetching Path");
        ctx.register_table(shard_id, provider)
            .expect("Failed to attach the Table");

        // Return back after wrapping in Box
        session_context_ptr = Box::into_raw(Box::new(ctx)) as jlong
    });

    session_context_ptr
}



#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_next(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    stream: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };

    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    runtime.block_on(async {
        //let fetch_start = std::time::Instant::now();
        let next = stream.try_next().await;
        //let fetch_time = fetch_start.elapsed();
        match next {
            Ok(Some(batch)) => {
                //let convert_start = std::time::Instant::now();
                // Convert to struct array for compatibility with FFI
                //println!("Num rows : {}", batch.num_rows());
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                //let convert_time = convert_start.elapsed();
                // ffi_array must remain alive until after the callback is called
                // let callback_start = std::time::Instant::now();
                set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_array));
                // let callback_time = callback_start.elapsed();
                // println!("Fetch: {:?}, Convert: {:?}, Callback: {:?}",
                //          fetch_time, convert_time, callback_time);
            }
            Ok(None) => {
                set_object_result_ok(&mut env, callback, 0 as *mut FFI_ArrowSchema);
            }
            Err(err) => {
                set_object_result_error(&mut env, callback, &err);
            }
        }
        //println!("Total time: {:?}", start.elapsed());
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_getSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream: jlong,
    callback: JObject,
) {
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    let schema = stream.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(&*schema);
    match ffi_schema {
        Ok(mut ffi_schema) => {
            // ffi_schema must remain alive until after the callback is called
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
        }
        Err(err) => {
            set_object_result_error(&mut env, callback, &err);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_initCacheManagerConfig(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let config = CacheManagerConfig::default();
    Box::into_raw(Box::new(config)) as jlong
}


/// Create a metadata cache and add it to the CacheManagerConfig
/// The config_ptr remains the same, only the contents are updated
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createMetadataCache(
    _env: JNIEnv,
    _class: JClass,
    config_ptr: jlong,
    size_limit: jlong,
) -> jlong {
    // Create cache with wrapper that implements FileMetadataCache
    let inner_cache = DefaultFilesMetadataCache::new(size_limit.try_into().unwrap());
    let wrapped_cache = Arc::new(MutexFileMetadataCache::new(inner_cache));

    // Update the CacheManagerConfig at the same memory location
    if config_ptr != 0 {
        let cache_manager_config = unsafe { &mut *(config_ptr as *mut CacheManagerConfig) };
        *cache_manager_config = cache_manager_config.clone()
            .with_file_metadata_cache(Some(wrapped_cache.clone()));
    }

    // Return the Arc<MutexFileMetadataCache> pointer for JNI operations
    Box::into_raw(Box::new(wrapped_cache)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCachePut(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString,
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false
    };
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    let data_format = if file_path.to_lowercase().ends_with(".parquet") {
        "parquet"
    } else {
        return false; // Skip unsupported formats
    };

    let object_meta = create_object_meta_from_file(&file_path);
    let store = Arc::new(object_store::local::LocalFileSystem::new());

    // Use Runtime to block on the async operation
    let metadata = Runtime::new()
        .expect("Failed to create Tokio Runtime")
        .block_on(async {
            construct_file_metadata(store.as_ref(), &object_meta, data_format)
                .await
                .expect("Failed to construct file metadata")
        });

    let metadata = cache.put(&object_meta, metadata);

    if metadata.is_none() {
        println!("Failed to cache metadata for: {}", file_path);
        return false;
    }

    println!("Cached metadata for: {}", file_path);
    true
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheRemove(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString,
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false,
    };
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    let object_meta = create_object_meta_from_file(&file_path);

    // Lock the mutex and remove
    if let Some(_cache_obj) = cache.inner.lock().unwrap().remove(&object_meta) {
        println!("Cache removed for: {}", file_path);
        true
    } else {
        println!("Item not found in cache: {}", file_path);
        false
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheGet(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString,
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false,
    };

    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    let object_meta = create_object_meta_from_file(&file_path);

    match cache.get(&object_meta) {
        Some(metadata) => {
            println!("Retrieved metadata for: {} - size: {:?}", file_path, metadata.memory_size());
            true
        },
        None => {
            println!("No metadata found for: {}", file_path);
            false
        },
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheGetEntries(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> jni::sys::jobjectArray {
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };

    // Get all entries from the cache
    let entries = cache.list_entries();

    println!("Retrieved {} cache entries", entries.len());

    // Create String array class
    let string_class = match env.find_class("java/lang/String") {
        Ok(cls) => cls,
        Err(e) => {
            println!("Failed to find String class: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // Create array with size = number of entries * 3 (path, size, hit_count for each entry)
    let array_size = (entries.len() * 3) as i32;
    let result_array = match env.new_object_array(array_size, &string_class, JObject::null()) {
        Ok(arr) => arr,
        Err(e) => {
            println!("Failed to create object array: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // Fill the array with entries
    let mut index = 0;
    for (path, entry) in entries.iter() {
        // Add path
        if let Ok(path_str) = env.new_string(path.as_ref()) {
            let _ = env.set_object_array_element(&result_array, index, path_str);
        }
        index += 1;

        // Add size
        if let Ok(size_str) = env.new_string(entry.size_bytes.to_string()) {
            let _ = env.set_object_array_element(&result_array, index, size_str);
        }
        index += 1;

        // Add hit_count
        if let Ok(hit_count_str) = env.new_string(entry.hits.to_string()) {
            let _ = env.set_object_array_element(&result_array, index, hit_count_str);
        }
        index += 1;
    }

    result_array.as_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheGetSize(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong
) -> usize {
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    cache.inner.lock().unwrap().memory_used()
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheContainsFile(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false
    };
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    let object_meta = create_object_meta_from_file(&file_path);
    cache.inner.lock().unwrap().contains_key(&object_meta)
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheUpdateSizeLimit(
     env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    new_size_limit: usize
) -> bool {
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    cache.inner.lock().unwrap().update_cache_limit(new_size_limit);
    if cache.inner.lock().unwrap().cache_limit() == new_size_limit {
        println!("Cache size limit updated to: {}", new_size_limit);
        true
    } else {
        println!("Failed to update cache size limit to: {}", new_size_limit);
        false
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheClear(
     env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong
) {
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    cache.inner.lock().unwrap().clear();
}
