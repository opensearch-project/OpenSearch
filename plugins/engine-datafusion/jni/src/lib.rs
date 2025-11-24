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
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use std::sync::Arc;
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
use std::time::Instant;

mod util;
mod row_id_optimizer;
mod listing_table;
mod cache;
mod custom_cache_manager;
mod memory;
mod partial_agg_optimizer;

use crate::custom_cache_manager::CustomCacheManager;
use crate::row_id_optimizer::ProjectRowIdOptimizer;
use crate::util::{
    create_file_meta_from_filenames, parse_string_arr, set_object_result_error,
    set_object_result_ok,
};
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
use futures::TryStreamExt;
use datafusion_substrait::substrait::proto::extensions::simple_extension_declaration::MappingType;

use object_store::ObjectMeta;
use prost::Message;
use tokio::runtime::Runtime;
use std::result;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;

// NativeBridge JNI implementations
use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use jni::objects::{JObjectArray, JString};
use crate::memory::{CustomMemoryPool, Monitor, MonitoredMemoryPool};

struct DataFusionRuntime {
    runtime_env: RuntimeEnv,
    custom_cache_manager: Option<CustomCacheManager>,
    tokio_runtime: Runtime,
    monitor: Arc<Monitor>,
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

        let tokio_runtime = Runtime::new().expect("Failed to create Tokio runtime");

        let runtime = DataFusionRuntime {
            runtime_env,
            custom_cache_manager: Some(custom_cache_manager),
            tokio_runtime,
            monitor,
        };

        Box::into_raw(Box::new(runtime)) as jlong
    } else {
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build().unwrap();

        let tokio_runtime = Runtime::new().expect("Failed to create Tokio runtime");

        let runtime = DataFusionRuntime {
            runtime_env,
            custom_cache_manager: None,
            tokio_runtime,
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
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createCustomCacheManager(
    mut env: JNIEnv,
    _class: JClass,
) -> jlong {
    // Create CustomCacheManager without any config
    let manager = custom_cache_manager::CustomCacheManager::new();
    Box::into_raw(Box::new(manager)) as jlong
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
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeQueryPhase(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: jbyteArray,
    runtime_ptr: jlong
) -> jlong {
    let overall = Instant::now();
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };
    let table_name: String = env
        .get_string(&table_name)
        .expect("Couldn't get java string!")
        .into();

    let runtimeEnv = &runtime.runtime_env;

    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_metadata();
    let object_meta: Arc<Vec<ObjectMeta>> = Arc::new(
        files_meta
            .iter()
            .map(|metadata| (*metadata.object_meta).clone())
            .collect(),
    );

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), object_meta);

    let runtime_env = RuntimeEnvBuilder::from_runtime_env(runtimeEnv)
        .with_cache_manager(
            CacheManagerConfig::default().with_list_files_cache(Some(list_file_cache.clone()))
                .with_file_metadata_cache(Some(runtimeEnv.cache_manager.get_file_metadata_cache())),
        )
        .build()
        .unwrap();


    // TODO: get config from CSV DataFormat
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = false;
    config.options_mut().execution.target_partitions = 1;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        // .with_physical_optimizer_rule(Arc::new(ProjectRowIdOptimizer))
        .with_physical_optimizer_rule(Arc::new(PartialAggregationOptimizer))
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet") // TODO: take this as parameter
        .with_files_metadata(files_meta)
        .with_table_partition_cols(vec![("row_base".to_string(), DataType::Int64)]);

    // Ideally the executor will give this
    runtime.tokio_runtime.block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();

        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        ctx.register_table(table_name.clone(), provider)
            .expect("Failed to attach the Table");
    });

    let start = Instant::now();
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
            // println!("SUBSTRAIT rust: Decoding is successful, Plan has {} relations", plan.relations.len());
            plan
        }
        Err(e) => {
            return 0;
        }
    };

    runtime.tokio_runtime.block_on(async {
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
            Ok(plan) => {
                // println!("SUBSTRAIT Rust: LogicalPlan: {:?}", plan);
                let duration = start.elapsed();
                println!(
                    "Rust: Substrait decoding time in milliseconds: {}",
                    duration.as_millis()
                );
                plan
            }
            Err(e) => {
                let error_msg = format!("Failed to convert Substrait plan: {}", e);
                println!("SUBSTRAIT Rust: {}", error_msg);
                let _ = env.throw_new("java/lang/RuntimeException", error_msg);
                return 0;
            }
        };

        let dataframe = ctx
            .execute_logical_plan(logical_plan)
            .await
            .expect("Failed to execute logical plan");
        let physical_plan = dataframe.clone().create_physical_plan().await.unwrap();
        println!(
            "Physical Plan:\n{}",
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
        );

        let stream = match dataframe.execute_stream().await {
            Ok(stream) => stream,
            Err(e) => {
                let error_msg = format!("Failed to execute stream: {}", e);
                println!("{}", error_msg);
                env.throw_new("java/lang/Exception", error_msg);
                return 0;
            }
        };
        let stream_ptr = Box::into_raw(Box::new(stream)) as jlong;

        let duration1 = overall.elapsed();
        println!(
            "Rust: Overall query setup time in milliseconds: {}",
            duration1.as_millis()
        );

        // set_projections(env, projections, callback);
        stream_ptr
    })
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamNext(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    stream: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    runtime.tokio_runtime.block_on(async {
        //let fetch_start = std::time::Instant::now();
        let next = futures::StreamExt::next(stream).await;
        //let fetch_time = fetch_start.elapsed();
        match next {
            Some(Ok(batch)) => {
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
            None => {
                set_object_result_ok(&mut env, callback, 0 as *mut FFI_ArrowSchema);
            }
            Some(Err(err)) => {
                set_object_result_error(&mut env, callback, &err);
            }
        }
        //println!("Total time: {:?}", start.elapsed());
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamGetSchema(
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

    // Safety checks
    if runtime_ptr == 0 {
        let error = DataFusionError::Execution("Null runtime pointer".to_string());
        set_object_result_error(&mut env, callback, &error);
        return 0;
    }

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

    runtime.tokio_runtime.block_on(async {
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

        let stream_ptr = Box::into_raw(Box::new(stream)) as jlong;

        stream_ptr
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
    let _ = unsafe { Box::from_raw(stream as *mut SendableRecordBatchStream) };
}

/// Destroy a CustomCacheManager instance
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_destroyCustomCacheManager(
    mut env: JNIEnv,
    _class: JClass,
    cache_manager_ptr: jlong,
) {
    if cache_manager_ptr != 0 {
        // Simply destroy the CustomCacheManager by converting back to Box and letting it drop
        let _ = unsafe { Box::from_raw(cache_manager_ptr as *mut custom_cache_manager::CustomCacheManager) };
        println!("[CACHE INFO] CustomCacheManager destroyed");
    }
}

/// Generic cache creation method that handles all cache types
/// This creates a cache and stores it in CustomCacheManager
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createCache(
    mut env: JNIEnv,
    _class: JClass,
    cache_manager_ptr: jlong,
    cache_type: JString,
    size_limit: jlong,
    eviction_type: JString,
) -> jlong {
    if cache_manager_ptr == 0 {
        let _ = env.throw_new("java/lang/DataFusionException", "CustomCacheManager pointer is null");
        return 0;
    }

    // Convert Java strings to Rust strings
    let cache_type_str: String = match env.get_string(&cache_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert cache_type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/lang/DataFusionException", &msg);
            return 0;
        }
    };

    let eviction_type_str: String = match env.get_string(&eviction_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert eviction_type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/lang/DataFusionException", &msg);
            return 0;
        }
    };

    println!("[CACHE INFO] Creating cache: type={}, size_limit={}, eviction_type={}",
             cache_type_str, size_limit, eviction_type_str);

    let manager = unsafe { &mut *(cache_manager_ptr as *mut custom_cache_manager::CustomCacheManager) };

    match cache_type_str.as_str() {
        cache::CACHE_TYPE_METADATA => {
            // Create a new metadata cache with MutexFileMetadataCache wrapper
            let inner_cache = DefaultFilesMetadataCache::new(size_limit as usize);
            let metadata_cache = Arc::new(cache::MutexFileMetadataCache::new(inner_cache));

            // Set it in CustomCacheManager
            manager.set_file_metadata_cache(metadata_cache);
            println!("[CACHE INFO] Successfully created {} cache in CustomCacheManager", cache_type_str);
        }
        cache::CACHE_TYPE_STATS => {
            let msg = "Stats cache not yet implemented";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/lang/DataFusionException", msg);
            return 0;
        }
        _ => {
            let msg = format!("Invalid cache type: {}", cache_type_str);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/lang/DataFusionException", &msg);
            return 0;
        }
    }

    // Return success indicator
    0
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerAddFiles(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    files: JObjectArray,
) {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    // Check if custom_cache_manager exists
    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            // Parse the array of file paths
            let file_paths: Vec<String> = match parse_string_arr(&mut env, files) {
                Ok(paths) => paths,
                Err(e) => {
                    let msg = format!("Failed to parse file paths array: {}", e);
                    eprintln!("[CACHE ERROR] {}", msg);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                    return;
                }
            };

            // Use the add_files method directly
            match manager.add_files(&file_paths) {
                Ok(results) => {
                    let mut failed_files = Vec::new();
                    for (file_path, success) in results {
                        if !success {
                            failed_files.push(file_path);
                        }
                    }

                    if !failed_files.is_empty() {
                        let msg = format!("Failed to add {} files to cache: {:?}", failed_files.len(), failed_files);
                        eprintln!("[CACHE ERROR] {}", msg);
                    }
                }
                Err(e) => {
                    let msg = format!("Failed to add files to cache: {}", e);
                    eprintln!("[CACHE ERROR] {}", msg);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                }
            }
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerRemoveFiles(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    files: JObjectArray,
) {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    // Parse the array of file paths
    let file_paths: Vec<String> = match parse_string_arr(&mut env, files) {
        Ok(paths) => paths,
        Err(e) => {
            let msg = format!("Failed to parse file paths array: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return;
        }
    };

        // Check if custom_cache_manager exists
    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            // Use the remove_files method directly
            match manager.remove_files(&file_paths) {
                Ok(results) => {
                    let mut failed_files = Vec::new();
                    for (file_path, removed) in results {
                        if !removed {
                            failed_files.push(file_path);
                        }
                    }

                    if !failed_files.is_empty() {
                        let msg = format!("Failed to remove {} files from cache: {:?}", failed_files.len(), failed_files);
                        eprintln!("[CACHE ERROR] {}", msg);
                    }
                }
                Err(e) => {
                    let msg = format!("Failed to remove files from cache: {}", e);
                    eprintln!("[CACHE ERROR] {}", msg);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                }
            }
        }
         None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerClear(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
) {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

      match &runtime_env.custom_cache_manager {
        Some(manager) => {
            manager.clear_all();
            println!("[CACHE INFO] Successfully cleared all caches");
        } None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerUpdateSizeLimitForCacheType(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    cache_type: JString,
    new_size_limit: jlong,
) -> bool {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return false;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    let cache_type: String = match env.get_string(&cache_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert cache type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return false;
        }
    };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
                match cache_type.as_str() {
                    cache::CACHE_TYPE_METADATA => {
                        manager.update_metadata_cache_limit(new_size_limit as usize);
                        true
                    }
                    _ => {
                        let msg = format!("Unknown cache type: {}", cache_type);
                        eprintln!("[CACHE ERROR] {}", msg);
                        let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                        false
                    }
            }
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
            return false;
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerGetMemoryConsumedForCacheType(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    cache_type: JString,
) -> jlong {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return 0;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    let cache_type: String = match env.get_string(&cache_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert cache type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return 0;
        }
    };
    match &runtime_env.custom_cache_manager {
        Some(manager) => {
        match cache_type.as_str() {
            cache::CACHE_TYPE_METADATA => {
                manager.get_total_memory_consumed() as jlong
            }
            _ => {
                let msg = format!("Unknown cache type: {}", cache_type);
                eprintln!("[CACHE ERROR] {}", msg);
                let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                0
            }
        }
    }
    None => {
    let msg = "No custom cache manager available";
    eprintln!("[CACHE ERROR] {}", msg);
    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
    return 0;
    }
    }
}

/// Get total memory consumed by all caches
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerGetTotalMemoryConsumed(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
) -> jlong {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return 0;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

     match &runtime_env.custom_cache_manager {
        Some(manager) => {
            let total = manager.get_total_memory_consumed();
            total as jlong
        }
         None => {
            0
         }
        }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerClearByCacheType(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    cache_type: JString,
) {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    let cache_type: String = match env.get_string(&cache_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert cache type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return;
        }
    };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            match manager.clear_cache_type(&cache_type) {
                Ok(_) => {
                    println!("[CACHE INFO] Cache Type: {} cleared", cache_type);
                }
                Err(e) => {
                    eprintln!("[CACHE ERROR] {}", e);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &e);
                }
            }
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
        }
    }
}


// Method added for Testing purposes only
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerGetItemByCacheType(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    cache_type: JString,
    file_path: JString,
) -> bool {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return false;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    let cache_type: String = match env.get_string(&cache_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert cache type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return false;
        }
    };

    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert file path string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return false;
        }
    };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            match cache_type.as_str() {
                cache::CACHE_TYPE_METADATA => {
                    manager.contains_file(&file_path)
                }
                _ => {
                    let msg = format!("Unknown cache type: {}", cache_type);
                    eprintln!("[CACHE ERROR] {}", msg);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                    false
                }
            }
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
            false
        }
    }
}
