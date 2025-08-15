/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use jni::objects::JClass;
use jni::sys::{jlong, jstring};
use jni::JNIEnv;
use std::sync::Arc;

use datafusion::execution::context::SessionContext;

use datafusion::DATAFUSION_VERSION;
use datafusion::execution::cache::cache_manager::{CacheManager, CacheManagerConfig, FileStatisticsCache};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionConfig;

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





