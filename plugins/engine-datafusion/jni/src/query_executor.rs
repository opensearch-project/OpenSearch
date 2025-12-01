/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;
use std::collections::{BTreeSet, HashMap};
use jni::sys::jlong;
use datafusion::{
    common::DataFusionError,
    datasource::file_format::parquet::ParquetFormat,
    datasource::listing::ListingTableUrl,
    datasource::object_store::ObjectStoreUrl,
    datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess},
    datasource::physical_plan::ParquetSource,
    execution::cache::cache_manager::CacheManagerConfig,
    execution::cache::cache_unit::DefaultListFilesCache,
    execution::cache::CacheAccessor,
    execution::context::SessionContext,
    execution::runtime_env::RuntimeEnvBuilder,
    execution::TaskContext,
    parquet::arrow::arrow_reader::RowSelector,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
    prelude::*,
};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::{Plan, extensions::simple_extension_declaration::MappingType};
use object_store::ObjectMeta;
use prost::Message;
use arrow_schema::DataType;
use log::error;

use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::partial_agg_optimizer::PartialAggregationOptimizer;
use crate::executor::DedicatedExecutor;
use crate::cross_rt_stream::CrossRtStream;
use crate::CustomFileMeta;
use crate::DataFusionRuntime;
use crate::row_id_optimizer::ProjectRowIdOptimizer;

pub async fn execute_query_with_cross_rt_stream(
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

    let file_metadata_cache = runtime.runtime_env.cache_manager.get_file_metadata_cache();

    let runtime_env = match RuntimeEnvBuilder::from_runtime_env(runtimeEnv)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache.clone()))
                .with_file_metadata_cache(Some(file_metadata_cache.clone()))
                .with_metadata_cache_limit(file_metadata_cache.cache_limit())
                .with_files_statistics_cache(runtimeEnv.cache_manager.get_file_statistic_cache()),
        ).build() {
        Ok(env) => env,
        Err(e) => {
            error!("Failed to build runtime env: {}", e);
            return Err(e);
        }
    };

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = false;
    config.options_mut().execution.target_partitions = 1;
    config.options_mut().execution.batch_size = 1024;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config.clone())
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        //.with_physical_optimizer_rule(Arc::new(ProjectRowIdOptimizer)) // TODO : uncomment this after fix
        .with_physical_optimizer_rule(Arc::new(PartialAggregationOptimizer))
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Register table
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_files_metadata(files_meta)
        .with_session_config_options(&config)
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

    let df_stream = match dataframe.execute_stream().await {
        Ok(stream) => stream,
        Err(e) => {
            error!("Failed to create execution stream: {}", e);
            return Err(e);
        }
    };

    Ok(get_cross_rt_stream(cpu_executor, df_stream))
}

pub fn get_cross_rt_stream(cpu_executor: DedicatedExecutor, df_stream: SendableRecordBatchStream) -> jlong {
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(
        df_stream,
        cpu_executor,
    );

    let wrapped_stream = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    Box::into_raw(Box::new(wrapped_stream)) as jlong
}

pub async fn execute_fetch_phase(
    table_path: ListingTableUrl,
    files_metadata: Arc<Vec<CustomFileMeta>>,
    row_ids: Vec<jlong>,
    projections: Vec<String>,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<jlong, DataFusionError> {
    let access_plans = create_access_plans(row_ids, files_metadata.clone()).await?;

    let object_meta: Arc<Vec<ObjectMeta>> = Arc::new(
        files_metadata
            .iter()
            .map(|metadata| (*metadata.object_meta).clone())
            .collect(),
    );

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), object_meta);

    let file_metadata_cache = runtime.runtime_env.cache_manager.get_file_metadata_cache();

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(
            CacheManagerConfig::default().with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(file_metadata_cache.clone()))
                .with_files_statistics_cache(runtime.runtime_env.cache_manager.get_file_statistic_cache())
                .with_metadata_cache_limit(file_metadata_cache.cache_limit()),
        )
        .build()?;
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime_env));

    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    let parquet_schema = listing_options.infer_schema(&ctx.state(), &table_path).await?;

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
    let file_source = Arc::new(ParquetSource::default());

    let mut projection_index = vec![];
    for field_name in projections.iter() {
        projection_index.push(
            parquet_schema
                .index_of(field_name)
                .map_err(|_| DataFusionError::Execution(format!("Projected field {} not found in Schema", field_name)))?,
        );
    }

    let file_scan_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        parquet_schema.clone(),
        file_source,
    )
    .with_projection(Option::from(projection_index.clone()))
    .with_file_group(file_group)
    .build();

    let parquet_exec = DataSourceExec::from_data_source(file_scan_config);
    let optimized_plan: Arc<dyn ExecutionPlan> = parquet_exec.clone();
    let task_ctx = Arc::new(TaskContext::default());
    let stream = optimized_plan.execute(0, task_ctx)?;

    Ok(get_cross_rt_stream(cpu_executor, stream))
}

async fn create_access_plans(
    row_ids: Vec<jlong>,
    files_metadata: Arc<Vec<CustomFileMeta>>,
) -> Result<Vec<ParquetAccessPlan>, DataFusionError> {
    let mut access_plans = Vec::new();
    let mut sorted_row_ids: Vec<i64> = row_ids.iter().map(|&id| id as i64).collect();
    sorted_row_ids.sort_unstable();

    for file_meta in files_metadata.iter() {
        let row_base = *file_meta.row_base;
        let total_row_groups = file_meta.row_group_row_counts.len();
        let mut access_plan = ParquetAccessPlan::new_all(total_row_groups);

        let file_total_rows: i64 = file_meta.row_group_row_counts.iter().map(|&x| x).sum();
        let file_end_row: i64 = row_base + file_total_rows;
        let file_row_ids: Vec<i64> = sorted_row_ids
            .iter()
            .copied()
            .filter(|&id| id >= row_base && id < file_end_row)
            .map(|id| id - row_base)
            .collect();

        if file_row_ids.is_empty() {
            for group_id in 0..total_row_groups {
                access_plan.skip(group_id);
            }
        } else {
            let mut cumulative_group_rows: Vec<i64> = Vec::with_capacity(total_row_groups + 1);
            cumulative_group_rows.push(0);
            let mut current_sum = 0;
            for &count in file_meta.row_group_row_counts.iter() {
                current_sum += count;
                cumulative_group_rows.push(current_sum);
            }

            let mut group_map: HashMap<usize, BTreeSet<i32>> = HashMap::new();
            for &row_id in &file_row_ids {
                let group_id = cumulative_group_rows
                    .windows(2)
                    .position(|window| row_id >= window[0] as i64 && row_id < window[1] as i64)
                    .unwrap();

                let relative_pos = row_id - cumulative_group_rows[group_id];
                group_map
                    .entry(group_id)
                    .or_default()
                    .insert(relative_pos as i32);
            }

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
                        let mut selectors = Vec::new();
                        let mut current_pos = 0;
                        let mut i = 0;
                        while i < relative_row_ids.len() {
                            let target_pos = relative_row_ids[i];
                            if target_pos > current_pos {
                                selectors.push(RowSelector::skip(target_pos - current_pos));
                            }
                            let mut select_count = 1;
                            while i + 1 < relative_row_ids.len()
                                && relative_row_ids[i + 1] == relative_row_ids[i] + 1
                            {
                                select_count += 1;
                                i += 1;
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
