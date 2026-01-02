/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;
use std::collections::{BTreeSet, HashMap, HashSet};
use datafusion::common::stats::Precision;
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
use arrow_schema::{DataType, Field, SchemaRef};
use chrono::TimeZone;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_expr::{LogicalPlan, Projection};
use log::error;
use object_store::path::Path;
use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::partial_agg_optimizer::PartialAggregationOptimizer;
use crate::executor::DedicatedExecutor;
use crate::cross_rt_stream::CrossRtStream;
use crate::{CustomFileMeta, FileStats};
use crate::DataFusionRuntime;
use crate::project_row_id_analyzer::ProjectRowIdAnalyzer;
use crate::absolute_row_id_optimizer::{AbsoluteRowIdOptimizer, ROW_BASE_FIELD_NAME, ROW_ID_FIELD_NAME};

/// Executes a query using DataFusion with cross-runtime streaming capabilities.
/// This function sets up the complete query execution pipeline including table registration,
/// plan optimization, and stream creation for efficient data processing across different runtimes.
///
/// # Arguments
/// * `table_path` - The URL path to the table data source (typically a directory containing Parquet files)
/// * `files_meta` - Metadata for all files in the table, including row counts and base offsets
/// * `table_name` - Name to register the table under in the DataFusion context
/// * `plan_bytes_vec` - Serialized Substrait query plan as bytes
/// * `is_aggregation_query` - Flag indicating if this is an aggregation query (affects optimization strategy)
/// * `runtime` - The DataFusion runtime environment containing configuration and caches
/// * `cpu_executor` - Dedicated executor for CPU-intensive operations
///
/// # Returns
/// A pointer (as jlong) to the cross-runtime stream that can be consumed from Java/JNI
///
/// # Process Overview
/// 1. Sets up file caching and runtime environment for optimal performance
/// 2. Configures session with appropriate settings (batch size, partitions, etc.)
/// 3. Registers the table with proper schema inference and partition columns
/// 4. Decodes and processes the Substrait plan, applying necessary transformations
/// 5. Applies query-specific optimizations (row ID handling for non-aggregation queries)
/// 6. Creates and returns a cross-runtime stream for result consumption
/// # Row ID Optimization Strategy (for non-aggregation queries)
///
/// The system uses a multi-phase approach to ensure queries return absolute row IDs:
///
/// **Phase 1: Logical Plan Analysis (ProjectRowIdAnalyzer)**
/// - Ensures ___row_id fields are included in TableScan projections
/// - Propagates ___row_id through Projection nodes in the logical plan
/// - Works at the logical level before physical plan generation
///
/// **Phase 2: Physical Plan Optimization (AbsoluteRowIdOptimizer)**
/// - Transforms relative row IDs to absolute row IDs at execution time
/// - Replaces ___row_id expressions with (___row_id + row_base) calculations
/// - row_base comes from partition columns and represents the file's starting row offset
///
/// **Phase 3: Final Projection**
/// - Creates a top-level projection that only selects ___row_id
/// - Ensures query results contain only the row identifiers needed for fetch operations
///
/// **Why This Approach:**
/// - Parquet files store relative row IDs (0-based within each file)
/// - We need absolute row IDs for global row identification across all files
/// - The row_base partition column provides the offset to convert relative → absolute
/// - This enables efficient two-phase query execution: filter → fetch
pub async fn execute_query_with_cross_rt_stream(
    table_path: ListingTableUrl,
    files_meta: Arc<Vec<CustomFileMeta>>,
    table_name: String,
    plan_bytes_vec: Vec<u8>,
    is_query_plan_explain_enabled: bool,
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
                .with_file_metadata_cache(Some(runtimeEnv.cache_manager.get_file_metadata_cache()))
                .with_files_statistics_cache(runtimeEnv.cache_manager.get_file_statistic_cache()),
        )
        .with_metadata_cache_limit(250 * 1024 * 1024) // 250 MB
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
    config.options_mut().execution.batch_size = 8192;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config.clone())
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(PartialAggregationOptimizer))
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Register table with partition column setup for row ID optimization
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_files_metadata(files_meta)
        .with_session_config_options(&config)
        .with_collect_stat(true)
        // Critical: Set up row_base as a partition column
        // This makes row_base available in expressions during query execution
        // row_base contains the starting row offset for each file, enabling
        // the AbsoluteRowIdOptimizer to convert relative row IDs to absolute ones
        .with_table_partition_cols(vec![(ROW_BASE_FIELD_NAME.to_string(), DataType::Int64)]);

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

    let mut logical_plan = match from_substrait_plan(&ctx.state(), &modified_plan).await {
        Ok(plan) => plan,
        Err(e) => {
            error!("Failed to convert Substrait plan: {}", e);
            return Err(e);
        }
    };

    let is_aggregation_query = is_aggs_query(&logical_plan);

    // For non-aggregation queries, we apply a two-phase optimization strategy to ensure
    // only absolute row IDs are returned, which is essential for subsequent fetch operations
    if !is_aggregation_query {
        // Phase 1: ProjectRowIdAnalyzer (Logical Plan Analysis)
        // This analyzer works at the logical plan level and ensures that:
        // 1. TableScan nodes include the ___row_id field in their projections
        // 2. Projection nodes propagate the ___row_id field through the query tree
        // 3. The ___row_id field is available at every level of the plan for later optimization
        logical_plan = ProjectRowIdAnalyzer.analyze(logical_plan, ctx.state().config_options())?;

        // Phase 2: Top-level Projection Restriction
        // Create a final projection that ONLY selects the ___row_id field
        // This ensures the query result contains only the row identifiers needed for the fetch phase
        // The AbsoluteRowIdOptimizer (applied earlier) will later transform these relative IDs
        // into absolute IDs during physical plan execution.
        // Creation of final projection is needed since in some case top-level projection is missing
        // from the plan if final projection schema matches downstream exec schemas, making
        // projection exec redundant.
        // OptimizeProjections LogicalPlan optimizer is applied during execution which removes any
        // additional projection execs are present.
        logical_plan = LogicalPlan::Projection(Projection::try_new(
            vec![col(ROW_ID_FIELD_NAME.to_string())],
            Arc::new(logical_plan),
        ).expect("Failed to create top level projection with ___row_id"));
    }

    let mut dataframe = match ctx.execute_logical_plan(logical_plan).await {
        Ok(df) => df,
        Err(e) => {
            error!("Failed to execute logical plan: {}", e);
            return Err(e);
        }
    };

    let mut physical_plan = dataframe.clone().create_physical_plan().await?;

    // For non-aggregation queries, we need to return absolute row IDs to identify specific rows
    // The AbsoluteRowIdOptimizer works at the physical plan level to transform relative row IDs
    // into absolute ones by adding the partition's row_base offset
    if !is_aggregation_query {
        // AbsoluteRowIdOptimizer: Transforms ___row_id expressions in the physical plan
        // It finds expressions that reference ___row_id and replaces them with:
        // ___row_id + row_base (where row_base comes from partition columns)
        // This converts file-relative row IDs to globally unique absolute row IDs
        physical_plan = AbsoluteRowIdOptimizer.optimize(physical_plan, ctx.state().config_options())
            .expect("Failed to optimize physical plan");
    }


    if is_query_plan_explain_enabled {
        println!("---- Explain plan ----");
        let clone_df = dataframe.clone().explain(false, true).expect("Failed to explain plan");
        clone_df.show().await?;
    }

    let df_stream = match execute_stream(physical_plan, ctx.task_ctx()) {
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

/// Executes the fetch phase of a two-phase query execution strategy.
/// This function takes absolute row IDs (returned from the query phase) and efficiently
/// retrieves the actual row data using Parquet's row-level access capabilities.
///
/// # Two-Phase Query Execution Strategy
///
/// **Phase 1 (Query):** `execute_query_with_cross_rt_stream`
/// - Applies filters and conditions to identify matching rows
/// - Returns only absolute row IDs (___row_id) for matching rows
/// - Uses optimizers to ensure row IDs are absolute (not file-relative)
///
/// **Phase 2 (Fetch):** This function
/// - Takes the absolute row IDs from phase 1
/// - Creates optimized Parquet access plans for targeted row retrieval
/// - Fetches only the requested columns for the identified rows
/// - Reconstructs absolute row IDs by adding row_base back to relative IDs
///
/// # Arguments
/// * `table_path` - The URL path to the table data source
/// * `files_metadata` - Metadata for all files including row counts and base offsets
/// * `row_ids` - Absolute row IDs to fetch (from query phase)
/// * `include_fields` - Specific fields to include in the result
/// * `exclude_fields` - Fields to exclude from the result
/// * `runtime` - The DataFusion runtime environment
/// * `cpu_executor` - Dedicated executor for CPU-intensive operations
///
/// # Returns
/// A pointer (as jlong) to the cross-runtime stream containing the fetched row data
///
/// # Optimization Details
/// - Uses ParquetAccessPlan for efficient row-group level access
/// - Skips entire row groups that don't contain target rows
/// - Uses RowSelector for precise row-level filtering within row groups
/// - Reconstructs absolute row IDs using row_base + relative_row_id calculation
pub async fn execute_fetch_phase(
    table_path: ListingTableUrl,
    files_metadata: Arc<Vec<CustomFileMeta>>,
    row_ids: Vec<jlong>,
    include_fields: Vec<String>,
    exclude_fields: Vec<String>,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<jlong, DataFusionError> {
    // Create optimized Parquet access plans for targeted row retrieval
    // This converts absolute row IDs back to file-relative positions and creates
    // efficient access patterns for each file's row groups
    let access_plans = create_access_plans(row_ids, files_metadata.clone()).await?;

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
                         .with_metadata_cache_limit(runtime.runtime_env.cache_manager.get_file_metadata_cache().cache_limit())
                .with_file_metadata_cache(Some(runtime.runtime_env.cache_manager.get_file_metadata_cache().clone()))
                .with_files_statistics_cache(runtime.runtime_env.cache_manager.get_file_statistic_cache()),

        )
        .build()?;

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.target_partitions = 1;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);

    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet").with_collect_stat(true);

    let parquet_schema = listing_options.infer_schema(&ctx.state(), &table_path).await?;
    let projections = create_projections(include_fields, exclude_fields, parquet_schema.clone());

    let partitioned_files: Vec<PartitionedFile> = files_metadata
        .iter()
        .zip(access_plans.iter())
        .map(|(meta, access_plan)| {
            PartitionedFile {
                object_meta:  ObjectMeta {
                    location: Path::from(meta.object_meta().location.to_string()),
                    last_modified: chrono::Utc.timestamp_nanos(0),
                    size: meta.object_meta.size,
                    e_tag: None,
                    version: None,
                },
                partition_values: vec![ScalarValue::Int64(Some(*meta.row_base))],
                range: None,
                statistics: None,
                extensions: None,
                metadata_size_hint: None,
            }
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

    // Ensure ___row_id is always included in projections for absolute row ID reconstruction
    // Even if not explicitly requested, we need it to rebuild absolute row IDs
    if(!projections.contains(&ROW_ID_FIELD_NAME.to_string())) {
        projection_index.push(parquet_schema.index_of(ROW_ID_FIELD_NAME).unwrap());
    }
    // Add row_base partition column index for absolute row ID calculation
    projection_index.push(parquet_schema.fields.len());

    let file_scan_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        parquet_schema.clone(),
        file_source,
    )
    .with_table_partition_cols(vec![Field::new(ROW_BASE_FIELD_NAME, DataType::Int64, false)])
    .with_projection_indices(Some(projection_index.clone()))
    .with_file_group(file_group)
    .build();

    let parquet_exec = DataSourceExec::from_data_source(file_scan_config.clone());

    let projection_exprs = build_projection_exprs(file_scan_config.projected_schema())
        .expect("Failed to build projection expressions");

    let projection_exec = Arc::new(ProjectionExec::try_new(projection_exprs, parquet_exec)
        .expect("Failed to create ProjectionExec"));
    let optimized_plan: Arc<dyn ExecutionPlan> = projection_exec.clone();
    let task_ctx = Arc::new(TaskContext::default());
    let stream = optimized_plan.execute(0, task_ctx)?;

    Ok(get_cross_rt_stream(cpu_executor, stream))
}

fn is_aggs_query(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => {
            true
        },
        LogicalPlan::TableScan(_) => {
            // reached leaf
            false
        },
        // … handle other variants as needed …
        other => {
            let mut is_aggs = false;
            for child in other.inputs() {
                is_aggs = is_aggs || is_aggs_query(child);
                if is_aggs {
                    return is_aggs;
                }
            }
            is_aggs
        }
    }
}

pub fn create_projections(
    include_fields: Vec<String>,
    exclude_fields: Vec<String>,
    schema: SchemaRef,
) -> Vec<String> {

    // Get all field names from schema
    let all_fields: Vec<String> = schema.fields().to_vec().iter().map(|f| f.name().to_string()).collect();

    match (include_fields.is_empty(), exclude_fields.is_empty()) {

        // includes empty, excludes empty → all fields
        (true, true) => all_fields.clone(),

        // includes non-empty → include only these fields
        (false, _) => include_fields
            .into_iter()
            .filter(|f| schema.field_with_name(f).is_ok())     // keep valid fields
            .collect(),

        // includes empty, excludes non-empty → remove excludes
        (true, false) => {
            let exclude_set: HashSet<String> =
                exclude_fields.into_iter().collect();

            all_fields
                .into_iter()
                .filter(|f| !exclude_set.contains(f))
                .collect()
        }
    }
}

/// Builds projection expressions that reconstruct absolute row IDs during fetch phase.
/// This function creates the physical expressions needed to convert file-relative row IDs
/// back to absolute row IDs by adding the row_base offset.
///
/// # Absolute Row ID Reconstruction
/// During the fetch phase, we read data directly from Parquet files, which contain
/// relative row IDs (0-based within each file). To maintain consistency with the
/// query phase results, we need to reconstruct the absolute row IDs using:
///
/// **absolute_row_id = relative_row_id + row_base**
///
/// Where:
/// - relative_row_id: The ___row_id field from the Parquet file (0-based)
/// - row_base: The partition column value representing this file's starting offset
/// - absolute_row_id: The globally unique row identifier
fn build_projection_exprs(new_schema: SchemaRef) -> std::result::Result<Vec<(Arc<dyn PhysicalExpr>, String)>, DataFusionError> {
    // Get column indices for the row ID reconstruction calculation
    let row_id_idx = new_schema.index_of(ROW_ID_FIELD_NAME).expect("Field ___row_id missing");
    let row_base_idx = new_schema.index_of(ROW_BASE_FIELD_NAME).expect("Field row_base missing");

    // Create the expression: ___row_id + row_base = absolute_row_id
    // This reconstructs the absolute row ID that was originally returned by the query phase
    let sum_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(datafusion::physical_expr::expressions::Column::new(ROW_ID_FIELD_NAME, row_id_idx)),
        Operator::Plus,
        Arc::new(datafusion::physical_expr::expressions::Column::new(ROW_BASE_FIELD_NAME, row_base_idx)),
    ));

    let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();

    let mut has_row_id = false;
    // Build projection expressions for all requested fields
    for field_name in new_schema.fields().to_vec() {
        if field_name.name() == ROW_ID_FIELD_NAME {
            // For ___row_id field, use the sum expression to get absolute row ID
            // This ensures the fetch phase returns the same absolute row IDs
            // that were originally identified in the query phase
            projection_exprs.push((sum_expr.clone(), field_name.name().clone()));
            has_row_id = true;
        } else if(field_name.name() != ROW_BASE_FIELD_NAME) {
            // For regular data fields, project them directly from the file
            // Skip row_base as it's only used for calculation, not output
            let idx = new_schema
                .index_of(&*field_name.name().clone())
                .unwrap_or_else(|_| panic!("Field {field_name} missing in schema"));
            projection_exprs.push((
                Arc::new(datafusion::physical_expr::expressions::Column::new(&*field_name.name(), idx)),
                field_name.name().clone(),
            ));
        }
    }

    // Ensure absolute row ID is always available in the output
    // This maintains consistency between query and fetch phases
    if !has_row_id {
        projection_exprs.push((sum_expr.clone(), ROW_ID_FIELD_NAME.parse().unwrap()));
    }
    Ok(projection_exprs)
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
