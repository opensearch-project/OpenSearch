/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
Indexed query executor — bridges indexed-table into engine-datafusion.

Registers an IndexedTableProvider (Lucene index + parquet) as the table,
then executes the substrait plan against it — same as the normal DF path
but with index-accelerated reads instead of full parquet scans.
**/

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionConfig;
use jni::sys::jlong;
use prost::Message;

use crate::indexed_table::index::BitsetMode;
use crate::indexed_table::table_provider::{IndexedTableConfig, IndexedTableProvider};
use crate::indexed_table::jni_helpers::build_segments;

use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::query_executor::get_cross_rt_stream;
use crate::DataFusionRuntime;

use datafusion::common::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use datafusion_substrait::substrait::proto::extensions::simple_extension_declaration::MappingType;
use vectorized_exec_spi::log_error;

/// Execute an indexed query with a substrait plan and return a CrossRtStream pointer.
pub async fn execute_indexed_query_stream(
    weight_ptr: i64,
    segment_max_docs: Vec<i64>,
    parquet_paths: Vec<String>,
    table_name: String,
    plan_bytes: Vec<u8>,
    num_partitions: usize,
    bitset_mode: BitsetMode,
    is_query_plan_explain_enabled: bool,
    jvm: Arc<jni::JavaVM>,
    searcher_class_ref: jni::objects::GlobalRef,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<RecordBatchStreamAdapter<CrossRtStream>, DataFusionError> {
    let t0 = std::time::Instant::now();

    let searcher = Arc::new(crate::indexed_table::JniShardSearcher::new(
        Arc::clone(&jvm),
        weight_ptr,
        searcher_class_ref,
        segment_max_docs.clone(),
    )) as Arc<dyn crate::indexed_table::index::ShardSearcher>;

    let (segments, schema) = build_segments(&parquet_paths, &segment_max_docs)
        .map_err(|e| DataFusionError::Execution(format!("build_segments: {}", e)))?;

    let provider = IndexedTableProvider::try_new(
        IndexedTableConfig::new(searcher, segments, schema)
            .with_bitset_mode(bitset_mode)
            .with_partitions(num_partitions),
    )?;

    let config = SessionConfig::new()
        .with_target_partitions(num_partitions);

    let ctx = SessionContext::new_with_config(config);
    ctx.register_table(&table_name, Arc::new(provider))
        .map_err(|e| DataFusionError::Execution(format!("register_table: {}", e)))?;

    let t_setup = t0.elapsed();

    let substrait_plan = Plan::decode(plan_bytes.as_slice())
        .map_err(|e| DataFusionError::Execution(format!("Failed to decode Substrait: {}", e)))?;

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

    let logical_plan = from_substrait_plan(&ctx.state(), &modified_plan).await
        .map_err(|e| { eprintln!("Failed to convert Substrait plan: {}", e); e })?;

    let mut dataframe = ctx.execute_logical_plan(logical_plan).await
        .map_err(|e| { eprintln!("Failed to execute logical plan: {}", e); e })?;

    let physical_plan = dataframe.clone().create_physical_plan().await?;

    let t_plan = t0.elapsed();

    // if is_query_plan_explain_enabled {
    //     log_error!("---- Indexed Query Explain Plan ----");
    //     let plan_str = format!("{}", datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true));
    //     for line in plan_str.lines() {
    //         log_error!("  {}", line);
    //     }
    // }

    if is_query_plan_explain_enabled {
        println!("---- Explain plan ----");
        let clone_df = dataframe.clone().explain(false, true).expect("Failed to explain plan");
        clone_df.show().await?;
    }

    let df_stream = execute_stream(physical_plan, ctx.task_ctx())?;

    let t_exec = t0.elapsed();
    eprintln!("[INDEXED-TIMING] setup={}ms, plan={}ms, execute_stream={}ms, explain={}",
        t_setup.as_millis(), (t_plan - t_setup).as_millis(), (t_exec - t_plan).as_millis(), is_query_plan_explain_enabled);

    Ok(get_cross_rt_stream(cpu_executor, df_stream))
}
