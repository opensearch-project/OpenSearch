/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Physical optimizer rule for the ListingTable QTF path.
//!
//! Downcasts to `DataSourceExec` → `FileScanConfig`, checks if `__row_id__` is in the
//! file schema, rebuilds the scan to include both `__row_id__` and `row_base` (partition
//! column), then wraps with `ProjectionExec` computing `__row_id__ + row_base`.
//!
//! This approach is resilient to DataFusion's built-in optimizers pruning `row_base`
//! from the scan output — we rebuild the DataSourceExec from the underlying config.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::Result;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::TableSchema;

pub const ROW_ID_FIELD_NAME: &str = "__row_id__";
pub const ROW_BASE_FIELD_NAME: &str = "row_base";

#[derive(Debug)]
pub struct ProjectRowIdOptimizer;

impl PhysicalOptimizerRule for ProjectRowIdOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let rewritten = plan.transform_up(|node| {
            // Only handle DataSourceExec nodes backed by FileScanConfig
            let Some(datasource_exec) = node.as_any().downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(file_scan_config) = datasource_exec
                .data_source()
                .as_ref()
                .as_any()
                .downcast_ref::<FileScanConfig>()
            else {
                return Ok(Transformed::no(node));
            };

            // Check if __row_id__ exists in the file schema
            let file_schema = file_scan_config.file_schema();
            if file_schema.index_of(ROW_ID_FIELD_NAME).is_err() {
                return Ok(Transformed::no(node));
            }

            // Check if this DataSourceExec has partition columns (ShardTableProvider sets row_base)
            let partition_cols = file_scan_config.table_partition_cols();
            if partition_cols.is_empty() {
                return Ok(Transformed::no(node));
            }

            // Rebuild the DataSourceExec to ensure both __row_id__ and row_base are in the output.
            // Get the current output schema fields to determine what's projected.
            let current_schema = datasource_exec.schema();

            // Build new projection indices: current projected file columns + __row_id__ + row_base
            let mut new_proj_indices: Vec<usize> = Vec::new();
            for field in current_schema.fields() {
                if field.name() == ROW_BASE_FIELD_NAME {
                    // row_base is partition col, it'll be added below
                    continue;
                }
                if let Ok(idx) = file_schema.index_of(field.name()) {
                    if !new_proj_indices.contains(&idx) {
                        new_proj_indices.push(idx);
                    }
                }
            }

            // Ensure __row_id__ is in the projection
            let row_id_file_idx = file_schema.index_of(ROW_ID_FIELD_NAME).unwrap();
            if !new_proj_indices.contains(&row_id_file_idx) {
                new_proj_indices.push(row_id_file_idx);
            }

            // Add partition column index (row_base is at file_schema.fields().len())
            let row_base_partition_idx = file_schema.fields().len();
            new_proj_indices.push(row_base_partition_idx);

            // Rebuild the DataSourceExec with new projections
            let new_table_schema = TableSchema::new(
                file_schema.clone(),
                partition_cols.clone(),
            );
            let new_source = Arc::new(ParquetSource::new(new_table_schema));

            let new_config = FileScanConfigBuilder::from(file_scan_config.clone())
                .with_source(new_source)
                .with_projection_indices(Some(new_proj_indices))
                .map_err(|e| datafusion::error::DataFusionError::Internal(
                    format!("ProjectRowIdOptimizer: set projection: {}", e)
                ))?
                .build();

            let new_datasource: Arc<dyn ExecutionPlan> =
                DataSourceExec::from_data_source(new_config);

            // Build projection: __row_id__ + row_base, drop row_base from output
            let new_schema = new_datasource.schema();
            let row_id_idx = new_schema.index_of(ROW_ID_FIELD_NAME).unwrap();
            let row_base_idx = new_schema.index_of(ROW_BASE_FIELD_NAME).unwrap();

            let sum_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::new(Column::new(ROW_ID_FIELD_NAME, row_id_idx)),
                Operator::Plus,
                Arc::new(Column::new(ROW_BASE_FIELD_NAME, row_base_idx)),
            ));

            let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
            for (i, field) in new_schema.fields().iter().enumerate() {
                if field.name() == ROW_ID_FIELD_NAME {
                    projection_exprs.push((sum_expr.clone(), ROW_ID_FIELD_NAME.to_string()));
                } else if field.name() == ROW_BASE_FIELD_NAME {
                    continue; // drop from output
                } else {
                    projection_exprs.push((
                        Arc::new(Column::new(field.name(), i)),
                        field.name().clone(),
                    ));
                }
            }

            let projection = ProjectionExec::try_new(projection_exprs, new_datasource)?;
            Ok(Transformed::new(
                Arc::new(projection) as Arc<dyn ExecutionPlan>,
                true,
                TreeNodeRecursion::Continue,
            ))
        })?;

        Ok(rewritten.data)
    }

    fn name(&self) -> &str {
        "ProjectRowIdOptimizer"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optimizer_name() {
        let opt = ProjectRowIdOptimizer;
        assert_eq!(opt.name(), "ProjectRowIdOptimizer");
    }

    #[test]
    fn optimizer_schema_check_disabled() {
        let opt = ProjectRowIdOptimizer;
        assert!(!opt.schema_check());
    }
}
