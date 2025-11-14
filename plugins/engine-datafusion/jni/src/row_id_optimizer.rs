/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fs;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow_schema::SchemaRef;
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileScanConfig, FileScanConfigBuilder},
        source::DataSourceExec,
    },
    error::DataFusionError,
    logical_expr::Operator,
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    physical_expr::{expressions::{BinaryExpr, Column}, PhysicalExpr},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{filter::FilterExec, projection::{ProjectionExec,ProjectionExpr}, ExecutionPlan},
};
use datafusion::physical_plan::projection::new_projections_for_columns;

#[derive(Debug)]
pub struct ProjectRowIdOptimizer;

impl ProjectRowIdOptimizer {

    /// Helper to build new schema and projection info with added `row_base` column.
    fn build_updated_file_source_schema(
        &self,
        datasource: &FileScanConfig,
        datasource_exec_schema: SchemaRef
    ) -> (SchemaRef, Vec<usize>) {
        // Clone projection and add new field index
        let mut projections = datasource.projection.clone().unwrap_or_default();
        let file_source_schema = datasource.file_schema.clone();

        let mut new_projections = vec![];

        // let mut fields = vec![];
        for field_name in datasource_exec_schema.fields().to_vec() {
            new_projections.push(file_source_schema.index_of(field_name.name()).unwrap());
        }

        // for field in file_source_schema.fields().to_vec() {
        //     if datasource_exec_schema.field_with_name(&field.name().clone()).is_ok() {
        //         fields.push(Arc::new(Field::new(field.name(), field.data_type().clone(), field.is_nullable())));
        //     }
        // }

        if !projections.contains(&file_source_schema.index_of("___row_id").unwrap()) {
            new_projections.push(file_source_schema.index_of("___row_id").unwrap());

            // let field  = file_source_schema.field_with_name(&*"___row_id").expect("Field ___row_id not found in file_source_schema");
            // fields.push(Arc::new(Field::new("___row_id", field.data_type().clone(), field.is_nullable())));
        }
        new_projections.push(file_source_schema.fields.len());
        // fields.push(Arc::new(Field::new("row_base", file_source_schema.field_with_name("___row_id").unwrap().data_type().clone(), true)));

        // Add row_base field to schema

        let mut new_fields = file_source_schema.fields().clone().to_vec();
        new_fields.push(Arc::new(Field::new("row_base", file_source_schema.field_with_name("___row_id").unwrap().data_type().clone(), true)));

        let new_schema = Arc::new(Schema {
            metadata: file_source_schema.metadata().clone(),
            fields: Fields::from(new_fields),
        });

        (new_schema, new_projections)
    }

    /// Creates a projection expression that adds `row_base` to `___row_id`.
    fn build_projection_exprs(&self, new_schema: &SchemaRef) -> Result<Vec<(Arc<dyn PhysicalExpr>, String)>, DataFusionError> {
        let row_id_idx = new_schema.index_of("___row_id").expect("Field ___row_id missing");
        let row_base_idx = new_schema.index_of("row_base").expect("Field row_base missing");

        let sum_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("___row_id", row_id_idx)),
            Operator::Plus,
            Arc::new(Column::new("row_base", row_base_idx)),
        ));

        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();

        let mut has_row_id = false;
        for field_name in new_schema.fields().to_vec() {

            if field_name.name() == "___row_id" {
                projection_exprs.push((sum_expr.clone(), field_name.name().clone()));
                has_row_id = true;
            } else if(field_name.name() != "row_base") {
                // Match the column by name from new_schema
                let idx = new_schema.index_of(&*field_name.name().clone())
                    .unwrap_or_else(|_| panic!("Field {field_name} missing in schema"));
                projection_exprs.push((Arc::new(Column::new(&*field_name.name(), idx)), field_name.name().clone()));
            }
        }
        if !has_row_id {
            projection_exprs.push((sum_expr.clone(), "___row_id".parse().unwrap()));
        }
        Ok(projection_exprs)
    }

    fn create_datasource_projection(&self, datasource: &FileScanConfig, data_source_exec_schema: SchemaRef) -> Result<ProjectionExec, DataFusionError> {
        let (new_schema, new_projections) = self.build_updated_file_source_schema(datasource, data_source_exec_schema.clone());
        let file_scan_config = FileScanConfigBuilder::from(datasource.clone())
            .with_source(datasource.file_source.with_schema(new_schema.clone()))
            .with_projection(Some(new_projections))
            .build();

        let new_datasource = DataSourceExec::from_data_source(file_scan_config);
        let projection_exprs = self.build_projection_exprs(&new_datasource.schema()).expect("Failed to build projection expressions");

        Ok(ProjectionExec::try_new(projection_exprs, new_datasource)
            .expect("Failed to create ProjectionExec"))
    }
}

impl PhysicalOptimizerRule for ProjectRowIdOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {

        let rewritten = plan.transform_up(|node| {
            if let Some(datasource_exec) = node.as_any().downcast_ref::<DataSourceExec>() {

                let datasource = datasource_exec
                    .data_source()
                    .as_ref()
                    .as_any()
                    .downcast_ref::<FileScanConfig>()
                    .expect("DataSource not found");
                let schema = datasource.file_schema.clone();
                schema.field_with_name("___row_id").expect("Field ___row_id missing");
                let projection = self.create_datasource_projection(datasource, datasource_exec.schema()).expect("Failed to create ProjectionExec from datasource");
                return Ok(Transformed::new(Arc::new(projection), true, TreeNodeRecursion::Continue));

            } else if let Some(projection_exec) = node.as_any().downcast_ref::<ProjectionExec>() {
                if !projection_exec.schema().field_with_name("___row_id").is_ok() {

                    let mut projection_exprs = projection_exec.expr().to_vec();
                    if projection_exec.input().schema().index_of("___row_id").is_ok() {
                        let row_id_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("___row_id", projection_exec.input().schema().index_of("___row_id").unwrap()));
                        projection_exprs.push(ProjectionExpr::new(row_id_col, "___row_id".to_string()));
                    }

                    let projection = ProjectionExec::try_new(projection_exprs, projection_exec.input().clone()).expect("Failed to create projection exec");
                    return Ok(Transformed::new(Arc::new(projection.clone()), true, TreeNodeRecursion::Continue));
                }
            }

            Ok(Transformed::no(node))
        })?;

        Ok(rewritten.data)
    }

    fn name(&self) -> &str {
        "project_row_id_optimizer"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
