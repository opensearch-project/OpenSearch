/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::fs;
use std::sync::Arc;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_plan::projection::ProjectionExec;

#[derive(Debug)]
pub struct FilterRowIdOptimizer;

impl PhysicalOptimizerRule for FilterRowIdOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut is_optimized = false;
        let rewritten = plan.transform_up(|node| {
            if let Some(filter) = node.as_any().downcast_ref::<FilterExec>() {
                // Check if input is DataSourceExec
                if let Some(datasource_exec) = filter.input().as_any().downcast_ref::<DataSourceExec>() {
                    // Check if ___row_id is present
                    let schema = datasource_exec.schema();
                    let has_row_id = schema.field_with_name("___row_id").is_ok();

                    if has_row_id {
                        let mut datasource = datasource_exec.data_source().as_ref().as_any().downcast_ref::<FileScanConfig>().unwrap();
                        // let _ = datasource.projection.insert(vec![0]);
                        let mut new_projections = datasource.clone().projection.clone().unwrap();
                        let file_schema = ParquetRecordBatchReaderBuilder::try_new(fs::File::open("/".to_owned()+&datasource.file_groups[0].files()[0].path().to_string()).unwrap()).unwrap();

                        new_projections.push(file_schema.schema().fields().len());

                        let mut fields = schema.fields().clone().to_vec();
                        fields.insert(fields.len(), Arc::new(Field::new("row_base", DataType::Int32, true)));
                        let new_schema = Arc::new(Schema{metadata: schema.metadata().clone(), fields: Fields::from(fields)});

                        let file_scan_config =
                            FileScanConfigBuilder::from(datasource.clone())
                                .with_source(datasource.clone().file_source.with_schema(new_schema.clone()))
                                .with_projection(Some(new_projections.clone()))
                                .build();

                        let new_datasource = DataSourceExec::from_data_source(file_scan_config);

                        // 2. Create new FilterExec with updated input schema
                        let new_filter = FilterExec::try_new(
                            filter.predicate().clone(),
                            new_datasource.clone(),
                        )?;
                        // 3. Create ProjectionExec for sum operation
                        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];

                        // Get indices from filter's schema
                        let row_id_idx = new_schema.index_of("___row_id")?;
                        let row_base_idx = new_schema.index_of("row_base")?;

                        // Create sum expression
                        let row_id_col = Arc::new(Column::new("___row_id", row_id_idx));
                        let row_base_col = Arc::new(Column::new("row_base", row_base_idx));
                        let sum_expr = Arc::new(BinaryExpr::new(
                            row_id_col,
                            Operator::Plus,
                            row_base_col,
                        ));


                        // Add sum expression as ___row_id

                        // Add other columns (except row_base)
                        for field in schema.fields() {
                            if field.name() != "___row_id" && field.name() != "row_base" {
                                let idx = new_schema.index_of(field.name())?;
                                projection_exprs.push((
                                    Arc::new(Column::new(field.name(), idx)),
                                    field.name().to_string(),
                                ));
                            }
                        }

                        projection_exprs.push((sum_expr, "___row_id".to_string()));

                        // println!("projection_exprs :{:?}", projection_exprs);

                        // Create final ProjectionExec
                        let projection = ProjectionExec::try_new(
                            projection_exprs,
                            Arc::new(new_filter),
                        )?;

                        // println!("projection :{:?}", projection);
                        is_optimized = true;
                        return Ok(Transformed::new(Arc::new(projection), true, TreeNodeRecursion::Continue));
                    }
                }
            } else if let Some(datasource_exec) = node.as_any().downcast_ref::<DataSourceExec>() {
                if(!is_optimized) {
                    let schema = datasource_exec.schema();
                    let has_row_id = schema.field_with_name("___row_id").is_ok();

                    if has_row_id {
                        let mut datasource = datasource_exec.data_source().as_ref().as_any().downcast_ref::<FileScanConfig>().unwrap();
                        // let _ = datasource.projection.insert(vec![0]);
                        let mut new_projections = datasource.clone().projection.clone().unwrap();
                        println!("path {}", datasource.file_groups[0].files()[0].path());
                        let file_schema = ParquetRecordBatchReaderBuilder::try_new(fs::File::open("/".to_owned() + &datasource.file_groups[0].files()[0].path().to_string()).unwrap()).unwrap();

                        new_projections.push(file_schema.schema().fields().len());

                        let mut fields = schema.fields().clone().to_vec();
                        fields.insert(fields.len(), Arc::new(Field::new("row_base", DataType::Int32, true)));
                        let new_schema = Arc::new(Schema { metadata: schema.metadata().clone(), fields: Fields::from(fields) });

                        let file_scan_config =
                            FileScanConfigBuilder::from(datasource.clone())
                                .with_source(datasource.clone().file_source.with_schema(new_schema.clone()))
                                .with_projection(Some(new_projections.clone()))
                                .build();

                        let new_datasource = DataSourceExec::from_data_source(file_scan_config);

                        // 3. Create ProjectionExec for sum operation
                        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];

                        // Get indices from filter's schema
                        let row_id_idx = new_schema.index_of("___row_id")?;
                        let row_base_idx = new_schema.index_of("row_base")?;

                        // Create sum expression
                        let row_id_col = Arc::new(Column::new("___row_id", row_id_idx));
                        let row_base_col = Arc::new(Column::new("row_base", row_base_idx));
                        let sum_expr = Arc::new(BinaryExpr::new(
                            row_id_col,
                            Operator::Plus,
                            row_base_col,
                        ));


                        // Add sum expression as ___row_id

                        // Add other columns (except row_base)
                        for field in schema.fields() {
                            if field.name() != "___row_id" && field.name() != "row_base" {
                                let idx = new_schema.index_of(field.name())?;
                                projection_exprs.push((
                                    Arc::new(Column::new(field.name(), idx)),
                                    field.name().to_string(),
                                ));
                            }
                        }

                        projection_exprs.push((sum_expr, "___row_id".to_string()));

                        // println!("projection_exprs :{:?}", projection_exprs);

                        // Create final ProjectionExec
                        let projection = ProjectionExec::try_new(
                            projection_exprs,
                            new_datasource,
                        )?;

                        // println!("projection :{:?}", projection);
                        is_optimized = true;
                        return Ok(Transformed::new(Arc::new(projection), true, TreeNodeRecursion::Continue));
                    }
                }
            }
            Ok(Transformed::no(node))
        })?;

        Ok(rewritten.data)
    }
    //
    // fn optimize(
    //     &self,
    //     plan: Arc<dyn ExecutionPlan>,
    //     _config: &ConfigOptions,
    // ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    //     let rewritten = plan.transform_up(|node| {
    //         if let Some(filter) = node.as_any().downcast_ref::<FilterExec>() {
    //             // Check if input is DataSourceExec
    //             if let Some(datasource_exec) = filter.input().as_any().downcast_ref::<DataSourceExec>() {
    //                 // Check if ___row_id is present
    //                 let schema = datasource_exec.schema();
    //                 let has_row_id = schema.field_with_name("___row_id").is_ok();
    //
    //                 if has_row_id {
    //                     let mut datasource = datasource_exec.data_source().as_ref().as_any().downcast_ref::<FileScanConfig>().unwrap();
    //                     // let _ = datasource.projection.insert(vec![0]);
    //                     let mut new_projections = datasource.clone().projection.clone().unwrap();
    //                     println!("path {}", datasource.file_groups[0].files()[0].path());
    //                     // let file_schema = ParquetRecordBatchReaderBuilder::try_new(fs::File::open("/".to_owned()+&datasource.file_groups[0].files()[0].path().to_string()).unwrap()).unwrap();
    //                     let file_schema = datasource.file_schema.clone();
    //
    //                     new_projections.push(file_schema.index_of("row_base").unwrap() );
    //
    //                     let mut fields = schema.fields().clone().to_vec();
    //                     fields.insert(fields.len(), Arc::new(Field::new("row_base", DataType::Int32, true)));
    //                     let new_schema = Arc::new(Schema{metadata: schema.metadata().clone(), fields: Fields::from(fields)});
    //
    //                     let file_scan_config =
    //                         FileScanConfigBuilder::from(datasource.clone())
    //                             .with_source(datasource.clone().file_source.with_schema(new_schema.clone()))
    //                             .with_projection(Some(new_projections.clone()))
    //                             .build();
    //
    //                     let new_datasource = DataSourceExec::from_data_source(file_scan_config);
    //
    //                     // 2. Create new FilterExec with updated input schema
    //                     let new_filter = FilterExec::try_new(
    //                         filter.predicate().clone(),
    //                         new_datasource.clone(),
    //                     )?;
    //                     println!("new schema :{}", new_schema);
    //                     // 3. Create ProjectionExec for sum operation
    //                     let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
    //
    //                     // Get indices from filter's schema
    //                     let row_id_idx = new_schema.index_of("___row_id")?;
    //                     let row_base_idx = new_schema.index_of("row_base")?;
    //
    //                     // Create sum expression
    //                     let row_id_col = Arc::new(Column::new("___row_id", row_id_idx));
    //                     let row_base_col = Arc::new(Column::new("row_base", row_base_idx));
    //                     let sum_expr = Arc::new(BinaryExpr::new(
    //                         row_id_col,
    //                         Operator::Plus,
    //                         row_base_col,
    //                     ));
    //
    //
    //                     // Add sum expression as ___row_id
    //
    //                     // Add other columns (except row_base)
    //                     for field in schema.fields() {
    //                         if field.name() != "___row_id" && field.name() != "row_base" {
    //                             let idx = new_schema.index_of(field.name())?;
    //                             projection_exprs.push((
    //                                 Arc::new(Column::new(field.name(), idx)),
    //                                 field.name().to_string(),
    //                             ));
    //                         }
    //                     }
    //
    //                     projection_exprs.push((sum_expr, "___row_id".to_string()));
    //
    //                     println!("projection_exprs :{:?}", projection_exprs);
    //
    //                     // Create final ProjectionExec
    //                     let projection = ProjectionExec::try_new(
    //                         projection_exprs,
    //                         Arc::new(new_filter),
    //                     )?;
    //
    //                     println!("projection :{:?}", projection);
    //
    //                     return Ok(Transformed::new(Arc::new(projection), true, TreeNodeRecursion::Continue));
    //                 }
    //             }
    //         }
    //         Ok(Transformed::no(node))
    //     })?;
    //
    //     Ok(rewritten.data)
    // }

    fn name(&self) -> &str {
        "filter_row_id_optimizer"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
