/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shard-level TableProvider with `row_base` partition column for global row ID computation.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, ScalarValue, Statistics};
use datafusion::common::stats::Precision;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::table_schema::TableSchema;
use datafusion_datasource::PartitionedFile;

pub use crate::api::ShardFileInfo;

pub struct ShardTableConfig {
    pub file_schema: SchemaRef,
    pub files: Vec<ShardFileInfo>,
    pub store_url: ObjectStoreUrl,
}

pub struct ShardTableProvider {
    table_schema: SchemaRef,
    config: ShardTableConfig,
}

impl ShardTableProvider {
    pub fn new(config: ShardTableConfig) -> Self {
        let mut fields: Vec<Arc<Field>> = config.file_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("row_base", DataType::Int64, true)));
        let table_schema = Arc::new(Schema::new(fields));
        Self { table_schema, config }
    }
}

impl std::fmt::Debug for ShardTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardTableProvider")
            .field("files", &self.config.files.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for ShardTableProvider {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.table_schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let num_file_cols = self.config.file_schema.fields().len();
        let partitioned_files: Vec<PartitionedFile> = self.config.files.iter()
            .map(|file_info| {
                let mut pf = PartitionedFile::from(file_info.object_meta.clone());
                pf.partition_values = vec![ScalarValue::Int64(Some(file_info.row_base))];
                let file_stats = Arc::new(Statistics {
                    num_rows: Precision::Exact(file_info.num_rows as usize),
                    total_byte_size: Precision::Inexact(file_info.object_meta.size as usize),
                    column_statistics: vec![
                        datafusion::common::ColumnStatistics::new_unknown();
                        num_file_cols
                    ],
                });
                pf = pf.with_statistics(file_stats);
                pf
            })
            .collect();

        let file_groups = vec![FileGroup::new(partitioned_files)];

        let table_schema = TableSchema::new(
            self.config.file_schema.clone(),
            vec![Arc::new(Field::new("row_base", DataType::Int64, true))],
        );

        let parquet_source = ParquetSource::new(table_schema);

        let mut builder = FileScanConfigBuilder::new(
            self.config.store_url.clone(),
            Arc::new(parquet_source),
        )
        .with_file_groups(file_groups);

        if let Some(proj) = projection {
            // Always include the row_base partition column (index = num_file_cols)
            // so ProjectRowIdOptimizer can compute __row_id__ + row_base.
            let row_base_idx = num_file_cols;
            let mut proj_with_row_base = proj.clone();
            if !proj_with_row_base.contains(&row_base_idx) {
                proj_with_row_base.push(row_base_idx);
            }
            builder = builder
                .with_projection_indices(Some(proj_with_row_base))
                .map_err(|e| datafusion::error::DataFusionError::Internal(format!("{}", e)))?;
        }

        let file_scan_config = builder.build();
        Ok(DataSourceExec::from_data_source(file_scan_config))
    }

    fn statistics(&self) -> Option<Statistics> { None }
}
