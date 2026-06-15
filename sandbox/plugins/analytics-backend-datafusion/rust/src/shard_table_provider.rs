/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shard-level TableProvider with `row_base` partition column for global row ID computation.

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
    fn schema(&self) -> SchemaRef { self.table_schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Inexact: we use the predicate for page-index pruning (and optional
        // decode-time pushdown), but DataFusion keeps the FilterExec to apply
        // the predicate exactly. This is the same contract DataFusion's own
        // ListingTable uses for non-partition filters.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Invariant: files are ordered by ascending row_base (build_shard_files contract).
        // ProjectRowIdOptimizer relies on the partition value matching the file's true offset.
        debug_assert!(
            self.config.files.windows(2).all(|w| w[0].row_base <= w[1].row_base),
            "ShardTableProvider: files not ordered by row_base — ProjectRowIdOptimizer would compute wrong global IDs"
        );
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
                if let Some(ref plan) = file_info.access_plan {
                    pf = pf.with_extensions(Arc::new(plan.clone()));
                }
                pf
            })
            .collect();

        let file_groups = vec![FileGroup::new(partitioned_files)];

        let table_schema = TableSchema::new(
            self.config.file_schema.clone(),
            vec![Arc::new(Field::new("row_base", DataType::Int64, true))],
        );

        let mut parquet_source = ParquetSource::new(table_schema);

        // Push a physical predicate built from the pushed-down filters so the
        // ParquetOpener can do page-index pruning, and so `ScopedPageIndexOptimizer`
        // (applied after planning) can read it to scope the page-index reader
        // factory. Without a predicate the opener never builds a page-pruning
        // predicate and never loads a page index. We convert against the *file*
        // schema (predicates reference file columns, not the appended row_base
        // partition column); any filter referencing row_base or failing physical
        // conversion is dropped from the pushed predicate (the FilterExec still
        // applies the full predicate exactly, so results stay correct).
        if !filters.is_empty() {
            if let Ok(df_schema) = datafusion::common::DFSchema::try_from(
                self.config.file_schema.as_ref().clone(),
            ) {
                let file_field_names: std::collections::HashSet<&str> = self
                    .config
                    .file_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect();
                let mut physical_preds: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
                    Vec::new();
                for filter in filters {
                    let refs_only_file_cols = filter
                        .column_refs()
                        .iter()
                        .all(|c| file_field_names.contains(c.name.as_str()));
                    if !refs_only_file_cols {
                        continue;
                    }
                    if let Ok(phys) = state.create_physical_expr(filter.clone(), &df_schema) {
                        physical_preds.push(phys);
                    }
                }
                if let Some(combined) =
                    datafusion::physical_expr::utils::conjunction_opt(physical_preds)
                {
                    parquet_source = parquet_source.with_predicate(combined);
                }
            }
        }

        // The scoped page-index reader factory is installed by
        // `ScopedPageIndexOptimizer` (a physical optimizer rule applied after
        // planning, for all strategies), which reads the predicate we pushed
        // above. Installing it uniformly in the rule — rather than here — means
        // the stock-`ListingTable` path (`QueryStrategy::None`) gets the same
        // treatment: there the rule REPLACES DataFusion's own
        // `CachedParquetFileReaderFactory`; here it installs onto a scan that has
        // none yet. One mechanism, both paths.

        let mut builder = FileScanConfigBuilder::new(
            self.config.store_url.clone(),
            Arc::new(parquet_source),
        )
        .with_file_groups(file_groups);

        // Always include the row_base partition column (index = num_file_cols)
        // so ProjectRowIdOptimizer can compute __row_id__ + row_base.
        let row_base_idx = num_file_cols;
        let proj_with_row_base = match projection {
            Some(proj) => {
                let mut p = proj.clone();
                if !p.contains(&row_base_idx) {
                    p.push(row_base_idx);
                }
                p
            }
            None => {
                let mut p: Vec<usize> = (0..num_file_cols).collect();
                p.push(row_base_idx);
                p
            }
        };
        builder = builder
            .with_projection_indices(Some(proj_with_row_base))
            .map_err(|e| datafusion::error::DataFusionError::Internal(format!("{}", e)))?;

        let file_scan_config = builder.build();
        Ok(DataSourceExec::from_data_source(file_scan_config))
    }

    fn statistics(&self) -> Option<Statistics> { None }
}
