/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::{Column, DFSchema};
use datafusion::common::tree_node::{TreeNode, Transformed};
use datafusion::config::ConfigOptions;
use datafusion::optimizer::{AnalyzerRule};
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, col
};
use datafusion::error::Result;
use datafusion_expr::{Projection, UserDefinedLogicalNode};
use crate::absolute_row_id_optimizer::ROW_ID_FIELD_NAME;

#[derive(Debug)]
pub struct ProjectRowIdAnalyzer;

impl ProjectRowIdAnalyzer {

    pub fn new() -> Self {
        Self {}
    }

    fn wrap_project_row_id(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(plan)
            .project(vec![col(ROW_ID_FIELD_NAME)])
            .map(|b| b.build())?
    }
}

impl AnalyzerRule for ProjectRowIdAnalyzer {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions
    ) -> Result<LogicalPlan>  {

        let rewritten = plan.transform_up(|node| {
            match &node {
                LogicalPlan::TableScan(scan) => {
                    let mut proj = scan.projection.clone().unwrap_or_else(|| {
                        (0..scan.projected_schema.fields().len()).collect()
                    });

                    let mut new_projected_schema = (*scan.projected_schema).clone();

                    // Append ___row_id field if not already present
                    if scan.source.schema().index_of(ROW_ID_FIELD_NAME).is_ok() {
                        let row_id_idx = scan.source.schema().index_of(ROW_ID_FIELD_NAME).unwrap();
                        if !proj.contains(&row_id_idx) {
                            proj.push(row_id_idx);
                            new_projected_schema = new_projected_schema
                                .join(&DFSchema::try_from_qualified_schema(
                                    scan.projected_schema.qualified_field(0).0.expect("Failed to get qualified name").clone(),
                                    &Schema::new(vec![
                                        Field::new(ROW_ID_FIELD_NAME, DataType::Int64, false),
                                    ]),
                                )?)
                                .expect("Failed to join schemas");
                        }
                    }

                    // Optionally, add row_base similarly
                    let new_scan = LogicalPlan::TableScan(datafusion_expr::TableScan {
                        table_name: scan.table_name.clone(),
                        source: scan.source.clone(),
                        projection: Some(proj),
                        projected_schema: Arc::new(new_projected_schema),
                        filters: scan.filters.clone(),
                        fetch: scan.fetch,
                    });
                    return Ok(Transformed::yes(new_scan));
                }

                LogicalPlan::Projection(p) => {
                    if !p.expr.iter().any(|e| matches!(e, Expr::Column(c) if c.name == ROW_ID_FIELD_NAME))
                        && p.input.schema().index_of_column(&Column::from_name("___row_id")).is_ok()
                    {
                        let mut new_exprs = p.expr.to_vec();
                        new_exprs.push(col(ROW_ID_FIELD_NAME));
                        // new_exprs.push(self.build_row_id_expr());
                        let mut new_fields = vec![];//p.schema.fields().to_vec();
                        new_fields.push(Field::new(ROW_ID_FIELD_NAME, DataType::Int64, false));
                        let new_schema = DFSchema::try_from_qualified_schema(
                            p.schema.qualified_field(0).0.expect("Failed to get qualified name").clone(),
                            &Schema::new(new_fields),
                        )?;

                        // if p.input.schema().index_of_column(&Column::from_name("___row_id")).is_ok()

                        let merged_schema  = if p.schema.index_of_column(&Column::from_name(ROW_ID_FIELD_NAME)).is_ok() { p.schema.clone() } else { Arc::new(p.schema.clone().join(&new_schema).expect("Failed to join schemas")) };
                        let new_proj = LogicalPlan::Projection(Projection::try_new_with_schema(new_exprs, p.input.clone(), merged_schema).expect("Failed to create projection"));
                        // println!("new_proj: {:?}", new_proj);

                        return Ok(Transformed::yes(new_proj));
                    }
                    Ok(Transformed::no(node))
                }

                _ => {Ok(Transformed::no(node))}
            }
        })?;

        // rewritten.data is the updated logical plan
        Ok(rewritten.data)
    }

    fn name(&self) -> &str {
        "project_row_id_logical_optimizer"
    }
}
