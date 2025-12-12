/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Project Row ID Analyzer
//! 
//! This module implements a DataFusion analyzer rule that ensures row ID fields are properly
//! projected through the query plan. The analyzer automatically adds the special `___row_id`
//! field to table scans and projections when it's available in the source schema but not
//! explicitly projected by the user.
//!
//! This is crucial for maintaining row identity throughout query execution, especially for
//! operations that need to track individual rows (like updates, deletes, or result ordering).

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

/// The ProjectRowIdAnalyzer is a DataFusion analyzer rule that ensures row ID fields
/// are properly included in query plans. It automatically adds the `___row_id` field
/// to table scans and projections when available in the source but not explicitly requested.
#[derive(Debug)]
pub struct ProjectRowIdAnalyzer;

impl ProjectRowIdAnalyzer {
    /// Creates a new instance of the ProjectRowIdAnalyzer
    pub fn new() -> Self {
        Self {}
    }

    /// Wraps a logical plan with a projection that only selects the row ID field.
    /// This is a utility method that can be used to create a plan that projects
    /// only the row ID column from the input plan.
    ///
    /// # Arguments
    /// * `plan` - The input logical plan to wrap
    ///
    /// # Returns
    /// A new logical plan that projects only the row ID field
    fn wrap_project_row_id(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(plan)
            .project(vec![col(ROW_ID_FIELD_NAME)])
            .map(|b| b.build())?
    }
}

impl AnalyzerRule for ProjectRowIdAnalyzer {
    /// Analyzes and transforms the logical plan to ensure row ID fields are properly projected.
    /// This method walks through the plan tree and modifies TableScan and Projection nodes
    /// to include the row ID field when it's available in the source schema.
    ///
    /// # Arguments
    /// * `plan` - The input logical plan to analyze
    /// * `_config` - DataFusion configuration options (unused in this implementation)
    ///
    /// # Returns
    /// A transformed logical plan with row ID fields properly projected
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions
    ) -> Result<LogicalPlan>  {
        // Transform the plan tree bottom-up, ensuring child nodes are processed before parents
        let rewritten = plan.transform_up(|node| {
            match &node {
                // Handle TableScan nodes - these are the leaf nodes that read from data sources
                LogicalPlan::TableScan(scan) => {
                    // Get the current projection indices, or create a default projection
                    // that includes all fields if no explicit projection exists
                    let mut proj = scan.projection.clone().unwrap_or_else(|| {
                        (0..scan.projected_schema.fields().len()).collect()
                    });

                    // Start with the current projected schema
                    let mut new_projected_schema = (*scan.projected_schema).clone();

                    // Check if the source table has a row ID field available
                    // If it does and it's not already in the projection, add it
                    if scan.source.schema().index_of(ROW_ID_FIELD_NAME).is_ok() {
                        let row_id_idx = scan.source.schema().index_of(ROW_ID_FIELD_NAME).unwrap();
                        
                        // Only add the row ID field if it's not already projected
                        if !proj.contains(&row_id_idx) {
                            // Add the row ID field index to the projection
                            proj.push(row_id_idx);
                            
                            // Update the schema to include the row ID field
                            // We need to join the current schema with a new schema containing the row ID field
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
                        && p.input.schema().index_of_column(&Column::from_name(ROW_ID_FIELD_NAME)).is_ok()
                    {
                        let mut new_exprs = p.expr.to_vec();
                        new_exprs.push(col(ROW_ID_FIELD_NAME));

                        let mut new_fields = vec![];
                        new_fields.push(Field::new(ROW_ID_FIELD_NAME, DataType::Int64, false));
                        let new_schema = DFSchema::try_from_qualified_schema(
                            // schema gives multiple qualified_fields, qualified_field(0) gives tuple (Option<TableReference>, Field).
                            // qualified_field(0).0 will return first object of tuple ie. Option<TableReference>
                            p.schema.qualified_field(0).0.expect("Failed to get qualified name").clone(), // TODO: Handle TableReference for multiple tables.
                            &Schema::new(new_fields),
                        )?;

                        let merged_schema  = if p.schema.index_of_column(&Column::from_name(ROW_ID_FIELD_NAME)).is_ok() { p.schema.clone() } else { Arc::new(p.schema.clone().join(&new_schema).expect("Failed to join schemas")) };
                        let new_proj = LogicalPlan::Projection(Projection::try_new_with_schema(new_exprs, p.input.clone(), merged_schema).expect("Failed to create projection"));
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
