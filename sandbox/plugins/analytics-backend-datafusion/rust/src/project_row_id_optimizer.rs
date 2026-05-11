/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Physical optimizer rule for Approach 1 (ListingTable + ProjectRowIdOptimizer).
//!
//! Walks the physical plan tree looking for `DataSourceExec` nodes whose file
//! schema contains `___row_id`. When found, inserts a `ProjectionExec` above
//! that computes `___row_id + row_base` and aliases it as `___row_id`.
//!
//! This optimizer is registered on the session only when `RowIdStrategy::ListingTable`
//! is active and the plan requests row IDs.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

/// Physical optimizer that rewrites `DataSourceExec` plans to compute
/// `___row_id + row_base` when the file schema contains `___row_id`.
///
/// This is a no-op when `___row_id` is not present in the schema.
#[derive(Debug)]
pub struct ProjectRowIdOptimizer;

impl PhysicalOptimizerRule for ProjectRowIdOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Walk the plan tree bottom-up, looking for DataSourceExec nodes
        // with ___row_id in their output schema.
        plan.transform_up(|node| {
            let schema = node.schema();
            // Check if this node's output schema has both ___row_id and row_base
            let has_row_id = schema.column_with_name("___row_id").is_some();
            let has_row_base = schema.column_with_name("row_base").is_some();

            if has_row_id && has_row_base {
                // Insert a ProjectionExec that computes ___row_id + row_base
                // and outputs it as ___row_id, dropping the raw row_base column.
                let row_id_idx = schema.index_of("___row_id").unwrap();
                let row_base_idx = schema.index_of("row_base").unwrap();

                // Build projection expressions: all columns except row_base,
                // with ___row_id replaced by (___row_id + row_base).
                let mut exprs: Vec<(
                    Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                    String,
                )> = Vec::new();

                for (i, field) in schema.fields().iter().enumerate() {
                    if i == row_base_idx {
                        // Skip row_base from output
                        continue;
                    }
                    if i == row_id_idx {
                        // Replace ___row_id with ___row_id + row_base
                        let row_id_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                            Arc::new(datafusion::physical_expr::expressions::Column::new(
                                "___row_id",
                                row_id_idx,
                            ));
                        let row_base_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                            Arc::new(datafusion::physical_expr::expressions::Column::new(
                                "row_base",
                                row_base_idx,
                            ));
                        let add_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                            Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                                row_id_col,
                                datafusion::logical_expr::Operator::Plus,
                                row_base_col,
                            ));
                        exprs.push((add_expr, "___row_id".to_string()));
                    } else {
                        let col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                            Arc::new(datafusion::physical_expr::expressions::Column::new(
                                field.name(),
                                i,
                            ));
                        exprs.push((col, field.name().clone()));
                    }
                }

                let projection = datafusion::physical_plan::projection::ProjectionExec::try_new(
                    exprs, node,
                )?;
                Ok(Transformed::yes(Arc::new(projection) as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ProjectRowIdOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn optimizer_name() {
        let opt = ProjectRowIdOptimizer;
        assert_eq!(opt.name(), "ProjectRowIdOptimizer");
    }

    #[test]
    fn optimizer_schema_check() {
        let opt = ProjectRowIdOptimizer;
        assert!(opt.schema_check());
    }
}
