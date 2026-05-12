/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Physical optimizer rule for the ListingTable QTF path.
//!
//! Walks the physical plan tree looking for `DataSourceExec` nodes that have
//! both `__row_id__` and `row_base` in their output schema. When found, wraps
//! with a `ProjectionExec` that computes `__row_id__ + row_base` and drops `row_base`.
//!
//! `schema_check() -> false` is critical — tells DataFusion to skip schema
//! validation after this rule runs (output schema changes from input).

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::Result;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

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
            let schema = node.schema();
            let has_row_id = schema.column_with_name(ROW_ID_FIELD_NAME).is_some();
            let has_row_base = schema.column_with_name(ROW_BASE_FIELD_NAME).is_some();

            if !has_row_id || !has_row_base {
                return Ok(Transformed::no(node));
            }

            let row_id_idx = schema.index_of(ROW_ID_FIELD_NAME).unwrap();
            let row_base_idx = schema.index_of(ROW_BASE_FIELD_NAME).unwrap();

            let sum_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::new(Column::new(ROW_ID_FIELD_NAME, row_id_idx)),
                Operator::Plus,
                Arc::new(Column::new(ROW_BASE_FIELD_NAME, row_base_idx)),
            ));

            let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
            for (i, field) in schema.fields().iter().enumerate() {
                if field.name() == ROW_ID_FIELD_NAME {
                    projection_exprs.push((sum_expr.clone(), ROW_ID_FIELD_NAME.to_string()));
                } else if field.name() == ROW_BASE_FIELD_NAME {
                    continue;
                } else {
                    projection_exprs.push((
                        Arc::new(Column::new(field.name(), i)),
                        field.name().clone(),
                    ));
                }
            }

            let projection = ProjectionExec::try_new(projection_exprs, node)?;
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
