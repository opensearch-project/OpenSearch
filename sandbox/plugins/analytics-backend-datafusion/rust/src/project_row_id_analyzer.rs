/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Project Row ID Analyzer
//!
//! Logical-level analyzer rule that ensures `__row_id__` is always projected through
//! the query plan when available in the source schema. Runs BEFORE physical optimization,
//! so the column survives all pruning passes.
//!
//! For the ListingTable QTF path: `ShardTableProvider` exposes `row_base` as a partition
//! column. This analyzer ensures `__row_id__` (and implicitly `row_base`) survive into
//! the physical plan where `ProjectRowIdOptimizer` computes `__row_id__ + row_base`.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::{col, Expr, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use datafusion_expr::Projection;

pub use crate::ROW_ID_COLUMN_NAME as ROW_ID_FIELD_NAME;

#[derive(Debug)]
pub struct ProjectRowIdAnalyzer;

impl ProjectRowIdAnalyzer {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for ProjectRowIdAnalyzer {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let rewritten = plan.transform_up(|node| {
            match &node {
                LogicalPlan::TableScan(scan) => {
                    let mut proj = scan.projection.clone().unwrap_or_else(|| {
                        (0..scan.projected_schema.fields().len()).collect()
                    });

                    let mut new_projected_schema = (*scan.projected_schema).clone();

                    if scan.source.schema().index_of(ROW_ID_FIELD_NAME).is_ok() {
                        let row_id_idx =
                            scan.source.schema().index_of(ROW_ID_FIELD_NAME).unwrap();

                        if !proj.contains(&row_id_idx) {
                            proj.push(row_id_idx);

                            let qualifier = scan
                                .projected_schema
                                .qualified_field(0)
                                .0
                                .cloned();

                            if let Some(q) = qualifier {
                                let row_id_schema = DFSchema::try_from_qualified_schema(
                                    q,
                                    &Schema::new(vec![Field::new(
                                        ROW_ID_FIELD_NAME,
                                        DataType::Int64,
                                        false,
                                    )]),
                                )?;
                                new_projected_schema = new_projected_schema
                                    .join(&row_id_schema)
                                    .map_err(|e| {
                                        datafusion::error::DataFusionError::Internal(format!(
                                            "ProjectRowIdAnalyzer: join schema: {}",
                                            e
                                        ))
                                    })?;
                            }
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
                    Ok(Transformed::yes(new_scan))
                }

                LogicalPlan::Projection(p) => {
                    let already_has_row_id = p.expr.iter().any(|e| {
                        matches!(e, Expr::Column(c) if c.name == ROW_ID_FIELD_NAME)
                    });
                    let input_has_row_id = p
                        .input
                        .schema()
                        .index_of_column(&Column::from_name(ROW_ID_FIELD_NAME))
                        .is_ok();

                    if !already_has_row_id && input_has_row_id {
                        let mut new_exprs = p.expr.to_vec();
                        new_exprs.push(col(ROW_ID_FIELD_NAME));

                        let row_id_schema = {
                            let qualifier = p.schema.qualified_field(0).0.cloned();
                            match qualifier {
                                Some(q) => DFSchema::try_from_qualified_schema(
                                    q,
                                    &Schema::new(vec![Field::new(
                                        ROW_ID_FIELD_NAME,
                                        DataType::Int64,
                                        false,
                                    )]),
                                )?,
                                None => DFSchema::try_from(Schema::new(vec![Field::new(
                                    ROW_ID_FIELD_NAME,
                                    DataType::Int64,
                                    false,
                                )]))?,
                            }
                        };

                        let merged_schema = if p
                            .schema
                            .index_of_column(&Column::from_name(ROW_ID_FIELD_NAME))
                            .is_ok()
                        {
                            p.schema.clone()
                        } else {
                            Arc::new(p.schema.as_ref().clone().join(&row_id_schema).map_err(
                                |e| {
                                    datafusion::error::DataFusionError::Internal(format!(
                                        "ProjectRowIdAnalyzer: join projection schema: {}",
                                        e
                                    ))
                                },
                            )?)
                        };

                        let new_proj = LogicalPlan::Projection(
                            Projection::try_new_with_schema(
                                new_exprs,
                                p.input.clone(),
                                merged_schema,
                            )
                            .map_err(|e| {
                                datafusion::error::DataFusionError::Internal(format!(
                                    "ProjectRowIdAnalyzer: create projection: {}",
                                    e
                                ))
                            })?,
                        );
                        return Ok(Transformed::yes(new_proj));
                    }
                    Ok(Transformed::no(node))
                }

                _ => Ok(Transformed::no(node)),
            }
        })?;

        Ok(rewritten.data)
    }

    fn name(&self) -> &str {
        "project_row_id_analyzer"
    }
}
