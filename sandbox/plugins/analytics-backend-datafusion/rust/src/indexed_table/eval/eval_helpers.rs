/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared helpers for evaluators (SingleCollector, PredicateOnly, Tree).

use std::sync::Arc;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNode;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use roaring::RoaringBitmap;

use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner};
use crate::indexed_table::stream::RowGroupInfo;

/// Compute page-pruned ranges for a row group.
/// Returns `None` if no pruning predicate is available (all rows pass).
/// Returns `Some(vec![])` if all pages are pruned (RG can be skipped).
pub fn compute_page_ranges(
    pruning_predicate: Option<&Arc<PruningPredicate>>,
    page_pruner: &PagePruner,
    rg: &RowGroupInfo,
    min_doc: i32,
    page_prune_metrics: Option<&PagePruneMetrics>,
) -> Option<Vec<(i32, i32)>> {
    pruning_predicate.and_then(|pp| {
        page_pruner
            .prune_rg(pp, rg.index, page_prune_metrics)
            .map(|sel| {
                let mut ranges = Vec::new();
                let mut rg_pos: i64 = 0;
                for s in sel.iter() {
                    if s.skip {
                        rg_pos += s.row_count as i64;
                    } else {
                        let abs_min = min_doc + rg_pos as i32;
                        let abs_max = min_doc + rg_pos as i32 + s.row_count as i32;
                        ranges.push((abs_min, abs_max));
                        rg_pos += s.row_count as i64;
                    }
                }
                ranges
            })
    })
}

/// Build a candidate bitmap from page-pruned ranges (universe — all surviving pages).
/// Returns `None` if all pages were pruned.
pub fn universe_bitmap_from_page_ranges(
    page_ranges: &Option<Vec<(i32, i32)>>,
    rg: &RowGroupInfo,
) -> Option<RoaringBitmap> {
    match page_ranges {
        Some(r) if r.is_empty() => None,
        Some(r) => {
            let mut bm = RoaringBitmap::new();
            for (r_min, r_max) in r {
                let lo = (*r_min as i64 - rg.first_row) as u32;
                let hi = (*r_max as i64 - rg.first_row) as u32;
                bm.insert_range(lo..hi);
            }
            Some(bm)
        }
        None => {
            let mut bm = RoaringBitmap::new();
            bm.insert_range(0..rg.num_rows as u32);
            Some(bm)
        }
    }
}

/// Evaluate a residual predicate against a batch, returning a BooleanArray mask.
pub fn evaluate_residual(
    residual: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
    batch_len: usize,
) -> Result<BooleanArray, String> {
    let remapped = remap_expr_to_batch(residual, batch)?;
    let value = remapped
        .evaluate(batch)
        .map_err(|e| format!("evaluate_residual: {}", e))?;
    let array = value
        .into_array(batch_len)
        .map_err(|e| format!("evaluate_residual into_array: {}", e))?;
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "evaluate_residual: did not produce BooleanArray".to_string())
        .cloned()
}

/// Remap column references in a PhysicalExpr to match the batch schema.
/// The expression may reference columns by index in the full table schema,
/// but the batch only contains projected columns. This rewrites Column
/// expressions to use the batch's field positions by name lookup.
pub fn remap_expr_to_batch(
    expr: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<Arc<dyn PhysicalExpr>, String> {
    let batch_schema = batch.schema();
    expr.clone()
        .transform(|node| {
            use datafusion::common::tree_node::Transformed;
            if let Some(col) = node.as_any().downcast_ref::<Column>() {
                if let Ok(idx) = batch_schema.index_of(col.name()) {
                    let new_col: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new(col.name(), idx));
                    return Ok(Transformed::yes(new_col));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .map_err(|e| format!("remap_expr_to_batch: {}", e))
}
