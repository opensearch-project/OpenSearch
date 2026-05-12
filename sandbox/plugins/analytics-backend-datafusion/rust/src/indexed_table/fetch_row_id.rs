/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Row ID computation for the fetch phase (QTF).
//!
//! Computes shard-global row IDs from position information and injects them
//! into the output batch at the correct column index. Used by `IndexedStream`
//! when `row_id_output_index` is set.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;

use super::row_selection::PositionMap;

/// Pre-captured state needed for row ID computation.
/// Captured BEFORE the filter mask is consumed (since mask consumption advances offsets).
pub struct RowIdContext {
    pub batch_offset: usize,
    pub position_map: Option<PositionMap>,
    pub base: u64,
    pub eval_mask: Option<BooleanArray>,
}

/// Compute global row IDs for surviving rows and inject into the output batch.
///
/// # Arguments
/// * `output` - The filtered batch (without `__row_id__` column)
/// * `ctx` - Pre-captured position info from before filtering
/// * `batch_len` - Original (pre-filter) batch length
/// * `current_mask` - Candidate-stage mask (used when eval_mask is None)
/// * `mask_offset_before` - mask_offset value before this batch was processed
/// * `row_id_idx` - Column index in the output schema for `__row_id__`
/// * `schema` - Output schema (includes `__row_id__` at `row_id_idx`)
pub fn inject_row_ids(
    output: &RecordBatch,
    ctx: &RowIdContext,
    batch_len: usize,
    current_mask: Option<&BooleanArray>,
    mask_offset_before: usize,
    row_id_idx: usize,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let num_surviving = output.num_rows();

    let row_ids: Vec<u64> = if num_surviving == 0 {
        vec![]
    } else {
        compute_row_ids(
            &ctx.eval_mask,
            current_mask,
            mask_offset_before,
            batch_len,
            ctx.batch_offset,
            ctx.position_map.as_ref(),
            ctx.base,
        )
    };

    let row_id_array: Arc<dyn Array> = Arc::new(UInt64Array::from(row_ids));

    // Build output columns: data columns from filtered batch + row_id at correct position.
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    let batch_schema = output.schema();
    let mut data_col = 0usize;
    for (i, field) in schema.fields().iter().enumerate() {
        if i == row_id_idx {
            columns.push(Arc::clone(&row_id_array));
        } else if let Ok(idx) = batch_schema.index_of(field.name()) {
            columns.push(Arc::clone(output.column(idx)));
            data_col += 1;
        } else {
            columns.push(Arc::clone(output.column(data_col.min(output.num_columns() - 1))));
            data_col += 1;
        }
    }

    if num_surviving == 0 {
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &datafusion::arrow::record_batch::RecordBatchOptions::new()
                .with_row_count(Some(0)),
        )
        .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))
    } else {
        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Compute global row IDs from position info.
fn compute_row_ids(
    eval_mask: &Option<BooleanArray>,
    current_mask: Option<&BooleanArray>,
    mask_offset_before: usize,
    batch_len: usize,
    batch_start_delivered: usize,
    pm: Option<&PositionMap>,
    base: u64,
) -> Vec<u64> {
    match eval_mask {
        Some(mask) => {
            (0..batch_len)
                .filter(|&i| mask.is_valid(i) && mask.value(i))
                .map(|i| position_to_global_id(batch_start_delivered + i, pm, base))
                .collect()
        }
        None => match current_mask {
            Some(candidate_mask) => {
                (0..batch_len)
                    .filter(|&i| {
                        let mi = mask_offset_before + i;
                        mi < candidate_mask.len()
                            && candidate_mask.is_valid(mi)
                            && candidate_mask.value(mi)
                    })
                    .map(|i| position_to_global_id(batch_start_delivered + i, pm, base))
                    .collect()
            }
            None => {
                (0..batch_len)
                    .map(|i| position_to_global_id(batch_start_delivered + i, pm, base))
                    .collect()
            }
        },
    }
}

/// Convert a delivered-row index to a shard-global row ID.
#[inline]
fn position_to_global_id(delivered_idx: usize, pm: Option<&PositionMap>, base: u64) -> u64 {
    let rg_pos = match pm {
        Some(p) => p.rg_position(delivered_idx).unwrap_or(delivered_idx),
        None => delivered_idx,
    };
    base + rg_pos as u64
}
