/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Row ID set evaluator — positional parquet read for the QTF fetch phase.
//!
//! Given a pre-computed set of segment-relative positions, returns only those
//! rows from parquet. Skips entire row groups that have no requested positions.
//! No Lucene collector, no predicate evaluation — pure positional access.

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::record_batch::RecordBatch;
use roaring::RoaringBitmap;

use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::row_selection::{bitmap_to_packed_bits, PositionMap};
use crate::indexed_table::stream::RowGroupInfo;

/// Evaluator that returns only pre-specified positions within a segment.
///
/// Used by the QTF fetch phase: the coordinator tells the data node "give me
/// rows at positions [5, 12, 30] in this segment" — this evaluator produces
/// a bitmap with exactly those positions set.
pub struct RowIdSetEvaluator {
    /// Segment-relative positions to include (0-based within the segment).
    requested_positions: RoaringBitmap,
}

impl RowIdSetEvaluator {
    pub fn new(positions: RoaringBitmap) -> Self {
        Self {
            requested_positions: positions,
        }
    }
}

impl RowGroupBitsetSource for RowIdSetEvaluator {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        _min_doc: i32,
        _max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let rg_start = rg.first_row as u32;
        let rg_end = rg_start + rg.num_rows as u32;

        // Intersect requested positions with this RG's range, shift to RG-relative
        let mut rg_bitmap = RoaringBitmap::new();
        for pos in self.requested_positions.range(rg_start..rg_end) {
            rg_bitmap.insert(pos - rg_start);
        }

        if rg_bitmap.is_empty() {
            return Ok(None); // Skip this RG entirely
        }

        let mask_len = rg.num_rows as usize;
        let packed = bitmap_to_packed_bits(&rg_bitmap, mask_len as u32);
        let mask_buffer = Buffer::from_vec(packed);

        Ok(Some(PrefetchedRg {
            candidates: rg_bitmap,
            eval_nanos: 0,
            context: Box::new(()),
            mask_buffer: Some(mask_buffer),
        }))
    }

    fn on_batch_mask(
        &self,
        _rg_state: &dyn std::any::Any,
        _rg_first_row: i64,
        _position_map: &PositionMap,
        _batch_offset: usize,
        _batch_len: usize,
        _batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        // No refinement needed — the RowSelection from prefetch_rg is exact.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_positions_skips_all_rgs() {
        let eval = RowIdSetEvaluator::new(RoaringBitmap::new());
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 100,
        };
        let result = eval.prefetch_rg(&rg, 0, 100).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_positions_outside_rg_skips() {
        let mut positions = RoaringBitmap::new();
        positions.insert(200); // outside RG [0, 100)
        positions.insert(300);

        let eval = RowIdSetEvaluator::new(positions);
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 100,
        };
        let result = eval.prefetch_rg(&rg, 0, 100).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_positions_within_rg() {
        let mut positions = RoaringBitmap::new();
        positions.insert(5);
        positions.insert(12);
        positions.insert(30);

        let eval = RowIdSetEvaluator::new(positions);
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 50,
        };
        let result = eval.prefetch_rg(&rg, 0, 50).unwrap();
        assert!(result.is_some());
        let prefetched = result.unwrap();
        assert_eq!(prefetched.candidates.len(), 3);
        assert!(prefetched.candidates.contains(5));
        assert!(prefetched.candidates.contains(12));
        assert!(prefetched.candidates.contains(30));
    }

    #[test]
    fn test_positions_spanning_multiple_rgs() {
        let mut positions = RoaringBitmap::new();
        positions.insert(5);   // in RG 0 [0, 50)
        positions.insert(60);  // in RG 1 [50, 100)
        positions.insert(99);  // in RG 1 [50, 100)

        let eval = RowIdSetEvaluator::new(positions);

        // RG 0
        let rg0 = RowGroupInfo { index: 0, first_row: 0, num_rows: 50 };
        let result0 = eval.prefetch_rg(&rg0, 0, 50).unwrap().unwrap();
        assert_eq!(result0.candidates.len(), 1);
        assert!(result0.candidates.contains(5)); // RG-relative

        // RG 1
        let rg1 = RowGroupInfo { index: 1, first_row: 50, num_rows: 50 };
        let result1 = eval.prefetch_rg(&rg1, 50, 100).unwrap().unwrap();
        assert_eq!(result1.candidates.len(), 2);
        assert!(result1.candidates.contains(10)); // 60 - 50 = 10 (RG-relative)
        assert!(result1.candidates.contains(49)); // 99 - 50 = 49 (RG-relative)
    }

    #[test]
    fn test_on_batch_mask_returns_none() {
        let eval = RowIdSetEvaluator::new(RoaringBitmap::new());
        let schema = datafusion::arrow::datatypes::Schema::new(vec![]);
        let batch = RecordBatch::new_empty(std::sync::Arc::new(schema));
        let pm = PositionMap::from_selection(
            &datafusion::parquet::arrow::arrow_reader::RowSelection::from(Vec::<datafusion::parquet::arrow::arrow_reader::RowSelector>::new()),
        );
        let result = eval.on_batch_mask(&(), 0, &pm, 0, 0, &batch).unwrap();
        assert!(result.is_none());
    }
}
