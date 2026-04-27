/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Single-collector evaluator — one backend collector plus DataFusion for
//! residual predicates.
//!
//! When the filter has exactly one `index_filter(...)` call AND'd with
//! (possibly zero, one, or many) parquet-native predicates, this evaluator
//! runs. Per RG:
//!
//! 1. Call the single collector → bitset.
//! 2. Apply page pruning (AND/OR mode depending on how the query combined them).
//! 3. Hand the bitset offsets to `IndexedStream` as a RowSelection.
//! 4. `on_batch_mask` returns `None` — DataFusion's
//!    `with_predicate(residual).with_pushdown_filters(true)` applies the
//!    residual predicates during decode, so indices stay aligned and no
//!    post-filtering is needed.

use std::sync::Arc;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use roaring::RoaringBitmap;

use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::row_selection::row_selection_to_bitmap;
use crate::indexed_table::stream::RowGroupInfo;
use datafusion::physical_optimizer::pruning::PruningPredicate;

/// Evaluator holding one collector and applying per-RG page pruning.
///
/// Always AND-intersects the collector bitmap with page pruning. The
/// `BitsetMode::Or` branch that previously existed was never emitted by
/// the classifier (reserved for a future `OR(Collector, predicates)`
/// extension) and has been removed; an OR-between-Collector-and-predicates
/// shape routes to the multi-filter tree path today.
pub struct SingleCollectorEvaluator {
    collector: Arc<dyn RowGroupDocsCollector>,
    page_pruner: Arc<PagePruner>,
    /// Residual pruning predicate: the non-Collector portion of the
    /// top-level AND, translated to a `PruningPredicate`. `None` means
    /// no residual predicate applies (nothing to prune with).
    pruning_predicate: Option<Arc<PruningPredicate>>,
}

impl SingleCollectorEvaluator {
    pub fn new(
        collector: Arc<dyn RowGroupDocsCollector>,
        page_pruner: Arc<PagePruner>,
        pruning_predicate: Option<Arc<PruningPredicate>>,
    ) -> Self {
        Self {
            collector,
            page_pruner,
            pruning_predicate,
        }
    }
}

impl RowGroupBitsetSource for SingleCollectorEvaluator {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let t = std::time::Instant::now();

        // Collect bitset from backend → RG-relative RoaringBitmap.
        let bitset = self.collector.collect_packed_u64_bitset(min_doc, max_doc)?;

        let mut candidates = RoaringBitmap::new();
        for (word_idx, &word) in bitset.iter().enumerate() {
            if word == 0 {
                continue;
            }
            let base = min_doc as i64 + (word_idx as i64 * 64);
            let mut w = word;
            while w != 0 {
                let bit = w.trailing_zeros() as i64;
                let doc_id = base + bit;
                if doc_id < max_doc as i64 {
                    let rel = doc_id - rg.first_row;
                    if rel >= 0 && rel <= u32::MAX as i64 {
                        candidates.insert(rel as u32);
                    }
                }
                w &= w - 1; // clear lowest set bit
            }
        }

        // Apply page-level pruning if we have a residual predicate.
        if let Some(ref pp) = self.pruning_predicate {
            if let Some(sel) = self.page_pruner.prune_rg(pp, rg.index) {
                let allowed = row_selection_to_bitmap(&sel);
                candidates &= allowed;
            }
            // `None` → no pruning available for this RG → keep candidates
            // unchanged (conservative).
        }

        if candidates.is_empty() {
            return Ok(None);
        }

        Ok(Some(PrefetchedRg::without_context(
            candidates,
            t.elapsed().as_nanos() as u64,
        )))
    }

    fn on_batch_mask(
        &self,
        _rg_state: &dyn std::any::Any,
        _rg_first_row: i64,
        _position_map: &crate::indexed_table::row_selection::PositionMap,
        _batch_offset: usize,
        _batch_len: usize,
        _batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        // Single-collector path relies on DataFusion predicate pushdown; no
        // refinement mask needed.
        Ok(None)
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
    use datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata;
    use std::fmt;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    /// Stub collector: returns a pre-defined set of doc IDs, encoded into
    /// the bitset the trait contract requires.
    #[derive(Debug)]
    struct StubCollector {
        docs: Vec<i32>,
    }

    impl RowGroupDocsCollector for StubCollector {
        fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
            let span = (max_doc - min_doc) as usize;
            let mut bitset = vec![0u64; (span + 63) / 64];
            for &doc in &self.docs {
                if doc >= min_doc && doc < max_doc {
                    let idx = (doc - min_doc) as usize;
                    bitset[idx / 64] |= 1u64 << (idx % 64);
                }
            }
            Ok(bitset)
        }
    }

    fn minimal_page_pruner() -> Arc<PagePruner> {
        // Build a 1-row-group parquet with no filters — page pruner becomes a no-op
        // (filter_row_ids returns input, candidate_row_ids returns [first_row, first_row+num_rows)).
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![0i32; 8]))],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut writer = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let file = tmp.reopen().unwrap();
        let options = ArrowReaderOptions::new().with_page_index(true);
        let meta = ArrowReaderMetadata::load(&file, options).unwrap();
        let pruner = PagePruner::new(meta.schema(), meta.metadata().clone());
        Arc::new(pruner)
    }

    #[test]
    fn path_b_and_mode_collects_docs_and_returns_offsets() {
        let collector = Arc::new(StubCollector {
            docs: vec![0, 3, 7],
        }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None);

        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    #[test]
    fn on_batch_mask_returns_none_for_path_b() {
        let collector = Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3]))],
        ).unwrap();
        // Empty position map is fine; SingleCollectorEvaluator ignores it.
        let pm = crate::indexed_table::row_selection::PositionMap::from_selection(
            &datafusion::parquet::arrow::arrow_reader::RowSelection::from(
                Vec::<datafusion::parquet::arrow::arrow_reader::RowSelector>::new(),
            ),
        );
        assert!(eval.on_batch_mask(&(), 0, &pm, 0, 3, &batch).unwrap().is_none());
    }

    #[test]
    fn single_collector_needs_row_mask() {
        // SingleCollectorEvaluator returns None from on_batch_mask, so
        // IndexedStream must build current_mask from candidate offsets
        // (it's the only post-decode filter we have on this path).
        let collector = Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None);
        assert!(eval.needs_row_mask());
    }

    #[test]
    fn empty_match_returns_none() {
        let collector = Arc::new(StubCollector { docs: vec![] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None);
        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        assert!(eval.prefetch_rg(&rg, 0, 8).unwrap().is_none());
    }

    #[test]
    fn empty_pruning_predicates_leave_collector_unchanged() {
        // With no pruning predicates, the evaluator is a pass-through for
        // the collector bitmap: every doc the collector returns remains a
        // candidate. (Contrast with the old BitsetMode::Or path, which
        // would have unioned with page-pruner-derived "anything-allowed"
        // row IDs — semantics that were never wired up in production.)
        let collector = Arc::new(StubCollector { docs: vec![0, 3, 7] })
            as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None);

        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    // Keep the `fmt` import used
    #[allow(dead_code)]
    fn _use(_: &dyn fmt::Debug) {}
}
