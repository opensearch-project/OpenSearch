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

use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::index::{BitsetMode, RowGroupDocsCollector};
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::stream::RowGroupInfo;

/// Evaluator holding one collector and applying per-RG page pruning.
pub struct SingleCollectorEvaluator {
    collector: Arc<dyn RowGroupDocsCollector>,
    page_pruner: Arc<PagePruner>,
    bitset_mode: BitsetMode,
}

impl SingleCollectorEvaluator {
    pub fn new(
        collector: Arc<dyn RowGroupDocsCollector>,
        page_pruner: Arc<PagePruner>,
        bitset_mode: BitsetMode,
    ) -> Self {
        Self {
            collector,
            page_pruner,
            bitset_mode,
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

        // Collect bitset from backend
        let bitset = self.collector.collect_packed_u64_bitset(min_doc, max_doc)?;

        // Expand bitset -> doc IDs. Iterate only set bits via trailing_zeros
        // rather than scanning all 64 bits of each word — for sparse bitsets
        // (typical when backend selectivity is low) this visits O(set_bits)
        // iterations instead of O(span).
        let mut raw_ids: Vec<i64> = Vec::new();
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
                    raw_ids.push(doc_id);
                }
                w &= w - 1; // clear lowest set bit
            }
        }

        // Apply page pruning
        let final_ids = match self.bitset_mode {
            BitsetMode::And => {
                self.page_pruner
                    .filter_row_ids(rg.index, &raw_ids, rg.first_row)
            }
            BitsetMode::Or => {
                let candidates =
                    self.page_pruner
                        .candidate_row_ids(rg.index, rg.first_row, rg.num_rows);
                sorted_union(&raw_ids, &candidates)
            }
        };

        if final_ids.is_empty() {
            return Ok(None);
        }

        let offsets: Vec<u64> = final_ids
            .iter()
            .map(|&id| (id - rg.first_row) as u64)
            .collect();

        Ok(Some(PrefetchedRg::without_context(
            offsets,
            t.elapsed().as_nanos() as u64,
        )))
    }

    fn on_batch_mask(
        &self,
        _rg_state: &dyn std::any::Any,
        _rg_first_row: i64,
        _batch_offset: usize,
        _batch_len: usize,
        _batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        // Single-collector path relies on DataFusion predicate pushdown; no
        // refinement mask needed.
        Ok(None)
    }
}

/// Merge two sorted `Vec<i64>` into one sorted `Vec<i64>` with duplicates removed.
fn sorted_union(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
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
        let pruner = PagePruner::new(meta.schema(), meta.metadata().clone(), &[]);
        Arc::new(pruner)
    }

    #[test]
    fn path_b_and_mode_collects_docs_and_returns_offsets() {
        let collector = Arc::new(StubCollector {
            docs: vec![0, 3, 7],
        }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, BitsetMode::And);

        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        assert_eq!(prefetched.offsets, vec![0, 3, 7]);
    }

    #[test]
    fn on_batch_mask_returns_none_for_path_b() {
        let collector = Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, BitsetMode::And);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3]))],
        ).unwrap();
        assert!(eval.on_batch_mask(&(), 0, 0, 3, &batch).unwrap().is_none());
    }

    #[test]
    fn single_collector_needs_row_mask() {
        // SingleCollectorEvaluator returns None from on_batch_mask, so
        // IndexedStream must build current_mask from candidate offsets
        // (it's the only post-decode filter we have on this path).
        let collector = Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, BitsetMode::And);
        assert!(eval.needs_row_mask());
    }

    #[test]
    fn empty_match_returns_none() {
        let collector = Arc::new(StubCollector { docs: vec![] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, BitsetMode::And);
        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        assert!(eval.prefetch_rg(&rg, 0, 8).unwrap().is_none());
    }

    #[test]
    fn path_b_or_mode_unions_collector_with_pruner_candidates() {
        // BitsetMode::Or is not yet emitted by classify_filter (reserved for
        // future OR(Collector, predicates) extension), but the branch is
        // wired and we exercise it here so a refactor can't silently break it.
        //
        // With `minimal_page_pruner` (no filters), candidate_row_ids returns
        // every row in the RG: [0, 1, ..., 7]. The collector contributes
        // {0, 3, 7}. Their union is {0..=7}. Offsets should be 0..=7.
        let collector = Arc::new(StubCollector { docs: vec![0, 3, 7] })
            as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, BitsetMode::Or);

        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        assert_eq!(prefetched.offsets, (0u64..8).collect::<Vec<_>>());
    }

    // Keep the `fmt` import used
    #[allow(dead_code)]
    fn _use(_: &dyn fmt::Debug) {}
}
