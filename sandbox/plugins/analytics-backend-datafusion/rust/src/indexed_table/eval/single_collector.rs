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
use crate::indexed_table::row_selection::{row_selection_to_bitmap, PositionMap};
use crate::indexed_table::stream::RowGroupInfo;
use datafusion::physical_optimizer::pruning::PruningPredicate;

/// Per-RG state the evaluator keeps for refinement. In row-granular
/// mode parquet narrowed fully via `with_predicate` + `RowSelection`
/// and nothing is needed here. In block-granular mode we need the
/// Collector candidate bitmap to build a post-decode mask.
struct SingleCollectorState {
    candidates: RoaringBitmap,
}

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
    /// Raw residual expression (non-Collector children of the top-level
    /// AND, converted to a single `PhysicalExpr`).
    ///
    /// Used in two modes:
    ///
    /// - **Row-granular** (`min_skip_run = 1`): the same expression is
    ///   stashed on `IndexedTableConfig.pushdown_predicate` and handed
    ///   to parquet's `with_predicate` for decode-time filtering.
    ///   Combined with the Collector-bitmap `RowSelection`, parquet
    ///   delivers exact `Collector ∧ residual` rows. `on_batch_mask`
    ///   returns `None` (nothing left to do).
    ///
    /// - **Block-granular** (`min_skip_run > 1`): pushdown is OFF
    ///   (alignment risk with coalesced selection). `on_batch_mask`
    ///   evaluates this expression against the decoded batch and
    ///   AND-combines with the Collector bitmap mask to produce the
    ///   exact result.
    residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Counters recorded by `page_pruner.prune_rg`. Built from the
    /// stream's `PartitionMetrics` at evaluator construction.
    page_prune_metrics:
        Option<crate::indexed_table::page_pruner::PagePruneMetrics>,
    /// Incremented once per `prefetch_rg` call (once per RG) — the
    /// Collector path always performs one FFM round-trip to Java.
    ffm_collector_calls: Option<datafusion::physical_plan::metrics::Count>,
}

impl SingleCollectorEvaluator {
    pub fn new(
        collector: Arc<dyn RowGroupDocsCollector>,
        page_pruner: Arc<PagePruner>,
        pruning_predicate: Option<Arc<PruningPredicate>>,
        residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        page_prune_metrics: Option<
            crate::indexed_table::page_pruner::PagePruneMetrics,
        >,
        ffm_collector_calls: Option<datafusion::physical_plan::metrics::Count>,
    ) -> Self {
        Self {
            collector,
            page_pruner,
            pruning_predicate,
            residual_expr,
            page_prune_metrics,
            ffm_collector_calls,
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
        let bitset = self.collector
            .collect_packed_u64_bitset(min_doc, max_doc)
            .map_err(|e| {
                format!(
                    "collector.collect_packed_u64_bitset(rg={}, [{}, {})): {}",
                    rg.index, min_doc, max_doc, e
                )
            })?;
        if let Some(ref c) = self.ffm_collector_calls {
            c.add(1);
        }

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
            if let Some(sel) = self.page_pruner.prune_rg(
                pp,
                rg.index,
                self.page_prune_metrics.as_ref(),
            ) {
                let allowed = row_selection_to_bitmap(&sel);
                candidates &= allowed;
            }
            // `None` → no pruning available for this RG → keep candidates
            // unchanged (conservative).
        }

        if candidates.is_empty() {
            return Ok(None);
        }

        // Attach the candidate bitmap as per-RG state. `on_batch_mask`
        // downcast's this to reconstruct the Collector mask in
        // block-granular mode (where parquet with_predicate is OFF to
        // avoid misalignment with post-decode masking).
        Ok(Some(PrefetchedRg {
            candidates: candidates.clone(),
            eval_nanos: t.elapsed().as_nanos() as u64,
            context: Box::new(SingleCollectorState { candidates }),
        }))
    }

    fn on_batch_mask(
        &self,
        rg_state: &dyn std::any::Any,
        _rg_first_row: i64,
        position_map: &PositionMap,
        batch_offset: usize,
        batch_len: usize,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        // No residual → no post-decode work. Stream's current_mask
        // (if built) handles Collector narrowing.
        let Some(ref residual) = self.residual_expr else {
            return Ok(None);
        };
        // Apply Collector bitmap AND residual predicate over the
        // delivered batch. In row-granular mode (pushdown ON) this
        // re-applies what parquet already did — redundant but correct.
        // In block-granular mode (pushdown OFF) this is the only
        // place the residual gets applied.
        let state = rg_state
            .downcast_ref::<SingleCollectorState>()
            .ok_or_else(|| {
                "SingleCollectorEvaluator: rg_state is not SingleCollectorState".to_string()
            })?;

        // Build Collector mask over delivered rows via PositionMap.
        // Same logic as `row_selection::build_mask` but sliced to
        // [batch_offset, batch_offset + batch_len).
        let mut collector_bits = Vec::with_capacity(batch_len);
        for i in 0..batch_len {
            let delivered_idx = batch_offset + i;
            let rg_pos = position_map
                .rg_position(delivered_idx)
                .ok_or_else(|| {
                    format!(
                        "SingleCollectorEvaluator: delivered_idx {} out of range",
                        delivered_idx
                    )
                })?;
            let hit = if rg_pos <= u32::MAX as usize {
                state.candidates.contains(rg_pos as u32)
            } else {
                false
            };
            collector_bits.push(hit);
        }
        let collector_mask = BooleanArray::from(collector_bits);

        // Evaluate residual against the batch.
        let residual_value = residual
            .evaluate(batch)
            .map_err(|e| format!("SingleCollectorEvaluator: residual.evaluate: {}", e))?;
        let residual_array = residual_value
            .into_array(batch_len)
            .map_err(|e| format!("SingleCollectorEvaluator: residual into_array: {}", e))?;
        let residual_mask = residual_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                "SingleCollectorEvaluator: residual did not produce BooleanArray".to_string()
            })?;

        // AND with kleene semantics (NULL → exclude).
        let combined =
            datafusion::arrow::compute::kernels::boolean::and_kleene(&collector_mask, residual_mask)
                .map_err(|e| format!("SingleCollectorEvaluator: and_kleene: {}", e))?;
        Ok(Some(combined))
    }

    /// When we have a residual to apply in `on_batch_mask`, pushdown
    /// must be OFF in **block-granular mode** because we use
    /// `PositionMap` to look up RG positions over the full delivered
    /// rowset — pushdown would drop rows and misalign. In
    /// **row-granular mode** (`min_skip_run == 1`), pushdown is safe
    /// and desirable: parquet applies the residual in lockstep with
    /// decoding, `on_batch_mask` returns `None`, and output is
    /// exact. But the evaluator doesn't know min_skip_run — the
    /// stream does. The stream guards this via its
    /// `alignment_risk = min_skip_run != 1 && needs_row_mask()`
    /// check plus `forbid_parquet_pushdown`. We return `false` here
    /// and rely on `needs_row_mask = true` (default when residual is
    /// present) to trigger the stream's alignment guard in block
    /// mode; in row-granular mode that guard is inactive and
    /// pushdown proceeds.
    fn forbid_parquet_pushdown(&self) -> bool {
        false
    }

    /// Stream's `current_mask` construction consults this. When
    /// residual is set, we return `true` so the stream knows our
    /// `on_batch_mask` uses PositionMap (alignment risk) — this flag
    /// flips the stream's `alignment_risk` computation which
    /// suppresses pushdown in block-granular mode. In row-granular
    /// mode (min_skip_run == 1) the stream ignores this flag's
    /// pushdown impact and pushes anyway (which is what we want:
    /// parquet applies residual during decode of already-narrowed
    /// rowset, on_batch_mask returns None below).
    ///
    /// Without residual, we return `true` too — stream builds
    /// `current_mask` from Collector bitmap to narrow post-decode
    /// (legacy path for SingleCollector without a residual wasn't
    /// used in production but kept for defensive correctness).
    fn needs_row_mask(&self) -> bool {
        true
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
        let eval = SingleCollectorEvaluator::new(collector, pruner, None, None, None, None);

        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    #[test]
    fn on_batch_mask_returns_none_for_path_b() {
        let collector = Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None, None, None, None);
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
        let eval = SingleCollectorEvaluator::new(collector, pruner, None, None, None, None);
        assert!(eval.needs_row_mask());
    }

    #[test]
    fn empty_match_returns_none() {
        let collector = Arc::new(StubCollector { docs: vec![] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None, None, None, None);
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
        let eval = SingleCollectorEvaluator::new(collector, pruner, None, None, None, None);

        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    // Keep the `fmt` import used
    #[allow(dead_code)]
    fn _use(_: &dyn fmt::Debug) {}
}
