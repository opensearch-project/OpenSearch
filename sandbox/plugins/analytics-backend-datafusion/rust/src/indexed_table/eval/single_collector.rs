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
use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner};
use crate::indexed_table::row_selection::{
    bitmap_to_packed_bits, packed_bits_to_boolean_array, row_selection_to_bitmap, PositionMap,
};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use std::time::Instant;

/// Re-exported from parent module for backward compatibility.
pub use super::CollectorCallStrategy;
use crate::indexed_table::stream::RowGroupInfo;

/// Per-RG state the evaluator keeps for refinement. In row-granular
/// mode parquet narrowed fully via `with_predicate` + `RowSelection`
/// and nothing is needed here. In block-granular mode we need the
/// Collector candidate bitmap to build a post-decode mask.
///
/// `mask_buffer` is the candidate bitmap in Arrow's native LSB-first bit
/// layout, wrapped as a refcounted `Buffer`. Sharing an `Arc<Buffer>` lets
/// `on_batch_mask` and `build_mask` build zero-copy `BooleanBuffer`
/// views via `BooleanBuffer::new(buf.clone(), bit_offset, bit_len)`.
/// Length of the underlying buffer covers `mask_len` bits (= rg_num_rows).
struct SingleCollectorState {
    candidates: RoaringBitmap,
    mask_buffer: datafusion::arrow::buffer::Buffer,
    mask_len: usize,
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
    page_prune_metrics: Option<PagePruneMetrics>,
    /// Incremented once per `prefetch_rg` call (once per RG) — the
    /// Collector path always performs one FFM round-trip to Java.
    ffm_collector_calls: Option<datafusion::physical_plan::metrics::Count>,
    call_strategy: CollectorCallStrategy,
}

impl SingleCollectorEvaluator {
    pub fn new(
        collector: Arc<dyn RowGroupDocsCollector>,
        page_pruner: Arc<PagePruner>,
        pruning_predicate: Option<Arc<PruningPredicate>>,
        residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        page_prune_metrics: Option<PagePruneMetrics>,
        ffm_collector_calls: Option<datafusion::physical_plan::metrics::Count>,
        call_strategy: CollectorCallStrategy,
    ) -> Self {
        Self {
            collector,
            page_pruner,
            pruning_predicate,
            residual_expr,
            page_prune_metrics,
            ffm_collector_calls,
            call_strategy,
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
        let t = Instant::now();

        // Page-prune to discover which row ranges survive.
        let page_ranges: Option<Vec<(i32, i32)>> = self.pruning_predicate.as_ref().and_then(|pp| {
            self.page_pruner
                .prune_rg(pp, rg.index, self.page_prune_metrics.as_ref())
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
        });

        // Build candidate bitmap from collector.
        let candidates = {
            let collector = &self.collector;
            // Dispatch collector call strategy.
            let call_ranges: Vec<(i32, i32)> = match self.call_strategy {
                CollectorCallStrategy::FullRange => vec![(min_doc, max_doc)],
                CollectorCallStrategy::TightenOuterBounds => match &page_ranges {
                    Some(r) if r.is_empty() => return Ok(None),
                    Some(r) => vec![(r.first().unwrap().0, r.last().unwrap().1)],
                    None => vec![(min_doc, max_doc)],
                },
                CollectorCallStrategy::PageRangeSplit => match &page_ranges {
                    Some(r) if r.is_empty() => return Ok(None),
                    Some(r) => r.clone(),
                    None => vec![(min_doc, max_doc)],
                },
            };

            // Call collector for each range, merge into one RG-relative bitmap.
            let mut candidates = RoaringBitmap::new();
            for (r_min, r_max) in &call_ranges {
                let bitset = collector
                    .collect_packed_u64_bitset(*r_min, *r_max)
                    .map_err(|e| {
                        format!(
                            "collector.collect_packed_u64_bitset(rg={}, [{}, {})): {}",
                            rg.index, r_min, r_max, e
                        )
                    })?;
                if let Some(ref c) = self.ffm_collector_calls {
                    c.add(1);
                }
                let offset = (*r_min as i64 - rg.first_row) as u32;
                let num_docs = (*r_max - *r_min) as u32;
                let bytes: &[u8] = unsafe {
                    std::slice::from_raw_parts(bitset.as_ptr() as *const u8, bitset.len() * 8)
                };
                let mut chunk = RoaringBitmap::from_lsb0_bytes(offset, bytes);
                let upper = offset.saturating_add(num_docs);
                if upper < u32::MAX {
                    chunk.remove_range(upper..);
                }
                candidates |= chunk;
            }

            // For FullRange and TightenOuterBounds, AND with page bitmap
            // to remove rows in dead pages that the collector scanned.
            if self.call_strategy != CollectorCallStrategy::PageRangeSplit {
                if let Some(ref ranges) = page_ranges {
                    let mut allowed = RoaringBitmap::new();
                    for (r_min, r_max) in ranges {
                        let lo = (*r_min as i64 - rg.first_row) as u32;
                        let hi = (*r_max as i64 - rg.first_row) as u32;
                        allowed.insert_range(lo..hi);
                    }
                    candidates &= allowed;
                }
            }
            candidates
        };

        if candidates.is_empty() {
            return Ok(None);
        }

        // Materialise the final RG-relative bitmap as an Arrow `Buffer`
        // in Arrow's native LSB-first layout. This is the ONLY
        // representation the hot paths (`on_batch_mask`, `build_mask`)
        // need; they construct zero-copy `BooleanBuffer` views via
        // `BooleanBuffer::new(buf.clone(), bit_offset, bit_len)`.
        let mask_len = rg.num_rows as usize;
        let packed_bits = bitmap_to_packed_bits(&candidates, mask_len as u32);
        let mask_buffer = datafusion::arrow::buffer::Buffer::from_vec(packed_bits);
        Ok(Some(PrefetchedRg {
            candidates: candidates.clone(),
            eval_nanos: t.elapsed().as_nanos() as u64,
            context: Box::new(SingleCollectorState {
                candidates,
                mask_buffer: mask_buffer.clone(),
                mask_len,
            }),
            mask_buffer: Some(mask_buffer),
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

        let state = rg_state
            .downcast_ref::<SingleCollectorState>()
            .ok_or_else(|| {
                "SingleCollectorEvaluator: rg_state is not SingleCollectorState".to_string()
            })?;

        // Build Collector mask over delivered rows via PositionMap.
        // All paths produce a `BooleanArray` whose underlying
        // `Buffer` is a refcounted view into `state.mask_buffer` —
        // zero allocation for Identity, at most one small packed
        // Vec<u64> for Runs.
        let collector_mask: BooleanArray = match position_map {
            // Identity: delivered row i == rg_position (batch_offset + i).
            // BooleanBuffer::new adjusts bit_offset without copying the
            // underlying Buffer. The returned BooleanArray points into
            // state.mask_buffer; lifecycle is Arc-managed.
            PositionMap::Identity { .. } => {
                let bb = datafusion::arrow::buffer::BooleanBuffer::new(
                    state.mask_buffer.clone(),
                    batch_offset,
                    batch_len,
                );
                BooleanArray::new(bb, None)
            }
            // Every delivered row is by construction a candidate — mask is all-true.
            PositionMap::Bitmap { .. } => BooleanArray::new(
                datafusion::arrow::buffer::BooleanBuffer::new_set(batch_len),
                None,
            ),
            // Runs: gather per-row bit from the shared mask_buffer into
            // a new packed Vec<u64> (small — bounded by batch_len/64).
            PositionMap::Runs { .. } => {
                let words = batch_len.div_ceil(64);
                let mut out = vec![0u64; words];
                let src_bytes = state.mask_buffer.as_slice();
                for i in 0..batch_len {
                    let delivered_idx = batch_offset + i;
                    let rg_pos = position_map.rg_position(delivered_idx).ok_or_else(|| {
                        format!(
                            "SingleCollectorEvaluator: delivered_idx {} out of range",
                            delivered_idx
                        )
                    })?;
                    // Read bit rg_pos from the packed buffer (LSB-first).
                    let hit = rg_pos < state.mask_len
                        && (src_bytes[rg_pos >> 3] >> (rg_pos & 7)) & 1 == 1;
                    if hit {
                        out[i >> 6] |= 1u64 << (i & 63);
                    }
                }
                packed_bits_to_boolean_array(out, batch_len)
            }
        };

        // Evaluate residual against the batch.
        let residual_mask = super::eval_helpers::evaluate_residual(residual, batch, batch_len)?;

        // AND with kleene semantics (NULL → exclude).
        let combined = datafusion::arrow::compute::kernels::boolean::and_kleene(
            &collector_mask,
            &residual_mask,
        )
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
    use datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata;
    use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
    use datafusion::parquet::arrow::ArrowWriter;
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
        fn collect_packed_u64_bitset(
            &self,
            min_doc: i32,
            max_doc: i32,
        ) -> Result<Vec<u64>, String> {
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
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(
                vec![0i32; 8],
            ))],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut writer =
                ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), None).unwrap();
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
        let eval = SingleCollectorEvaluator::new(collector, pruner, None, None, None, None, CollectorCallStrategy::FullRange);

        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    #[test]
    fn on_batch_mask_returns_none_for_path_b() {
        let collector = Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None, None, None, None, CollectorCallStrategy::FullRange);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3,
            ]))],
        )
        .unwrap();
        // Empty position map is fine; SingleCollectorEvaluator ignores it.
        let pm = PositionMap::from_selection(
            &datafusion::parquet::arrow::arrow_reader::RowSelection::from(Vec::<
                datafusion::parquet::arrow::arrow_reader::RowSelector,
            >::new()),
        );
        assert!(eval
            .on_batch_mask(&(), 0, &pm, 0, 3, &batch)
            .unwrap()
            .is_none());
    }

    #[test]
    fn single_collector_needs_row_mask() {
        // SingleCollectorEvaluator returns None from on_batch_mask, so
        // IndexedStream must build current_mask from candidate offsets
        // (it's the only post-decode filter we have on this path).
        let collector = Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None, None, None, None, CollectorCallStrategy::FullRange);
        assert!(eval.needs_row_mask());
    }

    #[test]
    fn empty_match_returns_none() {
        let collector = Arc::new(StubCollector { docs: vec![] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None, None, None, None, CollectorCallStrategy::FullRange);
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        assert!(eval.prefetch_rg(&rg, 0, 8).unwrap().is_none());
    }

    #[test]
    fn empty_pruning_predicates_leave_collector_unchanged() {
        // With no pruning predicates, the evaluator is a pass-through for
        // the collector bitmap: every doc the collector returns remains a
        // candidate. (Contrast with the old BitsetMode::Or path, which
        // would have unioned with page-pruner-derived "anything-allowed"
        // row IDs — semantics that were never wired up in production.)
        let collector = Arc::new(StubCollector {
            docs: vec![0, 3, 7],
        }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(collector, pruner, None, None, None, None, CollectorCallStrategy::FullRange);

        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    // Keep the `fmt` import used
    #[allow(dead_code)]
    fn _use(_: &dyn fmt::Debug) {}
}

/// Remap Column indices in a PhysicalExpr to match the batch schema by name.
pub fn remap_expr_to_batch(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<Arc<dyn datafusion::physical_expr::PhysicalExpr>, String> {
    use datafusion::common::tree_node::TreeNode;
    use datafusion::physical_expr::expressions::Column;

    expr.clone()
        .transform(|e| {
            if let Some(col) = e.as_any().downcast_ref::<Column>() {
                if let Ok(new_idx) = batch.schema().index_of(col.name()) {
                    if new_idx != col.index() {
                        let remapped = Arc::new(Column::new(col.name(), new_idx))
                            as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
                        return Ok(datafusion::common::tree_node::Transformed::yes(remapped));
                    }
                }
            }
            Ok(datafusion::common::tree_node::Transformed::no(e))
        })
        .map(|t| t.data)
        .map_err(|e| format!("remap_expr_to_batch: {}", e))
}
