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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use native_bridge_common::log_info;
use roaring::RoaringBitmap;

use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::ffm_callbacks::{create_provider, FfmSegmentCollector, ProviderHandle};
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

/// TODO(phase-99): hardcoded selectivity threshold for opportunistic peer consultation.
/// Replaced by a cluster setting plumbed through `WireConfigSnapshot` and
/// `DatafusionQueryConfig` in the very last phase, after Phase 7 OR/NOT support and
/// everything else. Until then, performance-delegated leaves consult the peer when DF
/// page-pruning kept more than 5% of an RG.
const HARDCODED_SELECTIVITY_THRESHOLD: f64 = 0.05;


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
    /// Always-call collector for correctness-delegated predicates. `None` when
    /// the query has only performance-delegated leaves (no peer call required
    /// upfront — see `performance_provider_locks`).
    collector: Option<Arc<dyn RowGroupDocsCollector>>,
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
    /// Lazy `ProviderHandle` cache, one per performance-delegated annotation_id.
    /// Empty when the query has no performance-delegated leaves. Populated by
    /// the factory at query setup; lookups + `OnceLock` init happen ONLY when
    /// `should_consult_lucene` decides DF's own pruning wasn't selective enough
    /// for an RG. Drop releases the Lucene Weight via `releaseProvider`.
    ///
    /// The HashMap is **query-scoped** (shared across all per-(segment×chunk)
    /// evaluators of a single query via `Arc::clone`), so threads racing to fill
    /// a slot do so once per (query × annotation_id) — not per chunk.
    performance_provider_locks: Arc<HashMap<i32, Arc<OnceLock<ProviderHandle>>>>,
    /// Segment ordinal this evaluator was bound to at factory time (the
    /// `SegmentFileInfo` the factory closure was invoked with). Captured so
    /// `prefetch_rg` can build a per-call `FfmSegmentCollector` lazily without
    /// re-deriving the segment from `RowGroupInfo` (which doesn't carry it).
    segment_ord: i32,
}

impl SingleCollectorEvaluator {
    pub fn new(
        collector: Option<Arc<dyn RowGroupDocsCollector>>,
        page_pruner: Arc<PagePruner>,
        pruning_predicate: Option<Arc<PruningPredicate>>,
        residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        page_prune_metrics: Option<PagePruneMetrics>,
        ffm_collector_calls: Option<datafusion::physical_plan::metrics::Count>,
        call_strategy: CollectorCallStrategy,
        performance_provider_locks: Arc<HashMap<i32, Arc<OnceLock<ProviderHandle>>>>,
        segment_ord: i32,
    ) -> Self {
        Self {
            collector,
            page_pruner,
            pruning_predicate,
            residual_expr,
            page_prune_metrics,
            ffm_collector_calls,
            call_strategy,
            performance_provider_locks,
            segment_ord,
        }
    }
}

/// Per-RG decision: should we consult the peer backend?
///
/// Pure function. Inputs: post-page-prune surviving ranges, RG row count, and the
/// configured selectivity threshold. The function says "consult" when DF kept
/// MORE than `threshold` of the RG (page pruning wasn't selective enough — peer
/// might narrow further); "skip" when DF already squeezed it below threshold
/// (peer call would be wasted work).
///
/// `page_ranges == None` means there's no usable PruningPredicate for the
/// residual at all (e.g. text-column predicate with no parquet stats) — DF
/// can't help, so consult.
fn should_consult_lucene(
    page_ranges: &Option<Vec<(i32, i32)>>,
    rg: &RowGroupInfo,
    threshold: f64,
) -> bool {
    let surviving_rows = match page_ranges {
        None => rg.num_rows as i64,
        Some(ranges) => ranges.iter().map(|(lo, hi)| (hi - lo) as i64).sum::<i64>(),
    };
    if rg.num_rows == 0 {
        return false;
    }
    let surviving_fraction = surviving_rows as f64 / rg.num_rows as f64;
    surviving_fraction > threshold
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

        // Build candidates either from the always-call correctness collector OR, when
        // the query is performance-only (no Collector leaves), from the page-pruned
        // universe. Performance leaves are AND'd in below if the selectivity gate fires.
        let mut candidates = match self.collector.as_ref() {
            Some(collector) => {
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
                let mut bm = RoaringBitmap::new();
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
                    bm |= chunk;
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
                        bm &= allowed;
                    }
                }
                bm
            }
            None => {
                // Performance-only query. Seed candidates with the page-pruned universe
                // (or the full RG if no PruningPredicate). The opportunistic peer branch
                // below may narrow further; otherwise DF's pushdown filter handles the
                // residual at decode time.
                let mut bm = RoaringBitmap::new();
                match &page_ranges {
                    Some(r) if r.is_empty() => return Ok(None),
                    Some(r) => {
                        for (r_min, r_max) in r {
                            let lo = (*r_min as i64 - rg.first_row) as u32;
                            let hi = (*r_max as i64 - rg.first_row) as u32;
                            bm.insert_range(lo..hi);
                        }
                    }
                    None => {
                        bm.insert_range(0..rg.num_rows as u32);
                    }
                }
                bm
            }
        };

        // Opportunistic peer consultation for performance-delegated leaves. Only fires
        // when DF page-pruning kept more than the configured fraction of the RG —
        // skipping the FFM round-trip when DF was already selective. Lazy: lock the
        // map only if the gate fires; create the provider only once per query × leaf.
        // TODO(d3): consult ALL performance leaves whose gate fires and AND their
        // bitsets. Today we consult the first leaf only — sufficient for AND-only
        // single-call demo. Multi-leaf intersection is part of D3 follow-up.
        if !self.performance_provider_locks.is_empty()
            && should_consult_lucene(&page_ranges, rg, HARDCODED_SELECTIVITY_THRESHOLD)
        {
            // Pick the smallest annotation_id deterministically so logs/tests are stable.
            // Avoids the Vec/sort allocation in the common single-leaf case.
            let annotation_id = *self
                .performance_provider_locks
                .keys()
                .min()
                .expect("performance_provider_locks is non-empty (just checked)");
            log_info!(
                "[scf-rust] consulting peer for performance leaf rg={} seg={} range=[{},{}) annotation_id={}",
                rg.index, self.segment_ord, min_doc, max_doc, annotation_id
            );
            let lock = self
                .performance_provider_locks
                .get(&annotation_id)
                .expect("annotation_id was just pulled from the map's keys");
            let mut just_initialized = false;
            let provider = lock.get_or_init(|| {
                just_initialized = true;
                create_provider(annotation_id).expect("create_provider FFM upcall failed")
            });
            if just_initialized {
                log_info!(
                    "[scf-rust] lazy provider initialized annotation_id={} provider_key={}",
                    annotation_id, provider.key()
                );
            }

            let collector = FfmSegmentCollector::create(
                provider.key(),
                self.segment_ord,
                min_doc,
                max_doc,
            )
            .map_err(|e| {
                format!(
                    "FfmSegmentCollector::create(perf provider={}, seg={}, doc_range=[{},{})): {}",
                    provider.key(),
                    self.segment_ord,
                    min_doc,
                    max_doc,
                    e
                )
            })?;
            let bitset = collector
                .collect_packed_u64_bitset(min_doc, max_doc)
                .map_err(|e| {
                    format!(
                        "perf collector.collect_packed_u64_bitset(rg={}, [{}, {})): {}",
                        rg.index, min_doc, max_doc, e
                    )
                })?;
            if let Some(ref c) = self.ffm_collector_calls {
                c.add(1);
            }
            let offset = (min_doc as i64 - rg.first_row) as u32;
            let num_docs = (max_doc - min_doc) as u32;
            let bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(bitset.as_ptr() as *const u8, bitset.len() * 8)
            };
            let mut peer_bm = RoaringBitmap::from_lsb0_bytes(offset, bytes);
            let upper = offset.saturating_add(num_docs);
            if upper < u32::MAX {
                peer_bm.remove_range(upper..);
            }
            let candidates_before = candidates.len();
            let peer_card = peer_bm.len();
            candidates &= peer_bm;
            log_info!(
                "[scf-rust] peer bitset intersected rg={} seg={} candidates_before={} peer_cardinality={} candidates_after={}",
                rg.index, self.segment_ord, candidates_before, peer_card, candidates.len()
            );
        }

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

        // Evaluate residual against the batch. The residual may use
        // full-schema column indices; remap to batch positions by name.
        let remapped_residual = super::remap_expr_to_batch(residual, batch)
            .map_err(|e| format!("SingleCollectorEvaluator: remap residual: {}", e))?;
        let residual_value = remapped_residual
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
        let combined = datafusion::arrow::compute::kernels::boolean::and_kleene(
            &collector_mask,
            residual_mask,
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
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), 0);

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
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), 0);
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
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), 0);
        assert!(eval.needs_row_mask());
    }

    #[test]
    fn empty_match_returns_none() {
        let collector = Arc::new(StubCollector { docs: vec![] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), 0);
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
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), 0);

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

