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
use native_bridge_common::log_debug;
use roaring::RoaringBitmap;

use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::ffm_callbacks::{create_provider, FfmSegmentCollector, ProviderHandle};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner, StatsPruneTree};
use crate::indexed_table::row_selection::{
    bitmap_to_packed_bits, packed_bits_to_boolean_array, row_selection_to_bitmap, PositionMap,
};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use std::time::Instant;

/// Re-exported from parent module for backward compatibility.
pub use super::CollectorCallStrategy;
use crate::indexed_table::stream::RowGroupInfo;

/// One segment's drained Lucene bitmap, lazily filled exactly once and shared
/// across every partition and chunk of that segment via `Arc`.
///
/// The cached value is a `Result`, not an `Option`, because a failed drain and
/// an empty (zero-match) drain must be told apart:
/// - `Ok(bm)` — drained successfully; an empty `bm` means the predicate
///   genuinely matched nothing in this segment.
/// - `Err(e)` — the FFM/Lucene drain failed. A **correctness** predicate has no
///   DataFusion fallback, so it MUST propagate this (failing the query) rather
///   than treat it as "no matches" and silently drop rows; a **performance
///   peer** may ignore it and let DataFusion's `FilterExec` evaluate the
///   predicate. Caching the error is intentional: correctness fails the query
///   on first sight, and a peer should not re-hammer a failing upcall.
pub type SegmentBitmapCell = Arc<OnceLock<Result<RoaringBitmap, String>>>;

/// Query-scoped, segment-keyed cache of drained Lucene bitmaps. Holds one
/// [`SegmentBitmapCell`] per key behind a cold-path `Mutex`; the hot path is
/// lock-free via the per-cell `OnceLock`. `K` is `writer_generation` for the
/// correctness bitmap (one per segment) and `(annotation_id, writer_generation)`
/// for peer bitmaps (one per peer predicate per segment).
pub struct SegmentBitmapCache<K: std::hash::Hash + Eq> {
    cells: std::sync::Mutex<HashMap<K, SegmentBitmapCell>>,
}

impl<K: std::hash::Hash + Eq> SegmentBitmapCache<K> {
    pub fn new() -> Self {
        Self { cells: std::sync::Mutex::new(HashMap::new()) }
    }

    /// Get (creating if absent) the shared cell for `key`. Cheap `Arc` clone of
    /// the cell; the bitmap itself is filled lazily on first `get_or_init`.
    pub fn cell(&self, key: K) -> SegmentBitmapCell {
        Arc::clone(
            self.cells
                .lock()
                .unwrap()
                .entry(key)
                .or_insert_with(|| Arc::new(OnceLock::new())),
        )
    }
}

impl<K: std::hash::Hash + Eq> Default for SegmentBitmapCache<K> {
    fn default() -> Self {
        Self::new()
    }
}

/// Selectivity threshold for opportunistic peer consultation: a performance-delegated
/// leaf consults the peer only when DF page-pruning kept more than 5% of an RG.
const PEER_CONSULT_SELECTIVITY_THRESHOLD: f64 = 0.05;

/// Builds delegated-backend collectors for performance-delegated leaves. Production impl
/// wraps `FfmSegmentCollector::create` (Java/Lucene round-trip); fuzz tests inject a
/// mock that replays a pre-computed bitset without an FFM call.
///
/// `context_id` is the per-query identifier passed through to every FFM upcall so Java
/// can route each callback to the correct per-query handle and tracker.
///
/// TODO: extend this factory to also build the *correctness* collector currently passed
/// in pre-built by `indexed_executor.rs`. Today delegated-backend (perf-delegated)
/// collectors are built inside this evaluator while correctness collectors are built
/// upstream — that asymmetry should go once we have more than one delegated backend
/// (DSL, vector, etc.) and the executor wants a single place to plug them in.
pub trait DelegatedBackendCollectorFactory: Send + Sync + std::fmt::Debug {
    fn create(
        &self,
        context_id: i64,
        provider_key: i32,
        writer_generation: i64,
        doc_min: i32,
        doc_max: i32,
    ) -> Result<Arc<dyn RowGroupDocsCollector>, String>;
}

/// Production factory: delegates to `FfmSegmentCollector::create`, which round-trips
/// to Java via FFM to build a Lucene-backed collector.
#[derive(Debug)]
pub struct FfmDelegatedBackendCollectorFactory;

impl DelegatedBackendCollectorFactory for FfmDelegatedBackendCollectorFactory {
    fn create(
        &self,
        context_id: i64,
        provider_key: i32,
        writer_generation: i64,
        doc_min: i32,
        doc_max: i32,
    ) -> Result<Arc<dyn RowGroupDocsCollector>, String> {
        let collector = FfmSegmentCollector::create(context_id, provider_key, writer_generation, doc_min, doc_max)?;
        Ok(Arc::new(collector) as Arc<dyn RowGroupDocsCollector>)
    }
}

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
    /// Pre-computed full-segment peer bitmap cache, keyed by annotation_id and
    /// shared across all partitions of the segment. Lazily populated on first
    /// access (when a partition's RG passes the selectivity gate). See
    /// [`SegmentBitmapCell`] for the `Result` value's meaning.
    peer_bitmap_cache: Arc<HashMap<i32, SegmentBitmapCell>>,
    /// Pre-computed full-segment **correctness** bitmap cell, shared across all
    /// partitions touching the segment. Lazily populated on the first prefetch_rg
    /// that needs the correctness collector; once init, all chunks/partitions
    /// slice from it with no FFM. `None` when the query has no
    /// correctness-delegated leaf (performance-only).
    correctness_bitmap_lock: Option<SegmentBitmapCell>,
    /// Provider for the correctness annotation, used by the bitmap-cache initializer
    /// to mint a single FfmSegmentCollector covering the full segment. `None` iff
    /// `correctness_bitmap_lock` is `None`.
    correctness_provider: Option<Arc<ProviderHandle>>,
    /// Writer generation identifying the segment this evaluator was bound to.
    writer_generation: i64,
    /// Global doc range [start, end) for this segment — derived from the segment's
    /// row_groups (first RG's first_row .. last RG's first_row + num_rows).
    segment_doc_range: (i32, i32),
    /// Builds the delegated-backend collector. Production wires
    /// `FfmDelegatedBackendCollectorFactory`; fuzz tests inject a mock.
    delegated_backend_collector_factory: Arc<dyn DelegatedBackendCollectorFactory>,
    /// Per-query context identifier passed through every FFM upcall so Java can route
    /// each callback to the correct per-query `FilterDelegationHandle` and tracker.
    context_id: i64,
    /// Bloom filter pruning config. None = disabled.
    bloom_config: Option<BloomConfig>,
    /// Precomputed per-RG/subtree match status from RG-level column stats.
    stats_prune_tree: Option<StatsPruneTree>,
}

/// Resources needed for per-RG bloom filter pruning.
pub struct BloomConfig {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub object_path: object_store::path::Path,
    pub metadata: Arc<ParquetMetaData>,
    pub arrow_schema: Arc<datafusion::arrow::datatypes::Schema>,
    pub io_handle: tokio::runtime::Handle,
    pub rg_bloom_pruned: Option<datafusion::physical_plan::metrics::Count>,
    pub bloom_filter_eval_time: Option<datafusion::physical_plan::metrics::Time>,
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
        peer_bitmap_cache: Arc<HashMap<i32, SegmentBitmapCell>>,
        correctness_bitmap_lock: Option<SegmentBitmapCell>,
        correctness_provider: Option<Arc<ProviderHandle>>,
        writer_generation: i64,
        segment_doc_range: (i32, i32),
        delegated_backend_collector_factory: Arc<dyn DelegatedBackendCollectorFactory>,
        context_id: i64,
        bloom_config: Option<BloomConfig>,
        stats_prune_tree: Option<StatsPruneTree>,
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
            peer_bitmap_cache,
            correctness_bitmap_lock,
            correctness_provider,
            writer_generation,
            segment_doc_range,
            delegated_backend_collector_factory,
            context_id,
            bloom_config,
            stats_prune_tree,
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

impl SingleCollectorEvaluator {
    /// Drain `[lo, hi)` from Lucene into a range-relative `RoaringBitmap`
    /// (bit 0 = doc `lo`). `Err` means the FFM/Lucene drain itself failed; an
    /// empty `Ok` means the predicate genuinely matched nothing. Callers MUST
    /// keep these apart — see [`SegmentBitmapCell`].
    fn drain_segment_range(
        factory: &Arc<dyn DelegatedBackendCollectorFactory>,
        context_id: i64,
        provider_key: i32,
        writer_gen: i64,
        lo: i32,
        hi: i32,
        ffm_calls: &Option<datafusion::physical_plan::metrics::Count>,
    ) -> Result<RoaringBitmap, String> {
        let collector = factory
            .create(context_id, provider_key, writer_gen, lo, hi)
            .map_err(|e| format!("drain_segment_range create [{},{}): {}", lo, hi, e))?;
        let bitset = collector
            .collect_packed_u64_bitset(lo, hi)
            .map_err(|e| format!("drain_segment_range collectDocs [{},{}): {}", lo, hi, e))?;
        if let Some(ref c) = ffm_calls {
            c.add(1);
        }
        let span = (hi - lo) as u32;
        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(bitset.as_ptr() as *const u8, bitset.len() * 8)
        };
        let mut bm = RoaringBitmap::from_lsb0_bytes(0, bytes);
        if span < u32::MAX {
            bm.remove_range(span..);
        }
        Ok(bm)
    }

    /// Get-or-drain the shared per-segment bitmap cell. The drain runs at most
    /// once per cell (`OnceLock`); all partitions of the segment share the
    /// result. Returns a borrow of the cached `Result` so callers decide how to
    /// treat an `Err` (correctness propagates, peer ignores).
    fn fill_segment_cell<'a>(
        cell: &'a SegmentBitmapCell,
        factory: &Arc<dyn DelegatedBackendCollectorFactory>,
        context_id: i64,
        provider_key: i32,
        writer_gen: i64,
        seg_start: i32,
        seg_end: i32,
        ffm_calls: &Option<datafusion::physical_plan::metrics::Count>,
    ) -> &'a Result<RoaringBitmap, String> {
        cell.get_or_init(|| {
            Self::drain_segment_range(
                factory, context_id, provider_key, writer_gen, seg_start, seg_end, ffm_calls,
            )
        })
    }

    /// Extract this RG's window from a segment-relative bitmap, rebased to
    /// RG-relative coordinates. `full_bm` bit 0 = doc `seg_start`; the returned
    /// bitmap's bit 0 = doc `rg.first_row`. The correctness path assigns this as
    /// the candidate set; the peer path ANDs it into the existing candidates.
    fn rg_slice(
        full_bm: &RoaringBitmap,
        rg: &RowGroupInfo,
        seg_start: i32,
        min_doc: i32,
        max_doc: i32,
    ) -> RoaringBitmap {
        let rg_offset_in_seg = (min_doc - seg_start) as u32;
        let rg_len = (max_doc - min_doc) as u32;
        let candidates_base = (min_doc as i64 - rg.first_row) as u32;
        let upper = rg_offset_in_seg.saturating_add(rg_len);
        let mut slice = RoaringBitmap::new();
        for d in full_bm.range(rg_offset_in_seg..upper) {
            slice.insert(candidates_base + (d - rg_offset_in_seg));
        }
        slice
    }

    /// Materialise the final RG-relative candidate bitmap as a `PrefetchedRg`.
    /// Shared between the legacy collector path and the bitmap-cache path so the
    /// downstream layout (mask buffer, RowSelection) stays identical.
    fn finish_candidates(
        candidates: RoaringBitmap,
        rg: &RowGroupInfo,
        t: Instant,
    ) -> Result<Option<PrefetchedRg>, String> {
        if candidates.is_empty() {
            return Ok(None);
        }
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
}

impl RowGroupBitsetSource for SingleCollectorEvaluator {
    /// Pre-warm only the per-segment correctness bitmap cache. Single-shot drain:
    /// a K-way parallel split was tried and reverted because the redundant FST walks
    /// on the Java/Lucene side serialized through shared state and made things slower
    /// (5s → 7s for the big segment). Skips page-pruning + per-RG metrics so it is
    /// safe to call from `QueryShardExec.execute()` without inflating counters.
    fn warm_cache(&self) {
        let (Some(bitmap_lock), Some(provider)) =
            (self.correctness_bitmap_lock.as_ref(), self.correctness_provider.as_ref())
        else {
            return;
        };
        let (seg_start, seg_end) = self.segment_doc_range;
        // Prime the shared cell; the actual prefetch_rg later reads the cached
        // result and decides how to treat any Err. A failure cached here is not
        // acted on until then.
        Self::fill_segment_cell(
            bitmap_lock,
            &self.delegated_backend_collector_factory,
            self.context_id,
            provider.key(),
            self.writer_generation,
            seg_start,
            seg_end,
            &self.ffm_collector_calls,
        );
    }

    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let t = Instant::now();

        // RG-level early-exit: precomputed from column stats at construction.
        if let Some(ref spt) = self.stats_prune_tree {
            if let Some(&false) = spt.rg_can_match.get(rg.index) {
                native_bridge_common::log_debug!(
                    "SingleCollector: skipping RG {} — pruned by RG-level stats",
                    rg.index
                );
                return Ok(None);
            }
        }

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

        // All pages pruned by stats → skip bloom + collector entirely.
        if let Some(ref ranges) = page_ranges {
            if ranges.is_empty() {
                return Ok(None);
            }
        }

        // Bloom filter pruning: runs after page pruning (free) but before
        // the expensive FFM collector call. Uses the IO runtime handle from
        // the RuntimeManager to drive the async object-store read.
        if let (Some(bloom), Some(pp)) = (&self.bloom_config, &self.pruning_predicate) {
            let _timer = bloom.bloom_filter_eval_time.as_ref().map(|t| t.timer());
            let pruned = bloom.io_handle.block_on(
                crate::indexed_table::bloom_pruner::bloom_prune_rg(
                    &*bloom.store,
                    &bloom.object_path,
                    &bloom.metadata,
                    &bloom.arrow_schema,
                    rg.index,
                    pp.as_ref(),
                )
            );
            if pruned {
                if let Some(ref c) = bloom.rg_bloom_pruned {
                    c.add(1);
                }
                return Ok(None);
            }
        }

        // Build the per-segment correctness bitmap once, shared across all chunks
        // of the segment via Arc<OnceLock>.
        if let (Some(bitmap_lock), Some(provider)) =
            (self.correctness_bitmap_lock.as_ref(), self.correctness_provider.as_ref())
        {
            let (seg_start, seg_end) = self.segment_doc_range;
            let drained = Self::fill_segment_cell(
                bitmap_lock,
                &self.delegated_backend_collector_factory,
                self.context_id,
                provider.key(),
                self.writer_generation,
                seg_start,
                seg_end,
                &self.ffm_collector_calls,
            );

            // A correctness predicate has NO DataFusion fallback, so a failed
            // drain MUST fail the query — treating it as "no matches" would
            // silently drop rows. Propagate the cached error.
            let full_bm = drained
                .as_ref()
                .map_err(|e| format!("correctness bitmap drain failed: {}", e))?;

            // Bitmap is segment-relative (bit 0 = doc seg_start); convert to RG-relative.
            let mut candidates = Self::rg_slice(full_bm, rg, seg_start, min_doc, max_doc);
            // Apply page-pruning AND for FullRange / TightenOuterBounds parity.
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
            return Self::finish_candidates(candidates, rg, t);
        }

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

        // Opportunistic peer consultation for performance-delegated leaves. The peer
        // bitmap is computed ONCE per (annotation_id × segment) on first access and
        // shared across all partitions — eliminating repeated scorer creation.
        // TODO: consult ALL performance leaves whose gate fires and AND their bitsets;
        // today we consult the first (lowest annotation_id) leaf only.
        if !self.performance_provider_locks.is_empty()
            && should_consult_lucene(&page_ranges, rg, PEER_CONSULT_SELECTIVITY_THRESHOLD)
        {
            let annotation_id = *self
                .performance_provider_locks
                .keys()
                .min()
                .expect("performance_provider_locks is non-empty (just checked)");
            let provider_lock = self
                .performance_provider_locks
                .get(&annotation_id)
                .expect("annotation_id in performance_provider_locks");
            let provider = provider_lock.get_or_init(|| {
                create_provider(self.context_id, annotation_id)
                    .expect("create_provider FFM upcall failed")
            });

            // A peer is optional pruning: on a successful drain, AND its RG-relative
            // slice into candidates; on a failed drain, leave candidates as the
            // page-pruned universe and let DataFusion's FilterExec evaluate the
            // retained predicate (safe — peers keep `original`).
            match self.peer_bitmap_cache.get(&annotation_id) {
                // Cache ON: build the peer bitmap ONCE per segment, shared across all
                // partitions, then slice this RG's window from it.
                Some(bitmap_lock) => {
                    let (seg_start, seg_end) = self.segment_doc_range;
                    match Self::fill_segment_cell(
                        bitmap_lock,
                        &self.delegated_backend_collector_factory,
                        self.context_id,
                        provider.key(),
                        self.writer_generation,
                        seg_start,
                        seg_end,
                        &self.ffm_collector_calls,
                    ) {
                        Ok(full_bm) => candidates &= Self::rg_slice(full_bm, rg, seg_start, min_doc, max_doc),
                        Err(e) => log_debug!("[scf-rust] peer segment drain failed (DF will filter): {}", e),
                    }
                }
                // Cache OFF (`bitmap_cache_enabled=false`): build the peer bitmap per
                // row group — delegation still runs, just rebuilt each RG instead of
                // shared. This keeps the kill-switch a true fallback to the legacy
                // path rather than silently disabling peer delegation entirely.
                None => {
                    match Self::drain_segment_range(
                        &self.delegated_backend_collector_factory,
                        self.context_id,
                        provider.key(),
                        self.writer_generation,
                        min_doc,
                        max_doc,
                        &self.ffm_collector_calls,
                    ) {
                        // `drain_segment_range` returns a bitmap relative to `min_doc`,
                        // so slice with `seg_start = min_doc` to rebase to RG-relative.
                        Ok(peer_bm) => candidates &= Self::rg_slice(&peer_bm, rg, min_doc, min_doc, max_doc),
                        Err(e) => log_debug!("[scf-rust] peer per-RG drain failed (DF will filter): {}", e),
                    }
                }
            }
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
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), Arc::new(HashMap::new()), None, None, 0, (0, 8), Arc::new(FfmDelegatedBackendCollectorFactory), 0, None, None);

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
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), Arc::new(HashMap::new()), None, None, 0, (0, 8), Arc::new(FfmDelegatedBackendCollectorFactory), 0, None, None);
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
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), Arc::new(HashMap::new()), None, None, 0, (0, 8), Arc::new(FfmDelegatedBackendCollectorFactory), 0, None, None);
        assert!(eval.needs_row_mask());
    }

    #[test]
    fn empty_match_returns_none() {
        let collector = Arc::new(StubCollector { docs: vec![] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), Arc::new(HashMap::new()), None, None, 0, (0, 8), Arc::new(FfmDelegatedBackendCollectorFactory), 0, None, None);
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
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), Arc::new(HashMap::new()), None, None, 0, (0, 8), Arc::new(FfmDelegatedBackendCollectorFactory), 0, None, None);

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
    fn stats_prune_tree_skips_rg_when_false() {
        let collector = Arc::new(StubCollector {
            docs: vec![0, 3, 7],
        }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let spt = StatsPruneTree {
            rg_can_match: vec![false],
            children: vec![],
        };
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), Arc::new(HashMap::new()), None, None, 0, (0, 8), Arc::new(FfmDelegatedBackendCollectorFactory), 0, None, Some(spt));
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        assert!(eval.prefetch_rg(&rg, 0, 8).unwrap().is_none());
    }

    #[test]
    fn stats_prune_tree_allows_rg_when_true() {
        let collector = Arc::new(StubCollector {
            docs: vec![0, 3, 7],
        }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let spt = StatsPruneTree {
            rg_can_match: vec![true],
            children: vec![],
        };
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), Arc::new(HashMap::new()), None, None, 0, (0, 8), Arc::new(FfmDelegatedBackendCollectorFactory), 0, None, Some(spt));
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("should have matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    #[test]
    fn stats_prune_tree_none_does_not_prune() {
        let collector = Arc::new(StubCollector {
            docs: vec![1, 5],
        }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(Some(collector), pruner, None, None, None, None, CollectorCallStrategy::FullRange, Arc::new(HashMap::new()), Arc::new(HashMap::new()), None, None, 0, (0, 8), Arc::new(FfmDelegatedBackendCollectorFactory), 0, None, None);
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("should have matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![1u32, 5]);
    }

    // Keep the `fmt` import used
    #[allow(dead_code)]
    fn _use(_: &dyn fmt::Debug) {}

    // ── correctness bitmap cache tests ───────────────────────────────────

    /// Counts how many times the factory builds a collector. Used to assert
    /// the bitmap cache hits across multiple `prefetch_rg` calls on the same
    /// segment (factory invoked exactly once).
    #[derive(Debug)]
    struct CountingFactory {
        docs: Vec<i32>,
        builds: std::sync::atomic::AtomicUsize,
    }

    impl DelegatedBackendCollectorFactory for CountingFactory {
        fn create(
            &self,
            _context_id: i64,
            _provider_key: i32,
            _writer_generation: i64,
            _doc_min: i32,
            _doc_max: i32,
        ) -> Result<Arc<dyn RowGroupDocsCollector>, String> {
            self.builds
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Arc::new(StubCollector {
                docs: self.docs.clone(),
            }) as Arc<dyn RowGroupDocsCollector>)
        }
    }

    /// Construct an evaluator that uses the bitmap-cache path (correctness path).
    fn make_cache_eval(
        factory: Arc<CountingFactory>,
        cache_lock: SegmentBitmapCell,
        seg_doc_range: (i32, i32),
    ) -> SingleCollectorEvaluator {
        // Provider with key=0 — the factory ignores it.
        let provider = Arc::new(ProviderHandle::new_for_test(0));
        SingleCollectorEvaluator::new(
            None,                              // collector (legacy path disabled)
            minimal_page_pruner(),
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            Some(cache_lock),                  // ← bitmap-cache path enabled
            Some(provider),
            0,
            seg_doc_range,
            factory as Arc<dyn DelegatedBackendCollectorFactory>,
            0,
            None, // bloom_config
            None, // stats_prune_tree
        )
    }

    #[test]
    fn cache_path_returns_same_docs_as_legacy_collector() {
        let factory = Arc::new(CountingFactory {
            docs: vec![0, 3, 7],
            builds: std::sync::atomic::AtomicUsize::new(0),
        });
        let cache_lock = Arc::new(OnceLock::new());
        let eval = make_cache_eval(Arc::clone(&factory), Arc::clone(&cache_lock), (0, 8));

        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
        assert_eq!(factory.builds.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn cache_path_is_built_once_across_calls() {
        // Two evaluators share the same OnceLock (same segment, different chunks).
        // Factory must be invoked exactly once across both prefetch_rg calls.
        let factory = Arc::new(CountingFactory {
            docs: vec![0, 3, 7],
            builds: std::sync::atomic::AtomicUsize::new(0),
        });
        let cache_lock = Arc::new(OnceLock::new());

        let eval1 = make_cache_eval(Arc::clone(&factory), Arc::clone(&cache_lock), (0, 8));
        let eval2 = make_cache_eval(Arc::clone(&factory), Arc::clone(&cache_lock), (0, 8));

        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let _ = eval1.prefetch_rg(&rg, 0, 8).unwrap();
        let _ = eval2.prefetch_rg(&rg, 0, 8).unwrap();

        assert_eq!(
            factory.builds.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "bitmap cache should mint the collector exactly once per segment"
        );
    }

    #[test]
    fn cache_path_slices_across_two_rgs_of_same_segment() {
        // Segment doc range [0, 16), 2 RGs of 8 rows each. Factory inserts
        // doc IDs 1, 9, 14 into the segment-relative bitmap; we verify
        // each RG sees only its slice.
        let factory = Arc::new(CountingFactory {
            docs: vec![1, 9, 14],
            builds: std::sync::atomic::AtomicUsize::new(0),
        });
        let cache_lock = Arc::new(OnceLock::new());
        let eval = make_cache_eval(Arc::clone(&factory), Arc::clone(&cache_lock), (0, 16));

        // RG 0 covers global docs [0, 8) → expect doc 1 → RG-relative offset 1.
        let rg0 = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let p0 = eval.prefetch_rg(&rg0, 0, 8).unwrap().expect("rg0 has matches");
        let got0: Vec<u32> = p0.candidates.iter().collect();
        assert_eq!(got0, vec![1u32]);

        // RG 1 covers global docs [8, 16) → expect docs 9, 14 → RG-relative 1, 6.
        // NOTE: minimal_page_pruner builds a 1-RG parquet so num_rows=8 here too.
        let rg1 = RowGroupInfo { index: 0, first_row: 8, num_rows: 8 };
        let p1 = eval.prefetch_rg(&rg1, 8, 16).unwrap().expect("rg1 has matches");
        let got1: Vec<u32> = p1.candidates.iter().collect();
        assert_eq!(got1, vec![1u32, 6]);

        assert_eq!(factory.builds.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// Factory whose collector creation always fails — models a transient
    /// FFM/Lucene error during the segment drain.
    #[derive(Debug)]
    struct FailingFactory;

    impl DelegatedBackendCollectorFactory for FailingFactory {
        fn create(
            &self,
            _context_id: i64,
            _provider_key: i32,
            _writer_generation: i64,
            _doc_min: i32,
            _doc_max: i32,
        ) -> Result<Arc<dyn RowGroupDocsCollector>, String> {
            Err("simulated FFM drain failure".to_string())
        }
    }

    /// Fix #1: a failed correctness drain MUST surface as `Err` (failing the
    /// query), never as a silently-empty RG — there is no DataFusion fallback
    /// for a correctness predicate, so dropping the RG would lose rows.
    #[test]
    fn correctness_drain_failure_propagates_as_err() {
        let provider = Arc::new(ProviderHandle::new_for_test(0));
        let eval = SingleCollectorEvaluator::new(
            None,
            minimal_page_pruner(),
            None, None, None, None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            Some(Arc::new(OnceLock::new())), // correctness cell
            Some(provider),
            0,
            (0, 8),
            Arc::new(FailingFactory),
            0,
            None, // bloom_config
            None, // stats_prune_tree
        );
        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let result = eval.prefetch_rg(&rg, 0, 8);
        assert!(result.is_err(), "correctness drain failure must propagate as Err, got {:?}", result.is_ok());
    }

    /// Fix #1 (peer side): a failed peer drain is NOT fatal — peers keep their
    /// `original` for DataFusion, so the RG survives with the full page-pruned
    /// candidate set (no pruning applied) rather than being dropped or erroring.
    #[test]
    fn peer_drain_failure_keeps_candidates_and_does_not_err() {
        let annotation_id = 1;
        let provider_locks: HashMap<i32, Arc<OnceLock<ProviderHandle>>> = {
            let mut m = HashMap::new();
            let lock = Arc::new(OnceLock::new());
            lock.set(ProviderHandle::new_for_test(0)).ok();
            m.insert(annotation_id, lock);
            m
        };
        let peer_cache: HashMap<i32, SegmentBitmapCell> = {
            let mut m = HashMap::new();
            m.insert(annotation_id, Arc::new(OnceLock::new()));
            m
        };
        let eval = SingleCollectorEvaluator::new(
            None,
            minimal_page_pruner(),
            None, None, None, None,
            CollectorCallStrategy::FullRange,
            Arc::new(provider_locks),
            Arc::new(peer_cache),
            None, // no correctness leaf — performance-only
            None,
            0,
            (0, 8),
            Arc::new(FailingFactory),
            0,
            None, // bloom_config
            None, // stats_prune_tree
        );
        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let prefetched = eval
            .prefetch_rg(&rg, 0, 8)
            .expect("peer drain failure must not error")
            .expect("RG must survive with full candidate set");
        // No pruning applied → all 8 rows remain candidates for DataFusion.
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, (0u32..8).collect::<Vec<_>>());
    }

    /// The other half of Fix #1: a correctness drain that SUCCEEDS but matched
    /// zero rows is `Ok(empty)`, not `Err`. It must skip the RG (`Ok(None)`),
    /// distinct from the failure case above which errors.
    #[test]
    fn correctness_empty_match_skips_rg_without_error() {
        let factory = Arc::new(CountingFactory {
            docs: vec![], // drained fine, matched nothing
            builds: std::sync::atomic::AtomicUsize::new(0),
        });
        let eval = make_cache_eval(Arc::clone(&factory), Arc::new(OnceLock::new()), (0, 8));
        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        assert!(eval.prefetch_rg(&rg, 0, 8).unwrap().is_none());
        assert_eq!(factory.builds.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// SegmentBitmapCache (Fix #3): the same key returns the same shared cell, so
    /// every partition/chunk of a segment drains it at most once.
    #[test]
    fn segment_bitmap_cache_returns_same_cell_per_key() {
        let cache: SegmentBitmapCache<i64> = SegmentBitmapCache::new();
        let a = cache.cell(7);
        let b = cache.cell(7);
        let c = cache.cell(9);
        assert!(Arc::ptr_eq(&a, &b), "same key must yield the same cell");
        assert!(!Arc::ptr_eq(&a, &c), "different keys must yield different cells");
    }

    /// Build a performance-peer evaluator (no correctness leaf). `cache_on`
    /// mirrors `bitmap_cache_enabled`: when true the peer cache is populated
    /// (per-segment shared bitmap); when false it is empty (cache OFF → per-RG
    /// peer build). The factory inserts `docs` as the peer match-set.
    fn make_peer_eval(factory: Arc<CountingFactory>, cache_on: bool) -> SingleCollectorEvaluator {
        let annotation_id = 1;
        let provider_locks: HashMap<i32, Arc<OnceLock<ProviderHandle>>> = {
            let mut m = HashMap::new();
            let lock = Arc::new(OnceLock::new());
            lock.set(ProviderHandle::new_for_test(0)).ok();
            m.insert(annotation_id, lock);
            m
        };
        let peer_cache: HashMap<i32, SegmentBitmapCell> = if cache_on {
            let mut m = HashMap::new();
            m.insert(annotation_id, Arc::new(OnceLock::new()));
            m
        } else {
            HashMap::new()
        };
        SingleCollectorEvaluator::new(
            None,
            minimal_page_pruner(),
            None, None, None, None,
            CollectorCallStrategy::FullRange,
            Arc::new(provider_locks),
            Arc::new(peer_cache),
            None, // no correctness leaf — performance-only
            None,
            0,
            (0, 8),
            factory as Arc<dyn DelegatedBackendCollectorFactory>,
            0,
            None, // bloom_config
            None, // stats_prune_tree
        )
    }

    /// Kill-switch OFF (empty peer cache) must STILL delegate the peer to Lucene
    /// — per row group — and prune, not silently fall through to "all rows".
    /// Regression guard: a previous version skipped the peer entirely when the
    /// cache was off, turning the flag into "disable peer delegation".
    #[test]
    fn peer_cache_off_still_delegates_per_rg() {
        let factory = Arc::new(CountingFactory {
            docs: vec![2, 5],
            builds: std::sync::atomic::AtomicUsize::new(0),
        });
        let eval = make_peer_eval(Arc::clone(&factory), /*cache_on=*/ false);
        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let prefetched = eval
            .prefetch_rg(&rg, 0, 8)
            .unwrap()
            .expect("peer delegation should narrow to the matched rows");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        // Pruned to exactly the peer match-set — proves Lucene ran (not the
        // page-pruned universe of all 8 rows).
        assert_eq!(got, vec![2u32, 5]);
        assert_eq!(
            factory.builds.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "cache OFF builds the peer collector per RG (one RG here = one build)"
        );
    }

    /// Kill-switch ON narrows to the same rows via the per-segment cached path.
    /// Pairs with `peer_cache_off_still_delegates_per_rg`: both arms delegate
    /// and produce identical results, which is what makes a cache A/B fair.
    #[test]
    fn peer_cache_on_narrows_to_same_rows() {
        let factory = Arc::new(CountingFactory {
            docs: vec![2, 5],
            builds: std::sync::atomic::AtomicUsize::new(0),
        });
        let eval = make_peer_eval(Arc::clone(&factory), /*cache_on=*/ true);
        let rg = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let prefetched = eval
            .prefetch_rg(&rg, 0, 8)
            .unwrap()
            .expect("peer delegation should narrow to the matched rows");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![2u32, 5]);
        assert_eq!(factory.builds.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
