/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `BitmapTreeEvaluator` — the default [`TreeEvaluator`] implementation.
//!
//! # Two-stage evaluation
//!
//! The tree is evaluated in two stages per row group:
//!
//! 1. **Candidate stage** (`prefetch`) — builds a *superset* candidate set of
//!    doc IDs for the RG. Works entirely in the RoaringBitmap domain:
//!    compact, cheap intersections, O(set-bits) operations. The stage walks
//!    the tree once, producing:
//!      - a top-level `RoaringBitmap` of candidate doc IDs (superset of the
//!        exact match set — this is what decides which parquet rows to read);
//!      - a side-table of per-leaf bitmaps, keyed by Collector leaf identity.
//!
//!    Collector leaves ask an external [`LeafBitmapSource`] for their bitmap
//!    (today that means an FFM upcall to the Java-side index). Predicate
//!    leaves use parquet page statistics via the caller's [`PagePruner`].
//!    The reason this is a superset, not the exact answer: predicate bitmaps
//!    come from page-level stats and are inherently coarse (pages are
//!    supersets of the rows that actually match the predicate).
//!
//! 2. **Refinement stage** (`on_batch`) — runs per record batch, after
//!    parquet delivered the decoded rows. Walks the same tree using Arrow
//!    `BooleanArray` kernels (`and_kleene`, `or_kleene`, `not`, cmp ops) to
//!    produce the *exact* per-row answer. Collector leaves look up their
//!    Phase 1 bitmap from the side-table and slice it to batch coordinates;
//!    Predicate leaves re-evaluate the comparison on actual column data.
//!
//! Why two stages and not one: Phase 1's bitmap-domain work decides *which
//! parquet rows to read at all* — for a selective query over a large RG,
//! we read only the few pages that could possibly match. Phase 2 then
//! filters those rows down to the exact answer. One-stage evaluation would
//! either read the whole RG (wasteful) or trust the coarse superset
//! (wrong, since predicate stats are supersets).
//!
//! # Child ordering
//!
//! The candidate stage sorts AND/OR children by [`subtree_cost`] before
//! walking (cheap-first), which lets a narrow Predicate leaf — or a
//! Predicate-dominated nested subtree — short-circuit a whole AND group
//! before any expensive Collector leaf work. The refinement stage walks
//! children in their *original* tree order, which is fine because Arrow
//! kernels don't short-circuit internally and leaf identity is by
//! `Arc::as_ptr`, not DFS position. See [`subtree_cost`] and the
//! `collect_collector_leaves` doc for the identity mechanism that lets
//! these two orderings coexist safely.
//!
//! Plus [`CollectorLeafBitmaps`] — the default [`LeafBitmapSource`] impl that
//! expands index-backed `RowGroupDocsCollector` output into RoaringBitmaps.
//! A different `LeafBitmapSource` could back Collector leaves by parquet
//! stats, external bitmap stores, or anything else implementing the trait.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, BooleanArray};
use datafusion::arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use datafusion::arrow::compute::{and_kleene as and, not, or_kleene as or};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
use roaring::RoaringBitmap;

use super::{LeafBitmapSource, RgEvalContext, TreeEvaluator, TreePrefetch};
use crate::indexed_table::bool_tree::ResolvedNode;
use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner};
use crate::indexed_table::row_selection::{packed_bits_to_boolean_array, PositionMap};
use datafusion::physical_optimizer::pruning::PruningPredicate;

/// In-process Rust `TreeEvaluator`. Stateless — all per-RG state lives in the
/// `TreePrefetch` value threaded through `RowGroupBitsetSource`.
pub struct BitmapTreeEvaluator;

impl TreeEvaluator for BitmapTreeEvaluator {
    fn prefetch(
        &self,
        tree: &ResolvedNode,
        ctx: &RgEvalContext,
        leaves: &dyn LeafBitmapSource,
        page_pruner: &PagePruner,
        pruning_predicates: &HashMap<usize, Arc<PruningPredicate>>,
        page_prune_metrics: Option<&PagePruneMetrics>,
    ) -> Result<TreePrefetch, String> {
        let mut per_leaf = Vec::new();
        let mut dfs_counter = 0usize;
        // Root call passes `under_all_and_path = true` — root's (empty)
        // ancestor chain is trivially all-AND, so if the root short-circuits
        // to empty, the candidate set is empty and refinement won't run.
        let candidates = prefetch_node(
            tree,
            ctx,
            leaves,
            page_pruner,
            pruning_predicates,
            page_prune_metrics,
            &mut dfs_counter,
            &mut per_leaf,
            /* under_all_and_path */ true,
        )?;
        Ok(TreePrefetch {
            candidates,
            per_leaf,
            min_doc: ctx.min_doc,
        })
    }

    fn on_batch(
        &self,
        tree: &ResolvedNode,
        state: &TreePrefetch,
        batch: &RecordBatch,
        rg_first_row: i64,
        position_map: &PositionMap,
        batch_offset: usize,
        batch_len: usize,
    ) -> Result<BooleanArray, String> {
        on_batch_node(
            tree,
            state,
            batch,
            rg_first_row,
            position_map,
            batch_offset,
            batch_len,
        )
    }
}

// Candidate stage: Filters the parquet data with candidate superset [ page pruning + lucene bitset ]
//                   [ either via filter exec or filter pushdown ] tree walker
//
// Walks the resolved tree to produce the top-level superset RoaringBitmap
// plus the per-leaf bitmap side-table.
//
// The `dfs` counter tracks the caller's position in a depth-first traversal.
// It's used only to assign a stable `leaf_dfs_index` to each leaf so a
// `LeafBitmapSource` implementation can identify which leaf it's being asked
// about. We advance `dfs` on every leaf whether we actually evaluate it or
// not (see the short-circuit branches in AND/OR) so downstream walkers that
// reproduce the DFS order (`collect_collector_leaves`, `skip_dfs`) stay in
// sync with this one.
//
// Note: the stored per-leaf bitmap entries use `Arc::as_ptr(collector)` as
// the key, not `leaf_dfs_index`. DFS position changes between
// `prefetch_node` (which sorts children by cost) and `on_batch_node` (which
// walks in original order), but `Arc` identity is stable across both. See
// the refinement-stage walker for the lookup.
//
// The `under_all_and_path` flag tracks whether every ancestor (up to root)
// is an AND node. When true, an empty candidate result here propagates all
// the way up — `TreeBitsetSource::prefetch_rg` returns `None`, the RG is
// skipped entirely, and the refinement stage never runs. In that case we
// can drop Collector bitmap materialisation in short-circuited branches
// (no one will look them up). When false, some ancestor is OR or NOT,
// which can recover from an empty subtree — refinement may still run and
// will need the bitmaps in `out`, so we materialise them defensively.
//
// Propagation rule:
//   - Root call: `under_all_and_path = true` (no ancestors).
//   - Recurse into an AND child: pass the flag unchanged.
//   - Recurse into an OR or NOT child: pass `false`.
// The universe-saturation short-circuit in OR is NOT affected — saturation
// produces a non-empty candidate set, so the RG is always read and
// refinement always runs. Bitmaps must be materialised regardless.

fn prefetch_node(
    node: &ResolvedNode,
    ctx: &RgEvalContext,
    leaves: &dyn LeafBitmapSource,
    page_pruner: &PagePruner,
    pruning_predicates: &HashMap<usize, Arc<PruningPredicate>>,
    page_prune_metrics: Option<&PagePruneMetrics>,
    dfs: &mut usize,
    out: &mut Vec<(usize, RoaringBitmap)>,
    under_all_and_path: bool,
) -> Result<RoaringBitmap, String> {
    match node {
        ResolvedNode::And(children) => {
            let mut indices: Vec<usize> = (0..children.len()).collect();
            indices.sort_by_key(|&i| subtree_cost(&children[i], ctx, page_pruner, pruning_predicates));

            let mut result_bitmap: Option<RoaringBitmap> = None;
            let mut ranges: Option<Vec<(i32, i32)>> = ctx.collector_call_ranges.clone();
            for &i in &indices {
                let child_ctx = if ranges != ctx.collector_call_ranges {
                    RgEvalContext {
                        collector_call_ranges: ranges.clone(),
                        ..ctx.clone()
                    }
                } else {
                    ctx.clone()
                };
                let child_bitmap = prefetch_node(
                    &children[i],
                    &child_ctx,
                    leaves,
                    page_pruner,
                    pruning_predicates,
                    page_prune_metrics,
                    dfs,
                    out,
                    under_all_and_path, // AND preserves the all-AND path
                )?;
                result_bitmap = Some(match result_bitmap {
                    None => child_bitmap,
                    Some(mut a) => {
                        a &= &child_bitmap;
                        a
                    }
                });

                // Tighten collector call ranges from the accumulator bitmap,
                // intersected with inherited ranges so nested ANDs never
                // widen beyond what the parent already narrowed to.
                if let Some(ref bm) = result_bitmap {
                    if !bm.is_empty() {
                        let new = ranges_from_bitmap(bm, ctx);
                        ranges = Some(match ranges {
                            Some(inherited) => intersect_range_lists(&inherited, &new),
                            None => new,
                        });
                    }
                }

                // Short circuit case
                // 1. Skip if subtree only consists of AND [ since all bits are not set here, no need to evaluate ]
                // 2. Collect if subtree is mixed with OR/NOT, which can produce set bits and recover
                if result_bitmap.as_ref().unwrap().is_empty() {
                    // Remaining children still need to advance `dfs` so leaf
                    // IDs remain stable.
                    for &j in indices.iter().skip_while(|&&x| x != i).skip(1) {
                        if under_all_and_path {
                            // Empty propagates to root → RG skipped → bitmaps
                            // unused. Just advance the counter.
                            skip_dfs(&children[j], dfs);
                        } else {
                            // OR/NOT ancestor can recover
                            collect_collector_leaves(&children[j], ctx, leaves, dfs, out)?;
                        }
                    }
                    break;
                }
            }
            Ok(result_bitmap.unwrap_or_default())
        }
        ResolvedNode::Or(children) => {
            let mut indices: Vec<usize> = (0..children.len()).collect();

            // sort the children by cost to prune children better
            indices.sort_by_key(|&i| subtree_cost(&children[i], ctx, page_pruner, pruning_predicates));
            let total_docs = (ctx.max_doc - ctx.min_doc) as u64;

            let mut result_bitmap = RoaringBitmap::new();
            for (arr_index, &val) in indices.iter().enumerate() {
                let filtered_bitmap = prefetch_node(
                    &children[val],
                    ctx,
                    leaves,
                    page_pruner,
                    pruning_predicates,
                    page_prune_metrics,
                    dfs,
                    out,
                    // OR breaks all-AND propagation for its subtree.
                    false,
                )?;
                result_bitmap |= &filtered_bitmap;

                // Short circuit case
                if result_bitmap.len() >= total_docs {
                    // If all values match, then result bitmap length will be
                    // same as total docs. In that case, we don't have to evaluate predicates
                    // since we know all bits are matching.
                    // We simply call collectors so that the bitsets are appended to 'out'
                    for &j in indices.iter().skip(arr_index + 1) {
                        collect_collector_leaves(&children[j], ctx, leaves, dfs, out)?;
                    }
                    break;
                }
            }
            Ok(result_bitmap)
        }
        // Mainly needed for collectors, predicate expressions are inversed where possible
        // and wouldn't usually hit this
        ResolvedNode::Not(child) => {
            // NOT breaks all-AND propagation — inverting empty gives universe,
            // which is non-empty, so the RG will be read and refinement will
            // run. Materialise bitmaps below.
            let child_bm = prefetch_node(
                child,
                ctx,
                leaves,
                page_pruner,
                pruning_predicates,
                page_prune_metrics,
                dfs,
                out,
                /* under_all_and_path */ false,
            )?;
            // Candidate-stage is a superset. Inverting a superset does
            // NOT yield a superset of the true NOT — it yields a subset
            // (wrong for candidate stage).
            // Two cases :
            // 1. Predicate : If the child's bitmap was computed
            // from anything non-exact (Predicate leaves use coarse page
            // stats), fall back to the full universe and let refinement pick
            // the exact set.
            // 2. Collector : If the child contained only Collector leaves
            // (exact bitmaps), inversion is safe.
            if subtree_has_predicate(child) {
                let mut universe = RoaringBitmap::new();
                let span = (ctx.max_doc - ctx.min_doc) as u32;
                universe.insert_range(0..span);
                Ok(universe)
            } else {
                let mut universe = RoaringBitmap::new();
                let span = (ctx.max_doc - ctx.min_doc) as u32;
                universe.insert_range(0..span);
                universe -= &child_bm;
                Ok(universe)
            }
        }
        ResolvedNode::Collector { collector, .. } => {
            let leaf_idx = *dfs;
            *dfs += 1;
            let key = Arc::as_ptr(collector) as *const () as usize;
            let bm = leaves.leaf_bitmap(node, leaf_idx, ctx)?;
            out.push((key, bm.clone()));
            Ok(bm)
        }
        ResolvedNode::Predicate(expr) => {
            let leaf_idx = *dfs;
            *dfs += 1;
            let _ = leaf_idx; // predicate leaves don't need per-leaf storage
            Ok(predicate_page_bitmap(
                expr,
                ctx,
                page_pruner,
                pruning_predicates,
                page_prune_metrics,
            ))
        }
        ResolvedNode::DelegationPossible { .. } => {
            // Invariant: DelegationPossible must never appear under OR or NOT.
            // The Java planner narrows performance peers off any AnnotatedPredicate
            // sitting under an OR/NOT ancestor — the leaf becomes single-viable on
            // the operator's backend and the resolver unwraps it natively (plain
            // Predicate on the Rust side). Reaching this arm means that contract
            // was violated: a planner bug, not an evaluator gap. Fail loud so the
            // bug is visible instead of silently giving wrong-shape candidates.
            unimplemented!(
                "invariant violation: DelegationPossible reached the Tree-path evaluator. \
                 Planner must drop performance peers under OR/NOT before fragment conversion."
            )
        }
    }
}

/// Walk a subtree without combining into the parent accumulator, but still
/// populate the per-leaf bitmap side-table that the refinement stage will
/// read from later.
///
/// Called when the parent's candidate-stage accumulator has short-circuited
/// (AND reached empty, OR reached the universe) and so this subtree's
/// contribution is no longer needed for the candidate superset. We can't
/// just skip the subtree entirely though — the refinement stage walks the
/// whole tree and will look up every Collector leaf's bitmap in the
/// side-table. Missing entries there would panic at refinement time. So we
/// still materialise the bitmaps (but skip the expensive AND/OR combine and
/// skip the page-pruner work for Predicate leaves, since those never enter
/// the side-table).
///
/// Also advances the `dfs` counter in lockstep with the main walker so
/// downstream leaf_dfs_index assignments stay consistent.
fn collect_collector_leaves(
    node: &ResolvedNode,
    ctx: &RgEvalContext,
    leaves: &dyn LeafBitmapSource,
    dfs: &mut usize,
    out: &mut Vec<(usize, RoaringBitmap)>,
) -> Result<(), String> {
    match node {
        ResolvedNode::And(children) | ResolvedNode::Or(children) => {
            for child in children {
                collect_collector_leaves(child, ctx, leaves, dfs, out)?;
            }
        }
        ResolvedNode::Not(child) => collect_collector_leaves(child, ctx, leaves, dfs, out)?,
        ResolvedNode::Collector { collector, .. } => {
            let leaf_idx = *dfs;
            *dfs += 1;
            let key = Arc::as_ptr(collector) as *const () as usize;
            let bm = leaves.leaf_bitmap(node, leaf_idx, ctx)?;
            out.push((key, bm));
        }
        ResolvedNode::Predicate(_) => {
            *dfs += 1;
        }
        ResolvedNode::DelegationPossible { .. } => {
            // Invariant: see prefetch_node arm. Same contract: planner must
            // strip performance peers under OR/NOT.
            unimplemented!(
                "invariant violation: DelegationPossible reached collect_collector_leaves. \
                 Planner must drop performance peers under OR/NOT before fragment conversion."
            )
        }
    }
    Ok(())
}

/// Advance the `dfs` counter over a subtree without doing any bitmap work.
/// Used at an AND short-circuit point when we know the whole candidate
/// result will be empty and the RG will be skipped — there's no refinement
/// stage to prepare bitmaps for, so we only need to keep leaf-ID assignment
/// stable. See the `under_all_and_path` handling in `prefetch_node`.
fn skip_dfs(node: &ResolvedNode, dfs: &mut usize) {
    match node {
        ResolvedNode::And(children) | ResolvedNode::Or(children) => {
            for c in children {
                skip_dfs(c, dfs);
            }
        }
        ResolvedNode::Not(child) => skip_dfs(child, dfs),
        ResolvedNode::Collector { .. } | ResolvedNode::Predicate(_) => *dfs += 1,
        ResolvedNode::DelegationPossible { .. } => {
            // Invariant: see prefetch_node arm. Same contract.
            unimplemented!(
                "invariant violation: DelegationPossible reached skip_dfs. \
                 Planner must drop performance peers under OR/NOT before fragment conversion."
            )
        }
    }
}

fn predicate_page_bitmap(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    ctx: &RgEvalContext,
    page_pruner: &PagePruner,
    pruning_predicates: &HashMap<usize, Arc<PruningPredicate>>,
    page_prune_metrics: Option<&PagePruneMetrics>,
) -> RoaringBitmap {
    // Identity key: same Arc used at build time is the same Arc we see here.
    let key = Arc::as_ptr(expr) as *const () as usize;
    let pruning_predicate = match pruning_predicates.get(&key) {
        Some(pp) => pp,
        // No pruning predicate available (schema mismatch at build time, or
        // `always_true`): conservative fallback is "every row in scope is a
        // candidate" — return a full-range bitmap so AND/OR with other
        // leaves combines correctly.
        None => {
            let mut bm = RoaringBitmap::new();
            bm.insert_range(0u32..((ctx.max_doc - ctx.min_doc) as u32));
            return bm;
        }
    };
    // Evaluate page pruning for this single conjunct.
    let selection = page_pruner.prune_rg(pruning_predicate, ctx.rg_idx, page_prune_metrics);
    let mut bm = RoaringBitmap::new();
    match selection {
        Some(sel) => {
            // The selection is RG-relative. Translate to min_doc-relative
            // space (the bitmap the tree evaluator walks over). Each
            // kept selector covers a contiguous row range; insert it as
            // a range in one call. `RoaringBitmap::insert_range` handles
            // a full page of rows in O(log n) per container (or O(1) for
            // full-container runs), vs. the naive one-bit-at-a-time loop
            // which is O(rows_kept) with per-insert overhead.
            let rg_offset = (ctx.rg_first_row as i32 - ctx.min_doc) as i64;
            let span = (ctx.max_doc - ctx.min_doc) as i64;
            let mut rg_pos: i64 = 0;
            for s in sel.iter() {
                if !s.skip {
                    // Selector covers [rg_pos, rg_pos + s.row_count) in
                    // RG-relative space; shift into scope-relative space
                    // and clamp to [0, span) since the scope bitmap only
                    // covers rows inside [min_doc, max_doc).
                    let start_rel = rg_pos + rg_offset;
                    let end_rel = start_rel + s.row_count as i64;
                    let lo = start_rel.max(0);
                    let hi = end_rel.min(span);
                    if lo < hi {
                        bm.insert_range(lo as u32..hi as u32);
                    }
                }
                rg_pos += s.row_count as i64;
            }
        }
        None => {
            // No pruning applicable (no page index or column missing) —
            // conservative: every row in scope is a candidate.
            bm.insert_range(0u32..((ctx.max_doc - ctx.min_doc) as u32));
        }
    }
    bm
}

/// Derive collector call ranges from a bitmap based on the strategy in `ctx`.
///
/// - `FullRange`: returns `[(min_doc, max_doc)]` (no narrowing).
/// - `TightenOuterBounds`: returns `[(first_set + min_doc, last_set + min_doc + 1)]`.
/// - `PageRangeSplit`: returns contiguous runs of set bits as absolute ranges.
fn ranges_from_bitmap(bm: &RoaringBitmap, ctx: &RgEvalContext) -> Vec<(i32, i32)> {
    use super::CollectorCallStrategy;
    match ctx.collector_strategy {
        CollectorCallStrategy::FullRange => vec![(ctx.min_doc, ctx.max_doc)],
        CollectorCallStrategy::TightenOuterBounds => {
            match (bm.min(), bm.max()) {
                (Some(lo), Some(hi)) => {
                    vec![(ctx.min_doc + lo as i32, ctx.min_doc + hi as i32 + 1)]
                }
                _ => vec![(ctx.min_doc, ctx.max_doc)],
            }
        }
        CollectorCallStrategy::PageRangeSplit => {
            // Extract contiguous runs of set bits as absolute doc ranges.
            let mut ranges = Vec::new();
            let mut iter = bm.iter();
            let Some(first) = iter.next() else {
                return vec![];
            };
            let mut run_start = first;
            let mut run_end = first; // inclusive
            for bit in iter {
                if bit == run_end + 1 {
                    run_end = bit;
                } else {
                    ranges.push((
                        ctx.min_doc + run_start as i32,
                        ctx.min_doc + run_end as i32 + 1,
                    ));
                    run_start = bit;
                    run_end = bit;
                }
            }
            ranges.push((
                ctx.min_doc + run_start as i32,
                ctx.min_doc + run_end as i32 + 1,
            ));
            ranges
        }
    }
}

/// Intersect two sorted, non-overlapping range lists. Both inputs are
/// `(start, end)` half-open intervals in absolute doc-id space. The
/// result contains only the portions where both lists overlap.
fn intersect_range_lists(a: &[(i32, i32)], b: &[(i32, i32)]) -> Vec<(i32, i32)> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let lo = a[i].0.max(b[j].0);
        let hi = a[i].1.min(b[j].1);
        if lo < hi {
            out.push((lo, hi));
        }
        if a[i].1 < b[j].1 {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

/// Cost weights used by `subtree_cost` to order AND/OR children in the
/// candidate stage. Tuning knobs, not a hard contract.
///
/// - Predicate = 1: page-stats-only, no I/O, a handful of array lookups.
/// - Collector = 10: requires materialising an actual doc-id bitset over
///   FFM — posting-list iteration on the Java side, bitset transport +
///   RoaringBitmap expansion on the Rust side. Relative cost is
///   workload-dependent (Lucene posting iteration is fast for narrow
///   queries, slower for wide ones) so "10" is a conservative default.
///   Tune (or make config-driven) if profiling shows it matters.

/// Internal scale factor for cost computation. All costs are multiplied
/// by this so integer division preserves meaningful selectivity differences.
/// A predicate keeping 1/8 pages costs `1000 * 1/8 = 125` vs one keeping
/// 5/8 pages at `1000 * 5/8 = 625`. Collector cost `10 * 1000 = 10_000`.
pub(crate) const COST_SCALE: u32 = 1000;

/// Recursively compute the accumulated cost of a subtree for
/// candidate-stage ordering.
///
/// For `Predicate` leaves with a matching `PruningPredicate`, the cost
/// is weighted by page-level selectivity: `cost_predicate * COST_SCALE * (surviving_pages / total_pages)`.
/// More selective predicates (fewer surviving pages) get lower cost and
/// are evaluated first in AND nodes, producing tighter ranges for
/// subsequent Collector siblings.
///
/// Falls back to the static `cost_predicate * COST_SCALE` when page stats are
/// unavailable (no page index, expression not translatable, etc.).
///
/// `Not` passes through to its child; `And`/`Or` sum their children.
pub(crate) fn subtree_cost(
    node: &ResolvedNode,
    ctx: &RgEvalContext,
    page_pruner: &PagePruner,
    pruning_predicates: &HashMap<usize, Arc<PruningPredicate>>,
) -> u32 {
    match node {
        ResolvedNode::Predicate(expr) => {
            let base = ctx.cost_predicate * COST_SCALE;
            let key = Arc::as_ptr(expr) as *const () as usize;
            if let Some(pp) = pruning_predicates.get(&key) {
                if let Some(page_counts) = page_pruner.page_row_counts(ctx.rg_idx) {
                    let total = page_counts.len() as u32;
                    if total > 0 {
                        if let Some(sel) = page_pruner.prune_rg(pp, ctx.rg_idx, None) {
                            // Count pages with at least one selected row.
                            // RowSelection merges adjacent same-decision
                            // selectors, so we walk the selection and map
                            // row offsets back to page boundaries.
                            let mut kept_pages = 0u32;
                            let mut row_offset = 0usize;
                            let mut page_idx = 0usize;
                            let mut page_start = 0usize;
                            let mut page_end = page_counts[0];
                            for s in sel.iter() {
                                let seg_end = row_offset + s.row_count;
                                while page_idx < total as usize {
                                    if !s.skip && row_offset < page_end && seg_end > page_start {
                                        kept_pages += 1;
                                        // Advance to next page to avoid double-counting.
                                        page_idx += 1;
                                        if page_idx < total as usize {
                                            page_start = page_end;
                                            page_end += page_counts[page_idx];
                                        }
                                    } else if page_end <= seg_end {
                                        page_idx += 1;
                                        if page_idx < total as usize {
                                            page_start = page_end;
                                            page_end += page_counts[page_idx];
                                        }
                                    } else {
                                        break;
                                    }
                                }
                                row_offset = seg_end;
                            }
                            return (base * kept_pages + total - 1) / total;
                        }
                    }
                }
            }
            base
        }
        ResolvedNode::Collector { .. } => ctx.cost_collector * COST_SCALE,
        ResolvedNode::Not(child) => subtree_cost(child, ctx, page_pruner, pruning_predicates),
        ResolvedNode::And(children) | ResolvedNode::Or(children) => children
            .iter()
            .map(|c| subtree_cost(c, ctx, page_pruner, pruning_predicates))
            .sum(),
        ResolvedNode::DelegationPossible { .. } => {
            // Invariant: see prefetch_node arm. Same contract.
            unimplemented!(
                "invariant violation: DelegationPossible reached subtree_cost. \
                 Planner must drop performance peers under OR/NOT before fragment conversion."
            )
        }
    }
}

/// True if `node` contains any `Predicate` leaf (transitively).
/// Used to decide if a `Not(child)` Phase 1 result is safe to invert via
/// universe subtraction. See the `Not` arm in `prefetch_node` for why.
fn subtree_has_predicate(node: &ResolvedNode) -> bool {
    match node {
        ResolvedNode::Predicate(_) => true,
        ResolvedNode::Collector { .. } => false,
        ResolvedNode::And(cs) | ResolvedNode::Or(cs) => cs.iter().any(subtree_has_predicate),
        ResolvedNode::Not(c) => subtree_has_predicate(c),
        ResolvedNode::DelegationPossible { .. } => {
            // Invariant: see prefetch_node arm. Same contract.
            unimplemented!(
                "invariant violation: DelegationPossible reached subtree_has_predicate. \
                 Planner must drop performance peers under OR/NOT before fragment conversion."
            )
        }
    }
}

// Refinement stage [ Post Decode, where we need the actual decoded values to evaluate ] : tree walker
//
// Runs after parquet has delivered a decoded record batch. Walks the same
// tree again — in original order this time, not cost-sorted — and combines
// per-row BooleanArrays using Arrow's 3VL-safe `and_kleene`/`or_kleene`/`not`
// kernels. Collector leaves read their cached bitmap from the side-table
// (keyed by `Arc::as_ptr(collector)`, which is stable across the cost-sort
// used in the candidate stage). Predicate leaves evaluate the actual
// comparison against the batch's column data. Short-circuits on
// definitively-all-false for AND and definitively-all-true for OR
// (Kleene-safe: both check `null_count == 0` first).

fn on_batch_node(
    node: &ResolvedNode,
    state: &TreePrefetch,
    batch: &RecordBatch,
    rg_first_row: i64,
    position_map: &PositionMap,
    batch_offset: usize,
    batch_len: usize,
) -> Result<BooleanArray, String> {
    match node {
        ResolvedNode::And(children) => {
            let mut optional_result_bitmap: Option<BooleanArray> = None;
            for child in children {
                let child_bitmap = on_batch_node(
                    child,
                    state,
                    batch,
                    rg_first_row,
                    position_map,
                    batch_offset,
                    batch_len,
                )?;
                optional_result_bitmap = Some(match optional_result_bitmap {
                    None => child_bitmap,
                    Some(result_bitmap) => {
                        and(&result_bitmap, &child_bitmap).map_err(|e| e.to_string())?
                    }
                });
                // Short-circuit: if every row is definitively false
                // (no nulls, zero trues), any further `FALSE AND x` is
                // still FALSE in SQL 3VL. Safe to stop.
                if let Some(ref result_bitmap) = optional_result_bitmap {
                    if result_bitmap.null_count() == 0 && result_bitmap.true_count() == 0 {
                        return Ok(result_bitmap.clone());
                    }
                }
            }
            Ok(optional_result_bitmap.unwrap_or_else(|| all_true(batch_len)))
        }
        ResolvedNode::Or(children) => {
            let mut optional_result_bitmap: Option<BooleanArray> = None;
            for child in children {
                let child_bitmap = on_batch_node(
                    child,
                    state,
                    batch,
                    rg_first_row,
                    position_map,
                    batch_offset,
                    batch_len,
                )?;
                optional_result_bitmap = Some(match optional_result_bitmap {
                    None => child_bitmap,
                    Some(result_bitmap) => {
                        or(&result_bitmap, &child_bitmap).map_err(|e| e.to_string())?
                    }
                });
                // Short-circuit: if every row is definitively true
                // (no nulls, zero falses), any further `TRUE OR x` is
                // still TRUE in SQL 3VL. Safe to stop.
                if let Some(ref result_bitmap) = optional_result_bitmap {
                    if result_bitmap.null_count() == 0 && result_bitmap.false_count() == 0 {
                        return Ok(result_bitmap.clone());
                    }
                }
            }
            Ok(optional_result_bitmap.unwrap_or_else(|| all_false(batch_len)))
        }
        ResolvedNode::Not(child) => {
            let child_bitmap = on_batch_node(
                child,
                state,
                batch,
                rg_first_row,
                position_map,
                batch_offset,
                batch_len,
            )?;
            not(&child_bitmap).map_err(|e| e.to_string())
        }
        ResolvedNode::Collector { collector, .. } => {
            let key = Arc::as_ptr(collector) as *const () as usize;
            let bitmap = state
                .per_leaf
                .iter()
                .find_map(|(i, bm)| if *i == key { Some(bm) } else { None })
                .ok_or_else(|| format!("Phase 2: leaf bitmap missing for key {:#x}", key))?;
            Ok(bitmap_to_batch_mask(
                bitmap,
                state.min_doc,
                rg_first_row,
                position_map,
                batch_offset,
                batch_len,
            ))
        }
        ResolvedNode::Predicate(expr) => predicate_to_batch_mask(batch, expr),
        ResolvedNode::DelegationPossible { .. } => {
            // Invariant: see prefetch_node arm. Same contract.
            unimplemented!(
                "invariant violation: DelegationPossible reached on_batch_node. \
                 Planner must drop performance peers under OR/NOT before fragment conversion."
            )
        }
    }
}

/// Translate a Collector leaf's bitmap (in min-doc-relative coordinates) to
/// a per-batch `BooleanArray`.
///
/// With block-granular RowSelection the delivered rows are a compacted
/// subset of the RG, not a contiguous span. `position_map` lets us recover
/// which RG-relative position each delivered row came from; from there we
/// compute the absolute doc id and look it up in `bm`.
///
/// `batch_offset` is the delivered-row index of the first row in this
/// batch; delivered row `batch_offset + i` maps to RG position
/// `position_map.rg_position(batch_offset + i)`.
fn bitmap_to_batch_mask(
    bm: &RoaringBitmap,
    min_doc: i32,
    rg_first_row: i64,
    position_map: &PositionMap,
    batch_offset: usize,
    batch_len: usize,
) -> BooleanArray {
    // Convert batch-row index -> min-doc-relative bitmap index.
    // delivered row i -> rg_position(batch_offset + i) -> abs_doc -> bit.
    //
    // For Identity position map, rg_position(k) == k, so the mapping is
    // linear: delivered row i -> bit (rg_first_row + batch_offset + i) - min_doc.
    // We iterate the set bits of `bm` within the batch's coverage and
    // translate back, instead of per-row `bm.contains()`.
    let words = batch_len.div_ceil(64);
    let mut out = vec![0u64; words];

    let anchor = rg_first_row - min_doc as i64; // rg_pos -> bit: rg_pos + anchor
    match position_map {
        PositionMap::Identity { .. } => {
            // delivered row i -> rg_pos = batch_offset + i -> bit = batch_offset + i + anchor.
            // Enumerate set bits in `bm` within [anchor + batch_offset, anchor + batch_offset + batch_len).
            let lo = (batch_offset as i64 + anchor).max(0);
            let hi = (batch_offset as i64 + anchor + batch_len as i64).max(0);
            if hi > 0 && lo <= u32::MAX as i64 {
                let lo_u32 = lo as u32;
                let hi_u32 = hi.min(u32::MAX as i64) as u32;
                for b in bm.range(lo_u32..hi_u32) {
                    // delivered index = bit - anchor - batch_offset
                    let delivered = (b as i64 - anchor - batch_offset as i64) as usize;
                    if delivered < batch_len {
                        out[delivered >> 6] |= 1u64 << (delivered & 63);
                    }
                }
            }
        }
        PositionMap::Bitmap { .. } | PositionMap::Runs { .. } => {
            // General case — fall back to per-row lookup but use packed-bit
            // assembly so we avoid the Vec<bool> + BooleanArray::from copy.
            for i in 0..batch_len {
                let rg_pos = match position_map.rg_position(batch_offset + i) {
                    Some(p) => p,
                    None => continue,
                };
                let abs_doc = rg_first_row + rg_pos as i64;
                let bit = abs_doc - min_doc as i64;
                if bit >= 0 && bit <= u32::MAX as i64 && bm.contains(bit as u32) {
                    out[i >> 6] |= 1u64 << (i & 63);
                }
            }
        }
    }
    packed_bits_to_boolean_array(out, batch_len)
}

// Evaluate an arbitrary boolean `PhysicalExpr` against a batch; return
// the resulting per-row mask. Uses DataFusion's expression evaluator —
// handles all operators, IN, IS NULL, LIKE, arithmetic, CAST, UDFs etc.
//
// Fast-path for `col OP literal` comparisons: skip the expression walk
// and dispatch directly to the arrow kernel. This is the dominant shape
// in production (Predicate leaves are almost always simple comparisons)
// and the kernel call is 3–5x cheaper than going through
// `BinaryExpr::evaluate` + column/literal dispatch.
fn predicate_to_batch_mask(
    batch: &RecordBatch,
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
) -> Result<BooleanArray, String> {
    // Fast-path: detect `col OP literal` and call the kernel directly.
    if let Some(bin) = expr.as_any().downcast_ref::<BinaryExpr>() {
        if let (Some(col), Some(lit)) = (
            bin.left().as_any().downcast_ref::<PhysColumn>(),
            bin.right().as_any().downcast_ref::<Literal>(),
        ) {
            match batch.column_by_name(col.name()) {
                None => {
                    // Column absent from batch schema: SQL UNKNOWN.
                    let nulls: Vec<Option<bool>> = (0..batch.num_rows()).map(|_| None).collect();
                    return Ok(BooleanArray::from(nulls));
                }
                Some(col_arr) => {
                    let scalar = lit.value().to_scalar().map_err(|e| e.to_string())?;
                    let kernel_result = match *bin.op() {
                        Operator::Eq => eq(col_arr, &scalar),
                        Operator::NotEq => neq(col_arr, &scalar),
                        Operator::Lt => lt(col_arr, &scalar),
                        Operator::LtEq => lt_eq(col_arr, &scalar),
                        Operator::Gt => gt(col_arr, &scalar),
                        Operator::GtEq => gt_eq(col_arr, &scalar),
                        _ => {
                            // Non-comparison op (And/Or/Plus/...) — fall
                            // through to the general evaluator path.
                            return evaluate_via_df(batch, expr);
                        }
                    };
                    return kernel_result.map_err(|e| e.to_string());
                }
            }
        }
    }
    evaluate_via_df(batch, expr)
}

/// General-case evaluator — `expr.evaluate(batch)` with schema-drift
/// safety check. Used for non-`col OP literal` shapes (IN, IS NULL,
/// arithmetic, NOT-wrapped, …).
fn evaluate_via_df(
    batch: &RecordBatch,
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
) -> Result<BooleanArray, String> {
    // Schema drift: if the expression references any column not present
    // in this batch's schema, SQL semantics demand UNKNOWN for every
    // row. Return an all-NULL BooleanArray so kleene AND/OR combine
    // correctly and `filter_record_batch` drops the UNKNOWN rows.
    let batch_schema = batch.schema();
    let referenced = datafusion::physical_expr::utils::collect_columns(expr);
    for col in &referenced {
        if batch_schema.index_of(col.name()).is_err() {
            let nulls: Vec<Option<bool>> = (0..batch.num_rows()).map(|_| None).collect();
            return Ok(BooleanArray::from(nulls));
        }
    }

    // Reseat Column indices to the batch's projected schema by name.
    // Substrait-decoded predicates carry indices into the full table schema;
    // delivered batches are projected (only predicate columns), so the
    // indices need to be remapped before `evaluate(batch)` reads them.
    let remapped = super::remap_expr_to_batch(expr, batch)?;

    let result = remapped
        .evaluate(batch)
        .map_err(|e| format!("expr.evaluate: {}", e))?;
    match result {
        ColumnarValue::Array(arr) => {
            if arr.data_type() == &datafusion::arrow::datatypes::DataType::Boolean {
                Ok(arr.as_boolean().clone())
            } else {
                Err(format!(
                    "predicate evaluation produced non-boolean array: {:?}",
                    arr.data_type()
                ))
            }
        }
        ColumnarValue::Scalar(sv) => match sv {
            datafusion::common::ScalarValue::Boolean(Some(b)) => {
                Ok(BooleanArray::from(vec![b; batch.num_rows()]))
            }
            datafusion::common::ScalarValue::Boolean(None) => {
                let nulls: Vec<Option<bool>> = (0..batch.num_rows()).map(|_| None).collect();
                Ok(BooleanArray::from(nulls))
            }
            other => Err(format!(
                "predicate evaluation produced non-boolean scalar: {:?}",
                other
            )),
        },
    }
}

fn all_true(n: usize) -> BooleanArray {
    BooleanArray::from(vec![true; n])
}
fn all_false(n: usize) -> BooleanArray {
    BooleanArray::from(vec![false; n])
}

/// CollectorLeafBitmaps — default LeafBitmapSource for today's flow
///
/// Expands index-backed `RowGroupDocsCollector` output into RoaringBitmaps.
/// Pulls the collector directly off the `ResolvedNode::Collector` passed to
/// it — no separate indexing required, so this impl is fully stateless.
pub struct CollectorLeafBitmaps {
    /// Incremented once per call to [`Self::leaf_bitmap`] — one FFM
    /// round-trip to Java per Collector leaf per RG. `None` for tests
    /// that don't care about metrics.
    pub ffm_collector_calls: Option<datafusion::physical_plan::metrics::Count>,
}

impl CollectorLeafBitmaps {
    /// Construct a `CollectorLeafBitmaps` with no metrics.
    pub fn without_metrics() -> Self {
        Self {
            ffm_collector_calls: None,
        }
    }
}

impl LeafBitmapSource for CollectorLeafBitmaps {
    fn leaf_bitmap(
        &self,
        collector_node: &ResolvedNode,
        _leaf_dfs_index: usize, // This is not used in this implementation
        ctx: &RgEvalContext,
    ) -> Result<RoaringBitmap, String> {
        let collector = match collector_node {
            ResolvedNode::Collector { collector, .. } => collector,
            _ => {
                return Err("CollectorLeafBitmaps: non-Collector node passed to leaf_bitmap".into())
            }
        };
        // Use the narrowed call ranges if available (set by AND evaluator
        // after earlier children shrink the candidate set). Each range
        // produces one FFM call; results are merged into one bitmap in
        // min_doc-relative coordinates.
        // Use narrowed call ranges if available (set by AND evaluator).
        let call_ranges = ctx
            .collector_call_ranges
            .clone()
            .unwrap_or_else(|| vec![(ctx.min_doc, ctx.max_doc)]);

        let mut result_bitmap = RoaringBitmap::new();
        for (call_min, call_max) in &call_ranges {
            let bitset = collector.collect_packed_u64_bitset(*call_min, *call_max)?;
            if let Some(ref c) = self.ffm_collector_calls {
                c.add(1);
            }
            let offset = (*call_min - ctx.min_doc) as u32;
            let num_docs = (*call_max - *call_min) as u32;
            let bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(bitset.as_ptr() as *const u8, bitset.len() * 8)
            };
            let mut chunk = RoaringBitmap::from_lsb0_bytes(offset, bytes);
            let upper = offset + num_docs;
            if upper < u32::MAX {
                chunk.remove_range(upper..);
            }
            result_bitmap |= chunk;
        }
        Ok(result_bitmap)
    }
}

// ══════════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::bool_tree::ResolvedNode;
    use crate::indexed_table::index::RowGroupDocsCollector;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
    };
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
    use std::collections::{HashMap, HashSet};

    /// Deterministic bitmap source for tests.
    struct FixedLeafBitmaps {
        bitmaps: Vec<RoaringBitmap>,
    }
    impl LeafBitmapSource for FixedLeafBitmaps {
        fn leaf_bitmap(
            &self,
            _tree: &ResolvedNode,
            idx: usize,
            _ctx: &RgEvalContext,
        ) -> Result<RoaringBitmap, String> {
            Ok(self.bitmaps[idx].clone())
        }
    }

    fn test_ctx() -> RgEvalContext {
        RgEvalContext {
            rg_idx: 0,
            rg_first_row: 0,
            rg_num_rows: 16,
            min_doc: 0,
            max_doc: 16,
            cost_predicate: 1,
            cost_collector: 10,
            collector_call_ranges: None,
            collector_strategy: super::super::CollectorCallStrategy::TightenOuterBounds,
        }
    }

    fn empty_pruner() -> PagePruner {
        // Build a minimal PagePruner with no filters — candidate_row_ids_for_filter
        // won't be called since we use no Predicate nodes in these tests.
        // We need a schema + metadata. Simplest: write a tiny parquet and load it.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![0i32; 16]))],
        )
        .unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        PagePruner::new(meta.schema(), meta.metadata().clone())
    }

    fn collector_leaf(idx: usize) -> ResolvedNode {
        // Use a no-op collector — LeafBitmapSource supplies bitmaps, not the collector
        #[derive(Debug)]
        struct Dummy;
        impl RowGroupDocsCollector for Dummy {
            fn collect_packed_u64_bitset(&self, _: i32, _: i32) -> Result<Vec<u64>, String> {
                Ok(vec![])
            }
        }
        let _ = idx;
        ResolvedNode::Collector {
            provider_key: 0,
            collector: Arc::new(Dummy),
        }
    }

    fn bm(docs: &[u32]) -> RoaringBitmap {
        let mut r = RoaringBitmap::new();
        for &d in docs {
            r.insert(d);
        }
        r
    }

    #[test]
    fn and_of_two_collectors_intersects_phase1() {
        let tree = ResolvedNode::And(vec![collector_leaf(0), collector_leaf(1)]);
        let leaves = FixedLeafBitmaps {
            bitmaps: vec![bm(&[1, 2, 3, 4]), bm(&[3, 4, 5])],
        };
        let pruner = empty_pruner();
        let result = BitmapTreeEvaluator
            .prefetch(&tree, &test_ctx(), &leaves, &pruner, &HashMap::new(), None)
            .unwrap();
        assert_eq!(result.candidates, bm(&[3, 4]));
        assert_eq!(result.per_leaf.len(), 2);
    }

    #[test]
    fn or_of_two_collectors_unions_phase1() {
        let tree = ResolvedNode::Or(vec![collector_leaf(0), collector_leaf(1)]);
        let leaves = FixedLeafBitmaps {
            bitmaps: vec![bm(&[1, 2]), bm(&[2, 3])],
        };
        let pruner = empty_pruner();
        let result = BitmapTreeEvaluator
            .prefetch(&tree, &test_ctx(), &leaves, &pruner, &HashMap::new(), None)
            .unwrap();
        assert_eq!(result.candidates, bm(&[1, 2, 3]));
    }

    #[test]
    fn not_collector_complements_against_universe() {
        let tree = ResolvedNode::Not(Box::new(collector_leaf(0)));
        let leaves = FixedLeafBitmaps {
            bitmaps: vec![bm(&[0, 1, 2])],
        };
        let pruner = empty_pruner();
        let result = BitmapTreeEvaluator
            .prefetch(&tree, &test_ctx(), &leaves, &pruner, &HashMap::new(), None)
            .unwrap();
        // Universe is [0, 16). Minus {0,1,2} = {3..15}
        let expected: RoaringBitmap = (3u32..16).collect();
        assert_eq!(result.candidates, expected);
    }

    #[test]
    fn phase2_collector_uses_cached_bitmap() {
        let tree = collector_leaf(0);
        let leaves = FixedLeafBitmaps {
            bitmaps: vec![bm(&[1, 3, 5])],
        };
        let pruner = empty_pruner();
        let state = BitmapTreeEvaluator
            .prefetch(&tree, &test_ctx(), &leaves, &pruner, &HashMap::new(), None)
            .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0i32; 8]))]).unwrap();
        // Batch covers docs [0, 8). Match bitmap {1,3,5}.
        // Full-scan position map: delivered index == RG position.
        let pm = PositionMap::from_selection(&RowSelection::from(vec![RowSelector::select(8)]));
        let mask = BitmapTreeEvaluator
            .on_batch(&tree, &state, &batch, 0, &pm, 0, 8)
            .unwrap();
        let expected =
            BooleanArray::from(vec![false, true, false, true, false, true, false, false]);
        assert_eq!(mask, expected);
    }

    /// Identity position map over `rg_num_rows`. Delivered index == RG
    /// position — matches the pre-block-granular full-scan behaviour and
    /// keeps the per-test expected values unchanged.
    fn identity_pm(rg_num_rows: usize) -> PositionMap {
        PositionMap::from_selection(&RowSelection::from(vec![RowSelector::select(rg_num_rows)]))
    }

    #[test]
    fn bitmap_to_batch_mask_anchors_correctly() {
        // min_doc = 100, bitmap has {1, 5} (min_doc-relative).
        // rg_first_row = 100, batch starts at offset 0, length 8.
        // For each row i: rg_pos = i, abs_doc = 100 + i,
        // rel_doc = abs_doc - min_doc = i. bits set at i=1 and i=5.
        let bm = {
            let mut b = RoaringBitmap::new();
            b.insert(1);
            b.insert(5);
            b
        };
        let pm = identity_pm(8);
        let mask = bitmap_to_batch_mask(
            &bm, /*min_doc*/ 100, /*rg_first_row*/ 100, &pm, 0, 8,
        );
        let got: Vec<bool> = (0..8).map(|i| mask.value(i)).collect();
        assert_eq!(
            got,
            vec![false, true, false, false, false, true, false, false]
        );
    }

    #[test]
    fn bitmap_to_batch_mask_handles_batch_offset_within_rg() {
        // min_doc = 0, rg_first_row = 0, batch starts at rg offset 4, len 4.
        // Identity position map over rg_num_rows=16.
        // For row i: rg_pos = 4 + i, abs_doc = 4 + i, rel = 4 + i.
        // Bitmap bits {0, 5, 9} → rows where (4+i) in {0,5,9} → i=1, i=5 (out of range), so only i=1.
        let bm = {
            let mut b = RoaringBitmap::new();
            b.insert(0);
            b.insert(5);
            b.insert(9);
            b
        };
        let pm = identity_pm(16);
        let mask = bitmap_to_batch_mask(&bm, 0, 0, &pm, 4, 4);
        let got: Vec<bool> = (0..4).map(|i| mask.value(i)).collect();
        assert_eq!(got, vec![false, true, false, false]);
    }

    #[test]
    fn bitmap_to_batch_mask_empty_bitmap_produces_all_false() {
        let bm = RoaringBitmap::new();
        let pm = identity_pm(5);
        let mask = bitmap_to_batch_mask(&bm, 0, 0, &pm, 0, 5);
        assert_eq!(mask.true_count(), 0);
        assert_eq!(mask.len(), 5);
    }

    #[test]
    fn bitmap_to_batch_mask_zero_length_batch() {
        let bm = {
            let mut b = RoaringBitmap::new();
            b.insert(0);
            b
        };
        let pm = identity_pm(1);
        let mask = bitmap_to_batch_mask(&bm, 0, 0, &pm, 0, 0);
        assert_eq!(mask.len(), 0);
    }

    #[test]
    fn bitmap_to_batch_mask_respects_position_map() {
        // RG has 10 rows; RowSelection selects rows [0..3] and [7..10],
        // skipping [3..7]. Delivered rows = 6 (3 + 3).
        // delivered idx 0 → rg_pos 0
        // delivered idx 1 → rg_pos 1
        // delivered idx 2 → rg_pos 2
        // delivered idx 3 → rg_pos 7
        // delivered idx 4 → rg_pos 8
        // delivered idx 5 → rg_pos 9
        // Bitmap (min_doc-relative, min_doc = 0, rg_first_row = 0) {2, 8}.
        // Expected mask per delivered index: [F,F,T,F,T,F]
        let sel = RowSelection::from(vec![
            RowSelector::select(3),
            RowSelector::skip(4),
            RowSelector::select(3),
        ]);
        let pm = PositionMap::from_selection(&sel);
        let bm = {
            let mut b = RoaringBitmap::new();
            b.insert(2);
            b.insert(8);
            b
        };
        let mask = bitmap_to_batch_mask(&bm, 0, 0, &pm, 0, 6);
        let got: Vec<bool> = (0..6).map(|i| mask.value(i)).collect();
        assert_eq!(got, vec![false, false, true, false, true, false]);
    }

    // ── Phase 2 short-circuit ─────────────────────────────────────────

    /// Evaluator that counts how many times its `leaf_bitmap` was called —
    /// used to observe Phase 2 short-circuit by wrapping predicate leaves as
    /// collectors whose bitmaps are the "predicate mask".
    ///
    /// We can't directly inspect Phase 2 calls since they go through
    /// `on_batch_node`, but we can observe them by making Phase 2 evaluation
    /// visible via side effect on a counting LeafBitmapSource.
    ///
    /// For Phase 2 specifically, `ResolvedNode::Collector` uses
    /// `state.per_leaf` lookup (cached Phase 1 bitmaps), not the
    /// LeafBitmapSource. So short-circuit observation has to be at the
    /// `on_batch_node` level — we use a custom node tree and assert on the
    /// resulting mask shape with deliberately-wrong siblings.
    ///
    /// The strategy: construct AND(all_false_child, poison_child) where
    /// `poison_child` would `panic!` if evaluated. If the test passes,
    /// short-circuit prevented evaluation of the poison child.

    /// Build a ResolvedNode::Collector whose cached Phase 1 bitmap is `bm`.
    fn cached_collector(bm: RoaringBitmap) -> (ResolvedNode, (usize, RoaringBitmap)) {
        #[derive(Debug)]
        struct Poison;
        impl RowGroupDocsCollector for Poison {
            fn collect_packed_u64_bitset(&self, _: i32, _: i32) -> Result<Vec<u64>, String> {
                unreachable!("Phase 2 must not call collect")
            }
        }
        let collector: Arc<dyn RowGroupDocsCollector> = Arc::new(Poison);
        let key = Arc::as_ptr(&collector) as *const () as usize;
        let node = ResolvedNode::Collector {
            provider_key: 0,
            collector,
        };
        (node, (key, bm))
    }

    #[test]
    fn phase2_and_short_circuits_on_all_false() {
        // AND(all_false_leaf, poison_leaf). The poison leaf's bitmap is
        // absent from `state.per_leaf`, so evaluating it would error with
        // "leaf bitmap missing". If short-circuit fires, poison is skipped
        // and we get the zero mask without erroring.
        let (false_leaf, false_entry) = cached_collector(RoaringBitmap::new());
        let (poison_leaf, _poison_entry) = cached_collector({
            let mut b = RoaringBitmap::new();
            b.insert(999); // doesn't matter — shouldn't be looked up
            b
        });

        let tree = ResolvedNode::And(vec![false_leaf, poison_leaf]);
        // Register ONLY the false leaf. If short-circuit misfires, Phase 2
        // will try to look up `poison_entry` and fail with "leaf bitmap missing".
        let state = TreePrefetch {
            candidates: RoaringBitmap::new(),
            per_leaf: vec![false_entry],
            min_doc: 0,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0i32; 4]))]).unwrap();

        let mask = on_batch_node(&tree, &state, &batch, 0, &identity_pm(4), 0, 4)
            .expect("AND should short-circuit on all-false acc, skipping poison leaf");
        assert_eq!(mask.true_count(), 0);
    }

    #[test]
    fn phase2_or_short_circuits_on_all_true() {
        // OR(all_true_leaf, poison_leaf). Same setup as AND case but inverted.
        let (true_leaf, true_entry) = cached_collector({
            let mut b = RoaringBitmap::new();
            b.insert_range(0..4);
            b
        });
        let (poison_leaf, _) = cached_collector({
            let mut b = RoaringBitmap::new();
            b.insert(999);
            b
        });

        let tree = ResolvedNode::Or(vec![true_leaf, poison_leaf]);
        let state = TreePrefetch {
            candidates: RoaringBitmap::new(),
            per_leaf: vec![true_entry],
            min_doc: 0,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0i32; 4]))]).unwrap();

        let mask = on_batch_node(&tree, &state, &batch, 0, &identity_pm(4), 0, 4)
            .expect("OR should short-circuit on all-true acc, skipping poison leaf");
        assert_eq!(mask.true_count(), 4);
    }

    // ── Candidate-stage skip of bitmap materialization ────────────────
    //
    // The tests below prove that when an AND short-circuits at a point
    // where every ancestor is AND (so the whole candidate set is doomed
    // to be empty and the RG will be skipped), the walker does NOT ask
    // the `LeafBitmapSource` for the remaining Collector leaves' bitmaps.
    // The symmetric case (AND under OR/NOT) must still materialise.

    /// LeafBitmapSource returning bitmaps by DFS index, panicking on forbidden indices.
    struct PoisonLeafBitmaps {
        allowed: HashMap<usize, RoaringBitmap>,
        forbidden: HashSet<usize>,
    }
    impl LeafBitmapSource for PoisonLeafBitmaps {
        fn leaf_bitmap(
            &self,
            _tree: &ResolvedNode,
            idx: usize,
            _ctx: &RgEvalContext,
        ) -> Result<RoaringBitmap, String> {
            if self.forbidden.contains(&idx) {
                panic!("leaf_bitmap called for forbidden leaf {}", idx);
            }
            Ok(self.allowed.get(&idx).cloned().unwrap_or_default())
        }
    }

    #[test]
    fn candidate_root_and_short_circuit_skips_forbidden_collector() {
        // Tree: AND(Collector, Collector). Root is AND.
        // - Leaves are cost-equal, stable-sort preserves input order; so
        //   DFS index 0 = first Collector, DFS index 1 = second.
        // - First returns empty → AND short-circuits.
        // - Because we're under root-AND, the whole candidate set is
        //   doomed empty and the RG will be skipped. The walker must NOT
        //   call LeafBitmapSource for the second Collector.
        let tree = ResolvedNode::And(vec![collector_leaf(0), collector_leaf(1)]);
        let mut allowed = HashMap::new();
        allowed.insert(0, RoaringBitmap::new()); // empty → trigger short-circuit
        let mut forbidden = HashSet::new();
        forbidden.insert(1); // any call for leaf 1 panics
        let leaves = PoisonLeafBitmaps { allowed, forbidden };
        let pruner = empty_pruner();

        let result = BitmapTreeEvaluator
            .prefetch(&tree, &test_ctx(), &leaves, &pruner, &HashMap::new(), None)
            .unwrap();
        assert!(result.candidates.is_empty());
    }

    #[test]
    fn candidate_and_short_circuit_under_or_still_materialises() {
        // Tree: OR(AND(empty_leaf, other_leaf), standalone_leaf).
        // Cost sort at root OR: [standalone_leaf (10), AND (20)].
        // DFS order:
        //   idx 0 = standalone_leaf (evaluated first by cost sort),
        //   idx 1 = empty_leaf (AND's first child),
        //   idx 2 = other_leaf (AND's second child).
        //
        // The AND short-circuits on idx 1 (empty). Because the path to
        // root contains an OR (not all-AND), the walker must still
        // materialise idx 2's bitmap so refinement can look it up.
        let tree = ResolvedNode::Or(vec![
            ResolvedNode::And(vec![collector_leaf(0), collector_leaf(1)]),
            collector_leaf(2),
        ]);
        let mut allowed = HashMap::new();
        allowed.insert(0, {
            let mut b = RoaringBitmap::new();
            b.insert(5);
            b
        });
        allowed.insert(1, RoaringBitmap::new()); // empty → short-circuit
        allowed.insert(2, {
            let mut b = RoaringBitmap::new();
            b.insert(7);
            b
        });
        let leaves = PoisonLeafBitmaps {
            allowed,
            forbidden: HashSet::new(),
        };
        let pruner = empty_pruner();

        let result = BitmapTreeEvaluator
            .prefetch(&tree, &test_ctx(), &leaves, &pruner, &HashMap::new(), None)
            .unwrap();
        // OR contributes {5} from standalone_leaf → non-empty candidates.
        assert!(!result.candidates.is_empty());
        // All 3 collector leaves must have per_leaf entries — AND
        // short-circuit under OR does NOT skip materialisation.
        assert_eq!(
            result.per_leaf.len(),
            3,
            "expected 3 per_leaf entries; got {}",
            result.per_leaf.len()
        );
    }

    #[test]
    fn candidate_and_short_circuit_under_not_still_materialises() {
        // Tree: NOT(AND(empty_leaf, other_leaf)).
        // Inner AND short-circuits on empty_leaf. NOT inverts empty to
        // universe → candidates non-empty → RG read → refinement will
        // look up other_leaf's bitmap.
        let tree = ResolvedNode::Not(Box::new(ResolvedNode::And(vec![
            collector_leaf(0),
            collector_leaf(1),
        ])));
        let mut allowed = HashMap::new();
        allowed.insert(0, RoaringBitmap::new()); // triggers short-circuit
        allowed.insert(1, {
            let mut b = RoaringBitmap::new();
            b.insert(9);
            b
        });
        let leaves = PoisonLeafBitmaps {
            allowed,
            forbidden: HashSet::new(),
        };
        let pruner = empty_pruner();

        let result = BitmapTreeEvaluator
            .prefetch(&tree, &test_ctx(), &leaves, &pruner, &HashMap::new(), None)
            .unwrap();
        // NOT inverts empty AND → universe.
        assert_eq!(result.candidates.len(), 16);
        // Both collector leaves materialised.
        assert_eq!(result.per_leaf.len(), 2);
    }

    // ── subtree_cost ─────────────────────────────────────────────────

    fn test_predicate_node() -> ResolvedNode {
        let left: std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            std::sync::Arc::new(PhysColumn::new("x", 0));
        let right: std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            std::sync::Arc::new(Literal::new(ScalarValue::Int32(Some(0))));
        ResolvedNode::Predicate(std::sync::Arc::new(BinaryExpr::new(
            left,
            Operator::Eq,
            right,
        )))
    }

    #[test]
    fn subtree_cost_leaf_nodes() {
        let ctx = test_ctx();
        let pruner = empty_pruner();
        let pp = HashMap::new();
        assert_eq!(
            subtree_cost(&test_predicate_node(), &ctx, &pruner, &pp),
            ctx.cost_predicate * COST_SCALE
        );
        assert_eq!(subtree_cost(&collector_leaf(0), &ctx, &pruner, &pp), ctx.cost_collector * COST_SCALE);
    }

    #[test]
    fn subtree_cost_not_passes_through() {
        let ctx = test_ctx();
        let pruner = empty_pruner();
        let pp = HashMap::new();
        let wrapped = ResolvedNode::Not(Box::new(test_predicate_node()));
        assert_eq!(subtree_cost(&wrapped, &ctx, &pruner, &pp), ctx.cost_predicate * COST_SCALE);
    }

    #[test]
    fn subtree_cost_sums_children() {
        let ctx = test_ctx();
        let pruner = empty_pruner();
        let pp = HashMap::new();
        let tree = ResolvedNode::And(vec![
            test_predicate_node(),
            test_predicate_node(),
            collector_leaf(0),
        ]);
        assert_eq!(
            subtree_cost(&tree, &ctx, &pruner, &pp),
            (2 * ctx.cost_predicate + ctx.cost_collector) * COST_SCALE
        );
    }

    #[test]
    fn subtree_cost_predicate_heavy_nested_beats_single_collector() {
        let nested = ResolvedNode::And(vec![
            test_predicate_node(),
            test_predicate_node(),
            test_predicate_node(),
        ]);
        let single_collector = collector_leaf(0);
        let ctx = test_ctx();
        let pruner = empty_pruner();
        let pp = HashMap::new();
        assert!(
            subtree_cost(&nested, &ctx, &pruner, &pp) < subtree_cost(&single_collector, &ctx, &pruner, &pp),
        );
    }

    #[test]
    fn subtree_cost_collector_heavy_nested_exceeds_single_collector() {
        let nested = ResolvedNode::And(vec![collector_leaf(0), collector_leaf(1)]);
        let single_collector = collector_leaf(0);
        let ctx = test_ctx();
        let pruner = empty_pruner();
        let pp = HashMap::new();
        assert!(subtree_cost(&nested, &ctx, &pruner, &pp) > subtree_cost(&single_collector, &ctx, &pruner, &pp));
    }

    // ── intersect_range_lists unit tests ────────────────────────────

    #[test]
    fn intersect_empty_with_anything() {
        assert_eq!(intersect_range_lists(&[], &[(0, 10)]), vec![]);
        assert_eq!(intersect_range_lists(&[(0, 10)], &[]), vec![]);
        assert_eq!(intersect_range_lists(&[], &[]), vec![]);
    }

    #[test]
    fn intersect_non_overlapping() {
        // [0,5) and [10,15) → empty
        assert_eq!(intersect_range_lists(&[(0, 5)], &[(10, 15)]), vec![]);
    }

    #[test]
    fn intersect_partial_overlap() {
        // [0,10) ∩ [5,15) → [5,10)
        assert_eq!(intersect_range_lists(&[(0, 10)], &[(5, 15)]), vec![(5, 10)]);
    }

    #[test]
    fn intersect_one_contains_other() {
        // [0,20) ∩ [5,10) → [5,10)
        assert_eq!(intersect_range_lists(&[(0, 20)], &[(5, 10)]), vec![(5, 10)]);
    }

    #[test]
    fn intersect_multiple_ranges() {
        // a: [0,5), [10,20), [30,40)
        // b: [3,12), [15,35)
        // intersections: [3,5), [10,12), [15,20), [30,35)
        let a = vec![(0, 5), (10, 20), (30, 40)];
        let b = vec![(3, 12), (15, 35)];
        assert_eq!(
            intersect_range_lists(&a, &b),
            vec![(3, 5), (10, 12), (15, 20), (30, 35)]
        );
    }

    #[test]
    fn intersect_identical() {
        let a = vec![(10, 20), (30, 40)];
        assert_eq!(intersect_range_lists(&a, &a), vec![(10, 20), (30, 40)]);
    }

    // ── ranges_from_bitmap unit tests ───────────────────────────────

    #[test]
    fn ranges_full_range_strategy() {
        let mut ctx = test_ctx();
        ctx.collector_strategy = super::super::CollectorCallStrategy::FullRange;
        let mut bm = RoaringBitmap::new();
        bm.insert_range(4..8);
        // FullRange ignores the bitmap, returns [min_doc, max_doc)
        assert_eq!(ranges_from_bitmap(&bm, &ctx), vec![(0, 16)]);
    }

    #[test]
    fn ranges_tighten_outer_bounds_strategy() {
        let mut ctx = test_ctx();
        ctx.collector_strategy = super::super::CollectorCallStrategy::TightenOuterBounds;
        let mut bm = RoaringBitmap::new();
        bm.insert_range(4..8);
        bm.insert(12);
        // TightenOuterBounds: [min_doc + bm.min(), min_doc + bm.max() + 1)
        assert_eq!(ranges_from_bitmap(&bm, &ctx), vec![(4, 13)]);
    }

    #[test]
    fn ranges_page_range_split_contiguous() {
        let mut ctx = test_ctx();
        ctx.collector_strategy = super::super::CollectorCallStrategy::PageRangeSplit;
        let mut bm = RoaringBitmap::new();
        bm.insert_range(4..8);
        // Single contiguous run → one range
        assert_eq!(ranges_from_bitmap(&bm, &ctx), vec![(4, 8)]);
    }

    #[test]
    fn ranges_page_range_split_with_gap() {
        let mut ctx = test_ctx();
        ctx.collector_strategy = super::super::CollectorCallStrategy::PageRangeSplit;
        let mut bm = RoaringBitmap::new();
        bm.insert_range(2..5);  // bits 2,3,4
        bm.insert_range(8..11); // bits 8,9,10
        bm.insert(14);          // bit 14
        // Three contiguous runs → three ranges
        assert_eq!(
            ranges_from_bitmap(&bm, &ctx),
            vec![(2, 5), (8, 11), (14, 15)]
        );
    }

    #[test]
    fn ranges_page_range_split_empty_bitmap() {
        let mut ctx = test_ctx();
        ctx.collector_strategy = super::super::CollectorCallStrategy::PageRangeSplit;
        let bm = RoaringBitmap::new();
        assert_eq!(ranges_from_bitmap(&bm, &ctx), vec![]);
    }
}
