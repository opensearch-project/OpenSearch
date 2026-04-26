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

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use datafusion::arrow::compute::{and_kleene as and, not, or_kleene as or};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use roaring::RoaringBitmap;

use super::{LeafBitmapSource, RgEvalContext, TreeEvaluator, TreePrefetch};
#[cfg(test)]
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::bool_tree::ResolvedNode;
use crate::indexed_table::page_pruner::PagePruner;

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
        batch_offset: usize,
        batch_len: usize,
    ) -> Result<BooleanArray, String> {
        // Absolute doc ID of row 0 of this batch
        let batch_first_doc = rg_first_row + batch_offset as i64;
        on_batch_node(tree, state, batch, batch_first_doc, batch_len)
    }
}

// Candidate stage: - Filters the parquet data with candidate superset [ page pruning + lucene bitset ]
//                    [ either via filter exec or filter pushdown ] tree walker
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
    dfs: &mut usize,
    out: &mut Vec<(usize, RoaringBitmap)>,
    under_all_and_path: bool,
) -> Result<RoaringBitmap, String> {
    match node {
        ResolvedNode::And(children) => {
            let mut indices: Vec<usize> = (0..children.len()).collect();
            indices.sort_by_key(|&i| subtree_cost(&children[i]));

            let mut result_bitmap: Option<RoaringBitmap> = None;
            for &i in &indices {
                let child_bitmap = prefetch_node(
                    &children[i],
                    ctx,
                    leaves,
                    page_pruner,
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
            indices.sort_by_key(|&i| subtree_cost(&children[i]));
            let total_docs = (ctx.max_doc - ctx.min_doc) as u64;

            let mut result_bitmap = RoaringBitmap::new();
            for (arr_index, &val) in indices.iter().enumerate() {
                let filtered_bitmap = prefetch_node(
                    &children[val],
                    ctx,
                    leaves,
                    page_pruner,
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
        ResolvedNode::Predicate { pred } => {
            let leaf_idx = *dfs;
            *dfs += 1;
            let _ = leaf_idx; // predicate leaves don't need per-leaf storage
            Ok(predicate_page_bitmap(
                &pred.column,
                &pred.op,
                &pred.value,
                ctx,
                page_pruner,
            ))
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
        ResolvedNode::Predicate { .. } => {
            *dfs += 1;
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
        ResolvedNode::Collector { .. } | ResolvedNode::Predicate { .. } => *dfs += 1,
    }
}

fn predicate_page_bitmap(
    column: &str,
    op: &Operator,
    value: &ScalarValue,
    ctx: &RgEvalContext,
    page_pruner: &PagePruner,
) -> RoaringBitmap {
    let candidates = page_pruner.candidate_row_ids_for_filter(
        column,
        op,
        value,
        ctx.rg_idx,
        ctx.rg_first_row,
        ctx.rg_num_rows,
    );
    let mut bm = RoaringBitmap::new();
    for doc_id in candidates {
        if doc_id >= ctx.min_doc as i64 && doc_id < ctx.max_doc as i64 {
            bm.insert((doc_id - ctx.min_doc as i64) as u32);
        }
    }
    bm
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
const COST_PREDICATE: u32 = 1;
const COST_COLLECTOR: u32 = 10;

/// Recursively compute the accumulated cost of a subtree for
/// candidate-stage ordering. Sum-of-leaves, not a tree-size metric: a
/// Nested subtree of three Predicate leaves (cost 3) ranks ahead of a
/// single Collector leaf (cost 10) — which matches the intuition that
/// "three metadata scans are faster than one index bitmap fetch."
///
/// `Not` passes through to its child; `And`/`Or` sum their children.
///
/// Unlike a static tier, this model sees *inside* nested subtrees and
/// orders them meaningfully against each other and against leaves.
fn subtree_cost(node: &ResolvedNode) -> u32 {
    match node {
        ResolvedNode::Predicate { .. } => COST_PREDICATE,
        ResolvedNode::Collector { .. } => COST_COLLECTOR,
        ResolvedNode::Not(child) => subtree_cost(child),
        ResolvedNode::And(children) | ResolvedNode::Or(children) => {
            children.iter().map(subtree_cost).sum()
        }
    }
}

/// True if `node` contains any `Predicate` leaf (transitively).
/// Used to decide if a `Not(child)` Phase 1 result is safe to invert via
/// universe subtraction. See the `Not` arm in `prefetch_node` for why.
fn subtree_has_predicate(node: &ResolvedNode) -> bool {
    match node {
        ResolvedNode::Predicate { .. } => true,
        ResolvedNode::Collector { .. } => false,
        ResolvedNode::And(cs) | ResolvedNode::Or(cs) => cs.iter().any(subtree_has_predicate),
        ResolvedNode::Not(c) => subtree_has_predicate(c),
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
    batch_first_doc: i64,
    batch_len: usize,
) -> Result<BooleanArray, String> {
    match node {
        ResolvedNode::And(children) => {
            let mut optional_result_bitmap: Option<BooleanArray> = None;
            for child in children {
                let child_bitmap = on_batch_node(child, state, batch, batch_first_doc, batch_len)?;
                optional_result_bitmap = Some(match optional_result_bitmap {
                    None => child_bitmap,
                    Some(result_bitmap) => and(&result_bitmap, &child_bitmap).map_err(|e| e.to_string())?,
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
                let child_bitmap = on_batch_node(child, state, batch, batch_first_doc, batch_len)?;
                optional_result_bitmap = Some(match optional_result_bitmap {
                    None => child_bitmap,
                    Some(result_bitmap) => or(&result_bitmap, &child_bitmap).map_err(|e| e.to_string())?,
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
            let child_bitmap = on_batch_node(child, state, batch, batch_first_doc, batch_len)?;
            not(&child_bitmap).map_err(|e| e.to_string())
        }
        ResolvedNode::Collector { collector, .. } => {
            let key = Arc::as_ptr(collector) as *const () as usize;
            let bitmap = state
                .per_leaf
                .iter()
                .find_map(|(i, bm)| if *i == key { Some(bm) } else { None })
                .ok_or_else(|| format!("Phase 2: leaf bitmap missing for key {:#x}", key))?;
            Ok(bitmap_to_batch_mask(bitmap, state.min_doc, batch_first_doc, batch_len))
        }
        ResolvedNode::Predicate { pred } => {
            predicate_to_batch_mask(batch, &pred.column, &pred.op, &pred.value)
        }
    }
}

fn bitmap_to_batch_mask(
    bm: &RoaringBitmap,
    min_doc: i32,
    batch_first_doc: i64,
    batch_len: usize,
) -> BooleanArray {
    // Write bits directly into a packed bitmap (8× less memory than Vec<bool>
    // and skips the pack-scan that BooleanArray::from(Vec<bool>) would do).
    use datafusion::arrow::array::builder::BooleanBufferBuilder;
    let mut builder = BooleanBufferBuilder::new(batch_len);
    builder.append_n(batch_len, false);
    let anchor = batch_first_doc - min_doc as i64;
    for row in 0..batch_len {
        let bit = anchor + row as i64;
        if bit >= 0 && bit <= u32::MAX as i64 && bm.contains(bit as u32) {
            builder.set_bit(row, true);
        }
    }
    BooleanArray::new(builder.finish(), None)
}

// Use arrow kernels to evaluate the parquet predicates expressions to return the resultant bitmap
fn predicate_to_batch_mask(
    batch: &RecordBatch,
    column: &str,
    op: &Operator,
    value: &ScalarValue,
) -> Result<BooleanArray, String> {
    // Column absent from this batch's schema: the parquet segment doesn't
    // carry that column at all (common when a mapping added the field after
    // the segment was written). SQL semantics: every row is UNKNOWN. Return
    // an all-NULL BooleanArray so the NULL-aware Arrow AND/OR (SQL 3VL)
    // kernels combine it correctly, and `filter_record_batch` drops the
    // UNKNOWN rows.
    let col = match batch.column_by_name(column) {
        Some(c) => c,
        None => {
            let nulls: Vec<Option<bool>> = (0..batch.num_rows()).map(|_| None).collect();
            return Ok(BooleanArray::from(nulls));
        }
    };
    let scalar_value = value
        .to_scalar()
        .map_err(|e| format!("to_scalar {}: {}", column, e))?;
    match op {
        Operator::Eq => eq(col, &scalar_value).map_err(|e| e.to_string()),
        Operator::NotEq => neq(col, &scalar_value).map_err(|e| e.to_string()),
        Operator::Lt => lt(col, &scalar_value).map_err(|e| e.to_string()),
        Operator::LtEq => lt_eq(col, &scalar_value).map_err(|e| e.to_string()),
        Operator::Gt => gt(col, &scalar_value).map_err(|e| e.to_string()),
        Operator::GtEq => gt_eq(col, &scalar_value).map_err(|e| e.to_string()),
        _ => Err(format!("unsupported predicate op {:?}", op)),
    }
}

fn all_true(n: usize) -> BooleanArray {
    BooleanArray::from(vec![true; n])
}
fn all_false(n: usize) -> BooleanArray {
    BooleanArray::from(vec![false; n])
}

// ══════════════════════════════════════════════════════════════════════
// CollectorLeafBitmaps — default LeafBitmapSource for today's flow
// ══════════════════════════════════════════════════════════════════════

/// Expands index-backed `RowGroupDocsCollector` output into RoaringBitmaps.
/// Pulls the collector directly off the `ResolvedNode::Collector` passed to
/// it — no separate indexing required, so this impl is fully stateless.
pub struct CollectorLeafBitmaps;

impl LeafBitmapSource for CollectorLeafBitmaps {
    fn leaf_bitmap(
        &self,
        collector_node: &ResolvedNode,
        _leaf_dfs_index: usize, // This is not used in this implementation
        ctx: &RgEvalContext,
    ) -> Result<RoaringBitmap, String> {
        let collector = match collector_node {
            ResolvedNode::Collector { collector, .. } => collector,
            _ => return Err("CollectorLeafBitmaps: non-Collector node passed to leaf_bitmap".into()),
        };
        let bitset = collector.collect_packed_u64_bitset(ctx.min_doc, ctx.max_doc)?;
        let mut result_bitmap = RoaringBitmap::new();
        // Iterate only set bits via trailing_zeros — sparse bitsets (typical
        // for selective queries) visit O(set_bits) instead of O(span).
        let num_docs = (ctx.max_doc - ctx.min_doc) as u32;
        for (word_idx, &word) in bitset.iter().enumerate() {
            // Fast path: whole word empty, skip all 64 bit positions at once.
            if word == 0 {
                continue;
            }
            let base = (word_idx as u32) * 64;
            let mut curr_word = word;
            while curr_word != 0 {
                // Position of the lowest set bit within the current word (0..=63).
                let bit = curr_word.trailing_zeros();
                // Absolute bit position in the bitset = word offset + in-word offset.
                let rel = base + bit;
                // Clamp: the last word may have bits set beyond (max_doc - min_doc)
                // because the bitset is 64-bit-word-aligned; those bits are padding
                // and must not enter the RoaringBitmap.
                if rel < num_docs {
                    result_bitmap.insert(rel);
                }
                // Clear that lowest set bit so the next trailing_zeros() finds
                // the next one. Classic `x & (x - 1)` trick: subtracting 1 flips
                // the lowest set bit to 0 and all lower bits to 1; AND keeps
                // only the higher bits unchanged, clearing exactly that one bit.
                curr_word &= curr_word - 1;
            }
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
    use crate::indexed_table::bool_tree::{ResolvedNode, ResolvedPredicate};
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

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
        }
    }

    fn empty_pruner() -> PagePruner {
        // Build a minimal PagePruner with no filters — candidate_row_ids_for_filter
        // won't be called since we use no Predicate nodes in these tests.
        // We need a schema + metadata. Simplest: write a tiny parquet and load it.
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![0i32; 16]))],
        )
        .unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut writer =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        PagePruner::new(meta.schema(), meta.metadata().clone(), &[])
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
            .prefetch(&tree, &test_ctx(), &leaves, &pruner)
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
            .prefetch(&tree, &test_ctx(), &leaves, &pruner)
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
            .prefetch(&tree, &test_ctx(), &leaves, &pruner)
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
            .prefetch(&tree, &test_ctx(), &leaves, &pruner)
            .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![0i32; 8]))],
        )
        .unwrap();
        // Batch covers docs [0, 8). Match bitmap {1,3,5}.
        let mask = BitmapTreeEvaluator
            .on_batch(&tree, &state, &batch, 0, 0, 8)
            .unwrap();
        let expected = BooleanArray::from(vec![
            false, true, false, true, false, true, false, false,
        ]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn bitmap_to_batch_mask_anchors_correctly() {
        // min_doc = 100, bitmap has {101, 105}. Batch starts at doc 100, length 8.
        // row i corresponds to doc (100 + i); relative bit index = i.
        // Bitmap is in min_doc-relative space, so {101, 105} means bits at
        // relative positions 1 and 5 → rows 1 and 5 in this batch.
        let bm = {
            let mut b = RoaringBitmap::new();
            b.insert(1);
            b.insert(5);
            b
        };
        let mask = bitmap_to_batch_mask(&bm, /*min_doc*/ 100, /*batch_first_doc*/ 100, 8);
        let got: Vec<bool> = (0..8).map(|i| mask.value(i)).collect();
        assert_eq!(got, vec![false, true, false, false, false, true, false, false]);
    }

    #[test]
    fn bitmap_to_batch_mask_handles_batch_offset_within_rg() {
        // min_doc = 0, batch_first_doc = 4 (batch is second half of the RG).
        // Bitmap has {0, 5, 9} in rg-relative space.
        // anchor = 4 - 0 = 4. Row i's relative bit = 4 + i.
        // Rows: 0→bit 4 (not in bm) | 1→bit 5 (yes) | 2→bit 6 (no) | 3→bit 7 (no)
        let bm = {
            let mut b = RoaringBitmap::new();
            b.insert(0);
            b.insert(5);
            b.insert(9);
            b
        };
        let mask = bitmap_to_batch_mask(&bm, 0, 4, 4);
        let got: Vec<bool> = (0..4).map(|i| mask.value(i)).collect();
        assert_eq!(got, vec![false, true, false, false]);
    }

    #[test]
    fn bitmap_to_batch_mask_empty_bitmap_produces_all_false() {
        let bm = RoaringBitmap::new();
        let mask = bitmap_to_batch_mask(&bm, 0, 0, 5);
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
        let mask = bitmap_to_batch_mask(&bm, 0, 0, 0);
        assert_eq!(mask.len(), 0);
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
        use crate::indexed_table::index::RowGroupDocsCollector;

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
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0i32; 4]))])
            .unwrap();

        let mask = on_batch_node(&tree, &state, &batch, 0, 4)
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
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0i32; 4]))])
            .unwrap();

        let mask = on_batch_node(&tree, &state, &batch, 0, 4)
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
        allowed: std::collections::HashMap<usize, RoaringBitmap>,
        forbidden: std::collections::HashSet<usize>,
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
        let mut allowed = std::collections::HashMap::new();
        allowed.insert(0, RoaringBitmap::new()); // empty → trigger short-circuit
        let mut forbidden = std::collections::HashSet::new();
        forbidden.insert(1); // any call for leaf 1 panics
        let leaves = PoisonLeafBitmaps { allowed, forbidden };
        let pruner = empty_pruner();

        let result = BitmapTreeEvaluator
            .prefetch(&tree, &test_ctx(), &leaves, &pruner)
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
        let mut allowed = std::collections::HashMap::new();
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
            forbidden: std::collections::HashSet::new(),
        };
        let pruner = empty_pruner();

        let result = BitmapTreeEvaluator
            .prefetch(&tree, &test_ctx(), &leaves, &pruner)
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
        let mut allowed = std::collections::HashMap::new();
        allowed.insert(0, RoaringBitmap::new()); // triggers short-circuit
        allowed.insert(1, {
            let mut b = RoaringBitmap::new();
            b.insert(9);
            b
        });
        let leaves = PoisonLeafBitmaps {
            allowed,
            forbidden: std::collections::HashSet::new(),
        };
        let pruner = empty_pruner();

        let result = BitmapTreeEvaluator
            .prefetch(&tree, &test_ctx(), &leaves, &pruner)
            .unwrap();
        // NOT inverts empty AND → universe.
        assert_eq!(result.candidates.len(), 16);
        // Both collector leaves materialised.
        assert_eq!(result.per_leaf.len(), 2);
    }

    // ── subtree_cost ─────────────────────────────────────────────────

    fn test_predicate_node() -> ResolvedNode {
        ResolvedNode::Predicate {
            pred: Arc::new(ResolvedPredicate {
                column: "x".into(),
                op: Operator::Eq,
                value: ScalarValue::Int32(Some(0)),
            }),
        }
    }

    #[test]
    fn subtree_cost_leaf_nodes() {
        assert_eq!(subtree_cost(&test_predicate_node()), COST_PREDICATE);
        assert_eq!(subtree_cost(&collector_leaf(0)), COST_COLLECTOR);
    }

    #[test]
    fn subtree_cost_not_passes_through() {
        let wrapped = ResolvedNode::Not(Box::new(test_predicate_node()));
        assert_eq!(subtree_cost(&wrapped), COST_PREDICATE);
    }

    #[test]
    fn subtree_cost_sums_children() {
        // AND(Predicate, Predicate, Collector) = 1 + 1 + 10 = 12
        let tree = ResolvedNode::And(vec![
            test_predicate_node(),
            test_predicate_node(),
            collector_leaf(0),
        ]);
        assert_eq!(subtree_cost(&tree), 2 * COST_PREDICATE + COST_COLLECTOR);
    }

    #[test]
    fn subtree_cost_predicate_heavy_nested_beats_single_collector() {
        // Key property: a Nested subtree of 3 Predicates (cost 3) ranks
        // lower than a single Collector (cost 10). The old tier-based
        // ordering got this backwards because it treated every Nested
        // subtree as uniformly "most expensive."
        let nested = ResolvedNode::And(vec![
            test_predicate_node(),
            test_predicate_node(),
            test_predicate_node(),
        ]);
        let single_collector = collector_leaf(0);
        assert!(
            subtree_cost(&nested) < subtree_cost(&single_collector),
            "predicate-heavy nested should cost less than single collector \
             (got nested={}, collector={})",
            subtree_cost(&nested),
            subtree_cost(&single_collector),
        );
    }

    #[test]
    fn subtree_cost_collector_heavy_nested_exceeds_single_collector() {
        // Symmetric: a Nested subtree of 2 Collectors (cost 20) ranks
        // higher than a single Collector (cost 10).
        let nested = ResolvedNode::And(vec![collector_leaf(0), collector_leaf(1)]);
        let single_collector = collector_leaf(0);
        assert!(subtree_cost(&nested) > subtree_cost(&single_collector));
    }
}
