/**
Two-phase boolean tree evaluation using Roaring bitmaps.

Phase 1 (prefetch): produces a SUPERSET bitmap for row-group-level pruning.
  - Collector leaves: collect from inverted index via JNI, stash bitmap for Phase 2.
  - Predicate leaves: page stats pruning (coarse, safe everywhere in tree).
  - AND: leader-follower ordering (cheapest first, short-circuit on empty).
  - OR: cheapest first, early termination on full universe.

Phase 2 (on-batch): evaluates tree on actual RecordBatch data (exact).
  - Collector leaves: lookup pre-computed bitmap -> per-row boolean mask.
  - Predicate leaves: vectorized cmp kernels on real data -> exact boolean.
  - AND: CollectorLeaf first (cheapest), short-circuit on all-false.
  - OR: CollectorLeaf first, short-circuit on all-true.
**/

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, Scalar};
use datafusion::arrow::compute::kernels::cast::cast;
use datafusion::arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use datafusion::arrow::compute::{and, not, or};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use roaring::RoaringBitmap;

use super::bool_tree::ResolvedNode;
use super::page_pruner::PagePruner;

/// Maps Collector leaf collector pointer (as usize) to its RoaringBitmap.
pub type CollectorBitmaps = HashMap<usize, RoaringBitmap>;

/// Context for evaluating a tree against a single row group.
pub struct EvalContext {
    pub rg_idx: usize,
    pub rg_first_row: i64,
    pub rg_num_rows: i64,
    pub min_doc: i32,
    pub max_doc: i32,
}

/// Result of Phase 1 (prefetch) evaluation.
pub struct PrefetchResult {
    pub candidates: RoaringBitmap,
    pub collector_bitmaps: CollectorBitmaps,
}

// ── Child cost classification for leader-follower ordering ─────────────

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Phase1Cost { Predicate, Collector, Nested }

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Phase2Cost { Collector, Predicate, Nested }

fn phase1_cost(node: &ResolvedNode) -> Phase1Cost {
    match node {
        ResolvedNode::Predicate { .. } => Phase1Cost::Predicate,
        ResolvedNode::Collector { .. } => Phase1Cost::Collector,
        _ => Phase1Cost::Nested,
    }
}

fn phase2_cost(node: &ResolvedNode) -> Phase2Cost {
    match node {
        ResolvedNode::Collector { .. } => Phase2Cost::Collector,
        ResolvedNode::Predicate { .. } => Phase2Cost::Predicate,
        _ => Phase2Cost::Nested,
    }
}

// ── Phase 1: Prefetch ──────────────────────────────────────────────────

/// Evaluate tree for prefetch — produces superset bitmap + per-Collector-leaf bitmaps.
pub fn evaluate_tree_prefetch(
    node: &ResolvedNode,
    ctx: &EvalContext,
    page_pruner: &PagePruner,
) -> Result<PrefetchResult, String> {
    match node {
        ResolvedNode::And(children) => prefetch_and(children, ctx, page_pruner),
        ResolvedNode::Or(children) => prefetch_or(children, ctx, page_pruner),
        ResolvedNode::Not(child) => {
            let mut r = evaluate_tree_prefetch(child, ctx, page_pruner)?;
            r.candidates = &universe_bitmap(ctx) - &r.candidates;
            Ok(r)
        }
        ResolvedNode::Collector { collector, .. } => prefetch_collector(collector, ctx),
        ResolvedNode::Predicate { column, op, value } => {
            prefetch_predicate(column, op, value, ctx, page_pruner)
        }
    }
}

/// AND with leader-follower: order by cost, evaluate leader first, short-circuit on empty.
fn prefetch_and(
    children: &[ResolvedNode],
    ctx: &EvalContext,
    page_pruner: &PagePruner,
) -> Result<PrefetchResult, String> {
    // Sort children by Phase 1 cost: Predicate < Collector < Nested
    let mut indices: Vec<usize> = (0..children.len()).collect();
    indices.sort_by_key(|&i| phase1_cost(&children[i]));

    // Evaluate leader (cheapest child)
    let leader = evaluate_tree_prefetch(&children[indices[0]], ctx, page_pruner)?;
    if leader.candidates.is_empty() {
        return Ok(leader);
    }

    let mut acc = leader.candidates;
    let mut merged = leader.collector_bitmaps;

    // Evaluate followers in cost order, intersecting as we go
    for &idx in &indices[1..] {
        let r = evaluate_tree_prefetch(&children[idx], ctx, page_pruner)?;
        acc &= &r.candidates;
        for (k, v) in r.collector_bitmaps { merged.insert(k, v); }
        if acc.is_empty() {
            return Ok(PrefetchResult { candidates: acc, collector_bitmaps: merged });
        }
    }

    Ok(PrefetchResult { candidates: acc, collector_bitmaps: merged })
}

/// OR with cost ordering and universe early termination.
fn prefetch_or(
    children: &[ResolvedNode],
    ctx: &EvalContext,
    page_pruner: &PagePruner,
) -> Result<PrefetchResult, String> {
    let mut indices: Vec<usize> = (0..children.len()).collect();
    indices.sort_by_key(|&i| phase1_cost(&children[i]));

    let universe_size = (ctx.max_doc - ctx.min_doc) as u64;
    let mut acc = RoaringBitmap::new();
    let mut merged = CollectorBitmaps::new();

    for &idx in &indices {
        let r = evaluate_tree_prefetch(&children[idx], ctx, page_pruner)?;
        acc |= &r.candidates;
        for (k, v) in r.collector_bitmaps { merged.insert(k, v); }

        // Early termination: full universe reached
        if acc.len() >= universe_size {
            // Still need collector bitmaps from remaining Collector leaves for Phase 2
            for &remaining_idx in &indices[indices.iter().position(|&i| i == idx).unwrap() + 1..] {
                if matches!(&children[remaining_idx], ResolvedNode::Collector { .. }) {
                    let r2 = evaluate_tree_prefetch(&children[remaining_idx], ctx, page_pruner)?;
                    for (k, v) in r2.collector_bitmaps { merged.insert(k, v); }
                }
            }
            return Ok(PrefetchResult { candidates: acc, collector_bitmaps: merged });
        }
    }

    Ok(PrefetchResult { candidates: acc, collector_bitmaps: merged })
}

fn prefetch_collector(
    collector: &Arc<dyn crate::indexed_table::index::RowGroupDocsCollector>,
    ctx: &EvalContext,
) -> Result<PrefetchResult, String> {
    let bm = collect_to_roaring(collector, ctx)?;
    let key = Arc::as_ptr(collector) as *const () as usize;
    let mut collector_bitmaps = CollectorBitmaps::new();
    collector_bitmaps.insert(key, bm.clone());
    Ok(PrefetchResult { candidates: bm, collector_bitmaps })
}

fn prefetch_predicate(
    column: &str,
    op: &Operator,
    value: &ScalarValue,
    ctx: &EvalContext,
    page_pruner: &PagePruner,
) -> Result<PrefetchResult, String> {
    let candidates = page_pruner.candidate_row_ids_for_filter(
        column, op, value, ctx.rg_idx, ctx.rg_first_row, ctx.rg_num_rows,
    );
    let mut bm = RoaringBitmap::new();
    for id in candidates {
        if id >= ctx.min_doc as i64 && id < ctx.max_doc as i64 {
            bm.insert(id as u32);
        }
    }
    Ok(PrefetchResult { candidates: bm, collector_bitmaps: CollectorBitmaps::new() })
}

fn collect_to_roaring(
    collector: &Arc<dyn crate::indexed_table::index::RowGroupDocsCollector>,
    ctx: &EvalContext,
) -> Result<RoaringBitmap, String> {
    let bitset = collector.collect(ctx.min_doc, ctx.max_doc)?;
    let mut bm = RoaringBitmap::new();
    let base = ctx.min_doc as i64;
    for (word_idx, &word) in bitset.iter().enumerate() {
        if word == 0 { continue; }
        let word_base = base + (word_idx as i64 * 64);
        for bit in 0..64u32 {
            if (word >> bit) & 1 == 1 {
                let doc_id = word_base + bit as i64;
                if doc_id >= ctx.min_doc as i64 && doc_id < ctx.max_doc as i64 {
                    bm.insert(doc_id as u32);
                }
            }
        }
    }
    Ok(bm)
}

fn universe_bitmap(ctx: &EvalContext) -> RoaringBitmap {
    RoaringBitmap::from_iter(ctx.min_doc as u32..ctx.max_doc as u32)
}

/// Convert bitmap of absolute doc IDs to row-group-relative offsets.
pub fn bitmap_to_offsets(bm: &RoaringBitmap, rg_first_row: i64) -> Vec<u64> {
    bm.iter().map(|doc_id| (doc_id as i64 - rg_first_row) as u64).collect()
}

// ── Phase 2: On-batch evaluation (exact) ───────────────────────────────

/// Evaluate tree on a RecordBatch, producing an exact boolean mask.
pub fn evaluate_tree_on_batch(
    node: &ResolvedNode,
    batch: &RecordBatch,
    collector_bitmaps: &CollectorBitmaps,
    rg_first_row: i64,
    batch_offset: usize,
    batch_len: usize,
) -> Result<BooleanArray, String> {
    match node {
        ResolvedNode::And(children) => on_batch_and(children, batch, collector_bitmaps, rg_first_row, batch_offset, batch_len),
        ResolvedNode::Or(children) => on_batch_or(children, batch, collector_bitmaps, rg_first_row, batch_offset, batch_len),
        ResolvedNode::Not(child) => {
            let m = evaluate_tree_on_batch(child, batch, collector_bitmaps, rg_first_row, batch_offset, batch_len)?;
            not(&m).map_err(|e| format!("NOT: {}", e))
        }
        ResolvedNode::Collector { collector, .. } => {
            let key = Arc::as_ptr(collector) as *const () as usize;
            let bm = collector_bitmaps.get(&key)
                .ok_or_else(|| "Collector bitmap not found".to_string())?;
            let mut values = Vec::with_capacity(batch_len);
            for i in 0..batch_len {
                let doc_id = (rg_first_row + batch_offset as i64 + i as i64) as u32;
                values.push(bm.contains(doc_id));
            }
            Ok(BooleanArray::from(values))
        }
        ResolvedNode::Predicate { column, op, value } => {
            evaluate_predicate_on_batch(column, op, value, batch)
        }
    }
}

/// AND with cost-based ordering and all-false short-circuit.
fn on_batch_and(
    children: &[ResolvedNode],
    batch: &RecordBatch,
    collector_bitmaps: &CollectorBitmaps,
    rg_first_row: i64,
    batch_offset: usize,
    batch_len: usize,
) -> Result<BooleanArray, String> {
    if children.is_empty() { return Ok(BooleanArray::from(vec![true; batch_len])); }

    let mut indices: Vec<usize> = (0..children.len()).collect();
    indices.sort_by_key(|&i| phase2_cost(&children[i]));

    let mut acc = evaluate_tree_on_batch(&children[indices[0]], batch, collector_bitmaps, rg_first_row, batch_offset, batch_len)?;

    for &idx in &indices[1..] {
        // Short-circuit: all-false means no rows can match
        if acc.true_count() == 0 { return Ok(acc); }
        let m = evaluate_tree_on_batch(&children[idx], batch, collector_bitmaps, rg_first_row, batch_offset, batch_len)?;
        acc = and(&acc, &m).map_err(|e| format!("AND: {}", e))?;
    }
    Ok(acc)
}

/// OR with cost-based ordering and all-true short-circuit.
fn on_batch_or(
    children: &[ResolvedNode],
    batch: &RecordBatch,
    collector_bitmaps: &CollectorBitmaps,
    rg_first_row: i64,
    batch_offset: usize,
    batch_len: usize,
) -> Result<BooleanArray, String> {
    if children.is_empty() { return Ok(BooleanArray::from(vec![false; batch_len])); }

    let mut indices: Vec<usize> = (0..children.len()).collect();
    indices.sort_by_key(|&i| phase2_cost(&children[i]));

    let mut acc = evaluate_tree_on_batch(&children[indices[0]], batch, collector_bitmaps, rg_first_row, batch_offset, batch_len)?;

    for &idx in &indices[1..] {
        // Short-circuit: all-true means all rows already match
        if acc.true_count() == batch_len { return Ok(acc); }
        let m = evaluate_tree_on_batch(&children[idx], batch, collector_bitmaps, rg_first_row, batch_offset, batch_len)?;
        acc = or(&acc, &m).map_err(|e| format!("OR: {}", e))?;
    }
    Ok(acc)
}

fn evaluate_predicate_on_batch(
    column: &str,
    op: &Operator,
    value: &ScalarValue,
    batch: &RecordBatch,
) -> Result<BooleanArray, String> {
    let col_array = batch.column_by_name(column)
        .ok_or_else(|| format!("column '{}' not found in batch", column))?;

    let scalar_array = value.to_array_of_size(1)
        .map_err(|e| format!("scalar to array: {}", e))?;

    let col_type = col_array.data_type();
    let casted = if scalar_array.data_type() != col_type {
        cast(&scalar_array, col_type).map_err(|e| format!("cast: {}", e))?
    } else {
        scalar_array
    };
    let scalar = Scalar::new(casted);

    match op {
        Operator::Lt => lt(col_array, &scalar).map_err(|e| format!("lt: {}", e)),
        Operator::LtEq => lt_eq(col_array, &scalar).map_err(|e| format!("lt_eq: {}", e)),
        Operator::Gt => gt(col_array, &scalar).map_err(|e| format!("gt: {}", e)),
        Operator::GtEq => gt_eq(col_array, &scalar).map_err(|e| format!("gt_eq: {}", e)),
        Operator::Eq => eq(col_array, &scalar).map_err(|e| format!("eq: {}", e)),
        Operator::NotEq => neq(col_array, &scalar).map_err(|e| format!("neq: {}", e)),
        _ => Err(format!("unsupported operator: {:?}", op)),
    }
}
