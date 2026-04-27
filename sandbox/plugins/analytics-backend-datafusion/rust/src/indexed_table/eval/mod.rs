/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Row-group-level bitset sources — the pluggability seam for where
//! boolean tree evaluation happens.
//!
//! [`IndexedStream`](crate::indexed_table::stream::IndexedStream) only depends
//! on [`RowGroupBitsetSource`]. The source of the bitset is abstracted.
//!
//! # Invariant — row-group-at-a-time
//!
//! The trait methods operate on one RG. There is no `prefetch_shard` or
//! `evaluate_full_filter` method. Even when tree evaluation eventually moves
//! elsewhere:
//!
//! - Bitsets stay small (~512 bytes per RG).
//! - Prefetch overlaps the next RG's bitset with the current RG's parquet read.
//! - Memory stays bounded regardless of shard size.
//!
//! # Pluggable tree evaluation (multi-filter tree path)
//!
//! For tree queries, evaluation has two orthogonal concerns:
//!
//! 1. **Tree evaluation strategy** ([`TreeEvaluator`]) — the algorithm that
//!    walks the tree, combines bitmaps, produces superset candidates +
//!    exact per-batch mask. Today: [`bitmap_tree::BitmapTreeEvaluator`].
//!    This is extensible to different implementations.
//! 2. **Leaf bitmap source** ([`LeafBitmapSource`]) — given a `Collector`
//!    leaf, produce its RoaringBitmap for this RG. Today: backend-backed
//!    (FFM upcall + bitset expansion).
//!
//! [`TreeBitsetSource`] composes any `TreeEvaluator` with any
//! `LeafBitmapSource` and exposes the composite as a `RowGroupBitsetSource`.
//! Swapping impls requires only passing different `Arc`s at construction.

pub mod bitmap_tree;
pub mod single_collector;

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use roaring::RoaringBitmap;

use super::bool_tree::ResolvedNode;
use super::page_pruner::PagePruner;
use super::stream::RowGroupInfo;

/// Per-row-group bitset producer. Plugs into `IndexedStream`.
pub trait RowGroupBitsetSource: Send + Sync {
    /// Build candidate[pre-scan] bitset for this RG. `None` = skip RG entirely.
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String>;

    /// Produce exact per-batch `BooleanArray` mask for refinement-stage [post-scan]
    /// filtering.
    ///
    /// - `rg_state` is the `context` returned by the last `prefetch_rg` for
    ///   this RG — evaluators downcast it to their own per-RG state type.
    /// - `position_map` translates delivered batch-row indices to RG-relative
    ///   positions (identity under full-scan; non-trivial under
    ///   block-granular RowSelection).
    /// - `None` = no refinement mask needed (e.g. `SingleCollectorEvaluator`
    ///   relies on DataFusion's own predicate pushdown, so the candidate
    ///   stage's RowSelection is authoritative).
    fn on_batch_mask(
        &self,
        rg_state: &dyn Any,
        rg_first_row: i64,
        position_map: &crate::indexed_table::row_selection::PositionMap,
        batch_offset: usize,
        batch_len: usize,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String>;

    /// Whether `IndexedStream` should build a post-decode `current_mask` from
    /// candidate offsets on the full-scan strategy. `true` for evaluators
    /// whose `on_batch_mask` returns `None` (e.g. `SingleCollectorEvaluator` —
    /// candidates are the only per-row filter available post-decode).
    /// `false` for evaluators whose `on_batch_mask` returns an exact refinement
    /// mask (e.g. `TreeBitsetSource` — refinement is authoritative and would
    /// ignore `current_mask` anyway). Default `true` keeps the current
    /// behaviour for any future evaluator that forgets to override.
    fn needs_row_mask(&self) -> bool {
        true
    }
}

/// Output of `prefetch_rg`.
pub struct PrefetchedRg {
    /// Candidate doc-id bitmap, RG-relative (bit 0 = first row of the RG
    /// doc range). `IndexedStream` converts this to a `RowSelection` using
    /// `min_skip_run` and keeps the matching `PositionMap` alongside for
    /// post-decode alignment.
    pub candidates: RoaringBitmap,
    /// Time spent producing the bitset (nanoseconds). For metrics.
    pub eval_nanos: u64,
    /// Opaque per-RG state threaded to `on_batch_mask` via `rg_state: &dyn Any`.
    /// Evaluators downcast to their own concrete type.
    pub context: Box<dyn Any + Send + Sync>,
}

impl PrefetchedRg {
    /// Helper for evaluators with no per-RG state (e.g. the single-collector
    /// path, which doesn't do refinement [post-scan]).
    pub fn without_context(candidates: RoaringBitmap, eval_nanos: u64) -> Self {
        Self {
            candidates,
            eval_nanos,
            context: Box::new(()),
        }
    }
}

/// Multi-filter tree path: pluggable tree evaluator + leaf bitmap source
/// 
/// Context for evaluating a tree against one row group.
#[derive(Debug, Clone, Copy)]
pub struct RgEvalContext {
    pub rg_idx: usize,
    pub rg_first_row: i64,
    pub rg_num_rows: i64,
    pub min_doc: i32,
    pub max_doc: i32,
    /// Candidate-stage leaf-reorder cost for `ResolvedNode::Predicate`.
    /// Plumbed from `DatafusionQueryConfig`; read on the hot path.
    pub cost_predicate: u32,
    /// Candidate-stage leaf-reorder cost for `ResolvedNode::Collector`.
    pub cost_collector: u32,
}

/// Candidate-stage output of a `TreeEvaluator`. `candidates` is a superset
/// bitmap of doc IDs relative to `ctx.min_doc`; `per_leaf` maps leaf
/// identity (implementation-defined — pointer or index) to that leaf's
/// bitmap in the same domain, which the refinement stage looks up per
/// batch.
pub struct TreePrefetch {
    pub candidates: RoaringBitmap,
    pub per_leaf: Vec<(usize, RoaringBitmap)>,
    /// Anchor doc ID (same as `ctx.min_doc` at prefetch time) so the
    /// refinement stage can convert batch offsets to doc IDs.
    pub min_doc: i32,
}

/// Produces per-leaf bitmaps for one row group.
///
/// Identified by DFS index in `tree`. Bitmap domain is `[ctx.min_doc, ctx.max_doc)`.
pub trait LeafBitmapSource: Send + Sync {
    fn leaf_bitmap(
        &self,
        tree: &ResolvedNode,
        leaf_dfs_index: usize,
        ctx: &RgEvalContext,
    ) -> Result<RoaringBitmap, String>;
}

/// Pluggable tree-evaluation strategy. The algorithm that walks the tree,
/// combines per-leaf bitmaps, produces candidates + per-batch masks.
pub trait TreeEvaluator: Send + Sync {
    /// Candidate stage: walk the tree for one row group and produce a
    /// superset RoaringBitmap of candidate doc IDs plus the per-leaf
    /// bitmap side-table that the refinement stage will read.
    ///
    /// `pruning_predicates` maps each `Predicate(expr)` leaf (keyed by
    /// its
    /// `Arc::as_ptr` identity) to a pre-built `PruningPredicate`. Empty
    /// map = no page-level predicate pruning; each Predicate leaf falls
    /// back to "every row is a candidate" (safe, identity for the
    /// candidate stage).
    fn prefetch(
        &self,
        tree: &ResolvedNode,
        ctx: &RgEvalContext,
        leaves: &dyn LeafBitmapSource,
        page_pruner: &PagePruner,
        pruning_predicates: &std::collections::HashMap<
            usize,
            Arc<datafusion::physical_optimizer::pruning::PruningPredicate>,
        >,
        page_prune_metrics: Option<
            &crate::indexed_table::page_pruner::PagePruneMetrics,
        >,
    ) -> Result<TreePrefetch, String>;

    /// Refinement stage: produce the exact per-row `BooleanArray` for one
    /// record batch, consuming the candidate-stage `state` for the RG this
    /// batch belongs to.
    ///
    /// `position_map` translates delivered batch-row index to RG-relative
    /// position (identity under full-scan; non-trivial under block-granular
    /// RowSelection). `batch_offset` is the delivered-row index of the
    /// first row in this batch.
    fn on_batch(
        &self,
        tree: &ResolvedNode,
        state: &TreePrefetch,
        batch: &RecordBatch,
        rg_first_row: i64,
        position_map: &crate::indexed_table::row_selection::PositionMap,
        batch_offset: usize,
        batch_len: usize,
    ) -> Result<BooleanArray, String>;
}

/// Composes a `TreeEvaluator` + `LeafBitmapSource` + `PagePruner` + resolved
/// tree into a `RowGroupBitsetSource`.
///
/// Usage:
/// ```ignore
/// let source = TreeBitsetSource {
///     tree: Arc::new(resolved),
///     evaluator: Arc::new(BitmapTreeEvaluator),        // or JavaTreeEvaluator
///     leaves: Arc::new(CollectorLeafBitmaps::without_metrics()),           // or ParquetStatsLeaves
///     page_pruner: Arc::new(pruner),
/// };
/// ```
///
/// # Batch projection requirement
///
/// The refinement stage evaluates `Predicate` leaves via Arrow cmp kernels
/// on the current `RecordBatch`. Every column referenced by a
/// `ResolvedNode::Predicate` in the tree **must be present in the batch**
/// at eval time, i.e. the physical plan's projection must include
/// predicate columns, not just the final
/// SELECT list. In production, substrait plans emitted by the planner project
/// predicate columns as part of the filter node, so this is naturally
/// satisfied. Test harnesses that bypass substrait and select only output
/// columns must explicitly expand the SELECT to include predicate columns.
pub struct TreeBitsetSource {
    pub tree: Arc<ResolvedNode>,
    pub evaluator: Arc<dyn TreeEvaluator>,
    pub leaves: Arc<dyn LeafBitmapSource>,
    pub page_pruner: Arc<PagePruner>,
    /// Pre-extracted from `DatafusionQueryConfig` at source-construction
    /// time so `prefetch_rg` doesn't need an `Arc` deref on the hot path.
    pub cost_predicate: u32,
    pub cost_collector: u32,
    /// Per-predicate `PruningPredicate` cache, keyed by
    /// `Arc::as_ptr(resolved_predicate) as usize`. Built once per query at
    /// dispatch time by the caller. Empty = page-level predicate pruning
    /// disabled (the tree path still works, each Predicate leaf falls
    /// back to "every row is a candidate").
    pub pruning_predicates: Arc<
        std::collections::HashMap<
            usize,
            Arc<datafusion::physical_optimizer::pruning::PruningPredicate>,
        >,
    >,
    /// Counters recorded by `page_pruner.prune_rg` at each Predicate
    /// leaf in the tree walk. Populated from the stream's
    /// `PartitionMetrics` at dispatch time.
    pub page_prune_metrics:
        Option<crate::indexed_table::page_pruner::PagePruneMetrics>,
}

impl RowGroupBitsetSource for TreeBitsetSource {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let t = std::time::Instant::now();
        let ctx = RgEvalContext {
            rg_idx: rg.index,
            rg_first_row: rg.first_row,
            rg_num_rows: rg.num_rows,
            min_doc,
            max_doc,
            cost_predicate: self.cost_predicate,
            cost_collector: self.cost_collector,
        };
        let prefetch = self
            .evaluator
            .prefetch(
                &self.tree,
                &ctx,
                &*self.leaves,
                &self.page_pruner,
                &self.pruning_predicates,
                self.page_prune_metrics.as_ref(),
            )
            .map_err(|e| format!("TreeBitsetSource::prefetch_rg(rg={}): {}", rg.index, e))?;
        if prefetch.candidates.is_empty() {
            return Ok(None);
        }
        // `prefetch.candidates` is in min_doc-relative space [0, max_doc - min_doc).
        // `PrefetchedRg.candidates` is in RG-relative space [0, rg.num_rows).
        // anchor = (min_doc - rg.first_row) shifts each relative bit.
        let anchor = (min_doc as i64) - rg.first_row;
        let mut rg_candidates = RoaringBitmap::new();
        for rel in prefetch.candidates.iter() {
            let shifted = rel as i64 + anchor;
            if shifted >= 0 && shifted <= u32::MAX as i64 {
                rg_candidates.insert(shifted as u32);
            }
        }
        Ok(Some(PrefetchedRg {
            candidates: rg_candidates,
            eval_nanos: t.elapsed().as_nanos() as u64,
            context: Box::new(prefetch),
        }))
    }

    fn on_batch_mask(
        &self,
        rg_state: &dyn Any,
        rg_first_row: i64,
        position_map: &crate::indexed_table::row_selection::PositionMap,
        batch_offset: usize,
        batch_len: usize,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        let state = rg_state.downcast_ref::<TreePrefetch>().ok_or_else(|| {
            "TreeBitsetSource::on_batch_mask: rg_state is not TreePrefetch".to_string()
        })?;
        let mask = self.evaluator.on_batch(
            &self.tree,
            state,
            batch,
            rg_first_row,
            position_map,
            batch_offset,
            batch_len,
        )?;
        Ok(Some(mask))
    }

    /// `TreeBitsetSource` always returns `Some(mask)` from `on_batch_mask` —
    /// the refinement mask is the exact per-row answer. `finalize_batch`
    /// ignores `current_mask` in that branch, so building it from candidates
    /// is wasted work.
    fn needs_row_mask(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::bool_tree::ResolvedNode;
    use crate::indexed_table::page_pruner::PagePruner;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;

    fn empty_pruner() -> Arc<PagePruner> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![0i32; 4]))],
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
        Arc::new(PagePruner::new(meta.schema(), meta.metadata().clone()))
    }

    /// Leaf source that returns empty bitmaps — enough to compose a
    /// TreeBitsetSource purely for testing its `needs_row_mask` override.
    struct NoopLeaves;
    impl LeafBitmapSource for NoopLeaves {
        fn leaf_bitmap(
            &self,
            _tree: &ResolvedNode,
            _idx: usize,
            _ctx: &RgEvalContext,
        ) -> Result<roaring::RoaringBitmap, String> {
            Ok(roaring::RoaringBitmap::new())
        }
    }

    /// Evaluator that mirrors the shape of BitmapTreeEvaluator for the trait
    /// needs_row_mask test (we don't import BitmapTreeEvaluator here to avoid
    /// a circular dependency with the bitmap_tree module's own tests).
    struct NoopTreeEvaluator;
    impl TreeEvaluator for NoopTreeEvaluator {
        fn prefetch(
            &self,
            _tree: &ResolvedNode,
            _ctx: &RgEvalContext,
            _leaves: &dyn LeafBitmapSource,
            _page_pruner: &PagePruner,
            _pruning_predicates: &std::collections::HashMap<
                usize,
                Arc<datafusion::physical_optimizer::pruning::PruningPredicate>,
            >,
            _page_prune_metrics: Option<
                &crate::indexed_table::page_pruner::PagePruneMetrics,
            >,
        ) -> Result<TreePrefetch, String> {
            Ok(TreePrefetch {
                candidates: roaring::RoaringBitmap::new(),
                per_leaf: Vec::new(),
                min_doc: 0,
            })
        }
        fn on_batch(
            &self,
            _tree: &ResolvedNode,
            _state: &TreePrefetch,
            _batch: &RecordBatch,
            _rg_first_row: i64,
            _position_map: &crate::indexed_table::row_selection::PositionMap,
            _batch_offset: usize,
            batch_len: usize,
        ) -> Result<datafusion::arrow::array::BooleanArray, String> {
            Ok(datafusion::arrow::array::BooleanArray::from(vec![false; batch_len]))
        }
    }

    #[test]
    fn tree_bitset_source_does_not_need_row_mask() {
        // `TreeBitsetSource::on_batch_mask` returns `Some(refinement_mask)`.
        // `finalize_batch` ignores `current_mask` in that branch, so
        // `IndexedStream` should skip building it.
        use crate::indexed_table::bool_tree::ResolvedNode;
        use crate::indexed_table::index::RowGroupDocsCollector;

        #[derive(Debug)]
        struct Dummy;
        impl RowGroupDocsCollector for Dummy {
            fn collect_packed_u64_bitset(&self, _: i32, _: i32) -> Result<Vec<u64>, String> {
                Ok(vec![])
            }
        }
        let source = TreeBitsetSource {
            tree: Arc::new(ResolvedNode::Collector {
                provider_key: 0,
                collector: Arc::new(Dummy),
            }),
            evaluator: Arc::new(NoopTreeEvaluator),
            leaves: Arc::new(NoopLeaves),
            page_pruner: empty_pruner(),
            cost_predicate: 1,
            cost_collector: 10,
            pruning_predicates: std::sync::Arc::new(std::collections::HashMap::new()),
                page_prune_metrics: None,
        };
        assert!(!source.needs_row_mask());
    }
}
