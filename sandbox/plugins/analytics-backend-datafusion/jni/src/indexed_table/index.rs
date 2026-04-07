/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
Core index traits — the contract between the index and the query engine.

These two traits are all the core crate needs. How the searcher was created
(Lucene JNI, Tantivy, test stub, Java-side Weight pointer) is irrelevant here.

ShardSearcher   (Equivalent of Lucene's Weight — once per shard)
  └── SegmentCollector (Equivalent of Lucene's Scorer — once per segment)
        └── collect(range) → Vec<u64> (Collects matching doc IDs as bitsets within the given doc range)
**/

use std::fmt::Debug;
use std::sync::Arc;

/// A collector that retrieves matching doc IDs as a bitset for a row group docs
/// range within a segment.
///
/// The `collect` method returns a bitset (Vec<u64>) where each set bit represents
/// a matching doc ID relative to `min_doc`.
pub trait RowGroupDocsCollector: Send + Sync + Debug {
    /// Collect matching doc IDs in `[min_doc, max_doc)` as a bitset.
    ///
    /// Each u64 word represents 64 consecutive doc IDs starting from `min_doc`.
    /// Bit `i` in word `j` represents doc ID `min_doc + j*64 + i`.
    ///
    /// The collector may be called multiple times with increasing ranges
    /// (forward-only iteration).
    fn collect(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String>;
}

/// A searcher scoped to a single shard (index), created once per query.
///
/// Analogous to Lucene's `Weight` — expensive to create (parses query, builds
/// automaton for wildcards, etc.), but creating segment collectors from it is cheap.
pub trait ShardSearcher: Send + Sync + Debug {
    /// Number of segments in this shard.
    fn segment_count(&self) -> usize;

    /// Max doc ID for a specific segment.
    fn segment_max_doc(&self, segment_ord: usize) -> Result<i64, String>;

    /// Create a collector for a specific segment and doc ID range.
    ///
    /// The collector will only return docs in `[doc_min, doc_max)` as we stream through the row groups
    /// This is cheap — analogous to `Weight.scorer(leafCtx, doc_min, doc_max)` in Lucene.
    fn collector(
        &self,
        segment_ord: usize,
        doc_min: i32,
        doc_max: i32,
    ) -> Result<Arc<dyn RowGroupDocsCollector>, String>;
}

/// How the index bitset relates to the parquet (non-indexed) filters.
///
/// Java decides this based on the top-level query structure:
///
/// In both cases, the result is a superset of the true answer. DataFusion's
/// residual filter cleans up false positives after parquet reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitsetMode {
    /// Intersect bitset with page pruner ranges (default).
    And,
    /// Union bitset with page pruner candidate rows.
    Or,
}

impl Default for BitsetMode {
    fn default() -> Self {
        BitsetMode::And
    }
}
