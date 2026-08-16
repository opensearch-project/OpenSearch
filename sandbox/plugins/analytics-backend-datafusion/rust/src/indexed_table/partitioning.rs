/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Partition assignment for distributing row groups across DataFusion partitions.
//!
//! Same model as DataFusion's `repartition_evenly_by_size`: flatten all row
//! groups across all segments, iterate sequentially, cut a new partition when
//! accumulated rows exceed `ceil(total_rows / num_partitions)`.
//!
//! A single partition may span multiple segments — each segment's RGs become
//! a separate `SegmentChunk`.
//!
//! Partitions MUST align to RG boundaries because:
//! 1. Splitting mid-RG causes duplicate processing.
//! 2. Row indices within a RG are contiguous `[0, num_rows)`.
//! 3. Doc IDs map 1:1 to row indices within each segment's parquet file.
//!
//! Reference: <https://github.com/apache/datafusion/blob/49776a6/datafusion/datasource/src/file_groups.rs#L204>
//!
//! Ported verbatim from PR #21164.

use super::stream::RowGroupInfo;
use super::table_provider::SegmentFileInfo;
use datafusion::common::ScalarValue;

/// One contiguous chunk of row groups within a single segment.
#[derive(Debug, Clone)]
pub struct SegmentChunk {
    pub segment_idx: usize,
    pub doc_min: i32,
    pub doc_max: i32,
    pub row_group_indices: Vec<usize>,
}

/// A partition which can span multiple segments.
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    pub chunks: Vec<SegmentChunk>,
}

/// Info about a segment needed for partition assignment.
pub struct SegmentLayout {
    pub row_groups: Vec<RowGroupInfo>,
}

/// Compute partition assignments aligned to row group boundaries.
pub fn compute_assignments(
    segments: &[SegmentLayout],
    num_partitions: usize,
) -> Vec<PartitionAssignment> {
    struct RGEntry {
        segment_idx: usize,
        rg_index: usize,
        first_row: i64,
        num_rows: i64,
    }

    let all_rgs: Vec<RGEntry> = segments
        .iter()
        .enumerate()
        .flat_map(|(seg_idx, seg)| {
            seg.row_groups.iter().map(move |rg| RGEntry {
                segment_idx: seg_idx,
                rg_index: rg.index,
                first_row: rg.first_row,
                num_rows: rg.num_rows,
            })
        })
        .collect();

    if all_rgs.is_empty() {
        return vec![];
    }

    let total_rows: i64 = all_rgs.iter().map(|rg| rg.num_rows).sum();
    let rows_per_partition = (total_rows as f64 / num_partitions as f64).ceil() as i64;

    let mut assignments: Vec<PartitionAssignment> = Vec::new();
    let mut current_chunks: Vec<SegmentChunk> = Vec::new();
    let mut current_rows: i64 = 0;

    let mut chunk_seg: Option<usize> = None;
    let mut chunk_rg_indices: Vec<usize> = Vec::new();
    let mut chunk_doc_min: i32 = 0;
    let mut chunk_doc_max: i32 = 0;

    for (i, rg) in all_rgs.iter().enumerate() {
        // Flush in-progress chunk if segment changed
        if chunk_seg.is_some() && chunk_seg != Some(rg.segment_idx) {
            if !chunk_rg_indices.is_empty() {
                current_chunks.push(SegmentChunk {
                    segment_idx: chunk_seg.unwrap(),
                    doc_min: chunk_doc_min,
                    doc_max: chunk_doc_max,
                    row_group_indices: chunk_rg_indices.clone(),
                });
                chunk_rg_indices.clear();
            }
        }

        if chunk_seg != Some(rg.segment_idx) {
            chunk_seg = Some(rg.segment_idx);
            chunk_doc_min = rg.first_row as i32;
        }

        chunk_rg_indices.push(rg.rg_index);
        chunk_doc_max = (rg.first_row + rg.num_rows) as i32;
        current_rows += rg.num_rows;

        let is_last = i == all_rgs.len() - 1;

        if current_rows >= rows_per_partition && assignments.len() < num_partitions - 1 && !is_last
        {
            current_chunks.push(SegmentChunk {
                segment_idx: chunk_seg.unwrap(),
                doc_min: chunk_doc_min,
                doc_max: chunk_doc_max,
                row_group_indices: chunk_rg_indices.clone(),
            });
            chunk_rg_indices.clear();
            chunk_doc_min = chunk_doc_max;

            assignments.push(PartitionAssignment {
                chunks: std::mem::take(&mut current_chunks),
            });
            current_rows = 0;
        }
    }

    // Flush remaining
    if !chunk_rg_indices.is_empty() {
        current_chunks.push(SegmentChunk {
            segment_idx: chunk_seg.unwrap(),
            doc_min: chunk_doc_min,
            doc_max: chunk_doc_max,
            row_group_indices: chunk_rg_indices,
        });
    }
    if !current_chunks.is_empty() {
        assignments.push(PartitionAssignment {
            chunks: current_chunks,
        });
    }

    assignments
}

/// Whether segments form a sorted chain on the lead `index.sort.field` column,
/// determined from per-segment min/max footer stats: after sorting segments by
/// `sort_min`, every segment's `sort_min` is strictly greater than the previous
/// segment's `sort_max`. Same algorithm as DataFusion's
/// `is_ordering_valid_for_file_groups` (`file_scan_config.rs:1347-1363`),
/// applied at the segment-within-a-shard granularity.
///
/// Returns `false` when:
/// - any segment has `sort_min`/`sort_max` set to `None` (treated as
///   "can't prove a chain" — same as DataFusion's missing-stats handling),
/// - any pair has overlapping ranges,
/// - `partial_cmp` returns `None` (incomparable types).
///
/// Empty input → `false` (nothing to advertise).
pub fn segments_chain_on_sort_key(segments: &[SegmentFileInfo]) -> bool {
    if segments.is_empty() {
        return false;
    }

    // Collect (min, max) for each segment; bail on the first None.
    let mut bounds: Vec<(&ScalarValue, &ScalarValue)> = Vec::with_capacity(segments.len());
    for seg in segments {
        match (&seg.sort_min, &seg.sort_max) {
            (Some(lo), Some(hi)) => bounds.push((lo, hi)),
            _ => return false,
        }
    }

    // Sort by min; bail if any pair is incomparable.
    let mut order: Vec<usize> = (0..bounds.len()).collect();
    let mut incomparable = false;
    order.sort_by(|&a, &b| {
        bounds[a].0.partial_cmp(bounds[b].0).unwrap_or_else(|| {
            incomparable = true;
            std::cmp::Ordering::Equal
        })
    });
    if incomparable {
        return false;
    }

    // Walk in sorted-by-min order: each segment's min must be > the previous segment's max.
    for w in order.windows(2) {
        let prev_max = bounds[w[0]].1;
        let curr_min = bounds[w[1]].0;
        match curr_min.partial_cmp(prev_max) {
            Some(std::cmp::Ordering::Greater) => continue,
            // Equal/less means overlap; None means incomparable.
            _ => return false,
        }
    }
    true
}

/// One-partition-per-segment assignment for the sort-aware path.
///
/// Used when:
/// - `sort_fields` is non-empty,
/// - `segments_chain_on_sort_key(...)` returned true,
/// - `target_partitions >= segments.len()` (so the optimizer's
///   `validated_output_ordering` chain check at `file_scan_config.rs:551`
///   would accept the layout — vanilla path mirror).
///
/// Produces one `PartitionAssignment` per segment, ordered by `sort_min`
/// ascending. Each assignment holds a single `SegmentChunk` covering all RGs
/// of that segment in their physical (sorted) order — because the writer
/// k-way-merges sorted chunks at finalize, RGs within a segment are in lead
/// sort-key order.
///
/// Caller must handle the case where this is inappropriate (chain doesn't
/// hold, target_partitions too low, sort fields empty) by falling back to
/// the row-count-balanced `compute_assignments`.
pub fn compute_assignments_one_per_segment(
    segments: &[SegmentFileInfo],
    layouts: &[SegmentLayout],
) -> Vec<PartitionAssignment> {
    debug_assert_eq!(
        segments.len(),
        layouts.len(),
        "segments and layouts must match (one entry per segment)"
    );
    if segments.is_empty() {
        return Vec::new();
    }

    // Order segments by sort_min — bail to existing partitioning if anything
    // is missing (defensive; caller is supposed to have checked already).
    let mut order: Vec<usize> = (0..segments.len()).collect();
    let mut incomparable = false;
    order.sort_by(|&a, &b| {
        match (segments[a].sort_min.as_ref(), segments[b].sort_min.as_ref()) {
            (Some(la), Some(lb)) => la.partial_cmp(lb).unwrap_or_else(|| {
                incomparable = true;
                std::cmp::Ordering::Equal
            }),
            // If we got here without sort_min on every segment, caller's
            // invariant is broken; preserve original order rather than panic.
            _ => {
                incomparable = true;
                std::cmp::Ordering::Equal
            }
        }
    });
    if incomparable {
        // Fail safe: caller is supposed to have run the chain check, but if
        // somehow we're here without comparable bounds, fall back to row-count
        // partitioning so we don't produce an unsorted-but-claimed-sorted plan.
        return compute_assignments(layouts, segments.len().max(1));
    }

    let mut assignments: Vec<PartitionAssignment> = Vec::with_capacity(segments.len());
    for seg_idx in order {
        let layout = &layouts[seg_idx];
        if layout.row_groups.is_empty() {
            continue;
        }
        let rg_indices: Vec<usize> = layout.row_groups.iter().map(|rg| rg.index).collect();
        let first = layout.row_groups.first().unwrap();
        let last = layout.row_groups.last().unwrap();
        let doc_min = first.first_row as i32;
        let doc_max = (last.first_row + last.num_rows) as i32;
        assignments.push(PartitionAssignment {
            chunks: vec![SegmentChunk {
                segment_idx: seg_idx,
                doc_min,
                doc_max,
                row_group_indices: rg_indices,
            }],
        });
    }
    assignments
}

/// Test-only variant of `segments_chain_on_sort_key` that takes the
/// `(sort_min, sort_max)` pairs directly — the chain logic is independent of
/// the rest of `SegmentFileInfo`. Public so unit tests can call it without
/// constructing a full `ParquetMetaData`.
#[cfg(test)]
pub(crate) fn chain_on_sort_bounds(bounds: &[(Option<ScalarValue>, Option<ScalarValue>)]) -> bool {
    if bounds.is_empty() {
        return false;
    }
    let mut refs: Vec<(&ScalarValue, &ScalarValue)> = Vec::with_capacity(bounds.len());
    for (lo, hi) in bounds {
        match (lo, hi) {
            (Some(lo), Some(hi)) => refs.push((lo, hi)),
            _ => return false,
        }
    }
    let mut order: Vec<usize> = (0..refs.len()).collect();
    let mut incomparable = false;
    order.sort_by(|&a, &b| {
        refs[a].0.partial_cmp(refs[b].0).unwrap_or_else(|| {
            incomparable = true;
            std::cmp::Ordering::Equal
        })
    });
    if incomparable {
        return false;
    }
    for w in order.windows(2) {
        let prev_max = refs[w[0]].1;
        let curr_min = refs[w[1]].0;
        match curr_min.partial_cmp(prev_max) {
            Some(std::cmp::Ordering::Greater) => continue,
            _ => return false,
        }
    }
    true
}

#[cfg(test)]
mod chain_tests {
    use super::*;
    use datafusion::common::ScalarValue;

    fn s(lo: Option<i64>, hi: Option<i64>) -> (Option<ScalarValue>, Option<ScalarValue>) {
        (
            lo.map(|v| ScalarValue::Int64(Some(v))),
            hi.map(|v| ScalarValue::Int64(Some(v))),
        )
    }

    #[test]
    fn empty_input_returns_false() {
        assert!(!chain_on_sort_bounds(&[]));
    }

    #[test]
    fn missing_stats_returns_false() {
        let bounds = [s(Some(1), Some(10)), s(None, Some(20))];
        assert!(!chain_on_sort_bounds(&bounds));
    }

    #[test]
    fn disjoint_chain_returns_true() {
        // Segments arrive in non-sorted order; the helper must sort them itself.
        let bounds = [
            s(Some(20), Some(30)),
            s(Some(1), Some(10)),
            s(Some(31), Some(40)),
        ];
        assert!(chain_on_sort_bounds(&bounds));
    }

    #[test]
    fn touching_boundary_is_overlap() {
        // Equality at the boundary doesn't chain — strict > only, matches DataFusion.
        let bounds = [s(Some(1), Some(10)), s(Some(10), Some(20))];
        assert!(!chain_on_sort_bounds(&bounds));
    }

    #[test]
    fn overlapping_ranges_returns_false() {
        let bounds = [s(Some(1), Some(15)), s(Some(10), Some(20))];
        assert!(!chain_on_sort_bounds(&bounds));
    }

    #[test]
    fn single_segment_with_stats_chains_trivially() {
        let bounds = [s(Some(1), Some(10))];
        assert!(chain_on_sort_bounds(&bounds));
    }
}
