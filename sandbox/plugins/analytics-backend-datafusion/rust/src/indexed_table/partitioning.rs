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

        if current_rows >= rows_per_partition
            && assignments.len() < num_partitions - 1
            && !is_last
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
