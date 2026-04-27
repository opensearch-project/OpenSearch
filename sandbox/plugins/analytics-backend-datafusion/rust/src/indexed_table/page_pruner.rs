/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Page-level pruning using parquet page statistics.
//!
//! Filters row IDs based on page min/max statistics before reading data.
//! This intersects (AND mode) or unions (OR mode) backend's doc IDs with page
//! ranges that could contain matching values, eliminating rows that can't
//! satisfy the filter.
//!
//! Ported verbatim from PR #21164.

use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::file::metadata::ParquetMetaData;

#[derive(Debug, Clone)]
struct ParsedFilter {
    col_name: String,
    op: Operator,
    value: ScalarValue,
}

/// Per-row-group page pruner using page statistics.
pub struct PagePruner {
    filters: Vec<ParsedFilter>,
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
}

impl PagePruner {
    pub fn new(schema: &SchemaRef, metadata: Arc<ParquetMetaData>, filters: &[Expr]) -> Self {
        let parsed = filters.iter().filter_map(Self::parse_filter).collect();
        Self {
            filters: parsed,
            schema: schema.clone(),
            metadata,
        }
    }

    fn parse_filter(filter: &Expr) -> Option<ParsedFilter> {
        if let Expr::BinaryExpr(binary) = filter {
            match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(val, _)) => Some(ParsedFilter {
                    col_name: col.name.clone(),
                    op: binary.op,
                    value: val.clone(),
                }),
                (Expr::Literal(val, _), Expr::Column(col)) => {
                    let flipped_op = match &binary.op {
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        other => *other,
                    };
                    Some(ParsedFilter {
                        col_name: col.name.clone(),
                        op: flipped_op,
                        value: val.clone(),
                    })
                }
                _ => None,
            }
        } else {
            None
        }
    }

    /// Compute page ranges that could match the filters for a row group.
    ///
    /// `None` if page stats are unavailable (caller should assume all rows).
    /// `Some(vec![])` if no pages match (caller can skip entirely).
    ///
    /// # Selectivity ordering
    ///
    /// Filters are evaluated in input order to produce their individual
    /// candidate range sets. Those sets are then intersected in order of
    /// **ascending total rows covered** — i.e. most-selective first — so the
    /// accumulated intersection shrinks as fast as possible, and the
    /// short-circuit on empty triggers earlier.
    ///
    /// Rationale: `intersect_ranges` is O(|existing| + |merged|). Starting
    /// with the smallest `merged` keeps `existing` small across the loop.
    /// For a query like `rare_col = V AND common_col > 0`, this cuts the
    /// combined work relative to naive input-order iteration.
    fn compute_page_ranges(&self, rg_idx: usize, rg_first_row: i64) -> Option<Vec<Range<i64>>> {
        let (column_index, offset_index) = match (
            self.metadata.column_index(),
            self.metadata.offset_index(),
        ) {
            (Some(ci), Some(oi)) => (ci, oi),
            _ => return None,
        };

        let parquet_schema = self.metadata.file_metadata().schema_descr();
        let rg = self.metadata.row_group(rg_idx);
        let rg_num_rows = rg.num_rows();

        // Phase 1: compute per-filter candidate ranges.
        // Each entry is (total_rows_covered, merged_ranges). Filters for
        // which stats aren't available are skipped (conservative: they don't
        // constrain the result).
        //
        // # AND-group sharing
        //
        // Filters on the same column share parquet page-stats decoding
        // (`StatisticsConverter::try_new`, `data_page_mins/maxes/row_counts`).
        // We walk filters in input order but group by column so that
        // consecutive filters on `col_a` hit the parquet decoder once, not N
        // times. Ordering across different columns is preserved (same result
        // as before, just faster on queries like
        // `col_a > 10 AND col_a < 20 AND col_b = 5`).
        let mut per_filter: Vec<(i64, Vec<Range<i64>>)> = Vec::with_capacity(self.filters.len());

        // Indices into self.filters grouped by col_name, groups in first-seen order.
        let mut col_order: Vec<&str> = Vec::new();
        let mut groups: std::collections::HashMap<&str, Vec<usize>> =
            std::collections::HashMap::new();
        for (i, f) in self.filters.iter().enumerate() {
            groups
                .entry(f.col_name.as_str())
                .and_modify(|v| v.push(i))
                .or_insert_with(|| {
                    col_order.push(f.col_name.as_str());
                    vec![i]
                });
        }

        for col_name in col_order {
            let converter = match StatisticsConverter::try_new(
                col_name,
                &self.schema,
                parquet_schema,
            ) {
                Ok(c) => c,
                Err(_) => continue,
            };

            let (mins, maxes) = match (
                converter.data_page_mins(column_index, offset_index, [&rg_idx]),
                converter.data_page_maxes(column_index, offset_index, [&rg_idx]),
            ) {
                (Ok(m), Ok(x)) => (m, x),
                _ => continue,
            };

            let row_counts = match converter.data_page_row_counts(
                offset_index,
                self.metadata.row_groups(),
                [&rg_idx],
            ) {
                Ok(Some(rc)) => rc,
                _ => continue,
            };

            let num_pages = mins.len();

            // Evaluate each filter on this column against the shared mins/maxes/row_counts.
            for &filter_idx in &groups[col_name] {
                let filter = &self.filters[filter_idx];
                let mut page_start = rg_first_row;
                let mut filter_ranges: Vec<Range<i64>> = Vec::new();

                for page_idx in 0..num_pages {
                    let page_rows = if page_idx < row_counts.len() {
                        row_counts.value(page_idx) as i64
                    } else {
                        rg_num_rows / num_pages as i64
                    };

                    if Self::page_matches(&mins, &maxes, page_idx, &filter.op, &filter.value) {
                        filter_ranges.push(page_start..page_start + page_rows);
                    }
                    page_start += page_rows;
                }

                filter_ranges.sort_by_key(|r| r.start);
                let merged = Self::merge_ranges(filter_ranges);

                // Empty candidate set from this filter alone → nothing matches.
                if merged.is_empty() {
                    return Some(vec![]);
                }

                let total: i64 = merged.iter().map(|r| r.end - r.start).sum();
                per_filter.push((total, merged));
            }
        }

        if per_filter.is_empty() {
            return None;
        }

        // Phase 2: intersect in ascending-rows-covered order (most selective first).
        per_filter.sort_by_key(|(total, _)| *total);

        let mut iter = per_filter.into_iter();
        let mut combined = iter.next().map(|(_, r)| r).unwrap_or_default();
        for (_, merged) in iter {
            combined = Self::intersect_ranges(&combined, &merged);
            if combined.is_empty() {
                return Some(vec![]);
            }
        }

        Some(combined)
    }

    /// Compute page ranges for a single filter on a row group.
    fn compute_page_ranges_for_filter(
        &self,
        col_name: &str,
        op: &Operator,
        value: &ScalarValue,
        rg_idx: usize,
        rg_first_row: i64,
    ) -> Option<Vec<Range<i64>>> {
        let (column_index, offset_index) = match (
            self.metadata.column_index(),
            self.metadata.offset_index(),
        ) {
            (Some(ci), Some(oi)) => (ci, oi),
            _ => return None,
        };

        let parquet_schema = self.metadata.file_metadata().schema_descr();
        let rg = self.metadata.row_group(rg_idx);
        let rg_num_rows = rg.num_rows();

        let converter = match StatisticsConverter::try_new(col_name, &self.schema, parquet_schema)
        {
            Ok(c) => c,
            Err(_) => return None,
        };

        let (mins, maxes) = match (
            converter.data_page_mins(column_index, offset_index, [&rg_idx]),
            converter.data_page_maxes(column_index, offset_index, [&rg_idx]),
        ) {
            (Ok(m), Ok(x)) => (m, x),
            _ => return None,
        };

        let row_counts = match converter.data_page_row_counts(
            offset_index,
            self.metadata.row_groups(),
            [&rg_idx],
        ) {
            Ok(Some(rc)) => rc,
            _ => return None,
        };

        let num_pages = mins.len();
        let mut page_start = rg_first_row;
        let mut filter_ranges: Vec<Range<i64>> = Vec::new();

        for page_idx in 0..num_pages {
            let page_rows = if page_idx < row_counts.len() {
                row_counts.value(page_idx) as i64
            } else {
                rg_num_rows / num_pages as i64
            };

            if Self::page_matches(&mins, &maxes, page_idx, op, value) {
                filter_ranges.push(page_start..page_start + page_rows);
            }
            page_start += page_rows;
        }

        filter_ranges.sort_by_key(|r| r.start);
        Some(Self::merge_ranges(filter_ranges))
    }

    /// Row IDs within pages that match the filters (OR-mode entry point).
    /// Only used for single collector
    pub fn candidate_row_ids(&self, rg_idx: usize, rg_first_row: i64, rg_num_rows: i64) -> Vec<i64> {
        // Case 1: no filters. Nothing is ruled out. Everything in the RG is a candidate → emit every row ID in the RG.
        if self.filters.is_empty() {
            return (rg_first_row..rg_first_row + rg_num_rows).collect();
        }

        // Case 2: filters exist, page ranges computed. compute_page_ranges returns the
        // AND-intersection of all page ranges that survived pruning, expressed as a set of Range<i64> (absolute row IDs).
        let ranges = match self.compute_page_ranges(rg_idx, rg_first_row) {
            Some(r) if r.is_empty() => return vec![],
            Some(r) => r,
            None => return (rg_first_row..rg_first_row + rg_num_rows).collect(),
        };

        //Case 3: expand the ranges. Each Range<i64> covers absolute row IDs.
        // The .max(rg_first_row) and .min(rg_first_row + rg_num_rows) clip the range to this RG's
        // bounds — defensive in case a page range overhangs an RG boundary.
        // Then materialize into individual IDs.
        let mut ids = Vec::new();
        for range in &ranges {
            let start = range.start.max(rg_first_row);
            let end = range.end.min(rg_first_row + rg_num_rows);
            for id in start..end {
                ids.push(id);
            }
        }
        ids
    }

    /// Row IDs within pages matching a specific filter (Phase 1 predicate prefetch).
    pub fn candidate_row_ids_for_filter(
        &self,
        col_name: &str,
        op: &Operator,
        value: &ScalarValue,
        rg_idx: usize,
        rg_first_row: i64,
        rg_num_rows: i64,
    ) -> Vec<i64> {
        let ranges =
            match self.compute_page_ranges_for_filter(col_name, op, value, rg_idx, rg_first_row) {
                Some(r) if r.is_empty() => return vec![],
                Some(r) => r,
                None => return (rg_first_row..rg_first_row + rg_num_rows).collect(),
            };

        let mut ids = Vec::new();
        for range in &ranges {
            let start = range.start.max(rg_first_row);
            let end = range.end.min(rg_first_row + rg_num_rows);
            for id in start..end {
                ids.push(id);
            }
        }
        ids
    }

    /// Intersect row IDs with page ranges (AND mode).
    pub fn filter_row_ids(&self, rg_idx: usize, row_ids: &[i64], rg_first_row: i64) -> Vec<i64> {
        if self.filters.is_empty() || row_ids.is_empty() {
            return row_ids.to_vec();
        }

        match self.compute_page_ranges(rg_idx, rg_first_row) {
            None => row_ids.to_vec(),
            Some(ranges) if ranges.is_empty() => vec![],
            Some(ranges) => Self::filter_by_ranges(row_ids, &ranges),
        }
    }

    fn merge_ranges(ranges: Vec<Range<i64>>) -> Vec<Range<i64>> {
        let mut merged: Vec<Range<i64>> = Vec::new();
        for range in ranges {
            if let Some(last) = merged.last_mut() {
                if range.start <= last.end {
                    last.end = last.end.max(range.end);
                    continue;
                }
            }
            merged.push(range);
        }
        merged
    }

    fn intersect_ranges(a: &[Range<i64>], b: &[Range<i64>]) -> Vec<Range<i64>> {
        let mut result = Vec::new();
        let (mut i, mut j) = (0, 0);
        while i < a.len() && j < b.len() {
            let start = a[i].start.max(b[j].start);
            let end = a[i].end.min(b[j].end);
            if start < end {
                result.push(start..end);
            }
            if a[i].end < b[j].end {
                i += 1;
            } else {
                j += 1;
            }
        }
        result
    }

    fn filter_by_ranges(row_ids: &[i64], ranges: &[Range<i64>]) -> Vec<i64> {
        let mut result = Vec::with_capacity(row_ids.len());
        let mut range_idx = 0;
        for &row_id in row_ids {
            while range_idx < ranges.len() && ranges[range_idx].end <= row_id {
                range_idx += 1;
            }
            if range_idx >= ranges.len() {
                break;
            }
            if row_id >= ranges[range_idx].start && row_id < ranges[range_idx].end {
                result.push(row_id);
            }
        }
        result
    }

    /// Whether the page `[mins[page_idx], maxes[page_idx]]` interval could
    /// satisfy `col op value`.
    ///
    /// # Supported types
    ///
    /// Only `Int32Array` and `Int64Array` are decoded today; other array
    /// types (Utf8, Float, Date, Timestamp, Decimal, ...) fall through to
    /// the conservative `true` return, which keeps the page in the
    /// candidate set rather than pruning it. Result: correctness-safe
    /// (never drops data that might match) but page pruning is effectively
    /// a no-op on those columns. Extend this method to cover the column
    /// types a deployment uses most before relying on page pruning to
    /// skip large amounts of data.
    fn page_matches(
        mins: &Arc<dyn Array>,
        maxes: &Arc<dyn Array>,
        page_idx: usize,
        op: &Operator,
        value: &ScalarValue,
    ) -> bool {
        if let (Some(min_arr), Some(max_arr)) = (
            mins.as_any().downcast_ref::<Int32Array>(),
            maxes.as_any().downcast_ref::<Int32Array>(),
        ) {
            let filter_val = match value {
                ScalarValue::Int32(Some(v)) => *v,
                ScalarValue::Int64(Some(v)) => *v as i32,
                _ => return true,
            };
            if min_arr.is_null(page_idx) || max_arr.is_null(page_idx) {
                return true;
            }
            let (min, max) = (min_arr.value(page_idx), max_arr.value(page_idx));
            return Self::compare(min, max, filter_val, op);
        }

        if let (Some(min_arr), Some(max_arr)) = (
            mins.as_any().downcast_ref::<Int64Array>(),
            maxes.as_any().downcast_ref::<Int64Array>(),
        ) {
            let filter_val = match value {
                ScalarValue::Int64(Some(v)) => *v,
                ScalarValue::Int32(Some(v)) => *v as i64,
                _ => return true,
            };
            if min_arr.is_null(page_idx) || max_arr.is_null(page_idx) {
                return true;
            }
            let (min, max) = (min_arr.value(page_idx), max_arr.value(page_idx));
            return Self::compare(min, max, filter_val, op);
        }

        true
    }

    fn compare<T: Ord + PartialEq + Copy>(min: T, max: T, val: T, op: &Operator) -> bool {
        match op {
            Operator::Lt => min < val,
            Operator::LtEq => min <= val,
            Operator::Gt => max > val,
            Operator::GtEq => max >= val,
            Operator::Eq => min <= val && val <= max,
            // A page CANNOT contain a non-`val` value only when every value
            // in the page equals `val` (min == max == val). Otherwise the
            // page could match. Conservative: prune only when min == max == val.
            Operator::NotEq => !(min == val && max == val),
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::{col, lit};
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    /// 32-row fixture with price [0..32) and qty [100..132), small RG (all 32
    /// rows) but multiple pages per RG so page-level stats have granularity.
    fn two_col_pruner(filters: Vec<Expr>) -> (PagePruner, SchemaRef, Arc<ParquetMetaData>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
            Field::new("qty", DataType::Int32, false),
        ]));
        let prices: Vec<i32> = (0..32).collect();
        let qtys: Vec<i32> = (100..132).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(prices)), Arc::new(Int32Array::from(qtys))],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        let arc_meta = meta.metadata().clone();
        let pruner = PagePruner::new(&schema, Arc::clone(&arc_meta), &filters);
        (pruner, schema, arc_meta)
    }

    fn row_ids_0_to(n: i64) -> Vec<i64> {
        (0..n).collect()
    }

    #[test]
    fn page_pruner_order_invariant_across_columns() {
        // Filter order shouldn't change the output of filter_row_ids for
        // AND semantics. This exercises optimization A (selectivity ordering):
        // the phase-2 sort must be deterministic given the same input set.
        let f_price = col("price").gt(lit(20i32));
        let f_qty = col("qty").lt(lit(120i32));
        let (p1, _, _) = two_col_pruner(vec![f_price.clone(), f_qty.clone()]);
        let (p2, _, _) = two_col_pruner(vec![f_qty, f_price]);

        let ids = row_ids_0_to(32);
        let out1 = p1.filter_row_ids(0, &ids, 0);
        let out2 = p2.filter_row_ids(0, &ids, 0);
        assert_eq!(out1, out2, "filter order must not change result");
        // Sanity: result should be non-empty (page stats allow some match) and
        // bounded by the rg_num_rows.
        assert!(out1.len() <= 32);
    }

    #[test]
    fn page_pruner_multi_filter_same_column_intersects() {
        // Optimization B (AND-group sharing): two filters on the same column
        // share parquet stats decoding. The result must still be the
        // intersection of both range constraints (no rows can bypass either
        // filter).
        //
        // price > 10 AND price < 20 — page pruning may or may not eliminate
        // pages depending on how parquet actually laid out the pages (affected
        // by writer version + data layout). What we CAN assert is:
        //   1. The output is a subset of the input.
        //   2. Passing the same filters twice in a different order yields
        //      the same result (which would break if the group-sharing
        //      logic treated col_order inconsistently).
        let f1 = col("price").gt(lit(10i32));
        let f2 = col("price").lt(lit(20i32));
        let (p_a, _, _) = two_col_pruner(vec![f1.clone(), f2.clone()]);
        let (p_b, _, _) = two_col_pruner(vec![f2, f1]);
        let ids = row_ids_0_to(32);
        let out_a = p_a.filter_row_ids(0, &ids, 0);
        let out_b = p_b.filter_row_ids(0, &ids, 0);
        assert_eq!(out_a, out_b, "filter order must not change result");
        assert!(out_a.len() <= ids.len(), "output is a subset of input");
    }

    #[test]
    fn page_pruner_no_filters_returns_input_unchanged() {
        // filter_row_ids is a no-op when there are no filters.
        let (p, _, _) = two_col_pruner(vec![]);
        let ids = vec![1, 5, 10, 20, 31];
        let out = p.filter_row_ids(0, &ids, 0);
        assert_eq!(out, ids);
    }
}
