/**
Page-level pruning using parquet page statistics.

Filters row IDs based on page min/max statistics before reading data.
This intersects Lucene's doc IDs with page ranges that could contain
matching values, eliminating rows that can't satisfy the filter.
**/
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
                    op: binary.op.clone(),
                    value: val.clone(),
                }),
                (Expr::Literal(val, _), Expr::Column(col)) => {
                    let flipped_op = match &binary.op {
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        other => other.clone(),
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
    /// Returns `None` if page stats are unavailable (caller should assume all rows).
    /// Returns `Some(vec![])` if no pages match (caller can skip entirely).
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

        let mut combined_ranges: Option<Vec<Range<i64>>> = None;

        for filter in &self.filters {
            let converter = match StatisticsConverter::try_new(
                &filter.col_name,
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

            combined_ranges = match combined_ranges {
                None if merged.is_empty() => return Some(vec![]),
                None => Some(merged),
                Some(_) if merged.is_empty() => return Some(vec![]),
                Some(existing) => {
                    let intersected = Self::intersect_ranges(&existing, &merged);
                    if intersected.is_empty() {
                        return Some(vec![]);
                    }
                    Some(intersected)
                }
            };
        }

        combined_ranges
    }

    /// Return all row IDs within pages that match the filters for a row group.
    ///
    /// Expands matching page ranges into individual row IDs. Used for OR-mode
    /// queries where the page pruner's candidates are UNION'd with index bitset rows.
    pub fn candidate_row_ids(&self, rg_idx: usize, rg_first_row: i64, rg_num_rows: i64) -> Vec<i64> {
        if self.filters.is_empty() {
            return (rg_first_row..rg_first_row + rg_num_rows).collect();
        }

        let ranges = match self.compute_page_ranges(rg_idx, rg_first_row) {
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

    /// Filter row IDs by intersecting with page ranges (AND mode).
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

    fn compare<T: Ord>(min: T, max: T, val: T, op: &Operator) -> bool {
        match op {
            Operator::Lt => min < val,
            Operator::LtEq => min <= val,
            Operator::Gt => max > val,
            Operator::GtEq => max >= val,
            Operator::Eq => min <= val && val <= max,
            Operator::NotEq => !(min == val && max == val),
            _ => true,
        }
    }
}
