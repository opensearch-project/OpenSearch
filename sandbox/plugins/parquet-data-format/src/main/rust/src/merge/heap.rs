/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{
    DataType as ArrowDataType, Date32Type, Date64Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt32Type,
};

use super::error::{MergeError, MergeResult};

// =============================================================================
// SortKey — typed sort value with null ordering baked in
// =============================================================================

#[derive(Debug, Clone)]
pub enum SortKey {
    NullFirst,
    NullLast,
    Int(i64),
    Float(f64),
    Bytes(Vec<u8>),
}

impl Eq for SortKey {}

impl PartialEq for SortKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (SortKey::NullFirst, SortKey::NullFirst) => Ordering::Equal,
            (SortKey::NullFirst, _) => Ordering::Less,
            (_, SortKey::NullFirst) => Ordering::Greater,
            (SortKey::NullLast, SortKey::NullLast) => Ordering::Equal,
            (SortKey::NullLast, _) => Ordering::Greater,
            (_, SortKey::NullLast) => Ordering::Less,
            (SortKey::Int(a), SortKey::Int(b)) => a.cmp(b),
            (SortKey::Float(a), SortKey::Float(b)) => a.total_cmp(b),
            (SortKey::Bytes(a), SortKey::Bytes(b)) => a.cmp(b),
            // Same column always produces the same variant; cross-variant is unreachable.
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// =============================================================================
// Sort-direction helpers
// =============================================================================

/// Lexicographic comparison of two sort-key tuples, respecting per-column
/// sort direction. Returns `Ordering::Equal` when all values match.
#[inline(always)]
pub fn cmp_sort_values(a: &[SortKey], b: &[SortKey], reverse_sorts: &[bool]) -> Ordering {
    for (i, (av, bv)) in a.iter().zip(b.iter()).enumerate() {
        let ord = av.cmp(bv);
        if ord != Ordering::Equal {
            let reverse = reverse_sorts.get(i).copied().unwrap_or(false);
            return if reverse { ord.reverse() } else { ord };
        }
    }
    Ordering::Equal
}

// =============================================================================
// HeapItem for k-way merge
// =============================================================================

#[derive(Debug)]
pub struct HeapItem {
    pub sort_values: Vec<SortKey>,
    pub file_id: usize,
    pub reverse_sorts: Arc<Vec<bool>>,
}

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_values == other.sort_values
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Swap other/self so max-heap behaves as min-heap.
        cmp_sort_values(&other.sort_values, &self.sort_values, &self.reverse_sorts)
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// =============================================================================
// Sort value extraction
// =============================================================================

#[inline]
pub fn get_sort_value(
    batch: &RecordBatch,
    row: usize,
    col_idx: usize,
    dtype: &ArrowDataType,
    null_first: bool,
) -> MergeResult<SortKey> {
    let col = batch.column(col_idx);
    if col.is_null(row) {
        return Ok(if null_first { SortKey::NullFirst } else { SortKey::NullLast });
    }
    let key = match dtype {
        // Integer types → SortKey::Int
        ArrowDataType::Int64 => SortKey::Int(col.as_primitive::<Int64Type>().value(row)),
        ArrowDataType::Int32 => SortKey::Int(col.as_primitive::<Int32Type>().value(row) as i64),
        ArrowDataType::Int16 => SortKey::Int(col.as_primitive::<Int16Type>().value(row) as i64),
        ArrowDataType::Int8 => SortKey::Int(col.as_primitive::<Int8Type>().value(row) as i64),
        ArrowDataType::UInt32 => SortKey::Int(col.as_primitive::<UInt32Type>().value(row) as i64),
        ArrowDataType::Date32 => SortKey::Int(col.as_primitive::<Date32Type>().value(row) as i64),
        ArrowDataType::Date64 => SortKey::Int(col.as_primitive::<Date64Type>().value(row)),
        ArrowDataType::Timestamp(unit, _) => SortKey::Int(match unit {
            arrow::datatypes::TimeUnit::Second => col.as_primitive::<TimestampSecondType>().value(row),
            arrow::datatypes::TimeUnit::Millisecond => col.as_primitive::<TimestampMillisecondType>().value(row),
            arrow::datatypes::TimeUnit::Microsecond => col.as_primitive::<TimestampMicrosecondType>().value(row),
            arrow::datatypes::TimeUnit::Nanosecond => col.as_primitive::<TimestampNanosecondType>().value(row),
        }),
        ArrowDataType::Duration(unit) => SortKey::Int(match unit {
            arrow::datatypes::TimeUnit::Second => col.as_primitive::<DurationSecondType>().value(row),
            arrow::datatypes::TimeUnit::Millisecond => col.as_primitive::<DurationMillisecondType>().value(row),
            arrow::datatypes::TimeUnit::Microsecond => col.as_primitive::<DurationMicrosecondType>().value(row),
            arrow::datatypes::TimeUnit::Nanosecond => col.as_primitive::<DurationNanosecondType>().value(row),
        }),

        // Float types → SortKey::Float
        ArrowDataType::Float64 => SortKey::Float(col.as_primitive::<Float64Type>().value(row)),
        ArrowDataType::Float32 => SortKey::Float(col.as_primitive::<Float32Type>().value(row) as f64),

        // String types → SortKey::Bytes
        ArrowDataType::Utf8 => SortKey::Bytes(col.as_string::<i32>().value(row).as_bytes().to_vec()),
        ArrowDataType::LargeUtf8 => SortKey::Bytes(col.as_string::<i64>().value(row).as_bytes().to_vec()),

        other => {
            return Err(MergeError::Logic(format!(
                "Unsupported sort column type: {:?}",
                other
            )));
        }
    };
    Ok(key)
}

#[inline]
pub fn get_sort_values(
    batch: &RecordBatch,
    row: usize,
    col_indices: &[usize],
    dtypes: &[ArrowDataType],
    nulls_first: &[bool],
) -> MergeResult<Vec<SortKey>> {
    let mut values = Vec::with_capacity(col_indices.len());
    for (i, (col_idx, dtype)) in col_indices.iter().zip(dtypes.iter()).enumerate() {
        let nf = nulls_first.get(i).copied().unwrap_or(false);
        values.push(get_sort_value(batch, row, *col_idx, dtype, nf)?);
    }
    Ok(values)
}
