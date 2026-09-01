/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, OffsetSizeTrait, RecordBatch, UInt64Array,
};
use arrow::compute::take;
use arrow::datatypes::{
    DataType as ArrowDataType, Date32Type, Date64Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
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
    UInt(u64),
    Float(f64),
    Bool(bool),
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
            (SortKey::UInt(a), SortKey::UInt(b)) => a.cmp(b),
            (SortKey::Float(a), SortKey::Float(b)) => a.total_cmp(b),
            (SortKey::Bool(a), SortKey::Bool(b)) => a.cmp(b),
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
            let is_null_cmp = matches!(av, SortKey::NullFirst | SortKey::NullLast)
                || matches!(bv, SortKey::NullFirst | SortKey::NullLast);
            return if reverse && !is_null_cmp {
                ord.reverse()
            } else {
                ord
            };
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
fn null_sort_key(null_first: bool) -> SortKey {
    if null_first {
        SortKey::NullFirst
    } else {
        SortKey::NullLast
    }
}

fn get_min_value(
    values: &dyn Array,
    start: usize,
    end: usize,
    dtype: &ArrowDataType,
    null_first: bool,
) -> MergeResult<SortKey> {
    let mut minimum = None;
    for index in start..end {
        if values.is_null(index) {
            continue;
        }
        let candidate = get_array_sort_value(values, index, dtype, false)?;
        if minimum.as_ref().is_none_or(|current| candidate < *current) {
            minimum = Some(candidate);
        }
    }
    Ok(minimum.unwrap_or_else(|| null_sort_key(null_first)))
}

fn get_list_min<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    row: usize,
    child_type: &ArrowDataType,
    null_first: bool,
) -> MergeResult<SortKey> {
    let offsets = list.value_offsets();
    get_min_value(
        list.values().as_ref(),
        offsets[row].as_usize(),
        offsets[row + 1].as_usize(),
        child_type,
        null_first,
    )
}

#[inline]
fn get_array_sort_value(
    col: &dyn Array,
    row: usize,
    dtype: &ArrowDataType,
    null_first: bool,
) -> MergeResult<SortKey> {
    if col.is_null(row) {
        return Ok(null_sort_key(null_first));
    }
    let key = match dtype {
        ArrowDataType::Int64 => SortKey::Int(col.as_primitive::<Int64Type>().value(row)),
        ArrowDataType::Int32 => SortKey::Int(col.as_primitive::<Int32Type>().value(row) as i64),
        ArrowDataType::Int16 => SortKey::Int(col.as_primitive::<Int16Type>().value(row) as i64),
        ArrowDataType::Int8 => SortKey::Int(col.as_primitive::<Int8Type>().value(row) as i64),
        ArrowDataType::UInt64 => SortKey::UInt(col.as_primitive::<UInt64Type>().value(row)),
        ArrowDataType::UInt32 => SortKey::UInt(col.as_primitive::<UInt32Type>().value(row) as u64),
        ArrowDataType::UInt16 => SortKey::UInt(col.as_primitive::<UInt16Type>().value(row) as u64),
        ArrowDataType::UInt8 => SortKey::UInt(col.as_primitive::<UInt8Type>().value(row) as u64),
        ArrowDataType::Date32 => SortKey::Int(col.as_primitive::<Date32Type>().value(row) as i64),
        ArrowDataType::Date64 => SortKey::Int(col.as_primitive::<Date64Type>().value(row)),
        ArrowDataType::Timestamp(unit, _) => SortKey::Int(match unit {
            arrow::datatypes::TimeUnit::Second => {
                col.as_primitive::<TimestampSecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                col.as_primitive::<TimestampMillisecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                col.as_primitive::<TimestampMicrosecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                col.as_primitive::<TimestampNanosecondType>().value(row)
            }
        }),
        ArrowDataType::Duration(unit) => SortKey::Int(match unit {
            arrow::datatypes::TimeUnit::Second => {
                col.as_primitive::<DurationSecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                col.as_primitive::<DurationMillisecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                col.as_primitive::<DurationMicrosecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                col.as_primitive::<DurationNanosecondType>().value(row)
            }
        }),
        ArrowDataType::Float64 => SortKey::Float(col.as_primitive::<Float64Type>().value(row)),
        ArrowDataType::Float32 => {
            SortKey::Float(col.as_primitive::<Float32Type>().value(row) as f64)
        }
        ArrowDataType::Float16 => {
            SortKey::Float(col.as_primitive::<Float16Type>().value(row).to_f32() as f64)
        }
        ArrowDataType::Boolean => SortKey::Bool(col.as_boolean().value(row)),
        ArrowDataType::Utf8 => {
            SortKey::Bytes(col.as_string::<i32>().value(row).as_bytes().to_vec())
        }
        ArrowDataType::LargeUtf8 => {
            SortKey::Bytes(col.as_string::<i64>().value(row).as_bytes().to_vec())
        }
        ArrowDataType::Binary => SortKey::Bytes(col.as_binary::<i32>().value(row).to_vec()),
        ArrowDataType::LargeBinary => SortKey::Bytes(col.as_binary::<i64>().value(row).to_vec()),
        ArrowDataType::List(field) => {
            get_list_min(col.as_list::<i32>(), row, field.data_type(), null_first)?
        }
        ArrowDataType::LargeList(field) => {
            get_list_min(col.as_list::<i64>(), row, field.data_type(), null_first)?
        }
        ArrowDataType::FixedSizeList(field, size) => {
            let start = row * *size as usize;
            get_min_value(
                col.as_fixed_size_list().values().as_ref(),
                start,
                start + *size as usize,
                field.data_type(),
                null_first,
            )?
        }
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
pub fn get_sort_value(
    batch: &RecordBatch,
    row: usize,
    col_idx: usize,
    dtype: &ArrowDataType,
    null_first: bool,
) -> MergeResult<SortKey> {
    get_array_sort_value(batch.column(col_idx).as_ref(), row, dtype, null_first)
}

fn min_value_index(
    values: &dyn Array,
    start: usize,
    end: usize,
    dtype: &ArrowDataType,
) -> MergeResult<Option<u64>> {
    let mut minimum = None;
    for index in start..end {
        if values.is_null(index) {
            continue;
        }
        let candidate = get_array_sort_value(values, index, dtype, false)?;
        if minimum
            .as_ref()
            .is_none_or(|(_, current): &(u64, SortKey)| candidate < *current)
        {
            minimum = Some((index as u64, candidate));
        }
    }
    Ok(minimum.map(|(index, _)| index))
}

fn reduce_list_array<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    child_type: &ArrowDataType,
) -> MergeResult<ArrayRef> {
    let values = list.values();
    let offsets = list.value_offsets();
    let indices = (0..list.len())
        .map(|row| {
            if list.is_null(row) {
                Ok(None)
            } else {
                min_value_index(
                    values.as_ref(),
                    offsets[row].as_usize(),
                    offsets[row + 1].as_usize(),
                    child_type,
                )
            }
        })
        .collect::<MergeResult<Vec<_>>>()?;
    Ok(take(values.as_ref(), &UInt64Array::from(indices), None)?)
}

/// Returns the scalar type used as the physical sort key. LIST columns always
/// reduce to the natural minimum non-null element; no customer-selectable mode
/// is exposed by the Parquet writer.
pub(crate) fn min_reduced_sort_type(dtype: &ArrowDataType) -> ArrowDataType {
    match dtype {
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _) => field.data_type().clone(),
        _ => dtype.clone(),
    }
}

/// Materializes one temporary scalar MIN key per row for Arrow's RowConverter.
/// The returned array is used only while sorting and is never written to Parquet.
pub(crate) fn min_reduced_sort_array(array: &ArrayRef) -> MergeResult<ArrayRef> {
    match array.data_type() {
        ArrowDataType::List(field) => reduce_list_array(array.as_list::<i32>(), field.data_type()),
        ArrowDataType::LargeList(field) => {
            reduce_list_array(array.as_list::<i64>(), field.data_type())
        }
        ArrowDataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let values = list.values();
            let indices = (0..list.len())
                .map(|row| {
                    if list.is_null(row) {
                        Ok(None)
                    } else {
                        let start = row * *size as usize;
                        min_value_index(
                            values.as_ref(),
                            start,
                            start + *size as usize,
                            field.data_type(),
                        )
                    }
                })
                .collect::<MergeResult<Vec<_>>>()?;
            Ok(take(values.as_ref(), &UInt64Array::from(indices), None)?)
        }
        _ => Ok(array.clone()),
    }
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
