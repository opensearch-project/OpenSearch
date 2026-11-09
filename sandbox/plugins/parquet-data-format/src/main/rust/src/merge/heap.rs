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

/// Compares two non-null elements without allocating byte buffers for string or
/// binary values. Other scalar types reuse the existing `SortKey` conversion,
/// which preserves their established ordering without heap allocation.
#[inline]
fn compare_elements(
    values: &dyn Array,
    a: usize,
    b: usize,
    dtype: &ArrowDataType,
) -> MergeResult<Ordering> {
    let ordering = match dtype {
        ArrowDataType::Utf8 => values
            .as_string::<i32>()
            .value(a)
            .as_bytes()
            .cmp(values.as_string::<i32>().value(b).as_bytes()),
        ArrowDataType::LargeUtf8 => values
            .as_string::<i64>()
            .value(a)
            .as_bytes()
            .cmp(values.as_string::<i64>().value(b).as_bytes()),
        ArrowDataType::Binary => values
            .as_binary::<i32>()
            .value(a)
            .cmp(values.as_binary::<i32>().value(b)),
        ArrowDataType::LargeBinary => values
            .as_binary::<i64>()
            .value(a)
            .cmp(values.as_binary::<i64>().value(b)),
        _ => {
            let left = get_array_sort_value(values, a, dtype, false, false)?;
            let right = get_array_sort_value(values, b, dtype, false, false)?;
            left.cmp(&right)
        }
    };
    Ok(ordering)
}

/// Scans `values[start..end]` and returns the index of the winning non-null
/// element for the requested reduction mode: the minimum element when
/// `max == false`, the maximum element when `max == true`. Returns `None` when
/// every element in the range is null (or the range is empty).
///
/// Comparison happens fully in place via [`compare_elements`] — no `SortKey`
/// (and therefore no `Vec<u8>` for string/binary) is materialized during the
/// scan. This is the shared winner-selection helper used by both the k-way
/// merge sort-value extraction and the per-row list reduction.
fn winning_index(
    values: &dyn Array,
    start: usize,
    end: usize,
    dtype: &ArrowDataType,
    max: bool,
) -> MergeResult<Option<usize>> {
    let mut winner: Option<usize> = None;
    for index in start..end {
        if values.is_null(index) {
            continue;
        }
        match winner {
            None => winner = Some(index),
            Some(current) => {
                let ord = compare_elements(values, index, current, dtype)?;
                // For MIN keep the smaller element, for MAX keep the larger.
                let replace = if max {
                    ord == Ordering::Greater
                } else {
                    ord == Ordering::Less
                };
                if replace {
                    winner = Some(index);
                }
            }
        }
    }
    Ok(winner)
}

/// Reduces the non-null elements of `values[start..end]` to a single
/// [`SortKey`] using the given reduction `mode` (MIN when `max == false`, MAX
/// when `max == true`). Exactly one `SortKey` is materialized — for the winning
/// element — after the allocation-free [`winning_index`] scan. An all-null or
/// empty range yields the null sentinel.
fn get_reduced_value(
    values: &dyn Array,
    start: usize,
    end: usize,
    dtype: &ArrowDataType,
    null_first: bool,
    max: bool,
) -> MergeResult<SortKey> {
    match winning_index(values, start, end, dtype, max)? {
        // The winner is a scalar leaf element; `max` is irrelevant when
        // materializing a single scalar, so pass `false`.
        Some(index) => get_array_sort_value(values, index, dtype, false, false),
        None => Ok(null_sort_key(null_first)),
    }
}

fn get_list_reduced<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    row: usize,
    child_type: &ArrowDataType,
    null_first: bool,
    max: bool,
) -> MergeResult<SortKey> {
    let offsets = list.value_offsets();
    get_reduced_value(
        list.values().as_ref(),
        offsets[row].as_usize(),
        offsets[row + 1].as_usize(),
        child_type,
        null_first,
        max,
    )
}

#[inline]
fn get_array_sort_value(
    col: &dyn Array,
    row: usize,
    dtype: &ArrowDataType,
    null_first: bool,
    max: bool,
) -> MergeResult<SortKey> {
    if col.is_null(row) {
        return Ok(null_sort_key(null_first));
    }
    // `max` only affects LIST/FixedSizeList reduction (MIN vs MAX element). For
    // scalar columns the value at `row` is the sort key regardless of mode.
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
        ArrowDataType::List(field) => get_list_reduced(
            col.as_list::<i32>(),
            row,
            field.data_type(),
            null_first,
            max,
        )?,
        ArrowDataType::LargeList(field) => get_list_reduced(
            col.as_list::<i64>(),
            row,
            field.data_type(),
            null_first,
            max,
        )?,
        ArrowDataType::FixedSizeList(field, size) => {
            let start = row * *size as usize;
            get_reduced_value(
                col.as_fixed_size_list().values().as_ref(),
                start,
                start + *size as usize,
                field.data_type(),
                null_first,
                max,
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
    max: bool,
) -> MergeResult<SortKey> {
    get_array_sort_value(batch.column(col_idx).as_ref(), row, dtype, null_first, max)
}

fn reduce_list_array<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    child_type: &ArrowDataType,
    max: bool,
) -> MergeResult<ArrayRef> {
    let values = list.values();
    let offsets = list.value_offsets();
    let indices = (0..list.len())
        .map(|row| {
            if list.is_null(row) {
                Ok(None)
            } else {
                Ok(winning_index(
                    values.as_ref(),
                    offsets[row].as_usize(),
                    offsets[row + 1].as_usize(),
                    child_type,
                    max,
                )?
                .map(|i| i as u64))
            }
        })
        .collect::<MergeResult<Vec<_>>>()?;
    Ok(take(values.as_ref(), &UInt64Array::from(indices), None)?)
}

/// Returns the scalar type used as the physical sort key. LIST columns reduce
/// to the winning (MIN or MAX, per the resolved sort mode) non-null element;
/// scalar columns keep their own type.
pub(crate) fn reduced_sort_type(dtype: &ArrowDataType) -> ArrowDataType {
    match dtype {
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _) => field.data_type().clone(),
        _ => dtype.clone(),
    }
}

/// Materializes one temporary scalar reduced key per row for Arrow's
/// RowConverter, using the resolved reduction mode (`max == true` selects the
/// MAX element, otherwise MIN). Scalar fields ignore the reduction mode and are
/// returned unchanged. The returned array is used only while sorting and is
/// never written to Parquet.
pub(crate) fn reduced_sort_array(array: &ArrayRef, max: bool) -> MergeResult<ArrayRef> {
    match array.data_type() {
        ArrowDataType::List(field) => {
            reduce_list_array(array.as_list::<i32>(), field.data_type(), max)
        }
        ArrowDataType::LargeList(field) => {
            reduce_list_array(array.as_list::<i64>(), field.data_type(), max)
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
                        Ok(winning_index(
                            values.as_ref(),
                            start,
                            start + *size as usize,
                            field.data_type(),
                            max,
                        )?
                        .map(|i| i as u64))
                    }
                })
                .collect::<MergeResult<Vec<_>>>()?;
            Ok(take(values.as_ref(), &UInt64Array::from(indices), None)?)
        }
        // Scalar fields ignore the reduction mode.
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
    max_sort_modes: &[bool],
) -> MergeResult<Vec<SortKey>> {
    let mut values = Vec::with_capacity(col_indices.len());
    for (i, (col_idx, dtype)) in col_indices.iter().zip(dtypes.iter()).enumerate() {
        let nf = nulls_first.get(i).copied().unwrap_or(false);
        let max = max_sort_modes.get(i).copied().unwrap_or(false);
        values.push(get_sort_value(batch, row, *col_idx, dtype, nf, max)?);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, Int64Builder, ListBuilder, RecordBatch, StringBuilder};
    use arrow::datatypes::{Field, Schema};

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Builds a `List<Int64>` from rows. `None` row => null list; empty inner
    /// vec => empty (non-null) list; inner `None` => null element.
    fn int_list(rows: &[Option<Vec<Option<i64>>>]) -> ArrayRef {
        let mut b = ListBuilder::new(Int64Builder::new());
        for row in rows {
            match row {
                None => b.append(false),
                Some(elems) => {
                    for e in elems {
                        match e {
                            Some(v) => b.values().append_value(*v),
                            None => b.values().append_null(),
                        }
                    }
                    b.append(true);
                }
            }
        }
        Arc::new(b.finish())
    }

    /// Builds a `List<Utf8>` from rows (same null conventions as `int_list`).
    fn str_list(rows: &[Option<Vec<Option<&str>>>]) -> ArrayRef {
        let mut b = ListBuilder::new(StringBuilder::new());
        for row in rows {
            match row {
                None => b.append(false),
                Some(elems) => {
                    for e in elems {
                        match e {
                            Some(v) => b.values().append_value(*v),
                            None => b.values().append_null(),
                        }
                    }
                    b.append(true);
                }
            }
        }
        Arc::new(b.finish())
    }

    fn reduced_ints(array: &ArrayRef, max: bool) -> Vec<Option<i64>> {
        let reduced = reduced_sort_array(array, max).unwrap();
        let arr = reduced.as_primitive::<Int64Type>();
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            })
            .collect()
    }

    fn reduced_strs(array: &ArrayRef, max: bool) -> Vec<Option<String>> {
        let reduced = reduced_sort_array(array, max).unwrap();
        let arr = reduced.as_string::<i32>();
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                }
            })
            .collect()
    }

    // ── LIST reduction: MIN vs MAX for numeric values ────────────────────

    #[test]
    fn test_reduce_int_list_min() {
        let list = int_list(&[
            Some(vec![Some(3), Some(1), Some(2)]),
            Some(vec![Some(10), Some(-5)]),
        ]);
        // MIN reduction picks the smallest element of each row.
        assert_eq!(reduced_ints(&list, false), vec![Some(1), Some(-5)]);
    }

    #[test]
    fn test_reduce_int_list_max() {
        let list = int_list(&[
            Some(vec![Some(3), Some(1), Some(2)]),
            Some(vec![Some(10), Some(-5)]),
        ]);
        // MAX reduction picks the largest element of each row.
        assert_eq!(reduced_ints(&list, true), vec![Some(3), Some(10)]);
    }

    // ── LIST reduction: MIN vs MAX for Utf8 values ───────────────────────

    #[test]
    fn test_reduce_utf8_list_min_and_max() {
        let list = str_list(&[
            Some(vec![Some("banana"), Some("apple"), Some("cherry")]),
            Some(vec![Some("x"), Some("m")]),
        ]);
        assert_eq!(
            reduced_strs(&list, false),
            vec![Some("apple".to_string()), Some("m".to_string())]
        );
        assert_eq!(
            reduced_strs(&list, true),
            vec![Some("cherry".to_string()), Some("x".to_string())]
        );
    }

    // ── Null / empty / all-null semantics ────────────────────────────────

    #[test]
    fn test_reduce_null_empty_and_all_null_rows() {
        let list = int_list(&[
            None,                               // null list => null key
            Some(vec![]),                       // empty list => null key
            Some(vec![None, None]),             // all-null elements => null key
            Some(vec![None, Some(7), Some(4)]), // mixed => ignores null element
        ]);
        // Both MIN and MAX yield null for null/empty/all-null rows.
        assert_eq!(reduced_ints(&list, false), vec![None, None, None, Some(4)]);
        assert_eq!(reduced_ints(&list, true), vec![None, None, None, Some(7)]);
    }

    // ── winning_index directly ───────────────────────────────────────────

    #[test]
    fn test_winning_index_min_max_and_empty() {
        let values = Int64Array::from(vec![5i64, 1, 9, 3]);
        let arr: &dyn Array = &values;
        assert_eq!(
            winning_index(arr, 0, 4, &ArrowDataType::Int64, false).unwrap(),
            Some(1)
        );
        assert_eq!(
            winning_index(arr, 0, 4, &ArrowDataType::Int64, true).unwrap(),
            Some(2)
        );
        // Empty range yields None regardless of mode.
        assert_eq!(
            winning_index(arr, 2, 2, &ArrowDataType::Int64, false).unwrap(),
            None
        );
    }

    // ── get_sort_values: scalar ignores mode; list honors mode ───────────

    fn scalar_key_i64(sk: &SortKey) -> i64 {
        match sk {
            SortKey::Int(v) => *v,
            other => panic!("expected Int key, got {:?}", other),
        }
    }

    #[test]
    fn test_get_sort_values_scalar_ignores_mode() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            ArrowDataType::Int64,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42i64]))]).unwrap();
        // Scalar field: MIN vs MAX must produce the same key.
        let min =
            get_sort_values(&batch, 0, &[0], &[ArrowDataType::Int64], &[false], &[false]).unwrap();
        let max =
            get_sort_values(&batch, 0, &[0], &[ArrowDataType::Int64], &[false], &[true]).unwrap();
        assert_eq!(scalar_key_i64(&min[0]), 42);
        assert_eq!(scalar_key_i64(&max[0]), 42);
    }

    #[test]
    fn test_get_sort_values_list_honors_mode() {
        let list = int_list(&[Some(vec![Some(3), Some(1), Some(8)])]);
        let list_type = list.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            list_type.clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![list]).unwrap();

        let min =
            get_sort_values(&batch, 0, &[0], &[list_type.clone()], &[false], &[false]).unwrap();
        let max = get_sort_values(&batch, 0, &[0], &[list_type], &[false], &[true]).unwrap();
        assert_eq!(scalar_key_i64(&min[0]), 1); // MIN element
        assert_eq!(scalar_key_i64(&max[0]), 8); // MAX element
    }

    #[test]
    fn test_get_sort_values_list_null_row_uses_null_sentinel() {
        let list = int_list(&[None]);
        let list_type = list.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            list_type.clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![list]).unwrap();

        // null_first => NullFirst sentinel; null_last => NullLast sentinel.
        let nf = get_sort_values(&batch, 0, &[0], &[list_type.clone()], &[true], &[false]).unwrap();
        let nl = get_sort_values(&batch, 0, &[0], &[list_type], &[false], &[false]).unwrap();
        assert!(matches!(nf[0], SortKey::NullFirst));
        assert!(matches!(nl[0], SortKey::NullLast));
    }

    #[test]
    fn test_reduce_str_list_min_used_by_string_array() {
        // Guards against accidental regression to per-element Vec<u8> allocation:
        // the reducer must still select the correct winner for strings.
        let list = str_list(&[Some(vec![Some("delta"), Some("alpha"), Some("charlie")])]);
        assert_eq!(reduced_strs(&list, false), vec![Some("alpha".to_string())]);
        assert_eq!(reduced_strs(&list, true), vec![Some("delta".to_string())]);
    }
}
