// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ops::Range;

use arrow::array::{Array, BooleanArray, BooleanBufferBuilder};
use arrow::buffer::{BooleanBuffer, MutableBuffer};
use arrow::util::bit_iterator::BitIndexIterator;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

/// A selection of rows using a boolean array.
#[derive(Debug, Clone, PartialEq)]
pub struct BooleanSelection {
    selectors: BooleanBuffer,
}

impl BooleanSelection {
    /// Create a new BooleanSelection from a list of BooleanArray.
    pub fn from_filters(filters: &[BooleanArray]) -> Self {
        let arrays: Vec<&dyn Array> = filters.iter().map(|x| x as &dyn Array).collect();
        let result = arrow::compute::concat(&arrays).unwrap().into_data();
        let (boolean_array, _null) = BooleanArray::from(result).into_parts();
        BooleanSelection {
            selectors: boolean_array,
        }
    }

    /// Create a new BooleanSelection with all rows unselected
    pub fn new_unselected(row_count: usize) -> Self {
        let buffer = BooleanBuffer::new_unset(row_count);

        BooleanSelection { selectors: buffer }
    }

    /// Create a new BooleanSelection with all rows selected
    pub fn new_selected(row_count: usize) -> Self {
        let buffer = BooleanBuffer::new_set(row_count);

        BooleanSelection { selectors: buffer }
    }

    /// Returns a new BooleanSelection that selects the inverse of this BooleanSelection.
    pub fn as_inverted(&self) -> Self {
        let buffer = !&self.selectors;
        BooleanSelection { selectors: buffer }
    }

    /// Returns the number of rows in this BooleanSelection.
    pub fn len(&self) -> usize {
        self.selectors.len()
    }

    /// Check if the BooleanSelection is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of rows selected by this BooleanSelection.
    pub fn row_count(&self) -> usize {
        self.selectors.count_set_bits()
    }

    /// Create a new BooleanSelection from a list of consecutive ranges.
    pub fn from_consecutive_ranges(
        ranges: impl Iterator<Item = Range<usize>>,
        total_rows: usize,
    ) -> Self {
        let mut buffer = BooleanBufferBuilder::new(total_rows);
        let mut last_end = 0;

        for range in ranges {
            let len = range.end - range.start;
            if len == 0 {
                continue;
            }

            if range.start > last_end {
                buffer.append_n(range.start - last_end, false);
            }
            buffer.append_n(len, true);
            last_end = range.end;
        }

        if last_end != total_rows {
            buffer.append_n(total_rows - last_end, false);
        }

        BooleanSelection {
            selectors: buffer.finish(),
        }
    }

    /// Compute the union of two [`RowSelection`]
    /// For example:
    /// self:      NNYYYYNNYYNYN
    /// other:     NYNNNNNNN
    ///
    /// returned:  NYYYYYNNYYNYN
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        // use arrow::compute::kernels::boolean::or;

        let union_selectors = &self.selectors | &other.selectors;

        BooleanSelection {
            selectors: union_selectors,
        }
    }

    /// Compute the intersection of two [`RowSelection`]
    /// For example:
    /// self:      NNYYYYNNYYNYN
    /// other:     NYNNNNNNY
    ///
    /// returned:  NNNNNNNNYYNYN
    #[must_use]
    pub fn intersection(&self, other: &Self) -> Self {
        let intersection_selectors = &self.selectors & &other.selectors;

        BooleanSelection {
            selectors: intersection_selectors,
        }
    }

    /// Combines this `BooleanSelection` with another using logical AND on the selected bits.
    ///
    /// The `other` `BooleanSelection` must have exactly as many set bits as `self`.
    /// This method will keep only the bits in `self` that are also set in `other`
    /// at the positions corresponding to `self`'s set bits.
    pub fn and_then(&self, other: &Self) -> Self {
        // Ensure that 'other' has exactly as many set bits as 'self'
        debug_assert_eq!(
            self.row_count(),
            other.len(),
            "The 'other' selection must have exactly as many set bits as 'self'."
        );

        if self.len() == other.len() {
            // fast path if the two selections are the same length
            // common if this is the first predicate
            debug_assert_eq!(self.row_count(), self.len());
            return self.intersection(other);
        }

        let mut buffer = MutableBuffer::from_len_zeroed(self.selectors.inner().len());
        buffer.copy_from_slice(self.selectors.values());
        let mut builder = BooleanBufferBuilder::new_from_buffer(buffer, self.len());

        // Create iterators for 'self' and 'other' bits
        let mut other_bits = other.selectors.iter();

        for bit_idx in self.positive_iter() {
            let predicate = other_bits
                .next()
                .expect("Mismatch in set bits between self and other");
            if !predicate {
                builder.set_bit(bit_idx, false);
            }
        }

        BooleanSelection {
            selectors: builder.finish(),
        }
    }

    /// Returns an iterator over the indices of the set bits in this [`BooleanSelection`]
    pub fn positive_iter(&self) -> BitIndexIterator<'_> {
        self.selectors.set_indices()
    }

    /// Returns `true` if this [BooleanSelection] selects any rows
    pub fn selects_any(&self) -> bool {
        self.row_count() > 0
    }

    /// Returns a new BooleanSelection that selects the rows in this BooleanSelection from `offset` to `offset + len`
    pub fn slice(&self, offset: usize, len: usize) -> BooleanArray {
        BooleanArray::new(self.selectors.slice(offset, len), None)
    }
}

impl From<Vec<RowSelector>> for BooleanSelection {
    fn from(selection: Vec<RowSelector>) -> Self {
        let selection = RowSelection::from(selection);
        RowSelection::into(selection)
    }
}

impl From<RowSelection> for BooleanSelection {
    fn from(selection: RowSelection) -> Self {
        let total_rows = selection.row_count();
        let mut builder = BooleanBufferBuilder::new(total_rows);

        for selector in selection.iter() {
            if selector.skip {
                builder.append_n(selector.row_count, false);
            } else {
                builder.append_n(selector.row_count, true);
            }
        }

        BooleanSelection {
            selectors: builder.finish(),
        }
    }
}

impl From<&BooleanSelection> for RowSelection {
    fn from(selection: &BooleanSelection) -> Self {
        let array = BooleanArray::new(selection.selectors.clone(), None);
        RowSelection::from_filters(&[array])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_selection_and_then() {
        // Initial mask: 001011010101
        let self_filters = vec![BooleanArray::from(vec![
            false, false, true, false, true, true, false, true, false, true, false, true,
        ])];
        let self_selection = BooleanSelection::from_filters(&self_filters);

        // Predicate mask (only for selected bits): 001101
        let other_filters = vec![BooleanArray::from(vec![
            false, false, true, true, false, true,
        ])];
        let other_selection = BooleanSelection::from_filters(&other_filters);

        let result = self_selection.and_then(&other_selection);

        // Expected result: 000001010001
        let expected_filters = vec![BooleanArray::from(vec![
            false, false, false, false, false, true, false, true, false, false, false, true,
        ])];
        let expected_selection = BooleanSelection::from_filters(&expected_filters);

        assert_eq!(result, expected_selection);
    }

    #[test]
    #[should_panic(
        expected = "The 'other' selection must have exactly as many set bits as 'self'."
    )]
    fn test_and_then_mismatched_set_bits() {
        let self_filters = vec![BooleanArray::from(vec![true, true, false])];
        let self_selection = BooleanSelection::from_filters(&self_filters);

        // 'other' has only one set bit, but 'self' has two
        let other_filters = vec![BooleanArray::from(vec![true, false, false])];
        let other_selection = BooleanSelection::from_filters(&other_filters);

        // This should panic
        let _ = self_selection.and_then(&other_selection);
    }
}
