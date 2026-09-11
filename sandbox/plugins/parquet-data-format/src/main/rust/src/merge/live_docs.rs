/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared live-docs bitset utilities for merge filtering.
//!
//! A `LiveBits` is an optional packed bitset in Lucene `FixedBitSet#getBits()` layout:
//! bit `i` set means row `i` is alive. `None` means all rows are alive (fast path).

use std::sync::Arc;

/// Per-file live-docs filter. `None` = all rows alive (no filtering needed).
pub type LiveBits = Option<Arc<Vec<u64>>>;

/// Returns `true` if the row at `abs_row_id` is alive.
///
/// - `None` bits → all alive
/// - Row beyond `num_rows` → alive (defensive)
/// - Word beyond bitset length → alive (defensive)
#[inline]
pub fn is_alive(bits: &LiveBits, num_rows: u64, abs_row_id: u64) -> bool {
    match bits {
        None => true,
        Some(words) => is_alive_in_words(words, num_rows, abs_row_id),
    }
}

/// Core bit check against a raw word slice.
#[inline]
pub fn is_alive_in_words(words: &[u64], num_rows: u64, abs_row_id: u64) -> bool {
    if abs_row_id >= num_rows {
        return true;
    }
    let word_idx = (abs_row_id / 64) as usize;
    let bit_idx = abs_row_id % 64;
    match words.get(word_idx) {
        Some(&word) => (word & (1u64 << bit_idx)) != 0,
        None => true,
    }
}

/// Builds a boolean filter mask for a batch of `batch_len` rows starting at `base_row_id`.
///
/// Returns `None` if all rows in the batch are alive (caller should skip filtering).
/// Otherwise returns `(mask, alive_count)`.
#[inline]
pub fn build_filter_mask(
    bits: &LiveBits,
    num_rows: u64,
    base_row_id: u64,
    batch_len: usize,
) -> Option<(Vec<bool>, usize)> {
    match bits {
        None => None,
        Some(words) => {
            let mut mask = Vec::with_capacity(batch_len);
            let mut alive_count = 0usize;
            for i in 0..batch_len {
                let alive = is_alive_in_words(words, num_rows, base_row_id + i as u64);
                if alive {
                    alive_count += 1;
                }
                mask.push(alive);
            }
            if alive_count == batch_len {
                None // all alive — caller skips filter_record_batch
            } else {
                Some((mask, alive_count))
            }
        }
    }
}

/// Counts total alive rows in `[0, num_rows)`.
/// Returns `num_rows` if bits is `None`.
pub fn count_alive(bits: &LiveBits, num_rows: u64) -> u64 {
    match bits {
        None => num_rows,
        Some(words) => {
            let full_words = (num_rows / 64) as usize;
            let remainder = num_rows % 64;
            let mut count: u64 = 0;

            for &word in words.iter().take(full_words) {
                count += word.count_ones() as u64;
            }

            if remainder > 0 {
                if let Some(&last_word) = words.get(full_words) {
                    let mask = (1u64 << remainder) - 1;
                    count += (last_word & mask).count_ones() as u64;
                }
            }

            count
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_is_all_alive() {
        let bits: LiveBits = None;
        for i in 0..100 {
            assert!(is_alive(&bits, 100, i));
        }
    }

    #[test]
    fn test_specific_bits() {
        let bits: LiveBits = Some(Arc::new(vec![0b101])); // rows 0,2 alive; row 1 dead
        assert!(is_alive(&bits, 3, 0));
        assert!(!is_alive(&bits, 3, 1));
        assert!(is_alive(&bits, 3, 2));
    }

    #[test]
    fn test_beyond_num_rows_is_alive() {
        let bits: LiveBits = Some(Arc::new(vec![0b101]));
        assert!(is_alive(&bits, 3, 99));
    }

    #[test]
    fn test_build_filter_mask_all_alive() {
        let bits: LiveBits = None;
        assert!(build_filter_mask(&bits, 100, 0, 10).is_none());
    }

    #[test]
    fn test_build_filter_mask_some_dead() {
        let bits: LiveBits = Some(Arc::new(vec![0b1011])); // rows 0,1,3 alive; row 2 dead
        let result = build_filter_mask(&bits, 4, 0, 4);
        assert!(result.is_some());
        let (mask, count) = result.unwrap();
        assert_eq!(mask, vec![true, true, false, true]);
        assert_eq!(count, 3);
    }

    #[test]
    fn test_build_filter_mask_all_alive_in_batch() {
        let bits: LiveBits = Some(Arc::new(vec![0xFF]));
        assert!(build_filter_mask(&bits, 8, 0, 4).is_none());
    }

    #[test]
    fn test_count_alive() {
        let bits: LiveBits = Some(Arc::new(vec![0b1011]));
        assert_eq!(count_alive(&bits, 4), 3);
    }

    #[test]
    fn test_count_alive_none() {
        let bits: LiveBits = None;
        assert_eq!(count_alive(&bits, 100), 100);
    }

    #[test]
    fn test_count_alive_multi_word() {
        let bits: LiveBits = Some(Arc::new(vec![u64::MAX, 0b11]));
        assert_eq!(count_alive(&bits, 66), 66);
    }
}
