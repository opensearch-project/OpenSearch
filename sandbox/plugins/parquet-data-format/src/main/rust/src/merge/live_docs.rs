/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared live-docs bitset check for merge filtering.
//!
//! Live bits arrive as a packed bitset in Lucene `FixedBitSet#getBits()` layout: bit `i` set
//! means row `i` is alive. Callers hold an `Option<...>` and treat `None` as "all alive"; this
//! module owns only the core word/bit lookup so the sorted and unsorted mergers cannot drift.

/// Returns `true` if the row at `abs_row_id` is alive in `words`.
///
/// - Row at or beyond `num_rows` → alive (defensive: the Java contract is that absent bits mean alive)
/// - Word beyond the bitset length → alive (same reasoning)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_specific_bits() {
        let words = [0b101u64]; // rows 0,2 alive; row 1 dead
        assert!(is_alive_in_words(&words, 3, 0));
        assert!(!is_alive_in_words(&words, 3, 1));
        assert!(is_alive_in_words(&words, 3, 2));
    }

    #[test]
    fn test_beyond_num_rows_is_alive() {
        let words = [0b101u64];
        assert!(is_alive_in_words(&words, 3, 99));
    }

    #[test]
    fn test_beyond_word_length_is_alive() {
        // num_rows says 130 rows exist but only one word was supplied.
        let words = [0u64];
        assert!(is_alive_in_words(&words, 130, 100));
    }

    #[test]
    fn test_crosses_word_boundary() {
        let words = [0u64, 0b1u64]; // only row 64 alive
        assert!(!is_alive_in_words(&words, 128, 63));
        assert!(is_alive_in_words(&words, 128, 64));
        assert!(!is_alive_in_words(&words, 128, 65));
    }
}
