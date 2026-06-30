//! Row-selection utilities for the indexed streaming path.
//!
//! Given a candidate `RoaringBitmap` of RG-relative doc positions, this
//! module produces:
//!
//! 1. A parquet [`RowSelection`] (`build_row_selection_with_min_skip_run`)
//!    whose "skip" runs are each at least `min_skip_run` long. Short gaps
//!    are absorbed into surrounding selects, trading a little over-read
//!    for a smaller, more block-friendly selector list.
//! 2. A [`PositionMap`] that translates delivered-batch-row index back to
//!    RG-relative position, which post-decode masks need for alignment.
//! 3. A delivered-row [`BooleanArray`] mask (`build_mask`) aligned to the
//!    rows parquet actually hands back.
//!
//! The three together replace the pre-refactor "`FilterStrategy =
//! RowSelection | BooleanMask`" branching. One code path, one knob.

use std::sync::Arc;

use datafusion::arrow::array::BooleanArray;
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use roaring::RoaringBitmap;

/// Build a `RowSelection` from a candidate `RoaringBitmap`, treating any
/// "gap" (consecutive non-set bits) shorter than `min_skip_run` as NOT worth
/// skipping — it gets absorbed into the surrounding `select` run. Gaps of
/// `>= min_skip_run` emit a real `skip`.
///
/// Trade-off knob:
/// - `min_skip_run = 1` → row-granular selection (every non-set bit is a
///   `skip`). Zero over-read but selector count is O(bitmap flips) and can
///   explode for noisy dense bitmaps.
/// - `min_skip_run = large` → coarser selection, fewer selectors, some
///   non-matching rows over-read (parquet reads them; the caller's mask
///   drops them post-decode). Run length of every `skip` is guaranteed
///   `>= min_skip_run`; `select` runs absorb tiny gaps.
/// - `min_skip_run = rg_num_rows + 1` → single big `select`, equivalent
///   to a full scan.
///
/// Returns a `RowSelection` ready for parquet. The caller must track a
/// [`PositionMap`] alongside it to translate delivered batch rows back to
/// RG-relative positions for post-decode mask alignment.
pub fn build_row_selection_with_min_skip_run(
    candidates: &RoaringBitmap,
    rg_num_rows: usize,
    min_skip_run: usize,
) -> RowSelection {
    if rg_num_rows == 0 {
        return RowSelection::from(Vec::<RowSelector>::new());
    }
    if candidates.is_empty() {
        return RowSelection::from(vec![RowSelector::skip(rg_num_rows)]);
    }

    // First pass: emit row-granular (select_run, skip_run) pairs by walking
    // set-bits in order. Then second pass: merge short skips into adjacent
    // selects. Keeping it in two passes is clearer and the intermediate
    // vector stays bounded by `2 * number_of_flips`.
    let mut raw: Vec<RowSelector> = Vec::new();
    let mut pos = 0u32;
    // RoaringBitmap.iter() yields set bits in ascending order.
    let mut iter = candidates.iter().peekable();
    while let Some(&start) = iter.peek() {
        // Out-of-range set bits are ignored defensively.
        if (start as usize) >= rg_num_rows {
            break;
        }
        if start > pos {
            raw.push(RowSelector::skip((start - pos) as usize));
        }
        let mut run_end = start;
        iter.next();
        while let Some(&next) = iter.peek() {
            if next == run_end + 1 && (next as usize) < rg_num_rows {
                run_end = next;
                iter.next();
            } else {
                break;
            }
        }
        let run_len = (run_end - start + 1) as usize;
        raw.push(RowSelector::select(run_len));
        pos = run_end + 1;
    }
    if (pos as usize) < rg_num_rows {
        raw.push(RowSelector::skip(rg_num_rows - pos as usize));
    }

    // Second pass: absorb skips with row_count < min_skip_run into
    // surrounding selects. A skip flanked by selects becomes part of one
    // big select covering both sides + the gap.
    coalesce_short_skips(raw, min_skip_run)
}

/// Merge any `skip(n)` with `n < min_skip_run` into its surrounding
/// `select` neighbours. Adjacent selects get combined.
///
/// Runs with `min_skip_run = 1` are a no-op (every skip is >= 1).
fn coalesce_short_skips(input: Vec<RowSelector>, min_skip_run: usize) -> RowSelection {
    if min_skip_run <= 1 || input.is_empty() {
        return RowSelection::from(input);
    }
    let mut out: Vec<RowSelector> = Vec::with_capacity(input.len());
    for s in input {
        if s.skip && s.row_count < min_skip_run {
            // Absorb into the trailing select (or become one if nothing yet).
            match out.last_mut() {
                Some(last) if !last.skip => {
                    last.row_count += s.row_count;
                }
                _ => {
                    // Previous was skip or nothing; promote this run to select.
                    out.push(RowSelector::select(s.row_count));
                }
            }
        } else if !s.skip {
            // Merge with previous select if any.
            match out.last_mut() {
                Some(last) if !last.skip => {
                    last.row_count += s.row_count;
                }
                _ => out.push(s),
            }
        } else {
            // A real skip (row_count >= min_skip_run).
            out.push(s);
        }
    }
    RowSelection::from(out)
}

/// Maps a delivered batch-row index (0-based across all selects in a
/// `RowSelection`, in order) to its RG-relative row position. Built from
/// the same `RowSelection` handed to parquet.
///
/// Under [`build_row_selection_with_min_skip_run`]'s "absorb short skips"
/// behaviour, parquet delivers rows from all `select` runs, packed
/// contiguously. Post-decode masks need this map to know which RG
/// position each delivered row came from.
///
/// Storage is chosen per regime to stay cheap at 1M-row RGs:
///
/// - `Identity` — whole RG selected. `rg_position(i) = i`. Zero bytes.
/// - `Bitmap` — row-granular selection (every delivered row is a set bit
///   in some bitmap). The i-th delivered row is the i-th set bit via
///   `RoaringBitmap::select`. O(log n) lookup, shares an `Arc` with the
///   caller (no new allocation). This is the regime that would otherwise
///   explode `runs` — e.g. 10k scattered candidates in a 1M-row RG.
/// - `Runs` — block-granular selection with a small number of select
///   runs (bounded by `rg_num_rows / min_skip_run`; at the 1024 default
///   on a 1M RG that's < 1k entries).
#[derive(Debug, Clone)]
pub enum PositionMap {
    Identity {
        delivered_count: usize,
    },
    Bitmap {
        /// Delivered row i corresponds to the i-th set bit of `bits`.
        bits: Arc<RoaringBitmap>,
        delivered_count: usize,
    },
    Runs {
        /// One entry per `select` run: (start_rg_position, start_delivered_idx, length).
        /// Sorted ascending on `start_delivered_idx`.
        runs: Vec<(usize, usize, usize)>,
        delivered_count: usize,
    },
}

impl PositionMap {
    /// Build a Runs-backed map from an arbitrary `RowSelection`. Used only
    /// in tests and the block-granular regime (where `runs` stays small).
    /// Hot paths should prefer `from_candidates_with_selection`, which
    /// picks the cheapest variant.
    pub fn from_selection(selection: &RowSelection) -> Self {
        let mut runs = Vec::new();
        let mut rg_pos = 0usize;
        let mut delivered_pos = 0usize;
        for s in selection.iter() {
            if s.skip {
                rg_pos += s.row_count;
            } else {
                runs.push((rg_pos, delivered_pos, s.row_count));
                delivered_pos += s.row_count;
                rg_pos += s.row_count;
            }
        }
        // If the selection is a single whole-RG select, use Identity; the
        // Runs variant would still be correct but Identity skips the
        // per-lookup indirection and allocation.
        if runs.len() == 1 && runs[0].0 == 0 && runs[0].1 == 0 {
            return Self::Identity {
                delivered_count: delivered_pos,
            };
        }
        Self::Runs {
            runs,
            delivered_count: delivered_pos,
        }
    }

    /// Build the cheapest `PositionMap` for a (candidates, selection) pair.
    ///
    /// - `min_skip_run == 1` → row-granular: every delivered row is a set
    ///   bit of `candidates`. Wrap the bitmap in `Arc` and use
    ///   `RoaringBitmap::select` for lookup.
    /// - else → delegate to `from_selection`, which picks `Identity`
    ///   (whole-RG) or `Runs` (small block-granular run count).
    pub fn from_candidates_with_selection(
        candidates: Arc<RoaringBitmap>,
        selection: &RowSelection,
        min_skip_run: usize,
    ) -> Self {
        if min_skip_run == 1 {
            let delivered_count = candidates.len() as usize;
            return Self::Bitmap {
                bits: candidates,
                delivered_count,
            };
        }
        Self::from_selection(selection)
    }

    pub fn delivered_count(&self) -> usize {
        match self {
            Self::Identity { delivered_count }
            | Self::Bitmap {
                delivered_count, ..
            }
            | Self::Runs {
                delivered_count, ..
            } => *delivered_count,
        }
    }

    /// Translate a delivered row index to its RG-relative position.
    /// Returns `None` if `delivered_idx >= delivered_count`.
    pub fn rg_position(&self, delivered_idx: usize) -> Option<usize> {
        match self {
            Self::Identity { delivered_count } => {
                if delivered_idx < *delivered_count {
                    Some(delivered_idx)
                } else {
                    None
                }
            }
            Self::Bitmap {
                bits,
                delivered_count,
            } => {
                if delivered_idx >= *delivered_count {
                    return None;
                }
                // RoaringBitmap::select(n) returns the n-th smallest set bit.
                bits.select(delivered_idx as u32).map(|b| b as usize)
            }
            Self::Runs {
                runs,
                delivered_count,
            } => {
                if delivered_idx >= *delivered_count {
                    return None;
                }
                for &(rg_start, del_start, len) in runs {
                    if delivered_idx < del_start + len {
                        return Some(rg_start + (delivered_idx - del_start));
                    }
                }
                None
            }
        }
    }
}

/// Expand a `RowSelection` to a `RoaringBitmap` of RG-relative row
/// positions that were selected.
///
/// Used when a caller receives a `RowSelection` (e.g. from page pruning)
/// and needs to AND/OR it with another bitmap. The bitmap contains one
/// bit per `select`ed row. `skip` runs contribute nothing.
pub fn row_selection_to_bitmap(selection: &RowSelection) -> RoaringBitmap {
    let mut out = RoaringBitmap::new();
    let mut rg_pos: u32 = 0;
    for s in selection.iter() {
        if s.skip {
            rg_pos = rg_pos.saturating_add(s.row_count as u32);
        } else {
            let end = rg_pos.saturating_add(s.row_count as u32);
            // RoaringBitmap's insert_range handles runs efficiently.
            out.insert_range(rg_pos..end);
            rg_pos = end;
        }
    }
    out
}

/// Materialize a `RoaringBitmap` into a packed u64 bit-vector with `len`
/// bits (LSB-first within each word, matching Arrow's `BooleanBuffer`
/// layout). Bits beyond `len` are truncated. O(set_bits), not O(len).
///
/// Faster than per-position `.contains()` for hot paths that need to do
/// many lookups against the bitmap — once the packed form exists, each
/// lookup is a single `(words[i >> 6] >> (i & 63)) & 1` operation.
///
/// The returned `Vec<u64>` has `ceil(len / 64)` words. Zero-allocate the
/// output and only touch words containing set bits.
pub fn bitmap_to_packed_bits(bm: &RoaringBitmap, len: u32) -> Vec<u64> {
    let words = len.div_ceil(64) as usize;
    let mut v = vec![0u64; words];
    for b in bm.iter() {
        if b >= len {
            break; // RoaringBitmap iter yields ascending; safe to stop.
        }
        v[(b as usize) >> 6] |= 1u64 << (b & 63);
    }
    v
}

/// Build a `BooleanArray` directly from a packed bit-vector of length
/// `len`. The `Vec<u64>` is consumed and wrapped as an Arrow `Buffer` via
/// `from_vec` — zero-copy. Bit layout matches Arrow's native LSB-first
/// format so no translation is needed.
pub fn packed_bits_to_boolean_array(bits: Vec<u64>, len: usize) -> BooleanArray {
    use datafusion::arrow::buffer::Buffer;
    let buffer = Buffer::from_vec(bits);
    let boolean = datafusion::arrow::buffer::BooleanBuffer::new(buffer, 0, len);
    BooleanArray::new(boolean, None)
}

/// given the `RowSelection` we handed it.
///
/// Length of the returned mask = `PositionMap::delivered_count`. Bit at
/// delivered-index `i` is `true` iff `candidates` contains the RG-relative
/// position that row `i` came from. Under the old full-scan strategy,
/// `PositionMap` was identity (delivered_idx == rg_position) and the mask
/// had every row in it — for block-granular RowSelection, we only emit a
/// bit per delivered row, which is a small fraction of `rg_num_rows`.
pub fn build_mask(candidates: &RoaringBitmap, position_map: &PositionMap) -> BooleanArray {
    let n = position_map.delivered_count();
    match position_map {
        // Identity → delivered_idx == rg_position. Materialise the
        // candidate bitmap as a packed BooleanArray in one shot;
        // memcpy-speed, O(set_bits + len/64).
        PositionMap::Identity { delivered_count } => {
            let bits = bitmap_to_packed_bits(candidates, *delivered_count as u32);
            packed_bits_to_boolean_array(bits, *delivered_count)
        }
        // Bitmap → delivered rows are exactly the candidate set bits. Every
        // delivered row is by construction a candidate, so the mask is
        // all-true.
        PositionMap::Bitmap {
            delivered_count, ..
        } => {
            let all_true = datafusion::arrow::buffer::BooleanBuffer::new_set(*delivered_count);
            BooleanArray::new(all_true, None)
        }
        // Runs → iterate runs, materialise each run's slice into the
        // output buffer. Each run maps a contiguous rg-position range to a
        // contiguous delivered-row range.
        PositionMap::Runs { runs, .. } => {
            let words = n.div_ceil(64);
            let mut out = vec![0u64; words];
            for &(rg_start, delivered_start, run_len) in runs {
                // Walk the candidate set bits that fall in this run's
                // rg-position range, translate to delivered position, set
                // the corresponding bit.
                let rg_start_u32 = rg_start.min(u32::MAX as usize) as u32;
                let rg_end_u32 = (rg_start + run_len).min(u32::MAX as usize) as u32;
                // Roaring's `range()` iterates bits within the range in
                // ascending order — O(set_bits_in_range).
                for b in candidates.range(rg_start_u32..rg_end_u32) {
                    let delivered_idx = delivered_start + (b as usize - rg_start);
                    out[delivered_idx >> 6] |= 1u64 << (delivered_idx & 63);
                }
            }
            packed_bits_to_boolean_array(out, n)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;

    // ── build_mask ──────────────────────────────────────────────────
    //
    // Helper: builds a full-scan PositionMap where delivered_idx == rg_position
    // (used by the old full-scan strategy and equivalent to min_skip_run =
    // rg_num_rows + 1). This keeps the legacy behaviour under test.
    fn identity_position_map(rg_num_rows: usize) -> PositionMap {
        let sel = RowSelection::from(vec![RowSelector::select(rg_num_rows)]);
        PositionMap::from_selection(&sel)
    }

    #[test]
    fn build_mask_empty_candidates_produces_all_false() {
        let candidates = RoaringBitmap::new();
        let m = build_mask(&candidates, &identity_position_map(8));
        assert_eq!(m.len(), 8);
        assert_eq!(m.true_count(), 0);
    }

    #[test]
    fn build_mask_sparse_candidates_set_only_named_bits() {
        let candidates = bm(&[0, 3, 7]);
        let m = build_mask(&candidates, &identity_position_map(8));
        assert_eq!(m.len(), 8);
        let got: Vec<bool> = (0..m.len()).map(|i| m.value(i)).collect();
        assert_eq!(
            got,
            vec![true, false, false, true, false, false, false, true]
        );
    }

    #[test]
    fn build_mask_candidates_outside_range_are_ignored() {
        let candidates = bm(&[2, 9, 100]);
        let m = build_mask(&candidates, &identity_position_map(4));
        assert_eq!(m.len(), 4);
        let got: Vec<bool> = (0..m.len()).map(|i| m.value(i)).collect();
        assert_eq!(got, vec![false, false, true, false]);
    }

    #[test]
    fn build_mask_dense_candidates_covers_every_position() {
        // All positions in candidates → all-true mask.
        let candidates: RoaringBitmap = (0u32..16).collect();
        let m = build_mask(&candidates, &identity_position_map(16));
        assert_eq!(m.true_count(), 16);
        assert_eq!(m.null_count(), 0);
    }

    #[test]
    fn build_mask_aligns_to_position_map_not_rg_positions() {
        // With a block-granular selection, parquet delivers only rows from
        // `select` runs. The mask length = delivered_count and each bit at
        // delivered index `i` is set iff the RG position mapped from `i`
        // is in candidates.
        //
        // Selection: select(3) skip(2) select(2)  → delivers 5 rows.
        //   delivered  0 1 2 3 4
        //   rg_pos     0 1 2 5 6
        // Candidates: {1, 5}
        //   mask     : [false, true, false, true, false]
        let sel = RowSelection::from(vec![
            RowSelector::select(3),
            RowSelector::skip(2),
            RowSelector::select(2),
        ]);
        let pm = PositionMap::from_selection(&sel);
        let candidates = bm(&[1, 5]);
        let m = build_mask(&candidates, &pm);
        assert_eq!(m.len(), 5);
        let got: Vec<bool> = (0..m.len()).map(|i| m.value(i)).collect();
        assert_eq!(got, vec![false, true, false, true, false]);
    }

    // ── build_row_selection_with_min_skip_run ────────────────────────

    fn bm(values: &[u32]) -> RoaringBitmap {
        let mut b = RoaringBitmap::new();
        for &v in values {
            b.insert(v);
        }
        b
    }

    fn selectors(sel: &RowSelection) -> Vec<(bool, usize)> {
        sel.iter().map(|s| (s.skip, s.row_count)).collect()
    }

    #[test]
    fn min_skip_run_one_is_row_granular() {
        // With min_skip_run=1, every non-set bit is a skip — equivalent
        // to the old row-granular behaviour.
        let sel = build_row_selection_with_min_skip_run(&bm(&[0, 3, 7]), 10, 1);
        assert_eq!(
            selectors(&sel),
            vec![
                (false, 1), // select row 0
                (true, 2),  // skip rows 1,2
                (false, 1), // select row 3
                (true, 3),  // skip rows 4,5,6
                (false, 1), // select row 7
                (true, 2),  // skip rows 8,9
            ]
        );
    }

    #[test]
    fn min_skip_run_absorbs_small_gaps() {
        // Candidates {0, 3, 7} on a 10-row RG with min_skip_run=4:
        //   raw: select(1) skip(2) select(1) skip(3) select(1) skip(2)
        //   after absorb (any skip<4 merges):
        //     skip(2) absorbed → merges the two selects into select(4)   [row 0..4]
        //     then another select(1) for row 7 — separated by skip(3) which is < 4
        //     so skip(3) is absorbed → select(4) + select(1) merged into select(8) [rows 0..8]
        //     final skip(2) < 4 → absorbed into the trailing select → select(10).
        let sel = build_row_selection_with_min_skip_run(&bm(&[0, 3, 7]), 10, 4);
        // Everything got absorbed into one big select.
        assert_eq!(selectors(&sel), vec![(false, 10)]);
    }

    #[test]
    fn min_skip_run_preserves_big_gaps() {
        // A gap >= min_skip_run stays as a real skip.
        // Candidates {0, 100}: gap is 99 rows. With min_skip_run=50, skip survives.
        let sel = build_row_selection_with_min_skip_run(&bm(&[0, 100]), 200, 50);
        assert_eq!(
            selectors(&sel),
            vec![
                (false, 1), // select row 0
                (true, 99), // skip rows 1..100 (>= 50 → preserved)
                (false, 1), // select row 100
                (true, 99), // skip rows 101..200 (>= 50 → preserved)
            ]
        );
    }

    #[test]
    fn min_skip_run_empty_bitmap_skips_everything() {
        let sel = build_row_selection_with_min_skip_run(&bm(&[]), 50, 10);
        assert_eq!(selectors(&sel), vec![(true, 50)]);
    }

    #[test]
    fn min_skip_run_all_bits_set_single_select() {
        let candidates: Vec<u32> = (0..10).collect();
        let sel = build_row_selection_with_min_skip_run(&bm(&candidates), 10, 5);
        assert_eq!(selectors(&sel), vec![(false, 10)]);
    }

    #[test]
    fn min_skip_run_oversized_threshold_becomes_full_scan() {
        // If min_skip_run > rg_num_rows, NO skip survives; the result
        // is one big select across the whole RG.
        let sel = build_row_selection_with_min_skip_run(&bm(&[0, 7]), 10, 1000);
        assert_eq!(selectors(&sel), vec![(false, 10)]);
    }

    #[test]
    fn min_skip_run_ignores_out_of_range_bits() {
        // Bits outside [0, rg_num_rows) are silently ignored (defensive).
        let sel = build_row_selection_with_min_skip_run(&bm(&[5, 999]), 10, 1);
        assert_eq!(selectors(&sel), vec![(true, 5), (false, 1), (true, 4)]);
    }

    // ── PositionMap ──────────────────────────────────────────────────

    #[test]
    fn position_map_empty_selection_is_empty() {
        let sel = RowSelection::from(vec![RowSelector::skip(10)]);
        let pm = PositionMap::from_selection(&sel);
        assert_eq!(pm.delivered_count(), 0);
        assert_eq!(pm.rg_position(0), None);
    }

    #[test]
    fn position_map_single_select() {
        let sel = RowSelection::from(vec![
            RowSelector::skip(3),
            RowSelector::select(4),
            RowSelector::skip(3),
        ]);
        let pm = PositionMap::from_selection(&sel);
        assert_eq!(pm.delivered_count(), 4);
        // Delivered rows 0..4 correspond to RG positions 3..7.
        assert_eq!(pm.rg_position(0), Some(3));
        assert_eq!(pm.rg_position(1), Some(4));
        assert_eq!(pm.rg_position(2), Some(5));
        assert_eq!(pm.rg_position(3), Some(6));
        assert_eq!(pm.rg_position(4), None);
    }

    #[test]
    fn position_map_multiple_selects() {
        let sel = RowSelection::from(vec![
            RowSelector::select(2),
            RowSelector::skip(5),
            RowSelector::select(3),
            RowSelector::skip(1),
            RowSelector::select(1),
        ]);
        let pm = PositionMap::from_selection(&sel);
        assert_eq!(pm.delivered_count(), 6);
        // First block: delivered [0,1] → RG [0,1].
        assert_eq!(pm.rg_position(0), Some(0));
        assert_eq!(pm.rg_position(1), Some(1));
        // Second block: delivered [2,3,4] → RG [7,8,9] (2 selected + 5 skipped = 7).
        assert_eq!(pm.rg_position(2), Some(7));
        assert_eq!(pm.rg_position(3), Some(8));
        assert_eq!(pm.rg_position(4), Some(9));
        // Third block: delivered [5] → RG [11] (7+3 selected + 1 skipped = 11).
        assert_eq!(pm.rg_position(5), Some(11));
        assert_eq!(pm.rg_position(6), None);
    }

    #[test]
    fn position_map_identity_fast_path() {
        // Whole-RG select collapses to Identity — no runs allocated.
        let sel = RowSelection::from(vec![RowSelector::select(1_000_000)]);
        let pm = PositionMap::from_selection(&sel);
        assert!(matches!(pm, PositionMap::Identity { .. }));
        assert_eq!(pm.delivered_count(), 1_000_000);
        assert_eq!(pm.rg_position(0), Some(0));
        assert_eq!(pm.rg_position(999_999), Some(999_999));
        assert_eq!(pm.rg_position(1_000_000), None);
    }

    #[test]
    fn position_map_bitmap_variant_row_granular() {
        // Row-granular regime: candidates drive both RowSelection and
        // PositionMap. 4 scattered bits; delivered index i → i-th set bit.
        let candidates = Arc::new(bm(&[3, 17, 42, 100]));
        // Build the matching selection (it's what the real code path
        // hands to parquet), but we pass min_skip_run = 1 so PositionMap
        // chooses the Bitmap variant rather than Runs.
        let selection = build_row_selection_with_min_skip_run(&candidates, 200, 1);
        let pm =
            PositionMap::from_candidates_with_selection(Arc::clone(&candidates), &selection, 1);
        assert!(matches!(pm, PositionMap::Bitmap { .. }));
        assert_eq!(pm.delivered_count(), 4);
        assert_eq!(pm.rg_position(0), Some(3));
        assert_eq!(pm.rg_position(1), Some(17));
        assert_eq!(pm.rg_position(2), Some(42));
        assert_eq!(pm.rg_position(3), Some(100));
        assert_eq!(pm.rg_position(4), None);
    }

    #[test]
    fn position_map_bitmap_variant_no_runs_allocated() {
        // Pathological row-granular case: 10_000 scattered bits over a 1M-row RG.
        // Under the old single-struct design this would allocate a
        // ~240 KB runs Vec. Bitmap variant carries only an Arc pointer.
        let mut bits = RoaringBitmap::new();
        for i in 0..10_000u32 {
            bits.insert(i * 97); // scattered, evenly spread up to ~970_000
        }
        let candidates = Arc::new(bits);
        let selection = build_row_selection_with_min_skip_run(&candidates, 1_000_000, 1);
        let pm =
            PositionMap::from_candidates_with_selection(Arc::clone(&candidates), &selection, 1);
        match &pm {
            PositionMap::Bitmap { .. } => {}
            other => panic!("expected Bitmap, got {:?}", other),
        }
        assert_eq!(pm.delivered_count(), 10_000);
        // Spot check a few delivered indices.
        assert_eq!(pm.rg_position(0), Some(0));
        assert_eq!(pm.rg_position(1), Some(97));
        assert_eq!(pm.rg_position(9_999), Some(9_999 * 97));
        assert_eq!(pm.rg_position(10_000), None);
    }
}
