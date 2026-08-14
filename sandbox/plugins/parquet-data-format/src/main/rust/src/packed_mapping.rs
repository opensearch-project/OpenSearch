// SPDX-License-Identifier: Apache-2.0
//
// The OpenSearch Contributors require contributions made to
// this file be licensed under the Apache-2.0 license or a
// compatible open source license.

//! Fixed-width bit packing for row ID mappings handed to Java.
//!
//! Both the forward permutation (`perm[oldId] = newId`) and its reverse are
//! permutations of `[0, count)`, so every value fits in
//! `bpv = 64 - leading_zeros(count - 1)` bits. Values are packed back-to-back
//! in little-endian order: value `i` occupies bits `[i*bpv, (i+1)*bpv)` of the
//! buffer. The Java reader (`NativePackedRowIdMapping`) decodes a value with a
//! single unaligned little-endian 8-byte read plus shift/mask, so this layout
//! is a cross-language serialization contract — do not change one side without
//! the other.
//!
//! The buffer is padded with [`TAIL_PAD`] extra zero bytes so the reader's
//! 8-byte window read at the last value never runs past the allocation.

/// Extra zero bytes appended so an unaligned 8-byte read at the final value
/// stays in bounds.
pub const TAIL_PAD: usize = 7;

/// Number of bits needed to represent any value in `[0, count)`.
/// Returns 1 for `count <= 1` so the packed buffer is never zero-width.
pub fn bits_per_value(count: usize) -> u32 {
    if count <= 1 {
        1
    } else {
        64 - ((count - 1) as u64).leading_zeros()
    }
}

/// Total byte length of a packed buffer for `count` values at `bpv` bits each,
/// including tail padding.
pub fn packed_byte_len(count: usize, bpv: u32) -> usize {
    (count * bpv as usize).div_ceil(8) + TAIL_PAD
}

/// Writes a single value at `index` into an already-zeroed packed buffer.
///
/// Safe to call in any index order (scatter) because each value's bits are
/// disjoint and the write is a read-OR-write of an 8-byte window. The buffer
/// must have been allocated with [`packed_byte_len`] and each index written
/// at most once.
///
/// Requires `bpv <= 57` so that any value plus the maximum intra-byte shift (7)
/// fits within one 8-byte window — guaranteed for row counts below 2^57.
pub fn pack_one(buf: &mut [u8], bpv: u32, index: usize, value: u64) {
    debug_assert!(bpv >= 1 && bpv <= 57, "bpv must be in [1, 57], got {}", bpv);
    debug_assert!(value < (1u64 << bpv), "value {} does not fit in {} bits", value, bpv);
    let bit_pos = index * bpv as usize;
    let byte_pos = bit_pos >> 3;
    let shift = (bit_pos & 7) as u32;
    let window: &mut [u8] = &mut buf[byte_pos..byte_pos + 8];
    let mut w = u64::from_le_bytes(window.try_into().unwrap());
    w |= value << shift;
    window.copy_from_slice(&w.to_le_bytes());
}

/// Bit-packs `values` at `bpv` bits per value into a little-endian buffer.
pub fn pack(values: &[i64], bpv: u32) -> Vec<u8> {
    let mut buf = vec![0u8; packed_byte_len(values.len(), bpv)];
    for (i, &v) in values.iter().enumerate() {
        pack_one(&mut buf, bpv, i, v as u64);
    }
    buf
}

/// Decodes value `i` from a packed buffer. Mirror of the Java reader; used in tests.
#[cfg(test)]
pub fn unpack(buf: &[u8], bpv: u32, i: usize) -> i64 {
    let bit_pos = i * bpv as usize;
    let byte_pos = bit_pos >> 3;
    let shift = (bit_pos & 7) as u32;
    let w = u64::from_le_bytes(buf[byte_pos..byte_pos + 8].try_into().unwrap());
    ((w >> shift) & ((1u64 << bpv) - 1)) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bits_per_value() {
        assert_eq!(bits_per_value(0), 1);
        assert_eq!(bits_per_value(1), 1);
        assert_eq!(bits_per_value(2), 1);
        assert_eq!(bits_per_value(3), 2);
        assert_eq!(bits_per_value(256), 8);
        assert_eq!(bits_per_value(257), 9);
        assert_eq!(bits_per_value(10_000_000), 24);
    }

    #[test]
    fn test_pack_unpack_round_trip() {
        // pseudo-random permutation of [0, n)
        let n = 4099usize; // non-multiple of 8 to exercise straddling reads
        let mut values: Vec<i64> = (0..n as i64).collect();
        // simple deterministic shuffle
        let mut seed = 0x9E3779B97F4A7C15u64;
        for i in (1..n).rev() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = (seed % (i as u64 + 1)) as usize;
            values.swap(i, j);
        }
        let bpv = bits_per_value(n);
        let buf = pack(&values, bpv);
        assert_eq!(buf.len(), packed_byte_len(n, bpv));
        for (i, &v) in values.iter().enumerate() {
            assert_eq!(unpack(&buf, bpv, i), v, "mismatch at index {}", i);
        }
    }

    #[test]
    fn test_pack_single_value() {
        let buf = pack(&[0], bits_per_value(1));
        assert_eq!(unpack(&buf, 1, 0), 0);
    }

    #[test]
    fn test_packed_size_is_compact() {
        // 10M rows at 24 bpv should be ~3 bytes per value, not 8
        let n = 10_000_000usize;
        let bpv = bits_per_value(n);
        let len = packed_byte_len(n, bpv);
        assert!(len < n * 4, "packed len {} should be well under 4 bytes/value", len);
        assert!(len >= n * 3, "packed len {} should be at least 3 bytes/value for 24 bpv", len);
    }
}
