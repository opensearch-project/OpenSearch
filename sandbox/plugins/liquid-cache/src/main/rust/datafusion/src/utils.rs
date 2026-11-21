use arrow::{
    array::BooleanBufferBuilder,
    buffer::{BooleanBuffer, MutableBuffer},
};
use parquet::arrow::arrow_reader::RowSelector;

fn boolean_buffer_and_then_fallback(left: &BooleanBuffer, right: &BooleanBuffer) -> BooleanBuffer {
    debug_assert_eq!(
        left.count_set_bits(),
        right.len(),
        "the right selection must have the same number of set bits as the left selection"
    );

    if left.len() == right.len() {
        debug_assert_eq!(left.count_set_bits(), left.len());
        return right.clone();
    }

    let mut buffer = MutableBuffer::from_len_zeroed(left.values().len());
    buffer.copy_from_slice(left.values());
    let mut builder = BooleanBufferBuilder::new_from_buffer(buffer, left.len());

    let mut other_bits = right.iter();

    for bit_idx in left.set_indices() {
        let predicate = other_bits
            .next()
            .expect("Mismatch in set bits between self and other");
        if !predicate {
            builder.set_bit(bit_idx, false);
        }
    }

    builder.finish()
}

/// Combines this [`BooleanBuffer`] with another using logical AND on the selected bits.
///
/// Unlike intersection, the `other` [`BooleanBuffer`] must have exactly as many **set bits** as `self`,
/// i.e., self.count_set_bits() == other.len().
///
/// This method will keep only the bits in `self` that are also set in `other`
/// at the positions corresponding to `self`'s set bits.
/// For example:
/// left:   NNYYYNNYYNYN
/// right:    YNY  NY N
/// result: NNYNYNNNYNNN
///
/// Optimized version of `boolean_buffer_and_then` using BMI2 PDEP instructions.
/// This function performs the same operation but uses bit manipulation instructions
/// for better performance on supported x86_64 CPUs.
pub fn boolean_buffer_and_then(left: &BooleanBuffer, right: &BooleanBuffer) -> BooleanBuffer {
    debug_assert_eq!(
        left.count_set_bits(),
        right.len(),
        "the right selection must have the same number of set bits as the left selection"
    );

    if left.len() == right.len() {
        debug_assert_eq!(left.count_set_bits(), left.len());
        return right.clone();
    }

    // Fast path for BMI2 support on x86_64
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("bmi2") {
            return unsafe { boolean_buffer_and_then_bmi2(left, right) };
        }
    }

    boolean_buffer_and_then_fallback(left, right)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn load_u64_zero_padded(base_ptr: *const u8, total_bytes: usize, offset: usize) -> u64 {
    let remaining = total_bytes.saturating_sub(offset);
    if remaining >= 8 {
        unsafe { core::ptr::read_unaligned(base_ptr.add(offset) as *const u64) }
    } else if remaining > 0 {
        let mut tmp = [0u8; 8];
        unsafe {
            core::ptr::copy_nonoverlapping(base_ptr.add(offset), tmp.as_mut_ptr(), remaining);
        }
        u64::from_le_bytes(tmp)
    } else {
        0
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "bmi2")]
unsafe fn boolean_buffer_and_then_bmi2(
    left: &BooleanBuffer,
    right: &BooleanBuffer,
) -> BooleanBuffer {
    use core::arch::x86_64::_pdep_u64;

    debug_assert_eq!(left.count_set_bits(), right.len());

    let bit_len = left.len();
    let byte_len = bit_len.div_ceil(8);
    let left_ptr = left.values().as_ptr();
    let right_bytes = right.len().div_ceil(8);
    let right_ptr = right.values().as_ptr();

    let mut out = MutableBuffer::from_len_zeroed(byte_len);
    let out_ptr = out.as_mut_ptr();

    let full_words = bit_len / 64;
    let mut right_bit_idx = 0; // how many bits we have processed from right

    for word_idx in 0..full_words {
        let left_word =
            unsafe { core::ptr::read_unaligned(left_ptr.add(word_idx * 8) as *const u64) };

        if left_word == 0 {
            continue;
        }

        let need = left_word.count_ones();

        // Absolute byte & bit offset of the first needed bit inside `right`.
        let rb_byte = right_bit_idx / 8;
        let rb_bit = (right_bit_idx & 7) as u32;

        let need_high = rb_bit != 0;
        let safe16 = right_bytes.saturating_sub(16);
        let mut r_bits;
        if rb_byte <= safe16 {
            let low = unsafe { core::ptr::read_unaligned(right_ptr.add(rb_byte) as *const u64) };
            if need_high {
                let high =
                    unsafe { core::ptr::read_unaligned(right_ptr.add(rb_byte + 8) as *const u64) };
                r_bits = (low >> rb_bit) | (high << (64 - rb_bit));
            } else {
                r_bits = low >> rb_bit;
            }
        } else {
            let low = load_u64_zero_padded(right_ptr, right_bytes, rb_byte);
            r_bits = low >> rb_bit;
            if need_high {
                let high = load_u64_zero_padded(right_ptr, right_bytes, rb_byte + 8);
                r_bits |= high << (64 - rb_bit);
            }
        }

        // Mask off the high garbage.
        r_bits &= 1u64.unbounded_shl(need).wrapping_sub(1);

        // The PDEP instruction: https://www.felixcloutier.com/x86/pdep
        // It takes left_word as the mask, and deposit the packed bits into the sparse positions of `left_word`.
        let result = _pdep_u64(r_bits, left_word);

        unsafe {
            core::ptr::write_unaligned(out_ptr.add(word_idx * 8) as *mut u64, result);
        }

        right_bit_idx += need as usize;
    }

    // Handle remaining bits that are less than 64 bits
    let tail_bits = bit_len & 63;
    if tail_bits != 0 {
        // Build the mask from the remaining bytes in one bounded copy
        let tail_bytes = tail_bits.div_ceil(8);
        let base = unsafe { left_ptr.add(full_words * 8) };
        let mut mask: u64;
        if tail_bytes == 8 {
            mask = unsafe { core::ptr::read_unaligned(base as *const u64) };
        } else {
            let mut buf = [0u8; 8];
            unsafe {
                core::ptr::copy_nonoverlapping(base, buf.as_mut_ptr(), tail_bytes);
            }
            mask = u64::from_le_bytes(buf);
        }
        // Clear any high bits beyond the actual tail length
        mask &= (1u64 << tail_bits) - 1;

        if mask != 0 {
            let need = mask.count_ones();

            let rb_byte = right_bit_idx / 8;
            let rb_bit = (right_bit_idx & 7) as u32;

            let need_high = rb_bit != 0;
            let safe16 = right_bytes.saturating_sub(16);
            let mut r_bits;
            if rb_byte <= safe16 {
                let low =
                    unsafe { core::ptr::read_unaligned(right_ptr.add(rb_byte) as *const u64) };
                if need_high {
                    let high = unsafe {
                        core::ptr::read_unaligned(right_ptr.add(rb_byte + 8) as *const u64)
                    };
                    r_bits = (low >> rb_bit) | (high << (64 - rb_bit));
                } else {
                    r_bits = low >> rb_bit;
                }
            } else {
                let low = load_u64_zero_padded(right_ptr, right_bytes, rb_byte);
                r_bits = low >> rb_bit;
                if need_high {
                    let high = load_u64_zero_padded(right_ptr, right_bytes, rb_byte + 8);
                    r_bits |= high << (64 - rb_bit);
                }
            }

            r_bits &= 1u64.unbounded_shl(need).wrapping_sub(1);

            let result = _pdep_u64(r_bits, mask);

            let tail_bytes = tail_bits.div_ceil(8);
            let result_bytes = result.to_le_bytes();
            let dst_off = full_words * 8;
            unsafe {
                let dst = core::slice::from_raw_parts_mut(out_ptr, byte_len);
                dst[dst_off..dst_off + tail_bytes].copy_from_slice(&result_bytes[..tail_bytes]);
            }
        }
    }

    BooleanBuffer::new(out.into(), 0, bit_len)
}

pub(super) fn row_selector_to_boolean_buffer(selection: &[RowSelector]) -> BooleanBuffer {
    let mut buffer = BooleanBufferBuilder::new(8192);
    for selector in selection.iter() {
        if selector.skip {
            buffer.append_n(selector.row_count, false);
        } else {
            buffer.append_n(selector.row_count, true);
        }
    }
    buffer.finish()
}

#[cfg(all(test, target_arch = "x86_64"))]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_buffer_and_then_bmi2_large() {
        use super::boolean_buffer_and_then_bmi2;

        // Test with larger buffer (more than 64 bits)
        let size = 128;
        let mut left_builder = BooleanBufferBuilder::new(size);
        let mut right_bits = Vec::new();

        // Create a pattern where every 3rd bit is set in left
        for i in 0..size {
            let is_set = i.is_multiple_of(3);
            left_builder.append(is_set);
            if is_set {
                // For right buffer, alternate between true/false
                right_bits.push(right_bits.len().is_multiple_of(2));
            }
        }
        let left = left_builder.finish();

        let mut right_builder = BooleanBufferBuilder::new(right_bits.len());
        for bit in right_bits {
            right_builder.append(bit);
        }
        let right = right_builder.finish();

        let result_bmi2 = unsafe { boolean_buffer_and_then_bmi2(&left, &right) };
        let result_orig = boolean_buffer_and_then_fallback(&left, &right);

        assert_eq!(result_bmi2.len(), result_orig.len());
        assert_eq!(result_bmi2.len(), size);

        // Verify they produce the same result
        for i in 0..size {
            assert_eq!(
                result_bmi2.value(i),
                result_orig.value(i),
                "Mismatch at position {i}"
            );
        }
    }

    #[test]
    fn test_boolean_buffer_and_then_bmi2_edge_cases() {
        use super::boolean_buffer_and_then_bmi2;

        // Test case: all bits set in left, alternating pattern in right
        let mut left_builder = BooleanBufferBuilder::new(16);
        for _ in 0..16 {
            left_builder.append(true);
        }
        let left = left_builder.finish();

        let mut right_builder = BooleanBufferBuilder::new(16);
        for i in 0..16 {
            right_builder.append(i % 2 == 0);
        }
        let right = right_builder.finish();

        let result_bmi2 = unsafe { boolean_buffer_and_then_bmi2(&left, &right) };
        let result_orig = boolean_buffer_and_then_fallback(&left, &right);

        assert_eq!(result_bmi2.len(), result_orig.len());
        for i in 0..16 {
            assert_eq!(
                result_bmi2.value(i),
                result_orig.value(i),
                "Mismatch at position {i}"
            );
            // Should be true for even indices, false for odd
            assert_eq!(result_bmi2.value(i), i.is_multiple_of(2));
        }

        // Test case: no bits set in left
        let mut left_empty_builder = BooleanBufferBuilder::new(8);
        for _ in 0..8 {
            left_empty_builder.append(false);
        }
        let left_empty = left_empty_builder.finish();
        let right_empty = BooleanBufferBuilder::new(0).finish();

        let result_bmi2_empty = unsafe { boolean_buffer_and_then_bmi2(&left_empty, &right_empty) };
        let result_orig_empty = boolean_buffer_and_then_fallback(&left_empty, &right_empty);

        assert_eq!(result_bmi2_empty.len(), result_orig_empty.len());
        assert_eq!(result_bmi2_empty.len(), 8);
        for i in 0..8 {
            assert!(!result_bmi2_empty.value(i));
            assert!(!result_orig_empty.value(i));
        }
    }
}
