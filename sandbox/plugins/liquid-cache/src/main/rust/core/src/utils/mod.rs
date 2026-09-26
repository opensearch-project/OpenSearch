//! Utility functions for the storage module.

use std::num::NonZero;

use datafusion_common::ScalarValue;

/// Get the bit width for a given max value.
/// Returns 1 if the max value is 0.
/// Returns 64 - max_value.leading_zeros() as u8 otherwise.
pub(crate) fn get_bit_width(max_value: u64) -> NonZero<u8> {
    if max_value == 0 {
        // todo: here we actually should return 0, as we should just use constant encoding.
        // but that's not implemented yet.
        NonZero::new(1).unwrap()
    } else {
        NonZero::new(64 - max_value.leading_zeros() as u8).unwrap()
    }
}

pub(crate) fn get_bytes_needle(value: &ScalarValue) -> Option<Vec<u8>> {
    match value {
        ScalarValue::Utf8(Some(v)) => Some(v.as_bytes().to_vec()),
        ScalarValue::Utf8View(Some(v)) => Some(v.as_bytes().to_vec()),
        ScalarValue::LargeUtf8(Some(v)) => Some(v.as_bytes().to_vec()),
        ScalarValue::Binary(Some(v)) => Some(v.clone()),
        ScalarValue::BinaryView(Some(v)) => Some(v.clone()),
        ScalarValue::FixedSizeBinary(_, Some(v)) => Some(v.clone()),
        ScalarValue::LargeBinary(Some(v)) => Some(v.clone()),
        ScalarValue::Dictionary(_, value) => get_bytes_needle(value.as_ref()),
        _ => None,
    }
}
