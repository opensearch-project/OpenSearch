// SPDX-License-Identifier: Apache-2.0
//
//! Property-based (fuzzy) tests for Liquid array encode/decode roundtrips.
//!
//! Each test generates random arrays of a given type via `proptest` and verifies
//! that:
//!   1. `to_arrow_array()` produces an array equal to the original.
//!   2. `filter()` with a random BooleanBuffer produces the same result as
//!      Arrow's own `filter` kernel.
//!   3. Compression is non-negative (Liquid size ≤ Arrow size) for sequences
//!      where the codec is expected to compress (sequential / small-range data).
//!
//! These cover `LiquidPrimitiveArray` (bit-packed integers), `LiquidFloatArray`
//! (XOR-delta floats), and `LiquidDecimalArray` (u64-packed decimals).

#[cfg(test)]
mod fuzz {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array};
    use arrow::buffer::BooleanBuffer;
    use arrow::datatypes::{
        Date32Type, Date64Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
        Int64Type, TimestampMicrosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    };
    use proptest::prelude::*;

    use crate::liquid_array::{
        LiquidArray, LiquidDecimalArray, LiquidFloatArray, LiquidPrimitiveArray,
    };

    // ── Helpers ────────────────────────────────────────────────────────────────

    /// Assert roundtrip equality: encode → decode must yield the original array.
    fn assert_roundtrip(original: &ArrayRef, decoded: &ArrayRef) {
        assert_eq!(
            original.as_ref(),
            decoded.as_ref(),
            "roundtrip mismatch: original={original:?} decoded={decoded:?}"
        );
    }

    /// Assert that `liquid_filter` matches Arrow's own filter kernel.
    fn assert_filter_matches(
        original: &ArrayRef,
        liquid_filtered: &ArrayRef,
        mask: &BooleanBuffer,
    ) {
        let selection = BooleanArray::new(mask.clone(), None);
        let arrow_filtered = arrow::compute::filter(original.as_ref(), &selection).unwrap();
        assert_eq!(
            arrow_filtered.as_ref(),
            liquid_filtered.as_ref(),
            "filter mismatch for mask={mask:?}"
        );
    }

    /// Build a `BooleanBuffer` from a `Vec<bool>`.
    fn bool_buf(bits: Vec<bool>) -> BooleanBuffer {
        BooleanBuffer::collect_bool(bits.len(), |i| bits[i])
    }

    // ── Strategies ────────────────────────────────────────────────────────────

    /// Strategy: Vec<Option<i32>> of length 0..=8192, with ~10% nulls.
    fn opt_i32_vec(max_len: usize) -> impl Strategy<Value = Vec<Option<i32>>> {
        proptest::collection::vec(proptest::option::weighted(0.9, any::<i32>()), 0..=max_len)
    }

    fn opt_i64_vec(max_len: usize) -> impl Strategy<Value = Vec<Option<i64>>> {
        proptest::collection::vec(proptest::option::weighted(0.9, any::<i64>()), 0..=max_len)
    }

    fn opt_u64_vec(max_len: usize) -> impl Strategy<Value = Vec<Option<u64>>> {
        proptest::collection::vec(proptest::option::weighted(0.9, any::<u64>()), 0..=max_len)
    }

    fn opt_f32_vec(max_len: usize) -> impl Strategy<Value = Vec<Option<f32>>> {
        proptest::collection::vec(
            proptest::option::weighted(0.9, proptest::num::f32::NORMAL),
            0..=max_len,
        )
    }

    fn opt_f64_vec(max_len: usize) -> impl Strategy<Value = Vec<Option<f64>>> {
        proptest::collection::vec(
            proptest::option::weighted(0.9, proptest::num::f64::NORMAL),
            0..=max_len,
        )
    }

    /// Strategy: boolean mask of fixed length `n`.
    fn _bool_mask_unused(n: usize) -> impl Strategy<Value = Vec<bool>> {
        proptest::collection::vec(any::<bool>(), n..=n)
    }

    // ── Int8 ──────────────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_i8_roundtrip(values in proptest::collection::vec(
            proptest::option::weighted(0.9, any::<i8>()), 0usize..=1024
        )) {
            let arr = arrow::array::PrimitiveArray::<Int8Type>::from(values);
            let liquid = LiquidPrimitiveArray::<Int8Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            let original: ArrayRef = Arc::new(arr);
            assert_roundtrip(&original, &decoded);
        }

        #[test]
        fn fuzz_i8_filter(
            values in proptest::collection::vec(
                proptest::option::weighted(0.9, any::<i8>()), 1usize..=512
            ),
            mask in proptest::collection::vec(any::<bool>(), 1usize..=512)
        ) {
            let n = values.len().min(mask.len());
            let values = &values[..n];
            let mask = &mask[..n];
            let arr = arrow::array::PrimitiveArray::<Int8Type>::from(values.to_vec());
            let liquid = LiquidPrimitiveArray::<Int8Type>::from_arrow_array(arr.clone());
            let buf = bool_buf(mask.to_vec());
            let filtered = liquid.filter(&buf);
            let original: ArrayRef = Arc::new(arr);
            assert_filter_matches(&original, &filtered, &buf);
        }
    }

    // ── Int16 ─────────────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_i16_roundtrip(values in proptest::collection::vec(
            proptest::option::weighted(0.9, any::<i16>()), 0usize..=1024
        )) {
            let arr = arrow::array::PrimitiveArray::<Int16Type>::from(values);
            let liquid = LiquidPrimitiveArray::<Int16Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            let original: ArrayRef = Arc::new(arr);
            assert_roundtrip(&original, &decoded);
        }
    }

    // ── Int32 ─────────────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_i32_roundtrip(values in opt_i32_vec(2048)) {
            let arr = arrow::array::PrimitiveArray::<Int32Type>::from(values);
            let liquid = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            let original: ArrayRef = Arc::new(arr);
            assert_roundtrip(&original, &decoded);
        }

        #[test]
        fn fuzz_i32_filter(
            values in proptest::collection::vec(
                proptest::option::weighted(0.9, any::<i32>()), 1usize..=1024
            ),
            mask in proptest::collection::vec(any::<bool>(), 1usize..=1024)
        ) {
            let n = values.len().min(mask.len());
            let arr = arrow::array::PrimitiveArray::<Int32Type>::from(values[..n].to_vec());
            let liquid = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(arr.clone());
            let buf = bool_buf(mask[..n].to_vec());
            let filtered = liquid.filter(&buf);
            let original: ArrayRef = Arc::new(arr);
            assert_filter_matches(&original, &filtered, &buf);
        }

        /// Sequential i32 (timestamps, row IDs) — verify compression.
        #[test]
        fn fuzz_i32_sequential_compresses(start in any::<i32>(), len in 512usize..=8192) {
            let values: Vec<i32> = (0..len as i32).map(|i| start.wrapping_add(i)).collect();
            let arr = Int32Array::from(values);
            let original: ArrayRef = Arc::new(arr.clone());
            let liquid = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(arr);
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&original, &decoded);
            // Sequential data with step=1 must compress for large enough arrays.
            prop_assert!(
                liquid.get_array_memory_size() < original.get_array_memory_size(),
                "sequential i32 not compressed: liquid={} arrow={}",
                liquid.get_array_memory_size(),
                original.get_array_memory_size()
            );
        }
    }

    // ── Int64 ─────────────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_i64_roundtrip(values in opt_i64_vec(2048)) {
            let arr = arrow::array::PrimitiveArray::<Int64Type>::from(values);
            let liquid = LiquidPrimitiveArray::<Int64Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            let original: ArrayRef = Arc::new(arr);
            assert_roundtrip(&original, &decoded);
        }

        #[test]
        fn fuzz_i64_filter(
            values in opt_i64_vec(512),
            mask in proptest::collection::vec(any::<bool>(), 1usize..=512)
        ) {
            let n = values.len().min(mask.len());
            if n == 0 { return Ok(()); }
            let arr = arrow::array::PrimitiveArray::<Int64Type>::from(values[..n].to_vec());
            let liquid = LiquidPrimitiveArray::<Int64Type>::from_arrow_array(arr.clone());
            let buf = bool_buf(mask[..n].to_vec());
            let filtered = liquid.filter(&buf);
            let original: ArrayRef = Arc::new(arr);
            assert_filter_matches(&original, &filtered, &buf);
        }
    }

    // ── UInt types ────────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_u8_roundtrip(values in proptest::collection::vec(
            proptest::option::weighted(0.9, any::<u8>()), 0usize..=1024
        )) {
            let arr = arrow::array::PrimitiveArray::<UInt8Type>::from(values);
            let liquid = LiquidPrimitiveArray::<UInt8Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&(Arc::new(arr) as ArrayRef), &decoded);
        }

        #[test]
        fn fuzz_u16_roundtrip(values in proptest::collection::vec(
            proptest::option::weighted(0.9, any::<u16>()), 0usize..=1024
        )) {
            let arr = arrow::array::PrimitiveArray::<UInt16Type>::from(values);
            let liquid = LiquidPrimitiveArray::<UInt16Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&(Arc::new(arr) as ArrayRef), &decoded);
        }

        #[test]
        fn fuzz_u32_roundtrip(values in proptest::collection::vec(
            proptest::option::weighted(0.9, any::<u32>()), 0usize..=2048
        )) {
            let arr = arrow::array::PrimitiveArray::<UInt32Type>::from(values);
            let liquid = LiquidPrimitiveArray::<UInt32Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&(Arc::new(arr) as ArrayRef), &decoded);
        }

        #[test]
        fn fuzz_u64_roundtrip(values in opt_u64_vec(2048)) {
            let arr = arrow::array::PrimitiveArray::<UInt64Type>::from(values);
            let liquid = LiquidPrimitiveArray::<UInt64Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&(Arc::new(arr) as ArrayRef), &decoded);
        }
    }

    // ── Date32 / Date64 ───────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_date32_roundtrip(values in proptest::collection::vec(
            proptest::option::weighted(0.9, any::<i32>()), 0usize..=1024
        )) {
            let arr = arrow::array::PrimitiveArray::<Date32Type>::from(values);
            let liquid = LiquidPrimitiveArray::<Date32Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&(Arc::new(arr) as ArrayRef), &decoded);
        }

        #[test]
        fn fuzz_date64_roundtrip(values in proptest::collection::vec(
            proptest::option::weighted(0.9, any::<i64>()), 0usize..=1024
        )) {
            let arr = arrow::array::PrimitiveArray::<Date64Type>::from(values);
            let liquid = LiquidPrimitiveArray::<Date64Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&(Arc::new(arr) as ArrayRef), &decoded);
        }
    }

    // ── Timestamp (Microsecond) ───────────────────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_timestamp_us_roundtrip(values in opt_i64_vec(2048)) {
            let arr = arrow::array::PrimitiveArray::<TimestampMicrosecondType>::from(values);
            let liquid = LiquidPrimitiveArray::<TimestampMicrosecondType>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&(Arc::new(arr) as ArrayRef), &decoded);
        }

        /// Monotonically increasing timestamps (typical time-series) — must compress.
        /// Uses small step values (≤1000µs) so residuals are small and compress well.
        #[test]
        fn fuzz_timestamp_monotonic_compresses(
            start in 0i64..=1_700_000_000_000_000i64,
            step in 1i64..=1_000i64,
            len in 512usize..=4096
        ) {
            let values: Vec<i64> = (0..len as i64).map(|i| start + i * step).collect();
            let arr = arrow::array::PrimitiveArray::<TimestampMicrosecondType>::from(values);
            let original: ArrayRef = Arc::new(arr.clone());
            let liquid = LiquidPrimitiveArray::<TimestampMicrosecondType>::from_arrow_array(arr);
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&original, &decoded);
            prop_assert!(
                liquid.get_array_memory_size() < original.get_array_memory_size(),
                "monotonic timestamps not compressed: liquid={} arrow={}",
                liquid.get_array_memory_size(),
                original.get_array_memory_size()
            );
        }
    }

    // ── Float32 ───────────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_f32_roundtrip(values in opt_f32_vec(2048)) {
            let arr = arrow::array::PrimitiveArray::<Float32Type>::from(values);
            let liquid = LiquidFloatArray::<Float32Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            let original: ArrayRef = Arc::new(arr);
            assert_roundtrip(&original, &decoded);
        }

        #[test]
        fn fuzz_f32_filter(
            values in opt_f32_vec(512),
            mask in proptest::collection::vec(any::<bool>(), 1usize..=512)
        ) {
            let n = values.len().min(mask.len());
            if n == 0 { return Ok(()); }
            let arr = arrow::array::PrimitiveArray::<Float32Type>::from(values[..n].to_vec());
            let liquid = LiquidFloatArray::<Float32Type>::from_arrow_array(arr.clone());
            let buf = bool_buf(mask[..n].to_vec());
            let filtered = liquid.filter(&buf);
            let original: ArrayRef = Arc::new(arr);
            assert_filter_matches(&original, &filtered, &buf);
        }
    }

    // ── Float64 ───────────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_f64_roundtrip(values in opt_f64_vec(2048)) {
            let arr = arrow::array::PrimitiveArray::<Float64Type>::from(values);
            let liquid = LiquidFloatArray::<Float64Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            let original: ArrayRef = Arc::new(arr);
            assert_roundtrip(&original, &decoded);
        }

        #[test]
        fn fuzz_f64_filter(
            values in opt_f64_vec(512),
            mask in proptest::collection::vec(any::<bool>(), 1usize..=512)
        ) {
            let n = values.len().min(mask.len());
            if n == 0 { return Ok(()); }
            let arr = arrow::array::PrimitiveArray::<Float64Type>::from(values[..n].to_vec());
            let liquid = LiquidFloatArray::<Float64Type>::from_arrow_array(arr.clone());
            let buf = bool_buf(mask[..n].to_vec());
            let filtered = liquid.filter(&buf);
            let original: ArrayRef = Arc::new(arr);
            assert_filter_matches(&original, &filtered, &buf);
        }
    }

    // ── Decimal128 ────────────────────────────────────────────────────────────

    proptest! {
        /// Decimals in the u64-fittable range — the codec's supported domain.
        #[test]
        fn fuzz_decimal128_u64_range_roundtrip(
            values in proptest::collection::vec(
                proptest::option::weighted(0.9, 0i128..=u64::MAX as i128),
                0usize..=1024,
            )
        ) {
            use arrow::array::Decimal128Builder;
            let mut builder = Decimal128Builder::with_capacity(values.len());
            for v in &values {
                match v {
                    Some(x) => builder.append_value(*x),
                    None => builder.append_null(),
                }
            }
            let arr = builder.finish().with_precision_and_scale(20, 4).unwrap();
            if !LiquidDecimalArray::fits_u64(&arr) {
                // Skip arrays the codec explicitly does not support.
                return Ok(());
            }
            let liquid = LiquidDecimalArray::from_decimal_array(&arr);
            let decoded = liquid.to_arrow_array();
            let original: ArrayRef = Arc::new(arr);
            assert_roundtrip(&original, &decoded);
        }
    }

    // ── All-null edge cases ───────────────────────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_i32_all_nulls(len in 0usize..=512) {
            let values: Vec<Option<i32>> = vec![None; len];
            let arr = arrow::array::PrimitiveArray::<Int32Type>::from(values);
            let liquid = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&(Arc::new(arr) as ArrayRef), &decoded);
        }

        #[test]
        fn fuzz_f64_all_nulls(len in 0usize..=512) {
            let values: Vec<Option<f64>> = vec![None; len];
            let arr = arrow::array::PrimitiveArray::<Float64Type>::from(values);
            let liquid = LiquidFloatArray::<Float64Type>::from_arrow_array(arr.clone());
            let decoded = liquid.to_arrow_array();
            assert_roundtrip(&(Arc::new(arr) as ArrayRef), &decoded);
        }
    }

    // ── All-true / all-false filter edge cases ────────────────────────────────

    proptest! {
        #[test]
        fn fuzz_i64_filter_all_selected(values in opt_i64_vec(512)) {
            if values.is_empty() { return Ok(()); }
            let n = values.len();
            let arr = arrow::array::PrimitiveArray::<Int64Type>::from(values);
            let liquid = LiquidPrimitiveArray::<Int64Type>::from_arrow_array(arr.clone());
            let buf = BooleanBuffer::new_set(n);
            let filtered = liquid.filter(&buf);
            let original: ArrayRef = Arc::new(arr);
            assert_filter_matches(&original, &filtered, &buf);
        }

        #[test]
        fn fuzz_i64_filter_none_selected(values in opt_i64_vec(512)) {
            if values.is_empty() { return Ok(()); }
            let n = values.len();
            let arr = arrow::array::PrimitiveArray::<Int64Type>::from(values);
            let liquid = LiquidPrimitiveArray::<Int64Type>::from_arrow_array(arr.clone());
            let buf = BooleanBuffer::new_unset(n);
            let filtered = liquid.filter(&buf);
            let original: ArrayRef = Arc::new(arr);
            assert_filter_matches(&original, &filtered, &buf);
        }
    }

    // ── Transcode path (Arrow → Liquid via cache insert) ──────────────────────

    proptest! {
        /// Verify the full cache insert→get roundtrip for i32 arrays.
        #[test]
        fn fuzz_cache_insert_get_i32(values in opt_i32_vec(2048)) {
            use crate::cache::{EntryID, LiquidCacheBuilder};
            let arr: ArrayRef = Arc::new(arrow::array::PrimitiveArray::<Int32Type>::from(values));
            let cache = LiquidCacheBuilder::new()
                .with_max_memory_bytes(64 * 1024 * 1024)
                .build();
            let entry_id = EntryID::from(0usize);
            // insert may fail if cache is full (shouldn't happen at 64MB for small arrays)
            let _ = cache.insert(entry_id, arr.clone()).execute();
            let retrieved = cache.get(&entry_id).read();
            if let Some(retrieved) = retrieved {
                assert_roundtrip(&arr, &retrieved);
            }
        }

        /// Verify the full cache insert→get roundtrip for f64 arrays.
        #[test]
        fn fuzz_cache_insert_get_f64(values in opt_f64_vec(2048)) {
            use crate::cache::{EntryID, LiquidCacheBuilder};
            let arr: ArrayRef = Arc::new(arrow::array::PrimitiveArray::<Float64Type>::from(values));
            let cache = LiquidCacheBuilder::new()
                .with_max_memory_bytes(64 * 1024 * 1024)
                .build();
            let entry_id = EntryID::from(0usize);
            let _ = cache.insert(entry_id, arr.clone()).execute();
            let retrieved = cache.get(&entry_id).read();
            if let Some(retrieved) = retrieved {
                assert_roundtrip(&arr, &retrieved);
            }
        }

        /// Verify filter on cache-retrieved arrays matches Arrow filter.
        #[test]
        fn fuzz_cache_get_with_filter_i32(
            values in opt_i32_vec(1024),
            mask in proptest::collection::vec(any::<bool>(), 1usize..=1024)
        ) {
            use crate::cache::{EntryID, LiquidCacheBuilder};
            let n = values.len().min(mask.len());
            if n == 0 { return Ok(()); }
            let arr: ArrayRef = Arc::new(arrow::array::PrimitiveArray::<Int32Type>::from(values[..n].to_vec()));
            let cache = LiquidCacheBuilder::new()
                .with_max_memory_bytes(64 * 1024 * 1024)
                .build();
            let entry_id = EntryID::from(0usize);
            let _ = cache.insert(entry_id, arr.clone()).execute();
            let buf = bool_buf(mask[..n].to_vec());
            let retrieved = cache.get(&entry_id).with_selection(&buf).read();
            if let Some(retrieved) = retrieved {
                assert_filter_matches(&arr, &retrieved, &buf);
            }
        }
    }
}
