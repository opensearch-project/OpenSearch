use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, BooleanArray, PrimitiveArray,
    types::{
        Date32Type, Date64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow::buffer::{BooleanBuffer, ScalarBuffer};
use arrow_schema::DataType;
use fastlanes::BitPacking;
use num_traits::{AsPrimitive, FromPrimitive};

use super::LiquidDataType;
use crate::cache::LiquidExpr;
use crate::liquid_array::raw::BitPackedArray;
use crate::liquid_array::{LiquidArray, PrimitiveKind, eval_predicate_on_array};
use crate::utils::get_bit_width;
use arrow::datatypes::ArrowNativeType;

mod private {
    pub trait Sealed {}
}

/// LiquidPrimitiveType is a sealed trait that represents the primitive types supported by Liquid.
/// Implemented for all supported integer, date, and timestamp Arrow primitive types.
///
/// I have to admit this trait is super complicated.
/// Luckily users never have to worry about it, they can just use the types that are already implemented.
/// We could have implemented this as a macro, but macro is ugly.
/// Type is spec, code is proof.
pub trait LiquidPrimitiveType:
    ArrowPrimitiveType<
        Native: AsPrimitive<<Self::UnSignedType as ArrowPrimitiveType>::Native>
                    + AsPrimitive<i64>
                    + FromPrimitive
                    + Display,
    > + Debug
    + Send
    + Sync
    + private::Sealed
    + PrimitiveKind
{
    /// The unsigned type that can be used to represent the signed type.
    type UnSignedType: ArrowPrimitiveType<Native: AsPrimitive<Self::Native> + AsPrimitive<u64> + BitPacking>
        + Debug;
}

macro_rules! impl_has_unsigned_type {
    ($($signed:ty => $unsigned:ty),*) => {
        $(
            impl private::Sealed for $signed {}
            impl LiquidPrimitiveType for $signed {
                type UnSignedType = $unsigned;
            }
        )*
    }
}

impl_has_unsigned_type! {
    Int32Type => UInt32Type,
    Int64Type => UInt64Type,
    Int16Type => UInt16Type,
    Int8Type => UInt8Type,
    UInt32Type => UInt32Type,
    UInt64Type => UInt64Type,
    UInt16Type => UInt16Type,
    UInt8Type => UInt8Type,
    Date64Type => UInt64Type,
    Date32Type => UInt32Type,
    TimestampSecondType => UInt64Type,
    TimestampMillisecondType => UInt64Type,
    TimestampMicrosecondType => UInt64Type,
    TimestampNanosecondType => UInt64Type
}

/// Liquid's unsigned 8-bit integer array.
pub type LiquidU8Array = LiquidPrimitiveArray<UInt8Type>;
/// Liquid's unsigned 16-bit integer array.
pub type LiquidU16Array = LiquidPrimitiveArray<UInt16Type>;
/// Liquid's unsigned 32-bit integer array.
pub type LiquidU32Array = LiquidPrimitiveArray<UInt32Type>;
/// Liquid's unsigned 64-bit integer array.
pub type LiquidU64Array = LiquidPrimitiveArray<UInt64Type>;
/// Liquid's signed 8-bit integer array.
pub type LiquidI8Array = LiquidPrimitiveArray<Int8Type>;
/// Liquid's signed 16-bit integer array.
pub type LiquidI16Array = LiquidPrimitiveArray<Int16Type>;
/// Liquid's signed 32-bit integer array.
pub type LiquidI32Array = LiquidPrimitiveArray<Int32Type>;
/// Liquid's signed 64-bit integer array.
pub type LiquidI64Array = LiquidPrimitiveArray<Int64Type>;
/// Liquid's 32-bit date array.
pub type LiquidDate32Array = LiquidPrimitiveArray<Date32Type>;
/// Liquid's 64-bit date array.
pub type LiquidDate64Array = LiquidPrimitiveArray<Date64Type>;

/// Liquid's primitive array
#[derive(Debug)]
pub struct LiquidPrimitiveArray<T: LiquidPrimitiveType> {
    bit_packed: BitPackedArray<T::UnSignedType>,
    reference_value: T::Native,
}

/// Liquid's primitive array which uses delta encoding for compression
#[derive(Debug, Clone)]
pub struct LiquidPrimitiveDeltaArray<T: LiquidPrimitiveType> {
    bit_packed: BitPackedArray<T::UnSignedType>,
    reference_value: T::Native,
}

impl<T> LiquidPrimitiveArray<T>
where
    T: LiquidPrimitiveType,
{
    /// Get the memory size of the Liquid primitive array.
    pub fn get_array_memory_size(&self) -> usize {
        self.bit_packed.get_array_memory_size() + std::mem::size_of::<T::Native>()
    }

    /// Get the length of the Liquid primitive array.
    pub fn len(&self) -> usize {
        self.bit_packed.len()
    }

    /// Check if the Liquid primitive array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a Liquid primitive array from an Arrow primitive array.
    pub fn from_arrow_array(arrow_array: PrimitiveArray<T>) -> LiquidPrimitiveArray<T> {
        let min = match arrow::compute::kernels::aggregate::min(&arrow_array) {
            Some(v) => v,
            None => {
                // entire array is null
                return Self {
                    bit_packed: BitPackedArray::new_null_array(arrow_array.len()),
                    reference_value: T::Native::ZERO,
                };
            }
        };
        let max = arrow::compute::kernels::aggregate::max(&arrow_array).unwrap();

        // be careful of overflow:
        // Want: 127i8 - (-128i8) -> 255u64,
        // but we get -1i8
        // (-1i8) as u8 as u64 -> 255u64
        let sub = max.sub_wrapping(min) as <T as ArrowPrimitiveType>::Native;
        let sub: <<T as LiquidPrimitiveType>::UnSignedType as ArrowPrimitiveType>::Native =
            sub.as_();
        let bit_width = get_bit_width(sub.as_());

        let (_data_type, values, nulls) = arrow_array.clone().into_parts();
        let values = if min != T::Native::ZERO {
            ScalarBuffer::from_iter(values.iter().map(|v| {
                let k: <<T as LiquidPrimitiveType>::UnSignedType as ArrowPrimitiveType>::Native =
                    v.sub_wrapping(min).as_();
                k
            }))
        } else {
            #[allow(clippy::missing_transmute_annotations)]
            unsafe {
                std::mem::transmute(values)
            }
        };

        let unsigned_array =
            PrimitiveArray::<<T as LiquidPrimitiveType>::UnSignedType>::new(values, nulls);

        let bit_packed_array = BitPackedArray::from_primitive(unsigned_array, bit_width);

        Self {
            bit_packed: bit_packed_array,
            reference_value: min,
        }
    }
}

impl<T> LiquidPrimitiveDeltaArray<T>
where
    T: LiquidPrimitiveType,
{
    /// Get the memory size of the Liquid primitive delta array.
    pub fn get_array_memory_size(&self) -> usize {
        self.bit_packed.get_array_memory_size() + std::mem::size_of::<T::Native>()
    }

    /// Get the length of the Liquid primitive delta array.
    pub fn len(&self) -> usize {
        self.bit_packed.len()
    }

    /// Check if the Liquid primitive delta array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a Liquid primitive delta array from an Arrow primitive array.
    pub fn from_arrow_array(arrow_array: PrimitiveArray<T>) -> LiquidPrimitiveDeltaArray<T> {
        use arrow::array::Array;

        let len = arrow_array.len();
        // check if entire array is already null
        if arrow_array.null_count() == len {
            return Self {
                bit_packed: BitPackedArray::new_null_array(len),
                reference_value: T::Native::ZERO,
            };
        }

        let (_dt, values, nulls) = arrow_array.clone().into_parts();
        let vals: Vec<T::Native> = values.to_vec();

        type UnsignedNative<TT> =
            <<TT as LiquidPrimitiveType>::UnSignedType as ArrowPrimitiveType>::Native;
        let mut out: Vec<UnsignedNative<T>> = Vec::with_capacity(len);
        let mut max_value: UnsignedNative<T> = UnsignedNative::<T>::ZERO;
        let mut anchor: T::Native = T::Native::ZERO;

        if let Some(_nb) = &nulls {
            // Nulls present: write 0 for nulls; prev will the last prev non-null value
            let nb = nulls.as_ref().unwrap();
            let mut have_prev = false;
            let mut prev: T::Native = T::Native::ZERO;

            for (i, &cur) in vals.iter().enumerate() {
                if !nb.is_valid(i) {
                    out.push(UnsignedNative::<T>::ZERO);
                    continue;
                }
                if !have_prev {
                    anchor = cur;
                    prev = cur;
                    have_prev = true;
                    out.push(UnsignedNative::<T>::ZERO);
                    continue;
                }
                let delta: T::Native = cur.sub_wrapping(prev);
                // zig zag encoding
                let delta_i64: i64 = delta.as_();
                let zigzag: u64 = ((delta_i64 << 1) ^ (delta_i64 >> 63)) as u64;
                let delta_unsigned: UnsignedNative<T> =
                    UnsignedNative::<T>::usize_as(zigzag as usize);
                if delta_unsigned > max_value {
                    max_value = delta_unsigned;
                }
                out.push(delta_unsigned);
                prev = cur;
            }
        } else {
            // No nulls: first value is anchor, remainder are deltas with their previous values
            anchor = vals[0];
            let mut prev: T::Native = anchor;
            out.push(UnsignedNative::<T>::ZERO); // anchor will have a difference of 0
            for &cur in vals.iter().skip(1) {
                let delta: T::Native = cur.sub_wrapping(prev);
                // zig zag encoding
                let delta_i64: i64 = delta.as_();
                let zigzag: u64 = ((delta_i64 << 1) ^ (delta_i64 >> 63)) as u64;
                let delta_unsigned: UnsignedNative<T> =
                    UnsignedNative::<T>::usize_as(zigzag as usize);
                if delta_unsigned > max_value {
                    max_value = delta_unsigned;
                }
                out.push(delta_unsigned);
                prev = cur;
            }
        }

        let bit_width = get_bit_width(max_value.as_());
        let values = ScalarBuffer::from_iter(out);
        let unsigned_array =
            PrimitiveArray::<<T as LiquidPrimitiveType>::UnSignedType>::new(values, nulls);
        let bit_packed_array = BitPackedArray::from_primitive(unsigned_array, bit_width);

        Self {
            bit_packed: bit_packed_array,
            reference_value: anchor,
        }
    }
}

impl<T> LiquidArray for LiquidPrimitiveArray<T>
where
    T: LiquidPrimitiveType + super::PrimitiveKind,
{
    fn get_array_memory_size(&self) -> usize {
        self.get_array_memory_size()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn original_arrow_data_type(&self) -> DataType {
        T::DATA_TYPE.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn to_arrow_array(&self) -> ArrayRef {
        let unsigned_array = self.bit_packed.to_primitive();
        let (_data_type, values, _nulls) = unsigned_array.into_parts();
        let nulls = self.bit_packed.nulls();
        let values = if self.reference_value != T::Native::ZERO {
            let reference_v = self.reference_value.as_();
            ScalarBuffer::from_iter(values.iter().map(|v| {
                let k: <T as ArrowPrimitiveType>::Native = (*v).add_wrapping(reference_v).as_();
                k
            }))
        } else {
            #[allow(clippy::missing_transmute_annotations)]
            unsafe {
                std::mem::transmute(values)
            }
        };

        Arc::new(PrimitiveArray::<T>::new(values, nulls.cloned()))
    }

    fn filter(&self, selection: &BooleanBuffer) -> ArrayRef {
        let arrow_array = self.to_arrow_array();
        let selection = BooleanArray::new(selection.clone(), None);
        arrow::compute::kernels::filter::filter(&arrow_array, &selection).unwrap()
    }

    fn try_eval_predicate(&self, predicate: &LiquidExpr, filter: &BooleanBuffer) -> BooleanArray {
        let filtered = self.filter(filter);
        eval_predicate_on_array(filtered, predicate)
    }

    fn data_type(&self) -> LiquidDataType {
        LiquidDataType::Integer
    }
}

impl<T> LiquidArray for LiquidPrimitiveDeltaArray<T>
where
    T: LiquidPrimitiveType + super::PrimitiveKind,
{
    fn get_array_memory_size(&self) -> usize {
        self.get_array_memory_size()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn original_arrow_data_type(&self) -> DataType {
        T::DATA_TYPE.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn to_arrow_array(&self) -> ArrayRef {
        // Reconstruct original values from deltas
        let unsigned_array = self.bit_packed.to_primitive();
        let (_data_type, delta_values, _nulls) = unsigned_array.into_parts();
        let nulls = self.bit_packed.nulls();

        // Reconstruct original values by applying deltas
        let mut reconstructed = Vec::with_capacity(delta_values.len());
        let mut current_value = self.reference_value; // anchor

        if let Some(nulls) = nulls {
            let mut have_prev = false;
            for (i, &delta_unsigned) in delta_values.iter().enumerate() {
                if !nulls.is_valid(i) {
                    reconstructed.push(T::Native::ZERO); // Will be masked out by nulls
                    continue;
                }
                if !have_prev {
                    // First non-null value is the anchor
                    reconstructed.push(current_value);
                    have_prev = true;
                } else {
                    // Apply delta to get next value
                    let zigzag: u64 = delta_unsigned.as_();
                    let delta_i64 = (zigzag >> 1) as i64 ^ -((zigzag & 1) as i64);
                    let delta: T::Native = T::Native::from_i64(delta_i64).unwrap();
                    current_value = current_value.add_wrapping(delta);
                    reconstructed.push(current_value);
                }
            }
        } else {
            // No nulls case
            reconstructed.push(current_value); // First value is anchor
            for &delta_unsigned in delta_values.iter().skip(1) {
                let zigzag: u64 = delta_unsigned.as_();
                let delta_i64 = (zigzag >> 1) as i64 ^ -((zigzag & 1) as i64);
                let delta: T::Native = T::Native::from_i64(delta_i64).unwrap();
                current_value = current_value.add_wrapping(delta);
                reconstructed.push(current_value);
            }
        }

        let values = ScalarBuffer::from_iter(reconstructed);
        Arc::new(PrimitiveArray::<T>::new(values, nulls.cloned()))
    }

    fn filter(&self, selection: &BooleanBuffer) -> ArrayRef {
        let arrow_array = self.to_arrow_array();
        let selection = BooleanArray::new(selection.clone(), None);
        arrow::compute::kernels::filter::filter(&arrow_array, &selection).unwrap()
    }

    fn try_eval_predicate(&self, predicate: &LiquidExpr, filter: &BooleanBuffer) -> BooleanArray {
        let filtered = self.filter(filter);
        eval_predicate_on_array(filtered, predicate)
    }

    fn data_type(&self) -> LiquidDataType {
        LiquidDataType::Integer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    macro_rules! test_roundtrip {
        ($test_name:ident, $type:ty, $values:expr) => {
            #[test]
            fn $test_name() {
                // Create the original array
                let original: Vec<Option<<$type as ArrowPrimitiveType>::Native>> = $values;
                let array = PrimitiveArray::<$type>::from(original.clone());

                // Convert to Liquid array and back
                let liquid_array = LiquidPrimitiveArray::<$type>::from_arrow_array(array.clone());
                let result_array = liquid_array.to_arrow_array();

                assert_eq!(result_array.as_ref(), &array);
            }
        };
    }

    // Test cases for Int8Type
    test_roundtrip!(
        test_int8_roundtrip_basic,
        Int8Type,
        vec![Some(1), Some(2), Some(3), None, Some(5)]
    );
    test_roundtrip!(
        test_int8_roundtrip_negative,
        Int8Type,
        vec![Some(-128), Some(-64), Some(0), Some(63), Some(127)]
    );

    // Test cases for Int16Type
    test_roundtrip!(
        test_int16_roundtrip_basic,
        Int16Type,
        vec![Some(1), Some(2), Some(3), None, Some(5)]
    );
    test_roundtrip!(
        test_int16_roundtrip_negative,
        Int16Type,
        vec![
            Some(-32768),
            Some(-16384),
            Some(0),
            Some(16383),
            Some(32767)
        ]
    );

    // Test cases for Int32Type
    test_roundtrip!(
        test_int32_roundtrip_basic,
        Int32Type,
        vec![Some(1), Some(2), Some(3), None, Some(5)]
    );
    test_roundtrip!(
        test_int32_roundtrip_negative,
        Int32Type,
        vec![
            Some(-2147483648),
            Some(-1073741824),
            Some(0),
            Some(1073741823),
            Some(2147483647)
        ]
    );

    // Test cases for Int64Type
    test_roundtrip!(
        test_int64_roundtrip_basic,
        Int64Type,
        vec![Some(1), Some(2), Some(3), None, Some(5)]
    );
    test_roundtrip!(
        test_int64_roundtrip_negative,
        Int64Type,
        vec![
            Some(-9223372036854775808),
            Some(-4611686018427387904),
            Some(0),
            Some(4611686018427387903),
            Some(9223372036854775807)
        ]
    );

    // Test cases for unsigned types
    test_roundtrip!(
        test_uint8_roundtrip,
        UInt8Type,
        vec![Some(0), Some(128), Some(255), None, Some(64)]
    );
    test_roundtrip!(
        test_uint16_roundtrip,
        UInt16Type,
        vec![Some(0), Some(32768), Some(65535), None, Some(16384)]
    );
    test_roundtrip!(
        test_uint32_roundtrip,
        UInt32Type,
        vec![
            Some(0),
            Some(2147483648),
            Some(4294967295),
            None,
            Some(1073741824)
        ]
    );
    test_roundtrip!(
        test_uint64_roundtrip,
        UInt64Type,
        vec![
            Some(0),
            Some(9223372036854775808),
            Some(18446744073709551615),
            None,
            Some(4611686018427387904)
        ]
    );

    test_roundtrip!(
        test_date32_roundtrip,
        Date32Type,
        vec![Some(-365), Some(0), Some(365), None, Some(18262)]
    );

    test_roundtrip!(
        test_date64_roundtrip,
        Date64Type,
        vec![Some(-365), Some(0), Some(365), None, Some(18262)]
    );

    // Edge cases
    #[test]
    fn test_all_nulls() {
        let original: Vec<Option<i32>> = vec![None, None, None];
        let array = PrimitiveArray::<Int32Type>::from(original.clone());
        let liquid_array = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(array);
        let result_array = liquid_array.to_arrow_array();

        assert_eq!(result_array.len(), original.len());
        assert_eq!(result_array.null_count(), original.len());
    }

    #[test]
    fn test_all_nulls_filter() {
        let original: Vec<Option<i32>> = vec![None, None, None];
        let array = PrimitiveArray::<Int32Type>::from(original.clone());
        let liquid_array = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(array);
        let result_array = liquid_array.filter(&BooleanBuffer::from(vec![true, false, true]));

        assert_eq!(result_array.len(), 2);
        assert_eq!(result_array.null_count(), 2);
    }

    #[test]
    fn test_zero_reference_value() {
        let original: Vec<Option<i32>> = vec![Some(0), Some(1), Some(2), None, Some(4)];
        let array = PrimitiveArray::<Int32Type>::from(original.clone());
        let liquid_array = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(array.clone());
        let result_array = liquid_array.to_arrow_array();

        assert_eq!(liquid_array.reference_value, 0);
        assert_eq!(result_array.as_ref(), &array);
    }

    #[test]
    fn test_single_value() {
        let original: Vec<Option<i32>> = vec![Some(42)];
        let array = PrimitiveArray::<Int32Type>::from(original.clone());
        let liquid_array = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(array.clone());
        let result_array = liquid_array.to_arrow_array();

        assert_eq!(result_array.as_ref(), &array);
    }

    #[test]
    fn test_filter_basic() {
        // Create original array with some values
        let original = vec![Some(1), Some(2), Some(3), None, Some(5)];
        let array = PrimitiveArray::<Int32Type>::from(original);
        let liquid_array = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(array);

        // Create selection mask: keep indices 0, 2, and 4
        let selection = BooleanBuffer::from(vec![true, false, true, false, true]);

        // Apply filter
        let result_array = liquid_array.filter(&selection);

        // Expected result after filtering
        let expected = PrimitiveArray::<Int32Type>::from(vec![Some(1), Some(3), Some(5)]);

        assert_eq!(result_array.as_ref(), &expected);
    }

    #[test]
    fn test_original_arrow_data_type_returns_int32() {
        let array = PrimitiveArray::<Int32Type>::from(vec![Some(1), Some(2)]);
        let liquid = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(array);
        assert_eq!(liquid.original_arrow_data_type(), DataType::Int32);
    }

    #[test]
    fn test_filter_all_nulls() {
        // Create array with all nulls
        let original = vec![None, None, None, None];
        let array = PrimitiveArray::<Int32Type>::from(original);
        let liquid_array = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(array);

        // Keep first and last elements
        let selection = BooleanBuffer::from(vec![true, false, false, true]);

        let result_array = liquid_array.filter(&selection);

        let expected = PrimitiveArray::<Int32Type>::from(vec![None, None]);

        assert_eq!(result_array.as_ref(), &expected);
    }

    #[test]
    fn test_filter_empty_result() {
        let original = vec![Some(1), Some(2), Some(3)];
        let array = PrimitiveArray::<Int32Type>::from(original);
        let liquid_array = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(array);

        // Filter out all elements
        let selection = BooleanBuffer::from(vec![false, false, false]);

        let result_array = liquid_array.filter(&selection);

        assert_eq!(result_array.len(), 0);
    }

    #[test]
    fn test_delta_encoding_basic_roundtrip() {
        let original = vec![Some(1), Some(3), Some(6), Some(10), Some(15)];
        let array = PrimitiveArray::<Int32Type>::from(original.clone());

        let liquid_delta = LiquidPrimitiveDeltaArray::<Int32Type>::from_arrow_array(array.clone());
        let result_array = liquid_delta.to_arrow_array();

        assert_eq!(result_array.as_ref(), &array);
    }

    #[test]
    fn test_delta_encoding_with_nulls() {
        let original = vec![Some(1), None, Some(4), Some(7), None, Some(12)];
        let array = PrimitiveArray::<Int32Type>::from(original.clone());

        let liquid_delta = LiquidPrimitiveDeltaArray::<Int32Type>::from_arrow_array(array.clone());
        let result_array = liquid_delta.to_arrow_array();

        assert_eq!(result_array.as_ref(), &array);
    }

    #[test]
    fn test_memory_comparison_sequential_data() {
        // Sequential data: delta encoding performs better
        let sequential_data: Vec<Option<i32>> = (0..1000).map(Some).collect();
        let array = PrimitiveArray::<Int32Type>::from(sequential_data);

        let liquid_regular = LiquidPrimitiveArray::<Int32Type>::from_arrow_array(array.clone());
        let liquid_delta = LiquidPrimitiveDeltaArray::<Int32Type>::from_arrow_array(array);

        let regular_size = liquid_regular.get_array_memory_size();
        let delta_size = liquid_delta.get_array_memory_size();

        println!(
            "Sequential data - Regular: {} bytes, Delta: {} bytes",
            regular_size, delta_size
        );
        assert!(
            delta_size <= regular_size,
            "Delta encoding should be more efficient for sequential data"
        );
    }
}
