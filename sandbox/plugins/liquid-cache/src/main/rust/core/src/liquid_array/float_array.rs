///
/// Acknowledgement:
/// The ALP compression implemented in this file is based on the Rust implementation available at https://github.com/spiraldb/alp
///
use std::{any::Any, fmt::Debug, ops::Mul, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, BooleanArray, PrimitiveArray},
    buffer::{BooleanBuffer, ScalarBuffer},
    datatypes::{
        ArrowNativeType, Float32Type, Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::DataType;
use fastlanes::BitPacking;
use num_traits::{AsPrimitive, Float, FromPrimitive};

use super::LiquidDataType;
use crate::cache::LiquidExpr;
use crate::liquid_array::LiquidArray;
use crate::liquid_array::eval_predicate_on_array;
use crate::liquid_array::raw::BitPackedArray;
use crate::utils::get_bit_width;

mod private {
    use arrow::{
        array::ArrowNumericType,
        datatypes::{Float32Type, Float64Type},
    };
    use num_traits::AsPrimitive;

    pub trait Sealed: ArrowNumericType<Native: AsPrimitive<f64> + AsPrimitive<f32>> {}

    impl Sealed for Float32Type {}
    impl Sealed for Float64Type {}
}

const NUM_SAMPLES: usize = 1024; // we use FASTLANES to encode array, the sample size needs to be at least 1024 to get a good estimate of the best exponents

/// LiquidFloatType is a sealed trait that represents all the float types supported by Liquid.
/// Implementors are Float32Type and Float64Type. TODO(): What about Float16Type, decimal types?
pub trait LiquidFloatType:
        ArrowPrimitiveType<
            Native: AsPrimitive<
                <Self::UnsignedIntType as ArrowPrimitiveType>::Native // Native must be convertible to the Native type of Self::UnSignedType
            >
            + AsPrimitive<<Self::SignedIntType as ArrowPrimitiveType>::Native>
            + FromPrimitive
            + AsPrimitive<<Self as ArrowPrimitiveType>::Native>
            + Mul<<Self as ArrowPrimitiveType>::Native>
            + Float // required for decode_single and encode_single_unchecked
        >
        + private::Sealed
        + Debug
{
    type UnsignedIntType:
        ArrowPrimitiveType<
            Native: BitPacking +
                AsPrimitive<<Self as ArrowPrimitiveType>::Native>
                + AsPrimitive<<Self::SignedIntType as ArrowPrimitiveType>::Native>
                + AsPrimitive<u64>
        >
        + Debug;
    type SignedIntType:
        ArrowPrimitiveType<
            Native: AsPrimitive<<Self as ArrowPrimitiveType>::Native>
                + AsPrimitive<<Self::UnsignedIntType as ArrowPrimitiveType>::Native>
                + Ord
                + From<i32>
        >
        + Debug + Sync + Send;

    const SWEET: <Self as ArrowPrimitiveType>::Native;
    const MAX_EXPONENT: u8;
    const FRACTIONAL_BITS: u8;
    const F10: &'static [<Self as ArrowPrimitiveType>::Native];
    const IF10: &'static [<Self as ArrowPrimitiveType>::Native];

    #[inline]
    fn fast_round(val: <Self as ArrowPrimitiveType>::Native) -> <Self::SignedIntType as ArrowPrimitiveType>::Native {
        ((val + Self::SWEET) - Self::SWEET).as_()
    }

    #[inline]
    fn encode_single_unchecked(val: &<Self as ArrowPrimitiveType>::Native, exp: &Exponents) -> <Self::SignedIntType as ArrowPrimitiveType>::Native {
        Self::fast_round(*val * Self::F10[exp.e as usize] * Self::IF10[exp.f as usize])
    }

    #[inline]
    fn decode_single(val: &<Self::SignedIntType as ArrowPrimitiveType>::Native, exp: &Exponents) -> <Self as ArrowPrimitiveType>::Native {
        let decoded_float: <Self as ArrowPrimitiveType>::Native = (*val).as_();
        decoded_float * Self::F10[exp.f as usize] * Self::IF10[exp.e as usize]
    }

}

impl LiquidFloatType for Float32Type {
    type UnsignedIntType = UInt32Type;
    type SignedIntType = Int32Type;
    const FRACTIONAL_BITS: u8 = 23;
    const MAX_EXPONENT: u8 = 10;
    const SWEET: <Self as ArrowPrimitiveType>::Native = (1 << Self::FRACTIONAL_BITS)
        as <Self as ArrowPrimitiveType>::Native
        + (1 << (Self::FRACTIONAL_BITS - 1)) as <Self as ArrowPrimitiveType>::Native;
    const F10: &'static [<Self as ArrowPrimitiveType>::Native] = &[
        1.0,
        10.0,
        100.0,
        1000.0,
        10000.0,
        100000.0,
        1000000.0,
        10000000.0,
        100000000.0,
        1000000000.0,
        10000000000.0, // 10^10
    ];
    const IF10: &'static [<Self as ArrowPrimitiveType>::Native] = &[
        1.0,
        0.1,
        0.01,
        0.001,
        0.0001,
        0.00001,
        0.000001,
        0.0000001,
        0.00000001,
        0.000000001,
        0.0000000001, // 10^-10
    ];
}

impl LiquidFloatType for Float64Type {
    type UnsignedIntType = UInt64Type;
    type SignedIntType = Int64Type;
    const FRACTIONAL_BITS: u8 = 52;
    const MAX_EXPONENT: u8 = 18;
    const SWEET: <Self as ArrowPrimitiveType>::Native = (1u64 << Self::FRACTIONAL_BITS)
        as <Self as ArrowPrimitiveType>::Native
        + (1u64 << (Self::FRACTIONAL_BITS - 1)) as <Self as ArrowPrimitiveType>::Native;
    const F10: &'static [<Self as ArrowPrimitiveType>::Native] = &[
        1.0,
        10.0,
        100.0,
        1000.0,
        10000.0,
        100000.0,
        1000000.0,
        10000000.0,
        100000000.0,
        1000000000.0,
        10000000000.0,
        100000000000.0,
        1000000000000.0,
        10000000000000.0,
        100000000000000.0,
        1000000000000000.0,
        10000000000000000.0,
        100000000000000000.0,
        1000000000000000000.0,
        10000000000000000000.0,
        100000000000000000000.0,
        1000000000000000000000.0,
        10000000000000000000000.0,
        100000000000000000000000.0, // 10^23
    ];

    const IF10: &'static [<Self as ArrowPrimitiveType>::Native] = &[
        1.0,
        0.1,
        0.01,
        0.001,
        0.0001,
        0.00001,
        0.000001,
        0.0000001,
        0.00000001,
        0.000000001,
        0.0000000001,
        0.00000000001,
        0.000000000001,
        0.0000000000001,
        0.00000000000001,
        0.000000000000001,
        0.0000000000000001,
        0.00000000000000001,
        0.000000000000000001,
        0.0000000000000000001,
        0.00000000000000000001,
        0.000000000000000000001,
        0.0000000000000000000001,
        0.00000000000000000000001, // 10^-23
    ];
}

/// Liquid's single-precision floating point array
pub type LiquidFloat32Array = LiquidFloatArray<Float32Type>;
/// Liquid's double precision floating point array
pub type LiquidFloat64Array = LiquidFloatArray<Float64Type>;

/// An array that stores floats in ALP
#[derive(Debug, Clone)]
pub struct LiquidFloatArray<T: LiquidFloatType> {
    exponent: Exponents,
    bit_packed: BitPackedArray<T::UnsignedIntType>,
    patch_indices: Vec<u64>,
    patch_values: Vec<T::Native>,
    reference_value: <T::SignedIntType as ArrowPrimitiveType>::Native,
}

impl<T> LiquidFloatArray<T>
where
    T: LiquidFloatType,
{
    /// Check if the Liquid float array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the length of the Liquid float array.
    pub fn len(&self) -> usize {
        self.bit_packed.len()
    }

    /// Get the memory size of the Liquid primitive array.
    pub fn get_array_memory_size(&self) -> usize {
        self.bit_packed.get_array_memory_size()
            + size_of::<Exponents>()
            + self.patch_indices.capacity() * size_of::<u64>()
            + self.patch_values.capacity() * size_of::<T::Native>()
            + size_of::<<T::SignedIntType as ArrowPrimitiveType>::Native>()
    }

    /// Create a Liquid primitive array from an Arrow float array.
    pub fn from_arrow_array(arrow_array: arrow::array::PrimitiveArray<T>) -> LiquidFloatArray<T> {
        let best_exponents = get_best_exponents::<T>(&arrow_array);
        encode_arrow_array(&arrow_array, &best_exponents)
    }
}

impl<T> LiquidArray for LiquidFloatArray<T>
where
    T: LiquidFloatType,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_array_memory_size(&self) -> usize {
        self.get_array_memory_size()
    }

    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn to_arrow_array(&self) -> ArrayRef {
        let unsigned_array = self.bit_packed.to_primitive();
        let (_data_type, values, _nulls) = unsigned_array.into_parts();
        let nulls = self.bit_packed.nulls();
        // TODO(): Check if we should align vectors to cache line boundary
        let mut decoded_values = Vec::from_iter(values.iter().map(|v| {
            let mut val: <T::SignedIntType as ArrowPrimitiveType>::Native = (*v).as_();
            val = val.add_wrapping(self.reference_value);
            T::decode_single(&val, &self.exponent)
        }));

        // Patch values
        if !self.patch_indices.is_empty() {
            for i in 0..self.patch_indices.len() {
                decoded_values[self.patch_indices[i].as_usize()] = self.patch_values[i];
            }
        }

        Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::<<T as ArrowPrimitiveType>::Native>::from(decoded_values),
            nulls.cloned(),
        ))
    }

    fn original_arrow_data_type(&self) -> DataType {
        T::DATA_TYPE.clone()
    }

    fn data_type(&self) -> LiquidDataType {
        LiquidDataType::Float
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn to_best_arrow_array(&self) -> ArrayRef {
        self.to_arrow_array()
    }

    fn try_eval_predicate(&self, predicate: &LiquidExpr, filter: &BooleanBuffer) -> BooleanArray {
        let filtered = self.filter(filter);
        eval_predicate_on_array(filtered, predicate)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Exponents {
    pub(crate) e: u8,
    pub(crate) f: u8,
}

fn encode_arrow_array<T: LiquidFloatType>(
    arrow_array: &PrimitiveArray<T>,
    exp: &Exponents, // fill_value: &mut Option<<T::UnsignedIntType as ArrowPrimitiveType>::Native>
) -> LiquidFloatArray<T> {
    let mut patch_indices: Vec<u64> = Vec::new();
    let mut patch_values: Vec<T::Native> = Vec::new();
    let mut patch_count: usize = 0;
    let mut fill_value: Option<<T::SignedIntType as ArrowPrimitiveType>::Native> = None;
    let values = arrow_array.values();
    let nulls = arrow_array.nulls();

    // All values are null
    if arrow_array.null_count() == arrow_array.len() {
        return LiquidFloatArray::<T> {
            bit_packed: BitPackedArray::new_null_array(arrow_array.len()),
            exponent: Exponents { e: 0, f: 0 },
            patch_indices: Vec::new(),
            patch_values: Vec::new(),
            reference_value: <T::SignedIntType as ArrowPrimitiveType>::Native::ZERO,
        };
    }

    let mut encoded_values = Vec::with_capacity(arrow_array.len());
    for v in values.iter() {
        let encoded = T::encode_single_unchecked(&v.as_(), exp);
        let decoded = T::decode_single(&encoded, exp);
        // TODO(): Check if this is a bitwise comparison
        let neq = !decoded.eq(&v.as_()) as usize;
        patch_count += neq;
        encoded_values.push(encoded);
    }

    if patch_count > 0 {
        patch_indices.resize_with(patch_count + 1, Default::default);
        patch_values.resize_with(patch_count + 1, Default::default);
        let mut patch_index: usize = 0;

        for i in 0..encoded_values.len() {
            let decoded = T::decode_single(&encoded_values[i], exp);
            patch_indices[patch_index] = i.as_();
            patch_values[patch_index] = arrow_array.value(i).as_();
            patch_index += !(decoded.eq(&values[i].as_())) as usize;
        }
        assert_eq!(patch_index, patch_count);
        unsafe {
            patch_indices.set_len(patch_count);
            patch_values.set_len(patch_count);
        }
    }

    // find the first successfully encoded value (i.e., not patched)
    // this is our fill value for missing values
    if patch_count > 0 && patch_count < arrow_array.len() {
        for i in 0..encoded_values.len() {
            if i >= patch_indices.len() || patch_indices[i] != i as u64 {
                fill_value = encoded_values.get(i).copied();
                break;
            }
        }
    }

    // replace the patched values in the encoded array with the fill value
    // for better downstream compression
    if let Some(fill_value) = fill_value {
        // handle the edge case where the first N >= 1 chunks are all patches
        for patch_idx in &patch_indices {
            encoded_values[*patch_idx as usize] = fill_value;
        }
    }

    let min = *encoded_values
        .iter()
        .min()
        .expect("`encoded_values` shouldn't be all nulls");
    let max = *encoded_values
        .iter()
        .max()
        .expect("`encoded_values` shouldn't be all nulls");
    let sub: <T::UnsignedIntType as ArrowPrimitiveType>::Native = max.sub_wrapping(min).as_();

    let unsigned_encoded_values = encoded_values
        .iter()
        .map(|v| {
            let k: <T::UnsignedIntType as ArrowPrimitiveType>::Native = v.sub_wrapping(min).as_();
            k
        })
        .collect::<Vec<_>>();
    let encoded_output = PrimitiveArray::<<T as LiquidFloatType>::UnsignedIntType>::new(
        ScalarBuffer::from(unsigned_encoded_values),
        nulls.cloned(),
    );

    let bit_width = get_bit_width(sub.as_());
    let bit_packed_array = BitPackedArray::from_primitive(encoded_output, bit_width);

    LiquidFloatArray::<T> {
        bit_packed: bit_packed_array,
        exponent: *exp,
        patch_indices,
        patch_values,
        reference_value: min,
    }
}

fn get_best_exponents<T: LiquidFloatType>(arrow_array: &PrimitiveArray<T>) -> Exponents {
    let mut best_exponents = Exponents { e: 0, f: 0 };
    let mut min_encoded_size: usize = usize::MAX;

    let sample_arrow_array: Option<PrimitiveArray<T>> =
        (arrow_array.len() > NUM_SAMPLES).then(|| {
            arrow_array
                .iter()
                .step_by(arrow_array.len() / NUM_SAMPLES)
                .filter(|s| s.is_some())
                .collect()
        });

    for e in 0..T::MAX_EXPONENT {
        for f in 0..e {
            let exp = Exponents { e, f };
            let liquid_array =
                encode_arrow_array(sample_arrow_array.as_ref().unwrap_or(arrow_array), &exp);
            if liquid_array.get_array_memory_size() < min_encoded_size {
                best_exponents = exp;
                min_encoded_size = liquid_array.get_array_memory_size();
            }
        }
    }
    best_exponents
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_roundtrip {
        ($test_name: ident, $type:ty, $values: expr) => {
            #[test]
            fn $test_name() {
                let original: Vec<Option<<$type as ArrowPrimitiveType>::Native>> = $values;
                let array = PrimitiveArray::<$type>::from(original.clone());

                // Convert to Liquid array and back
                let liquid_array = LiquidFloatArray::<$type>::from_arrow_array(array.clone());
                let result_array = liquid_array.to_arrow_array();

                assert_eq!(result_array.as_ref(), &array);
            }
        };
    }

    // Test cases for Float32
    test_roundtrip!(
        test_float32_roundtrip_basic,
        Float32Type,
        vec![Some(-1.0), Some(1.0), Some(0.0)]
    );

    test_roundtrip!(
        test_float32_roundtrip_with_nones,
        Float32Type,
        vec![Some(-1.0), Some(1.0), Some(0.0), None]
    );

    test_roundtrip!(
        test_float32_roundtrip_all_nones,
        Float32Type,
        vec![None, None, None, None]
    );

    test_roundtrip!(test_float32_roundtrip_empty, Float32Type, vec![]);

    // Test cases for Float64
    test_roundtrip!(
        test_float64_roundtrip_basic,
        Float64Type,
        vec![Some(-1.0), Some(1.0), Some(0.0)]
    );

    test_roundtrip!(
        test_float64_roundtrip_with_nones,
        Float64Type,
        vec![Some(-1.0), Some(1.0), Some(0.0), None]
    );

    test_roundtrip!(
        test_float64_roundtrip_all_nones,
        Float64Type,
        vec![None, None, None, None]
    );

    test_roundtrip!(test_float64_roundtrip_empty, Float64Type, vec![]);

    // Tests with ilters
    #[test]
    fn test_filter_basic() {
        // Create original array with some values
        let original = vec![Some(1.0), Some(2.1), Some(3.2), None, Some(5.5)];
        let array = PrimitiveArray::<Float32Type>::from(original);
        let liquid_array = LiquidFloatArray::<Float32Type>::from_arrow_array(array);

        // Create selection mask: keep indices 0, 2, and 4
        let selection = BooleanBuffer::from(vec![true, false, true, false, true]);

        // Apply filter
        let result_array = liquid_array.filter(&selection);

        // Expected result after filtering
        let expected = PrimitiveArray::<Float32Type>::from(vec![Some(1.0), Some(3.2), Some(5.5)]);

        assert_eq!(result_array.as_ref(), &expected);
    }

    #[test]
    fn test_original_arrow_data_type_returns_float32() {
        let array = PrimitiveArray::<Float32Type>::from(vec![Some(1.0), Some(2.5)]);
        let liquid = LiquidFloatArray::<Float32Type>::from_arrow_array(array);
        assert_eq!(liquid.original_arrow_data_type(), DataType::Float32);
    }

    #[test]
    fn test_filter_all_nulls() {
        // Create array with all nulls
        let original = vec![None, None, None, None];
        let array = PrimitiveArray::<Float32Type>::from(original);
        let liquid_array = LiquidFloatArray::<Float32Type>::from_arrow_array(array);

        // Keep first and last elements
        let selection = BooleanBuffer::from(vec![true, false, false, true]);

        let result_array = liquid_array.filter(&selection);

        let expected = PrimitiveArray::<Float32Type>::from(vec![None, None]);

        assert_eq!(result_array.as_ref(), &expected);
    }

    #[test]
    fn test_filter_empty_result() {
        let original = vec![Some(1.0), Some(2.1), Some(3.3)];
        let array = PrimitiveArray::<Float32Type>::from(original);
        let liquid_array = LiquidFloatArray::<Float32Type>::from_arrow_array(array);

        // Filter out all elements
        let selection = BooleanBuffer::from(vec![false, false, false]);

        let result_array = liquid_array.filter(&selection);

        assert_eq!(result_array.len(), 0);
    }

    #[test]
    fn test_compression_f32_f64() {
        fn run_compression_test<T: LiquidFloatType>(
            type_name: &str,
            data_fn: impl Fn(usize) -> T::Native,
        ) {
            let original: Vec<T::Native> = (0..2000).map(data_fn).collect();
            let array = PrimitiveArray::<T>::from_iter_values(original);
            let uncompressed_size = array.get_array_memory_size();

            let liquid_array = LiquidFloatArray::<T>::from_arrow_array(array);
            let compressed_size = liquid_array.get_array_memory_size();

            println!(
                "Type: {type_name}, uncompressed_size: {uncompressed_size}, compressed_size: {compressed_size}"
            );
            // Assert that compression actually reduced the size
            assert!(
                compressed_size < uncompressed_size,
                "{type_name} compression failed to reduce size"
            );
        }

        // Run for f32
        run_compression_test::<Float32Type>("f32", |i| i as f32);

        // Run for f64
        run_compression_test::<Float64Type>("f64", |i| i as f64);
    }
}
