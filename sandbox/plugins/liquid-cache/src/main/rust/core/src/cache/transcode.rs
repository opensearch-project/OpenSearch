use std::sync::Arc;

use arrow::array::types::*;
use arrow::array::{ArrayRef, AsArray};
use arrow_schema::{DataType, TimeUnit};

use crate::liquid_array::{
    LiquidArrayRef, LiquidDecimalArray, LiquidFloatArray, LiquidPrimitiveArray,
};

/// This method is used to transcode an arrow array into a liquid array.
///
/// Returns the transcoded liquid array if successful, otherwise returns the original arrow array.
pub fn transcode_liquid_inner(array: &ArrayRef) -> Result<LiquidArrayRef, &ArrayRef> {
    let data_type = array.data_type();
    if data_type.is_primitive() {
        // For primitive types, perform the transcoding.
        let liquid_array: LiquidArrayRef = match data_type {
            DataType::Int8 => Arc::new(LiquidPrimitiveArray::<Int8Type>::from_arrow_array(
                array.as_primitive::<Int8Type>().clone(),
            )),
            DataType::Int16 => Arc::new(LiquidPrimitiveArray::<Int16Type>::from_arrow_array(
                array.as_primitive::<Int16Type>().clone(),
            )),
            DataType::Int32 => Arc::new(LiquidPrimitiveArray::<Int32Type>::from_arrow_array(
                array.as_primitive::<Int32Type>().clone(),
            )),
            DataType::Int64 => Arc::new(LiquidPrimitiveArray::<Int64Type>::from_arrow_array(
                array.as_primitive::<Int64Type>().clone(),
            )),
            DataType::UInt8 => Arc::new(LiquidPrimitiveArray::<UInt8Type>::from_arrow_array(
                array.as_primitive::<UInt8Type>().clone(),
            )),
            DataType::UInt16 => Arc::new(LiquidPrimitiveArray::<UInt16Type>::from_arrow_array(
                array.as_primitive::<UInt16Type>().clone(),
            )),
            DataType::UInt32 => Arc::new(LiquidPrimitiveArray::<UInt32Type>::from_arrow_array(
                array.as_primitive::<UInt32Type>().clone(),
            )),
            DataType::UInt64 => Arc::new(LiquidPrimitiveArray::<UInt64Type>::from_arrow_array(
                array.as_primitive::<UInt64Type>().clone(),
            )),
            DataType::Date32 => Arc::new(LiquidPrimitiveArray::<Date32Type>::from_arrow_array(
                array.as_primitive::<Date32Type>().clone(),
            )),
            DataType::Date64 => Arc::new(LiquidPrimitiveArray::<Date64Type>::from_arrow_array(
                array.as_primitive::<Date64Type>().clone(),
            )),
            DataType::Timestamp(TimeUnit::Second, None) => Arc::new(LiquidPrimitiveArray::<
                TimestampSecondType,
            >::from_arrow_array(
                array.as_primitive::<TimestampSecondType>().clone(),
            )),
            DataType::Timestamp(TimeUnit::Millisecond, None) => Arc::new(LiquidPrimitiveArray::<
                TimestampMillisecondType,
            >::from_arrow_array(
                array.as_primitive::<TimestampMillisecondType>().clone(),
            )),
            DataType::Timestamp(TimeUnit::Microsecond, None) => Arc::new(LiquidPrimitiveArray::<
                TimestampMicrosecondType,
            >::from_arrow_array(
                array.as_primitive::<TimestampMicrosecondType>().clone(),
            )),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => Arc::new(LiquidPrimitiveArray::<
                TimestampNanosecondType,
            >::from_arrow_array(
                array.as_primitive::<TimestampNanosecondType>().clone(),
            )),
            DataType::Timestamp(_, Some(_)) => {
                log::warn!("unsupported timestamp type with timezone {data_type:?}");
                return Err(array);
            }
            DataType::Float32 => Arc::new(LiquidFloatArray::<Float32Type>::from_arrow_array(
                array.as_primitive::<Float32Type>().clone(),
            )),
            DataType::Float64 => Arc::new(LiquidFloatArray::<Float64Type>::from_arrow_array(
                array.as_primitive::<Float64Type>().clone(),
            )),
            DataType::Decimal128(_, _) => {
                let decimals = array.as_primitive::<Decimal128Type>();
                if LiquidDecimalArray::fits_u64(decimals) {
                    return Ok(Arc::new(LiquidDecimalArray::from_decimal_array(decimals)));
                }
                log::debug!("decimal128 does not fit u64, not transcoding");
                return Err(array);
            }
            DataType::Decimal256(_, _) => {
                let decimals = array.as_primitive::<Decimal256Type>();
                if LiquidDecimalArray::fits_u64(decimals) {
                    return Ok(Arc::new(LiquidDecimalArray::from_decimal_array(decimals)));
                }
                log::debug!("decimal256 does not fit u64, not transcoding");
                return Err(array);
            }
            _ => {
                // For unsupported primitive types, leave the value unchanged.
                log::warn!("unsupported primitive type {data_type:?}");
                return Err(array);
            }
        };
        return Ok(liquid_array);
    }

    log::debug!("unsupported data type {:?}", array.data_type());
    Err(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
        TimestampMicrosecondArray,
    };

    const TEST_ARRAY_SIZE: usize = 8192;

    fn assert_transcode(original: &ArrayRef, transcoded: &LiquidArrayRef) {
        assert!(
            transcoded.get_array_memory_size() < original.get_array_memory_size(),
            "transcoded size: {}, original size: {}",
            transcoded.get_array_memory_size(),
            original.get_array_memory_size()
        );
        let back_to_arrow = transcoded.to_arrow_array();
        assert_eq!(original, &back_to_arrow);
    }

    #[test]
    fn test_transcode_int32() {
        let array: ArrayRef = Arc::new(Int32Array::from_iter_values(0..TEST_ARRAY_SIZE as i32));
        let transcoded = transcode_liquid_inner(&array).unwrap();
        assert_transcode(&array, &transcoded);
    }

    #[test]
    fn test_transcode_int64() {
        let array: ArrayRef = Arc::new(Int64Array::from_iter_values(0..TEST_ARRAY_SIZE as i64));
        let transcoded = transcode_liquid_inner(&array).unwrap();
        assert_transcode(&array, &transcoded);
    }

    #[test]
    fn test_transcode_float32() {
        let array: ArrayRef = Arc::new(Float32Array::from_iter_values(
            (0..TEST_ARRAY_SIZE).map(|i| i as f32),
        ));
        let transcoded = transcode_liquid_inner(&array).unwrap();
        assert_transcode(&array, &transcoded);
    }

    #[test]
    fn test_transcode_float64() {
        let array: ArrayRef = Arc::new(Float64Array::from_iter_values(
            (0..TEST_ARRAY_SIZE).map(|i| i as f64),
        ));

        let transcoded = transcode_liquid_inner(&array).unwrap();
        assert_transcode(&array, &transcoded);
    }

    #[test]
    fn test_transcode_timestamp_microsecond() {
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from_iter_values(
            (0..TEST_ARRAY_SIZE).map(|i| (i as i64) * 1_000),
        ));

        let transcoded = transcode_liquid_inner(&array).unwrap();
        assert_transcode(&array, &transcoded);
    }

    #[test]
    fn test_transcode_decimal128() {
        use arrow::array::Decimal128Builder;
        let mut builder = Decimal128Builder::new();
        for i in 0..TEST_ARRAY_SIZE {
            builder.append_value(i as i128 * 100);
        }
        let array: ArrayRef = Arc::new(builder.finish().with_precision_and_scale(20, 2).unwrap());

        let transcoded = transcode_liquid_inner(&array).unwrap();
        let back_to_arrow = transcoded.to_arrow_array();
        assert_eq!(&array, &back_to_arrow);
    }

    #[test]
    fn test_transcode_unsupported_type() {
        // Create a boolean array which is not supported by the transcoder
        let values: Vec<bool> = (0..TEST_ARRAY_SIZE).map(|i| i.is_multiple_of(2)).collect();
        let array: ArrayRef = Arc::new(BooleanArray::from(values));

        // Try to transcode and expect an error
        let result = transcode_liquid_inner(&array);

        // Verify it returns Err with the original array
        assert!(result.is_err());
        if let Err(original) = result {
            assert_eq!(&array, original);
        }
    }
}
