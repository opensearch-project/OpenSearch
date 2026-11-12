use std::any::Any;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{Decimal128Type, Decimal256Type, DecimalType, UInt64Type, i256};
use arrow_schema::DataType;
use num_traits::ToPrimitive;

use super::{LiquidArray, LiquidDataType};
use crate::liquid_array::raw::BitPackedArray;
use crate::utils::get_bit_width;

#[derive(Debug, Clone, Copy)]
struct DecimalMeta {
    precision: u8,
    scale: i8,
    is_256: bool,
}

impl DecimalMeta {
    fn from_data_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::Decimal128(precision, scale) => Self {
                precision: *precision,
                scale: *scale,
                is_256: false,
            },
            DataType::Decimal256(precision, scale) => Self {
                precision: *precision,
                scale: *scale,
                is_256: true,
            },
            _ => panic!("unsupported decimal data type: {data_type:?}"),
        }
    }

    fn data_type(&self) -> DataType {
        if self.is_256 {
            DataType::Decimal256(self.precision, self.scale)
        } else {
            DataType::Decimal128(self.precision, self.scale)
        }
    }
}

/// Liquid decimal array stored as a compressed u64 primitive.
#[derive(Debug)]
pub struct LiquidDecimalArray {
    meta: DecimalMeta,
    bit_packed: BitPackedArray<UInt64Type>,
    reference_value: u64,
}

impl LiquidDecimalArray {
    pub(crate) fn fits_u64<T: DecimalType>(array: &PrimitiveArray<T>) -> bool
    where
        T::Native: ToPrimitive,
    {
        array.iter().flatten().all(|v| v.to_u64().is_some())
    }

    pub(crate) fn from_decimal_array<T: DecimalType>(array: &PrimitiveArray<T>) -> Self
    where
        T::Native: ToPrimitive,
    {
        debug_assert!(Self::fits_u64(array));
        let meta = DecimalMeta::from_data_type(array.data_type());
        if array.null_count() == array.len() {
            return Self {
                meta,
                bit_packed: BitPackedArray::new_null_array(array.len()),
                reference_value: 0,
            };
        }

        let nulls = array.nulls().cloned();
        let mut min = u64::MAX;
        let mut max = 0u64;
        let values: Vec<u64> = array
            .iter()
            .map(|v| match v {
                Some(v) => {
                    let value = v.to_u64().expect("decimal fits u64");
                    if value < min {
                        min = value;
                    }
                    if value > max {
                        max = value;
                    }
                    value
                }
                None => 0,
            })
            .collect();

        let bit_width = get_bit_width(max - min);
        let offsets = ScalarBuffer::from_iter(values.iter().map(|v| v.saturating_sub(min)));
        let unsigned_array = PrimitiveArray::<UInt64Type>::new(offsets, nulls);
        let bit_packed = BitPackedArray::from_primitive(unsigned_array, bit_width);

        Self {
            meta,
            bit_packed,
            reference_value: min,
        }
    }

    fn to_u64_array(&self) -> PrimitiveArray<UInt64Type> {
        let unsigned_array = self.bit_packed.to_primitive();
        let (_data_type, values, _nulls) = unsigned_array.into_parts();
        let nulls = self.bit_packed.nulls();
        let values = if self.reference_value != 0 {
            let reference_value = self.reference_value;
            ScalarBuffer::from_iter(values.iter().map(|v| v.wrapping_add(reference_value)))
        } else {
            values
        };
        PrimitiveArray::<UInt64Type>::new(values, nulls.cloned())
    }
}

impl LiquidArray for LiquidDecimalArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_array_memory_size(&self) -> usize {
        self.bit_packed.get_array_memory_size() + size_of::<u64>() + size_of::<DecimalMeta>()
    }

    fn len(&self) -> usize {
        self.bit_packed.len()
    }

    fn to_arrow_array(&self) -> ArrayRef {
        let u64_array = self.to_u64_array();
        let (_data_type, values, nulls) = u64_array.into_parts();
        let data_type = self.meta.data_type();
        if self.meta.is_256 {
            let values_i256 =
                ScalarBuffer::from_iter(values.iter().map(|v| i256::from_i128(*v as i128)));
            let array = PrimitiveArray::<Decimal256Type>::new(values_i256, nulls);
            Arc::new(array.with_data_type(data_type))
        } else {
            let values_i128 = ScalarBuffer::from_iter(values.iter().map(|v| *v as i128));
            let array = PrimitiveArray::<Decimal128Type>::new(values_i128, nulls);
            Arc::new(array.with_data_type(data_type))
        }
    }

    fn original_arrow_data_type(&self) -> DataType {
        self.meta.data_type()
    }

    fn data_type(&self) -> LiquidDataType {
        LiquidDataType::Decimal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::LiquidExpr;
    use arrow::array::{BooleanArray, Decimal128Builder};
    use arrow::buffer::BooleanBuffer;
    use datafusion_common::ScalarValue;
    use datafusion_expr_common::operator::Operator as DFOperator;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    use std::sync::Arc;

    #[test]
    fn decimal_u64_roundtrip() {
        let mut builder = Decimal128Builder::new();
        builder.append_value(100_i128);
        builder.append_null();
        builder.append_value(250_i128);
        let original = builder.finish().with_precision_and_scale(10, 2).unwrap();

        let liquid = LiquidDecimalArray::from_decimal_array(&original);
        let arrow = liquid.to_arrow_array();
        assert_eq!(arrow.as_ref(), &original);
    }

    #[test]
    fn decimal_predicate_eval() {
        let mut builder = Decimal128Builder::new();
        builder.append_value(100_i128);
        builder.append_value(200_i128);
        builder.append_null();
        builder.append_value(300_i128);
        let original = builder.finish().with_precision_and_scale(10, 2).unwrap();

        let liquid = LiquidDecimalArray::from_decimal_array(&original);

        let mask = BooleanBuffer::new_set(original.len());
        let lit = Arc::new(Literal::new(ScalarValue::Decimal128(Some(150_i128), 10, 2)));
        let col = Arc::new(Column::new("col", 0));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(col, DFOperator::GtEq, lit));

        let got = liquid.try_eval_predicate(&LiquidExpr::new_unchecked(expr), &mask);
        let expected = BooleanArray::from(vec![Some(false), Some(true), None, Some(true)]);
        assert_eq!(got, expected);
    }
}
