//! LiquidArray is the core data structure of LiquidCache.
//! You should not use this module directly.
//! Instead, use `liquid_cache_datafusion_server` or `liquid_cache_datafusion_client` to interact with LiquidCache.
mod decimal_array;
mod float_array;
mod linear_integer_array;
mod primitive_array;
pub mod raw;

use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanArray, cast::AsArray},
    buffer::BooleanBuffer,
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
pub use decimal_array::LiquidDecimalArray;
pub use float_array::{LiquidFloat32Array, LiquidFloat64Array, LiquidFloatArray};
pub use linear_integer_array::{
    LiquidLinearArray, LiquidLinearDate32Array, LiquidLinearDate64Array, LiquidLinearI8Array,
    LiquidLinearI16Array, LiquidLinearI32Array, LiquidLinearI64Array, LiquidLinearU8Array,
    LiquidLinearU16Array, LiquidLinearU32Array, LiquidLinearU64Array,
};
pub use primitive_array::{
    LiquidDate32Array, LiquidDate64Array, LiquidI8Array, LiquidI16Array, LiquidI32Array,
    LiquidI64Array, LiquidPrimitiveArray, LiquidPrimitiveDeltaArray, LiquidPrimitiveType,
    LiquidU8Array, LiquidU16Array, LiquidU32Array, LiquidU64Array,
};

use crate::cache::LiquidExpr;

/// Liquid data type is only logical type
#[derive(Debug, Clone, Copy)]
#[repr(u16)]
pub enum LiquidDataType {
    /// An integer.
    Integer = 1,
    /// A float.
    Float = 2,
    /// A linear-model based integer (signed residuals + model params).
    LinearInteger = 5,
    /// A decimal encoded as a primitive u64 array.
    Decimal = 6,
}

/// A Liquid array.
pub trait LiquidArray: std::fmt::Debug + Send + Sync {
    /// Get the underlying any type.
    fn as_any(&self) -> &dyn Any;

    /// Get the memory size of the Liquid array.
    fn get_array_memory_size(&self) -> usize;

    /// Get the length of the Liquid array.
    fn len(&self) -> usize;

    /// Check if the Liquid array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert the Liquid array to an Arrow array.
    fn to_arrow_array(&self) -> ArrayRef;

    /// Convert the Liquid array to an Arrow array.
    /// Except that it will pick the best encoding for the arrow array.
    /// Meaning that it may not obey the data type of the original arrow array.
    fn to_best_arrow_array(&self) -> ArrayRef {
        self.to_arrow_array()
    }

    /// Get the logical data type of the Liquid array.
    fn data_type(&self) -> LiquidDataType;

    /// Get the original arrow data type of the Liquid array.
    fn original_arrow_data_type(&self) -> DataType;

    /// Filter the Liquid array with a boolean array and return an **arrow array**.
    fn filter(&self, selection: &BooleanBuffer) -> ArrayRef {
        let arrow_array = self.to_arrow_array();
        let selection = BooleanArray::new(selection.clone(), None);
        arrow::compute::kernels::filter::filter(&arrow_array, &selection).unwrap()
    }

    /// Evaluate a predicate on the Liquid array with a filter.
    ///
    /// Note that the filter is a boolean buffer, not a boolean array, i.e., filter can't be nullable.
    /// The returned boolean mask is nullable if the the original array is nullable.
    fn try_eval_predicate(&self, predicate: &LiquidExpr, filter: &BooleanBuffer) -> BooleanArray {
        let filtered = self.filter(filter);
        eval_predicate_on_array(filtered, predicate)
    }
}

/// A reference to a Liquid array.
pub type LiquidArrayRef = Arc<dyn LiquidArray>;

pub(crate) fn eval_predicate_on_array(array: ArrayRef, predicate: &LiquidExpr) -> BooleanArray {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "liquid_predicate_col",
        array.data_type().clone(),
        true,
    )]));
    let record_batch = RecordBatch::try_new(schema, vec![array]).expect("predicate input batch");
    let result = predicate
        .physical_expr()
        .evaluate(&record_batch)
        .expect("validated LiquidExpr must evaluate");
    let boolean_array = result
        .into_array(record_batch.num_rows())
        .expect("predicate output must be an array");
    boolean_array.as_boolean().clone()
}

/// Compile-time info about primitive kind (signed vs unsigned) and bounds.
/// Implemented for all Liquid-supported primitive integer and date types.
pub trait PrimitiveKind {
    /// Whether the logical type is unsigned (true for u8/u16/u32/u64).
    const IS_UNSIGNED: bool;
    /// Maximum representable value as u64 for unsigned types (unused for signed).
    const MAX_U64: u64;
    /// Minimum representable value as i64 for signed/date types (unused for unsigned).
    const MIN_I64: i64;
    /// Maximum representable value as i64 for signed/date types (unused for unsigned).
    const MAX_I64: i64;
}

macro_rules! impl_unsigned_kind {
    ($t:ty, $max:expr) => {
        impl PrimitiveKind for $t {
            const IS_UNSIGNED: bool = true;
            const MAX_U64: u64 = $max as u64;
            const MIN_I64: i64 = 0; // unused
            const MAX_I64: i64 = 0; // unused
        }
    };
}

macro_rules! impl_signed_kind {
    ($t:ty, $min:expr, $max:expr) => {
        impl PrimitiveKind for $t {
            const IS_UNSIGNED: bool = false;
            const MAX_U64: u64 = 0; // unused
            const MIN_I64: i64 = $min as i64;
            const MAX_I64: i64 = $max as i64;
        }
    };
}

use arrow::datatypes::{
    Date32Type, Date64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};

impl_unsigned_kind!(UInt8Type, u8::MAX);
impl_unsigned_kind!(UInt16Type, u16::MAX);
impl_unsigned_kind!(UInt32Type, u32::MAX);
impl_unsigned_kind!(UInt64Type, u64::MAX);

impl_signed_kind!(Int8Type, i8::MIN, i8::MAX);
impl_signed_kind!(Int16Type, i16::MIN, i16::MAX);
impl_signed_kind!(Int32Type, i32::MIN, i32::MAX);
impl_signed_kind!(Int64Type, i64::MIN, i64::MAX);

// Dates are logically signed in Arrow (Date32: i32 days, Date64: i64 ms)
impl_signed_kind!(Date32Type, i32::MIN, i32::MAX);
impl_signed_kind!(Date64Type, i64::MIN, i64::MAX);
impl_signed_kind!(TimestampSecondType, i64::MIN, i64::MAX);
impl_signed_kind!(TimestampMillisecondType, i64::MIN, i64::MAX);
impl_signed_kind!(TimestampMicrosecondType, i64::MIN, i64::MAX);
impl_signed_kind!(TimestampNanosecondType, i64::MIN, i64::MAX);
