use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use super::PrimitiveKind;
use super::{LiquidArray, LiquidDataType, LiquidPrimitiveType};
use crate::cache::LiquidExpr;
use crate::liquid_array::LiquidPrimitiveArray;
use crate::liquid_array::eval_predicate_on_array;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, PrimitiveArray,
    cast::AsArray,
    types::{
        Date32Type, Date64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow::buffer::{BooleanBuffer, ScalarBuffer};
use arrow::compute::kernels::filter;
use arrow_schema::DataType;
use num_traits::{AsPrimitive, Bounded, FromPrimitive};

/// A linear-model based integer array, **only use it when you know the array is monotonic** and **you don't care about encoding speed!**.
///
/// Under the hood, it uses a linear model to predict the values and store the residuals:
/// value\[i\] = intercept + round(slope * i) + residual\[i\]
///
/// Where `intercept` and `slope` are computed using a L-infinity linear fit, **this is time-consuming!**.
///
/// This array is only recommended if you know the array follows a linear model, e.g., kinetic values, offsets, etc.
///
/// Examples of not recommended use cases: random values like ids, categorical values, etc.
#[derive(Debug)]
pub struct LiquidLinearArray<T: LiquidPrimitiveType>
where
    T::Native: AsPrimitive<f64> + FromPrimitive + Bounded,
{
    // Signed residuals, bit-packed as a Liquid primitive array of i64.
    residuals: LiquidPrimitiveArray<Int64Type>,
    // Intercept term stored as f64 for simpler math/IO.
    intercept: f64,
    // Slope term of the linear model.
    slope: f64,
    // Keep the logical type parameter.
    _phantom: PhantomData<T>,
}

/// Backward-compatible alias for i32.
pub type LiquidLinearI32Array = LiquidLinearArray<Int32Type>;
/// Linear-model array for `i8`.
pub type LiquidLinearI8Array = LiquidLinearArray<Int8Type>;
/// Linear-model array for `i16`.
pub type LiquidLinearI16Array = LiquidLinearArray<Int16Type>;
/// Linear-model array for `i64`.
pub type LiquidLinearI64Array = LiquidLinearArray<Int64Type>;
/// Linear-model array for `u8`.
pub type LiquidLinearU8Array = LiquidLinearArray<UInt8Type>;
/// Linear-model array for `u16`.
pub type LiquidLinearU16Array = LiquidLinearArray<UInt16Type>;
/// Linear-model array for `u32`.
pub type LiquidLinearU32Array = LiquidLinearArray<UInt32Type>;
/// Linear-model array for `u64`.
pub type LiquidLinearU64Array = LiquidLinearArray<UInt64Type>;
/// Linear-model array for `Date32` (days since epoch).
pub type LiquidLinearDate32Array = LiquidLinearArray<Date32Type>;
/// Linear-model array for `Date64` (ms since epoch).
pub type LiquidLinearDate64Array = LiquidLinearArray<Date64Type>;

impl<T> LiquidLinearArray<T>
where
    T: LiquidPrimitiveType,
    T::Native: AsPrimitive<f64> + FromPrimitive + Bounded,
{
    /// Build from an Arrow `PrimitiveArray<T>` by training a linear model
    /// using a fast L-infinity fit (Option 3) and storing residuals.
    pub fn from_arrow_array(arrow_array: PrimitiveArray<T>) -> Self {
        let len = arrow_array.len();

        // All nulls
        if arrow_array.null_count() == len {
            // All nulls
            let res = PrimitiveArray::<Int64Type>::new_null(len);
            return Self {
                residuals: LiquidPrimitiveArray::<Int64Type>::from_arrow_array(res),
                intercept: 0.0, // arbitrary, unused since all nulls
                slope: 0.0,
                _phantom: PhantomData,
            };
        }

        // Prepare compact non-null buffers for fast fitting (avoid iterator Option cost).
        let (nn_values, nn_indices) = collect_non_null_f64_and_indices::<T>(&arrow_array);

        // Option 3 parameters: L-infinity (Chebyshev) regression.
        let (mut intercept, mut slope) = fit_linf(&nn_values, &nn_indices);

        // Compute residuals in one unified loop; also track ranges for fallback decision.
        let mut residuals: Vec<i64> = Vec::with_capacity(len);
        let is_unsigned = <T as PrimitiveKind>::IS_UNSIGNED;
        let vals = arrow_array.values();
        let nulls_opt = arrow_array.nulls();

        // Original value range
        let mut orig_min_u64 = u64::MAX;
        let mut orig_max_u64 = 0u64;
        let mut orig_min_i64 = i64::MAX;
        let mut orig_max_i64 = i64::MIN;
        // Residual range
        let mut res_min = i64::MAX;
        let mut res_max = i64::MIN;

        if is_unsigned {
            let max_u64: u64 = <T as PrimitiveKind>::MAX_U64;
            for i in 0..len {
                let valid = nulls_opt.as_ref().is_none_or(|n| n.is_valid(i));
                if valid {
                    type U<TT> =
                        <<TT as LiquidPrimitiveType>::UnSignedType as ArrowPrimitiveType>::Native;
                    let v_u: U<T> = vals[i].as_();
                    let v_u64: u64 = v_u.as_();
                    if v_u64 < orig_min_u64 {
                        orig_min_u64 = v_u64;
                    }
                    if v_u64 > orig_max_u64 {
                        orig_max_u64 = v_u64;
                    }
                    let pr = slope * (i as f64) + intercept;
                    let p = predict_u64_saturated(pr, max_u64);
                    let (pos, mag) = if v_u64 >= p {
                        (true, v_u64 - p)
                    } else {
                        (false, p - v_u64)
                    };
                    let m = (mag & (i64::MAX as u64)) as i64;
                    let r = if pos { m } else { -m };
                    if r < res_min {
                        res_min = r;
                    }
                    if r > res_max {
                        res_max = r;
                    }
                    residuals.push(r);
                } else {
                    residuals.push(0);
                }
            }
        } else {
            let (min_i64, max_i64): (i64, i64) =
                (<T as PrimitiveKind>::MIN_I64, <T as PrimitiveKind>::MAX_I64);
            for i in 0..len {
                let valid = nulls_opt.as_ref().is_none_or(|n| n.is_valid(i));
                if valid {
                    let v_i64: i64 = vals[i].as_();
                    if v_i64 < orig_min_i64 {
                        orig_min_i64 = v_i64;
                    }
                    if v_i64 > orig_max_i64 {
                        orig_max_i64 = v_i64;
                    }
                    let pr = slope * (i as f64) + intercept;
                    let p = predict_i64_saturated(pr, min_i64, max_i64);
                    let r = v_i64 - p;
                    if r < res_min {
                        res_min = r;
                    }
                    if r > res_max {
                        res_max = r;
                    }
                    residuals.push(r);
                } else {
                    residuals.push(0);
                }
            }
        }

        // Fallback: ensure residual range is strictly smaller than original range
        let res_width: u128 = (res_max as i128 - res_min as i128) as u128;
        let orig_width: u128 = if is_unsigned {
            (orig_max_u64 as u128).saturating_sub(orig_min_u64 as u128)
        } else {
            (orig_max_i64 as i128 - orig_min_i64 as i128) as u128
        };
        if res_width >= orig_width {
            // Rebuild residuals with zero model
            intercept = 0.0;
            slope = 0.0;
            residuals.clear();
            if is_unsigned {
                for i in 0..len {
                    let valid = nulls_opt.as_ref().is_none_or(|n| n.is_valid(i));
                    if valid {
                        type U<TT> = <<TT as LiquidPrimitiveType>::UnSignedType as ArrowPrimitiveType>::Native;
                        let v_u: U<T> = vals[i].as_();
                        let v_u64: u64 = v_u.as_();
                        let r = (v_u64 & (i64::MAX as u64)) as i64;
                        residuals.push(r);
                    } else {
                        residuals.push(0);
                    }
                }
            } else {
                for i in 0..len {
                    let valid = nulls_opt.as_ref().is_none_or(|n| n.is_valid(i));
                    if valid {
                        let v_i64: i64 = vals[i].as_();
                        residuals.push(v_i64);
                    } else {
                        residuals.push(0);
                    }
                }
            }
        }
        let residuals_buf: ScalarBuffer<i64> = ScalarBuffer::from(residuals);
        let nulls = arrow_array.nulls().cloned();
        let res_prim = PrimitiveArray::<Int64Type>::new(residuals_buf, nulls);
        let residuals = LiquidPrimitiveArray::<Int64Type>::from_arrow_array(res_prim);

        Self {
            residuals,
            intercept,
            slope,
            _phantom: PhantomData,
        }
    }

    fn len(&self) -> usize {
        self.residuals.len()
    }
}

impl<T> LiquidArray for LiquidLinearArray<T>
where
    T: LiquidPrimitiveType,
    T::Native: AsPrimitive<f64> + FromPrimitive + Bounded,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn original_arrow_data_type(&self) -> DataType {
        T::DATA_TYPE.clone()
    }

    fn get_array_memory_size(&self) -> usize {
        self.residuals.get_array_memory_size()
            + std::mem::size_of::<f64>() // intercept
            + std::mem::size_of::<f64>() // slope
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn to_arrow_array(&self) -> ArrayRef {
        let arr = self.residuals.to_arrow_array();
        let (_dt, residuals, nulls) = arr.as_primitive::<Int64Type>().clone().into_parts();

        // Reconstruct final values: predicted(i) +/- |residual_i|
        let mut final_values = Vec::<T::Native>::with_capacity(self.len());
        let is_unsigned = <T as PrimitiveKind>::IS_UNSIGNED;
        if is_unsigned {
            let max_u64: u64 = <T as PrimitiveKind>::MAX_U64;
            for (i, &e) in residuals.iter().enumerate() {
                let pr = self.slope * (i as f64) + self.intercept;
                let p = predict_u64_saturated(pr, max_u64);
                let mag = e.unsigned_abs();
                let sum = if e >= 0 {
                    p.saturating_add(mag)
                } else {
                    p.saturating_sub(mag)
                };
                final_values.push(T::Native::from_u64(sum).unwrap());
            }
        } else {
            let (min_i64, max_i64): (i64, i64) =
                (<T as PrimitiveKind>::MIN_I64, <T as PrimitiveKind>::MAX_I64);
            for (i, &e) in residuals.iter().enumerate() {
                let pr = self.slope * (i as f64) + self.intercept;
                let p = predict_i64_saturated(pr, min_i64, max_i64);
                let sum = p.saturating_add(e);
                final_values.push(T::Native::from_i64(sum).unwrap());
            }
        }

        let values_buf: ScalarBuffer<T::Native> = ScalarBuffer::from(final_values);
        Arc::new(PrimitiveArray::<T>::new(values_buf, nulls))
    }

    fn filter(&self, selection: &BooleanBuffer) -> ArrayRef {
        let arr = self.to_arrow_array();
        let selection = BooleanArray::new(selection.clone(), None);
        filter::filter(&arr, &selection).unwrap()
    }

    fn try_eval_predicate(&self, predicate: &LiquidExpr, filter: &BooleanBuffer) -> BooleanArray {
        let arr = self.filter(filter);
        eval_predicate_on_array(arr, predicate)
    }

    fn data_type(&self) -> LiquidDataType {
        LiquidDataType::LinearInteger
    }
}

#[inline]
fn predict_u64_saturated(pred: f64, max_u64: u64) -> u64 {
    if !pred.is_finite() || pred <= 0.0 {
        0
    } else if pred >= max_u64 as f64 {
        max_u64
    } else {
        pred.round() as u64
    }
}

#[inline]
fn predict_i64_saturated(pred: f64, min_i64: i64, max_i64: i64) -> i64 {
    if !pred.is_finite() {
        0
    } else if pred <= min_i64 as f64 {
        min_i64
    } else if pred >= max_i64 as f64 {
        max_i64
    } else {
        pred.round() as i64
    }
}

/// L-infinity linear fit for y\[i\] ≈ intercept + slope * i (before rounding),
/// minimizing the maximum absolute error over all non-null points.
///
/// Approach: minimize R(m) = max_i (y_i - m i) - min_i (y_i - m i), which is
/// convex in m. Given m, the best intercept is b = (max_i s_i + min_i s_i)/2
/// where s_i = y_i - m i. We find m by a few rounds of bisection using the
/// subgradient sign derived from the argmax/argmin indices. O(n) per iteration.
fn fit_linf(values: &[f64], idxs: &[u32]) -> (f64, f64) {
    let n = values.len();
    assert_eq!(values.len(), idxs.len());
    if n == 0 {
        return (0.0, 0.0);
    }
    if n == 1 {
        return (values[0], 0.0);
    }

    let mut slope_min = f64::INFINITY;
    let mut slope_max = f64::NEG_INFINITY;
    for k in 1..n {
        let di = (idxs[k] - idxs[k - 1]) as f64;
        if di > 0.0 {
            let dv = values[k] - values[k - 1];
            let s = dv / di;
            if s < slope_min {
                slope_min = s;
            }
            if s > slope_max {
                slope_max = s;
            }
        }
    }
    if !slope_min.is_finite() || !slope_max.is_finite() {
        slope_min = 0.0;
        slope_max = 0.0;
    }

    let mut lo = slope_min.min(slope_max);
    let mut hi = slope_min.max(slope_max);
    if (hi - lo).abs() < 1e-12 {
        let pad = if hi.abs() < 1.0 { 1.0 } else { hi.abs() * 1e-6 };
        lo -= pad;
        hi += pad;
    }

    #[inline]
    fn range_stats(values: &[f64], idxs: &[u32], m: f64) -> (f64, u32, f64, u32) {
        let mut min_s = f64::INFINITY;
        let mut max_s = f64::NEG_INFINITY;
        let mut i_min = 0u32;
        let mut i_max = 0u32;
        for k in 0..values.len() {
            let i = idxs[k] as f64;
            let s = values[k] - m * i;
            if s < min_s {
                min_s = s;
                i_min = idxs[k];
            }
            if s > max_s {
                max_s = s;
                i_max = idxs[k];
            }
        }
        (min_s, i_min, max_s, i_max)
    }

    const MAX_ITERS: usize = 8;
    for _ in 0..MAX_ITERS {
        let m = 0.5 * (lo + hi);
        let (_min_s, i_min, _max_s, i_max) = range_stats(values, idxs, m);
        let g = (i_min as i64) - (i_max as i64);
        if g > 0 {
            hi = m;
        } else if g < 0 {
            lo = m;
        } else {
            lo = m;
            hi = m;
            break;
        }
        if (hi - lo).abs() < 1e-12 {
            break;
        }
    }

    let m = 0.5 * (lo + hi);
    let (min_s, _i_min, max_s, _i_max) = range_stats(values, idxs, m);
    let b = 0.5 * (max_s + min_s);
    (b, m)
}

#[inline]
fn collect_non_null_f64_and_indices<T>(arr: &PrimitiveArray<T>) -> (Vec<f64>, Vec<u32>)
where
    T: LiquidPrimitiveType,
    T::Native: AsPrimitive<f64>,
{
    let nn = arr.len() - arr.null_count();
    let mut values = Vec::with_capacity(nn);
    let mut idxs = Vec::with_capacity(nn);
    let vals = arr.values();
    if arr.null_count() == 0 {
        for (i, v) in vals.iter().enumerate() {
            values.push(v.as_());
            idxs.push(i as u32);
        }
    } else {
        let nulls = arr.nulls().unwrap();
        for (i, v) in vals.iter().enumerate() {
            if nulls.is_valid(i) {
                values.push(v.as_());
                idxs.push(i as u32);
            }
        }
    }
    (values, idxs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_eq(values: Vec<Option<i32>>) {
        let arr = PrimitiveArray::<Int32Type>::from(values.clone());
        let linear = LiquidLinearI32Array::from_arrow_array(arr.clone());
        let decoded = linear.to_arrow_array();
        assert_eq!(decoded.as_ref(), &arr);
    }

    macro_rules! roundtrip_eq_t {
        ($T:ty, $values:expr) => {{
            let arr = PrimitiveArray::<$T>::from(($values).clone());
            let linear = LiquidLinearArray::<$T>::from_arrow_array(arr.clone());
            let decoded = linear.to_arrow_array();
            assert_eq!(decoded.as_ref(), &arr);
        }};
    }

    #[test]
    fn test_roundtrip_basic() {
        // Non-monotonic values to ensure we don't rely on simple increasing sequences
        roundtrip_eq(vec![
            Some(10),
            Some(15),
            Some(14),
            Some(20),
            Some(18),
            Some(25),
            Some(24),
        ]);
    }

    #[test]
    fn test_roundtrip_with_nulls() {
        roundtrip_eq(vec![Some(10), None, Some(30), None, Some(50), Some(70)]);
    }

    #[test]
    fn test_all_nulls() {
        roundtrip_eq(vec![None, None, None, None]);
    }

    #[test]
    fn test_single_value() {
        roundtrip_eq(vec![Some(42)]);
    }

    #[test]
    fn test_empty() {
        roundtrip_eq(vec![]);
    }

    #[test]
    fn test_negative_values() {
        roundtrip_eq(vec![
            Some(-100),
            Some(-50),
            Some(0),
            Some(50),
            Some(25),
            None,
            Some(-25),
        ]);
    }

    #[test]
    fn test_filter_basic() {
        let original: Vec<Option<i32>> = vec![Some(1), Some(2), Some(3), None, Some(5), Some(8)];
        let arr = PrimitiveArray::<Int32Type>::from(original.clone());
        let linear = LiquidLinearI32Array::from_arrow_array(arr);
        let selection = BooleanBuffer::from(vec![true, false, true, false, true, false]);
        let result = linear.filter(&selection);
        let expected = PrimitiveArray::<Int32Type>::from(vec![Some(1), Some(3), Some(5)]);
        assert_eq!(result.as_ref(), &expected);
    }

    #[test]
    fn test_original_arrow_data_type_returns_int32() {
        let arr = PrimitiveArray::<Int32Type>::from(vec![Some(1), Some(2)]);
        let linear = LiquidLinearI32Array::from_arrow_array(arr);
        assert_eq!(linear.original_arrow_data_type(), DataType::Int32);
    }

    #[test]
    fn test_roundtrip_i8() {
        roundtrip_eq_t!(Int8Type, vec![Some(-10), Some(0), Some(10), None, Some(20)]);
    }

    #[test]
    fn test_roundtrip_i16() {
        roundtrip_eq_t!(
            Int16Type,
            vec![Some(-1000), Some(0), Some(1000), None, Some(2000)]
        );
    }

    #[test]
    fn test_roundtrip_i64() {
        roundtrip_eq_t!(
            Int64Type,
            vec![
                Some(-10_000_000_000),
                Some(0),
                Some(10_000_000_000),
                None,
                Some(20_000_000_000),
            ]
        );
    }

    #[test]
    fn test_roundtrip_u8() {
        roundtrip_eq_t!(
            UInt8Type,
            vec![Some(0), Some(10), Some(200), None, Some(255)]
        );
    }

    #[test]
    fn test_roundtrip_u16() {
        roundtrip_eq_t!(
            UInt16Type,
            vec![Some(0), Some(1000), Some(60000), None, Some(500)]
        );
    }

    #[test]
    fn test_roundtrip_u32() {
        roundtrip_eq_t!(
            UInt32Type,
            vec![
                Some(0),
                Some(1_000_000),
                Some(3_000_000_000),
                None,
                Some(123_456_789),
            ]
        );
    }

    #[test]
    fn test_roundtrip_u64() {
        roundtrip_eq_t!(
            UInt64Type,
            vec![
                Some(0),
                Some(10_000_000_000),
                Some(9_000_000_000_000_000_000u64),
                None,
                Some(42),
            ]
        );
    }

    #[test]
    fn test_roundtrip_date32() {
        roundtrip_eq_t!(
            Date32Type,
            vec![Some(-365), Some(0), Some(365), None, Some(18262)]
        );
    }

    #[test]
    fn test_roundtrip_date64() {
        roundtrip_eq_t!(
            Date64Type,
            vec![
                Some(-86_400_000),
                Some(0),
                Some(86_400_000),
                None,
                Some(1_000_000_000_000),
            ]
        );
    }

    #[test]
    fn test_compression() {
        let original = (0..1_000_000).step_by(100).collect::<Vec<_>>();

        let original = PrimitiveArray::<Int32Type>::from_iter_values(original);
        let arrow_size = original.get_array_memory_size();

        let liquid_linear = LiquidLinearI32Array::from_arrow_array(original.clone());
        let liquid_linear_size = liquid_linear.get_array_memory_size();

        let liquid_primitive =
            LiquidPrimitiveArray::<Int32Type>::from_arrow_array(original.clone());
        let liquid_primitive_size = liquid_primitive.get_array_memory_size();

        println!(
            "arrow_size: {arrow_size}, liquid_linear_size: {liquid_linear_size}, liquid_primitive_size: {liquid_primitive_size}",
        );

        assert!(liquid_linear_size < arrow_size);
        assert!(liquid_primitive_size < arrow_size);
        assert!(liquid_linear_size < liquid_primitive_size);

        let original: ArrayRef = Arc::new(original);
        assert_eq!(original.as_ref(), liquid_linear.to_arrow_array().as_ref());
        assert_eq!(
            original.as_ref(),
            liquid_primitive.to_arrow_array().as_ref()
        );
    }
}
