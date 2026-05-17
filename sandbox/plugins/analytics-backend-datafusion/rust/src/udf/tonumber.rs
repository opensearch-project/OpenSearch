/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`tonumber(string, base)`](https://docs.opensearch.org/latest/sql-and-ppl/ppl/functions/conversion/#tonumber)
//! base-N integer parse.
//!
//! # Semantics
//! * `base` must be in the inclusive range `[2, 36]`
//! * Output type is `Float64`

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Float64Builder};

use super::json_common::StringArrayView;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ToNumberUdf::new()));
}

/// `tonumber(varchar, int)` → fp64. Base-N string-to-integer parse widened to double
#[derive(Debug)]
pub struct ToNumberUdf {
    signature: Signature,
}

impl ToNumberUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8, DataType::Int32])],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ToNumberUdf {
    fn default() -> Self {
        Self::new()
    }
}

// `ScalarUDFImpl` requires `DynEq` + `DynHash`. All instances are functionally
// identical (no parameterization), so equality is trivial.
impl PartialEq for ToNumberUdf {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ToNumberUdf {}
impl Hash for ToNumberUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "tonumber".hash(state);
    }
}

/// How the resolved (constant) `base` argument behaves across the batch.
/// Resolved once up front so the hot loop doesn't repeat the scalar dispatch
/// or the range check on every row.
enum BaseMode {
    /// Base is `NULL` or outside `[2, 36]`. Every output row is NULL regardless
    /// of the value column — we skip reading it.
    AllNull,
    /// Base is valid. Carries the validated radix as `u32` (the type
    /// {@code i64::from_str_radix} wants), so the per-row code skips both the
    /// scalar dispatch and the range check.
    Valid(u32),
}

/// How the `value` argument is supplied
enum ValueSource<'a> {
    Scalar(Option<&'a str>),
    Array(StringArrayView<'a>),
}

impl<'a> ValueSource<'a> {
    /// Returns the string at row `i`, or `None`
    fn at(&self, i: usize) -> Option<&str> {
        match self {
            ValueSource::Scalar(s) => *s,
            ValueSource::Array(view) => view.cell(i),
        }
    }
}

impl ScalarUDFImpl for ToNumberUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "tonumber"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!(
                "tonumber expects exactly 2 arguments (string, base), got {}",
                args.args.len()
            );
        }

        let value_col = &args.args[0];
        let base_col = &args.args[1];

        // Full-scalar fast path — a plain literal `tonumber('FA34', 16)` plan.
        if let (
            ColumnarValue::Scalar(ScalarValue::Utf8(s)),
            ColumnarValue::Scalar(ScalarValue::Int32(b)),
        ) = (value_col, base_col)
        {
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                parse_with_base(s.as_deref(), *b),
            )));
        }

        let n = args.number_rows;
        let BaseMode::Valid(radix) = resolve_base(base_col)? else {
            let mut builder = Float64Builder::with_capacity(n);
            builder.append_nulls(n);
            return Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef));
        };

        // Materialize the value column. For scalars we keep the raw &str to
        // avoid a pointless n-wide StringArray allocation.
        let values_arr_ref: Option<ArrayRef> = match value_col {
            ColumnarValue::Array(_) => Some(value_col.clone().into_array(n)?),
            _ => None,
        };
        let values: ValueSource = match (&values_arr_ref, value_col) {
            (Some(arr), _) => ValueSource::Array(StringArrayView::from_array(arr)?),
            (None, ColumnarValue::Scalar(
                ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) | ScalarValue::Utf8View(opt),
            )) => ValueSource::Scalar(opt.as_deref()),
            (None, other) => {
                return exec_err!("tonumber: value expected Utf8, got {other:?}");
            }
        };

        let mut builder = Float64Builder::with_capacity(n);
        for i in 0..n {
            match values.at(i).and_then(|s| i64::from_str_radix(s, radix).ok()) {
                Some(v) => builder.append_value(v as f64),
                None => builder.append_null(),
            }
        }
        let out: Float64Array = builder.finish();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

/// Resolve the `base` argument
fn resolve_base(base_col: &ColumnarValue) -> Result<BaseMode> {
    match base_col {
        ColumnarValue::Scalar(ScalarValue::Int32(None)) => Ok(BaseMode::AllNull),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(b))) => Ok(match validate_base(*b) {
            Some(r) => BaseMode::Valid(r),
            None => BaseMode::AllNull,
        }),
        ColumnarValue::Scalar(other) => {
            exec_err!("tonumber: base expected Int32 literal, got {other:?}")
        }
        ColumnarValue::Array(arr) => {
            exec_err!(
                "tonumber: base must be a literal integer, got column of type {:?}",
                arr.data_type()
            )
        }
    }
}

/// Convert `base` to the radix expected by [`i64::from_str_radix`], or `None`
/// when the base is outside `[2, 36]` range.
fn validate_base(base: i32) -> Option<u32> {
    if (2..=36).contains(&base) {
        Some(base as u32)
    } else {
        None
    }
}

fn parse_with_base(s: Option<&str>, base: Option<i32>) -> Option<f64> {
    let s = s?;
    let radix = validate_base(base?)?;
    i64::from_str_radix(s, radix).ok().map(|v| v as f64)
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, AsArray, Int32Array, StringArray};
    use datafusion::arrow::datatypes::Field;

    fn invoke_scalar(value: Option<&str>, base: Option<i32>) -> Option<f64> {
        let u = ToNumberUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(value.map(|s| s.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Int32(base)),
            ],
            arg_fields: vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("b", DataType::Int32, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new(u.name(), DataType::Float64, true)),
            config_options: Arc::new(Default::default()),
        };
        match u.invoke_with_args(args).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Float64(opt)) => opt,
            other => panic!("expected Float64 scalar, got {other:?}"),
        }
    }

    #[test]
    fn base10_integer_doc_example() {
        assert_eq!(invoke_scalar(Some("4598"), Some(10)), Some(4598.0));
    }

    #[test]
    fn binary_doc_example() {
        assert_eq!(invoke_scalar(Some("010101"), Some(2)), Some(21.0));
    }

    #[test]
    fn hex_doc_example() {
        assert_eq!(invoke_scalar(Some("FA34"), Some(16)), Some(64052.0));
        assert_eq!(invoke_scalar(Some("fa34"), Some(16)), Some(64052.0));
    }

    #[test]
    fn signed_inputs_parse() {
        assert_eq!(invoke_scalar(Some("-21"), Some(10)), Some(-21.0));
        assert_eq!(invoke_scalar(Some("+FA"), Some(16)), Some(250.0));
        assert_eq!(invoke_scalar(Some("-10"), Some(2)), Some(-2.0));
    }

    #[test]
    fn base_boundary_values() {
        assert_eq!(invoke_scalar(Some("1"), Some(2)), Some(1.0));
        assert_eq!(invoke_scalar(Some("Z"), Some(36)), Some(35.0));
    }

    #[test]
    fn out_of_range_base_returns_null() {
        assert!(invoke_scalar(Some("10"), Some(0)).is_none());
        assert!(invoke_scalar(Some("10"), Some(1)).is_none());
        assert!(invoke_scalar(Some("10"), Some(37)).is_none());
        assert!(invoke_scalar(Some("10"), Some(-5)).is_none());
    }

    #[test]
    fn unparseable_string_returns_null() {
        assert!(invoke_scalar(Some("FA34"), Some(10)).is_none());
        assert!(invoke_scalar(Some("12"), Some(2)).is_none());
        assert!(invoke_scalar(Some(""), Some(10)).is_none());
        assert!(invoke_scalar(Some("1 2"), Some(10)).is_none());
        assert!(invoke_scalar(Some("3.14"), Some(10)).is_none());
    }

    #[test]
    fn overflow_returns_null() {
        assert!(invoke_scalar(Some("9223372036854775808"), Some(10)).is_none());
    }

    #[test]
    fn null_inputs_return_null() {
        assert!(invoke_scalar(None, Some(10)).is_none());
        assert!(invoke_scalar(Some("10"), None).is_none());
        assert!(invoke_scalar(None, None).is_none());
    }

    #[test]
    fn array_values_with_scalar_base_takes_fast_path() {
        let u = ToNumberUdf::new();
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("FA34"),
            Some("nope"),
            None,
            Some("ff"),
        ]));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(values),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(16))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("b", DataType::Int32, true)),
            ],
            number_rows: 4,
            return_field: Arc::new(Field::new("out", DataType::Float64, true)),
            config_options: Arc::new(Default::default()),
        };
        let arr = match u.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a,
            other => panic!("expected array, got {other:?}"),
        };
        let f = arr.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
        assert_eq!(f.value(0), 64052.0);
        assert!(f.is_null(1), "unparseable → NULL");
        assert!(f.is_null(2), "null input → NULL");
        assert_eq!(f.value(3), 255.0);
    }

    #[test]
    fn scalar_null_base_produces_all_null_output() {
        let u = ToNumberUdf::new();
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("FA34"),
            Some("nope"),
            Some("10"),
        ]));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(values),
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ],
            arg_fields: vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("b", DataType::Int32, true)),
            ],
            number_rows: 3,
            return_field: Arc::new(Field::new("out", DataType::Float64, true)),
            config_options: Arc::new(Default::default()),
        };
        let arr = match u.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a,
            other => panic!("expected array, got {other:?}"),
        };
        let f = arr.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
        assert_eq!(f.len(), 3);
        for i in 0..3 {
            assert!(f.is_null(i), "row {i} must be NULL");
        }
    }

    #[test]
    fn scalar_out_of_range_base_produces_all_null_output() {
        let u = ToNumberUdf::new();
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("FA34"), Some("10")]));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(values),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(42))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("b", DataType::Int32, true)),
            ],
            number_rows: 2,
            return_field: Arc::new(Field::new("out", DataType::Float64, true)),
            config_options: Arc::new(Default::default()),
        };
        let arr = match u.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a,
            other => panic!("expected array, got {other:?}"),
        };
        let f = arr.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
        assert!(f.is_null(0));
        assert!(f.is_null(1));
    }
}
