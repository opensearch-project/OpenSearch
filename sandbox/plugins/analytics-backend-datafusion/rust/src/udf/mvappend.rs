/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `mvappend(arg1, arg2, …)` — flatten a mixed list of array and scalar args
//! into a single array, dropping null args AND null elements inside array args.
//!
//! Mirrors PPL's [`MVAppendFunctionImpl`] / [`MVAppendCore`] semantics:
//!
//! * For each argument, in order:
//!   * NULL argument → skipped entirely.
//!   * Array argument → each non-null element is appended to the output.
//!   * Scalar argument → appended as a single element.
//! * Returns NULL if no non-null elements were collected (PPL convention —
//!   distinguishes `mvappend(null)` from `mvappend()` from `mvappend([])`).
//!
//! ## Type homogeneity
//!
//! The Java adapter (`MvappendAdapter`) casts every scalar argument to the
//! call's array component type and every array argument to
//! `ARRAY<componentType>` before this UDF runs, so by the time we see operands
//! they share a single element type. The element-conversion macro below
//! handles each supported scalar Arrow type explicitly; a list whose data
//! vector type isn't covered surfaces as a planning error rather than a
//! silent coercion.
//!
//! Mixed-type calls (`mvappend(1, 'text', 2.5)`) end up with Calcite type
//! `ARRAY<ANY>` which substrait doesn't have an encoding for — those fail at
//! substrait conversion, before reaching this UDF, and aren't addressed here.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, Float32Array, Float32Builder,
    Float64Array, Float64Builder, GenericListArray, Int16Array, Int16Builder, Int32Array,
    Int32Builder, Int64Array, Int64Builder, Int8Array, Int8Builder, ListArray, ListBuilder,
    StringArray, StringBuilder, StringViewArray, StringViewBuilder, UInt16Array, UInt16Builder,
    UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, UInt8Array, UInt8Builder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(MvappendUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MvappendUdf {
    signature: Signature,
}

impl MvappendUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for MvappendUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for MvappendUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mvappend"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return plan_err!("mvappend expects at least 1 argument, got 0");
        }
        // Java adapter pre-coerces every operand to either ARRAY<E> or E for a
        // single E. Use whichever element type we see first.
        let element_type = element_type(arg_types)
            .ok_or_else(|| DataFusionError::Plan("mvappend: unable to determine element type from operand types".to_string()))?;
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            element_type,
            true,
        ))))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return plan_err!("mvappend expects at least 1 argument, got 0");
        }
        // Trust the Java adapter to have already coerced operands. coerce_types here
        // exists only because Signature::user_defined demands an implementation; we
        // pass each type through unchanged.
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let element_type = element_type(&args.arg_fields.iter().map(|f| f.data_type().clone()).collect::<Vec<_>>())
            .ok_or_else(|| DataFusionError::Internal("mvappend: lost element type at invoke".to_string()))?;

        // Materialize each operand as an ArrayRef whose Arrow type is either
        // {element_type} or List<element_type>. Scalar operands replicate to n rows.
        let operand_arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|a| a.clone().into_array(n))
            .collect::<Result<Vec<_>>>()?;

        macro_rules! build {
            ($Builder:ty, $Scalar:ty, $List:ty) => {{
                let inner = <$Builder>::new();
                let mut builder = ListBuilder::new(inner);
                for row in 0..n {
                    let mut any_value = false;
                    for arr in &operand_arrays {
                        if arr.is_null(row) {
                            continue;
                        }
                        if let Some(list_arr) = arr.as_any().downcast_ref::<GenericListArray<i32>>() {
                            // Iterate elements of the list at this row.
                            let row_list = list_arr.value(row);
                            let typed = row_list
                                .as_any()
                                .downcast_ref::<$Scalar>()
                                .ok_or_else(|| DataFusionError::Internal(format!(
                                    "mvappend: list element vector type mismatch ({:?})",
                                    row_list.data_type()
                                )))?;
                            for i in 0..typed.len() {
                                if !typed.is_null(i) {
                                    builder.values().append_value(typed.value(i));
                                    any_value = true;
                                }
                            }
                        } else if let Some(typed) = arr.as_any().downcast_ref::<$Scalar>() {
                            builder.values().append_value(typed.value(row));
                            any_value = true;
                        } else {
                            return plan_err!(
                                "mvappend: unexpected operand vector type {:?}",
                                arr.data_type()
                            );
                        }
                    }
                    if any_value {
                        builder.append(true);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }};
        }

        let result: ArrayRef = match &element_type {
            DataType::Int8 => build!(Int8Builder, Int8Array, ListArray),
            DataType::Int16 => build!(Int16Builder, Int16Array, ListArray),
            DataType::Int32 => build!(Int32Builder, Int32Array, ListArray),
            DataType::Int64 => build!(Int64Builder, Int64Array, ListArray),
            DataType::UInt8 => build!(UInt8Builder, UInt8Array, ListArray),
            DataType::UInt16 => build!(UInt16Builder, UInt16Array, ListArray),
            DataType::UInt32 => build!(UInt32Builder, UInt32Array, ListArray),
            DataType::UInt64 => build!(UInt64Builder, UInt64Array, ListArray),
            DataType::Float32 => build!(Float32Builder, Float32Array, ListArray),
            DataType::Float64 => build!(Float64Builder, Float64Array, ListArray),
            DataType::Boolean => build!(BooleanBuilder, BooleanArray, ListArray),
            // String element types — handled specially because list children may be any of
            // {Utf8, LargeUtf8, Utf8View} depending on whether the operand is a string literal,
            // a field read (DataFusion's substrait consumer uses Utf8View for column reads in
            // 52+), or a computed expression. The output type must match what {@code return_type}
            // declared (which is whatever {@code element_type()} returned, driven by the first
            // operand) — DataFusion validates the actual output schema against the declared
            // schema and rejects mismatches with "column types must match schema types".
            DataType::Utf8 | DataType::LargeUtf8 => {
                let mut builder = ListBuilder::new(StringBuilder::new());
                build_string_rows::<StringBuilder>(&operand_arrays, n, &mut builder)?;
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Utf8View => {
                let mut builder = ListBuilder::new(StringViewBuilder::new());
                build_string_rows::<StringViewBuilder>(&operand_arrays, n, &mut builder)?;
                Arc::new(builder.finish()) as ArrayRef
            }
            other => {
                return plan_err!("mvappend: unsupported element type {other:?}");
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}

/// First operand type drives the element type. The Java adapter has already
/// normalized everything to that single element type — either bare scalar or
/// `List<element>`. String element types are normalized to {@code Utf8} so the
/// match arm in {@link MvappendUdf::invoke_with_args} catches every flavor —
/// scalar literals come through as {@code Utf8}, field reads as {@code Utf8View},
/// computed expressions as {@code Utf8} or {@code LargeUtf8}.
fn element_type(arg_types: &[DataType]) -> Option<DataType> {
    arg_types.iter().find_map(|t| match t {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.data_type().clone())
        }
        DataType::Null => None,
        other => Some(other.clone()),
    })
}

/// Trait abstracting the difference between {@link StringBuilder} and
/// {@link StringViewBuilder} when appending {@code &str} values. Both expose
/// `append_value(&str)`, but they're concrete types with no shared trait, so
/// this glue lets {@link build_string_rows} drive either via the same code.
trait StrAppend {
    fn append_str(&mut self, s: &str);
}
impl StrAppend for StringBuilder {
    fn append_str(&mut self, s: &str) {
        self.append_value(s);
    }
}
impl StrAppend for StringViewBuilder {
    fn append_str(&mut self, s: &str) {
        self.append_value(s);
    }
}

/// Generic per-row writer for string-typed mvappend output. Dispatches list-child
/// downcasts across all three Utf8 flavors so the input doesn't need to match the
/// output type.
fn build_string_rows<B: StrAppend + datafusion::arrow::array::ArrayBuilder>(
    operand_arrays: &[ArrayRef],
    n: usize,
    builder: &mut ListBuilder<B>,
) -> Result<()> {
    for row in 0..n {
        let mut any_value = false;
        for arr in operand_arrays {
            if arr.is_null(row) {
                continue;
            }
            if let Some(list_arr) = arr.as_any().downcast_ref::<GenericListArray<i32>>() {
                let row_list = list_arr.value(row);
                append_string_elements(row_list.as_ref(), builder.values(), &mut any_value)?;
            } else {
                append_string_scalar(arr.as_ref(), row, builder.values(), &mut any_value)?;
            }
        }
        if any_value {
            builder.append(true);
        } else {
            builder.append_null();
        }
    }
    Ok(())
}

/// Append all non-null string elements from a row's list to the output builder.
/// Handles list children typed as Utf8, LargeUtf8, or Utf8View.
fn append_string_elements<B: StrAppend>(
    row_list: &dyn Array,
    out: &mut B,
    any_value: &mut bool,
) -> Result<()> {
    if let Some(typed) = row_list.as_any().downcast_ref::<StringArray>() {
        for i in 0..typed.len() {
            if !typed.is_null(i) {
                out.append_str(typed.value(i));
                *any_value = true;
            }
        }
        return Ok(());
    }
    if let Some(typed) = row_list.as_any().downcast_ref::<StringViewArray>() {
        for i in 0..typed.len() {
            if !typed.is_null(i) {
                out.append_str(typed.value(i));
                *any_value = true;
            }
        }
        return Ok(());
    }
    if let Some(large) = row_list.as_string_opt::<i64>() {
        for i in 0..large.len() {
            if !large.is_null(i) {
                out.append_str(large.value(i));
                *any_value = true;
            }
        }
        return Ok(());
    }
    plan_err!(
        "mvappend: list element vector type mismatch — expected string, got {:?}",
        row_list.data_type()
    )
}

/// Append a single scalar string operand at the given row to the output builder.
/// Handles operands typed as Utf8, LargeUtf8, or Utf8View.
fn append_string_scalar<B: StrAppend>(
    arr: &dyn Array,
    row: usize,
    out: &mut B,
    any_value: &mut bool,
) -> Result<()> {
    if let Some(typed) = arr.as_any().downcast_ref::<StringArray>() {
        out.append_str(typed.value(row));
        *any_value = true;
        return Ok(());
    }
    if let Some(typed) = arr.as_any().downcast_ref::<StringViewArray>() {
        out.append_str(typed.value(row));
        *any_value = true;
        return Ok(());
    }
    if let Some(large) = arr.as_string_opt::<i64>() {
        out.append_str(large.value(row));
        *any_value = true;
        return Ok(());
    }
    plan_err!(
        "mvappend: scalar operand vector type mismatch — expected string, got {:?}",
        arr.data_type()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;
    use datafusion::common::ScalarValue;

    fn run(args: Vec<ColumnarValue>, n: usize) -> ArrayRef {
        let arg_fields: Vec<Arc<Field>> = args
            .iter()
            .enumerate()
            .map(|(i, cv)| {
                let dt = match cv {
                    ColumnarValue::Array(a) => a.data_type().clone(),
                    ColumnarValue::Scalar(sv) => sv.data_type(),
                };
                Arc::new(Field::new(format!("a{i}"), dt, true))
            })
            .collect();
        let return_field = Arc::new(Field::new(
            "out",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ));
        let result = MvappendUdf::new()
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: n,
                return_field,
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .unwrap();
        match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(_) => panic!("expected array"),
        }
    }

    fn list_of_ints(rows: &[Option<&[Option<i32>]>]) -> ArrayRef {
        let mut builder = ListBuilder::new(Int32Builder::new());
        for row in rows {
            match row {
                None => builder.append_null(),
                Some(elems) => {
                    for e in *elems {
                        match e {
                            Some(v) => builder.values().append_value(*v),
                            None => builder.values().append_null(),
                        }
                    }
                    builder.append(true);
                }
            }
        }
        Arc::new(builder.finish())
    }

    fn extract_row_ints(arr: &ArrayRef, row: usize) -> Vec<i32> {
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        if list.is_null(row) {
            return vec![];
        }
        let inner = list.value(row);
        let typed = inner.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        (0..typed.len()).filter(|i| !typed.is_null(*i)).map(|i| typed.value(i)).collect()
    }

    #[test]
    fn three_scalar_ints() {
        // mvappend(1, 2, 3) → [1, 2, 3]
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
        ];
        let result = run(args, 1);
        assert_eq!(extract_row_ints(&result, 0), vec![1, 2, 3]);
    }

    #[test]
    fn flattens_array_argument() {
        // mvappend([1, 2], 3) → [1, 2, 3]
        let arr = list_of_ints(&[Some(&[Some(1), Some(2)])]);
        let args = vec![
            ColumnarValue::Array(arr),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
        ];
        let result = run(args, 1);
        assert_eq!(extract_row_ints(&result, 0), vec![1, 2, 3]);
    }

    #[test]
    fn drops_null_arg() {
        // mvappend(NULL, 1, 2) → [1, 2]
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
        ];
        let result = run(args, 1);
        assert_eq!(extract_row_ints(&result, 0), vec![1, 2]);
    }

    #[test]
    fn drops_null_elements_inside_array() {
        // mvappend([1, NULL, 2], 3) → [1, 2, 3]
        let arr = list_of_ints(&[Some(&[Some(1), None, Some(2)])]);
        let args = vec![
            ColumnarValue::Array(arr),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
        ];
        let result = run(args, 1);
        assert_eq!(extract_row_ints(&result, 0), vec![1, 2, 3]);
    }

    #[test]
    fn all_null_args_yield_null_row() {
        // mvappend(NULL, NULL) → NULL
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ColumnarValue::Scalar(ScalarValue::Int32(None)),
        ];
        let result = run(args, 1);
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.is_null(0));
    }

    #[test]
    fn empty_array_in_args_contributes_nothing() {
        // mvappend([], 1) → [1]
        let arr = list_of_ints(&[Some(&[])]);
        let args = vec![
            ColumnarValue::Array(arr),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
        ];
        let result = run(args, 1);
        assert_eq!(extract_row_ints(&result, 0), vec![1]);
    }
}
