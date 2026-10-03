/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Placeholder registration for `nested_project`.
//!
//! This UDF is never executed. `NestedProjectRewriteRule` rewrites every `nested_project` call to
//! a native `array_transform` HOF before the physical planner runs. The registration exists only
//! so the Substrait consumer can resolve the function name when building the logical plan.
//!
//! TODO(native-array_transform): delete this placeholder once a real array_transform lambda
//! round-trips through Substrait.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::udf_identity;

#[derive(Debug)]
pub struct NestedProject {
    signature: Signature,
}

udf_identity!(NestedProject, "nested_project");

impl NestedProject {
    pub fn new() -> Self {
        // Accept any 2-argument call — the rewrite rule replaces this before type-checking matters.
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NestedProject {
    fn name(&self) -> &str {
        "nested_project"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Fallback only. The real type is computed in return_field_from_args (which can see the
        // path-literal arg); return_type alone can't tell the leaf type from arg types.
        Ok(DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))))
    }

    /// Declares the true `List<leaf>` type by reading the JSON path literal (arg1) against the
    /// element struct of the array column (arg0). This MUST match what the analyzer's
    /// `array_transform` rewrite produces — otherwise the analyzer changes the column type and
    /// `optimize_projections`' schema-invariant check fails (500). Falls back to `List<Utf8>`.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let leaf = nested_project_leaf(&args).unwrap_or(DataType::Utf8);
        // List and item nullability are hardcoded true to match what array_transform derives. That
        // holds because a nested array and its elements are always nullable in OpenSearch; derive it
        // here if that ever changes.
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(Arc::new(Field::new("item", leaf, true))),
            true,
        )))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::common::internal_err!(
            "nested_project placeholder invoked — NestedProjectRewriteRule must have failed to rewrite"
        )
    }
}

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(NestedProject::new()));
}

/// element type (arg0's List element) + JSON path (arg1's string literal) → projected leaf type.
fn nested_project_leaf(args: &ReturnFieldArgs) -> Option<DataType> {
    let element = match args.arg_fields.first()?.data_type() {
        DataType::List(f) => f.data_type().clone(),
        _ => return None,
    };
    let path = match args.scalar_arguments.get(1).copied().flatten()? {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => s.as_str(),
        _ => return None,
    };
    crate::nested_project_rewrite_analyzer::project_leaf_type(&element, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::FunctionRegistry;
    use datafusion::logical_expr::ScalarUDFImpl;

    #[test]
    fn reports_name_and_list_return() {
        let udf = NestedProject::new();
        assert_eq!(udf.name(), "nested_project");
        assert!(matches!(udf.return_type(&[]).unwrap(), DataType::List(_)));
    }

    #[test]
    fn register_all_makes_the_name_resolvable() {
        let ctx = SessionContext::new();
        register_all(&ctx);
        assert!(ctx.udf("nested_project").is_ok());
    }

    /// return_field_from_args reads the path literal (arg1) against arg0's List element and declares
    /// the true `List<leaf>` — Utf8/Int32/Map/map-value. Declaring the wrong leaf (e.g. defaulting a
    /// numeric or whole-map projection to Utf8) is exactly what made those shapes 500 at execution.
    #[test]
    fn return_field_declares_list_of_leaf_per_shape() {
        use datafusion::arrow::datatypes::Fields;

        let map_ty = DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        );
        let element = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("count", DataType::Int32, true),
            Field::new("attributes", map_ty.clone(), true),
        ]));
        let arg0 = Arc::new(Field::new("events", DataType::List(Arc::new(Field::new("item", element, true))), true));
        let path_field = Arc::new(Field::new("path", DataType::Utf8, false));
        let udf = NestedProject::new();

        let mut check = |json: &str, expect_leaf: DataType| {
            let path = ScalarValue::Utf8(Some(json.to_string()));
            let arg_fields = [arg0.clone(), path_field.clone()];
            let scalar_arguments = [None, Some(&path)];
            let args = ReturnFieldArgs { arg_fields: &arg_fields, scalar_arguments: &scalar_arguments };
            let f = udf.return_field_from_args(args).expect("return field");
            assert_eq!(
                f.data_type(),
                &DataType::List(Arc::new(Field::new("item", expect_leaf, true))),
                "declared leaf type for {json}"
            );
        };
        check(r#"{"field":"name"}"#, DataType::Utf8);
        check(r#"{"field":"count"}"#, DataType::Int32);
        check(r#"{"field":"attributes"}"#, map_ty.clone());
        check(r#"{"field":"attributes","key":"k"}"#, DataType::Utf8);

        // No path literal available → falls back to List<Utf8> without panicking.
        let arg_fields = [arg0.clone(), path_field.clone()];
        let scalar_arguments = [None, None];
        let args = ReturnFieldArgs { arg_fields: &arg_fields, scalar_arguments: &scalar_arguments };
        let f = udf.return_field_from_args(args).expect("return field");
        assert_eq!(f.data_type(), &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))));
    }
}
