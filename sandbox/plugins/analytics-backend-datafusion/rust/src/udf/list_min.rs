/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use datafusion::arrow::array::{make_comparator, Array, ArrayRef, ListArray, UInt64Array};
use datafusion::arrow::compute::{take, SortOptions};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{exec_err, plan_err, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::ScalarFunctionExpr;

use super::udf_identity;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ListMinUdf::new()));
}

pub fn udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(ListMinUdf::new()))
}

pub fn expr(input: Expr) -> Expr {
    udf().call(vec![input])
}

pub fn physical_expr(
    input: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        udf(),
        vec![input],
        schema,
        Arc::new(ConfigOptions::default()),
    )?))
}

#[derive(Debug)]
pub struct ListMinUdf {
    signature: Signature,
}

impl ListMinUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(ListMinUdf, "list_min");

impl ScalarUDFImpl for ListMinUdf {
    fn name(&self) -> &str {
        "list_min"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return plan_err!("list_min expects one argument, got {}", arg_types.len());
        }
        match &arg_types[0] {
            DataType::List(child) => Ok(child.data_type().clone()),
            other => plan_err!("list_min expects List<T>, got {other:?}"),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.return_type(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("list_min expects one argument, got {}", args.args.len());
        }
        let array = args.args[0].clone().into_array(args.number_rows)?;
        let lists = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(format!(
                "list_min expected ListArray, got {:?}",
                array.data_type()
            ))
        })?;
        Ok(ColumnarValue::Array(min_values(lists)?))
    }
}

fn min_values(lists: &ListArray) -> Result<ArrayRef> {
    let values = lists.values();
    let compare = make_comparator(
        values.as_ref(),
        values.as_ref(),
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )?;
    let offsets = lists.value_offsets();
    let mut indices = Vec::with_capacity(lists.len());
    for row in 0..lists.len() {
        if lists.is_null(row) {
            indices.push(None);
            continue;
        }
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let mut minimum = None;
        for index in start..end {
            if values.is_null(index) {
                continue;
            }
            minimum = Some(match minimum {
                Some(current) if compare(current, index).is_le() => current,
                _ => index,
            });
        }
        indices.push(minimum.map(|index| index as u64));
    }
    take(values.as_ref(), &UInt64Array::from(indices), None).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ListBuilder, StringViewArray, StringViewBuilder};

    #[test]
    fn returns_minimum_non_null_element_per_row() {
        let mut builder = ListBuilder::new(StringViewBuilder::new());
        for value in [Some("z"), None, Some("alpha")] {
            match value {
                Some(value) => builder.values().append_value(value),
                None => builder.values().append_null(),
            }
        }
        builder.append(true);
        builder.append(true);
        builder.append(false);
        builder.values().append_null();
        builder.append(true);
        for value in ["omega", "beta"] {
            builder.values().append_value(value);
        }
        builder.append(true);

        let result = min_values(&builder.finish()).unwrap();
        let result = result.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(result.value(0), "alpha");
        assert!(result.is_null(1));
        assert!(result.is_null(2));
        assert!(result.is_null(3));
        assert_eq!(result.value(4), "beta");
    }

    #[test]
    fn rejects_non_list_input_type() {
        let error = ListMinUdf::new()
            .return_type(&[DataType::Utf8View])
            .unwrap_err();
        assert!(error.to_string().contains("expects List<T>"));
    }
}
