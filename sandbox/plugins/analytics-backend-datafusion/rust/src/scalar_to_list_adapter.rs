/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{ListArray, RecordBatch};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};

#[derive(Debug, Default)]
pub struct ScalarToListExprAdapterFactory;

impl PhysicalExprAdapterFactory for ScalarToListExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        // Let DataFusion's default adapter resolve names, physical indices, missing columns, and
        // ordinary casts. For promoted columns only, temporarily present the physical scalar field
        // as the logical field so the default adapter does not reject the intentional T -> List<T>
        // evolution before our second pass wraps that resolved scalar expression.
        let default_logical_schema = scalar_compatible_logical_schema(
            logical_file_schema.as_ref(),
            physical_file_schema.as_ref(),
        );
        let default = DefaultPhysicalExprAdapterFactory
            .create(default_logical_schema, Arc::clone(&physical_file_schema))?;
        Ok(Arc::new(ScalarToListExprAdapter {
            logical_file_schema,
            physical_file_schema,
            default,
        }))
    }
}

#[derive(Debug)]
struct ScalarToListExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    default: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for ScalarToListExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let resolved = self.default.rewrite(expr)?;
        resolved
            .transform(|expr| {
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let Ok(logical_field) = self.logical_file_schema.field_with_name(column.name())
                else {
                    return Ok(Transformed::no(expr));
                };
                let Ok(physical_field) = self.physical_file_schema.field_with_name(column.name())
                else {
                    return Ok(Transformed::no(expr));
                };
                let DataType::List(child) = logical_field.data_type() else {
                    return Ok(Transformed::no(expr));
                };
                if !can_promote_scalar_to_list(physical_field.data_type(), child.data_type()) {
                    return Ok(Transformed::no(expr));
                }
                Ok(Transformed::yes(Arc::new(ScalarToListExpr {
                    name: column.name().to_string(),
                    input: Arc::clone(&expr),
                    child: Arc::clone(child),
                    nullable: logical_field.is_nullable(),
                })
                    as Arc<dyn PhysicalExpr>))
            })
            .data()
    }
}

#[derive(Debug)]
struct ScalarToListExpr {
    name: String,
    input: Arc<dyn PhysicalExpr>,
    child: FieldRef,
    nullable: bool,
}

impl PartialEq for ScalarToListExpr {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.input.eq(&other.input)
            && self.child == other.child
            && self.nullable == other.nullable
    }
}

impl Eq for ScalarToListExpr {}

impl Hash for ScalarToListExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.input.hash(state);
        self.child.hash(state);
        self.nullable.hash(state);
    }
}

impl Display for ScalarToListExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "singleton_list({})", self.name)
    }
}

impl PhysicalExpr for ScalarToListExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::List(Arc::clone(&self.child)))
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.nullable)
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            &self.name,
            DataType::List(Arc::clone(&self.child)),
            self.nullable,
        )))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let physical_values = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        let values = if physical_values.data_type() == self.child.data_type() {
            physical_values
        } else {
            datafusion::arrow::compute::cast(physical_values.as_ref(), self.child.data_type())?
        };
        let offsets = OffsetBuffer::new((0..=batch.num_rows() as i32).collect::<Vec<_>>().into());
        let nulls = values.nulls().cloned();
        Ok(ColumnarValue::Array(Arc::new(ListArray::new(
            Arc::clone(&self.child),
            offsets,
            values,
            nulls,
        ))))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            name: self.name.clone(),
            input: Arc::clone(&children[0]),
            child: Arc::clone(&self.child),
            nullable: self.nullable,
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

fn scalar_compatible_logical_schema(logical: &Schema, physical: &Schema) -> SchemaRef {
    let fields = logical
        .fields()
        .iter()
        .map(|logical_field| {
            let Ok(physical_field) = physical.field_with_name(logical_field.name()) else {
                return Arc::clone(logical_field);
            };
            let DataType::List(child) = logical_field.data_type() else {
                return Arc::clone(logical_field);
            };
            if can_promote_scalar_to_list(physical_field.data_type(), child.data_type()) {
                Arc::new(physical_field.clone())
            } else {
                Arc::clone(logical_field)
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(
        fields,
        logical.metadata().clone(),
    ))
}

fn can_promote_scalar_to_list(physical: &DataType, logical_child: &DataType) -> bool {
    physical == logical_child
        || matches!(
            (physical, logical_child),
            (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8View)
                | (
                    DataType::Binary | DataType::LargeBinary,
                    DataType::BinaryView
                )
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, StringArray, StringViewArray};

    #[test]
    fn wraps_scalar_column_as_singleton_lists() {
        let child = Arc::new(Field::new("element", DataType::Utf8, true));
        let logical = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::clone(&child)),
            true,
        )]));
        let physical = Arc::new(Schema::new(vec![Field::new("tags", DataType::Utf8, true)]));
        let adapter = ScalarToListExprAdapterFactory
            .create(Arc::clone(&logical), Arc::clone(&physical))
            .unwrap();
        let expr = adapter.rewrite(Arc::new(Column::new("tags", 0))).unwrap();
        let batch = RecordBatch::try_new(
            physical,
            vec![Arc::new(StringArray::from(vec![Some("prod"), None]))],
        )
        .unwrap();

        let ColumnarValue::Array(array) = expr.evaluate(&batch).unwrap() else {
            panic!("expected array result");
        };
        let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.len(), 2);
        let first = lists.value(0);
        let first = first.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(first.value(0), "prod");
        assert!(lists.is_null(1));
    }

    #[test]
    fn wraps_utf8_scalar_as_singleton_utf8_view_lists() {
        let child = Arc::new(Field::new("element", DataType::Utf8View, true));
        let logical = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::clone(&child)),
            true,
        )]));
        let physical = Arc::new(Schema::new(vec![Field::new("tags", DataType::Utf8, true)]));
        let adapter = ScalarToListExprAdapterFactory
            .create(Arc::clone(&logical), Arc::clone(&physical))
            .unwrap();
        let expr = adapter.rewrite(Arc::new(Column::new("tags", 0))).unwrap();
        let batch = RecordBatch::try_new(
            physical,
            vec![Arc::new(StringArray::from(vec![Some("prod"), None]))],
        )
        .unwrap();

        let ColumnarValue::Array(array) = expr.evaluate(&batch).unwrap() else {
            panic!("expected array result");
        };
        let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
        let first = lists.value(0);
        let first = first.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(first.value(0), "prod");
        assert!(lists.is_null(1));
    }
}
