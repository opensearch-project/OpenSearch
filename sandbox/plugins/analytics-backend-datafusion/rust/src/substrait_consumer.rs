/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::common::{not_impl_err, substrait_err, DFSchema, TableReference};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::functions_nested::expr_fn::{array_distinct, array_slice};
use datafusion::logical_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    from_substrait_plan_with_consumer, DefaultSubstraitConsumer, SubstraitConsumer,
};
use substrait::proto::{ExtensionSingleRel, Plan};

pub const MULTI_VALUE_EXPAND_TYPE_URL: &str = "opensearch://analytics/multi_value_expand/v1";
const PAYLOAD_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpandSpec {
    field_index: usize,
    limit: Option<usize>,
    append: bool,
    distinct: bool,
}

impl ExpandSpec {
    fn decode(bytes: &[u8]) -> datafusion::common::Result<Self> {
        if bytes.len() != PAYLOAD_LEN {
            return substrait_err!(
                "multi-value expand payload must contain {PAYLOAD_LEN} bytes, got {}",
                bytes.len()
            );
        }
        let read_i32 = |offset| {
            i32::from_be_bytes(
                bytes[offset..offset + 4]
                    .try_into()
                    .expect("four-byte slice"),
            )
        };
        let field_index = read_i32(0);
        let limit = read_i32(4);
        let append = read_i32(8);
        let distinct = read_i32(12);
        if field_index < 0 || !matches!(append, 0 | 1) || !matches!(distinct, 0 | 1) || limit < -1 {
            return substrait_err!("invalid multi-value expand payload");
        }
        Ok(Self {
            field_index: field_index as usize,
            limit: (limit >= 0).then_some(limit as usize),
            append: append == 1,
            distinct: distinct == 1,
        })
    }
}

struct OpenSearchSubstraitConsumer<'a> {
    default: DefaultSubstraitConsumer<'a>,
}

impl<'a> OpenSearchSubstraitConsumer<'a> {
    fn new(extensions: &'a Extensions, state: &'a SessionState) -> Self {
        Self {
            default: DefaultSubstraitConsumer::new(extensions, state),
        }
    }
}

#[async_trait]
impl SubstraitConsumer for OpenSearchSubstraitConsumer<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        self.default.resolve_table_ref(table_ref).await
    }

    fn get_extensions(&self) -> &Extensions {
        self.default.get_extensions()
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.default.get_function_registry()
    }

    fn push_outer_schema(&self, schema: Arc<DFSchema>) {
        self.default.push_outer_schema(schema);
    }

    fn pop_outer_schema(&self) {
        self.default.pop_outer_schema();
    }

    fn get_outer_schema(&self, steps_out: usize) -> Option<Arc<DFSchema>> {
        self.default.get_outer_schema(steps_out)
    }

    async fn consume_extension_single(
        &self,
        rel: &ExtensionSingleRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        let detail = rel.detail.as_ref().ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "ExtensionSingleRel missing detail".to_string(),
            )
        })?;
        if detail.type_url != MULTI_VALUE_EXPAND_TYPE_URL {
            return self.default.consume_extension_single(rel).await;
        }
        let input = self
            .consume_rel(rel.input.as_ref().ok_or_else(|| {
                datafusion::common::DataFusionError::Plan(
                    "multi-value expand missing input".to_string(),
                )
            })?)
            .await?;
        expand_multivalue(input, ExpandSpec::decode(&detail.value)?)
    }
}

pub async fn from_substrait_plan(
    state: &SessionState,
    plan: &Plan,
) -> datafusion::common::Result<LogicalPlan> {
    let extensions = Extensions::try_from(&plan.extensions)?;
    if !extensions.type_variations.is_empty() {
        return not_impl_err!("Type variation extensions are not supported");
    }
    let consumer = OpenSearchSubstraitConsumer::new(&extensions, state);
    from_substrait_plan_with_consumer(&consumer, plan).await
}

fn expand_multivalue(
    input: LogicalPlan,
    spec: ExpandSpec,
) -> datafusion::common::Result<LogicalPlan> {
    let columns = input.schema().columns();
    let source = columns.get(spec.field_index).cloned().ok_or_else(|| {
        datafusion::common::DataFusionError::Plan(format!(
            "multi-value expand field index {} is outside {} columns",
            spec.field_index,
            columns.len()
        ))
    })?;
    let mut expanded = Expr::Column(source.clone());
    if spec.distinct {
        expanded = array_distinct(expanded);
    }
    if let Some(limit) = spec.limit {
        expanded = array_slice(expanded, lit(1_i64), lit(limit as i64), None);
    }

    let expand_name = if spec.append {
        let mut candidate = format!("___mvexpand_{}", spec.field_index);
        while input
            .schema()
            .field_with_unqualified_name(&candidate)
            .is_ok()
        {
            candidate.push('_');
        }
        candidate
    } else {
        source.name.clone()
    };

    let mut projection = columns
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    if spec.append {
        projection.push(expanded.alias(&expand_name));
    } else {
        projection[spec.field_index] = expanded.alias(&expand_name);
    }

    LogicalPlanBuilder::from(input)
        .project(projection)?
        .unnest_column(expand_name)?
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int32Array, ListBuilder};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    fn string_lists(rows: &[&[&str]]) -> datafusion::arrow::array::ListArray {
        let mut builder = ListBuilder::new(datafusion::arrow::array::StringViewBuilder::new());
        for values in rows {
            for value in *values {
                builder.values().append_value(*value);
            }
            builder.append(true);
        }
        builder.finish()
    }

    fn input_batch() -> RecordBatch {
        let tags = string_lists(&[&["b", "a", "a"], &["c", "d"]]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tags", tags.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(tags)],
        )
        .unwrap()
    }

    async fn run(spec: ExpandSpec) -> Vec<(i32, String)> {
        let ctx = SessionContext::new();
        let batch = input_batch();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        let input = ctx.table("t").await.unwrap().into_unoptimized_plan();
        let plan = expand_multivalue(input, spec).unwrap();
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column(if spec.append { 2 } else { 1 })
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((ids.value(row), values.value(row).to_string()));
            }
        }
        rows
    }

    #[tokio::test]
    async fn explicit_expand_appends_elements_and_honors_per_document_limit() {
        assert_eq!(
            run(ExpandSpec {
                field_index: 1,
                limit: Some(2),
                append: true,
                distinct: false,
            })
            .await,
            vec![
                (1, "b".into()),
                (1, "a".into()),
                (2, "c".into()),
                (2, "d".into())
            ]
        );
    }

    #[tokio::test]
    async fn group_expand_replaces_list_and_deduplicates_within_document() {
        assert_eq!(
            run(ExpandSpec {
                field_index: 1,
                limit: None,
                append: false,
                distinct: true,
            })
            .await,
            vec![
                (1, "b".into()),
                (1, "a".into()),
                (2, "c".into()),
                (2, "d".into())
            ]
        );
    }

    #[tokio::test]
    async fn sequential_group_expansion_forms_cartesian_product() {
        let first = string_lists(&[&["a", "a", "b"]]);
        let second = string_lists(&[&["x", "y"]]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first", first.data_type().clone(), true),
            Field::new("second", second.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(first),
                Arc::new(second),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        let input = ctx.table("t").await.unwrap().into_unoptimized_plan();
        let first_expanded = expand_multivalue(
            input,
            ExpandSpec {
                field_index: 1,
                limit: None,
                append: false,
                distinct: true,
            },
        )
        .unwrap();
        let plan = expand_multivalue(
            first_expanded,
            ExpandSpec {
                field_index: 2,
                limit: None,
                append: false,
                distinct: true,
            },
        )
        .unwrap();
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let first = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                .unwrap();
            let second = batch
                .column(2)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((first.value(row).to_string(), second.value(row).to_string()));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("a".into(), "x".into()),
                ("a".into(), "y".into()),
                ("b".into(), "x".into()),
                ("b".into(), "y".into()),
            ]
        );
    }

    #[test]
    fn payload_validation_rejects_invalid_flags() {
        let mut payload = Vec::new();
        for value in [1_i32, -1, 2, 0] {
            payload.extend_from_slice(&value.to_be_bytes());
        }
        assert!(ExpandSpec::decode(&payload).is_err());
    }
}
