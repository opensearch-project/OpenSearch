/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Post-consume analyzer rule that rewrites `nested_project` placeholder calls to native
//! `array_transform` higher-order function expressions — the projection counterpart of
//! `NestedAnyMatchRewriteRule` (filter). See design docs 20 / 20a.
//!
//! After the Substrait consumer builds the plan, a nested sub-path projection looks like:
//! ```text
//! Projection[ ScalarUDF(nested_project, [$events, "{\"field\":\"name\"}"]) ]
//! ```
//! This rule rewrites it to:
//! ```text
//! Projection[ HigherOrderFunction(array_transform, [$events, Lambda(e -> get_field(e,"name"))]) ]
//! ```
//! Grain-preserving: one `ARRAY<leaf>` per row.
//!
//! A map-value path (`{"field":"attributes","key":"k"}`) looks like:
//! ```text
//! Projection[ ScalarUDF(nested_project, [$events, "{\"field\":\"attributes\",\"key\":\"k\"}"]) ]
//! ```
//! and rewrites to:
//! ```text
//! Projection[ HigherOrderFunction(array_transform,
//!     [$events, Lambda(e -> array_element(map_extract(get_field(e,"attributes"),"k"), 1))]) ]
//! ```
//! `map_extract` returns a list (the values for the key), so element 1 is unwrapped to a scalar.
//!
//! TODO(native-array_transform): delete this rule once DataFusion consumes a Substrait HOF+lambda natively.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue, Spans};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::{HigherOrderFunction, Lambda, LambdaVariable, ScalarFunction};
use datafusion::logical_expr::{lit, Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use serde_json::Value;

#[derive(Debug)]
pub struct NestedProjectRewriteRule;

impl AnalyzerRule for NestedProjectRewriteRule {
    fn name(&self) -> &str {
        "nested_project_rewrite"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let result = plan.transform_up(|node| match node {
            LogicalPlan::Projection(proj) => {
                let schema = proj.input.schema().clone();
                let mut changed = false;
                let mut new_exprs = Vec::with_capacity(proj.expr.len());
                for e in proj.expr.iter() {
                    // Preserve the projection's output column name: swapping nested_project(...) for
                    // array_transform(...) changes the expr's derived name, which would break parent
                    // nodes that reference the column by name (optimize_projections fails otherwise).
                    let original_name = e.name_for_alias()?;
                    let rewritten = rewrite_nested_project_in_expr(e.clone(), schema.as_ref())?;
                    if &rewritten != e {
                        changed = true;
                        new_exprs.push(rewritten.alias_if_changed(original_name)?);
                    } else {
                        new_exprs.push(rewritten);
                    }
                }
                if !changed {
                    return Ok(Transformed::no(LogicalPlan::Projection(proj)));
                }
                let new_proj = datafusion::logical_expr::Projection::try_new(new_exprs, proj.input.clone())?;
                Ok(Transformed::yes(LogicalPlan::Projection(new_proj)))
            }
            other => Ok(Transformed::no(other)),
        })?;
        Ok(result.data)
    }
}

/// Rewrites every `nested_project` call in `expr` to a native `array_transform` HOF. Non-matching
/// subexpressions pass through unchanged.
pub fn rewrite_nested_project_in_expr(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    let result = expr.transform_up(|expr| {
        if let Some(rewritten) = try_rewrite_nested_project(&expr, schema) {
            Ok(Transformed::yes(rewritten?))
        } else {
            Ok(Transformed::no(expr))
        }
    })?;
    Ok(result.data)
}

fn try_rewrite_nested_project(expr: &Expr, schema: &DFSchema) -> Option<Result<Expr>> {
    let sf = match expr {
        Expr::ScalarFunction(sf) if sf.name() == "nested_project" => sf,
        _ => return None,
    };
    if sf.args.len() != 2 {
        return None;
    }
    let col_expr = sf.args[0].clone();
    let path_json = match &sf.args[1] {
        Expr::Literal(sv, _) => match sv {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => s.clone(),
            _ => return None,
        },
        _ => return None,
    };

    let col_type = col_expr.get_type(schema).ok()?;
    let element_type = match &col_type {
        DataType::List(field) => field.data_type().clone(),
        _ => return None,
    };

    let json: Value = serde_json::from_str(&path_json).ok()?;
    let obj = json.as_object()?;
    let field = obj.get("field")?.as_str()?.to_string();
    let map_key = obj.get("key").and_then(|k| k.as_str()).map(|s| s.to_string());

    let lambda_var = Expr::LambdaVariable(LambdaVariable {
        name: "e".to_string(),
        field: Some(Arc::new(Field::new("e", element_type, true))),
        spans: Spans::new(),
    });
    let body = build_project_body(&lambda_var, &field, map_key.as_deref());
    let lambda = Expr::Lambda(Lambda {
        params: vec!["e".to_string()],
        body: Box::new(body),
    });

    let hof = datafusion::functions_nested::array_transform::array_transform_higher_order_function();
    Some(Ok(Expr::HigherOrderFunction(HigherOrderFunction::new(hof, vec![col_expr, lambda]))))
}

/// The Arrow leaf type a projected sub-path yields per element: the struct field's type, or (for a
/// map key) the map's value type. Mirrors what `array_transform`'s lambda body produces, so the
/// `nested_project` placeholder can declare the correct `List<leaf>` return type up front — the
/// analyzer's rewrite must not change the column type (optimize_projections forbids it).
pub fn project_leaf_type(element_type: &DataType, path_json: &str) -> Option<DataType> {
    let json: Value = serde_json::from_str(path_json).ok()?;
    let obj = json.as_object()?;
    let field = obj.get("field")?.as_str()?;
    let fields = match element_type {
        DataType::Struct(fs) => fs,
        _ => return None,
    };
    let field_type = fields.iter().find(|f| f.name() == field)?.data_type().clone();
    match obj.get("key").and_then(|k| k.as_str()) {
        // Map value: field is Map(entry: Struct{key, value}) → the value field's type.
        Some(_) => match &field_type {
            DataType::Map(entry, _) => match entry.data_type() {
                DataType::Struct(kv) => kv.get(1).map(|f| f.data_type().clone()),
                _ => None,
            },
            _ => None,
        },
        None => Some(field_type),
    }
}

/// Builds the per-element projection: `get_field(e, field)` for a struct leaf or whole map, or
/// `array_element(map_extract(get_field(e, field), key), 1)` for a map value (map_extract returns
/// a list — unwrap element 1).
fn build_project_body(var: &Expr, field: &str, map_key: Option<&str>) -> Expr {
    let field_expr = datafusion::functions::core::expr_fn::get_field(var.clone(), field);
    match map_key {
        None => field_expr,
        Some(key) => {
            let map_extract = datafusion::functions_nested::map_extract::map_extract_udf();
            let extracted = Expr::ScalarFunction(ScalarFunction::new_udf(map_extract, vec![field_expr, lit(key)]));
            let array_element = datafusion::functions_nested::extract::array_element_udf();
            Expr::ScalarFunction(ScalarFunction::new_udf(array_element, vec![extracted, lit(1_i64)]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use datafusion::arrow::array::{ListArray, MapArray, StringArray, StructArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::DFSchema;
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::Expr;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn events_list_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "events",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![Field::new("name", DataType::Utf8, true)])),
                true,
            ))),
            true,
        )]))
    }

    fn nested_project_call(json: &str) -> Expr {
        let udf = Arc::new(datafusion::logical_expr::ScalarUDF::from(
            crate::udf::nested_project::NestedProject::new(),
        ));
        udf.call(vec![
            Expr::Column(datafusion::common::Column::from_name("events")),
            datafusion::logical_expr::lit(json),
        ])
    }

    /// A leaf path rewrites to an `array_transform` HOF.
    #[test]
    fn leaf_path_rewrites_to_array_transform() {
        let df_schema = DFSchema::try_from(events_list_schema().as_ref().clone()).unwrap();
        let call = nested_project_call(r#"{"field":"name"}"#);
        let rewritten = try_rewrite_nested_project(&call, &df_schema)
            .expect("rule must rewrite")
            .expect("rewrite must succeed");
        assert!(
            matches!(rewritten, Expr::HigherOrderFunction(_)),
            "expected array_transform HOF, got: {rewritten:?}"
        );
    }

    /// A map-value path rewrites to an array_transform whose lambda unwraps the map value via
    /// array_element(map_extract(get_field(e,"attributes"),"k"), 1) — the map_key branch of
    /// build_project_body that a plain struct-leaf path does not exercise.
    #[test]
    fn map_value_path_lowers_to_array_element_map_extract() {
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
        let element = DataType::Struct(Fields::from(vec![Field::new("attributes", map_ty, true)]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "events",
            DataType::List(Arc::new(Field::new("item", element, true))),
            true,
        )]));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();

        let call = nested_project_call(r#"{"field":"attributes","key":"k"}"#);
        let rewritten = try_rewrite_nested_project(&call, &df_schema)
            .expect("rule must rewrite")
            .expect("rewrite must succeed");

        // array_transform(e -> array_element(map_extract(get_field(e,"attributes"),"k"), 1))
        let Expr::HigherOrderFunction(hof) = &rewritten else {
            panic!("expected array_transform HOF, got: {rewritten:?}");
        };
        let Expr::Lambda(lambda) = &hof.args[1] else {
            panic!("expected a lambda as the 2nd arg, got: {:?}", hof.args[1]);
        };
        let Expr::ScalarFunction(outer) = lambda.body.as_ref() else {
            panic!("expected a scalar-fn lambda body, got: {:?}", lambda.body);
        };
        assert_eq!(outer.func.name(), "array_element", "map value must be unwrapped by array_element");
        let Expr::ScalarFunction(mid) = &outer.args[0] else {
            panic!("expected map_extract inside array_element, got: {:?}", outer.args[0]);
        };
        assert_eq!(mid.func.name(), "map_extract", "map value must come from map_extract");
    }

    /// project_leaf_type resolves the projected leaf's Arrow type per shape, so the placeholder can
    /// declare the correct List<leaf> up front (a numeric/map leaf must NOT default to Utf8 — that
    /// mismatch is what made the numeric/whole-map projections 500 before the fix).
    #[test]
    fn project_leaf_type_resolves_per_shape() {
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
        assert_eq!(project_leaf_type(&element, r#"{"field":"name"}"#), Some(DataType::Utf8));
        assert_eq!(project_leaf_type(&element, r#"{"field":"count"}"#), Some(DataType::Int32));
        assert_eq!(project_leaf_type(&element, r#"{"field":"attributes"}"#), Some(map_ty));
        assert_eq!(
            project_leaf_type(&element, r#"{"field":"attributes","key":"k"}"#),
            Some(DataType::Utf8)
        );
    }

    /// Non-`nested_project` expressions pass through unchanged.
    #[test]
    fn non_matching_expr_unchanged() {
        let df_schema = DFSchema::try_from(events_list_schema().as_ref().clone()).unwrap();
        let expr = datafusion::logical_expr::lit(1_i64);
        let out = rewrite_nested_project_in_expr(expr.clone(), &df_schema).unwrap();
        assert_eq!(out, expr);
    }

    /// End-to-end: project a nested leaf and confirm the per-row array is produced (one row in,
    /// one row out; the placeholder would have errored if not rewritten).
    #[tokio::test]
    async fn projects_leaf_as_per_row_array() {
        let struct_fields = Fields::from(vec![Field::new("name", DataType::Utf8, true)]);
        let item_field = Arc::new(Field::new("item", DataType::Struct(struct_fields.clone()), true));

        // one row: events = [{name:"a"}, {name:"b"}]
        let names = StringArray::from(vec![Some("a"), Some("b")]);
        let struct_arr = StructArray::new(struct_fields, vec![Arc::new(names) as _], None);
        let offsets = OffsetBuffer::new(vec![0i32, 2].into());
        let list_arr = ListArray::new(item_field.clone(), offsets, Arc::new(struct_arr), None);

        let schema = Arc::new(Schema::new(vec![Field::new("events", DataType::List(item_field), true)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list_arr) as _]).expect("batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");

        let analyzer_rules = {
            let mut rules = Analyzer::default().rules;
            rules.push(Arc::new(NestedProjectRewriteRule));
            rules
        };
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features()
            .with_analyzer_rules(analyzer_rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        crate::udf::nested_project::register_all(&ctx);
        ctx.register_table("t", Arc::new(table)).unwrap();

        let df = ctx
            .sql(r#"SELECT nested_project(events, '{"field":"name"}') AS names FROM t"#)
            .await
            .expect("sql plan");
        let rows: Vec<RecordBatch> = df.collect().await.expect("execute");
        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "grain preserved: one source row -> one output row");
    }

    /// Map-value shape end to end: the lambda is array_element(map_extract(get_field(e,"attributes"),"k"), 1).
    /// Confirms it plans, executes, and keeps grain (one row in/out) — the shape most likely to hit a
    /// 500 if nested_project's declared type disagrees with what array_transform produces.
    #[tokio::test]
    async fn projects_map_value_as_per_row_array() {
        // two maps {"k":"v1"} and {"k":"v2"} — one per event element
        let keys = StringArray::from(vec!["k", "k"]);
        let vals = StringArray::from(vec![Some("v1"), Some("v2")]);
        let entry_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]);
        let entries = StructArray::new(entry_fields.clone(), vec![Arc::new(keys) as _, Arc::new(vals) as _], None);
        let map_field = Arc::new(Field::new("key_value", DataType::Struct(entry_fields), false));
        let map_offsets = OffsetBuffer::new(vec![0i32, 1, 2].into());
        let map_arr = MapArray::new(map_field.clone(), map_offsets, entries, None, false);

        // element struct { attributes: Map }, two elements
        let elem_fields = Fields::from(vec![Field::new("attributes", DataType::Map(map_field.clone(), false), true)]);
        let struct_arr = StructArray::new(elem_fields.clone(), vec![Arc::new(map_arr) as _], None);

        // one row: events = [ {attributes:{k:v1}}, {attributes:{k:v2}} ]
        let item_field = Arc::new(Field::new("item", DataType::Struct(elem_fields), true));
        let list_offsets = OffsetBuffer::new(vec![0i32, 2].into());
        let list_arr = ListArray::new(item_field.clone(), list_offsets, Arc::new(struct_arr), None);

        let schema = Arc::new(Schema::new(vec![Field::new("events", DataType::List(item_field), true)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list_arr) as _]).expect("batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");

        let analyzer_rules = {
            let mut rules = Analyzer::default().rules;
            rules.push(Arc::new(NestedProjectRewriteRule));
            rules
        };
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features()
            .with_analyzer_rules(analyzer_rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        crate::udf::nested_project::register_all(&ctx);
        ctx.register_table("t", Arc::new(table)).unwrap();

        let df = ctx
            .sql(r#"SELECT nested_project(events, '{"field":"attributes","key":"k"}') AS vals FROM t"#)
            .await
            .expect("sql plan");
        let rows: Vec<RecordBatch> = df.collect().await.expect("execute");
        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "grain preserved: one source row -> one output row");
    }
}
