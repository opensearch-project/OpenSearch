/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Post-TypeCoercion analyzer rule that rewrites `nested_any_match` scalar UDF calls
//! to native `array_any_match` higher-order function expressions.
//!
//! After the Substrait consumer builds a logical plan, every nested predicate looks like:
//! ```text
//! Filter[ ScalarUDF(nested_any_match, [$events, json_string]) ]
//! ```
//! This rule rewrites that to:
//! ```text
//! Filter[ HigherOrderFunction(array_any_match, [$events, Lambda(e → <predicate>)]) ]
//! ```
//!
//! The lambda body is built from the JSON predicate tree the Java `ExprTreeBuilder` emitted.
//! Running AFTER TypeCoercion is deliberate: TypeCoercion types a lambda body against the outer
//! schema and cannot resolve struct-field access inside it, so building the lambda here — past
//! that check — lets field access inside the lambda work correctly.
//!
//! TODO(native-array_any_match): delete this rule once DataFusion consumes a Substrait HOF+lambda
//! natively -- the array_any_match we emit then needs no rewrite.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue, Spans};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{expr_fn, Expr, ExprSchemable, LogicalPlan};
use datafusion::logical_expr::expr::{HigherOrderFunction, Lambda, LambdaVariable};
use datafusion::optimizer::AnalyzerRule;
use serde_json::Value;

/// Plan-level rule that rewrites `nested_any_match` placeholders in `Filter` predicates to native
/// `array_any_match` HOFs (see the module docs for the JSON grammar).
///
/// This rule runs only on the normal query plan. When a nested predicate is combined with a
/// delegated (indexed) predicate, DataFusion lowers it a different way — through
/// `create_physical_expr`, which skips plan-level `AnalyzerRule`s like this one — so this rule
/// never sees it. `substrait_to_tree` handles that case by calling
/// [`rewrite_nested_any_match_in_expr`] directly.
#[derive(Debug)]
pub struct NestedAnyMatchRewriteRule;

impl AnalyzerRule for NestedAnyMatchRewriteRule {
    fn name(&self) -> &str {
        "nested_any_match_rewrite"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let result = plan.transform_up(|node| match node {
            LogicalPlan::Filter(filter) => {
                let input_schema = filter.input.schema().clone();
                let new_pred = rewrite_nested_any_match_in_expr(
                    filter.predicate.clone(),
                    input_schema.as_ref(),
                )?;
                if new_pred == filter.predicate {
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                }
                let new_filter =
                    datafusion::logical_expr::Filter::try_new(new_pred, filter.input.clone())?;
                Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
            }
            other => Ok(Transformed::no(other)),
        })?;
        Ok(result.data)
    }
}

/// Rewrites every `nested_any_match` call in `expr` to a native `array_any_match` HOF,
/// resolving element types from `schema`. Non-matching subexpressions pass through unchanged.
///
/// Shared by the plan-level [`NestedAnyMatchRewriteRule`] and the residual-predicate path in
/// `substrait_to_tree`, where a delegatable parent conjunct causes the nested conjunct to be
/// lowered to a `PhysicalExpr` directly — bypassing `AnalyzerRule`s. Both paths must rewrite the
/// placeholder before it reaches execution.
pub fn rewrite_nested_any_match_in_expr(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    let result = expr.transform_up(|expr| {
        if let Some(rewritten) = try_rewrite_nested_any_match(&expr, schema) {
            Ok(Transformed::yes(rewritten?))
        } else {
            Ok(Transformed::no(expr))
        }
    })?;
    Ok(result.data)
}

/// Returns `Some(Ok(rewritten))` when `expr` is a `nested_any_match` call that can be
/// lowered to a native `array_any_match` HOF, or `None` to leave the expression unchanged.
fn try_rewrite_nested_any_match(expr: &Expr, schema: &DFSchema) -> Option<Result<Expr>> {
    let sf = match expr {
        Expr::ScalarFunction(sf) if sf.name() == "nested_any_match" => sf,
        _ => return None,
    };
    if sf.args.len() != 2 {
        return None;
    }

    let col_expr = sf.args[0].clone();
    let json_str = match &sf.args[1] {
        Expr::Literal(sv, _) => match sv {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)) => s.clone(),
            _ => return None,
        },
        _ => return None,
    };

    let col_type = col_expr.get_type(schema).ok()?;
    let element_type = match &col_type {
        DataType::List(field) => field.data_type().clone(),
        _ => return None,
    };
    let element_fields = match &element_type {
        DataType::Struct(fields) => fields.clone(),
        _ => Fields::empty(),
    };

    let json_val: Value = serde_json::from_str(&json_str).ok()?;

    let lambda_var = Expr::LambdaVariable(LambdaVariable {
        name: "e".to_string(),
        field: Some(Arc::new(Field::new("e", element_type, true))),
        spans: Spans::new(),
    });

    let ctx = LambdaContext {
        var: &lambda_var,
        fields: &element_fields,
    };
    let body = build_predicate(&json_val, &ctx)?;

    let lambda = Expr::Lambda(Lambda {
        params: vec!["e".to_string()],
        body: Box::new(body),
    });

    let hof_fn =
        datafusion::functions_nested::array_any_match::array_any_match_higher_order_function();
    let rewritten =
        Expr::HigherOrderFunction(HigherOrderFunction::new(hof_fn, vec![col_expr, lambda]));

    Some(Ok(rewritten))
}

/// The lambda variable being matched plus the struct fields of the element type.
///
/// `fields` lets comparison builders resolve a leaf's Arrow type so a JSON literal can be coerced
/// to match it (e.g. an `Int64` JSON number against an `Int32` column) before the comparison is
/// built — this rule runs after TypeCoercion, so nothing else reconciles the width mismatch.
struct LambdaContext<'a> {
    var: &'a Expr,
    fields: &'a Fields,
}

/// Builds a DataFusion `Expr` from a JSON predicate node produced by `ExprTreeBuilder`.
///
/// Node shapes:
/// - `{"op":"="|"!="|">"|">="|"<"|"<=", "args":[left, right]}` — comparison
/// - `{"op":"AND"|"OR", "args":[...]}` — boolean connective
/// - `{"op":"NOT", "args":[child]}` — negation
/// - `{"op":"EXISTS", "args":[{"field":"name"}]}` — IS NOT NULL
/// - `{"op":"NOT_EXISTS", "args":[{"field":"name"}]}` — IS NULL
/// - `{"field":"name"}` — get_field(lambda_var, "name")
/// - `{"lit": value}` — literal
fn build_predicate(value: &Value, ctx: &LambdaContext) -> Option<Expr> {
    let obj = value.as_object()?;
    let op = obj.get("op")?.as_str()?;
    let args = obj.get("args")?.as_array()?;

    match op {
        "=" | "!=" | ">" | ">=" | "<" | "<=" => build_comparison(op, args, ctx),
        "AND" => {
            let mut result = build_predicate(args.first()?, ctx)?;
            for arg in args.iter().skip(1) {
                result = result.and(build_predicate(arg, ctx)?);
            }
            Some(result)
        }
        "OR" => {
            let mut result = build_predicate(args.first()?, ctx)?;
            for arg in args.iter().skip(1) {
                result = result.or(build_predicate(arg, ctx)?);
            }
            Some(result)
        }
        "NOT" => {
            let operand = build_predicate(args.first()?, ctx)?;
            Some(expr_fn::not(operand))
        }
        "EXISTS" => {
            let field_expr = build_value_expr(args.first()?, ctx)?;
            Some(field_expr.is_not_null())
        }
        "NOT_EXISTS" => {
            let field_expr = build_value_expr(args.first()?, ctx)?;
            Some(field_expr.is_null())
        }
        _ => None,
    }
}

/// Builds a binary comparison, coercing a literal operand to the other operand's field type so the
/// Arrow comparison kernel sees matching widths (see [`LambdaContext`]).
fn build_comparison(op: &str, args: &[Value], ctx: &LambdaContext) -> Option<Expr> {
    let left_node = args.first()?;
    let right_node = args.get(1)?;
    let left = build_value_expr(left_node, ctx)?;
    let right = build_value_expr(right_node, ctx)?;
    let (left, right) = coerce_operands(left_node, left, right_node, right, ctx.fields);
    match op {
        "=" => Some(left.eq(right)),
        "!=" => Some(left.not_eq(right)),
        ">" => Some(left.gt(right)),
        ">=" => Some(left.gt_eq(right)),
        "<" => Some(left.lt(right)),
        "<=" => Some(left.lt_eq(right)),
        _ => None,
    }
}

/// When one operand is a field access and the other a literal, casts the literal to the field's
/// Arrow type. Field-to-field and literal-to-literal comparisons are left untouched.
fn coerce_operands(
    left_node: &Value,
    left: Expr,
    right_node: &Value,
    right: Expr,
    fields: &Fields,
) -> (Expr, Expr) {
    if is_lit_node(right_node) {
        if let Some(target) = field_target_type(left_node, fields) {
            return (left, coerce_literal_to(right, &target));
        }
    }
    if is_lit_node(left_node) {
        if let Some(target) = field_target_type(right_node, fields) {
            return (coerce_literal_to(left, &target), right);
        }
    }
    (left, right)
}

/// Resolves the Arrow type of a `{"field":"name"}` node from the element struct, or `None` when the
/// node is not a field access or the name is absent from the struct.
fn field_target_type(node: &Value, fields: &Fields) -> Option<DataType> {
    let name = node.as_object()?.get("field")?.as_str()?;
    fields
        .iter()
        .find(|f| f.name() == name)
        .map(|f| f.data_type().clone())
}

fn is_lit_node(node: &Value) -> bool {
    node.as_object().is_some_and(|o| o.contains_key("lit"))
}

/// Coerces a literal `Expr` to `target`, folding the cast at rewrite time when the value converts
/// cleanly and falling back to a runtime `CAST` otherwise. Non-literal expressions are returned
/// unchanged.
fn coerce_literal_to(expr: Expr, target: &DataType) -> Expr {
    match expr {
        Expr::Literal(sv, metadata) => {
            if sv.data_type() == *target {
                return Expr::Literal(sv, metadata);
            }
            match sv.cast_to(target) {
                Ok(coerced) => Expr::Literal(coerced, metadata),
                Err(_) => expr_fn::cast(Expr::Literal(sv, metadata), target.clone()),
            }
        }
        other => other,
    }
}

/// Builds a value expression from a JSON node: `{"field":"name"}`, `{"lit":value}`, or a
/// nested predicate used as an arithmetic value.
fn build_value_expr(value: &Value, ctx: &LambdaContext) -> Option<Expr> {
    let obj = value.as_object()?;
    if let Some(field_val) = obj.get("field") {
        let field_name = field_val.as_str()?;
        Some(datafusion::functions::core::expr_fn::get_field(
            ctx.var.clone(),
            field_name,
        ))
    } else if let Some(lit_val) = obj.get("lit") {
        json_to_lit(lit_val)
    } else if obj.contains_key("op") {
        build_predicate(value, ctx)
    } else {
        None
    }
}

fn json_to_lit(value: &Value) -> Option<Expr> {
    use datafusion::logical_expr::lit;
    match value {
        Value::String(s) => Some(lit(s.clone())),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(lit(i))
            } else {
                n.as_f64().map(lit)
            }
        }
        Value::Bool(b) => Some(lit(*b)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use datafusion::arrow::array::{ListArray, StringArray, StructArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::DFSchema;
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::Expr;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn make_nested_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "events",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("name", DataType::Utf8, true),
                        Field::new("count", DataType::Int32, true),
                    ])),
                    true,
                ))),
                true,
            ),
        ]))
    }

    /// Verifies that the rule correctly rewrites a `nested_any_match` scalar-function call
    /// on a `List<Struct>` column to an `array_any_match` HOF with the correct lambda shape.
    #[tokio::test]
    async fn rule_rewrites_scalar_func_to_array_any_match() {
        let schema = make_nested_schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();

        // Build: nested_any_match(events, '{"op":"=","args":[{"field":"name"},{"lit":"exception"}]}')
        let json = r#"{"op":"=","args":[{"field":"name"},{"lit":"exception"}]}"#;
        let col_expr = Expr::Column(datafusion::common::Column::from_name("events"));
        let json_lit = datafusion::logical_expr::lit(json);

        let udf = Arc::new(datafusion::logical_expr::ScalarUDF::from(
            crate::udf::nested_any_match::NestedAnyMatch::new(),
        ));
        let scalar_call = udf.call(vec![col_expr, json_lit]);

        let rewritten = try_rewrite_nested_any_match(&scalar_call, &df_schema);
        assert!(rewritten.is_some(), "rule must rewrite nested_any_match");
        let result = rewritten.unwrap().expect("rewrite must succeed");
        assert!(
            matches!(result, Expr::HigherOrderFunction(_)),
            "result must be HigherOrderFunction, got: {result:?}"
        );
    }

    /// Verifies end-to-end execution: register a MemTable with List<Struct> data,
    /// run the analyzer rule, and execute the rewritten plan to assert correct rows.
    #[tokio::test]
    async fn rule_executes_correctly_against_list_struct_data() {
        let struct_fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("count", DataType::Int32, true),
        ]);
        let item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(struct_fields.clone()),
            true,
        ));

        // events: row 0 = [{name:"exception", count:1}], row 1 = [{name:"info", count:2}]
        let names = StringArray::from(vec![Some("exception"), Some("info")]);
        let counts = datafusion::arrow::array::Int32Array::from(vec![Some(1), Some(2)]);
        let struct_arr = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(names) as _, Arc::new(counts) as _],
            None,
        );
        let offsets = OffsetBuffer::new(vec![0i32, 1, 2].into());
        let list_arr = ListArray::new(item_field.clone(), offsets, Arc::new(struct_arr), None);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("events", DataType::List(item_field), true),
        ]));

        let ids = datafusion::arrow::array::Int32Array::from(vec![1i32, 2]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(ids) as _, Arc::new(list_arr) as _],
        )
        .expect("batch");

        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");

        let ctx = analyzer_context();
        // Register the placeholder UDF so the SQL parser can resolve the function name.
        crate::udf::nested_any_match::register_all(&ctx);
        ctx.register_table("t", Arc::new(table)).unwrap();

        let df = ctx
            .sql(
                r#"SELECT id FROM t WHERE nested_any_match(events,
                   '{"op":"=","args":[{"field":"name"},{"lit":"exception"}]}')"#,
            )
            .await
            .expect("sql plan");
        let rows: Vec<RecordBatch> = df.collect().await.expect("execute");
        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "only the row with events.name='exception' should match");
    }

    /// A `List<Struct>` numeric comparison must coerce the JSON `Int64` literal to the field's
    /// `Int32` type; otherwise Arrow rejects `Int32 > Int64` at execution.
    #[tokio::test]
    async fn numeric_comparison_coerces_literal_and_executes() {
        let struct_fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("count", DataType::Int32, true),
        ]);
        let item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(struct_fields.clone()),
            true,
        ));
        let list_type = DataType::List(item_field.clone());

        // The rewrite must fold the Int64 JSON literal `0` down to the field's Int32 type.
        let df_schema = DFSchema::try_from(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("events", list_type.clone(), true),
        ]))
        .unwrap();
        let scalar_call = nested_any_match_call(r#"{"op":">","args":[{"field":"count"},{"lit":0}]}"#);
        let rewritten = try_rewrite_nested_any_match(&scalar_call, &df_schema)
            .expect("rule must rewrite")
            .expect("rewrite must succeed");
        let literal = comparison_literal(&rewritten).expect("comparison must hold a literal operand");
        assert!(
            matches!(literal, ScalarValue::Int32(_)),
            "literal must be coerced to the field's Int32 type, got: {literal:?}"
        );

        // events: row 0 = [{count:5}] matches `> 0`; row 1 = [{count:0}] does not.
        let names = StringArray::from(vec![Some("a"), Some("b")]);
        let counts = datafusion::arrow::array::Int32Array::from(vec![Some(5), Some(0)]);
        let struct_arr = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(names) as _, Arc::new(counts) as _],
            None,
        );
        let offsets = OffsetBuffer::new(vec![0i32, 1, 2].into());
        let list_arr = ListArray::new(item_field, offsets, Arc::new(struct_arr), None);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("events", list_type, true),
        ]));
        let ids = datafusion::arrow::array::Int32Array::from(vec![1i32, 2]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(ids) as _, Arc::new(list_arr) as _],
        )
        .expect("batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");

        let ctx = analyzer_context();
        crate::udf::nested_any_match::register_all(&ctx);
        ctx.register_table("t", Arc::new(table)).unwrap();

        let df = ctx
            .sql(
                r#"SELECT id FROM t WHERE nested_any_match(events,
                   '{"op":">","args":[{"field":"count"},{"lit":0}]}')"#,
            )
            .await
            .expect("sql plan");
        let rows: Vec<RecordBatch> = df.collect().await.expect("execute");
        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "only the row with events.count > 0 should match");
    }

    /// The residual path (a nested conjunct ANDed with a delegatable parent scalar) never runs
    /// `AnalyzerRule`s. The shared rewrite must lower the placeholder to a HOF that
    /// `create_physical_expr` can plan, so the erroring placeholder never reaches execution.
    #[tokio::test]
    async fn shared_rewrite_lowers_residual_without_placeholder() {
        let schema = make_nested_schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();

        let residual = nested_any_match_call(
            r#"{"op":"=","args":[{"field":"name"},{"lit":"exception"}]}"#,
        );
        let rewritten =
            rewrite_nested_any_match_in_expr(residual, &df_schema).expect("rewrite must succeed");
        assert!(
            matches!(rewritten, Expr::HigherOrderFunction(_)),
            "residual must be rewritten to a HOF, got: {rewritten:?}"
        );

        let state = SessionContext::new().state();
        let phys = state
            .create_physical_expr(rewritten, &df_schema)
            .expect("lowering the rewritten residual must succeed");
        assert!(
            !format!("{phys:?}").contains("nested_any_match"),
            "lowered residual must not carry the placeholder UDF"
        );
    }

    /// A `SessionContext` whose analyzer pipeline includes [`NestedAnyMatchRewriteRule`].
    fn analyzer_context() -> SessionContext {
        let analyzer_rules = {
            let mut rules = Analyzer::default().rules;
            rules.push(Arc::new(NestedAnyMatchRewriteRule));
            rules
        };
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features()
            .with_analyzer_rules(analyzer_rules)
            .build();
        SessionContext::new_with_state(state)
    }

    /// Builds a `nested_any_match(events, <json>)` placeholder call.
    fn nested_any_match_call(json: &str) -> Expr {
        let udf = Arc::new(datafusion::logical_expr::ScalarUDF::from(
            crate::udf::nested_any_match::NestedAnyMatch::new(),
        ));
        udf.call(vec![
            Expr::Column(datafusion::common::Column::from_name("events")),
            datafusion::logical_expr::lit(json),
        ])
    }

    /// Extracts the right-hand literal from an `array_any_match(col, e -> <field> op <lit>)` HOF.
    fn comparison_literal(expr: &Expr) -> Option<ScalarValue> {
        let hof = match expr {
            Expr::HigherOrderFunction(hof) => hof,
            _ => return None,
        };
        let lambda = match hof.args.get(1)? {
            Expr::Lambda(lambda) => lambda,
            _ => return None,
        };
        match lambda.body.as_ref() {
            Expr::BinaryExpr(bin) => match bin.right.as_ref() {
                Expr::Literal(sv, _) => Some(sv.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    /// Builds a table `(id INT, events LIST<STRUCT<name:Utf8, count:Int32>>)` from per-row element
    /// lists, runs `nested_any_match(events, <json>)` through the analyzer, and returns the
    /// number of matching rows. `rows[i]` is the element list of row `i`; each element is
    /// `(name, count)`.
    async fn run_nested_filter(json: &str, rows: &[&[(Option<&str>, Option<i32>)]]) -> usize {
        let struct_fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("count", DataType::Int32, true),
        ]);
        let item_field = Arc::new(Field::new("item", DataType::Struct(struct_fields.clone()), true));

        let mut names: Vec<Option<&str>> = Vec::new();
        let mut counts: Vec<Option<i32>> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        for row in rows {
            for (name, count) in row.iter() {
                names.push(*name);
                counts.push(*count);
            }
            offsets.push(names.len() as i32);
        }
        let struct_arr = StructArray::new(
            struct_fields,
            vec![
                Arc::new(StringArray::from(names)) as _,
                Arc::new(datafusion::arrow::array::Int32Array::from(counts)) as _,
            ],
            None,
        );
        let list_arr = ListArray::new(
            item_field.clone(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(struct_arr),
            None,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("events", DataType::List(item_field), true),
        ]));
        let ids = datafusion::arrow::array::Int32Array::from((0..rows.len() as i32).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(ids) as _, Arc::new(list_arr) as _],
        )
        .expect("batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");

        let ctx = analyzer_context();
        crate::udf::nested_any_match::register_all(&ctx);
        ctx.register_table("t", Arc::new(table)).unwrap();

        // JSON uses only double quotes, so it embeds safely in a single-quoted SQL literal.
        // Built by concatenation (not format!) to avoid the `{`/`}` being read as placeholders.
        let sql = ["SELECT id FROM t WHERE nested_any_match(events, '", json, "')"].concat();
        let df = ctx.sql(&sql).await.expect("sql plan");
        let batches: Vec<RecordBatch> = df.collect().await.expect("execute");
        batches.iter().map(|b| b.num_rows()).sum()
    }

    /// A fused AND enforces `∃e:(A ∧ B)`, not `(∃e:A) ∧ (∃e:B)`: a single element must satisfy
    /// both conjuncts. Row 0 splits them across two elements (no match); row 1 has one element
    /// meeting both (match).
    #[tokio::test]
    async fn and_fusion_requires_single_element_to_satisfy_all() {
        let json = r#"{"op":"AND","args":[{"op":"=","args":[{"field":"name"},{"lit":"exception"}]},{"op":">","args":[{"field":"count"},{"lit":0}]}]}"#;
        let matched = run_nested_filter(
            json,
            &[
                &[(Some("exception"), Some(0)), (Some("info"), Some(5))],
                &[(Some("exception"), Some(5))],
            ],
        )
        .await;
        assert_eq!(matched, 1, "only the row with one element meeting both conjuncts matches");
    }

    /// OR matches when any element satisfies either branch.
    #[tokio::test]
    async fn or_matches_either_branch() {
        let json = r#"{"op":"OR","args":[{"op":"=","args":[{"field":"name"},{"lit":"a"}]},{"op":"=","args":[{"field":"name"},{"lit":"b"}]}]}"#;
        let matched = run_nested_filter(
            json,
            &[&[(Some("a"), None)], &[(Some("c"), None)], &[(Some("b"), None)]],
        )
        .await;
        assert_eq!(matched, 2);
    }

    /// NOT negates the per-element predicate. Names are non-null to keep NOT out of 3VL null
    /// poisoning (a separate, documented caveat).
    #[tokio::test]
    async fn not_negates_predicate() {
        let json = r#"{"op":"NOT","args":[{"op":"=","args":[{"field":"name"},{"lit":"a"}]}]}"#;
        let matched =
            run_nested_filter(json, &[&[(Some("a"), None)], &[(Some("b"), None)]]).await;
        assert_eq!(matched, 1, "the row whose only element is name != 'a' matches");
    }

    /// EXISTS lowers to `is_not_null(field)`.
    #[tokio::test]
    async fn exists_checks_field_not_null() {
        let json = r#"{"op":"EXISTS","args":[{"field":"name"}]}"#;
        let matched =
            run_nested_filter(json, &[&[(Some("a"), None)], &[(None, Some(1))]]).await;
        assert_eq!(matched, 1, "only the row with a non-null name matches");
    }

    /// NOT_EXISTS lowers to `is_null(field)`.
    #[tokio::test]
    async fn not_exists_checks_field_null() {
        let json = r#"{"op":"NOT_EXISTS","args":[{"field":"name"}]}"#;
        let matched =
            run_nested_filter(json, &[&[(Some("a"), None)], &[(None, Some(1))]]).await;
        assert_eq!(matched, 1, "only the row with a null name matches");
    }

    /// Expressions that contain no `nested_any_match` call are returned untouched.
    #[test]
    fn non_matching_expr_passes_through_unchanged() {
        let df_schema = DFSchema::try_from(make_nested_schema().as_ref().clone()).unwrap();
        let expr = Expr::Column(datafusion::common::Column::from_name("id"))
            .eq(datafusion::logical_expr::lit(1i32));
        let out = rewrite_nested_any_match_in_expr(expr.clone(), &df_schema).unwrap();
        assert_eq!(out, expr, "expressions without nested_any_match are untouched");
    }

    /// Unparseable JSON cannot be rewritten, so the placeholder is left in place (and would error
    /// at execution) rather than being silently dropped.
    #[test]
    fn malformed_json_leaves_call_unchanged() {
        let df_schema = DFSchema::try_from(make_nested_schema().as_ref().clone()).unwrap();
        let call = nested_any_match_call("{ not valid json");
        assert!(try_rewrite_nested_any_match(&call, &df_schema).is_none());
        let out = rewrite_nested_any_match_in_expr(call.clone(), &df_schema).unwrap();
        assert_eq!(out, call, "unparseable JSON leaves the placeholder untouched");
    }

    /// A call with the wrong argument count is not a well-formed placeholder — leave it alone.
    #[test]
    fn wrong_arg_count_is_not_rewritten() {
        let df_schema = DFSchema::try_from(make_nested_schema().as_ref().clone()).unwrap();
        let udf = Arc::new(datafusion::logical_expr::ScalarUDF::from(
            crate::udf::nested_any_match::NestedAnyMatch::new(),
        ));
        let call = udf.call(vec![Expr::Column(datafusion::common::Column::from_name("events"))]);
        assert!(try_rewrite_nested_any_match(&call, &df_schema).is_none());
    }

    /// The first argument must be a `List` column; anything else cannot yield an element type.
    #[test]
    fn non_list_column_is_not_rewritten() {
        let df_schema = DFSchema::try_from(make_nested_schema().as_ref().clone()).unwrap();
        let udf = Arc::new(datafusion::logical_expr::ScalarUDF::from(
            crate::udf::nested_any_match::NestedAnyMatch::new(),
        ));
        let call = udf.call(vec![
            Expr::Column(datafusion::common::Column::from_name("id")),
            datafusion::logical_expr::lit(r#"{"op":"EXISTS","args":[{"field":"name"}]}"#),
        ]);
        assert!(try_rewrite_nested_any_match(&call, &df_schema).is_none());
    }
}
