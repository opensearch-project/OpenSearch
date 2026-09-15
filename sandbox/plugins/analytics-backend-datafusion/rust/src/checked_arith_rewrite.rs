/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Logical-plan rewrite that makes BIGINT (`Int64`) `+`, `-`, `*` overflow *error* instead of
//! silently wrapping on the DataFusion backend.
//!
//! DataFusion lowers `Operator::Plus/Minus/Multiply` to arrow's wrapping integer kernels, so
//! `9223372036854775807 * 2` produces a wrapped negative value with no error. PPL callers expect
//! an error. This pass rewrites every logical `BinaryExpr { op: Plus | Minus | Multiply }` whose
//! **both operands resolve to `Int64`** into a call to the overflow-checked UDF equivalent
//! (`checked_add_i64` / `checked_sub_i64` / `checked_mul_i64` — see [`crate::udf::checked_arith`]).
//!
//! # Why logical, not physical
//!
//! Walking the *logical* plan reaches arithmetic in every node uniformly — Projection exprs,
//! Filter predicates, Aggregate group/argument exprs (`sum(a*b)`), Window args, Sort keys, Join
//! conditions — because [`LogicalPlan::map_expressions`] visits each node's top-level exprs and
//! `Expr::transform_up` recurses into every sub-expr. A physical-plan pass would have to reach the
//! exprs through per-node accessors, which do not uniformly surface aggregate/window argument
//! expressions.
//!
//! # Type gate
//!
//! Only `Int64 op Int64` is rewritten. `Float32/Float64` (which wrap to ±INF by PPL parity),
//! `Decimal*`, `UInt64`, and narrower integers all pass through untouched. After the SQL-plugin
//! widening (sql#5603), the only integer arithmetic that reaches DataFusion and can still overflow
//! is `Int64 op Int64`, so this gate is both correct and sufficient. Note the analytics scan
//! boundary already narrows `UInt64 -> Int64` (see `schema_coerce::coerce_inferred_schema`), so
//! OpenSearch `long` columns resolve to `Int64` here.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, ScalarUDF};

use crate::udf::checked_arith::{checked_add_udf, checked_mul_udf, checked_sub_udf};

/// Rewrite every `Int64 {+,-,*} Int64` `BinaryExpr` in `plan` into an overflow-checked UDF call.
/// Non-`Int64` arithmetic and non-`+/-/*` operators are left unchanged.
pub fn rewrite_checked_int64_arith(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up(|node| {
        // Operands of a node's expressions are evaluated over the node's *input* columns, so
        // resolve types against the merged input schema. For leaf nodes (no inputs) fall back to
        // the node's own schema.
        let schema = merged_input_schema(&node);
        // Preserve each expression's output name: rewriting `sum(a * b)` to
        // `sum(checked_mul_i64(a, b))` would otherwise let a later `recompute_schema` rename the
        // output field, breaking any parent node that references it by the original derived name.
        // NamePreserver re-aliases only when the name actually changed, and is a no-op for nodes
        // whose expressions don't contribute to the output schema (Filter/Join/…).
        let name_preserver = NamePreserver::new(&node);
        node.map_expressions(|expr| {
            let saved_name = name_preserver.save(&expr);
            expr.transform_up(|e| rewrite_expr(e, &schema))
                .map(|t| t.update_data(|rewritten| saved_name.restore(rewritten)))
        })
    })
    .map(|t| t.data)
}

/// Merge the schemas of a node's inputs so operand `Column`s resolve. Leaf nodes have no inputs,
/// so their own schema is used (their exprs, if any, are typed over it).
fn merged_input_schema(node: &LogicalPlan) -> DFSchemaRef {
    let inputs = node.inputs();
    if inputs.is_empty() {
        return Arc::new(node.schema().as_ref().clone());
    }
    let mut merged = DFSchema::empty();
    for child in inputs {
        // merge() is order-preserving and dedups by qualified name; ignore duplicate-column errors.
        merged.merge(child.schema());
    }
    Arc::new(merged)
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !matches!(op, Operator::Plus | Operator::Minus | Operator::Multiply) {
        return Ok(Transformed::no(expr));
    }
    // Resolve operand types. If either operand's type cannot be resolved against this schema
    // (an edge case such as a correlated column not present in the merged input schema), leave
    // the expression unchanged rather than failing the whole query — a missed rewrite falls back
    // to today's wrapping behavior, whereas propagating the error would break a working query.
    let (Ok(lt), Ok(rt)) = (left.get_type(schema), right.get_type(schema)) else {
        return Ok(Transformed::no(expr));
    };
    if lt != DataType::Int64 || rt != DataType::Int64 {
        return Ok(Transformed::no(expr));
    }

    // Consume `expr` to move the boxed operands into the UDF call without cloning.
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        unreachable!("matched BinaryExpr above");
    };
    let udf: Arc<ScalarUDF> = match op {
        Operator::Plus => checked_add_udf(),
        Operator::Minus => checked_sub_udf(),
        Operator::Multiply => checked_mul_udf(),
        _ => unreachable!("op guarded above"),
    };
    let call = Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![*left, *right]));
    Ok(Transformed::yes(call))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::{col, lit, LogicalPlanBuilder};

    /// Count how many `ScalarFunction`s named `name` appear anywhere in the plan's expressions.
    fn count_udf(plan: &LogicalPlan, name: &str) -> usize {
        use std::cell::Cell;
        let count = Cell::new(0usize);
        plan.apply(|node| {
            node.apply_expressions(|expr| {
                expr.apply(|e| {
                    if let Expr::ScalarFunction(sf) = e {
                        if sf.func.name() == name {
                            count.set(count.get() + 1);
                        }
                    }
                    Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
                })
            })
        })
        .unwrap();
        count.get()
    }

    /// A schema-carrying `EmptyRelation` builder; enough to type-resolve column operands for the
    /// rewrite without a real table provider.
    fn scan(fields: Vec<Field>) -> LogicalPlanBuilder {
        use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
        let arrow_schema = datafusion::arrow::datatypes::Schema::new(fields);
        let schema = Arc::new(DFSchema::try_from(arrow_schema).unwrap());
        LogicalPlanBuilder::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema,
        }))
    }

    fn int_float_scan() -> LogicalPlanBuilder {
        scan(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("g", DataType::Float64, true),
        ])
    }

    #[test]
    fn rewrites_int64_mul_in_projection() {
        let plan = int_float_scan()
            .project(vec![(col("a") * col("b")).alias("x")])
            .unwrap()
            .build()
            .unwrap();
        let out = rewrite_checked_int64_arith(plan).unwrap();
        assert_eq!(count_udf(&out, "checked_mul_i64"), 1);
        assert_eq!(count_udf(&out, "checked_add_i64"), 0);
    }

    #[test]
    fn rewrites_int64_add_and_sub() {
        let plan = int_float_scan()
            .project(vec![
                (col("a") + col("b")).alias("s"),
                (col("a") - col("b")).alias("d"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let out = rewrite_checked_int64_arith(plan).unwrap();
        assert_eq!(count_udf(&out, "checked_add_i64"), 1);
        assert_eq!(count_udf(&out, "checked_sub_i64"), 1);
    }

    #[test]
    fn leaves_float_mul_untouched() {
        let plan = int_float_scan()
            .project(vec![(col("f") * col("g")).alias("x")])
            .unwrap()
            .build()
            .unwrap();
        let out = rewrite_checked_int64_arith(plan).unwrap();
        assert_eq!(count_udf(&out, "checked_mul_i64"), 0);
    }

    #[test]
    fn leaves_divide_untouched() {
        let plan = int_float_scan()
            .project(vec![(col("a") / col("b")).alias("x")])
            .unwrap()
            .build()
            .unwrap();
        let out = rewrite_checked_int64_arith(plan).unwrap();
        // No checked_* udf: DIVIDE is not in {Plus,Minus,Multiply}.
        assert_eq!(count_udf(&out, "checked_mul_i64"), 0);
        assert_eq!(count_udf(&out, "checked_add_i64"), 0);
        assert_eq!(count_udf(&out, "checked_sub_i64"), 0);
    }

    #[test]
    fn rewrites_arithmetic_in_filter_predicate() {
        // Proves node-agnostic coverage: the arithmetic lives in a Filter, not a Projection.
        let plan = int_float_scan()
            .filter((col("a") * col("b")).gt(lit(5i64)))
            .unwrap()
            .build()
            .unwrap();
        let out = rewrite_checked_int64_arith(plan).unwrap();
        assert_eq!(count_udf(&out, "checked_mul_i64"), 1);
    }

    #[test]
    fn rewrites_nested_arithmetic_bottom_up() {
        // (a * b) + c  ->  checked_add(checked_mul(a, b), c)
        let plan = int_float_scan()
            .project(vec![((col("a") * col("b")) + col("c")).alias("x")])
            .unwrap()
            .build()
            .unwrap();
        let out = rewrite_checked_int64_arith(plan).unwrap();
        assert_eq!(count_udf(&out, "checked_mul_i64"), 1);
        assert_eq!(count_udf(&out, "checked_add_i64"), 1);
    }

    #[test]
    fn rewrites_arithmetic_in_sort_key() {
        // Arithmetic inside a Sort key must be reached (node-agnostic coverage beyond
        // Projection/Filter — the same map_expressions traversal reaches Aggregate/Window args).
        let plan = int_float_scan()
            .sort(vec![(col("a") * col("b")).sort(true, false)])
            .unwrap()
            .build()
            .unwrap();
        let out = rewrite_checked_int64_arith(plan).unwrap();
        assert_eq!(count_udf(&out, "checked_mul_i64"), 1);
    }

    #[test]
    fn leaves_int64_literal_times_float_untouched() {
        // Mixed Int64 * Float64 -> operands are (Int64, Float64); gate requires BOTH Int64.
        let plan = int_float_scan()
            .project(vec![(col("a") * col("f")).alias("x")])
            .unwrap()
            .build()
            .unwrap();
        let out = rewrite_checked_int64_arith(plan).unwrap();
        assert_eq!(count_udf(&out, "checked_mul_i64"), 0);
    }
}
