/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
Substrait-to-tree conversion: extracts the filter expression from a DataFusion
LogicalPlan and converts it to a SubstraitBoolNode tree.

Also provides `create_index_filter_udf()` — a DataFusion UDF that exists only
for plan representation so that `index_filter` survives the Substrait round-trip.
**/

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlan, Operator, ScalarFunctionArgs, ScalarUDF, Signature,
    TypeSignature, Volatility,
};
use datafusion_expr::ScalarUDFImpl;

use super::bool_tree::{ResolvedPredicate, SubstraitBoolNode};

// ── index_filter UDF ───────────────────────────────────────────────────

/// Creates a DataFusion scalar UDF named "index_filter" accepting two Utf8 args
/// (column, value) and returning Boolean. The UDF is never invoked directly —
/// it exists only for Substrait plan representation.
pub fn create_index_filter_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(IndexFilterUdf::new())
}

#[derive(Debug)]
struct IndexFilterUdf {
    signature: Signature,
}

impl IndexFilterUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl std::hash::Hash for IndexFilterUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl PartialEq for IndexFilterUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl Eq for IndexFilterUdf {}

impl ScalarUDFImpl for IndexFilterUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "index_filter"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        // When the plan is executed against a TreeIndexedTableProvider, the tree
        // evaluation handles the actual filtering. The UDF returns `true` for every
        // row so DataFusion's residual filter doesn't discard any rows — the tree
        // already did the filtering at the row-group/bitset level.
        use datafusion::arrow::array::BooleanArray;
        let num_rows = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        if num_rows == 1 {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
        } else {
            Ok(ColumnarValue::Array(std::sync::Arc::new(
                BooleanArray::from(vec![true; num_rows]),
            )))
        }
    }
}

// ── Filter extraction ──────────────────────────────────────────────────

/// Walks the LogicalPlan to find the Filter node and returns its filter expression.
pub fn extract_filter_expr(plan: &LogicalPlan) -> Result<Expr, String> {
    match plan {
        LogicalPlan::Filter(filter) => Ok(filter.predicate.clone()),
        _ => {
            // Recurse into children
            for child in plan.inputs() {
                if let Ok(expr) = extract_filter_expr(child) {
                    return Ok(expr);
                }
            }
            Err("no Filter node found in LogicalPlan".into())
        }
    }
}

// ── Expr → SubstraitBoolNode conversion ────────────────────────────────

/// Converts a DataFusion Expr (from the filter) into a SubstraitBoolNode tree
/// and a list of ResolvedPredicates for non-indexed columns.
///
/// - `index_filter('col', 'val')` → `SubstraitBoolNode::Collector { column, value }`
/// - `col > 42` → `SubstraitBoolNode::Predicate { predicate_id }` + push to predicates vec
/// - `AND(a, b)` → `SubstraitBoolNode::And(...)`
/// - `OR(a, b)` → `SubstraitBoolNode::Or(...)`
/// - `NOT(a)` → `SubstraitBoolNode::Not(...)`
pub fn expr_to_bool_tree(
    expr: &Expr,
) -> Result<(SubstraitBoolNode, Vec<ResolvedPredicate>), String> {
    let mut predicates = Vec::new();
    let node = convert_expr(expr, &mut predicates)?;
    Ok((node, predicates))
}

fn convert_expr(
    expr: &Expr,
    predicates: &mut Vec<ResolvedPredicate>,
) -> Result<SubstraitBoolNode, String> {
    match expr {
        // index_filter('column', 'value') UDF call → Collector
        Expr::ScalarFunction(func) if func.name() == "index_filter" => {
            if func.args.len() != 2 {
                return Err(format!(
                    "index_filter expects 2 args, got {}",
                    func.args.len()
                ));
            }
            let column = extract_string_literal(&func.args[0])?;
            let value = extract_string_literal(&func.args[1])?;
            Ok(SubstraitBoolNode::Collector { column, value })
        }

        // AND
        Expr::BinaryExpr(bin) if bin.op == Operator::And => {
            let left = convert_expr(&bin.left, predicates)?;
            let right = convert_expr(&bin.right, predicates)?;
            Ok(SubstraitBoolNode::And(vec![left, right]))
        }

        // OR
        Expr::BinaryExpr(bin) if bin.op == Operator::Or => {
            let left = convert_expr(&bin.left, predicates)?;
            let right = convert_expr(&bin.right, predicates)?;
            Ok(SubstraitBoolNode::Or(vec![left, right]))
        }

        // NOT (standard form)
        Expr::Not(inner) => {
            let child = convert_expr(inner, predicates)?;
            Ok(SubstraitBoolNode::Not(Box::new(child)))
        }

        // NOT rewritten as `expr = false` by optimizer
        Expr::BinaryExpr(bin) if bin.op == Operator::Eq => {
            if matches!(bin.right.as_ref(), Expr::Literal(ScalarValue::Boolean(Some(false)), _)) {
                let child = convert_expr(&bin.left, predicates)?;
                return Ok(SubstraitBoolNode::Not(Box::new(child)));
            }
            if matches!(bin.left.as_ref(), Expr::Literal(ScalarValue::Boolean(Some(false)), _)) {
                let child = convert_expr(&bin.right, predicates)?;
                return Ok(SubstraitBoolNode::Not(Box::new(child)));
            }
            // Regular equality comparison: Column = Literal → Predicate
            let (column, op, value) = extract_comparison(bin)?;
            let predicate_id = predicates.len() as u16;
            predicates.push(ResolvedPredicate { column, op, value });
            Ok(SubstraitBoolNode::Predicate { predicate_id })
        }

        // Comparison: Column op Literal → Predicate
        Expr::BinaryExpr(bin) if is_comparison_op(&bin.op) => {
            let (column, op, value) = extract_comparison(bin)?;
            let predicate_id = predicates.len() as u16;
            predicates.push(ResolvedPredicate { column, op, value });
            Ok(SubstraitBoolNode::Predicate { predicate_id })
        }

        // IsFalse(expr) — another optimizer rewrite of NOT
        Expr::IsFalse(inner) => {
            let child = convert_expr(inner, predicates)?;
            Ok(SubstraitBoolNode::Not(Box::new(child)))
        }

        // IsTrue(expr) — identity, just unwrap
        Expr::IsTrue(inner) => convert_expr(inner, predicates),

        // Anything else: Passthrough — let DataFusion's residual filter handle it.
        // This makes the converter robust against any unsupported expression pattern
        // (IN, LIKE, IS NULL, BETWEEN, nested functions, CASE/WHEN, etc.).
        _ => {
            log::warn!("expr_to_bool_tree: unsupported expression, using Passthrough: {:?}", expr);
            Ok(SubstraitBoolNode::Passthrough)
        }
    }
}

fn is_comparison_op(op: &Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
}

fn extract_string_literal(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Literal(lit, _) => match lit {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(s.clone()),
            other => Err(format!("expected string literal, got {:?}", other)),
        },
        _ => Err(format!("expected literal expression, got {:?}", expr)),
    }
}

fn extract_comparison(
    bin: &datafusion::logical_expr::BinaryExpr,
) -> Result<(String, Operator, ScalarValue), String> {
    // Column op Literal
    if let (Expr::Column(col), Expr::Literal(lit, _)) = (bin.left.as_ref(), bin.right.as_ref()) {
        return Ok((col.name().to_string(), bin.op, lit.clone()));
    }
    // Literal op Column → flip the operator
    if let (Expr::Literal(lit, _), Expr::Column(col)) = (bin.left.as_ref(), bin.right.as_ref()) {
        let flipped_op = flip_operator(&bin.op)?;
        return Ok((col.name().to_string(), flipped_op, lit.clone()));
    }
    Err(format!(
        "comparison must be Column op Literal or Literal op Column, got: {:?}",
        bin
    ))
}

fn flip_operator(op: &Operator) -> Result<Operator, String> {
    match op {
        Operator::Eq => Ok(Operator::Eq),
        Operator::NotEq => Ok(Operator::NotEq),
        Operator::Lt => Ok(Operator::Gt),
        Operator::LtEq => Ok(Operator::GtEq),
        Operator::Gt => Ok(Operator::Lt),
        Operator::GtEq => Ok(Operator::LtEq),
        _ => Err(format!("cannot flip operator: {:?}", op)),
    }
}
