/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Substrait → boolean tree conversion.
//!
//! After Substrait is decoded into a DataFusion `LogicalPlan`, the filter
//! expression is a tree of `Expr` nodes. This module walks that tree and
//! classifies each node:
//!
//! - `AND` / `OR` / `NOT` → `BoolNode::And` / `Or` / `Not`
//! - `ScalarFunction` named `COLLECTOR_FUNCTION_NAME` with one `Binary`
//!   literal argument → `BoolNode::Collector { query_bytes }`. Those bytes
//!   are the serialized backend query payload; they're handed to a Java
//!   factory at query-resolve time to create a provider.
//! - Any other comparison (`=`, `>`, `<`, etc.) → `BoolNode::Predicate`
//!   — details stashed in a side vec, referenced by index.
//!
//! **The substrait plan is the wire format.** Java never serializes an
//! `IndexFilterTree`; it rewrites `column = 'value'` on indexed columns to
//! `index_filter(query_bytes)` UDF calls during the Calcite marking phase,
//! and that survives the substrait round-trip. Rust just reads it back out
//! of the decoded `LogicalPlan`.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlan, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};

use super::bool_tree::{BoolNode, ResolvedPredicate};

/// The UDF name Calcite emits for indexed-column filter markers.
pub const COLLECTOR_FUNCTION_NAME: &str = "index_filter";

/// Classification of a query's filter expression — drives the evaluator choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterClass {
    /// Zero `index_filter` calls. Path A — regular DataFusion `ListingTable`,
    /// no `IndexedTableProvider` involvement.
    None,
    /// Exactly one `index_filter`, AND'd with parquet-native predicates.
    /// Path B — `SingleCollectorEvaluator`, DataFusion handles residual via
    /// predicate pushdown.
    SingleCollector,
    /// Multiple `index_filter` calls, or OR/NOT mixing them with predicates.
    /// Path C — `BitmapTreeEvaluator` two-phase evaluation.
    Tree,
}

/// Result of `expr_to_bool_tree`.
pub struct ExtractionResult {
    pub tree: BoolNode,
    pub predicates: Vec<ResolvedPredicate>,
}

/// Extract the filter expression from a DataFusion logical plan.
///
/// Walks down through Projection/SubqueryAlias/etc. nodes to find the first
/// `Filter` node. Returns `None` if there's no filter.
pub fn extract_filter_expr(plan: &LogicalPlan) -> Option<Expr> {
    match plan {
        LogicalPlan::Filter(filter) => Some(filter.predicate.clone()),
        _ => {
            for child in plan.inputs() {
                if let Some(expr) = extract_filter_expr(child) {
                    return Some(expr);
                }
            }
            None
        }
    }
}

/// Convert a DataFusion filter `Expr` to a `BoolNode` tree plus the list of
/// predicates referenced by `PredicateLeaf` nodes.
pub fn expr_to_bool_tree(expr: &Expr) -> Result<ExtractionResult, String> {
    let mut predicates = Vec::new();
    let tree = convert_expr(expr, &mut predicates)?;
    Ok(ExtractionResult { tree, predicates })
}

fn convert_expr(expr: &Expr, predicates: &mut Vec<ResolvedPredicate>) -> Result<BoolNode, String> {
    match expr {
        Expr::BinaryExpr(bin) => match bin.op {
            Operator::And => {
                let left = convert_expr(&bin.left, predicates)?;
                let right = convert_expr(&bin.right, predicates)?;
                Ok(BoolNode::And(vec![left, right]))
            }
            Operator::Or => {
                let left = convert_expr(&bin.left, predicates)?;
                let right = convert_expr(&bin.right, predicates)?;
                Ok(BoolNode::Or(vec![left, right]))
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => convert_comparison(&bin.left, bin.op, &bin.right, predicates),
            _ => Err(format!("unsupported binary operator: {:?}", bin.op)),
        },
        Expr::Not(inner) => {
            let child = convert_expr(inner, predicates)?;
            Ok(BoolNode::Not(Box::new(child)))
        }
        Expr::ScalarFunction(func) if func.name() == COLLECTOR_FUNCTION_NAME => {
            convert_collector_function(&func.args)
        }
        _ => Err(format!("unsupported expression: {:?}", expr)),
    }
}

/// Column op Literal  →  Predicate leaf + ResolvedPredicate.
/// Literal op Column  →  flipped-operator form of the above.
fn convert_comparison(
    left: &Expr,
    op: Operator,
    right: &Expr,
    predicates: &mut Vec<ResolvedPredicate>,
) -> Result<BoolNode, String> {
    let (column, resolved_op, value) = match (left, right) {
        (Expr::Column(col), Expr::Literal(v, _)) => (col.name().to_string(), op, v.clone()),
        (Expr::Literal(v, _), Expr::Column(col)) => {
            (col.name().to_string(), flip_operator(op), v.clone())
        }
        _ => {
            return Err(format!(
                "comparison must be column op literal or literal op column, got {:?} {:?} {:?}",
                left, op, right
            ));
        }
    };
    let id = predicates.len() as u16;
    predicates.push(ResolvedPredicate {
        column,
        op: resolved_op,
        value,
    });
    Ok(BoolNode::Predicate { predicate_id: id })
}

fn flip_operator(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// `index_filter(query_bytes)` — a single `Binary` literal arg.
fn convert_collector_function(args: &[Expr]) -> Result<BoolNode, String> {
    if args.len() != 1 {
        return Err(format!(
            "{} expects 1 arg (query_bytes), got {}",
            COLLECTOR_FUNCTION_NAME,
            args.len()
        ));
    }
    let bytes = extract_binary_literal(&args[0])?;
    Ok(BoolNode::Collector { query_bytes: bytes })
}

fn extract_binary_literal(expr: &Expr) -> Result<Arc<[u8]>, String> {
    match expr {
        Expr::Literal(ScalarValue::Binary(Some(v)), _) => Ok(Arc::from(v.as_slice())),
        Expr::Literal(ScalarValue::LargeBinary(Some(v)), _) => Ok(Arc::from(v.as_slice())),
        Expr::Literal(ScalarValue::FixedSizeBinary(_, Some(v)), _) => Ok(Arc::from(v.as_slice())),
        _ => Err(format!(
            "{} arg must be a Binary literal, got {:?}",
            COLLECTOR_FUNCTION_NAME, expr
        )),
    }
}

/// Classify a filter tree to decide which execution path to take.
///
/// - 0 collector leaves → `FilterClass::None`
/// - 1 collector leaf, top-level AND with non-collector children only → `FilterClass::SingleCollector`
/// - anything else (OR / NOT / multiple collectors / bare collector) → `FilterClass::Tree`
pub fn classify_filter(tree: &BoolNode) -> FilterClass {
    match tree.collector_leaf_count() {
        0 => FilterClass::None,
        1 => {
            if is_and_of_collector_plus_predicates(tree) {
                FilterClass::SingleCollector
            } else {
                FilterClass::Tree
            }
        }
        _ => FilterClass::Tree,
    }
}

/// Returns true if `tree` is `AND(Collector, ...non-collector-children)` with
/// exactly one `Collector` child directly under the top-level `AND` and all
/// other children having zero collector leaves. Path B shape.
fn is_and_of_collector_plus_predicates(tree: &BoolNode) -> bool {
    match tree {
        BoolNode::And(children) => {
            let mut collector_count = 0;
            for child in children {
                match child {
                    BoolNode::Collector { .. } => collector_count += 1,
                    other if other.collector_leaf_count() == 0 => {}
                    _ => return false, // nested collector — Tree path
                }
            }
            collector_count == 1
        }
        _ => false,
    }
}

/// Create the `index_filter(query_bytes) → Boolean` UDF.
///
/// This UDF exists solely as a marker for `classify_filter` / `expr_to_bool_tree`.
/// Its body is deliberately wired to return a hard `DataFusionError` if it
/// ever executes, because a production execution of the body would silently
/// produce all-true results and mask a routing bug in the dispatcher.
/// Register in a `SessionContext` before decoding substrait plans that
/// contain the UDF.
pub fn create_index_filter_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(IndexFilterUdf::new())
}

#[derive(Debug)]
struct IndexFilterUdf {
    signature: Signature,
}

impl IndexFilterUdf {
    fn new() -> Self {
        // The UDF takes exactly one binary payload; LargeBinary is accepted
        // for payloads that overflow Binary's 2 GiB limit. FixedSizeBinary is
        // also accepted at decode time (see `extract_binary_literal`) but we
        // don't enumerate every fixed size in the signature.
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary]),
                ],
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
        COLLECTOR_FUNCTION_NAME
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        // This body must never execute in production. `classify_filter`
        // recognizes the UDF by name and routes to the indexed evaluator;
        // when it works correctly, DataFusion never evaluates the UDF as a
        // function. If we reach here, classification missed the marker and
        // would otherwise silently return all-true, masking the bug and
        // producing wrong results. Fail loudly instead.
        Err(datafusion::common::DataFusionError::Internal(
            format!(
                "{} UDF body invoked — classify_filter did not recognize the marker; \
                 treat as a serious correctness bug",
                COLLECTOR_FUNCTION_NAME
            ),
        ))
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};
    use std::sync::Arc;

    // ── expr_to_bool_tree ────────────────────────────────────────────

    #[test]
    fn simple_predicate() {
        let expr = col("price").gt(lit(100i32));
        let r = expr_to_bool_tree(&expr).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate { predicate_id: 0 }));
        assert_eq!(r.predicates.len(), 1);
        assert_eq!(r.predicates[0].column, "price");
        assert_eq!(r.predicates[0].op, Operator::Gt);
    }

    #[test]
    fn literal_op_column_flips_operator() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(lit(100i32)),
            Operator::Lt,
            Box::new(col("price")),
        ));
        let r = expr_to_bool_tree(&expr).unwrap();
        // 100 < price → price > 100
        assert_eq!(r.predicates[0].op, Operator::Gt);
    }

    #[test]
    fn and_of_predicates() {
        let expr = col("price").gt(lit(100i32)).and(col("qty").lt(lit(50i32)));
        let r = expr_to_bool_tree(&expr).unwrap();
        assert!(matches!(r.tree, BoolNode::And(_)));
        assert_eq!(r.predicates.len(), 2);
    }

    #[test]
    fn not_predicate() {
        let expr = Expr::Not(Box::new(col("active").eq(lit(true))));
        let r = expr_to_bool_tree(&expr).unwrap();
        assert!(matches!(r.tree, BoolNode::Not(_)));
    }

    #[test]
    fn collector_function() {
        let udf = Arc::new(create_index_filter_udf());
        let expr = Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            udf,
            vec![lit(ScalarValue::Binary(Some(b"hello-query".to_vec())))],
        ));
        let r = expr_to_bool_tree(&expr).unwrap();
        match r.tree {
            BoolNode::Collector { query_bytes } => {
                assert_eq!(&*query_bytes, b"hello-query");
            }
            _ => panic!("expected Collector"),
        }
        assert!(r.predicates.is_empty());
    }

    #[test]
    fn mixed_tree() {
        // AND(collector(bytes), OR(price > 100, qty < 50))
        let udf = Arc::new(create_index_filter_udf());
        let collector =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                udf,
                vec![lit(ScalarValue::Binary(Some(b"mixed".to_vec())))],
            ));
        let or_branch = col("price").gt(lit(100i32)).or(col("qty").lt(lit(50i32)));
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(collector),
            Operator::And,
            Box::new(or_branch),
        ));
        let r = expr_to_bool_tree(&expr).unwrap();
        assert!(matches!(r.tree, BoolNode::And(_)));
        assert_eq!(r.predicates.len(), 2);
    }

    // ── classify_filter ──────────────────────────────────────────────

    fn collector(tag: &[u8]) -> BoolNode {
        BoolNode::Collector {
            query_bytes: Arc::from(tag),
        }
    }
    fn predicate(id: u16) -> BoolNode {
        BoolNode::Predicate { predicate_id: id }
    }

    #[test]
    fn classify_no_collectors_is_none() {
        assert_eq!(classify_filter(&predicate(0)), FilterClass::None);
        assert_eq!(
            classify_filter(&BoolNode::And(vec![predicate(0), predicate(1)])),
            FilterClass::None
        );
    }

    #[test]
    fn classify_bare_collector_is_tree() {
        assert_eq!(classify_filter(&collector(b"x")), FilterClass::Tree);
    }

    #[test]
    fn classify_and_of_collector_and_predicates_is_single() {
        let tree = BoolNode::And(vec![collector(b"x"), predicate(0), predicate(1)]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_and_with_two_collectors_is_tree() {
        let tree = BoolNode::And(vec![collector(b"x"), collector(b"y"), predicate(0)]);
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }

    #[test]
    fn classify_or_containing_collector_is_tree() {
        let tree = BoolNode::Or(vec![collector(b"x"), predicate(0)]);
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }

    #[test]
    fn classify_not_of_collector_is_tree() {
        let tree = BoolNode::Not(Box::new(collector(b"x")));
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }

    #[test]
    fn classify_and_with_nested_collector_is_tree() {
        let tree = BoolNode::And(vec![
            BoolNode::Or(vec![collector(b"x"), predicate(0)]),
            predicate(1),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }
}
