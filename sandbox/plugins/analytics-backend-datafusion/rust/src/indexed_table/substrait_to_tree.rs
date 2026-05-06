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
//!   literal argument → `BoolNode::Collector { annotation_id }`. The ID
//!   are the serialized backend query payload; they're handed to a Java
//!   factory at query-resolve time to create a provider.
//! - **Anything else** → lowered to [`Arc<dyn PhysicalExpr>`] via
//!   DataFusion's `create_physical_expr`, wrapped in
//!   [`BoolNode::Predicate`]. Comparisons, `IS NULL`, `IN`, `BETWEEN`,
//!   arithmetic, casts, UDFs — any boolean-valued DataFusion expression
//!   is accepted.
//!
//! **The substrait plan is the wire format.** Java never serializes an
//! `IndexFilterTree`; it rewrites `column = 'value'` on indexed columns to
//! `delegated_predicate(annotationId)` UDF calls during the Calcite marking phase,
//! and that survives the substrait round-trip. Rust just reads it back out
//! of the decoded `LogicalPlan`.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlan, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use datafusion::physical_expr::create_physical_expr;
#[cfg(test)]
use datafusion::physical_expr::PhysicalExpr;

use super::bool_tree::BoolNode;

/// The UDF name Calcite emits for indexed-column filter markers.
pub const COLLECTOR_FUNCTION_NAME: &str = "delegated_predicate";

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

/// Result of `expr_to_bool_tree` — just the tree. No sidecar list needed
/// now that `Predicate` leaves carry `Arc<dyn PhysicalExpr>` directly.
#[derive(Debug)]
pub struct ExtractionResult {
    pub tree: BoolNode,
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

/// Convert a DataFusion filter `Expr` to a `BoolNode` tree.
///
/// `schema` is used to lower non-combinator subexpressions to
/// `Arc<dyn PhysicalExpr>` via `create_physical_expr`. The expression at
/// those leaves must be boolean-valued; anything else is rejected.
pub fn expr_to_bool_tree(expr: &Expr, schema: &SchemaRef) -> Result<ExtractionResult, String> {
    let df_schema =
        DFSchema::try_from(schema.as_ref().clone()).map_err(|e| format!("DFSchema: {}", e))?;
    let props = ExecutionProps::new();
    let tree = convert_expr(expr, schema, &df_schema, &props)?;
    Ok(ExtractionResult { tree })
}

fn convert_expr(
    expr: &Expr,
    schema: &Schema,
    df_schema: &DFSchema,
    props: &ExecutionProps,
) -> Result<BoolNode, String> {
    match expr {
        Expr::BinaryExpr(bin) if bin.op == Operator::And => {
            let left = convert_expr(&bin.left, schema, df_schema, props)?;
            let right = convert_expr(&bin.right, schema, df_schema, props)?;
            Ok(BoolNode::And(vec![left, right]))
        }
        Expr::BinaryExpr(bin) if bin.op == Operator::Or => {
            let left = convert_expr(&bin.left, schema, df_schema, props)?;
            let right = convert_expr(&bin.right, schema, df_schema, props)?;
            Ok(BoolNode::Or(vec![left, right]))
        }
        Expr::Not(inner) => {
            let child = convert_expr(inner, schema, df_schema, props)?;
            Ok(BoolNode::Not(Box::new(child)))
        }
        Expr::ScalarFunction(func) if func.name() == COLLECTOR_FUNCTION_NAME => {
            convert_collector_function(&func.args)
        }
        // Anything else — comparison, IS NULL, IN, BETWEEN, arithmetic,
        // CAST, UDF — gets lowered to a DataFusion PhysicalExpr. We
        // require boolean return type so the tree evaluator can
        // interpret the result as a per-row mask.
        other => {
            // Strip table qualifiers from Column references. DataFusion's
            // substrait consumer qualifies field references with the
            // NamedScan table name (e.g. "test_table.elb_status_code"),
            // but the parquet schema has bare names. Without stripping,
            // `create_physical_expr` fails with "No field named ...".
            let unqualified = strip_column_qualifiers(other);
            let phys = create_physical_expr(&unqualified, df_schema, props)
                .map_err(|e| format!("create_physical_expr for {:?}: {}", unqualified, e))?;
            let return_type = phys
                .data_type(schema)
                .map_err(|e| format!("data_type: {}", e))?;
            if return_type != DataType::Boolean {
                return Err(format!(
                    "indexed-query expression must be boolean-valued, got {:?}: {:?}",
                    return_type, other
                ));
            }
            Ok(BoolNode::Predicate(phys))
        }
    }
}

/// `delegated_predicate(annotationId)` — a single `Int32` literal arg.
fn convert_collector_function(args: &[Expr]) -> Result<BoolNode, String> {
    if args.len() != 1 {
        return Err(format!(
            "{} expects 1 arg (annotationId), got {}",
            COLLECTOR_FUNCTION_NAME,
            args.len()
        ));
    }
    let annotation_id = extract_int32_literal(&args[0])?;
    Ok(BoolNode::Collector { annotation_id })
}

/// Strip table qualifiers from `Column` references in an `Expr` tree.
/// DataFusion's substrait consumer qualifies field references with the
/// NamedScan table name, but the parquet schema has bare column names.
fn strip_column_qualifiers(expr: &Expr) -> Expr {
    expr.clone()
        .transform(|e| {
            if let Expr::Column(col) = &e {
                if col.relation.is_some() {
                    return Ok(datafusion::common::tree_node::Transformed::yes(
                        Expr::Column(datafusion::common::Column::new_unqualified(&col.name)),
                    ));
                }
            }
            Ok(datafusion::common::tree_node::Transformed::no(e))
        })
        .unwrap()
        .data
}

fn extract_int32_literal(expr: &Expr) -> Result<i32, String> {
    match expr {
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(*v),
        _ => Err(format!(
            "{} arg must be an Int32 literal, got {:?}",
            COLLECTOR_FUNCTION_NAME, expr
        )),
    }
}

/// Classify a filter tree to decide which execution path to take.
///
/// - 0 collector leaves → `FilterClass::None`
/// - bare collector → `FilterClass::SingleCollector`
/// - any AND-only tree (no OR/NOT above collectors) with ≥1 collector
///   → `FilterClass::SingleCollector`. Nested ANDs with mixed
///   collectors + predicates are accepted; `single_collector_bytes`
///   merges the collectors and `extract_single_collector_residual`
///   strips them to produce the predicate residual.
/// - anything else (OR / NOT above a collector) → `FilterClass::Tree`
pub fn classify_filter(tree: &BoolNode) -> FilterClass {
    if tree.collector_leaf_count() == 0 {
        return FilterClass::None;
    }
    if matches!(tree, BoolNode::Collector { .. }) {
        return FilterClass::SingleCollector;
    }
    if is_and_only_collector_tree(tree) {
        FilterClass::SingleCollector
    } else {
        FilterClass::Tree
    }
}

/// Returns true when every collector in `tree` is reachable only
/// through AND nodes (no OR or NOT on the path from root to any
/// collector leaf). Predicates, ANDs, and collector leaves are fine;
/// OR or NOT containing a collector disqualifies.
fn is_and_only_collector_tree(tree: &BoolNode) -> bool {
    match tree {
        BoolNode::And(children) => children.iter().all(is_and_only_collector_tree),
        BoolNode::Collector { .. } | BoolNode::Predicate(_) => true,
        // OR or NOT containing any collector → Tree path.
        BoolNode::Or(_) | BoolNode::Not(_) => tree.collector_leaf_count() == 0,
    }
}

/// Create the `delegated_predicate(annotationId) → Boolean` UDF.
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
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int32]),
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
        Err(datafusion::common::DataFusionError::Internal(format!(
            "{} UDF body invoked — classify_filter did not recognize the marker; \
                 treat as a serious correctness bug",
            COLLECTOR_FUNCTION_NAME
        )))
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_expr::expressions::{Column as PhysColumn, Literal};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
            Field::new("qty", DataType::Int32, false),
            Field::new("active", DataType::Boolean, false),
        ]))
    }

    // ── expr_to_bool_tree ────────────────────────────────────────────

    #[test]
    fn simple_predicate() {
        let expr = col("price").gt(lit(100i32));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn literal_op_column_works() {
        // 100 < price — valid boolean expression, lowered as-is.
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(lit(100i32)),
            Operator::Lt,
            Box::new(col("price")),
        ));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn and_of_predicates() {
        let expr = col("price").gt(lit(100i32)).and(col("qty").lt(lit(50i32)));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::And(_)));
    }

    #[test]
    fn not_predicate() {
        let expr = Expr::Not(Box::new(col("active").eq(lit(true))));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Not(_)));
    }

    #[test]
    fn in_list_expression_is_accepted() {
        let expr = col("price").in_list(vec![lit(5i32), lit(10i32), lit(15i32)], false);
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn is_null_expression_is_accepted() {
        let expr = Expr::IsNull(Box::new(col("price")));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn between_expression_is_accepted() {
        // price BETWEEN 10 AND 50
        let expr = col("price").between(lit(10i32), lit(50i32));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        // BETWEEN may desugar into And internally or stay as-is; either
        // shape is accepted so long as the result is boolean-valued.
        match r.tree {
            BoolNode::Predicate(_) | BoolNode::And(_) => {}
            other => panic!("expected Predicate or And, got {:?}", other),
        }
    }

    #[test]
    fn arithmetic_comparison_is_accepted() {
        // (price + 10) > 100 — our old converter would reject this.
        let expr = (col("price") + lit(10i32)).gt(lit(100i32));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn non_boolean_expression_is_rejected() {
        // `price + 10` on its own is Int32, not Boolean → must error.
        let expr = col("price") + lit(10i32);
        let r = expr_to_bool_tree(&expr, &test_schema());
        assert!(r.is_err());
        let e = r.unwrap_err();
        assert!(e.contains("boolean"), "got: {}", e);
    }

    #[test]
    fn collector_function() {
        let udf = Arc::new(create_index_filter_udf());
        let expr = Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            udf,
            vec![lit(ScalarValue::Binary(Some(b"hello-query".to_vec())))],
        ));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        match r.tree {
            BoolNode::Collector { query_bytes } => {
                assert_eq!(&*query_bytes, b"hello-query");
            }
            _ => panic!("expected Collector"),
        }
    }

    #[test]
    fn mixed_tree() {
        // AND(collector(bytes), OR(price > 100, qty < 50))
        let udf = Arc::new(create_index_filter_udf());
        let collector_expr =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                udf,
                vec![lit(ScalarValue::Binary(Some(b"mixed".to_vec())))],
            ));
        let or_branch = col("price").gt(lit(100i32)).or(col("qty").lt(lit(50i32)));
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(collector_expr),
            Operator::And,
            Box::new(or_branch),
        ));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::And(_)));
    }

    // ── classify_filter ──────────────────────────────────────────────

    fn collector(tag: &[u8]) -> BoolNode {
        BoolNode::Collector {
            query_bytes: Arc::from(tag),
        }
    }
    fn dummy_predicate() -> BoolNode {
        // A stand-in Predicate leaf — classify only cares about shape,
        // not expression contents. Build a minimal boolean PhysicalExpr.
        let schema = test_schema();
        let col_idx = schema.index_of("price").unwrap();
        let left: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new("price", col_idx));
        let right: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(0))));
        BoolNode::Predicate(Arc::new(
            datafusion::physical_expr::expressions::BinaryExpr::new(left, Operator::Eq, right),
        ))
    }

    #[test]
    fn classify_no_collectors_is_none() {
        assert_eq!(classify_filter(&dummy_predicate()), FilterClass::None);
        assert_eq!(
            classify_filter(&BoolNode::And(vec![dummy_predicate(), dummy_predicate()])),
            FilterClass::None
        );
    }

    #[test]
    fn classify_bare_collector_is_single() {
        assert_eq!(
            classify_filter(&collector(b"x")),
            FilterClass::SingleCollector
        );
    }

    #[test]
    fn classify_and_of_collector_and_predicates_is_single() {
        let tree = BoolNode::And(vec![collector(b"x"), dummy_predicate(), dummy_predicate()]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_and_with_two_collectors_is_single() {
        // AND(C, C, P) — all collectors under AND-only path → SingleCollector.
        let tree = BoolNode::And(vec![collector(b"x"), collector(b"y"), dummy_predicate()]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_or_containing_collector_is_tree() {
        let tree = BoolNode::Or(vec![collector(b"x"), dummy_predicate()]);
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
            BoolNode::Or(vec![collector(b"x"), dummy_predicate()]),
            dummy_predicate(),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }

    // ── Nested AND shapes → SingleCollector ──────────────────────────

    #[test]
    fn classify_nested_and_collector_plus_predicate_is_single() {
        // AND(C₁, AND(C₂, P)) — nested AND, all collectors under AND-only path.
        let tree = BoolNode::And(vec![
            collector(b"x"),
            BoolNode::And(vec![collector(b"y"), dummy_predicate()]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_deeply_nested_and_is_single() {
        // AND(P, AND(C₁, AND(C₂, AND(C₃, P)))) — depth 4, all AND.
        let tree = BoolNode::And(vec![
            dummy_predicate(),
            BoolNode::And(vec![
                collector(b"a"),
                BoolNode::And(vec![
                    collector(b"b"),
                    BoolNode::And(vec![collector(b"c"), dummy_predicate()]),
                ]),
            ]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_nested_and_only_collectors_is_single() {
        // AND(AND(C₁, C₂), AND(C₃, C₄)) — nested AND of only collectors.
        let tree = BoolNode::And(vec![
            BoolNode::And(vec![collector(b"a"), collector(b"b")]),
            BoolNode::And(vec![collector(b"c"), collector(b"d")]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_nested_and_with_or_predicate_is_single() {
        // AND(C, AND(P, OR(P, P))) — OR contains only predicates, no collectors.
        let tree = BoolNode::And(vec![
            collector(b"x"),
            BoolNode::And(vec![
                dummy_predicate(),
                BoolNode::Or(vec![dummy_predicate(), dummy_predicate()]),
            ]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_nested_and_with_not_predicate_is_single() {
        // AND(C, NOT(P)) — NOT wraps a predicate, not a collector.
        let tree = BoolNode::And(vec![
            collector(b"x"),
            BoolNode::Not(Box::new(dummy_predicate())),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_nested_and_or_containing_collector_is_tree() {
        // AND(C₁, AND(OR(C₂, P), P)) — OR above C₂ → Tree.
        let tree = BoolNode::And(vec![
            collector(b"x"),
            BoolNode::And(vec![
                BoolNode::Or(vec![collector(b"y"), dummy_predicate()]),
                dummy_predicate(),
            ]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }

    #[test]
    fn classify_nested_and_not_containing_collector_is_tree() {
        // AND(C₁, AND(NOT(C₂), P)) — NOT above C₂ → Tree.
        let tree = BoolNode::And(vec![
            collector(b"x"),
            BoolNode::And(vec![
                BoolNode::Not(Box::new(collector(b"y"))),
                dummy_predicate(),
            ]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }
}
