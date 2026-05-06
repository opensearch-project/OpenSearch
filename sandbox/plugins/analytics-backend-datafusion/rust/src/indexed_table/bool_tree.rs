/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Boolean query tree representation.
//!
//! **No wire format.** The tree is built from the Substrait plan's filter
//! expression (see [`crate::indexed_table::substrait_to_tree`]), never
//! serialized, never crosses the FFM boundary.
//!
//! Two flavors:
//!
//! - [`BoolNode`] — unresolved. Produced by `expr_to_bool_tree`.
//!   `Collector` leaves carry the annotation ID identifying the delegated predicate
//!   (as extracted from the `index_filter(bytes)` UDF call);
//!   `Predicate` leaves carry an arbitrary DataFusion
//!   [`PhysicalExpr`](datafusion::physical_expr::PhysicalExpr) —
//!   comparisons, IN, IS NULL, arithmetic, whatever produces a boolean.
//! - [`ResolvedNode`] — resolved. Produced by
//!   `BoolNode::resolve(collectors)` — `Collector` leaves get turned
//!   into `(provider_key, Arc<dyn RowGroupDocsCollector>)` pairs by the
//!   caller; Predicate leaves pass through unchanged. This is what the
//!   evaluator walks.

use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;

use super::index::RowGroupDocsCollector;

/// A node in the boolean query tree (unresolved).
#[derive(Debug, Clone)]
pub enum BoolNode {
    And(Vec<BoolNode>),
    Or(Vec<BoolNode>),
    Not(Box<BoolNode>),
    /// Delegated predicate identified by annotation ID. At query-resolve time,
    /// the indexed executor upcalls into Java with this ID to get a `provider_key`,
    /// then creates per-segment collectors. The annotation ID maps to a pre-compiled
    /// query on the Java side (via FilterDelegationHandle).
    Collector {
        annotation_id: i32,
    },
    /// Arbitrary boolean-valued DataFusion expression. At refinement
    /// time, `expr.evaluate(batch)` produces the per-row mask; at page-
    /// prune time, the expression is handed to DataFusion's
    /// `PruningPredicate` directly.
    Predicate(Arc<dyn PhysicalExpr>),
}

/// Resolved tree. `Collector` leaves carry the provider-key returned by the
/// Java factory plus the concrete collector reference; `Predicate` leaves
/// carry the same `Arc<dyn PhysicalExpr>` as [`BoolNode::Predicate`].
#[derive(Debug)]
pub enum ResolvedNode {
    And(Vec<ResolvedNode>),
    Or(Vec<ResolvedNode>),
    Not(Box<ResolvedNode>),
    Collector {
        provider_key: i32,
        collector: Arc<dyn RowGroupDocsCollector>,
    },
    Predicate(Arc<dyn PhysicalExpr>),
}

impl BoolNode {
    /// Count `Collector` leaf occurrences in the tree (DFS).
    pub fn collector_leaf_count(&self) -> usize {
        match self {
            BoolNode::And(children) | BoolNode::Or(children) => {
                children.iter().map(|c| c.collector_leaf_count()).sum()
            }
            BoolNode::Not(child) => child.collector_leaf_count(),
            BoolNode::Collector { .. } => 1,
            BoolNode::Predicate(_) => 0,
        }
    }

    /// Return the serialized query bytes for each `Collector` leaf in DFS order.
    /// Caller uses this to issue one `createProvider(bytes)` upcall per leaf.
    ///
    /// # Ordering invariant
    ///
    /// This method MUST walk children in the same order as
    /// [`Self::resolve`] consumes them. Both visit And/Or children left-to-
    /// right, recurse into Not, then yield leaves. The positional pairing in
    /// `resolve` (via the `*next` index) relies on this invariant; if you
    /// change one traversal you MUST change the other in lockstep, or
    /// collector-to-leaf matching will silently become wrong.
    pub fn collector_leaves(&self) -> Vec<i32> {
        let mut out = Vec::new();
        self.collect_leaves(&mut out);
        out
    }

    fn collect_leaves(&self, out: &mut Vec<i32>) {
        match self {
            BoolNode::And(children) | BoolNode::Or(children) => {
                for c in children {
                    c.collect_leaves(out);
                }
            }
            BoolNode::Not(child) => child.collect_leaves(out),
            BoolNode::Collector { annotation_id } => {
                out.push(*annotation_id);
            }
            BoolNode::Predicate(_) => {}
        }
    }

    /// De Morgan's NOT push-down normalization.
    /// After this, `Not` only appears directly above `Collector` or `Predicate` leaves.
    pub fn push_not_down(self) -> BoolNode {
        match self {
            BoolNode::And(children) => {
                BoolNode::And(children.into_iter().map(|c| c.push_not_down()).collect())
            }
            BoolNode::Or(children) => {
                BoolNode::Or(children.into_iter().map(|c| c.push_not_down()).collect())
            }
            BoolNode::Not(child) => push_not_into(*child),
            leaf => leaf,
        }
    }

    /// Collapse nested same-kind connectives:
    /// `And(And(x, y), z)` → `And(x, y, z)`, similarly for `Or`.
    ///
    /// Substrait decodes N-ary AND/OR as left-deep binary trees. Flattening
    /// cuts evaluator recursion depth and lets Path C allocate one Phase 1
    /// bitmap per conceptual child instead of one per binary split.
    /// Idempotent and semantic-preserving.
    pub fn flatten(self) -> BoolNode {
        match self {
            BoolNode::And(children) => {
                let mut out = Vec::with_capacity(children.len());
                for c in children {
                    match c.flatten() {
                        BoolNode::And(inner) => out.extend(inner),
                        other => out.push(other),
                    }
                }
                BoolNode::And(out)
            }
            BoolNode::Or(children) => {
                let mut out = Vec::with_capacity(children.len());
                for c in children {
                    match c.flatten() {
                        BoolNode::Or(inner) => out.extend(inner),
                        other => out.push(other),
                    }
                }
                BoolNode::Or(out)
            }
            BoolNode::Not(child) => BoolNode::Not(Box::new(child.flatten())),
            leaf => leaf,
        }
    }

    /// Resolve the tree: walk in DFS order, consuming pre-built `(provider_key,
    /// collector)` pairs (one per `Collector` leaf, same DFS order as
    /// [`Self::collector_leaves`]) and expanding `Predicate` IDs into
    /// `(column, op, value)`.
    ///
    /// Caller is responsible for creating the collectors — typically by
    /// upcalling Java `createProvider(annotation_id)` per leaf to get a
    /// `provider_key`, then `createCollector(provider_key, seg, min, max)`
    /// per chunk.
    ///
    /// # Ordering invariant
    ///
    /// The `collectors` slice is consumed positionally; its order must match
    /// the DFS order produced by [`Self::collector_leaves`]. See that method
    /// for the traversal contract. A mismatch causes collector-to-leaf
    /// misalignment with no runtime error — wrong data, silent.
    pub fn resolve(
        &self,
        collectors: &[(i32, Arc<dyn RowGroupDocsCollector>)],
    ) -> Result<ResolvedNode, String> {
        let mut next = 0usize;
        self.resolve_rec(collectors, &mut next)
    }

    fn resolve_rec(
        &self,
        collectors: &[(i32, Arc<dyn RowGroupDocsCollector>)],
        next: &mut usize,
    ) -> Result<ResolvedNode, String> {
        match self {
            BoolNode::And(children) => {
                let resolved: Result<Vec<_>, _> = children
                    .iter()
                    .map(|c| c.resolve_rec(collectors, next))
                    .collect();
                Ok(ResolvedNode::And(resolved?))
            }
            BoolNode::Or(children) => {
                let resolved: Result<Vec<_>, _> = children
                    .iter()
                    .map(|c| c.resolve_rec(collectors, next))
                    .collect();
                Ok(ResolvedNode::Or(resolved?))
            }
            BoolNode::Not(child) => {
                let resolved_child = child.resolve_rec(collectors, next)?;
                // Fast-path: NOT over a `Predicate(col op literal)` folds
                // into `Predicate(col flipped_op literal)`. Saves one
                // kleene-`not()` kernel per batch in the refinement stage
                // and one universe subtraction per RG in the candidate
                // stage. Falls back to wrapping `Not` when the child
                // isn't a recognizable comparison.
                match resolved_child {
                    ResolvedNode::Predicate(ref expr) => match try_negate_cmp_expr(expr) {
                        Some(flipped) => Ok(ResolvedNode::Predicate(flipped)),
                        None => Ok(ResolvedNode::Not(Box::new(ResolvedNode::Predicate(
                            Arc::clone(expr),
                        )))),
                    },
                    other => Ok(ResolvedNode::Not(Box::new(other))),
                }
            }
            BoolNode::Collector { .. } => {
                let (provider_key, collector) = collectors
                    .get(*next)
                    .ok_or_else(|| format!("collector index {} out of range", *next))?;
                *next += 1;
                Ok(ResolvedNode::Collector {
                    provider_key: *provider_key,
                    collector: Arc::clone(collector),
                })
            }
            BoolNode::Predicate(expr) => Ok(ResolvedNode::Predicate(Arc::clone(expr))),
        }
    }
}

/// If `expr` is a `BinaryExpr(col, cmp, literal)` with an invertible
/// comparison operator, return the same expression with the operator
/// negated. Otherwise `None`.
///
/// Used by `BoolNode::resolve_rec` to fold `Not(Predicate(cmp))` into a
/// single flipped `Predicate` so the refinement stage doesn't have to
/// call `not_kleene()` per batch.
fn try_negate_cmp_expr(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
) -> Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::BinaryExpr;

    let bin = expr.as_any().downcast_ref::<BinaryExpr>()?;
    let flipped = match *bin.op() {
        Operator::Eq => Operator::NotEq,
        Operator::NotEq => Operator::Eq,
        Operator::Lt => Operator::GtEq,
        Operator::LtEq => Operator::Gt,
        Operator::Gt => Operator::LtEq,
        Operator::GtEq => Operator::Lt,
        _ => return None,
    };
    Some(Arc::new(BinaryExpr::new(
        Arc::clone(bin.left()),
        flipped,
        Arc::clone(bin.right()),
    )))
}

fn push_not_into(child: BoolNode) -> BoolNode {
    match child {
        // De Morgan's: NOT(AND(a, b, ...)) → OR(NOT(a), NOT(b), ...)
        BoolNode::And(children) => {
            BoolNode::Or(children.into_iter().map(push_not_into).collect()).push_not_down()
        }
        // De Morgan's: NOT(OR(a, b, ...)) → AND(NOT(a), NOT(b), ...)
        BoolNode::Or(children) => {
            BoolNode::And(children.into_iter().map(push_not_into).collect()).push_not_down()
        }
        // Double negation
        BoolNode::Not(inner) => inner.push_not_down(),
        // NOT(Collector) / NOT(Predicate) — stay wrapped; evaluator handles the negation
        leaf => BoolNode::Not(Box::new(leaf)),
    }
}

/// Convert a Collector-free `BoolNode` (the residual of a
/// `SingleCollector`-classified tree, or any subtree guaranteed to
/// have no `Collector` leaves) into a single
/// `Arc<dyn PhysicalExpr>` suitable for parquet's `with_predicate`
/// pushdown or DataFusion's `Expr::evaluate(batch)`.
///
/// Contrast with `page_pruner::bool_tree_to_pruning_expr`:
/// - That helper replaces `Collector` leaves with `Literal(true)` so
///   the result can feed DataFusion's `PruningPredicate` rewriter
///   (which evaluates only against per-page stats, not cell values).
/// - This helper assumes no Collectors are present (appropriate for
///   a SingleCollector residual). Returns `None` if a Collector is
///   encountered (shouldn't happen for a well-formed residual).
///
/// NOT handling: emits `NotExpr`. Callers that need De Morgan
/// normalization should `push_not_down` first.
pub fn residual_bool_to_physical_expr(
    node: &BoolNode,
) -> Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, NotExpr};

    match node {
        BoolNode::Predicate(expr) => Some(Arc::clone(expr)),
        BoolNode::And(children) => {
            if children.is_empty() {
                return None;
            }
            let mut iter = children.iter();
            let mut acc = residual_bool_to_physical_expr(iter.next().unwrap())?;
            for c in iter {
                let child = residual_bool_to_physical_expr(c)?;
                acc = Arc::new(BinaryExpr::new(acc, Operator::And, child));
            }
            Some(acc)
        }
        BoolNode::Or(children) => {
            if children.is_empty() {
                return None;
            }
            let mut iter = children.iter();
            let mut acc = residual_bool_to_physical_expr(iter.next().unwrap())?;
            for c in iter {
                let child = residual_bool_to_physical_expr(c)?;
                acc = Arc::new(BinaryExpr::new(acc, Operator::Or, child));
            }
            Some(acc)
        }
        BoolNode::Not(child) => {
            let inner = residual_bool_to_physical_expr(child)?;
            Some(Arc::new(NotExpr::new(inner)))
        }
        BoolNode::Collector { .. } => None,
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::index::RowGroupDocsCollector;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
    use datafusion::physical_expr::PhysicalExpr;

    #[derive(Debug)]
    struct StubCollector(u8);
    impl RowGroupDocsCollector for StubCollector {
        fn collect_packed_u64_bitset(&self, _: i32, _: i32) -> Result<Vec<u64>, String> {
            Ok(vec![self.0 as u64])
        }
    }

    fn collector(id: i32) -> BoolNode {
        BoolNode::Collector {
            annotation_id: id,
        }
    }

    fn predicate(col: &str, op: Operator, v: i32) -> BoolNode {
        let schema = Schema::new(vec![Field::new(col, DataType::Int32, false)]);
        let col_idx = schema.index_of(col).unwrap();
        let left: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new(col, col_idx));
        let right: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(v))));
        BoolNode::Predicate(Arc::new(BinaryExpr::new(left, op, right)))
    }

    // ── collector_leaf_count / collector_leaves ───────────────────────

    #[test]
    fn leaf_count_counts_only_collectors() {
        let tree = BoolNode::And(vec![
            collector(b"a"),
            BoolNode::Or(vec![collector(b"b"), predicate("x", Operator::Eq, 1)]),
            predicate("y", Operator::Eq, 2),
        ]);
        assert_eq!(tree.collector_leaf_count(), 2);
    }

    #[test]
    fn leaves_dfs_order() {
        let tree = BoolNode::And(vec![
            collector(b"x"),
            BoolNode::Or(vec![collector(b"y"), collector(b"z")]),
        ]);
        let leaves = tree.collector_leaves();
        assert_eq!(leaves.len(), 3);
        assert_eq!(&*leaves[0], b"x");
        assert_eq!(&*leaves[1], b"y");
        assert_eq!(&*leaves[2], b"z");
    }

    // ── push_not_down (De Morgan) ─────────────────────────────────────

    #[test]
    fn not_collector_stays_wrapped() {
        let tree = BoolNode::Not(Box::new(collector(b"x")));
        let n = tree.push_not_down();
        assert!(matches!(n, BoolNode::Not(b) if matches!(*b, BoolNode::Collector { .. })));
    }

    #[test]
    fn de_morgan_not_and_to_or() {
        let tree = BoolNode::Not(Box::new(BoolNode::And(vec![
            collector(b"a"),
            collector(b"b"),
        ])));
        match tree.push_not_down() {
            BoolNode::Or(children) => {
                assert_eq!(children.len(), 2);
                for c in &children {
                    assert!(matches!(c, BoolNode::Not(_)));
                }
            }
            other => panic!("expected Or, got {:?}", other),
        }
    }

    #[test]
    fn de_morgan_not_or_to_and() {
        let tree = BoolNode::Not(Box::new(BoolNode::Or(vec![
            predicate("a", Operator::Eq, 1),
            predicate("b", Operator::Eq, 2),
        ])));
        match tree.push_not_down() {
            BoolNode::And(children) => {
                assert_eq!(children.len(), 2);
                for c in &children {
                    assert!(matches!(c, BoolNode::Not(_)));
                }
            }
            other => panic!("expected And, got {:?}", other),
        }
    }

    #[test]
    fn double_negation_cancels() {
        let tree = BoolNode::Not(Box::new(BoolNode::Not(Box::new(collector(b"x")))));
        let n = tree.push_not_down();
        assert!(matches!(n, BoolNode::Collector { .. }));
    }

    #[test]
    fn nested_not_recurses_through_and_or() {
        let tree = BoolNode::Not(Box::new(BoolNode::And(vec![
            BoolNode::Or(vec![collector(b"a"), collector(b"b")]),
            collector(b"c"),
        ])));
        match tree.push_not_down() {
            BoolNode::Or(outer) => {
                assert_eq!(outer.len(), 2);
                assert!(matches!(outer[0], BoolNode::And(_)));
                assert!(matches!(outer[1], BoolNode::Not(_)));
            }
            other => panic!("expected Or, got {:?}", other),
        }
    }

    // ── flatten ───────────────────────────────────────────────────────

    #[test]
    fn flatten_collapses_nested_and() {
        let tree = BoolNode::And(vec![
            BoolNode::And(vec![collector(b"a"), collector(b"b")]),
            collector(b"c"),
        ]);
        match tree.flatten() {
            BoolNode::And(children) => {
                assert_eq!(children.len(), 3);
                for c in &children {
                    assert!(matches!(c, BoolNode::Collector { .. }));
                }
            }
            other => panic!("expected flat And with 3 children, got {:?}", other),
        }
    }

    #[test]
    fn flatten_collapses_nested_or() {
        let tree = BoolNode::Or(vec![
            collector(b"a"),
            BoolNode::Or(vec![
                collector(b"b"),
                BoolNode::Or(vec![collector(b"c"), collector(b"d")]),
            ]),
        ]);
        match tree.flatten() {
            BoolNode::Or(children) => assert_eq!(children.len(), 4),
            other => panic!("expected flat Or with 4 children, got {:?}", other),
        }
    }

    #[test]
    fn flatten_preserves_mixed_connectives() {
        let tree = BoolNode::And(vec![
            collector(b"a"),
            BoolNode::Or(vec![collector(b"b"), collector(b"c")]),
            BoolNode::And(vec![collector(b"d"), collector(b"e")]),
        ]);
        match tree.flatten() {
            BoolNode::And(children) => {
                assert_eq!(children.len(), 4);
                assert!(matches!(children[1], BoolNode::Or(_)));
            }
            other => panic!("expected And with 4 children, got {:?}", other),
        }
    }

    #[test]
    fn flatten_descends_into_not() {
        let tree = BoolNode::Not(Box::new(BoolNode::And(vec![
            BoolNode::And(vec![collector(b"a"), collector(b"b")]),
            collector(b"c"),
        ])));
        match tree.flatten() {
            BoolNode::Not(inner) => match *inner {
                BoolNode::And(children) => assert_eq!(children.len(), 3),
                other => panic!("expected And under Not, got {:?}", other),
            },
            other => panic!("expected Not, got {:?}", other),
        }
    }

    // ── resolve ────────────────────────────────────────────────────────

    #[test]
    fn resolve_replaces_collector_bytes_with_refs() {
        let tree = BoolNode::And(vec![collector(b"a"), collector(b"b")]);
        let a: Arc<dyn RowGroupDocsCollector> = Arc::new(StubCollector(1));
        let b: Arc<dyn RowGroupDocsCollector> = Arc::new(StubCollector(2));
        let resolved = tree.resolve(&[(10, a), (20, b)]).unwrap();
        match resolved {
            ResolvedNode::And(children) => {
                assert_eq!(children.len(), 2);
                match (&children[0], &children[1]) {
                    (
                        ResolvedNode::Collector {
                            provider_key: p1, ..
                        },
                        ResolvedNode::Collector {
                            provider_key: p2, ..
                        },
                    ) => {
                        assert_eq!(*p1, 10);
                        assert_eq!(*p2, 20);
                    }
                    _ => panic!("expected Collector pair"),
                }
            }
            other => panic!("expected And, got {:?}", other),
        }
    }

    #[test]
    fn resolve_passes_predicate_expr_through() {
        let tree = predicate("status", Operator::Eq, 1);
        let resolved = tree.resolve(&[]).unwrap();
        assert!(matches!(resolved, ResolvedNode::Predicate(_)));
    }

    #[test]
    fn resolve_out_of_range_errors() {
        let tree = collector(b"x");
        let err = tree.resolve(&[]).unwrap_err();
        assert!(err.contains("out of range"), "got: {}", err);
    }

    #[test]
    fn resolve_not_collector_still_wraps() {
        let tree = BoolNode::Not(Box::new(collector(b"x")));
        let c: Arc<dyn RowGroupDocsCollector> = Arc::new(StubCollector(0));
        let resolved = tree.resolve(&[(1, c)]).unwrap();
        match resolved {
            ResolvedNode::Not(inner) => {
                assert!(matches!(*inner, ResolvedNode::Collector { .. }));
            }
            other => panic!("expected Not(Collector), got {:?}", other),
        }
    }

    // ── Not(Predicate) op-flip during resolve ─────────────────────────

    /// Extract `(op)` from a `ResolvedNode::Predicate` whose child is a
    /// `BinaryExpr(col, op, literal)`. Panics otherwise.
    fn predicate_op(node: &ResolvedNode) -> Operator {
        use datafusion::physical_expr::expressions::BinaryExpr;
        match node {
            ResolvedNode::Predicate(expr) => {
                let bin = expr
                    .as_any()
                    .downcast_ref::<BinaryExpr>()
                    .expect("expected BinaryExpr leaf");
                *bin.op()
            }
            other => panic!("expected Predicate, got {:?}", other),
        }
    }

    #[test]
    fn resolve_not_predicate_flips_op() {
        // Not(price > 10) should resolve to price <= 10, not
        // Not(Predicate(price > 10)).
        let tree = BoolNode::Not(Box::new(predicate("price", Operator::Gt, 10)));
        let resolved = tree.resolve(&[]).unwrap();
        assert_eq!(predicate_op(&resolved), Operator::LtEq);
    }

    #[test]
    fn resolve_not_predicate_flip_table() {
        let cases = [
            (Operator::Lt, Operator::GtEq),
            (Operator::LtEq, Operator::Gt),
            (Operator::Gt, Operator::LtEq),
            (Operator::GtEq, Operator::Lt),
            (Operator::Eq, Operator::NotEq),
            (Operator::NotEq, Operator::Eq),
        ];
        for (orig, expected) in cases {
            let tree = BoolNode::Not(Box::new(predicate("x", orig, 0)));
            let resolved = tree.resolve(&[]).unwrap();
            assert_eq!(predicate_op(&resolved), expected, "flipping {:?}", orig);
        }
    }
}
