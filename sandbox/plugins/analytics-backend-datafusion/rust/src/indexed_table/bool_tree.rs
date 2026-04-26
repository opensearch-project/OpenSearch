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
//! serialized, never crosses the FFM boundary. Predicates live in the
//! substrait plan; we only hold lightweight references to them by index.
//!
//! Two flavors:
//!
//! - [`BoolNode`] — unresolved. Produced by `expr_to_bool_tree`.
//!   `Collector` leaves carry the serialized query bytes
//!   (as extracted from the `index_filter(bytes)` UDF call);
//!   `Predicate` leaves carry an index into a sidecar `Vec<ResolvedPredicate>`.
//! - [`ResolvedNode`] — resolved. Produced by `BoolNode::resolve(collectors, predicates)`
//!   — `Collector` leaves get turned into `(provider_key, Arc<dyn
//!   RowGroupDocsCollector>)` pairs by the caller (who upcalls Java to
//!   build a provider from the query bytes). Predicate refs expanded to
//!   `(column, op, value)`. This is what the evaluator walks.

use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;

use super::index::RowGroupDocsCollector;

/// A node in the boolean query tree (unresolved).
#[derive(Debug, Clone)]
pub enum BoolNode {
    And(Vec<BoolNode>),
    Or(Vec<BoolNode>),
    Not(Box<BoolNode>),
    /// index-backend query payload. The caller (typically the indexed_executor)
    /// upcalls into Java with these bytes at query-resolve time to get a
    /// `provider_key`, then creates per-segment collectors. Bytes are opaque
    /// to Rust; only the Java factory knows how to interpret them.
    Collector { query_bytes: Arc<[u8]> },
    /// `predicate_id` indexes into the `Vec<ResolvedPredicate>` sidecar from
    /// `expr_to_bool_tree`. Rust resolves column/op/value — they're never in
    /// the wire format.
    Predicate { predicate_id: u16 },
}

/// A resolved predicate extracted from the Substrait plan.
#[derive(Debug, Clone)]
pub struct ResolvedPredicate {
    pub column: String,
    pub op: Operator,
    pub value: ScalarValue,
}

/// Resolved tree. `Collector` leaves carry the provider-key returned by the
/// Java factory plus the concrete collector reference; the evaluator walks this.
#[derive(Debug)]
pub enum ResolvedNode {
    And(Vec<ResolvedNode>),
    Or(Vec<ResolvedNode>),
    Not(Box<ResolvedNode>),
    Collector {
        provider_key: i32,
        collector: Arc<dyn RowGroupDocsCollector>,
    },
    Predicate {
        /// Resolved predicate — shared via Arc so per-chunk `resolve()` calls
        /// don't clone `String` + `ScalarValue` once per Predicate leaf. The
        /// predicate contents are query-scoped (don't vary per chunk); only
        /// Collector leaves carry chunk-scoped state.
        pred: Arc<ResolvedPredicate>,
    },
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
            BoolNode::Predicate { .. } => 0,
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
    pub fn collector_leaves(&self) -> Vec<Arc<[u8]>> {
        let mut out = Vec::new();
        self.collect_leaves(&mut out);
        out
    }

    fn collect_leaves(&self, out: &mut Vec<Arc<[u8]>>) {
        match self {
            BoolNode::And(children) | BoolNode::Or(children) => {
                for c in children {
                    c.collect_leaves(out);
                }
            }
            BoolNode::Not(child) => child.collect_leaves(out),
            BoolNode::Collector { query_bytes } => {
                out.push(Arc::clone(query_bytes));
            }
            BoolNode::Predicate { .. } => {}
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
    /// upcalling Java `createProvider(query_bytes)` per leaf to get a
    /// `provider_key`, then `createCollector(provider_key, seg, min, max)`
    /// per chunk.
    ///
    /// # Ordering invariant
    ///
    /// The `collectors` slice is consumed positionally; its order must match
    /// the DFS order produced by [`Self::collector_leaves`]. See that method
    /// for the traversal contract. A mismatch causes collector-to-leaf
    /// misalignment with no runtime error — wrong data, silent.
    ///
    /// `predicates` is taken as `&[Arc<ResolvedPredicate>]` so per-chunk
    /// resolves share the same predicate instances across chunks — the
    /// per-chunk work is a cheap `Arc::clone` per Predicate leaf, not a
    /// `String` + `ScalarValue` deep clone.
    pub fn resolve(
        &self,
        collectors: &[(i32, Arc<dyn RowGroupDocsCollector>)],
        predicates: &[Arc<ResolvedPredicate>],
    ) -> Result<ResolvedNode, String> {
        let mut next = 0usize;
        self.resolve_rec(collectors, predicates, &mut next)
    }

    fn resolve_rec(
        &self,
        collectors: &[(i32, Arc<dyn RowGroupDocsCollector>)],
        predicates: &[Arc<ResolvedPredicate>],
        next: &mut usize,
    ) -> Result<ResolvedNode, String> {
        match self {
            BoolNode::And(children) => {
                let resolved: Result<Vec<_>, _> = children
                    .iter()
                    .map(|c| c.resolve_rec(collectors, predicates, next))
                    .collect();
                Ok(ResolvedNode::And(resolved?))
            }
            BoolNode::Or(children) => {
                let resolved: Result<Vec<_>, _> = children
                    .iter()
                    .map(|c| c.resolve_rec(collectors, predicates, next))
                    .collect();
                Ok(ResolvedNode::Or(resolved?))
            }
            // Fast-path: Not directly above a Predicate leaf folds into the
            // Predicate by flipping its operator. Saves one `not()` kernel
            // call per batch in the refinement stage and one universe
            // subtraction per RG in the candidate stage. Relies on the tree
            // having been `push_not_down()`ed already so Not-above-Predicate
            // is the only way a Not can wrap a leaf.
            BoolNode::Not(inner) if matches!(**inner, BoolNode::Predicate { .. }) => {
                if let BoolNode::Predicate { predicate_id } = **inner {
                    let pred = predicates
                        .get(predicate_id as usize)
                        .ok_or_else(|| format!("predicate_id {} out of range", predicate_id))?;
                    let flipped = invert_predicate_op(pred);
                    match flipped {
                        Some(p) => Ok(ResolvedNode::Predicate { pred: p }),
                        // Op doesn't have a clean inversion (e.g. Like,
                        // IsNull) — fall back to wrapping in Not so the
                        // evaluator's kernel-level not() kicks in.
                        None => Ok(ResolvedNode::Not(Box::new(
                            ResolvedNode::Predicate { pred: Arc::clone(pred) },
                        ))),
                    }
                } else {
                    unreachable!("guard above ensures inner is Predicate")
                }
            }
            BoolNode::Not(child) => Ok(ResolvedNode::Not(Box::new(
                child.resolve_rec(collectors, predicates, next)?,
            ))),
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
            BoolNode::Predicate { predicate_id } => {
                let pred = predicates
                    .get(*predicate_id as usize)
                    .ok_or_else(|| format!("predicate_id {} out of range", predicate_id))?;
                Ok(ResolvedNode::Predicate { pred: Arc::clone(pred) })
            }
        }
    }
}

/// Return a flipped copy of `pred` representing the logical NOT of the
/// original comparison, or `None` if the op has no straightforward inverse.
///
/// Used by `resolve_rec` when it encounters `Not(Predicate)` (which the
/// `push_not_down` pass normalized to be the only remaining position of
/// `Not` above a Predicate leaf). Folding the negation into the op lets the
/// candidate + refinement stages evaluate one predicate per leaf instead of
/// (predicate + `not()`).
///
/// Value field is `Arc::clone`d — unlike the op flip, the value itself
/// isn't affected by negation.
fn invert_predicate_op(pred: &Arc<ResolvedPredicate>) -> Option<Arc<ResolvedPredicate>> {
    let flipped_op = match pred.op {
        Operator::Lt => Operator::GtEq,
        Operator::LtEq => Operator::Gt,
        Operator::Gt => Operator::LtEq,
        Operator::GtEq => Operator::Lt,
        Operator::Eq => Operator::NotEq,
        Operator::NotEq => Operator::Eq,
        _ => return None,
    };
    Some(Arc::new(ResolvedPredicate {
        column: pred.column.clone(),
        op: flipped_op,
        value: pred.value.clone(),
    }))
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

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn collector(bytes: &[u8]) -> BoolNode {
        BoolNode::Collector {
            query_bytes: Arc::from(bytes),
        }
    }
    fn predicate(id: u16) -> BoolNode {
        BoolNode::Predicate { predicate_id: id }
    }

    // ── collector_leaf_count / collector_leaves ───────────────────────

    #[test]
    fn leaf_count_counts_only_collectors() {
        let tree = BoolNode::And(vec![
            collector(b"a"),
            BoolNode::Or(vec![collector(b"b"), predicate(0)]),
            predicate(1),
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
        let tree = BoolNode::Not(Box::new(BoolNode::Or(vec![predicate(0), predicate(1)])));
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
        // AND(AND(a, b), c) → AND(a, b, c)
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
        // OR(a, OR(b, OR(c, d))) → OR(a, b, c, d)
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
        // AND(a, OR(b, c), AND(d, e)) → AND(a, OR(b, c), d, e)
        // (only same-kind nests collapse; Or inside And stays an Or child)
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
    fn flatten_is_idempotent() {
        let tree = BoolNode::And(vec![
            BoolNode::And(vec![collector(b"a"), collector(b"b")]),
            collector(b"c"),
        ]);
        let once = tree.flatten();
        let twice = once.clone().flatten();
        // Both should be flat Ands with 3 children.
        match (once, twice) {
            (BoolNode::And(a), BoolNode::And(b)) => assert_eq!(a.len(), b.len()),
            _ => panic!("expected And on both"),
        }
    }

    #[test]
    fn flatten_descends_into_not() {
        // NOT(AND(AND(a, b), c)) → NOT(AND(a, b, c))
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

    use crate::indexed_table::index::RowGroupDocsCollector;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;

    #[derive(Debug)]
    struct StubCollector(u8);
    impl RowGroupDocsCollector for StubCollector {
        fn collect_packed_u64_bitset(&self, _: i32, _: i32) -> Result<Vec<u64>, String> {
            Ok(vec![self.0 as u64])
        }
    }

    fn stub_pred(col: &str) -> ResolvedPredicate {
        ResolvedPredicate {
            column: col.into(),
            op: Operator::Eq,
            value: ScalarValue::Int32(Some(42)),
        }
    }

    #[test]
    fn resolve_replaces_collector_bytes_with_refs() {
        let tree = BoolNode::And(vec![collector(b"a"), collector(b"b")]);
        let a: Arc<dyn RowGroupDocsCollector> = Arc::new(StubCollector(1));
        let b: Arc<dyn RowGroupDocsCollector> = Arc::new(StubCollector(2));
        let resolved = tree.resolve(&[(10, a), (20, b)], &[]).unwrap();
        match resolved {
            ResolvedNode::And(children) => {
                assert_eq!(children.len(), 2);
                match (&children[0], &children[1]) {
                    (
                        ResolvedNode::Collector { provider_key: p1, .. },
                        ResolvedNode::Collector { provider_key: p2, .. },
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
    fn resolve_expands_predicate_refs() {
        let tree = predicate(0);
        let preds = vec![Arc::new(stub_pred("status"))];
        let resolved = tree.resolve(&[], &preds).unwrap();
        match resolved {
            ResolvedNode::Predicate { pred } => {
                assert_eq!(pred.column, "status");
                assert_eq!(pred.op, Operator::Eq);
            }
            other => panic!("expected Predicate, got {:?}", other),
        }
    }

    #[test]
    fn resolve_out_of_range_errors() {
        let tree = collector(b"x");
        let err = tree.resolve(&[], &[]).unwrap_err();
        assert!(err.contains("out of range"), "got: {}", err);
    }

    // ── Not(Predicate) op-flip during resolve ─────────────────────────

    #[test]
    fn resolve_not_predicate_flips_op() {
        // Not(price > 10) should resolve to price <= 10, not
        // Not(Predicate(price > 10)).
        let tree = BoolNode::Not(Box::new(predicate(0)));
        let preds = vec![Arc::new(ResolvedPredicate {
            column: "price".into(),
            op: Operator::Gt,
            value: ScalarValue::Int32(Some(10)),
        })];
        let resolved = tree.resolve(&[], &preds).unwrap();
        match resolved {
            ResolvedNode::Predicate { pred } => {
                assert_eq!(pred.column, "price");
                assert_eq!(pred.op, Operator::LtEq);
            }
            other => panic!("expected flipped Predicate, got {:?}", other),
        }
    }

    #[test]
    fn resolve_not_predicate_flip_table() {
        // Each row: original op → flipped op.
        let cases = [
            (Operator::Lt, Operator::GtEq),
            (Operator::LtEq, Operator::Gt),
            (Operator::Gt, Operator::LtEq),
            (Operator::GtEq, Operator::Lt),
            (Operator::Eq, Operator::NotEq),
            (Operator::NotEq, Operator::Eq),
        ];
        for (orig, expected) in cases {
            let tree = BoolNode::Not(Box::new(predicate(0)));
            let preds = vec![Arc::new(ResolvedPredicate {
                column: "x".into(),
                op: orig,
                value: ScalarValue::Int32(Some(0)),
            })];
            let resolved = tree.resolve(&[], &preds).unwrap();
            match resolved {
                ResolvedNode::Predicate { pred } => {
                    assert_eq!(pred.op, expected, "flipping {:?}", orig);
                }
                _ => panic!("expected flipped Predicate for op {:?}", orig),
            }
        }
    }

    #[test]
    fn resolve_not_collector_still_wraps() {
        // Collectors can't be inverted (backend query bytes are opaque);
        // Not(Collector) must stay wrapped for the evaluator to handle.
        let tree = BoolNode::Not(Box::new(collector(b"x")));
        let c: Arc<dyn RowGroupDocsCollector> = Arc::new(StubCollector(0));
        let resolved = tree.resolve(&[(1, c)], &[]).unwrap();
        match resolved {
            ResolvedNode::Not(inner) => {
                assert!(matches!(*inner, ResolvedNode::Collector { .. }));
            }
            other => panic!("expected Not(Collector), got {:?}", other),
        }
    }
}
