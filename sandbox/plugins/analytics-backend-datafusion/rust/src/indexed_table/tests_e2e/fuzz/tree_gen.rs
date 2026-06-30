/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Random `BoolNode` tree generation for the fuzz suite.
//!
//! Given a schema + a Collector-leaf id space (0..num_collector_leaves),
//! produces a random tree bounded by `tree_max_depth` and
//! `tree_max_fanout`. Every AND/OR/NOT/Collector/Predicate shape gets
//! exercised.
//!
//! The returned tree uses tagged Collector leaves: `query_bytes = [id]`
//! where `id` is a distinct u8 in `[0, num_collector_leaves)`. The
//! harness wires each tagged leaf to a pre-computed doc-id set (via
//! `MockCollector`), and the oracle uses the same id-to-set map.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::Rng;

use super::corpus::{CellValue, Corpus};
use crate::indexed_table::bool_tree::BoolNode;

/// What the tree-gen exposes to the harness: the tree itself plus a
/// side-table mapping each Collector leaf id (u8) to the set of
/// matching doc ids the oracle and mock collector will replay.
pub(in crate::indexed_table::tests_e2e) struct GeneratedTree {
    pub tree: BoolNode,
    /// `collector_matches[leaf_id] = Vec<doc_id>`. The matching doc ids
    /// are drawn from `0..corpus.num_rows()` at the configured density.
    pub collector_matches: Vec<Vec<i32>>,
}

/// Generate a random tree + collector-match sets against the given
/// corpus. Deterministic given the rng state.
pub(in crate::indexed_table::tests_e2e) fn generate_tree(
    rng: &mut StdRng,
    corpus: &Corpus,
) -> GeneratedTree {
    let schema = corpus.schema.clone();
    let num_collectors = corpus.config.num_collector_leaves;

    // Pre-compute each Collector leaf's matching doc-id set. Density
    // = fraction of total rows. A "match" is just a randomly picked
    // row id — the oracle uses these sets verbatim.
    let density = corpus.config.collector_density.clamp(0.0, 1.0);
    let per_leaf_matches: Vec<Vec<i32>> = (0..num_collectors)
        .map(|_| {
            let target = ((corpus.num_rows() as f64) * density) as usize;
            let mut matches: Vec<i32> = (0..corpus.num_rows() as i32).collect();
            matches.shuffle(rng);
            matches.truncate(target);
            matches.sort_unstable();
            matches
        })
        .collect();

    let tree = gen_node(
        rng,
        &schema,
        num_collectors,
        corpus.config.tree_max_depth,
        corpus.config.tree_max_fanout.max(2),
    );

    GeneratedTree {
        tree,
        collector_matches: per_leaf_matches,
    }
}

/// Recursive tree builder. Depth-bounded; at depth 0 always returns a
/// leaf. Picks AND/OR/NOT/Collector/Predicate with roughly equal weight
/// at interior depths.
fn gen_node(
    rng: &mut StdRng,
    schema: &SchemaRef,
    num_collectors: usize,
    depth_remaining: u32,
    max_fanout: usize,
) -> BoolNode {
    if depth_remaining == 0 {
        return gen_leaf(rng, schema, num_collectors);
    }
    // Strongly favor connectives so trees actually reach `tree_max_depth`.
    // Previously the 70/40 split stopped most trees around depth 1–2.
    let connective_prob = match depth_remaining {
        d if d >= 3 => 0.90,
        2 => 0.75,
        _ => 0.50,
    };
    if !rng.gen_bool(connective_prob) {
        return gen_leaf(rng, schema, num_collectors);
    }
    // Fanout skew: bias upward. `2..=max` uniform gives mean ≈ (max+2)/2;
    // we pick two candidates and take the larger, which pulls the mean up
    // without flattening to a single hot value. Keeps a chance of low
    // fanout (simple shapes) while emphasizing wider nodes.
    let pick_fanout = |r: &mut StdRng| -> usize {
        let a = r.gen_range(2..=max_fanout);
        let b = r.gen_range(2..=max_fanout);
        a.max(b)
    };
    match rng.gen_range(0..3u32) {
        0 => {
            let n = pick_fanout(rng);
            let children: Vec<BoolNode> = (0..n)
                .map(|_| gen_node(rng, schema, num_collectors, depth_remaining - 1, max_fanout))
                .collect();
            BoolNode::And(children)
        }
        1 => {
            let n = pick_fanout(rng);
            let children: Vec<BoolNode> = (0..n)
                .map(|_| gen_node(rng, schema, num_collectors, depth_remaining - 1, max_fanout))
                .collect();
            BoolNode::Or(children)
        }
        _ => BoolNode::Not(Box::new(gen_node(
            rng,
            schema,
            num_collectors,
            depth_remaining - 1,
            max_fanout,
        ))),
    }
}

fn gen_leaf(rng: &mut StdRng, schema: &SchemaRef, num_collectors: usize) -> BoolNode {
    // 35% collector, 65% predicate — predicates are cheaper to evaluate
    // and we want more of them for coverage, but we still want Collector
    // leaves to show up in every tree.
    let make_collector = num_collectors > 0 && rng.gen_bool(0.35);
    if make_collector {
        let id = rng.gen_range(0..num_collectors) as u8;
        BoolNode::Collector {
            annotation_id: id as i32,
        }
    } else {
        gen_predicate_leaf(rng, schema)
    }
}

/// Pick which predicate shape to emit. Weights approximate real-world
/// mix: plenty of simple comparisons, some IN lists, some IS NULL,
/// occasional LIKE on string columns.
enum PredicateShape {
    Binary,
    InList,
    IsNull,
    Like,
}

fn pick_predicate_shape(rng: &mut StdRng, has_string_col: bool) -> PredicateShape {
    // Without a string column, LIKE would always pick a non-string and
    // fail at evaluate time. Skip it in that case.
    let w_like: u32 = if has_string_col { 10 } else { 0 };
    let total = 60 + 20 + 10 + w_like;
    let x = rng.gen_range(0..total);
    if x < 60 {
        PredicateShape::Binary
    } else if x < 60 + 20 {
        PredicateShape::InList
    } else if x < 60 + 20 + 10 {
        PredicateShape::IsNull
    } else {
        PredicateShape::Like
    }
}

/// Build a `Predicate(Arc<dyn PhysicalExpr>)`. Dispatches to the chosen
/// shape builder.
fn gen_predicate_leaf(rng: &mut StdRng, schema: &SchemaRef) -> BoolNode {
    let has_string_col = schema
        .fields()
        .iter()
        .skip(1) // skip __doc_id
        .any(|f| matches!(f.data_type(), DataType::Utf8));
    match pick_predicate_shape(rng, has_string_col) {
        PredicateShape::Binary => gen_binary_predicate(rng, schema),
        PredicateShape::InList => gen_in_list_predicate(rng, schema),
        PredicateShape::IsNull => gen_is_null_predicate(rng, schema),
        PredicateShape::Like => gen_like_predicate(rng, schema),
    }
}

fn gen_binary_predicate(rng: &mut StdRng, schema: &SchemaRef) -> BoolNode {
    let col_idx = rng.gen_range(1..schema.fields().len());
    let field = schema.field(col_idx);
    let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), col_idx));
    let ops = [
        Operator::Eq,
        Operator::NotEq,
        Operator::Lt,
        Operator::LtEq,
        Operator::Gt,
        Operator::GtEq,
    ];
    let op = *ops.choose(rng).unwrap();
    let literal = pick_literal_for(rng, field.data_type());
    let lit_expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(literal));
    BoolNode::Predicate(Arc::new(BinaryExpr::new(col_expr, op, lit_expr)))
}

/// `col IN (lit, lit, ...)` with 1..=4 literals. With some probability,
/// one literal is NULL — exercises 3VL semantics for `IN` lists.
fn gen_in_list_predicate(rng: &mut StdRng, schema: &SchemaRef) -> BoolNode {
    use datafusion::physical_expr::expressions::InListExpr;

    let col_idx = rng.gen_range(1..schema.fields().len());
    let field = schema.field(col_idx);
    let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), col_idx));
    let n = rng.gen_range(1..=4);
    let include_null = rng.gen_bool(0.2); // 20% of IN lists include a NULL
    let list: Vec<Arc<dyn PhysicalExpr>> = (0..n)
        .map(|i| {
            // Place the NULL at a random position in the list if asked.
            let null_here = include_null && i == 0;
            let lit = if null_here {
                null_scalar_for(field.data_type())
            } else {
                pick_literal_for(rng, field.data_type())
            };
            Arc::new(Literal::new(lit)) as Arc<dyn PhysicalExpr>
        })
        .collect();
    let negated = rng.gen_bool(0.2);
    let bare_schema = datafusion::arrow::datatypes::Schema::new(
        schema.fields().iter().cloned().collect::<Vec<_>>(),
    );
    let in_list = InListExpr::try_new(col_expr, list, negated, &bare_schema)
        .expect("InListExpr try_new should succeed with schema-typed inputs");
    BoolNode::Predicate(Arc::new(in_list))
}

/// Typed NULL literal matching the given column's datatype.
fn null_scalar_for(dt: &DataType) -> ScalarValue {
    match dt {
        DataType::Utf8 => ScalarValue::Utf8(None),
        DataType::Int32 => ScalarValue::Int32(None),
        DataType::Int64 => ScalarValue::Int64(None),
        DataType::Float64 => ScalarValue::Float64(None),
        DataType::Boolean => ScalarValue::Boolean(None),
        DataType::Date32 => ScalarValue::Date32(None),
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None) => {
            ScalarValue::TimestampNanosecond(None, None)
        }
        other => panic!("null_scalar_for: unsupported type {:?}", other),
    }
}

/// `col IS NULL` or `col IS NOT NULL`. Emits `IsNullExpr` wrapped in
/// NOT for the negated form — so the oracle can keep its
/// `BoolNode::Not(Predicate(IsNull))` shape.
fn gen_is_null_predicate(rng: &mut StdRng, schema: &SchemaRef) -> BoolNode {
    use datafusion::physical_expr::expressions::IsNullExpr;

    let col_idx = rng.gen_range(1..schema.fields().len());
    let field = schema.field(col_idx);
    let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), col_idx));
    let is_null: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(col_expr));
    let node = BoolNode::Predicate(is_null);
    if rng.gen_bool(0.5) {
        BoolNode::Not(Box::new(node))
    } else {
        node
    }
}

/// `col LIKE 'pat'` on a Utf8 column. Picks a small wildcard pattern.
fn gen_like_predicate(rng: &mut StdRng, schema: &SchemaRef) -> BoolNode {
    use datafusion::physical_expr::expressions::LikeExpr;

    // Choose a random Utf8 column (guaranteed to exist by caller).
    let string_col_idxs: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .skip(1)
        .filter_map(|(i, f)| matches!(f.data_type(), DataType::Utf8).then_some(i))
        .collect();
    let col_idx = *string_col_idxs.choose(rng).unwrap();
    let field = schema.field(col_idx);
    let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), col_idx));

    // Pattern: single char + '%', or '%' + char, or just a char.
    // With Alphanumeric corpus, these hit somewhere.
    let ch: char = {
        let c: u32 = rng.gen_range(0..62);
        match c {
            0..=9 => (b'0' + c as u8) as char,
            10..=35 => (b'a' + (c as u8 - 10)) as char,
            _ => (b'A' + (c as u8 - 36)) as char,
        }
    };
    let pattern: String = match rng.gen_range(0..4) {
        0 => format!("{}%", ch),
        1 => format!("%{}", ch),
        2 => format!("%{}%", ch),
        _ => format!("{}", ch),
    };
    let pat_expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Utf8(Some(pattern))));
    let negated = rng.gen_bool(0.2);
    let case_insensitive = false;
    let like = LikeExpr::new(negated, case_insensitive, col_expr, pat_expr);
    BoolNode::Predicate(Arc::new(like))
}

/// Pick a literal value that matches the column's type, within a range
/// that overlaps the corpus's value distribution (so predicates
/// actually select a meaningful slice of rows).
fn pick_literal_for(rng: &mut StdRng, dt: &DataType) -> ScalarValue {
    // Mix strategy: 70% in-distribution (selects some rows), 15% below
    // min (0%), 15% above max (100% on <, 0% on >). This catches off-by-
    // one and empty-result short-circuit bugs.
    let strategy = rng.gen_range(0..100u32);
    match dt {
        DataType::Utf8 => {
            // Random alphanumeric — mostly doesn't match exactly with
            // the distinct pool, which is fine (tests eq/neq at 0%).
            let s: String = (0..rng.gen_range(1..=6))
                .map(|_| {
                    let c: u32 = rng.gen_range(0..62);
                    match c {
                        0..=9 => (b'0' + c as u8) as char,
                        10..=35 => (b'a' + (c as u8 - 10)) as char,
                        _ => (b'A' + (c as u8 - 36)) as char,
                    }
                })
                .collect();
            ScalarValue::Utf8(Some(s))
        }
        DataType::Int32 => {
            let v = match strategy {
                0..=69 => rng.gen_range(0..1000),      // in typical range
                70..=84 => rng.gen_range(i32::MIN..0), // below typical
                _ => rng.gen_range(1000..i32::MAX),    // above typical
            };
            ScalarValue::Int32(Some(v))
        }
        DataType::Int64 => {
            let v = match strategy {
                0..=69 => rng.gen_range(0..10_000),
                70..=84 => rng.gen_range(i64::MIN..0),
                _ => rng.gen_range(10_000..i64::MAX),
            };
            ScalarValue::Int64(Some(v))
        }
        DataType::Float64 => {
            let v = match strategy {
                0..=69 => rng.gen_range(0.0..100.0),
                70..=84 => rng.gen_range(-1e9..0.0),
                _ => rng.gen_range(100.0..1e9),
            };
            ScalarValue::Float64(Some(v))
        }
        DataType::Boolean => ScalarValue::Boolean(Some(rng.gen())),
        DataType::Date32 => {
            let v = match strategy {
                0..=69 => rng.gen_range(18_262..20_454),
                70..=84 => rng.gen_range(0..18_262),
                _ => rng.gen_range(20_454..i32::MAX),
            };
            ScalarValue::Date32(Some(v))
        }
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None) => {
            let v: i64 = match strategy {
                0..=69 => {
                    rng.gen_range(1_704_067_200_000_000_000_i64..1_735_689_600_000_000_000_i64)
                }
                70..=84 => rng.gen_range(0..1_704_067_200_000_000_000_i64),
                _ => rng.gen_range(1_735_689_600_000_000_000_i64..i64::MAX),
            };
            ScalarValue::TimestampNanosecond(Some(v), None)
        }
        other => panic!("tree_gen: unsupported schema type {:?}", other),
    }
}

/// Peer into a `BoolNode` to collect Collector leaf tags in DFS order.
/// Harness uses this to wire the same tag → doc-set mapping on both
/// sides (oracle + mock collector).
pub(in crate::indexed_table::tests_e2e) fn collect_collector_tags(tree: &BoolNode) -> Vec<u8> {
    fn walk(n: &BoolNode, out: &mut Vec<u8>) {
        match n {
            BoolNode::And(cs) | BoolNode::Or(cs) => cs.iter().for_each(|c| walk(c, out)),
            BoolNode::Not(c) => walk(c, out),
            BoolNode::Collector { annotation_id } => {
                out.push(*annotation_id as u8);
            }
            BoolNode::Predicate(_) => {}
            BoolNode::DelegationPossible { .. } => {}
        }
    }
    let mut out = Vec::new();
    walk(tree, &mut out);
    out
}

// Silence warnings: CellValue/Corpus used only via tree-gen callers.
#[allow(dead_code)]
fn _keep_imports(_: &CellValue, _: &Corpus) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::tests_e2e::fuzz::{build_corpus, FixtureConfig};
    use rand::SeedableRng;

    #[test]
    fn tree_gen_is_deterministic() {
        let corpus = build_corpus(FixtureConfig::small(42));
        let mut r1 = StdRng::seed_from_u64(1);
        let mut r2 = StdRng::seed_from_u64(1);
        let t1 = generate_tree(&mut r1, &corpus);
        let t2 = generate_tree(&mut r2, &corpus);
        assert_eq!(
            format!("{:?}", t1.tree),
            format!("{:?}", t2.tree),
            "same seed must yield same tree"
        );
        assert_eq!(t1.collector_matches, t2.collector_matches);
    }

    #[test]
    fn tree_gen_respects_depth_and_fanout() {
        let corpus = build_corpus(FixtureConfig::small(7));
        let mut rng = StdRng::seed_from_u64(7);
        let gt = generate_tree(&mut rng, &corpus);
        let d = depth(&gt.tree);
        assert!(
            d <= corpus.config.tree_max_depth as usize + 1,
            "depth {} exceeds bound {}",
            d,
            corpus.config.tree_max_depth
        );
        let fo = max_fanout(&gt.tree);
        assert!(
            fo <= corpus.config.tree_max_fanout,
            "fanout {} exceeds bound {}",
            fo,
            corpus.config.tree_max_fanout
        );
    }

    /// Sanity: over many seeds, at least some trees should actually
    /// reach the configured max depth. Otherwise tree-gen is quietly
    /// producing shallower trees than requested.
    #[test]
    fn tree_gen_reaches_high_depth() {
        let corpus = build_corpus(FixtureConfig::small(42));
        let mut max_seen = 0usize;
        let mut total_fanout_sum = 0usize;
        let mut total_fanout_count = 0usize;
        for s in 0..40u64 {
            let mut rng = StdRng::seed_from_u64(s);
            let gt = generate_tree(&mut rng, &corpus);
            max_seen = max_seen.max(depth(&gt.tree));
            count_fanouts(&gt.tree, &mut total_fanout_sum, &mut total_fanout_count);
        }
        assert!(
            max_seen as u32 >= corpus.config.tree_max_depth - 1,
            "max depth over 40 trees was {}; expected to reach near {}",
            max_seen,
            corpus.config.tree_max_depth,
        );
        let avg_fanout = if total_fanout_count > 0 {
            total_fanout_sum as f64 / total_fanout_count as f64
        } else {
            0.0
        };
        // With the skewed pick-larger-of-two scheme and max_fanout=5,
        // E[fanout] is around 3.5. Demand at least 3.0 so the test
        // catches a regression to uniform 2..=max (E ≈ 3.5 anyway,
        // but with pick_larger we push it higher).
        assert!(
            avg_fanout >= 3.0,
            "avg fanout {:.2} < 3.0 over 40 trees; tree-gen too narrow",
            avg_fanout
        );
    }

    fn count_fanouts(n: &BoolNode, sum: &mut usize, count: &mut usize) {
        match n {
            BoolNode::And(cs) | BoolNode::Or(cs) => {
                *sum += cs.len();
                *count += 1;
                cs.iter().for_each(|c| count_fanouts(c, sum, count));
            }
            BoolNode::Not(c) => count_fanouts(c, sum, count),
            BoolNode::Collector { .. } | BoolNode::Predicate(_) | BoolNode::DelegationPossible { .. } => {}
        }
    }

    #[test]
    fn tree_gen_produces_collector_leaves_sometimes() {
        let corpus = build_corpus(FixtureConfig::small(11));
        let mut saw_collector = false;
        for iter in 0..50u64 {
            let mut rng = StdRng::seed_from_u64(iter);
            let gt = generate_tree(&mut rng, &corpus);
            if !collect_collector_tags(&gt.tree).is_empty() {
                saw_collector = true;
                break;
            }
        }
        assert!(saw_collector, "50 trees and not one Collector leaf");
    }

    fn depth(n: &BoolNode) -> usize {
        match n {
            BoolNode::And(cs) | BoolNode::Or(cs) => 1 + cs.iter().map(depth).max().unwrap_or(0),
            BoolNode::Not(c) => 1 + depth(c),
            BoolNode::Collector { .. } | BoolNode::Predicate(_) | BoolNode::DelegationPossible { .. } => 0,
        }
    }

    fn max_fanout(n: &BoolNode) -> usize {
        match n {
            BoolNode::And(cs) | BoolNode::Or(cs) => {
                cs.len().max(cs.iter().map(max_fanout).max().unwrap_or(0))
            }
            BoolNode::Not(c) => max_fanout(c),
            BoolNode::Collector { .. } | BoolNode::Predicate(_) | BoolNode::DelegationPossible { .. } => 0,
        }
    }
}
