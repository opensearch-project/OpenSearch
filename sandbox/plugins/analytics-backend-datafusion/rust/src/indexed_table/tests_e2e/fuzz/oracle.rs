/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Ground-truth oracle: evaluate a `BoolNode` tree row-by-row against
//! the raw corpus. Trusted because it's obviously correct by
//! construction — a simple recursive SQL-3VL evaluator.
//!
//! Each Predicate leaf is required to be a `BinaryExpr(Column, Op,
//! Literal)` — that's the shape `tree_gen` produces. The evaluator
//! panics on any other shape; if the tree-gen grows new shapes, this
//! oracle must grow with it.

use std::collections::HashSet;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;

use super::corpus::{CellValue, Corpus};
use super::tree_gen::GeneratedTree;
use crate::indexed_table::bool_tree::BoolNode;

/// 3-valued logic: TRUE / FALSE / UNKNOWN (NULL).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tri {
    True,
    False,
    Unknown,
}

impl Tri {
    fn and(self, other: Tri) -> Tri {
        match (self, other) {
            (Tri::False, _) | (_, Tri::False) => Tri::False,
            (Tri::True, Tri::True) => Tri::True,
            _ => Tri::Unknown,
        }
    }
    fn or(self, other: Tri) -> Tri {
        match (self, other) {
            (Tri::True, _) | (_, Tri::True) => Tri::True,
            (Tri::False, Tri::False) => Tri::False,
            _ => Tri::Unknown,
        }
    }
    fn not(self) -> Tri {
        match self {
            Tri::True => Tri::False,
            Tri::False => Tri::True,
            Tri::Unknown => Tri::Unknown,
        }
    }
}

/// Evaluate `tree` row-by-row. Returns the sorted vec of matching doc
/// ids — a row matches iff the tree's Tri result is `True` (SQL
/// semantics: NULL doesn't pass WHERE).
pub(in crate::indexed_table::tests_e2e) fn oracle_evaluate(
    tree: &GeneratedTree,
    corpus: &Corpus,
) -> Vec<i32> {
    let collector_sets: Vec<HashSet<i32>> = tree
        .collector_matches
        .iter()
        .map(|v| v.iter().copied().collect())
        .collect();

    let mut result = Vec::new();
    for row in 0..corpus.num_rows() as i32 {
        if eval_row(&tree.tree, corpus, row, &collector_sets) == Tri::True {
            result.push(row);
        }
    }
    result
}

fn eval_row(node: &BoolNode, corpus: &Corpus, row: i32, collector_sets: &[HashSet<i32>]) -> Tri {
    match node {
        BoolNode::And(children) => {
            let mut acc = Tri::True;
            for c in children {
                acc = acc.and(eval_row(c, corpus, row, collector_sets));
                if acc == Tri::False {
                    return Tri::False;
                }
            }
            acc
        }
        BoolNode::Or(children) => {
            let mut acc = Tri::False;
            for c in children {
                acc = acc.or(eval_row(c, corpus, row, collector_sets));
                if acc == Tri::True {
                    return Tri::True;
                }
            }
            acc
        }
        BoolNode::Not(child) => eval_row(child, corpus, row, collector_sets).not(),
        BoolNode::Collector { annotation_id } => {
            let tag = *annotation_id as u8 as usize;
            let set = collector_sets
                .get(tag)
                .unwrap_or_else(|| panic!("oracle: Collector tag {} has no matching set", tag));
            if set.contains(&row) {
                Tri::True
            } else {
                Tri::False
            }
        }
        BoolNode::Predicate(expr) => eval_predicate(expr, corpus, row as usize),
        BoolNode::DelegationPossible { .. } => {
            unreachable!("fuzz tree generator does not produce DelegationPossible leaves")
        }
    }
}

/// Evaluate a predicate expression at a single row. Supports:
/// - `BinaryExpr(Column, cmp_op, Literal)`
/// - `InListExpr { Column, list: Vec<Literal>, negated }`
/// - `IsNullExpr(Column)`
/// - `LikeExpr { Column, pattern: Literal, negated, case_insensitive=false }`
fn eval_predicate(expr: &std::sync::Arc<dyn PhysicalExpr>, corpus: &Corpus, row: usize) -> Tri {
    use datafusion::physical_expr::expressions::{InListExpr, IsNullExpr, LikeExpr};

    let any = expr.as_any();

    if let Some(bin) = any.downcast_ref::<BinaryExpr>() {
        let col = bin
            .left()
            .as_any()
            .downcast_ref::<Column>()
            .expect("oracle: BinaryExpr lhs must be Column");
        let lit = bin
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .expect("oracle: BinaryExpr rhs must be Literal");
        let cell = get_cell(corpus, col.name(), row);
        return compare_cell_lit(cell, *bin.op(), lit.value());
    }

    if let Some(in_list) = any.downcast_ref::<InListExpr>() {
        let col = in_list
            .expr()
            .as_any()
            .downcast_ref::<Column>()
            .expect("oracle: InList target must be Column");
        let cell = get_cell(corpus, col.name(), row);
        // SQL 3VL for IN:
        // - cell IS NULL → UNKNOWN
        // - any (cell == lit) → TRUE
        // - any lit IS NULL → UNKNOWN (not found yet; keep going looking for match)
        // - none match, no NULL lit → FALSE
        // Then `negated` flips TRUE↔FALSE (UNKNOWN stays).
        let base = match cell_null(cell) {
            true => Tri::Unknown,
            false => {
                let mut seen_unknown = false;
                let mut found = false;
                for lit_expr in in_list.list() {
                    let lit = lit_expr
                        .as_any()
                        .downcast_ref::<Literal>()
                        .expect("oracle: InList list entry must be Literal");
                    match compare_cell_lit(cell, Operator::Eq, lit.value()) {
                        Tri::True => {
                            found = true;
                            break;
                        }
                        Tri::Unknown => seen_unknown = true,
                        Tri::False => {}
                    }
                }
                if found {
                    Tri::True
                } else if seen_unknown {
                    Tri::Unknown
                } else {
                    Tri::False
                }
            }
        };
        if in_list.negated() {
            return base.not();
        }
        return base;
    }

    if let Some(is_null) = any.downcast_ref::<IsNullExpr>() {
        let col = is_null
            .arg()
            .as_any()
            .downcast_ref::<Column>()
            .expect("oracle: IsNull target must be Column");
        let cell = get_cell(corpus, col.name(), row);
        // IS NULL returns TRUE/FALSE — never UNKNOWN — per SQL.
        return if cell_null(cell) {
            Tri::True
        } else {
            Tri::False
        };
    }

    if let Some(like) = any.downcast_ref::<LikeExpr>() {
        let col = like
            .expr()
            .as_any()
            .downcast_ref::<Column>()
            .expect("oracle: Like target must be Column");
        let pat_lit = like
            .pattern()
            .as_any()
            .downcast_ref::<Literal>()
            .expect("oracle: Like pattern must be Literal");
        let cell = get_cell(corpus, col.name(), row);
        let pat_str = match pat_lit.value() {
            ScalarValue::Utf8(Some(s)) => s.as_str(),
            ScalarValue::Utf8(None) => return Tri::Unknown,
            other => panic!("oracle: Like pattern not Utf8: {:?}", other),
        };
        let val_str = match cell {
            CellValue::Utf8(Some(s)) => s.as_str(),
            CellValue::Utf8(None) => return Tri::Unknown,
            other => panic!("oracle: Like target not Utf8 cell: {:?}", other),
        };
        let m = sql_like_match(val_str, pat_str);
        let base = if m { Tri::True } else { Tri::False };
        if like.negated() {
            return base.not();
        }
        return base;
    }

    panic!("oracle: unsupported Predicate shape {:?}", expr);
}

fn get_cell<'a>(corpus: &'a Corpus, col_name: &str, row: usize) -> &'a CellValue {
    let col_idx = *corpus
        .col_idx
        .get(col_name)
        .unwrap_or_else(|| panic!("oracle: column {:?} not in corpus", col_name));
    &corpus.cells[col_idx][row]
}

fn cell_null(cell: &CellValue) -> bool {
    match cell {
        CellValue::Utf8(v) => v.is_none(),
        CellValue::Int32(v) => v.is_none(),
        CellValue::Int64(v) => v.is_none(),
        CellValue::Float64(v) => v.is_none(),
        CellValue::Boolean(v) => v.is_none(),
        CellValue::Date32(v) => v.is_none(),
        CellValue::TimestampNanos(v) => v.is_none(),
    }
}

/// Minimal SQL LIKE matcher: `%` matches any (possibly empty) substring,
/// `_` matches any single char. No escape handling (our patterns never
/// include backslashes).
fn sql_like_match(value: &str, pattern: &str) -> bool {
    // Convert pattern to regex-ish DP.
    // Classic DP: dp[i][j] = true iff value[0..i] matches pattern[0..j].
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let n = v.len();
    let m = p.len();
    let mut dp = vec![vec![false; m + 1]; n + 1];
    dp[0][0] = true;
    for j in 1..=m {
        if p[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }
    for i in 1..=n {
        for j in 1..=m {
            dp[i][j] = match p[j - 1] {
                '%' => dp[i][j - 1] || dp[i - 1][j],
                '_' => dp[i - 1][j - 1],
                c => v[i - 1] == c && dp[i - 1][j - 1],
            };
        }
    }
    dp[n][m]
}

fn compare_cell_lit(cell: &CellValue, op: Operator, lit: &ScalarValue) -> Tri {
    // Any null → UNKNOWN.
    macro_rules! none_unknown {
        ($x:expr) => {
            match $x {
                None => return Tri::Unknown,
                Some(v) => v,
            }
        };
    }
    match (cell, lit) {
        (CellValue::Utf8(c), ScalarValue::Utf8(l)) => {
            let c = none_unknown!(c);
            let l = none_unknown!(l);
            cmp_to_tri(c.as_str().cmp(l.as_str()), op)
        }
        (CellValue::Int32(c), ScalarValue::Int32(l)) => {
            let c = none_unknown!(c);
            let l = none_unknown!(l);
            cmp_to_tri(c.cmp(l), op)
        }
        (CellValue::Int64(c), ScalarValue::Int64(l)) => {
            let c = none_unknown!(c);
            let l = none_unknown!(l);
            cmp_to_tri(c.cmp(l), op)
        }
        (CellValue::Float64(c), ScalarValue::Float64(l)) => {
            let c = none_unknown!(c);
            let l = none_unknown!(l);
            // f64 cmp: treat NaN as UNKNOWN (matches SQL semantics).
            if c.is_nan() || l.is_nan() {
                return Tri::Unknown;
            }
            let ord = c.partial_cmp(l).expect("checked non-NaN");
            cmp_to_tri(ord, op)
        }
        (CellValue::Boolean(c), ScalarValue::Boolean(l)) => {
            let c = none_unknown!(c);
            let l = none_unknown!(l);
            // Only Eq and NotEq are meaningful for booleans (our tree-gen
            // may emit ordering ops but arrow compares false < true so
            // we honor that for parity).
            cmp_to_tri((*c as i32).cmp(&(*l as i32)), op)
        }
        (CellValue::Date32(c), ScalarValue::Date32(l)) => {
            let c = none_unknown!(c);
            let l = none_unknown!(l);
            cmp_to_tri(c.cmp(l), op)
        }
        (CellValue::TimestampNanos(c), ScalarValue::TimestampNanosecond(l, _tz)) => {
            let c = none_unknown!(c);
            let l = none_unknown!(l);
            cmp_to_tri(c.cmp(l), op)
        }
        (cell, lit) => panic!("oracle: type mismatch cell={:?} lit={:?}", cell, lit),
    }
}

fn cmp_to_tri(ord: std::cmp::Ordering, op: Operator) -> Tri {
    use std::cmp::Ordering::*;
    let result = match op {
        Operator::Eq => ord == Equal,
        Operator::NotEq => ord != Equal,
        Operator::Lt => ord == Less,
        Operator::LtEq => ord != Greater,
        Operator::Gt => ord == Greater,
        Operator::GtEq => ord != Less,
        other => panic!("oracle: unsupported operator {:?}", other),
    };
    if result {
        Tri::True
    } else {
        Tri::False
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::tests_e2e::fuzz::{build_corpus, generate_tree, FixtureConfig};
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn oracle_produces_sorted_unique_output() {
        let corpus = build_corpus(FixtureConfig::small(0xa));
        let mut rng = StdRng::seed_from_u64(0xa);
        let tree = generate_tree(&mut rng, &corpus);
        let out = oracle_evaluate(&tree, &corpus);
        // Sorted.
        for w in out.windows(2) {
            assert!(w[0] < w[1], "oracle output not strictly sorted");
        }
        // Within corpus range.
        for d in &out {
            assert!(*d >= 0 && (*d as usize) < corpus.num_rows());
        }
    }

    #[test]
    fn oracle_is_deterministic() {
        let corpus = build_corpus(FixtureConfig::small(0xb));
        let mut rng = StdRng::seed_from_u64(0xb);
        let tree = generate_tree(&mut rng, &corpus);
        let a = oracle_evaluate(&tree, &corpus);
        let b = oracle_evaluate(&tree, &corpus);
        assert_eq!(a, b);
    }

    #[test]
    fn oracle_not_trivially_empty_or_full() {
        // Ensure we're actually exercising the evaluator: for a handful
        // of random seeds at least one result is neither empty nor full.
        let corpus = build_corpus(FixtureConfig::small(0xc));
        let mut saw_non_trivial = false;
        for s in 0..20u64 {
            let mut rng = StdRng::seed_from_u64(s);
            let tree = generate_tree(&mut rng, &corpus);
            let out = oracle_evaluate(&tree, &corpus);
            if !out.is_empty() && out.len() < corpus.num_rows() {
                saw_non_trivial = true;
                break;
            }
        }
        assert!(saw_non_trivial, "all 20 trees were trivially empty/full");
    }
}
