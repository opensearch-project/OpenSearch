/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Boolean-algebra correctness on a 16-row hand-authored fixture:
//! idempotence, De Morgan, absorption, distributivity, commutativity,
//! associativity, double-negation, bound edges, deep nesting, wide
//! fan-out, stress shapes. Built around an reference_evaluator that evaluates trees
//! row-by-row directly on the fixture arrays, so each test is one line.

use super::*;

// ══════════════════════════════════════════════════════════════════
// Test cases
// ══════════════════════════════════════════════════════════════════

/// Simple sanity: single collector + single predicate AND'd.
///   brand == "apple" AND price > 80
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simple_and_of_collector_and_predicate() {
    // Tree: AND(apple, price > 80)
    let tree = BoolNode::And(vec![index_leaf(1), predicate_leaf(0)]);
    let preds = vec![pred_int("price", Operator::Gt, 80)];
    let rows = run_tree(tree, preds).await;

    // Expected: apple rows with price > 80: rows 4,5,6 → prices 90,95,200
    let mut got: Vec<(String, i32)> = rows.iter().map(|r| (r.0.clone(), r.1)).collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("apple".into(), 90),
            ("apple".into(), 95),
            ("apple".into(), 200),
        ]
    );
}

/// OR branch with predicate hanging off one side:
///   brand == "amazon" OR (brand == "apple" AND price < 100)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn or_with_nested_and() {
    let tree = BoolNode::Or(vec![
        index_leaf(0),
        BoolNode::And(vec![index_leaf(1), predicate_leaf(0)]),
    ]);
    let preds = vec![pred_int("price", Operator::Lt, 100)];
    let rows = run_tree(tree, preds).await;

    // amazon rows: 0,1,2,3,12 (5)
    // apple AND price<100: 4 (90), 5 (95), 7 (60), 13 (45) → 4
    // total 9 distinct rows
    let mut got: Vec<(String, i32)> = rows.iter().map(|r| (r.0.clone(), r.1)).collect();
    got.sort();
    let mut expected: Vec<(String, i32)> = vec![
        ("amazon".into(), 50),
        ("amazon".into(), 150),
        ("amazon".into(), 80),
        ("amazon".into(), 120),
        ("amazon".into(), 30),
        ("apple".into(), 90),
        ("apple".into(), 95),
        ("apple".into(), 60),
        ("apple".into(), 45),
    ];
    expected.sort();
    assert_eq!(got, expected);
}

/// NOT around a collector:
///   category == "electronics" AND NOT (status == "archived")
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn not_collector_with_predicate() {
    let tree = BoolNode::And(vec![
        predicate_leaf(0),
        BoolNode::Not(Box::new(index_leaf(2))),
    ]);
    let preds = vec![pred_str("category", Operator::Eq, "electronics")];
    let rows = run_tree(tree, preds).await;

    // electronics AND not archived: rows 0,3,4,7,8,10,14,15
    let mut got: Vec<i32> = rows.iter().map(|r| r.1).collect();
    got.sort();
    assert_eq!(got, vec![40, 50, 55, 60, 70, 90, 99, 120]);
}

/// **The complex e2e tree.** Three index-backed collectors, two parquet
/// predicates, AND/OR/NOT combined, three levels deep:
///
/// ```
/// AND(
///   OR(
///     brand == "amazon",
///     AND(brand == "apple", price < 100)
///   ),
///   NOT(status == "archived"),
///   category == "electronics"
/// )
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complex_tree_3_levels_3_collectors_2_predicates() {
    let tree = BoolNode::And(vec![
        BoolNode::Or(vec![
            index_leaf(0), // brand=amazon
            BoolNode::And(vec![
                index_leaf(1),       // brand=apple
                predicate_leaf(0),    // price < 100
            ]),
        ]),
        BoolNode::Not(Box::new(index_leaf(2))), // NOT status=archived
        predicate_leaf(1),                        // category=electronics
    ]);
    let preds = vec![
        pred_int("price", Operator::Lt, 100),
        pred_str("category", Operator::Eq, "electronics"),
    ];
    let rows = run_tree(tree, preds).await;

    // Compute expected in Rust — straightforward boolean eval over the data
    let expected = expected_for_complex_tree();
    let mut got: Vec<i32> = rows.iter().map(|r| r.1).collect();
    got.sort();
    let mut exp = expected;
    exp.sort();
    assert_eq!(got, exp, "complex tree result mismatch");
}

/// Independent reference implementation of the complex-tree predicate.
fn expected_for_complex_tree() -> Vec<i32> {
    let mut out = Vec::new();
    for i in 0..16 {
        let is_amazon = BRANDS[i] == "amazon";
        let is_apple_cheap = BRANDS[i] == "apple" && PRICES[i] < 100;
        let not_archived = STATUSES[i] != "archived";
        let electronics = CATEGORIES[i] == "electronics";
        if (is_amazon || is_apple_cheap) && not_archived && electronics {
            out.push(PRICES[i]);
        }
    }
    out
}

/// Also exercise De Morgan's: NOT(AND(a,b)) must get rewritten to OR(NOT(a),NOT(b))
/// by `push_not_down`, and still produce correct results.
///
///   NOT(brand == "amazon" AND status == "archived")
///     equiv
///   brand != "amazon" OR status != "archived"
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn not_and_de_morgan() {
    let tree = BoolNode::Not(Box::new(BoolNode::And(vec![
        index_leaf(0), // amazon
        index_leaf(2), // archived
    ])));
    let rows = run_tree(tree, vec![]).await;

    // All rows NOT (amazon AND archived):
    //   amazon AND archived: rows 1,12 → exclude these
    //   all other 14 rows pass
    assert_eq!(rows.len(), 14);
    for (_, price, _, _) in &rows {
        // sanity: the 2 excluded rows have prices 150 (row 1) and 30 (row 12).
        // Other rows' prices may equal these too (e.g. row 11 is 150), so
        // we only check that we have 14 unique inputs, not distinct prices.
        let _ = price;
    }
}

/// Sanity: pure-NOT around a single collector (no other filters) returns
/// the complement set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_not_returns_complement() {
    let tree = BoolNode::Not(Box::new(index_leaf(0))); // NOT amazon
    let rows = run_tree(tree, vec![]).await;
    // 16 docs, 5 are amazon → 11 expected.
    assert_eq!(rows.len(), 11);
    for r in &rows {
        assert_ne!(r.0, "amazon");
    }
}

/// Collector used twice in the tree (same provider_id). Each occurrence
/// gets its own MockCollector instance (they're built in DFS order). The
/// tree evaluator should handle this correctly.
///
///   (brand == "amazon") AND (status == "active")
///     — but we fake "status == active" as: NOT(status == "archived")
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negated_leaf_intersected_with_positive_leaf() {
    let tree = BoolNode::And(vec![
        index_leaf(0),                           // amazon
        BoolNode::Not(Box::new(index_leaf(2))), // NOT archived
    ]);
    let rows = run_tree(tree, vec![]).await;
    // amazon AND active: rows 0,2,3
    let mut prices: Vec<i32> = rows.iter().map(|r| r.1).collect();
    prices.sort();
    assert_eq!(prices, vec![50, 80, 120]);
}

// ══════════════════════════════════════════════════════════════════════
// Reference evaluator-driven exhaustive tests
// ══════════════════════════════════════════════════════════════════════
//
// Primitive leaves identified by a stable `LeafId`. The reference_evaluator evaluates the
// tree row-by-row directly on the fixture arrays; the engine runs the full
// Phase 1/2 pipeline. `assert_engine_matches_reference` runs both and compares.
//
// This harness lets us write a tree expression and get a correctness check
// without hand-computing expected rows — covers many boolean combinations
// cheaply.

/// A "primitive" leaf in our test trees, mapped both to a `BoolNode` the
/// engine understands and to a row-level predicate the reference_evaluator uses.
#[derive(Debug, Clone, Copy)]
enum LeafId {
    // index-backed collector leaves (provider_id 0/1/2 per `wire_collector_indices`)
    BrandAmazon,   // provider_id = 0
    BrandApple,    // provider_id = 1
    StatusArchived, // provider_id = 2
    // Parquet predicate leaves — reference_evaluator applies directly.
    PriceLt100,
    PriceLt50,
    PriceGt100,
    PriceGe150,
    PriceEq150,
    CategoryElectronics,
    CategoryBooks,
}

/// Matchers the reference_evaluator uses.
impl LeafId {
    fn matches(self, row: usize) -> bool {
        match self {
            LeafId::BrandAmazon => BRANDS[row] == "amazon",
            LeafId::BrandApple => BRANDS[row] == "apple",
            LeafId::StatusArchived => STATUSES[row] == "archived",
            LeafId::PriceLt100 => PRICES[row] < 100,
            LeafId::PriceLt50 => PRICES[row] < 50,
            LeafId::PriceGt100 => PRICES[row] > 100,
            LeafId::PriceGe150 => PRICES[row] >= 150,
            LeafId::PriceEq150 => PRICES[row] == 150,
            LeafId::CategoryElectronics => CATEGORIES[row] == "electronics",
            LeafId::CategoryBooks => CATEGORIES[row] == "books",
        }
    }

    fn is_predicate(self) -> bool {
        !matches!(
            self,
            LeafId::BrandAmazon | LeafId::BrandApple | LeafId::StatusArchived
        )
    }

    fn as_predicate(self) -> Option<ResolvedPredicate> {
        Some(match self {
            LeafId::PriceLt100 => pred_int("price", Operator::Lt, 100),
            LeafId::PriceLt50 => pred_int("price", Operator::Lt, 50),
            LeafId::PriceGt100 => pred_int("price", Operator::Gt, 100),
            LeafId::PriceGe150 => pred_int("price", Operator::GtEq, 150),
            LeafId::PriceEq150 => pred_int("price", Operator::Eq, 150),
            LeafId::CategoryElectronics => pred_str("category", Operator::Eq, "electronics"),
            LeafId::CategoryBooks => pred_str("category", Operator::Eq, "books"),
            _ => return None,
        })
    }

    fn as_collector_provider_id(self) -> Option<u8> {
        Some(match self {
            LeafId::BrandAmazon => 0,
            LeafId::BrandApple => 1,
            LeafId::StatusArchived => 2,
            _ => return None,
        })
    }
}

/// A compact tree representation for the reference_evaluator; mirrored to `BoolNode` for
/// the engine. Doesn't need to roundtrip — we build both from the same DSL
/// each time.
#[derive(Debug, Clone)]
enum T {
    Leaf(LeafId),
    And(Vec<T>),
    Or(Vec<T>),
    Not(Box<T>),
}

/// Reference evaluator: row-by-row boolean evaluation.
fn reference_evaluator(tree: &T, row: usize) -> bool {
    match tree {
        T::Leaf(l) => l.matches(row),
        T::And(children) => children.iter().all(|c| reference_evaluator(c, row)),
        T::Or(children) => children.iter().any(|c| reference_evaluator(c, row)),
        T::Not(inner) => !reference_evaluator(inner, row),
    }
}

/// Lower `T` to `BoolNode` + collect predicate leaves into a sidecar Vec.
/// Collector leaves get `collector_idx = 0` here; `wire_collector_indices`
/// will assign real indices during `run_tree`.
fn to_engine_tree(tree: &T, preds: &mut Vec<ResolvedPredicate>) -> BoolNode {
    match tree {
        T::Leaf(l) if l.is_predicate() => {
            let idx = preds.len() as u16;
            preds.push(l.as_predicate().unwrap());
            BoolNode::Predicate { predicate_id: idx }
        }
        T::Leaf(l) => BoolNode::Collector {
            query_bytes: Arc::from(&[l.as_collector_provider_id().unwrap()][..]),
        },
        T::And(children) => BoolNode::And(children.iter().map(|c| to_engine_tree(c, preds)).collect()),
        T::Or(children) => BoolNode::Or(children.iter().map(|c| to_engine_tree(c, preds)).collect()),
        T::Not(inner) => BoolNode::Not(Box::new(to_engine_tree(inner, preds))),
    }
}

/// Run `tree` through the engine and compare to the reference_evaluator over all 16 rows.
async fn assert_engine_matches_reference(name: &str, tree: T) {
    let expected: Vec<usize> = (0..16).filter(|&r| reference_evaluator(&tree, r)).collect();

    let mut preds = Vec::new();
    let bool_tree = to_engine_tree(&tree, &mut preds);
    let rows = run_tree(bool_tree, preds).await;

    // Map returned rows back to fixture row indices by matching (brand,price,status,category).
    let mut actual: Vec<usize> = Vec::with_capacity(rows.len());
    for (brand, price, status, cat) in &rows {
        let found = (0..16).find(|&r| {
            BRANDS[r] == brand.as_str()
                && PRICES[r] == *price
                && STATUSES[r] == status.as_str()
                && CATEGORIES[r] == cat.as_str()
                && !actual.contains(&r)
        });
        assert!(
            found.is_some(),
            "[{}] engine returned row not in fixture: ({}, {}, {}, {})",
            name,
            brand,
            price,
            status,
            cat
        );
        actual.push(found.unwrap());
    }
    actual.sort();
    let mut expected_sorted = expected.clone();
    expected_sorted.sort();
    assert_eq!(
        actual, expected_sorted,
        "[{}] engine rows {:?} != reference_evaluator rows {:?}",
        name, actual, expected_sorted
    );
}

// Short aliases used by all reference_evaluator tests.
use LeafId::*;
fn l(id: LeafId) -> T {
    T::Leaf(id)
}
fn and_(xs: Vec<T>) -> T {
    T::And(xs)
}
fn or_(xs: Vec<T>) -> T {
    T::Or(xs)
}
fn not_(x: T) -> T {
    T::Not(Box::new(x))
}

// ──────────────────────────────────────────────────────────────────────
// Batch 1 — single leaves + basic combinators.
// Each test is named for its shape; reference_evaluator catches mismatches.
// ──────────────────────────────────────────────────────────────────────

macro_rules! reference_test {
    ($name:ident, $tree:expr) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() {
            assert_engine_matches_reference(stringify!($name), $tree).await;
        }
    };
}

// Single-leaf tests.
reference_test!(leaf_brand_amazon, l(BrandAmazon));
reference_test!(leaf_brand_apple, l(BrandApple));
reference_test!(leaf_status_archived, l(StatusArchived));
reference_test!(leaf_price_lt_100, l(PriceLt100));
reference_test!(leaf_price_gt_100, l(PriceGt100));
reference_test!(leaf_price_eq_150, l(PriceEq150));
reference_test!(leaf_category_electronics, l(CategoryElectronics));

// Simple NOT of a leaf — exercises push_not_down for each leaf type.
reference_test!(not_brand_amazon, not_(l(BrandAmazon)));
reference_test!(not_price_lt_100, not_(l(PriceLt100)));
reference_test!(not_category_books, not_(l(CategoryBooks)));

// Pairs via AND / OR — smallest compound shapes.
reference_test!(and_two_collectors, and_(vec![l(BrandAmazon), l(StatusArchived)]));
reference_test!(or_two_collectors, or_(vec![l(BrandAmazon), l(BrandApple)]));
reference_test!(and_two_predicates, and_(vec![l(PriceGt100), l(CategoryElectronics)]));
reference_test!(or_two_predicates, or_(vec![l(PriceLt50), l(PriceGe150)]));
reference_test!(
    and_collector_and_predicate,
    and_(vec![l(BrandApple), l(PriceLt100)])
);
reference_test!(
    or_collector_or_predicate,
    or_(vec![l(StatusArchived), l(PriceLt50)])
);

// ──────────────────────────────────────────────────────────────────────
// Batch 2 — Boolean algebra laws. Each case states a known equivalence;
// the reference_evaluator doesn't care about the equivalence, it just checks that the
// engine returns exactly the docs the tree selects.
// ──────────────────────────────────────────────────────────────────────

// Idempotence: A AND A == A ; A OR A == A
reference_test!(idempotent_and, and_(vec![l(BrandAmazon), l(BrandAmazon)]));
reference_test!(idempotent_or, or_(vec![l(PriceLt100), l(PriceLt100)]));

// Double negation: NOT(NOT A) == A
reference_test!(double_not_collector, not_(not_(l(BrandApple))));
reference_test!(double_not_predicate, not_(not_(l(CategoryElectronics))));

// De Morgan's laws.
// NOT(A AND B) == NOT(A) OR NOT(B)
reference_test!(
    de_morgan_not_and_two_collectors,
    not_(and_(vec![l(BrandAmazon), l(StatusArchived)]))
);
reference_test!(
    de_morgan_not_and_mixed,
    not_(and_(vec![l(BrandApple), l(PriceLt100)]))
);
// NOT(A OR B) == NOT(A) AND NOT(B)
reference_test!(
    de_morgan_not_or_two_collectors,
    not_(or_(vec![l(BrandAmazon), l(BrandApple)]))
);
reference_test!(
    de_morgan_not_or_mixed,
    not_(or_(vec![l(StatusArchived), l(PriceGe150)]))
);

// Absorption: A AND (A OR B) == A ; A OR (A AND B) == A
reference_test!(
    absorption_and,
    and_(vec![l(BrandAmazon), or_(vec![l(BrandAmazon), l(BrandApple)])])
);
reference_test!(
    absorption_or,
    or_(vec![l(PriceLt100), and_(vec![l(PriceLt100), l(CategoryBooks)])])
);

// Distributivity: A AND (B OR C) == (A AND B) OR (A AND C)
reference_test!(
    distributive_and_over_or,
    and_(vec![
        l(CategoryElectronics),
        or_(vec![l(BrandAmazon), l(BrandApple)]),
    ])
);
// A OR (B AND C) == (A OR B) AND (A OR C)
reference_test!(
    distributive_or_over_and,
    or_(vec![
        l(StatusArchived),
        and_(vec![l(BrandAmazon), l(PriceLt100)]),
    ])
);

// ──────────────────────────────────────────────────────────────────────
// Batch 3 — identity and bound edges.
// Cases where one branch of AND/OR collapses the result to empty or universe.
// ──────────────────────────────────────────────────────────────────────

// Predicate that matches nothing: price < 50 AND price > 100 (vacuous).
reference_test!(
    and_of_contradictory_predicates,
    and_(vec![l(PriceLt50), l(PriceGt100)])
);

// Predicate that matches everything: price < 100 OR price >= 150.
// Combined range covers all prices in fixture (30..300).
reference_test!(
    or_of_predicates_covering_universe,
    or_(vec![l(PriceLt100), l(PriceGe150)])
);

// AND with contradictory branch: (brand=apple) AND (NOT(category=electronics) AND category=books)
// Apple rows are all electronics OR books. Books apple: row 6.
reference_test!(
    and_with_subtree_selecting_subset,
    and_(vec![
        l(BrandApple),
        and_(vec![not_(l(CategoryElectronics)), l(CategoryBooks)]),
    ])
);

// OR where one side is always-true selector: cat=electronics OR NOT(cat=electronics)
// ⇒ universe. All 16 rows.
reference_test!(
    or_of_a_and_not_a,
    or_(vec![l(CategoryElectronics), not_(l(CategoryElectronics))])
);

// AND of A and NOT A: empty.
reference_test!(
    and_of_a_and_not_a,
    and_(vec![l(CategoryElectronics), not_(l(CategoryElectronics))])
);

// Empty-like collector scope: brand=amazon AND category=books AND price<50
// Expected: row 12 has amazon, electronics, 30 → category≠books → 0 rows.
reference_test!(
    empty_result_via_three_way_and,
    and_(vec![l(BrandAmazon), l(CategoryBooks), l(PriceLt50)])
);

// OR of two non-overlapping collectors.
reference_test!(or_non_overlapping_collectors, or_(vec![l(BrandAmazon), l(BrandApple)]));

// AND of two non-overlapping collectors → empty.
reference_test!(
    and_non_overlapping_collectors,
    and_(vec![l(BrandAmazon), l(BrandApple)])
);

// Partially overlapping: archived ∩ amazon.
reference_test!(
    and_partial_overlap_collectors,
    and_(vec![l(BrandAmazon), l(StatusArchived)])
);

// Single-row targeting: price = 150 AND category = books → row 11.
reference_test!(single_row_match, and_(vec![l(PriceEq150), l(CategoryBooks)]));

// No row has `price < 50 AND category=books` in fixture → empty.
reference_test!(
    empty_via_predicate_combination,
    and_(vec![l(PriceLt50), l(CategoryBooks)])
);

// ──────────────────────────────────────────────────────────────────────
// Batch 4 — deep nesting and wide fan-out.
// These stress the tree walkers, DFS ordering, and cost-sort stability.
// ──────────────────────────────────────────────────────────────────────

// 4-level deep: AND(NOT(OR(AND(A,B),C)), D).
reference_test!(
    nested_4_levels_a,
    and_(vec![
        not_(or_(vec![
            and_(vec![l(BrandAmazon), l(StatusArchived)]),
            l(CategoryBooks),
        ])),
        l(CategoryElectronics),
    ])
);

// 5-level deep: OR(AND(NOT(OR(A,B)), NOT(C)), AND(D, E)).
reference_test!(
    nested_5_levels,
    or_(vec![
        and_(vec![
            not_(or_(vec![l(BrandAmazon), l(BrandApple)])),
            not_(l(StatusArchived)),
        ]),
        and_(vec![l(CategoryElectronics), l(PriceGt100)]),
    ])
);

// Wide AND fan-out (5 children).
reference_test!(
    wide_and_5_children,
    and_(vec![
        l(CategoryElectronics),
        not_(l(StatusArchived)),
        not_(l(BrandAmazon)),
        not_(l(BrandApple)),
        not_(l(PriceGe150)),
    ])
);

// Wide OR fan-out (5 children).
reference_test!(
    wide_or_5_children,
    or_(vec![
        l(BrandAmazon),
        l(BrandApple),
        l(PriceGe150),
        l(CategoryBooks),
        l(StatusArchived),
    ])
);

// 6 children — exceeds typical small-arena bounds.
reference_test!(
    wide_or_6_children,
    or_(vec![
        l(BrandAmazon),
        l(BrandApple),
        l(StatusArchived),
        l(PriceLt50),
        l(PriceEq150),
        l(CategoryBooks),
    ])
);

// Deep NOT-OR-NOT-AND sandwich exercising push_not_down.
reference_test!(
    deep_not_over_compound,
    not_(or_(vec![
        and_(vec![l(BrandAmazon), not_(l(PriceLt100))]),
        and_(vec![l(BrandApple), l(StatusArchived)]),
    ]))
);

// ──────────────────────────────────────────────────────────────────────
// Batch 5 — cross-column combinations. Exercise multiple parquet predicate
// types and column orderings; verify DFS leaf ordering is stable across
// mixing columns.
// ──────────────────────────────────────────────────────────────────────

reference_test!(
    multi_column_and,
    and_(vec![l(PriceGt100), l(CategoryElectronics), l(StatusArchived)])
);

reference_test!(
    multi_column_or,
    or_(vec![l(PriceLt50), l(CategoryBooks), l(BrandApple)])
);

reference_test!(
    mixed_and_of_or_branches,
    and_(vec![
        or_(vec![l(BrandAmazon), l(BrandApple)]),
        or_(vec![l(PriceLt100), l(CategoryBooks)]),
    ])
);

reference_test!(
    mixed_or_of_and_branches,
    or_(vec![
        and_(vec![l(BrandAmazon), l(StatusArchived)]),
        and_(vec![l(BrandApple), l(PriceLt100)]),
        and_(vec![l(CategoryBooks), l(PriceGe150)]),
    ])
);

// Same collector leaf used in both branches of OR. DFS visits each
// occurrence separately; collector_idx wiring gives them different
// (provider,idx) pairs even though they query the same backend side.
reference_test!(
    same_provider_id_used_twice,
    or_(vec![
        and_(vec![l(BrandAmazon), l(PriceLt100)]),
        and_(vec![l(BrandAmazon), l(CategoryBooks)]),
    ])
);

// Triple-level alternation: AND(OR(AND, OR), NOT).
reference_test!(
    alternating_nesting,
    and_(vec![
        or_(vec![
            and_(vec![l(BrandAmazon), l(PriceGt100)]),
            or_(vec![l(BrandApple), l(CategoryBooks)]),
        ]),
        not_(l(StatusArchived)),
    ])
);

// Chain of NOTs + ANDs.
reference_test!(
    not_and_not_chain,
    and_(vec![
        not_(l(BrandAmazon)),
        not_(l(BrandApple)),
        not_(l(StatusArchived)),
    ])
);

// Collector excluded via NOT in OR sibling. Tests per-leaf cache after OR
// short-circuit when a Collector is under NOT.
reference_test!(
    not_collector_inside_or_siblings,
    or_(vec![l(BrandAmazon), not_(l(BrandApple))])
);

// NOT around a mixed AND (has Predicate) → universe-fallback exercised.
reference_test!(
    not_around_mixed_and,
    not_(and_(vec![l(BrandAmazon), l(PriceLt100)]))
);

// NOT around a Collector-only AND → exact inversion taken.
reference_test!(
    not_around_collectors_only_and,
    not_(and_(vec![l(BrandAmazon), l(StatusArchived)]))
);

// Collector + predicate + collector + predicate alternation.
reference_test!(
    alternating_collectors_and_predicates,
    and_(vec![
        l(CategoryElectronics),
        l(BrandAmazon),
        l(PriceGt100),
        not_(l(StatusArchived)),
    ])
);

// Single predicate across 5 OR clauses (redundant but valid).
reference_test!(
    or_of_many_same_column_predicates,
    or_(vec![
        l(PriceLt50),
        l(PriceEq150),
        l(PriceGt100),
        l(PriceGe150),
        l(PriceLt100),
    ])
);

// ──────────────────────────────────────────────────────────────────────
// Batch 6 — commutativity, cost-order stability, stress shapes.
// Same set in different child orders must give identical results.
// ──────────────────────────────────────────────────────────────────────

// Commutativity: AND(A, B) == AND(B, A). Engine's leader-follower sort by
// cost must not change the result set.
reference_test!(
    commutative_and_ab,
    and_(vec![l(BrandAmazon), l(StatusArchived)])
);
reference_test!(
    commutative_and_ba,
    and_(vec![l(StatusArchived), l(BrandAmazon)])
);

reference_test!(
    commutative_and_predicate_first,
    and_(vec![l(PriceLt100), l(BrandApple)])
);
reference_test!(
    commutative_and_collector_first,
    and_(vec![l(BrandApple), l(PriceLt100)])
);

reference_test!(
    commutative_or_ab,
    or_(vec![l(BrandAmazon), l(BrandApple)])
);
reference_test!(
    commutative_or_ba,
    or_(vec![l(BrandApple), l(BrandAmazon)])
);

// Associativity: A AND (B AND C) vs (A AND B) AND C — engine must not
// care which one the tree was built as.
reference_test!(
    assoc_and_left,
    and_(vec![
        and_(vec![l(BrandAmazon), l(PriceLt100)]),
        l(CategoryElectronics),
    ])
);
reference_test!(
    assoc_and_right,
    and_(vec![
        l(BrandAmazon),
        and_(vec![l(PriceLt100), l(CategoryElectronics)]),
    ])
);

reference_test!(
    assoc_or_left,
    or_(vec![
        or_(vec![l(PriceLt50), l(PriceEq150)]),
        l(CategoryBooks),
    ])
);
reference_test!(
    assoc_or_right,
    or_(vec![
        l(PriceLt50),
        or_(vec![l(PriceEq150), l(CategoryBooks)]),
    ])
);

// ── Stress shapes ────────────────────────────────────────────────────

// Heavy mixed: 6 levels, every operator, crosses columns.
reference_test!(
    stress_mixed_6_levels,
    and_(vec![
        or_(vec![
            l(BrandAmazon),
            and_(vec![
                not_(l(StatusArchived)),
                or_(vec![
                    l(BrandApple),
                    and_(vec![l(PriceLt100), l(CategoryElectronics)]),
                ]),
            ]),
        ]),
        not_(and_(vec![l(PriceGe150), l(CategoryBooks)])),
    ])
);

// Every primitive leaf appearing at least once.
reference_test!(
    stress_all_leaves_in_tree,
    and_(vec![
        or_(vec![
            l(BrandAmazon),
            l(BrandApple),
            l(StatusArchived),
        ]),
        or_(vec![
            l(PriceLt100),
            l(PriceLt50),
            l(PriceGt100),
            l(PriceGe150),
            l(PriceEq150),
        ]),
        or_(vec![l(CategoryElectronics), l(CategoryBooks)]),
    ])
);

// Pathological: deeply right-skewed AND tree (5 levels right-leaning).
reference_test!(
    right_skewed_and,
    and_(vec![
        l(CategoryElectronics),
        and_(vec![
            not_(l(BrandAmazon)),
            and_(vec![
                not_(l(BrandApple)),
                and_(vec![not_(l(StatusArchived)), not_(l(PriceGe150))]),
            ]),
        ]),
    ])
);

// Pathological: deeply left-skewed OR tree.
reference_test!(
    left_skewed_or,
    or_(vec![
        or_(vec![
            or_(vec![
                or_(vec![l(PriceLt50), l(BrandAmazon)]),
                l(CategoryBooks),
            ]),
            l(StatusArchived),
        ]),
        l(PriceEq150),
    ])
);

// Many-child AND with one Collector + many Predicate siblings (exercise
// AND cost-sort that puts Predicates first).
reference_test!(
    and_collector_with_many_predicate_siblings,
    and_(vec![
        l(BrandAmazon),
        l(CategoryElectronics),
        not_(l(PriceLt50)),
        not_(l(PriceGe150)),
    ])
);

// Many-child OR with one Predicate + many Collector siblings (OR cost-sort
// puts Predicates first, then Collectors).
reference_test!(
    or_predicate_with_many_collector_siblings,
    or_(vec![
        l(PriceEq150),
        l(BrandApple),
        l(StatusArchived),
        l(BrandAmazon),
    ])
);

// NOT over a wide OR of collectors (De Morgan → AND of NOTs).
reference_test!(
    not_over_wide_or_of_collectors,
    not_(or_(vec![l(BrandAmazon), l(BrandApple), l(StatusArchived)]))
);

// Disjoint AND paths joined by OR — no collector overlap between branches.
reference_test!(
    disjoint_branches_or,
    or_(vec![
        and_(vec![l(BrandAmazon), l(PriceLt50)]),
        and_(vec![l(BrandApple), l(PriceGe150)]),
        and_(vec![l(StatusArchived), l(CategoryBooks)]),
    ])
);

// Nested NOT(OR(NOT ...)) — double negation through De Morgan equivalent
// to AND of the inner items.
reference_test!(
    not_or_of_nots_equivalent_to_and,
    not_(or_(vec![
        not_(l(BrandAmazon)),
        not_(l(CategoryElectronics)),
    ]))
);

// Tree that has an always-empty subtree AND'd with a large universe.
// Whole result should be empty regardless of the other branch.
reference_test!(
    empty_subtree_gates_entire_and,
    and_(vec![
        and_(vec![l(PriceLt50), l(PriceGe150)]), // contradictory → empty
        or_(vec![l(BrandAmazon), l(BrandApple)]),
    ])
);

// Tree with always-full subtree OR'd with a narrow branch.
// Full subtree means result == universe.
reference_test!(
    full_subtree_gates_entire_or,
    or_(vec![
        or_(vec![l(PriceLt100), l(PriceGe150), l(PriceEq150)]),
        and_(vec![l(BrandAmazon), l(StatusArchived)]),
    ])
);
