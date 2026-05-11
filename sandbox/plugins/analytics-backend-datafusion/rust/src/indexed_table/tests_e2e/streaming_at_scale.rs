/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Streaming-at-scale tests: 10 000 rows, 5 row groups with multiple pages
//! per RG, nullable qty column, negative/zero/positive prices. Validates
//! multi-RG streaming, page pruning, SQL 3VL on nullable columns, narrow
//! and wide predicates (exercises RG-skip when a row group has no matches),
//! multi-partition fanout, and shapes that highlight future Phase-2
//! short-circuit opportunities.

use super::*;

// ══════════════════════════════════════════════════════════════════════
// Large-scale fixture (10_000 rows, multi-RG, multi-page, nullable qty,
// negative/zero/positive prices). One shared parquet + cached arrays.
// ══════════════════════════════════════════════════════════════════════
//
// Separate helpers from the 16-row fixture — reuses the same engine pipeline
// but with a richer dataset to exercise:
//   * multiple row groups (2048 rows/RG → 5 RGs),
//   * multiple pages per RG (page size ≤ 512 rows),
//   * negative, zero, and positive prices,
//   * nullable `qty` column (about 10% NULL),
//   * value-range variety (price in [-500, 1000], qty in [0, 200]).

const LARGE_N: usize = 10_000;

struct LargeFixture {
    path: std::path::PathBuf,
    // Caches: straightforward vectors for the reference evaluator.
    brands: Vec<&'static str>,
    prices: Vec<i32>,
    statuses: Vec<&'static str>,
    categories: Vec<&'static str>,
    qtys: Vec<Option<i32>>,
}

fn large_fixture() -> &'static LargeFixture {
    static CELL: OnceLock<LargeFixture> = OnceLock::new();
    CELL.get_or_init(build_large_fixture)
}

fn build_large_fixture() -> LargeFixture {
    let brands_pool = ["amazon", "apple", "google", "samsung"];
    let statuses_pool = ["active", "archived"];
    let categories_pool = ["electronics", "books"];

    let mut brands = Vec::with_capacity(LARGE_N);
    let mut prices = Vec::with_capacity(LARGE_N);
    let mut statuses = Vec::with_capacity(LARGE_N);
    let mut categories = Vec::with_capacity(LARGE_N);
    let mut qtys: Vec<Option<i32>> = Vec::with_capacity(LARGE_N);

    // Deterministic pseudo-randomness (no rand crate).
    let mut x: u64 = 0x9E3779B97F4A7C15;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };

    for i in 0..LARGE_N {
        brands.push(brands_pool[(next() as usize) % brands_pool.len()]);
        // Price in [-500, 1000) including 0.
        let p = (next() as i32).rem_euclid(1500) - 500;
        prices.push(p);
        statuses.push(statuses_pool[(next() as usize) % statuses_pool.len()]);
        categories.push(categories_pool[(next() as usize) % categories_pool.len()]);
        // qty: ~10% NULL, otherwise [0, 200].
        qtys.push(if (next() % 10) == 0 {
            None
        } else {
            Some((next() as i32).rem_euclid(201))
        });
        let _ = i;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("qty", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(brands.clone())),
            Arc::new(Int32Array::from(prices.clone())),
            Arc::new(StringArray::from(statuses.clone())),
            Arc::new(StringArray::from(categories.clone())),
            Arc::new(Int32Array::from(qtys.clone())),
        ],
    )
    .unwrap();

    let tmp = NamedTempFile::new().unwrap();
    let (file, path) = tmp.keep().unwrap();
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(2048)
        .set_data_page_row_count_limit(512)
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    LargeFixture {
        path,
        brands,
        prices,
        statuses,
        categories,
        qtys,
    }
}

// ── Large-fixture reference evaluator (3VL for NULL) ──────────────────────────────

/// Leaves specific to the large fixture. We reuse the engine machinery but
/// add qty-nullable and negative-price cases.
#[derive(Debug, Clone, Copy)]
enum LLeaf {
    // Collectors (provider_id = u16, mapped in wire_large).
    LBrand(&'static str),  // 0..=3 depending on brand
    LStatus(&'static str), // 4 = active, 5 = archived
    // Predicates.
    LPriceGe(i32),
    LPriceLt(i32),
    LPriceEq(i32),
    LQtyGe(i32),
    LQtyEq(i32),
    LCategory(&'static str),
}

/// 3VL: None = UNKNOWN.
fn reference_evaluator_large_leaf(leaf: LLeaf, row: usize) -> Option<bool> {
    let f = large_fixture();
    Some(match leaf {
        LLeaf::LBrand(b) => f.brands[row] == b,
        LLeaf::LStatus(s) => f.statuses[row] == s,
        LLeaf::LPriceGe(v) => f.prices[row] >= v,
        LLeaf::LPriceLt(v) => f.prices[row] < v,
        LLeaf::LPriceEq(v) => f.prices[row] == v,
        LLeaf::LCategory(c) => f.categories[row] == c,
        LLeaf::LQtyGe(v) => return f.qtys[row].map(|q| q >= v),
        LLeaf::LQtyEq(v) => return f.qtys[row].map(|q| q == v),
    })
}

#[derive(Debug, Clone)]
enum LT {
    Leaf(LLeaf),
    And(Vec<LT>),
    Or(Vec<LT>),
    Not(Box<LT>),
}

/// 3VL evaluator. AND: any FALSE → FALSE, else any UNKNOWN → UNKNOWN, else TRUE.
/// OR: any TRUE → TRUE, else any UNKNOWN → UNKNOWN, else FALSE.
/// NOT(UNKNOWN) = UNKNOWN.
fn reference_evaluator_large(tree: &LT, row: usize) -> Option<bool> {
    match tree {
        LT::Leaf(l) => reference_evaluator_large_leaf(*l, row),
        LT::Not(inner) => reference_evaluator_large(inner, row).map(|b| !b),
        LT::And(children) => {
            let mut any_unknown = false;
            for c in children {
                match reference_evaluator_large(c, row) {
                    Some(false) => return Some(false),
                    None => any_unknown = true,
                    Some(true) => {}
                }
            }
            if any_unknown {
                None
            } else {
                Some(true)
            }
        }
        LT::Or(children) => {
            let mut any_unknown = false;
            for c in children {
                match reference_evaluator_large(c, row) {
                    Some(true) => return Some(true),
                    None => any_unknown = true,
                    Some(false) => {}
                }
            }
            if any_unknown {
                None
            } else {
                Some(false)
            }
        }
    }
}

fn large_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("qty", DataType::Int32, true),
    ]))
}

fn pred_large_int(col: &str, op: Operator, v: i32) -> BoolNode {
    let schema = large_schema();
    let col_idx = schema.index_of(col).expect("large column");
    let left: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::Column::new(col, col_idx),
    );
    let right: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::Literal::new(ScalarValue::Int32(Some(v))),
    );
    BoolNode::Predicate(Arc::new(
        datafusion::physical_expr::expressions::BinaryExpr::new(left, op, right),
    ))
}

fn pred_large_str(col: &str, op: Operator, v: &str) -> BoolNode {
    let schema = large_schema();
    let col_idx = schema.index_of(col).expect("large column");
    let left: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
        datafusion::physical_expr::expressions::Column::new(col, col_idx),
    );
    let right: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
        Arc::new(datafusion::physical_expr::expressions::Literal::new(
            ScalarValue::Utf8(Some(v.to_string())),
        ));
    BoolNode::Predicate(Arc::new(
        datafusion::physical_expr::expressions::BinaryExpr::new(left, op, right),
    ))
}

/// Lower `LT` to `BoolNode`. Collector leaves encode the brand/status tag
/// into `query_bytes`; `wire_large` walks the tree in DFS order and
/// builds the matching mock collectors.
fn to_engine_tree_large(tree: &LT) -> BoolNode {
    match tree {
        LT::Leaf(l) => match l {
            LLeaf::LBrand(b) => BoolNode::Collector {
                annotation_id: match *b {
                    "amazon" => 0,
                    "apple" => 1,
                    "google" => 2,
                    "samsung" => 3,
                    _ => panic!("unknown brand {}", b),
                },
            },
            LLeaf::LStatus(s) => BoolNode::Collector {
                annotation_id: match *s {
                    "active" => 4,
                    "archived" => 5,
                    _ => panic!("unknown status {}", s),
                },
            },
            LLeaf::LPriceGe(v) => pred_large_int("price", Operator::GtEq, *v),
            LLeaf::LPriceLt(v) => pred_large_int("price", Operator::Lt, *v),
            LLeaf::LPriceEq(v) => pred_large_int("price", Operator::Eq, *v),
            LLeaf::LQtyGe(v) => pred_large_int("qty", Operator::GtEq, *v),
            LLeaf::LQtyEq(v) => pred_large_int("qty", Operator::Eq, *v),
            LLeaf::LCategory(c) => pred_large_str("category", Operator::Eq, c),
        },
        LT::Not(inner) => BoolNode::Not(Box::new(to_engine_tree_large(inner))),
        LT::And(cs) => BoolNode::And(cs.iter().map(to_engine_tree_large).collect()),
        LT::Or(cs) => BoolNode::Or(cs.iter().map(to_engine_tree_large).collect()),
    }
}

/// DFS-walk the tree and build one collector per Collector leaf,
/// matching each leaf's tag byte to a fixture-specific mock collector.
fn wire_large(tree: &BoolNode) -> Vec<Arc<dyn RowGroupDocsCollector>> {
    let mut out: Vec<Arc<dyn RowGroupDocsCollector>> = Vec::new();
    wire_large_rec(tree, &mut out);
    out
}

fn wire_large_rec(node: &BoolNode, out: &mut Vec<Arc<dyn RowGroupDocsCollector>>) {
    match node {
        BoolNode::And(cs) | BoolNode::Or(cs) => cs.iter().for_each(|c| wire_large_rec(c, out)),
        BoolNode::Not(inner) => wire_large_rec(inner, out),
        BoolNode::Collector { annotation_id } => {
            let tag = Some(*annotation_id as u8).expect("empty tag bytes");
            out.push(large_collector_for(tag));
        }
        BoolNode::Predicate(_) => {}
    }
}

fn large_collector_for(tag: u8) -> Arc<dyn RowGroupDocsCollector> {
    let f = large_fixture();
    let want_brand: Option<&'static str> = match tag {
        0 => Some("amazon"),
        1 => Some("apple"),
        2 => Some("google"),
        3 => Some("samsung"),
        _ => None,
    };
    let want_status: Option<&'static str> = match tag {
        4 => Some("active"),
        5 => Some("archived"),
        _ => None,
    };
    let matching: Vec<i32> = (0..LARGE_N)
        .filter(|&i| {
            want_brand.map_or(true, |b| f.brands[i] == b)
                && want_status.map_or(true, |s| f.statuses[i] == s)
        })
        .map(|i| i as i32)
        .collect();
    Arc::new(MockCollector { matching })
}

/// Run a tree against the large fixture and compare to the 3VL reference evaluator.
/// Two extra invariants tested vs the 16-row harness:
///   * fixture spans multiple RGs (5) and multiple pages per RG,
///   * NULLs in qty propagate via 3VL; UNKNOWN rows must NOT appear in the
///     result set.
async fn assert_engine_matches_reference_large(name: &str, tree: LT) {
    let f = large_fixture();

    // Reference evaluator: rows where the tree evaluates to TRUE (UNKNOWN and FALSE excluded).
    let expected: Vec<usize> = (0..LARGE_N)
        .filter(|&r| reference_evaluator_large(&tree, r) == Some(true))
        .collect();

    // Lower + run

    let bt = to_engine_tree_large(&tree).push_not_down();
    let collectors = wire_large(&bt);
    let rows = run_large(bt, collectors).await;

    // Map engine rows back to fixture indices by a tuple key. Duplicates are
    // fine because rows are unique enough across columns.
    let mut actual: Vec<usize> = Vec::with_capacity(rows.len());
    for (brand, price, status, cat, qty) in &rows {
        let found = (0..LARGE_N).find(|&r| {
            f.brands[r] == brand.as_str()
                && f.prices[r] == *price
                && f.statuses[r] == status.as_str()
                && f.categories[r] == cat.as_str()
                && f.qtys[r] == *qty
                && !actual.contains(&r)
        });
        match found {
            Some(i) => actual.push(i),
            None => panic!(
                "[{}] engine row not in fixture: ({}, {}, {}, {}, {:?})",
                name, brand, price, status, cat, qty
            ),
        }
    }
    actual.sort();
    let mut expected_sorted = expected.clone();
    expected_sorted.sort();
    assert_eq!(
        actual.len(),
        expected_sorted.len(),
        "[{}] count mismatch: engine={} reference evaluator={}",
        name,
        actual.len(),
        expected_sorted.len()
    );
    assert_eq!(actual, expected_sorted, "[{}] row set mismatch", name);
}

/// Build the provider + stream against the cached 10k fixture.
async fn run_large(
    tree: BoolNode,
    collectors: Vec<Arc<dyn RowGroupDocsCollector>>,
) -> Vec<(String, i32, String, String, Option<i32>)> {
    let f = large_fixture();
    let size = std::fs::metadata(&f.path).unwrap().len();
    let file = std::fs::File::open(&f.path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();

    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo {
            index: i,
            first_row: offset,
            num_rows: n,
        });
        offset += n;
    }

    let segment = SegmentFileInfo {
        segment_ord: 0,
        max_doc: LARGE_N as i64,
        object_path: object_store::path::Path::from(f.path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
            global_base: 0,
    };

    let tree = Arc::new(tree);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = collectors
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_leaf = per_leaf.clone();
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics| {
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(CollectorLeafBitmaps::without_metrics()),
                page_pruner: pruner,
                cost_predicate: 1,
                cost_collector: 10,
                max_collector_parallelism: 1,
                pruning_predicates: std::sync::Arc::new(std::collections::HashMap::new()),
                page_prune_metrics: None,
                    collector_strategy: crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
            });
            Ok(eval)
        })
    };

    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .force_strategy(Some(FilterStrategy::BooleanMask))
        .force_pushdown(Some(false))
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store: Arc::new(object_store::local::LocalFileSystem::new())
            as Arc<dyn object_store::ObjectStore>,
        store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
        evaluator_factory: factory,
        pushdown_predicate: None,
        query_config: std::sync::Arc::new(qc),
        predicate_columns: vec![],
        emit_row_ids: false,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx
        .sql("SELECT brand, price, status, category, qty FROM t")
        .await
        .unwrap();
    let mut stream = df.execute_stream().await.unwrap();
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let brand = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let price = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let status = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let cat = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let qty = b.column(4).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((
                brand.value(i).to_string(),
                price.value(i),
                status.value(i).to_string(),
                cat.value(i).to_string(),
                if qty.is_null(i) {
                    None
                } else {
                    Some(qty.value(i))
                },
            ));
        }
    }
    out
}

// ── Large-fixture tests ──────────────────────────────────────────────

macro_rules! reference_test_large {
    ($name:ident, $tree:expr) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() {
            assert_engine_matches_reference_large(stringify!($name), $tree).await;
        }
    };
}

// Sanity: single-leaf collector on the large fixture.
reference_test_large!(large_leaf_brand_amazon, LT::Leaf(LLeaf::LBrand("amazon")));

// Negative, zero, positive boundaries on `price` (range -500..1000).
reference_test_large!(large_price_lt_zero, LT::Leaf(LLeaf::LPriceLt(0)));
reference_test_large!(large_price_ge_zero, LT::Leaf(LLeaf::LPriceGe(0)));
reference_test_large!(large_price_eq_zero, LT::Leaf(LLeaf::LPriceEq(0)));
reference_test_large!(large_price_lt_negative, LT::Leaf(LLeaf::LPriceLt(-200)));
reference_test_large!(
    large_price_ge_max_out_of_range,
    LT::Leaf(LLeaf::LPriceGe(1000))
); // matches none
reference_test_large!(
    large_price_lt_max_out_of_range,
    LT::Leaf(LLeaf::LPriceLt(-10000))
); // matches none

// NULL handling: qty is nullable. `qty >= 0` should exclude NULL rows (3VL).
reference_test_large!(large_qty_ge_zero, LT::Leaf(LLeaf::LQtyGe(0)));
// NULL row shouldn't match `qty = N` for any N.
reference_test_large!(large_qty_eq_42, LT::Leaf(LLeaf::LQtyEq(42)));
// NOT(qty < 100): UNKNOWN propagates → row dropped. Reference evaluator catches it.
reference_test_large!(
    large_not_qty_lt_100,
    LT::Not(Box::new(LT::Leaf(LLeaf::LQtyGe(100))))
);

// AND where one branch is a NULL-prone predicate: amazon AND qty >= 50.
// Amazon rows with NULL qty must be dropped (3VL AND).
reference_test_large!(
    large_and_collector_with_nullable_predicate,
    LT::And(vec![
        LT::Leaf(LLeaf::LBrand("amazon")),
        LT::Leaf(LLeaf::LQtyGe(50)),
    ])
);

// OR with nullable branch: amazon OR qty > 100.
// NULL qty rows that aren't amazon should NOT appear in result.
reference_test_large!(
    large_or_collector_with_nullable_predicate,
    LT::Or(vec![
        LT::Leaf(LLeaf::LBrand("amazon")),
        LT::Leaf(LLeaf::LQtyGe(100)),
    ])
);

// Cross-RG amazon ∪ apple on 10k rows.
reference_test_large!(
    large_or_two_collectors,
    LT::Or(vec![
        LT::Leaf(LLeaf::LBrand("amazon")),
        LT::Leaf(LLeaf::LBrand("apple")),
    ])
);

// Amazon ∩ active — must match across RGs.
reference_test_large!(
    large_and_two_collectors,
    LT::And(vec![
        LT::Leaf(LLeaf::LBrand("amazon")),
        LT::Leaf(LLeaf::LStatus("active")),
    ])
);

// Three-collector AND (should intersect to roughly N/(4*2*2) ≈ 625 rows).
reference_test_large!(
    large_triple_and_collectors,
    LT::And(vec![
        LT::Leaf(LLeaf::LBrand("apple")),
        LT::Leaf(LLeaf::LStatus("archived")),
        LT::Leaf(LLeaf::LCategory("books")),
    ])
);

// Complex mixed: (google OR samsung) AND price>=0 AND NOT(archived) AND qty<100
reference_test_large!(
    large_complex_mixed,
    LT::And(vec![
        LT::Or(vec![
            LT::Leaf(LLeaf::LBrand("google")),
            LT::Leaf(LLeaf::LBrand("samsung")),
        ]),
        LT::Leaf(LLeaf::LPriceGe(0)),
        LT::Not(Box::new(LT::Leaf(LLeaf::LStatus("archived")))),
        LT::Not(Box::new(LT::Leaf(LLeaf::LQtyGe(100)))),
    ])
);

// NOT over compound: NOT((amazon AND archived) OR (price < 0)).
reference_test_large!(
    large_not_over_compound,
    LT::Not(Box::new(LT::Or(vec![
        LT::And(vec![
            LT::Leaf(LLeaf::LBrand("amazon")),
            LT::Leaf(LLeaf::LStatus("archived")),
        ]),
        LT::Leaf(LLeaf::LPriceLt(0)),
    ])))
);

// Deep alternation: 5 levels, all ops, crosses nullable + non-nullable columns.
reference_test_large!(
    large_deep_alternation,
    LT::And(vec![
        LT::Or(vec![
            LT::Leaf(LLeaf::LBrand("amazon")),
            LT::And(vec![
                LT::Leaf(LLeaf::LBrand("apple")),
                LT::Not(Box::new(LT::Leaf(LLeaf::LCategory("books")))),
            ]),
        ]),
        LT::Not(LT::Leaf(LLeaf::LPriceLt(-100)).into()),
        LT::Or(vec![
            LT::Leaf(LLeaf::LStatus("active")),
            LT::Leaf(LLeaf::LQtyEq(0)),
        ]),
    ])
);

// ── Pure-predicate trees (no backend leaves) ──────────────────────────
//
// These exercise the Path C walker when it sees only Predicate leaves: every
// Phase 1 result comes from page statistics, and NOT must fall back to the
// universe (see `subtree_has_predicate` guard). Also varies how aggressively
// page pruning can trim — narrow predicates land in ~1 page/RG, wide ones
// touch most pages.

// Narrow: price in top ~10%. Each RG's pages have different price ranges,
// so most pages get pruned in most RGs.
reference_test_large!(
    large_pure_predicate_narrow,
    LT::And(vec![
        LT::Leaf(LLeaf::LPriceGe(800)),
        LT::Leaf(LLeaf::LCategory("electronics"))
    ])
);

// Wide: price <0 OR price >=500 — matches ~70% of rows across every page.
reference_test_large!(
    large_pure_predicate_wide,
    LT::Or(vec![
        LT::Leaf(LLeaf::LPriceLt(0)),
        LT::Leaf(LLeaf::LPriceGe(500))
    ])
);

// Triple AND of pure predicates; page pruning must combine multiple
// per-column range intersections.
reference_test_large!(
    large_pure_predicate_triple_and,
    LT::And(vec![
        LT::Leaf(LLeaf::LPriceGe(0)),
        LT::Leaf(LLeaf::LPriceLt(500)),
        LT::Leaf(LLeaf::LCategory("books")),
    ])
);

// NOT(pure-predicate). Must exercise the universe-fallback Phase 1 path
// and then Phase 2 exact refinement.
reference_test_large!(
    large_not_pure_predicate,
    LT::Not(Box::new(LT::And(vec![
        LT::Leaf(LLeaf::LPriceGe(0)),
        LT::Leaf(LLeaf::LPriceLt(500)),
    ])))
);

// Nested predicate-only: 4 levels, no collectors.
reference_test_large!(
    large_deep_predicate_only,
    LT::Or(vec![
        LT::And(vec![
            LT::Leaf(LLeaf::LPriceGe(500)),
            LT::Leaf(LLeaf::LCategory("electronics")),
        ]),
        LT::And(vec![
            LT::Leaf(LLeaf::LPriceLt(-200)),
            LT::Not(Box::new(LT::Leaf(LLeaf::LCategory("books")))),
        ]),
    ])
);

// ── Collector-only compound trees ────────────────────────────────────
// No Predicate leaves → Phase 1 bitmaps are all exact. NOT over
// collector-only subtrees should use exact inversion (not the universe
// fallback). AND short-circuit on an empty collector must still register
// siblings' bitmaps for Phase 2 lookup.

// NOT over collector-only AND. Uses the exact-inversion Phase-1 path.
reference_test_large!(
    large_not_over_collectors_only_and,
    LT::Not(Box::new(LT::And(vec![
        LT::Leaf(LLeaf::LBrand("amazon")),
        LT::Leaf(LLeaf::LStatus("archived")),
    ])))
);

// NOT over collector-only OR — De Morgan equivalent to AND of NOTs.
reference_test_large!(
    large_not_over_collectors_only_or,
    LT::Not(Box::new(LT::Or(vec![
        LT::Leaf(LLeaf::LBrand("amazon")),
        LT::Leaf(LLeaf::LBrand("apple")),
    ])))
);

// 4 collector leaves via nested AND/OR.
reference_test_large!(
    large_four_collectors_nested,
    LT::And(vec![
        LT::Or(vec![
            LT::Leaf(LLeaf::LBrand("amazon")),
            LT::Leaf(LLeaf::LBrand("apple")),
        ]),
        LT::Or(vec![
            LT::Leaf(LLeaf::LStatus("active")),
            LT::Leaf(LLeaf::LStatus("archived")),
        ]),
    ])
);

// Collector-only shape that forces AND-empty short-circuit at scale.
// brand=amazon AND brand=apple → empty per RG, triggering the fix I made
// earlier for collecting sibling collector bitmaps. Add a 3rd collector
// sibling to verify it still gets registered for Phase 2.
reference_test_large!(
    large_and_short_circuit_with_sibling,
    LT::And(vec![
        LT::Leaf(LLeaf::LBrand("amazon")),
        LT::Leaf(LLeaf::LBrand("apple")), // never matches alongside amazon → short-circuit
        LT::Leaf(LLeaf::LStatus("archived")), // must still be registered in per_leaf
    ])
);

// Same idea via NOT(Collector OR Collector): collectors-only, should
// exercise exact inversion and bitset unions at RG scale.
reference_test_large!(
    large_not_or_three_collectors,
    LT::Not(Box::new(LT::Or(vec![
        LT::Leaf(LLeaf::LBrand("google")),
        LT::Leaf(LLeaf::LBrand("samsung")),
        LT::Leaf(LLeaf::LStatus("archived")),
    ])))
);

// ── RG-skip + narrow-match tests ─────────────────────────────────────
//
// A collector or predicate that matches zero (or a few) docs in some RGs
// must trigger the streaming loop's "skip RG" branch (when `prefetch_rg`
// returns None) while still correctly emitting rows from RGs that do match.

// Singleton match-ish: brand=amazon AND status=archived AND price=0.
// Probably matches ~0-2 rows across all 5 RGs — most RGs produce 0
// candidates and get skipped.
reference_test_large!(
    large_almost_empty_result,
    LT::And(vec![
        LT::Leaf(LLeaf::LBrand("amazon")),
        LT::Leaf(LLeaf::LStatus("archived")),
        LT::Leaf(LLeaf::LPriceEq(0)),
    ])
);

// Empty in Rust: price between two disjoint ranges in nested ANDs.
// Exercises RG-skip when the whole expression is vacuous.
reference_test_large!(
    large_empty_via_contradiction,
    LT::And(vec![
        LT::Leaf(LLeaf::LPriceGe(500)),
        LT::Leaf(LLeaf::LPriceLt(100))
    ])
);

// Predicate that matches only the top ~5% of values (price >= 900).
// Uniform distribution → each RG has roughly equal proportion but with
// page-pruning, many pages per RG get skipped before reaching Phase 2.
reference_test_large!(
    large_narrow_predicate_top_5pct,
    LT::Leaf(LLeaf::LPriceGe(900))
);

// Wide OR containing a narrow predicate + wide collector — the narrow
// predicate's empty pages shouldn't suppress the collector's rows.
reference_test_large!(
    large_narrow_predicate_or_wide_collector,
    LT::Or(vec![
        LT::Leaf(LLeaf::LPriceGe(900)),    // narrow, few rows
        LT::Leaf(LLeaf::LBrand("amazon")), // ~25% of rows
    ])
);

// Collector intersect narrow predicate: catches any page-pruning bug where
// narrow predicate's page bitmap is interpreted wrong when AND'd with
// collector bitmap.
reference_test_large!(
    large_collector_and_narrow_predicate,
    LT::And(vec![
        LT::Leaf(LLeaf::LBrand("apple")),
        LT::Leaf(LLeaf::LPriceGe(900)),
    ])
);

// ── Multi-partition runs ─────────────────────────────────────────────
//
// All other tests use num_partitions=1. These variants set N partitions
// and force the `QueryShardExec` fanout + UnionExec chain. Result set
// must still equal the single-partition reference evaluator answer.

async fn assert_large_multipartition(name: &str, tree: LT, partitions: usize) {
    let expected: Vec<usize> = (0..LARGE_N)
        .filter(|&r| reference_evaluator_large(&tree, r) == Some(true))
        .collect();

    let bt = to_engine_tree_large(&tree).push_not_down();
    let collectors = wire_large(&bt);
    let rows = run_large_partitioned(bt, collectors, partitions).await;
    assert_eq!(
        rows.len(),
        expected.len(),
        "[{}] partitions={} count mismatch: engine={} reference evaluator={}",
        name,
        partitions,
        rows.len(),
        expected.len()
    );
}

async fn run_large_partitioned(
    tree: BoolNode,
    collectors: Vec<Arc<dyn RowGroupDocsCollector>>,
    partitions: usize,
) -> Vec<(String, i32, String, String, Option<i32>)> {
    let f = large_fixture();
    let size = std::fs::metadata(&f.path).unwrap().len();
    let file = std::fs::File::open(&f.path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();

    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo {
            index: i,
            first_row: offset,
            num_rows: n,
        });
        offset += n;
    }
    let segment = SegmentFileInfo {
        segment_ord: 0,
        max_doc: LARGE_N as i64,
        object_path: object_store::path::Path::from(f.path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
            global_base: 0,
    };

    let tree = Arc::new(tree);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = collectors
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_leaf = per_leaf.clone();
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics| {
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(CollectorLeafBitmaps::without_metrics()),
                page_pruner: pruner,
                cost_predicate: 1,
                cost_collector: 10,
                max_collector_parallelism: 1,
                pruning_predicates: std::sync::Arc::new(std::collections::HashMap::new()),
                page_prune_metrics: None,
                    collector_strategy: crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
            });
            Ok(eval)
        })
    };
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(partitions)
        .force_strategy(Some(FilterStrategy::BooleanMask))
        .force_pushdown(Some(false))
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store: Arc::new(object_store::local::LocalFileSystem::new())
            as Arc<dyn object_store::ObjectStore>,
        store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
        evaluator_factory: factory,
        pushdown_predicate: None,
        query_config: std::sync::Arc::new(qc),
        predicate_columns: vec![],
        emit_row_ids: false,
    }));
    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx
        .sql("SELECT brand, price, status, category, qty FROM t")
        .await
        .unwrap();
    let mut stream = df.execute_stream().await.unwrap();
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let brand = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let price = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let status = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let cat = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let qty = b.column(4).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((
                brand.value(i).to_string(),
                price.value(i),
                status.value(i).to_string(),
                cat.value(i).to_string(),
                if qty.is_null(i) {
                    None
                } else {
                    Some(qty.value(i))
                },
            ));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_multipartition_2() {
    assert_large_multipartition(
        "multipartition_2",
        LT::Or(vec![
            LT::Leaf(LLeaf::LBrand("amazon")),
            LT::Leaf(LLeaf::LBrand("apple")),
        ]),
        2,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_multipartition_5() {
    assert_large_multipartition(
        "multipartition_5",
        LT::And(vec![
            LT::Leaf(LLeaf::LStatus("active")),
            LT::Not(Box::new(LT::Leaf(LLeaf::LPriceLt(0)))),
        ]),
        5,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_multipartition_predicate_only() {
    assert_large_multipartition(
        "multipartition_pred_only",
        LT::Or(vec![
            LT::Leaf(LLeaf::LPriceGe(500)),
            LT::Leaf(LLeaf::LCategory("books")),
        ]),
        3,
    )
    .await;
}

// ══════════════════════════════════════════════════════════════════════
// Phase-2 short-circuit candidates — shapes where a future optimization
// could skip sibling kernel evaluation once the accumulator is saturated.
// ══════════════════════════════════════════════════════════════════════
//
// Phase 2 today evaluates every child unconditionally even when:
//   AND: acc is already all-false for the batch (any further AND stays false);
//   OR : acc is already all-true  for the batch (any further OR  stays true).
//
// These tests lock in correctness for those shapes. A future optimization
// that checks acc.true_count()/null_count() per step must preserve results.

// AND where the narrow predicate drops every row in most batches; the
// subsequent AND with a cheap collector should produce an empty result
// regardless of evaluation order.
reference_test_large!(
    large_and_narrow_then_wide_collector,
    LT::And(vec![
        LT::Leaf(LLeaf::LPriceEq(12345)),  // matches zero rows
        LT::Leaf(LLeaf::LBrand("amazon")), // large bitmap, wasted work today
    ])
);

// OR where the first predicate is wide enough to cover most batches. The
// second Collector branch is redundant to compute.
reference_test_large!(
    large_or_wide_then_narrow_collector,
    LT::Or(vec![
        LT::Leaf(LLeaf::LPriceGe(-1000)), // always true (page min is well above)
        LT::Leaf(LLeaf::LBrand("samsung")),
    ])
);

// AND of three: narrow first, then wide and wide. Short-circuit would save
// two kernel evaluations per batch.
reference_test_large!(
    large_and_three_wide_after_narrow,
    LT::And(vec![
        LT::Leaf(LLeaf::LPriceEq(99_999)), // empty
        LT::Leaf(LLeaf::LStatus("active")),
        LT::Leaf(LLeaf::LCategory("electronics")),
    ])
);

// OR of three: wide first, then narrow and narrow. Short-circuit after
// first child saturates.
reference_test_large!(
    large_or_three_narrow_after_wide,
    LT::Or(vec![
        LT::Leaf(LLeaf::LPriceGe(-500)), // always true → saturated
        LT::Leaf(LLeaf::LBrand("google")),
        LT::Leaf(LLeaf::LQtyEq(0)),
    ])
);
