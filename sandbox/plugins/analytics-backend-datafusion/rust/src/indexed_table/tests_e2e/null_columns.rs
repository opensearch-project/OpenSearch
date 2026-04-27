/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! NULL-column edge cases: columns that are entirely NULL in a segment,
//! NULL in only some row groups, or present in some row groups and NULL
//! in others. Mock collectors are scoped to specific RG ranges so we can
//! test "collector empty in some RGs" alongside "predicate NULL in some
//! RGs" combinations that the random-distribution 10k fixture rarely hits.

use super::*;


// ══════════════════════════════════════════════════════════════════════
// Null-density fixture — 4096 rows, 4 RGs, columns deliberately NULL at
// different granularities.
// ══════════════════════════════════════════════════════════════════════
//
// Rows | Layout
// 0..1024  (RG0): all_null_col NULL, mostly_null_col NULL, half_null_col NULL
// 1024..2048 (RG1): all_null_col NULL, mostly_null_col NULL, half_null_col present
// 2048..3072 (RG2): all_null_col NULL, mostly_null_col present, half_null_col NULL
// 3072..4096 (RG3): all_null_col NULL, mostly_null_col present, half_null_col present
//
// Collector mocks can be scoped to specific RGs to test "collector matches
// nothing in a whole RG" / "matches only in one RG" scenarios.

const NULL_N: usize = 4096;

struct NullFixture {
    path: std::path::PathBuf,
    // Column caches for the reference evaluator.
    all_null: Vec<Option<i32>>,    // always None
    mostly_null: Vec<Option<i32>>, // None in RG0,RG1; Some in RG2,RG3
    half_null: Vec<Option<i32>>,   // None in RG0,RG2; Some in RG1,RG3
    tag: Vec<&'static str>,        // non-null always
}

fn null_fixture() -> &'static NullFixture {
    static CELL: OnceLock<NullFixture> = OnceLock::new();
    CELL.get_or_init(build_null_fixture)
}

fn build_null_fixture() -> NullFixture {
    let mut all_null: Vec<Option<i32>> = Vec::with_capacity(NULL_N);
    let mut mostly_null: Vec<Option<i32>> = Vec::with_capacity(NULL_N);
    let mut half_null: Vec<Option<i32>> = Vec::with_capacity(NULL_N);
    let mut tag: Vec<&'static str> = Vec::with_capacity(NULL_N);

    for i in 0..NULL_N {
        all_null.push(None);
        let rg = i / 1024;
        mostly_null.push(if rg < 2 { None } else { Some((i as i32) % 200) });
        half_null.push(if rg % 2 == 0 { None } else { Some((i as i32) % 500) });
        tag.push(if i % 2 == 0 { "even" } else { "odd" });
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("all_null_col", DataType::Int32, true),
        Field::new("mostly_null_col", DataType::Int32, true),
        Field::new("half_null_col", DataType::Int32, true),
        Field::new("tag", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(all_null.clone())),
            Arc::new(Int32Array::from(mostly_null.clone())),
            Arc::new(Int32Array::from(half_null.clone())),
            Arc::new(StringArray::from(tag.clone())),
        ],
    )
    .unwrap();

    let tmp = NamedTempFile::new().unwrap();
    let (file, path) = tmp.keep().unwrap();
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(1024)
        .set_data_page_row_count_limit(256)
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    NullFixture {
        path,
        all_null,
        mostly_null,
        half_null,
        tag,
    }
}

/// Collector that matches only rows in a specific RG range.
#[derive(Debug)]
struct RgScopedCollector {
    matching_rows: Vec<i32>, // absolute doc IDs to match
}

impl RowGroupDocsCollector for RgScopedCollector {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        let span = (max_doc - min_doc) as usize;
        let mut out = vec![0u64; span.div_ceil(64)];
        for &doc in &self.matching_rows {
            if doc >= min_doc && doc < max_doc {
                let rel = (doc - min_doc) as usize;
                out[rel / 64] |= 1u64 << (rel % 64);
            }
        }
        Ok(out)
    }
}

/// Leaf types for null-fixture tests.
#[derive(Debug, Clone)]
enum NullLeaf {
    // Collectors: explicitly scoped to specific RGs via an absolute row set.
    Collector(Vec<i32>),
    // Predicates against NULL-laden columns.
    AllNullGe(i32),
    MostlyNullGe(i32),
    HalfNullGe(i32),
    MostlyNullEq(i32),
    TagEq(&'static str),
}

#[derive(Debug, Clone)]
enum NT {
    Leaf(NullLeaf),
    And(Vec<NT>),
    Or(Vec<NT>),
    Not(Box<NT>),
}

fn reference_evaluator_null_leaf(leaf: &NullLeaf, row: usize) -> Option<bool> {
    let f = null_fixture();
    match leaf {
        NullLeaf::Collector(matching) => Some(matching.contains(&(row as i32))),
        NullLeaf::AllNullGe(v) => f.all_null[row].map(|x| x >= *v),
        NullLeaf::MostlyNullGe(v) => f.mostly_null[row].map(|x| x >= *v),
        NullLeaf::HalfNullGe(v) => f.half_null[row].map(|x| x >= *v),
        NullLeaf::MostlyNullEq(v) => f.mostly_null[row].map(|x| x == *v),
        NullLeaf::TagEq(s) => Some(f.tag[row] == *s),
    }
}

fn reference_evaluator_null(tree: &NT, row: usize) -> Option<bool> {
    match tree {
        NT::Leaf(l) => reference_evaluator_null_leaf(l, row),
        NT::Not(inner) => reference_evaluator_null(inner, row).map(|b| !b),
        NT::And(cs) => {
            let mut u = false;
            for c in cs {
                match reference_evaluator_null(c, row) {
                    Some(false) => return Some(false),
                    None => u = true,
                    Some(true) => {}
                }
            }
            if u { None } else { Some(true) }
        }
        NT::Or(cs) => {
            let mut u = false;
            for c in cs {
                match reference_evaluator_null(c, row) {
                    Some(true) => return Some(true),
                    None => u = true,
                    Some(false) => {}
                }
            }
            if u { None } else { Some(false) }
        }
    }
}

/// Lower `NT` to `BoolNode`. Collector leaves embed their DFS index as a
/// single byte in `query_bytes`; `wire_null` uses that byte to look up
/// the matching-set for the leaf.
fn to_engine_tree_null(tree: &NT, coll_seq: &mut u8) -> BoolNode {
    fn null_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("all_null_col", DataType::Int32, true),
            Field::new("mostly_null_col", DataType::Int32, true),
            Field::new("half_null_col", DataType::Int32, true),
            Field::new("tag", DataType::Utf8, false),
        ]))
    }
    fn pred_int_local(col: &str, op: Operator, v: i32) -> BoolNode {
        let schema = null_schema();
        let col_idx = schema.index_of(col).expect("null column");
        let left: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new(col, col_idx));
        let right: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Literal::new(
                ScalarValue::Int32(Some(v)),
            ));
        BoolNode::Predicate(Arc::new(
            datafusion::physical_expr::expressions::BinaryExpr::new(left, op, right),
        ))
    }
    fn pred_str_local(col: &str, op: Operator, v: &str) -> BoolNode {
        let schema = null_schema();
        let col_idx = schema.index_of(col).expect("null column");
        let left: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Column::new(col, col_idx));
        let right: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Literal::new(
                ScalarValue::Utf8(Some(v.to_string())),
            ));
        BoolNode::Predicate(Arc::new(
            datafusion::physical_expr::expressions::BinaryExpr::new(left, op, right),
        ))
    }
    match tree {
        NT::Leaf(NullLeaf::Collector(_)) => {
            let tag = *coll_seq;
            *coll_seq += 1;
            BoolNode::Collector { query_bytes: Arc::from(&[tag][..]) }
        }
        NT::Leaf(NullLeaf::AllNullGe(v)) => pred_int_local("all_null_col", Operator::GtEq, *v),
        NT::Leaf(NullLeaf::MostlyNullGe(v)) => pred_int_local("mostly_null_col", Operator::GtEq, *v),
        NT::Leaf(NullLeaf::HalfNullGe(v)) => pred_int_local("half_null_col", Operator::GtEq, *v),
        NT::Leaf(NullLeaf::MostlyNullEq(v)) => pred_int_local("mostly_null_col", Operator::Eq, *v),
        NT::Leaf(NullLeaf::TagEq(s)) => pred_str_local("tag", Operator::Eq, s),
        NT::Not(inner) => BoolNode::Not(Box::new(to_engine_tree_null(inner, coll_seq))),
        NT::And(cs) => BoolNode::And(cs.iter().map(|c| to_engine_tree_null(c, coll_seq)).collect()),
        NT::Or(cs) => BoolNode::Or(cs.iter().map(|c| to_engine_tree_null(c, coll_seq)).collect()),
    }
}

/// Walk `tree` in DFS order and build one `RgScopedCollector` per Collector
/// leaf, looked up by the tag byte the leaf carries.
fn wire_null(bt: &BoolNode, matching_sets: &[Vec<i32>]) -> Vec<Arc<dyn RowGroupDocsCollector>> {
    let mut out: Vec<Arc<dyn RowGroupDocsCollector>> = Vec::new();
    wire_null_rec(bt, matching_sets, &mut out);
    out
}
fn wire_null_rec(
    node: &BoolNode,
    matching_sets: &[Vec<i32>],
    out: &mut Vec<Arc<dyn RowGroupDocsCollector>>,
) {
    match node {
        BoolNode::And(cs) | BoolNode::Or(cs) => {
            cs.iter().for_each(|c| wire_null_rec(c, matching_sets, out))
        }
        BoolNode::Not(inner) => wire_null_rec(inner, matching_sets, out),
        BoolNode::Collector { query_bytes } => {
            let tag = query_bytes.first().copied().expect("empty tag bytes") as usize;
            let set = &matching_sets[tag];
            out.push(Arc::new(RgScopedCollector { matching_rows: set.clone() }));
        }
        BoolNode::Predicate(_) => {}
    }
}

fn collect_matching_sets(tree: &NT, out: &mut Vec<Vec<i32>>) {
    match tree {
        NT::Leaf(NullLeaf::Collector(set)) => out.push(set.clone()),
        NT::Leaf(_) => {}
        NT::Not(inner) => collect_matching_sets(inner, out),
        NT::And(cs) | NT::Or(cs) => cs.iter().for_each(|c| collect_matching_sets(c, out)),
    }
}

async fn assert_engine_matches_reference_null(name: &str, tree: NT) {
    let f = null_fixture();
    let expected: Vec<usize> =
        (0..NULL_N).filter(|&r| reference_evaluator_null(&tree, r) == Some(true)).collect();

    let mut matching_sets = Vec::new();
    collect_matching_sets(&tree, &mut matching_sets);

    let mut seq = 0u8;
    let bt = to_engine_tree_null(&tree, &mut seq).push_not_down();
    let collectors = wire_null(&bt, &matching_sets);

    let size = std::fs::metadata(&f.path).unwrap().len();
    let file = std::fs::File::open(&f.path).unwrap();
    let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
        .unwrap();
    let schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();

    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo { index: i, first_row: offset, num_rows: n });
        offset += n;
    }
    let segment = SegmentFileInfo {
        segment_ord: 0,
        max_doc: NULL_N as i64,
        object_path: object_store::path::Path::from(f.path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
    };

    let tree = Arc::new(bt);
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
                pruning_predicates: std::sync::Arc::new(std::collections::HashMap::new()),
                page_prune_metrics: None,
            });
            Ok(eval)
        })
    };

    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store: Arc::new(object_store::local::LocalFileSystem::new()) as Arc<dyn object_store::ObjectStore>,
        store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
        evaluator_factory: factory,
        target_partitions: 1,
        force_strategy: Some(FilterStrategy::BooleanMask),
        force_pushdown: Some(false),
        query_config: std::sync::Arc::new(crate::datafusion_query_config::DatafusionQueryConfig::default()),
    }));
    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT tag, all_null_col, mostly_null_col, half_null_col FROM t").await.unwrap();
    let mut stream = df.execute_stream().await.unwrap();

    let mut count = 0;
    while let Some(batch) = stream.next().await {
        count += batch.unwrap().num_rows();
    }
    assert_eq!(
        count,
        expected.len(),
        "[{}] count mismatch: engine={} reference evaluator={}",
        name,
        count,
        expected.len()
    );
}

macro_rules! reference_test_null {
    ($name:ident, $tree:expr) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() {
            assert_engine_matches_reference_null(stringify!($name), $tree).await;
        }
    };
}

// ── The tests ───────────────────────────────────────────────────────

// 1. Predicate against a column that is entirely NULL across all segments.
// 3VL: every row → UNKNOWN → expected 0 rows.
reference_test_null!(null_predicate_on_all_null_column, NT::Leaf(NullLeaf::AllNullGe(0)));

// 2. NOT of a predicate on an all-NULL column. 3VL: NOT(UNKNOWN) = UNKNOWN →
// expected 0 rows.
reference_test_null!(
    null_not_predicate_on_all_null_column,
    NT::Not(Box::new(NT::Leaf(NullLeaf::AllNullGe(0))))
);

// 3. AND with an all-NULL predicate: tag='even' AND all_null >= 0 → 0 rows.
reference_test_null!(
    null_and_with_all_null_predicate,
    NT::And(vec![
        NT::Leaf(NullLeaf::TagEq("even")),
        NT::Leaf(NullLeaf::AllNullGe(0)),
    ])
);

// 4. OR with all-NULL predicate. tag='even' OR all_null >= 0.
// Result == tag='even' rows (SQL 3VL: TRUE OR UNKNOWN = TRUE; UNKNOWN OR
// UNKNOWN = UNKNOWN, so non-even rows are UNKNOWN and get filtered out).
reference_test_null!(
    null_or_with_all_null_predicate,
    NT::Or(vec![
        NT::Leaf(NullLeaf::TagEq("even")),
        NT::Leaf(NullLeaf::AllNullGe(0)),
    ])
);

// 5. Predicate on a column NULL in some RGs (RG0,RG1) + present in others
// (RG2,RG3). RGs with no non-null pages should prune; RGs with data should
// evaluate. Expected count: all rows in RG2,RG3 where mostly_null_col >= 0.
reference_test_null!(
    null_predicate_on_mostly_null_column,
    NT::Leaf(NullLeaf::MostlyNullGe(0))
);

// 6. Predicate narrow against mostly-null column (no match in any page).
reference_test_null!(
    null_mostly_null_no_match,
    NT::Leaf(NullLeaf::MostlyNullEq(10_000))
);

// 7. Collector matching zero rows in the whole segment. Every RG's
// prefetch_rg returns None → full segment skip, emits zero rows.
reference_test_null!(
    null_collector_empty_in_whole_segment,
    NT::Leaf(NullLeaf::Collector(vec![]))
);

// 8. Collector matching only rows in RG2 (2048..3072). RGs 0,1,3 must skip.
reference_test_null!(
    null_collector_only_in_one_rg,
    NT::Leaf(NullLeaf::Collector((2048..3072).collect()))
);

// 9. AND between a collector in RG2 and a predicate that's NULL in RG2.
// mostly_null_col is present in RG2, so predicate applies normally. Result:
// rows in RG2 whose mostly_null_col >= 100.
reference_test_null!(
    null_and_rg_scoped_collector_with_nullable_predicate,
    NT::And(vec![
        NT::Leaf(NullLeaf::Collector((2048..3072).collect())),
        NT::Leaf(NullLeaf::MostlyNullGe(100)),
    ])
);

// 10. Same shape but predicate hits a NULL-in-this-RG column: collector in
// RG2, predicate on half_null_col (NULL in RG0+RG2, present in RG1+RG3).
// RG2 has collector matches but predicate is all UNKNOWN → 0 rows.
reference_test_null!(
    null_and_rg_scoped_collector_with_null_in_that_rg,
    NT::And(vec![
        NT::Leaf(NullLeaf::Collector((2048..3072).collect())),
        NT::Leaf(NullLeaf::HalfNullGe(0)),
    ])
);

// 11. OR between a collector in RG0 (where its column is all NULL in parquet)
// and a predicate on that NULL column: collector dominates, predicate
// contributes nothing. Result: exactly the collector's matching rows.
reference_test_null!(
    null_or_collector_rg0_with_null_predicate_rg0,
    NT::Or(vec![
        NT::Leaf(NullLeaf::Collector((0..1024).step_by(7).collect())),
        NT::Leaf(NullLeaf::HalfNullGe(0)),
    ])
);
