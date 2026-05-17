/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Schema drift — predicates referencing columns that are absent from a
//! segment's parquet file. Common when a mapping added the field after
//! the segment was written. Expected product behaviour: every row in that
//! segment evaluates to UNKNOWN for that predicate and gets filtered out.

use super::*;

// ══════════════════════════════════════════════════════════════════════
// Missing-column fixture — parquet written without a column that the
// tree still references. Simulates a segment written before a mapping
// added the field: parquet literally has no such column, so Phase 2's
// column lookup on the RecordBatch returns None and our product code
// treats that as all-UNKNOWN (SQL semantics).
// ══════════════════════════════════════════════════════════════════════

const MISSING_N: usize = 2048;

struct MissingColFixture {
    path: std::path::PathBuf,
    // Column caches — only for columns actually in the parquet.
    name: Vec<&'static str>,
    score: Vec<i32>,
}

fn missing_col_fixture() -> &'static MissingColFixture {
    static CELL: OnceLock<MissingColFixture> = OnceLock::new();
    CELL.get_or_init(build_missing_col_fixture)
}

fn build_missing_col_fixture() -> MissingColFixture {
    let mut name = Vec::with_capacity(MISSING_N);
    let mut score = Vec::with_capacity(MISSING_N);
    for i in 0..MISSING_N {
        name.push(if i % 2 == 0 { "foo" } else { "bar" });
        score.push((i as i32) % 1000);
    }
    // Parquet schema intentionally excludes `missing_col`.
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(name.clone())),
            Arc::new(Int32Array::from(score.clone())),
        ],
    )
    .unwrap();
    let tmp = NamedTempFile::new().unwrap();
    let (file, path) = tmp.keep().unwrap();
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(512)
        .set_data_page_row_count_limit(128)
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    MissingColFixture { path, name, score }
}

/// Run a tree whose predicates may reference `missing_col` (not in parquet
/// schema). Returns the row count the engine emits.
async fn run_missing_col_tree(tree_bool: BoolNode) -> usize {
    let f = missing_col_fixture();
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
        max_doc: MISSING_N as i64,
        object_path: object_store::path::Path::from(f.path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
            global_base: 0,
    };

    let tree = Arc::new(tree_bool);
    let factory: super::super::table_provider::EvaluatorFactory = {
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics| {
            let resolved = tree.resolve(&[])?;
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
    // SELECT only columns that exist in parquet.
    let df = ctx.sql("SELECT name, score FROM t").await.unwrap();
    let mut stream = df.execute_stream().await.unwrap();
    let mut count = 0;
    while let Some(b) = stream.next().await {
        count += b.unwrap().num_rows();
    }
    count
}

/// Local schema that *includes* missing_col — used for building
/// PhysicalExprs that reference a column absent from the parquet file.
/// The evaluator's refinement stage handles the missing column (emits
/// UNKNOWN) per its normal semantics.
fn schema_with_missing() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
        Field::new("missing_col", DataType::Int32, true),
    ]))
}

fn pred_missing_int(col: &str, op: Operator, v: i32) -> BoolNode {
    let schema = schema_with_missing();
    let col_idx = schema.index_of(col).expect("column in local schema");
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

fn pred_missing_str(col: &str, op: Operator, v: &str) -> BoolNode {
    let schema = schema_with_missing();
    let col_idx = schema.index_of(col).expect("column in local schema");
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

fn only_pred(p: BoolNode) -> BoolNode {
    p
}
fn not_pred(p: BoolNode) -> BoolNode {
    BoolNode::Not(Box::new(p))
}

// 1. Predicate on missing column -> all UNKNOWN -> 0 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_col_predicate_returns_zero_rows() {
    let tree = only_pred(pred_missing_int("missing_col", Operator::GtEq, 0));
    assert_eq!(run_missing_col_tree(tree).await, 0);
}

// 2. NOT(missing-col predicate) -> UNKNOWN -> 0 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_col_not_predicate_returns_zero_rows() {
    let tree = not_pred(pred_missing_int("missing_col", Operator::GtEq, 0));
    assert_eq!(run_missing_col_tree(tree).await, 0);
}

// 3. AND(existing, missing): all rows UNKNOWN via missing branch -> 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_col_and_with_existing_returns_zero() {
    let tree = BoolNode::And(vec![
        pred_missing_str("name", Operator::Eq, "foo"),
        pred_missing_int("missing_col", Operator::GtEq, 0),
    ]);
    assert_eq!(run_missing_col_tree(tree).await, 0);
}

// 4. OR(existing, missing): existing branch dominates. Expected = name='foo'
// row count (half of MISSING_N).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_col_or_with_existing_keeps_existing() {
    let tree = BoolNode::Or(vec![
        pred_missing_str("name", Operator::Eq, "foo"),
        pred_missing_int("missing_col", Operator::GtEq, 0),
    ]);
    let expected = missing_col_fixture()
        .name
        .iter()
        .filter(|n| **n == "foo")
        .count();
    assert_eq!(run_missing_col_tree(tree).await, expected);
}

// 5. Nested: (name='foo' AND score<500) OR (missing_col=42). The missing_col
// branch never matches; result = (name='foo' AND score<500) rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_col_nested_or_kept_existing_only() {
    let tree = BoolNode::Or(vec![
        BoolNode::And(vec![
            pred_missing_str("name", Operator::Eq, "foo"),
            pred_missing_int("score", Operator::Lt, 500),
        ]),
        pred_missing_int("missing_col", Operator::Eq, 42),
    ]);
    let f = missing_col_fixture();
    let expected = (0..MISSING_N)
        .filter(|&i| f.name[i] == "foo" && f.score[i] < 500)
        .count();
    assert_eq!(run_missing_col_tree(tree).await, expected);
}

// 6. Page pruner direct invariant: a predicate that ANDs an existing
// column with a missing column must prune identically to the
// existing-column predicate alone — the missing-column clause
// contributes only "unknown" per grid cell, so it can neither add nor
// remove page selections beyond what the existing column decides.
//
// Pre-fix, this assertion failed because `prune_rg` bailed out to
// `None` whenever any referenced column was absent from the parquet
// file. Post-fix, the missing column contributes typed all-null stats
// per grid cell and the existing-column clause still prunes.
#[test]
fn page_pruner_prunes_existing_column_despite_missing_column() {
    use crate::indexed_table::page_pruner::{build_pruning_predicate, PagePruner};
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

    let f = missing_col_fixture();
    let file = std::fs::File::open(&f.path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    // Schema handed to the pruner includes the missing column.
    let drift_schema = schema_with_missing();
    let pruner = PagePruner::new(&drift_schema, meta.metadata().clone());

    // Existing-column predicate: score < 100 (fixture has score = i % 1000
    // so ~10% of rows match, concentrated at the start of every 1000-row
    // cycle → some pages will be prunable).
    let score_idx = drift_schema.index_of("score").unwrap();
    let score_col: std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr> = std::sync::Arc::new(
        datafusion::physical_expr::expressions::Column::new("score", score_idx),
    );
    let lit_100: std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr> = std::sync::Arc::new(
        datafusion::physical_expr::expressions::Literal::new(ScalarValue::Int32(Some(100))),
    );
    let score_lt: std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr> = std::sync::Arc::new(
        datafusion::physical_expr::expressions::BinaryExpr::new(
            score_col,
            Operator::Lt,
            lit_100,
        ),
    );
    let pp_solo = build_pruning_predicate(&score_lt, drift_schema.clone())
        .expect("score<100 is not always_true on this fixture");

    // Combined predicate: score < 100 AND missing_col > 0. Missing
    // column has all-null stats → clause evaluates to unknown per
    // grid cell → AND combines as "score<100 AND unknown".
    let missing_idx = drift_schema.index_of("missing_col").unwrap();
    let missing_col: std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr> =
        std::sync::Arc::new(datafusion::physical_expr::expressions::Column::new(
            "missing_col",
            missing_idx,
        ));
    let lit_0: std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr> = std::sync::Arc::new(
        datafusion::physical_expr::expressions::Literal::new(ScalarValue::Int32(Some(0))),
    );
    let missing_gt: std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr> =
        std::sync::Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
            missing_col,
            Operator::Gt,
            lit_0,
        ));
    let combined: std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr> =
        std::sync::Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
            score_lt.clone(),
            Operator::And,
            missing_gt,
        ));
    let pp_combined = build_pruning_predicate(&combined, drift_schema)
        .expect("combined predicate not always_true");

    // Walk every RG in the fixture and check the invariant. For each
    // RG, the combined selection must keep at most as many rows as the
    // solo selection (the missing-column clause is unknown → it can
    // only downgrade, never add rows). If the combined predicate
    // caused the pruner to bail out (returning `None` where solo
    // returned `Some`), that's the pre-fix regression we're guarding
    // against.
    for rg_idx in 0..meta.metadata().num_row_groups() {
        let solo = pruner.prune_rg(&pp_solo, rg_idx, None);
        let combined = pruner.prune_rg(&pp_combined, rg_idx, None);
        match (solo.as_ref(), combined.as_ref()) {
            (Some(s), Some(c)) => {
                let solo_kept: usize = s.iter().filter(|r| !r.skip).map(|r| r.row_count).sum();
                let combined_kept: usize =
                    c.iter().filter(|r| !r.skip).map(|r| r.row_count).sum();
                assert!(
                    combined_kept <= solo_kept,
                    "rg {}: combined selection kept {} rows, solo kept {} — \
                     missing-column clause must not add rows",
                    rg_idx,
                    combined_kept,
                    solo_kept
                );
            }
            (Some(_), None) => {
                panic!(
                    "rg {}: pruner bailed out on combined predicate despite \
                     present column — missing-column fix regression",
                    rg_idx
                );
            }
            _ => {}
        }
    }
}
