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
async fn run_missing_col_tree(tree_bool: BoolNode, predicates: Vec<ResolvedPredicate>) -> usize {
    let f = missing_col_fixture();
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
        max_doc: MISSING_N as i64,
        object_path: object_store::path::Path::from(f.path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
    };

    let tree = Arc::new(tree_bool);
    let predicates: Arc<Vec<Arc<ResolvedPredicate>>> = Arc::new(predicates.into_iter().map(Arc::new).collect());
    let factory: super::super::table_provider::EvaluatorFactory = {
        let predicates = Arc::clone(&predicates);
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk| {
            let resolved = tree.resolve(&[], &predicates)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata), &[]));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(CollectorLeafBitmaps),
                page_pruner: pruner,
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
        num_partitions: 1,
        force_strategy: Some(FilterStrategy::BooleanMask),
        force_pushdown: Some(false),
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

fn only_pred(p: ResolvedPredicate) -> (BoolNode, Vec<ResolvedPredicate>) {
    (BoolNode::Predicate { predicate_id: 0 }, vec![p])
}
fn not_pred(p: ResolvedPredicate) -> (BoolNode, Vec<ResolvedPredicate>) {
    (
        BoolNode::Not(Box::new(BoolNode::Predicate { predicate_id: 0 })),
        vec![p],
    )
}

// 1. Predicate on missing column -> all UNKNOWN -> 0 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_col_predicate_returns_zero_rows() {
    let (tree, preds) = only_pred(pred_int("missing_col", Operator::GtEq, 0));
    assert_eq!(run_missing_col_tree(tree, preds).await, 0);
}

// 2. NOT(missing-col predicate) -> UNKNOWN -> 0 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_col_not_predicate_returns_zero_rows() {
    let (tree, preds) = not_pred(pred_int("missing_col", Operator::GtEq, 0));
    assert_eq!(run_missing_col_tree(tree, preds).await, 0);
}

// 3. AND(existing, missing): all rows UNKNOWN via missing branch -> 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_col_and_with_existing_returns_zero() {
    let preds = vec![
        pred_str("name", Operator::Eq, "foo"),   // matches half the rows
        pred_int("missing_col", Operator::GtEq, 0), // always UNKNOWN
    ];
    let tree = BoolNode::And(vec![
        BoolNode::Predicate { predicate_id: 0 },
        BoolNode::Predicate { predicate_id: 1 },
    ]);
    assert_eq!(run_missing_col_tree(tree, preds).await, 0);
}

// 4. OR(existing, missing): existing branch dominates. Expected = name='foo'
// row count (half of MISSING_N).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_col_or_with_existing_keeps_existing() {
    let preds = vec![
        pred_str("name", Operator::Eq, "foo"),
        pred_int("missing_col", Operator::GtEq, 0),
    ];
    let tree = BoolNode::Or(vec![
        BoolNode::Predicate { predicate_id: 0 },
        BoolNode::Predicate { predicate_id: 1 },
    ]);
    let expected = missing_col_fixture().name.iter().filter(|n| **n == "foo").count();
    assert_eq!(run_missing_col_tree(tree, preds).await, expected);
}

// 5. Nested: (name='foo' AND score<500) OR (missing_col=42). The missing_col
// branch never matches; result = (name='foo' AND score<500) rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_col_nested_or_kept_existing_only() {
    let preds = vec![
        pred_str("name", Operator::Eq, "foo"),
        pred_int("score", Operator::Lt, 500),
        pred_int("missing_col", Operator::Eq, 42),
    ];
    let tree = BoolNode::Or(vec![
        BoolNode::And(vec![
            BoolNode::Predicate { predicate_id: 0 },
            BoolNode::Predicate { predicate_id: 1 },
        ]),
        BoolNode::Predicate { predicate_id: 2 },
    ]);
    let f = missing_col_fixture();
    let expected = (0..MISSING_N).filter(|&i| f.name[i] == "foo" && f.score[i] < 500).count();
    assert_eq!(run_missing_col_tree(tree, preds).await, expected);
}
