/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Performance-leaf ownership: each `DelegationPossible` leaf is evaluated by exactly
//! one backend per row group.
//!
//! Result equality cannot catch a leaf being evaluated by both backends (a conjunct
//! evaluated twice gives the same rows), so these tests measure the work instead:
//!
//! - **Evaluations**: every leaf expression is wrapped in [`CountingExpr`], which counts
//!   the rows DataFusion evaluates it on, whichever code path does it (residual,
//!   per-RG `perf_residual`, pushdown).
//! - **Bytes read**: the object store records every fetched byte range, and the tests
//!   check them against each column chunk's byte range, per row group.
//! - **Lucene calls**: the mock delegated backend records every `(annotation_id,
//!   doc_min)` it is asked for.
//! - **Authority**: a Lucene-owned leaf whose peer returns an extra row must keep that
//!   row — proving DataFusion did not re-check it.
//!
//! The filter is split by the production `plan_single_collector_filter`, and the
//! evaluator is the production `SingleCollectorEvaluator`.
//!
//! # Fixture
//!
//! 2 RGs × 8192 rows, 256-row pages (32 pages per RG), page statistics on.
//!
//! | column   | value for global row `i` (RG-relative `j`)                               |
//! |----------|--------------------------------------------------------------------------|
//! | `id`     | `i`                                                                      |
//! | `price`  | RG0: `j` (sorted); RG1: `(j * 7919) % 8192` (a permutation, every page wide) |
//! | `status` | `["ok", "warn", "err"][i % 3]` (every page spans all values)             |
//!
//! So `price < 256` keeps 1/32 pages of RG0 (below the 5% gate → DataFusion owns it)
//! and ~all of RG1 (→ Lucene owns it); `status = 'ok'` can never be page-pruned
//! (→ Lucene owns it in both RGs).

#![cfg(test)]

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result as DfResult, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
use datafusion::physical_expr::PhysicalExpr;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use prost::bytes::Bytes;
use tempfile::NamedTempFile;

use crate::indexed_executor::{plan_single_collector_filter, SingleCollectorFilterPlan};
use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::eval::single_collector::{
    DelegatedBackendCollectorFactory, SingleCollectorEvaluator,
};
use crate::indexed_table::eval::{CollectorCallStrategy, RowGroupBitsetSource};
use crate::indexed_table::ffm_callbacks::ProviderHandle;
use crate::indexed_table::index::{CollectDocsResult, RowGroupDocsCollector};
use crate::indexed_table::page_pruner::{build_pruning_predicate, PagePruneMetrics, PagePruner};
use crate::indexed_table::stream::RowGroupInfo;
use crate::indexed_table::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};

const ROWS_PER_PAGE: usize = 256;
const RG_ROWS: usize = 8192;
const NUM_RGS: usize = 2;
const NUM_ROWS: usize = RG_ROWS * NUM_RGS;
const STATUSES: [&str; 3] = ["ok", "warn", "err"];

const COL_ID: usize = 0;
const COL_PRICE: usize = 1;
const COL_STATUS: usize = 2;

const CORRECTNESS_ID: i32 = 0;
const PRICE_LEAF_ID: i32 = 1;
const STATUS_LEAF_ID: i32 = 2;

// ── Fixture ─────────────────────────────────────────────────────────

fn fixture_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("price", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
    ]))
}

fn price_of(i: usize) -> i32 {
    let j = i % RG_ROWS;
    if i < RG_ROWS {
        j as i32
    } else {
        ((j * 7919) % RG_ROWS) as i32
    }
}

fn status_of(i: usize) -> &'static str {
    STATUSES[i % 3]
}

fn write_fixture() -> NamedTempFile {
    let schema = fixture_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..NUM_ROWS as i32)),
            Arc::new(Int32Array::from_iter_values((0..NUM_ROWS).map(price_of))),
            Arc::new(StringArray::from_iter_values((0..NUM_ROWS).map(status_of))),
        ],
    )
    .unwrap();
    let tmp = NamedTempFile::new().unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(RG_ROWS))
        .set_data_page_row_count_limit(ROWS_PER_PAGE)
        .set_write_batch_size(ROWS_PER_PAGE)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    tmp
}

fn load_segment(tmp: &NamedTempFile) -> SegmentFileInfo {
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::File::open(&path).unwrap();
    let meta = ArrowReaderMetadata::load(
        &file,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .unwrap();
    let parquet_meta = meta.metadata().clone();
    assert_eq!(parquet_meta.num_row_groups(), NUM_RGS);
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
    SegmentFileInfo {
        writer_generation: 0,
        max_doc: NUM_ROWS as i64,
        object_path: ObjPath::from(path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups: rgs,
        arrow_schema: meta.schema().clone(),
        metadata: parquet_meta,
        global_base: 0,
        sort_min: None,
        sort_max: None,
    }
}

// ── Expressions ─────────────────────────────────────────────────────

fn col(name: &str, idx: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(PhysColumn::new(name, idx))
}

fn price_lt(v: i32) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(
        col("price", COL_PRICE),
        Operator::Lt,
        Arc::new(Literal::new(ScalarValue::Int32(Some(v)))),
    ))
}

fn status_eq(v: &str) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(
        col("status", COL_STATUS),
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Utf8(Some(v.to_string())))),
    ))
}

fn id_lt(v: i32) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(
        col("id", COL_ID),
        Operator::Lt,
        Arc::new(Literal::new(ScalarValue::Int32(Some(v)))),
    ))
}

/// Transparent wrapper that counts the rows DataFusion evaluates `inner` on.
#[derive(Debug)]
struct CountingExpr {
    inner: Arc<dyn PhysicalExpr>,
    rows: Arc<AtomicUsize>,
}

impl fmt::Display for CountingExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "counting({})", self.inner)
    }
}

impl PartialEq for CountingExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.rows, &other.rows) && self.inner.as_ref() == other.inner.as_ref()
    }
}

impl Eq for CountingExpr {}

impl Hash for CountingExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.rows) as usize).hash(state);
    }
}

impl PhysicalExpr for CountingExpr {
    fn data_type(&self, input_schema: &Schema) -> DfResult<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> DfResult<bool> {
        self.inner.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DfResult<ColumnarValue> {
        self.rows.fetch_add(batch.num_rows(), Ordering::Relaxed);
        self.inner.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DfResult<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CountingExpr {
            inner: Arc::clone(&children[0]),
            rows: Arc::clone(&self.rows),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(f)
    }
}

/// A performance leaf under test: the plain expression plus its counted wrapper.
struct Leaf {
    annotation_id: i32,
    plain: Arc<dyn PhysicalExpr>,
    counted: Arc<dyn PhysicalExpr>,
    rows_evaluated: Arc<AtomicUsize>,
}

impl Leaf {
    fn new(annotation_id: i32, plain: Arc<dyn PhysicalExpr>) -> Self {
        let rows_evaluated = Arc::new(AtomicUsize::new(0));
        let counted: Arc<dyn PhysicalExpr> = Arc::new(CountingExpr {
            inner: Arc::clone(&plain),
            rows: Arc::clone(&rows_evaluated),
        });
        Self {
            annotation_id,
            plain,
            counted,
            rows_evaluated,
        }
    }

    fn node(&self) -> BoolNode {
        BoolNode::DelegationPossible {
            annotation_id: self.annotation_id,
            original_expr: Arc::clone(&self.counted),
        }
    }

    fn rows_evaluated(&self) -> usize {
        self.rows_evaluated.load(Ordering::Relaxed)
    }
}

/// Production split of the filter, with the leaves' counted wrappers in the tree so any
/// DataFusion evaluation of a leaf is observed. `CountingExpr` is opaque to
/// `PruningPredicate`, so the stats-derived pieces are rebuilt from the plain leaf
/// expressions — the same inputs production gets — to keep the owner election real.
fn plan_with_counters(
    tree: &BoolNode,
    leaves: &[&Leaf],
    schema: &SchemaRef,
) -> SingleCollectorFilterPlan {
    let mut plan = plan_single_collector_filter(tree, schema);
    assert_eq!(plan.performance_leaves.len(), leaves.len());
    for pl in &mut plan.performance_leaves {
        let leaf = leaves
            .iter()
            .find(|l| l.annotation_id == pl.annotation_id)
            .unwrap();
        pl.pruning_predicate = build_pruning_predicate(&leaf.plain, Arc::clone(schema));
    }
    let conjuncts: Vec<Arc<dyn PhysicalExpr>> = plan
        .residual_expr
        .iter()
        .cloned()
        .chain(leaves.iter().map(|l| Arc::clone(&l.plain)))
        .collect();
    plan.pruning_predicate = build_pruning_predicate(
        &datafusion::physical_expr::utils::conjunction(conjuncts),
        Arc::clone(schema),
    );
    plan
}

// ── Mock backends ───────────────────────────────────────────────────

#[derive(Debug)]
struct FixedDocsCollector {
    docs: Arc<BTreeSet<i32>>,
}

impl RowGroupDocsCollector for FixedDocsCollector {
    fn collect_packed_u64_bitset(
        &self,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<CollectDocsResult, String> {
        let span = (max_doc - min_doc) as usize;
        let mut out = vec![0u64; span.div_ceil(64)];
        for &doc in self.docs.range(min_doc..max_doc) {
            let rel = (doc - min_doc) as usize;
            out[rel / 64] |= 1u64 << (rel % 64);
        }
        Ok(out.into())
    }
}

/// Delegated backend keyed by `provider_key == annotation_id`; records every call.
#[derive(Debug, Default)]
struct RecordingDelegatedBackend {
    match_sets: HashMap<i32, Arc<BTreeSet<i32>>>,
    calls: Mutex<Vec<(i32, i32)>>,
}

impl RecordingDelegatedBackend {
    fn calls_for(&self, annotation_id: i32) -> Vec<i32> {
        let mut doc_mins: Vec<i32> = self
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| *k == annotation_id)
            .map(|(_, d)| *d)
            .collect();
        doc_mins.sort_unstable();
        doc_mins
    }
}

impl DelegatedBackendCollectorFactory for RecordingDelegatedBackend {
    fn create(
        &self,
        _context_id: i64,
        provider_key: i32,
        _writer_generation: i64,
        doc_min: i32,
        _doc_max: i32,
    ) -> Result<Arc<dyn RowGroupDocsCollector>, String> {
        self.calls.lock().unwrap().push((provider_key, doc_min));
        let docs = self
            .match_sets
            .get(&provider_key)
            .cloned()
            .ok_or_else(|| format!("no match set for provider_key {}", provider_key))?;
        Ok(Arc::new(FixedDocsCollector { docs }))
    }
}

fn docs_where(pred: impl Fn(usize) -> bool) -> Arc<BTreeSet<i32>> {
    Arc::new(
        (0..NUM_ROWS)
            .filter(|&i| pred(i))
            .map(|i| i as i32)
            .collect(),
    )
}

// ── Recording object store ──────────────────────────────────────────

/// Delegates to `LocalFileSystem` and records every byte range fetched.
#[derive(Debug)]
struct RecordingStore {
    inner: Arc<dyn ObjectStore>,
    fetched: Mutex<Vec<Range<u64>>>,
}

impl RecordingStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(object_store::local::LocalFileSystem::new()),
            fetched: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, r: Range<u64>) {
        self.fetched.lock().unwrap().push(r);
    }

    fn overlaps(&self, (start, len): (u64, u64)) -> bool {
        let end = start + len;
        self.fetched
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.start < end && start < r.end)
    }
}

impl fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !options.head {
            match &options.range {
                Some(GetRange::Bounded(r)) => self.record(r.clone()),
                // Unbounded reads count as reading everything.
                _ => self.record(0..u64::MAX),
            }
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjPath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        for r in ranges {
            self.record(r.clone());
        }
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// ── Harness ─────────────────────────────────────────────────────────

struct RunResult {
    ids: Vec<i32>,
    store: Arc<RecordingStore>,
    metadata: Arc<ParquetMetaData>,
}

impl RunResult {
    /// Whether any byte of `column`'s chunk in row group `rg` was fetched.
    fn read_column(&self, rg: usize, column: usize) -> bool {
        self.store
            .overlaps(self.metadata.row_group(rg).column(column).byte_range())
    }
}

/// Run `SELECT id FROM t WHERE <tree>` through the production SingleCollector wiring.
async fn run(
    tree: BoolNode,
    leaves: &[&Leaf],
    correctness: Option<Arc<BTreeSet<i32>>>,
    backend: Arc<RecordingDelegatedBackend>,
) -> RunResult {
    let tmp = write_fixture();
    let seg = load_segment(&tmp);
    let schema = fixture_schema();
    let metadata = Arc::clone(&seg.metadata);
    let plan = plan_with_counters(&tree, leaves, &schema);

    let provider_locks: Arc<HashMap<i32, Arc<OnceLock<ProviderHandle>>>> = Arc::new(
        leaves
            .iter()
            .map(|l| {
                let lock = Arc::new(OnceLock::new());
                lock.set(ProviderHandle::new_for_test(l.annotation_id))
                    .unwrap();
                (l.annotation_id, lock)
            })
            .collect(),
    );

    let factory: EvaluatorFactory = {
        let schema = schema.clone();
        let residual_expr = plan.residual_expr.clone();
        let pruning_predicate = plan.pruning_predicate.clone();
        let performance_leaves = plan.performance_leaves.clone();
        Arc::new(move |segment, _chunk, sm, _spt| {
            let pruner = Arc::new(PagePruner::new(
                &schema,
                Arc::clone(&segment.metadata),
                schema.clone(),
            ));
            let collector = correctness.as_ref().map(|docs| {
                Arc::new(FixedDocsCollector {
                    docs: Arc::clone(docs),
                }) as Arc<dyn RowGroupDocsCollector>
            });
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(SingleCollectorEvaluator::new(
                collector,
                pruner,
                pruning_predicate.clone(),
                residual_expr.clone(),
                Some(PagePruneMetrics::from_stream_metrics(sm)),
                sm.ffm_collector_calls.clone(),
                CollectorCallStrategy::PageRangeSplit,
                Arc::clone(&provider_locks),
                segment.writer_generation,
                Arc::clone(&backend) as Arc<dyn DelegatedBackendCollectorFactory>,
                0,
                None,
                None,
                HashMap::new(),
                performance_leaves.clone(),
            ));
            Ok(eval)
        })
    };

    // Same query-wide predicate columns as production: every leaf's columns.
    let mut predicate_columns: BTreeSet<usize> = BTreeSet::new();
    for e in plan
        .residual_expr
        .iter()
        .chain(leaves.iter().map(|l| &l.plain))
    {
        for c in datafusion::physical_expr::utils::collect_columns(e) {
            predicate_columns.insert(c.index());
        }
    }

    let store = Arc::new(RecordingStore::new());
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .indexed_pushdown_filters(true)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![seg],
        store: Arc::clone(&store) as Arc<dyn ObjectStore>,
        store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
        evaluator_factory: factory,
        pushdown_predicate: plan.residual_expr.clone(),
        query_config: Arc::new(qc),
        predicate_columns: predicate_columns.into_iter().collect(),
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        cancellation_token: None,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT id FROM t").await.unwrap();
    let physical = df.create_physical_plan().await.unwrap();
    let mut stream = datafusion::physical_plan::execute_stream(physical, ctx.task_ctx()).unwrap();
    let mut ids = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        ids.extend((0..arr.len()).map(|i| arr.value(i)));
    }
    ids.sort_unstable();
    drop(tmp);
    RunResult {
        ids,
        store,
        metadata,
    }
}

fn expected(pred: impl Fn(usize) -> bool) -> Vec<i32> {
    (0..NUM_ROWS)
        .filter(|&i| pred(i))
        .map(|i| i as i32)
        .collect()
}

/// Compare row sets, reporting sizes and a few differences instead of dumping both.
fn assert_ids(actual: &[i32], expected: &[i32]) {
    if actual == expected {
        return;
    }
    let a: BTreeSet<i32> = actual.iter().copied().collect();
    let e: BTreeSet<i32> = expected.iter().copied().collect();
    let extra: Vec<i32> = a.difference(&e).take(10).copied().collect();
    let missing: Vec<i32> = e.difference(&a).take(10).copied().collect();
    panic!(
        "row mismatch: actual.len={} expected.len={} extra(first 10)={:?} missing(first 10)={:?}",
        actual.len(),
        expected.len(),
        extra,
        missing
    );
}

const RG0_MIN_DOC: i32 = 0;
const RG1_MIN_DOC: i32 = RG_ROWS as i32;

// ── Tests ───────────────────────────────────────────────────────────

/// `status = 'ok'` cannot be page-pruned, so Lucene owns it in every RG. DataFusion
/// must neither read the `status` column nor evaluate the leaf.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lucene_owned_leaf_is_not_read_or_evaluated() {
    let status = Leaf::new(STATUS_LEAF_ID, status_eq("ok"));
    let backend = Arc::new(RecordingDelegatedBackend {
        match_sets: HashMap::from([(STATUS_LEAF_ID, docs_where(|i| status_of(i) == "ok"))]),
        ..Default::default()
    });

    let r = run(
        BoolNode::And(vec![status.node()]),
        &[&status],
        None,
        Arc::clone(&backend),
    )
    .await;

    assert_ids(&r.ids, &expected(|i| status_of(i) == "ok"));
    assert_eq!(
        status.rows_evaluated(),
        0,
        "DataFusion evaluated a Lucene-owned leaf"
    );
    assert_eq!(
        backend.calls_for(STATUS_LEAF_ID),
        vec![RG0_MIN_DOC, RG1_MIN_DOC]
    );
    for rg in 0..NUM_RGS {
        assert!(
            !r.read_column(rg, COL_STATUS),
            "RG{rg}: read the Lucene-owned status column"
        );
        assert!(
            r.read_column(rg, COL_ID),
            "RG{rg}: output column id not read"
        );
    }
}

/// Lucene is authoritative for a leaf it owns: an extra row from the peer survives,
/// because DataFusion does not re-check the leaf.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lucene_owned_leaf_is_authoritative() {
    let status = Leaf::new(STATUS_LEAF_ID, status_eq("ok"));
    // Row 1 has status 'warn'; the peer claims it matches anyway.
    let peer = docs_where(|i| status_of(i) == "ok" || i == 1);
    let backend = Arc::new(RecordingDelegatedBackend {
        match_sets: HashMap::from([(STATUS_LEAF_ID, peer)]),
        ..Default::default()
    });

    let r = run(
        BoolNode::And(vec![status.node()]),
        &[&status],
        None,
        backend,
    )
    .await;

    assert_ids(&r.ids, &expected(|i| status_of(i) == "ok" || i == 1));
    assert_eq!(status.rows_evaluated(), 0);
}

/// `price < 256` is selective in RG0 (DataFusion owns it) and not in RG1 (Lucene owns
/// it). Per RG, exactly one backend does the work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_is_elected_per_row_group() {
    let price = Leaf::new(PRICE_LEAF_ID, price_lt(256));
    let backend = Arc::new(RecordingDelegatedBackend {
        match_sets: HashMap::from([(PRICE_LEAF_ID, docs_where(|i| price_of(i) < 256))]),
        ..Default::default()
    });

    let r = run(
        BoolNode::And(vec![price.node()]),
        &[&price],
        None,
        Arc::clone(&backend),
    )
    .await;

    assert_ids(&r.ids, &expected(|i| price_of(i) < 256));
    assert_eq!(r.ids.len(), 512);

    // RG0: DataFusion owns it. Page stats narrow RG0 to page 0 (256 rows); the leaf is
    // evaluated on exactly those rows, once. RG1: Lucene owns it, so no evaluation.
    assert_eq!(
        price.rows_evaluated(),
        ROWS_PER_PAGE,
        "leaf must be evaluated once, in RG0 only"
    );
    assert_eq!(
        backend.calls_for(PRICE_LEAF_ID),
        vec![RG1_MIN_DOC],
        "Lucene consulted only for RG1"
    );
    assert!(
        r.read_column(0, COL_PRICE),
        "RG0: DataFusion-owned price column not read"
    );
    assert!(
        !r.read_column(1, COL_PRICE),
        "RG1: read the Lucene-owned price column"
    );
}

/// Correctness collector + native predicate + one leaf of each owner kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_collector_native_and_both_owners() {
    let price = Leaf::new(PRICE_LEAF_ID, price_lt(256));
    let status = Leaf::new(STATUS_LEAF_ID, status_eq("ok"));
    let backend = Arc::new(RecordingDelegatedBackend {
        match_sets: HashMap::from([
            (PRICE_LEAF_ID, docs_where(|i| price_of(i) < 256)),
            (STATUS_LEAF_ID, docs_where(|i| status_of(i) == "ok")),
        ]),
        ..Default::default()
    });
    let tree = BoolNode::And(vec![
        BoolNode::Collector {
            annotation_id: CORRECTNESS_ID,
        },
        BoolNode::Predicate(id_lt(12_000)),
        price.node(),
        status.node(),
    ]);

    let r = run(
        tree,
        &[&price, &status],
        Some(docs_where(|i| i % 2 == 0)),
        Arc::clone(&backend),
    )
    .await;

    assert_ids(
        &r.ids,
        &expected(|i| i % 2 == 0 && i < 12_000 && price_of(i) < 256 && status_of(i) == "ok"),
    );
    assert!(!r.ids.is_empty());

    // status: Lucene-owned everywhere.
    assert_eq!(status.rows_evaluated(), 0);
    assert_eq!(
        backend.calls_for(STATUS_LEAF_ID),
        vec![RG0_MIN_DOC, RG1_MIN_DOC]
    );
    // price: DataFusion in RG0 (at most the 256 page-0 rows, once), Lucene in RG1.
    let price_rows = price.rows_evaluated();
    assert!(
        price_rows > 0 && price_rows <= ROWS_PER_PAGE,
        "price leaf evaluated on {price_rows} rows, expected (0, {ROWS_PER_PAGE}]"
    );
    assert_eq!(backend.calls_for(PRICE_LEAF_ID), vec![RG1_MIN_DOC]);
    for rg in 0..NUM_RGS {
        assert!(
            !r.read_column(rg, COL_STATUS),
            "RG{rg}: read the Lucene-owned status column"
        );
    }
    assert!(r.read_column(0, COL_PRICE));
    assert!(
        !r.read_column(1, COL_PRICE),
        "RG1: read the Lucene-owned price column"
    );
}
