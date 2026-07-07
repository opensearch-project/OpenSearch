/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Fuzz coverage for performance-delegated leaves (`BoolNode::DelegationPossible`).
//!
//! Production behavior under test: when a SingleCollector tree has
//! `DelegationPossible` leaves and DF page-pruning isn't selective enough, the
//! evaluator consults a delegated-backend (Lucene) collector and AND-intersects its
//! bitset into the candidate set. Regardless of how the delegated backend responds,
//! the residual `original_expr` is re-applied by DataFusion's `FilterExec` — so the
//! final result must equal `Predicate(original_expr)` evaluated row-by-row.
//!
//! Two test scenarios:
//!
//! - **Passthrough**: the mock delegated-backend collector returns *exactly* the
//!   rows where `original_expr` is TRUE. AND-intersection is a no-op for
//!   correctness; result must match the oracle.
//! - **Sloppy delegated backend**: the mock returns a *superset* — TRUE rows plus
//!   random extras. The extras are filtered out by `FilterExec`'s residual on
//!   `original_expr`. Final result still matches the oracle. This proves the
//!   delegated-backend bitset isn't trusted for correctness; it's a candidate
//!   narrowing hint.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::Array;
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use futures::StreamExt;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::Rng;
use rand::SeedableRng;

use super::corpus::{CellValue, Corpus};
use super::harness::LoadedSegment;
use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::eval::single_collector::{
    CollectorCallStrategy, DelegatedBackendCollectorFactory, SingleCollectorEvaluator,
};
use crate::indexed_table::eval::RowGroupBitsetSource;
use crate::indexed_table::ffm_callbacks::ProviderHandle;
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider,
};

/// One generated delegation-shape tree:
///   `AND(Collector{tag=0}, DelegationPossible{id=1, expr1}, ..., DelegationPossible{id=N, exprN})`.
/// `tree` is the full BoolNode; `original_exprs` lets the harness compute peer
/// match-sets per annotation_id without re-walking the tree.
pub(in crate::indexed_table::tests_e2e) struct DelegationTree {
    /// Kept for diagnostics / panic messages even though execution drives off
    /// `original_exprs` and `collector_match_set` directly.
    #[allow(dead_code)]
    pub tree: BoolNode,
    /// Doc-id set the always-call correctness collector returns.
    pub collector_match_set: Vec<i32>,
    /// `[(annotation_id, original_expr)]` for every DelegationPossible leaf in DFS order.
    pub original_exprs: Vec<(i32, Arc<dyn PhysicalExpr>)>,
}

/// Decides the bitset shape returned by the mock delegated-backend collector.
#[derive(Clone, Copy)]
pub(in crate::indexed_table::tests_e2e) enum DelegatedBackendBehavior {
    /// Collector returns exactly the rows where `original_expr` is TRUE.
    Passthrough,
    /// Collector returns the TRUE rows plus a random ~30% extra rows. Residual
    /// FilterExec must filter those out.
    Sloppy,
}

/// Generate a SingleCollector-shape tree with `num_delegation` delegation leaves.
/// The Collector tag is always 0; delegation annotation ids are 1..=num_delegation.
pub(in crate::indexed_table::tests_e2e) fn generate_delegation_tree(
    rng: &mut StdRng,
    corpus: &Corpus,
    num_delegation: usize,
) -> DelegationTree {
    // Pre-compute the always-call correctness collector's match-set.
    let density = corpus.config.collector_density.clamp(0.0, 1.0);
    let target = ((corpus.num_rows() as f64) * density) as usize;
    let mut matches: Vec<i32> = (0..corpus.num_rows() as i32).collect();
    matches.shuffle(rng);
    matches.truncate(target);
    matches.sort_unstable();

    // Build N DelegationPossible leaves wrapping random binary predicates.
    let mut original_exprs: Vec<(i32, Arc<dyn PhysicalExpr>)> = Vec::with_capacity(num_delegation);
    let mut children: Vec<BoolNode> = Vec::with_capacity(num_delegation + 1);
    children.push(BoolNode::Collector { annotation_id: 0 });
    for i in 0..num_delegation {
        let annotation_id = (i + 1) as i32;
        let original_expr = gen_binary_predicate_expr(rng, &corpus.schema);
        children.push(BoolNode::DelegationPossible {
            annotation_id,
            original_expr: Arc::clone(&original_expr),
        });
        original_exprs.push((annotation_id, original_expr));
    }

    DelegationTree {
        tree: BoolNode::And(children),
        collector_match_set: matches,
        original_exprs,
    }
}

/// Build a `Column op Literal` PhysicalExpr — same shape `tree_gen::gen_binary_predicate`
/// uses (private there, inlined here so we don't need to widen its visibility).
fn gen_binary_predicate_expr(
    rng: &mut StdRng,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Arc<dyn PhysicalExpr> {
    use datafusion::arrow::datatypes::DataType;
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
    Arc::new(BinaryExpr::new(col_expr, op, lit_expr))
}

fn pick_literal_for(rng: &mut StdRng, dt: &datafusion::arrow::datatypes::DataType) -> ScalarValue {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    let strategy = rng.gen_range(0..100u32);
    match dt {
        DataType::Utf8 => {
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
                0..=69 => rng.gen_range(0..1000),
                70..=84 => rng.gen_range(i32::MIN..0),
                _ => rng.gen_range(1000..i32::MAX),
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
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let v: i64 = match strategy {
                0..=69 => {
                    rng.gen_range(1_704_067_200_000_000_000_i64..1_735_689_600_000_000_000_i64)
                }
                70..=84 => rng.gen_range(0..1_704_067_200_000_000_000_i64),
                _ => rng.gen_range(1_735_689_600_000_000_000_i64..i64::MAX),
            };
            ScalarValue::TimestampNanosecond(Some(v), None)
        }
        other => panic!("delegation fuzz: unsupported schema type {:?}", other),
    }
}

/// Mock collector whose match set is fixed at construction. Same shape as
/// `harness::MockCollector` but private to this module so we don't widen visibility.
#[derive(Debug)]
struct StaticBitsetCollector {
    matching: Vec<i32>,
}

impl RowGroupDocsCollector for StaticBitsetCollector {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        let span = (max_doc - min_doc) as usize;
        let mut out = vec![0u64; span.div_ceil(64)];
        for &doc in &self.matching {
            if doc >= min_doc && doc < max_doc {
                let rel = (doc - min_doc) as usize;
                out[rel / 64] |= 1u64 << (rel % 64);
            }
        }
        Ok(out)
    }
}

/// Mock delegated-backend factory: looks up a per-`provider_key` match-set built
/// from `original_expr` evaluation (passthrough) or from TRUE-rows + extras (sloppy).
/// `provider_key == annotation_id` in this test (we control both ends).
#[derive(Debug)]
struct MockDelegatedBackendCollectorFactory {
    /// `provider_key` → set of doc-ids the collector pretends to match.
    match_sets: HashMap<i32, Vec<i32>>,
}

impl DelegatedBackendCollectorFactory for MockDelegatedBackendCollectorFactory {
    fn create(
        &self,
        _context_id: i64,
        provider_key: i32,
        _writer_generation: i64,
        _doc_min: i32,
        _doc_max: i32,
    ) -> Result<Arc<dyn RowGroupDocsCollector>, String> {
        let matching = self.match_sets.get(&provider_key).cloned().ok_or_else(|| {
            format!(
                "MockDelegatedBackend: no match-set for provider_key={}",
                provider_key
            )
        })?;
        Ok(Arc::new(StaticBitsetCollector { matching }) as Arc<dyn RowGroupDocsCollector>)
    }
}

/// Compute the doc-ids where `original_expr` evaluates TRUE, row-by-row over the
/// corpus. Reuses the same 3VL semantics as the oracle for parity. Only handles
/// `BinaryExpr(Column op Literal)`, which is the only shape `gen_binary_predicate_expr`
/// emits — all `original_expr`s in delegation trees pass through that fn.
fn rows_matching_predicate(corpus: &Corpus, expr: &Arc<dyn PhysicalExpr>) -> Vec<i32> {
    let bin = expr
        .downcast_ref::<BinaryExpr>()
        .expect("delegation fuzz: original_expr must be BinaryExpr");
    let col = bin
        .left()
        .downcast_ref::<Column>()
        .expect("delegation fuzz: BinaryExpr lhs must be Column");
    let lit = bin
        .right()
        .downcast_ref::<Literal>()
        .expect("delegation fuzz: BinaryExpr rhs must be Literal");
    let col_idx = *corpus
        .col_idx
        .get(col.name())
        .unwrap_or_else(|| panic!("column {:?} not in corpus", col.name()));
    let mut out = Vec::new();
    for row in 0..corpus.num_rows() {
        let cell = &corpus.cells[col_idx][row];
        if compare_cell_lit_true(cell, *bin.op(), lit.value()) {
            out.push(row as i32);
        }
    }
    out
}

/// Returns true iff `cell op lit` is TRUE under SQL 3VL (UNKNOWN / NULL → false,
/// matching `oracle::compare_cell_lit`'s top-level "row matches iff Tri::True" rule).
fn compare_cell_lit_true(cell: &CellValue, op: Operator, lit: &ScalarValue) -> bool {
    use std::cmp::Ordering;
    macro_rules! bail_unknown {
        ($x:expr) => {
            match $x {
                None => return false,
                Some(v) => v,
            }
        };
    }
    let ord: Option<Ordering> = match (cell, lit) {
        (CellValue::Utf8(c), ScalarValue::Utf8(l)) => {
            Some(bail_unknown!(c).as_str().cmp(bail_unknown!(l).as_str()))
        }
        (CellValue::Int32(c), ScalarValue::Int32(l)) => {
            Some(bail_unknown!(c).cmp(bail_unknown!(l)))
        }
        (CellValue::Int64(c), ScalarValue::Int64(l)) => {
            Some(bail_unknown!(c).cmp(bail_unknown!(l)))
        }
        (CellValue::Float64(c), ScalarValue::Float64(l)) => {
            let c = bail_unknown!(c);
            let l = bail_unknown!(l);
            if c.is_nan() || l.is_nan() {
                return false;
            }
            c.partial_cmp(l)
        }
        (CellValue::Boolean(c), ScalarValue::Boolean(l)) => {
            Some((*bail_unknown!(c) as i32).cmp(&(*bail_unknown!(l) as i32)))
        }
        (CellValue::Date32(c), ScalarValue::Date32(l)) => {
            Some(bail_unknown!(c).cmp(bail_unknown!(l)))
        }
        (CellValue::TimestampNanos(c), ScalarValue::TimestampNanosecond(l, _)) => {
            Some(bail_unknown!(c).cmp(bail_unknown!(l)))
        }
        _ => panic!("delegation fuzz: cell/lit type mismatch"),
    };
    let ord = match ord {
        Some(o) => o,
        None => return false,
    };
    match op {
        Operator::Eq => ord == Ordering::Equal,
        Operator::NotEq => ord != Ordering::Equal,
        Operator::Lt => ord == Ordering::Less,
        Operator::LtEq => ord != Ordering::Greater,
        Operator::Gt => ord == Ordering::Greater,
        Operator::GtEq => ord != Ordering::Less,
        other => panic!("delegation fuzz: unsupported operator {:?}", other),
    }
}

/// Build per-(annotation_id) match-sets for the mock delegated backend.
/// - Passthrough: TRUE-rows of `original_expr`.
/// - Sloppy: TRUE-rows + ~30% additional random doc-ids (still in [0, num_rows)).
fn build_match_sets(
    corpus: &Corpus,
    rng: &mut StdRng,
    original_exprs: &[(i32, Arc<dyn PhysicalExpr>)],
    behavior: DelegatedBackendBehavior,
) -> HashMap<i32, Vec<i32>> {
    let mut out = HashMap::with_capacity(original_exprs.len());
    for (annotation_id, expr) in original_exprs {
        let mut rows = rows_matching_predicate(corpus, expr);
        if matches!(behavior, DelegatedBackendBehavior::Sloppy) {
            let extras = corpus.num_rows() / 3; // ~33%
            for _ in 0..extras {
                let r = rng.gen_range(0..corpus.num_rows() as i32);
                rows.push(r);
            }
            rows.sort_unstable();
            rows.dedup();
        }
        out.insert(*annotation_id, rows);
    }
    out
}

/// End-to-end execution of a delegation tree.
///
/// 1. Pre-seed `performance_provider_locks` with `OnceLock`s that hold a
///    `ProviderHandle::new_for_test(annotation_id)` — `provider_key == annotation_id`.
/// 2. Build `MockDelegatedBackendCollectorFactory` from `match_sets`.
/// 3. Wire those into `SingleCollectorEvaluator` via the new factory parameter.
/// 4. Stash the residual (AND of all `original_expr`s) as `pushdown_predicate` so
///    DataFusion's FilterExec re-applies it on decoded batches.
pub(in crate::indexed_table::tests_e2e) async fn execute_delegation_tree(
    corpus: &Corpus,
    loaded: &LoadedSegment,
    dt: &DelegationTree,
    behavior: DelegatedBackendBehavior,
) -> Vec<i32> {
    // ── Mock delegated-backend factory + provider locks ──
    let mut rng = StdRng::seed_from_u64(corpus.config.seed.wrapping_add(0xD3EE));
    let match_sets = build_match_sets(corpus, &mut rng, &dt.original_exprs, behavior);
    let factory = Arc::new(MockDelegatedBackendCollectorFactory { match_sets })
        as Arc<dyn DelegatedBackendCollectorFactory>;

    let mut provider_locks: HashMap<i32, Arc<OnceLock<ProviderHandle>>> = HashMap::new();
    for (annotation_id, _) in &dt.original_exprs {
        let lock = Arc::new(OnceLock::new());
        // provider_key == annotation_id by construction.
        lock.set(ProviderHandle::new_for_test(*annotation_id))
            .expect("OnceLock set");
        provider_locks.insert(*annotation_id, lock);
    }
    let provider_locks = Arc::new(provider_locks);

    // ── Always-call correctness collector ──
    let correctness: Arc<dyn RowGroupDocsCollector> = Arc::new(StaticBitsetCollector {
        matching: dt.collector_match_set.clone(),
    });

    // ── Residual: AND of all original_exprs. DataFusion's FilterExec re-applies
    //    it on decoded rows — that's what enforces correctness regardless of how
    //    the delegated backend responded.
    let residual_physical: Arc<dyn PhysicalExpr> = and_all(&dt.original_exprs);

    let eval_factory: EvaluatorFactory = {
        let correctness = Arc::clone(&correctness);
        let residual_physical = Arc::clone(&residual_physical);
        let factory = Arc::clone(&factory);
        let provider_locks = Arc::clone(&provider_locks);
        let schema = loaded.schema.clone();
        Arc::new(move |segment, _chunk, stream_metrics, _stats_prune_tree| {
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(SingleCollectorEvaluator::new(
                Some(Arc::clone(&correctness)),
                pruner,
                None,
                Some(Arc::clone(&residual_physical)),
                None,
                stream_metrics.ffm_collector_calls.clone(),
                CollectorCallStrategy::FullRange,
                Arc::clone(&provider_locks),
                segment.writer_generation,
                Arc::clone(&factory),
                0,
                None,
                None,
                HashMap::new(),
            ));
            Ok(eval)
        })
    };

    // ── Execute. Same shape as `run_single_collector_query`: SELECT * FROM t,
    //    pushdown_predicate carries the residual, FilterExec is built by the planner.
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    // Honor the corpus's target_partitions so multi-segment fixtures fan out across
    // partitions — `EvaluatorFactory` closure then runs once per (partition × segment)
    // and exercises the multi-segment offset math:
    //
    //   peer_bm offset = (min_doc as i64 - rg.first_row) as u32
    //
    // Segment 0 has rg.first_row==0; segments 1..N have first_row != 0. If the offset
    // arithmetic were wrong we'd silently corrupt results on any segment after the first.
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(corpus.config.target_partitions.max(1))
        .indexed_pushdown_filters(true)
        .batch_size(1024)
        .build();

    let pred_cols: Vec<usize> = {
        use datafusion::common::tree_node::TreeNode;
        let mut indices = std::collections::BTreeSet::new();
        let _ = residual_physical.apply(|node| {
            if let Some(col) = node.downcast_ref::<datafusion::physical_expr::expressions::Column>()
            {
                indices.insert(col.index());
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        });
        indices.into_iter().collect()
    };

    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: loaded.schema.clone(),
        segments: loaded.segments.clone(),
        store,
        store_url,
        evaluator_factory: eval_factory,
        pushdown_predicate: Some(Arc::clone(&residual_physical)),
        query_config: Arc::new(qc),
        predicate_columns: pred_cols,
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        cancellation_token: None,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), task_ctx).unwrap();
    let mut doc_ids: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let schema = b.schema();
        let idx = schema.index_of("__doc_id").expect("__doc_id in batch");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .expect("__doc_id is Int32");
        for i in 0..arr.len() {
            assert!(arr.is_valid(i), "__doc_id is non-null");
            doc_ids.push(arr.value(i));
        }
    }
    doc_ids.sort_unstable();
    doc_ids
}

/// Build `expr0 AND expr1 AND ... AND exprN` as a single PhysicalExpr.
fn and_all(original_exprs: &[(i32, Arc<dyn PhysicalExpr>)]) -> Arc<dyn PhysicalExpr> {
    assert!(!original_exprs.is_empty(), "and_all: empty exprs");
    let mut iter = original_exprs.iter();
    let mut acc = Arc::clone(&iter.next().unwrap().1);
    for (_, e) in iter {
        acc = Arc::new(BinaryExpr::new(acc, Operator::And, Arc::clone(e))) as Arc<dyn PhysicalExpr>;
    }
    acc
}

/// Compute the expected result by intersecting:
///   correctness collector match-set ∩ {rows where every original_expr is TRUE}
fn expected_doc_ids(corpus: &Corpus, dt: &DelegationTree) -> Vec<i32> {
    use std::collections::BTreeSet;
    let mut acc: BTreeSet<i32> = dt.collector_match_set.iter().copied().collect();
    for (_, expr) in &dt.original_exprs {
        let m: BTreeSet<i32> = rows_matching_predicate(corpus, expr).into_iter().collect();
        acc = acc.intersection(&m).copied().collect();
    }
    acc.into_iter().collect()
}

/// One iteration. `behavior` decides whether the delegated backend lies (sloppy)
/// or replays exactly (passthrough). Either way, the residual FilterExec must
/// produce the canonical (correctness ∩ predicates) result.
pub(in crate::indexed_table::tests_e2e) async fn run_delegation_iteration(
    corpus: &Corpus,
    loaded: &LoadedSegment,
    dt: &DelegationTree,
    behavior: DelegatedBackendBehavior,
) -> Result<(), String> {
    let expected = expected_doc_ids(corpus, dt);
    let actual = execute_delegation_tree(corpus, loaded, dt, behavior).await;
    if expected != actual {
        return Err(format!(
            "delegation mismatch (behavior={:?}):\n  expected.len={} actual.len={}\n  first_mismatch_at_idx={}\n",
            match behavior {
                DelegatedBackendBehavior::Passthrough => "passthrough",
                DelegatedBackendBehavior::Sloppy => "sloppy",
            },
            expected.len(),
            actual.len(),
            expected
                .iter()
                .zip(actual.iter())
                .position(|(a, b)| a != b)
                .map(|i| i.to_string())
                .unwrap_or_else(|| "len".to_string()),
        ));
    }
    Ok(())
}
