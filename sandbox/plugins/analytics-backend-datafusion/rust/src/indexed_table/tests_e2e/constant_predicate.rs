/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Constant (column-less) predicate on the indexed path with `emit_row_ids =
//! false` — e.g. an unfoldable `mktime('...') > N`, which once errored with
//! `no index_filter(...) in plan`. Asserts constant-true keeps every row and
//! constant-false drops every row (the residual is evaluated, not ignored).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Operator;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
use datafusion::physical_expr::PhysicalExpr;
use futures::StreamExt;

use super::super::eval::predicate_evaluator::PredicateOnlyEvaluator;
use super::super::eval::RowGroupBitsetSource;
use super::super::page_pruner::{PagePruneMetrics, PagePruner};
use super::super::stream::{FilterStrategy, RowGroupInfo};
use super::super::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};
use super::write_fixture_parquet;

/// Column-less `<lhs> <op> <rhs>` over two Int32 literals — a constant boolean.
fn const_predicate(lhs: i32, op: Operator, rhs: i32) -> Arc<dyn PhysicalExpr> {
    let left: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(lhs))));
    let right: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(rhs))));
    Arc::new(BinaryExpr::new(left, op, right))
}

/// Scan the 16-row fixture via the predicate-only (`FilterClass::None`) factory
/// with `emit_row_ids = false`, as `execute_indexed_with_context` does. Returns
/// the emitted row count.
async fn run_constant_residual(residual: Arc<dyn PhysicalExpr>) -> usize {
    let tmp = write_fixture_parquet();
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();

    let file = std::fs::File::open(&path).unwrap();
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

    let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
    let segment = SegmentFileInfo {
        writer_generation: 0,
        max_doc: 16,
        object_path,
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
        global_base: 0,
        sort_min: None,
        sort_max: None,
    };

    // FilterClass::None: no pruning predicate (column-less), constant applied
    // as residual in on_batch_mask.
    let factory: EvaluatorFactory = {
        let schema = schema.clone();
        let residual = Arc::clone(&residual);
        Arc::new(
            move |segment: &SegmentFileInfo, _chunk, stream_metrics, _stats_prune_tree| {
                let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
                let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(PredicateOnlyEvaluator::new(
                    pruner,
                    None,
                    Some(Arc::clone(&residual)),
                    Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                    None,
                    HashMap::new(),
                ));
                Ok(eval)
            },
        )
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = ObjectStoreUrl::local_filesystem();
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .force_strategy(Some(FilterStrategy::BooleanMask))
        .indexed_pushdown_filters(false)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store,
        store_url,
        evaluator_factory: factory,
        pushdown_predicate: None,
        query_config: std::sync::Arc::new(qc),
        predicate_columns: vec![],
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        cancellation_token: None,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT brand, price FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let mut stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        // Sanity: projected columns decode.
        let _ = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let _ = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        rows += b.num_rows();
    }
    rows
}

#[tokio::test]
async fn constant_true_predicate_keeps_all_rows() {
    // 1 > 0 → true for every row → full 16-row fixture passes.
    let rows = run_constant_residual(const_predicate(1, Operator::Gt, 0)).await;
    assert_eq!(16, rows, "constant-true residual should keep every row");
}

#[tokio::test]
async fn constant_false_predicate_drops_all_rows() {
    // 1 < 0 → false for every row → zero rows emitted.
    let rows = run_constant_residual(const_predicate(1, Operator::Lt, 0)).await;
    assert_eq!(0, rows, "constant-false residual should drop every row");
}
