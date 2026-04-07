/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
`TreeIndexedTableProvider` — DataFusion TableProvider for boolean tree queries.

Takes a `BoolNode` tree + one `ShardSearcher` per Index leaf.
The tree is shared across all partitions; each partition chunk gets its
own set of collectors from the searchers.

No SQL filters are pushed down — the tree owns all boolean logic.
**/

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion_common::DataFusionError;
use futures::StreamExt;

use super::bool_tree::BoolNode;
use super::index::ShardSearcher;
use super::metrics::PartitionMetrics;
use super::partitioning::{compute_assignments, PartitionAssignment, SegmentLayout};
use super::stream::RowGroupInfo;
use super::table_provider::SegmentFileInfo;
use super::tree_stream::TreeIndexedExec;

/// Configuration for a tree-indexed table.
pub struct TreeIndexedTableConfig {
    pub tree: Arc<BoolNode>,
    pub searchers: Vec<Arc<dyn ShardSearcher>>,
    pub predicates: Vec<super::bool_tree::ResolvedPredicate>,
    pub segments: Vec<SegmentFileInfo>,
    pub schema: SchemaRef,
    pub num_partitions: Option<usize>,
}

impl TreeIndexedTableConfig {
    pub fn new(
        tree: Arc<BoolNode>,
        searchers: Vec<Arc<dyn ShardSearcher>>,
        predicates: Vec<super::bool_tree::ResolvedPredicate>,
        segments: Vec<SegmentFileInfo>,
        schema: SchemaRef,
    ) -> Self {
        Self { tree, searchers, predicates, segments, schema, num_partitions: None }
    }

    pub fn with_partitions(mut self, n: usize) -> Self {
        self.num_partitions = Some(n.max(1));
        self
    }
}

/// Multi-segment TableProvider using boolean tree evaluation.
pub struct TreeIndexedTableProvider {
    schema: SchemaRef,
    segments: Vec<SegmentFileInfo>,
    tree: Arc<BoolNode>,
    searchers: Vec<Arc<dyn ShardSearcher>>,
    predicates: Vec<super::bool_tree::ResolvedPredicate>,
    num_partitions: Option<usize>,
}

impl std::fmt::Debug for TreeIndexedTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeIndexedTableProvider")
            .field("segments", &self.segments.len())
            .finish()
    }
}

impl TreeIndexedTableProvider {
    pub fn try_new(config: TreeIndexedTableConfig) -> std::result::Result<Self, DataFusionError> {
        if config.segments.is_empty() {
            return Err(DataFusionError::External("No segments provided".into()));
        }
        Ok(Self {
            schema: config.schema, segments: config.segments,
            tree: config.tree, searchers: config.searchers,
            predicates: config.predicates,
            num_partitions: config.num_partitions,
        })
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions.unwrap_or(self.segments.len())
    }
}

#[async_trait]
impl TableProvider for TreeIndexedTableProvider {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self, _state: &dyn Session, projection: Option<&Vec<usize>>,
        _filters: &[Expr], _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let output_schema = match projection {
            Some(proj) => Arc::new(Schema::new(
                proj.iter().map(|&i| self.schema.field(i).clone()).collect::<Vec<_>>(),
            )),
            None => self.schema.clone(),
        };

        // Widen projection to include columns referenced by Predicate leaves
        let internal_projection = match projection {
            Some(proj) => {
                let cols: std::collections::HashSet<String> = self.predicates.iter()
                    .map(|p| p.column.clone())
                    .collect();
                if cols.is_empty() { Some(proj.clone()) }
                else {
                    let mut widened = proj.clone();
                    for col_name in &cols {
                        if let Some((idx, _)) = self.schema.column_with_name(col_name) {
                            if !widened.contains(&idx) { widened.push(idx); }
                        }
                    }
                    Some(widened)
                }
            }
            None => None,
        };

        let num_partitions = self.num_partitions();
        let seg_info: Vec<SegmentLayout> = self.segments.iter()
            .map(|s| SegmentLayout { row_groups: s.row_groups.clone() })
            .collect();
        let assignments = compute_assignments(&seg_info, num_partitions);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(assignments.len()),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Ok(Arc::new(TreeQueryShardExec {
            schema: output_schema, full_schema: self.schema.clone(),
            segments: self.segments.clone(), assignments,
            projection: internal_projection, properties,
            tree: Arc::clone(&self.tree), searchers: self.searchers.clone(),
            predicates: self.predicates.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            inner_parquet_metrics: Arc::new(std::sync::Mutex::new(Vec::new())),
        }))
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters.iter().map(|_| TableProviderFilterPushDown::Unsupported).collect())
    }
}

// ── TreeQueryShardExec ─────────────────────────────────────────────────

struct TreeQueryShardExec {
    schema: SchemaRef,
    full_schema: SchemaRef,
    segments: Vec<SegmentFileInfo>,
    assignments: Vec<PartitionAssignment>,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
    tree: Arc<BoolNode>,
    searchers: Vec<Arc<dyn ShardSearcher>>,
    predicates: Vec<super::bool_tree::ResolvedPredicate>,
    metrics: ExecutionPlanMetricsSet,
    inner_parquet_metrics: Arc<std::sync::Mutex<Vec<MetricsSet>>>,
}

impl std::fmt::Debug for TreeQueryShardExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeQueryShardExec")
            .field("segments", &self.segments.len())
            .field("partitions", &self.assignments.len())
            .finish()
    }
}

impl DisplayAs for TreeQueryShardExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let total_rgs: usize = self.segments.iter().map(|s| s.row_groups.len()).sum();
        write!(f, "TreeQueryShardExec: segments={}, partitions={}, rg={}",
            self.segments.len(), self.assignments.len(), total_rgs)
    }
}

/// Empty collector that returns no matches.
#[derive(Debug)]
struct EmptyCollector;

impl super::index::RowGroupDocsCollector for EmptyCollector {
    fn collect(&self, _min_doc: i32, _max_doc: i32) -> std::result::Result<Vec<u64>, String> {
        Ok(Vec::new())
    }
}

impl ExecutionPlan for TreeQueryShardExec {
    fn name(&self) -> &str { "TreeQueryShardExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn properties(&self) -> &PlanProperties { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
    fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> { Ok(self) }

    fn metrics(&self) -> Option<MetricsSet> {
        let mut combined = self.metrics.clone_inner();
        if let Ok(inner_sets) = self.inner_parquet_metrics.lock() {
            for inner_set in inner_sets.iter() {
                for metric in inner_set.iter() { combined.push(Arc::clone(metric)); }
            }
        }
        Some(combined)
    }

    fn execute(
        &self, partition: usize, context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        let pm = PartitionMetrics::new(&self.metrics, partition);
        if partition >= self.assignments.len() {
            return Ok(Box::pin(datafusion::physical_plan::stream::EmptyRecordBatchStream::new(self.schema.clone())));
        }
        let assignment = &self.assignments[partition];
        if assignment.chunks.is_empty() {
            return Ok(Box::pin(datafusion::physical_plan::stream::EmptyRecordBatchStream::new(self.schema.clone())));
        }
        let stream_metrics = pm.into_stream_metrics(Some(Arc::clone(&self.inner_parquet_metrics)));
        let mut streams: Vec<datafusion::execution::SendableRecordBatchStream> = Vec::new();

        for chunk in &assignment.chunks {
            let segment = &self.segments[chunk.segment_idx];
            let partition_rgs: Vec<RowGroupInfo> = chunk.row_group_indices.iter()
                .filter_map(|&rg_idx| segment.row_groups.get(rg_idx).cloned())
                .collect();
            if partition_rgs.is_empty() { continue; }

            // Create per-Index-leaf collectors for this segment chunk
            let mut chunk_collectors: Vec<Arc<dyn super::index::RowGroupDocsCollector>> =
                Vec::with_capacity(self.searchers.len());
            for searcher in &self.searchers {
                match searcher.collector(chunk.segment_idx, chunk.doc_min, chunk.doc_max) {
                    Ok(c) => chunk_collectors.push(c),
                    Err(_) => chunk_collectors.push(Arc::new(EmptyCollector)),
                }
            }

            let properties = PlanProperties::new(
                EquivalenceProperties::new(self.schema.clone()),
                Partitioning::UnknownPartitioning(1),
                datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            );

            let exec = TreeIndexedExec {
                schema: self.schema.clone(), full_schema: self.full_schema.clone(),
                file_path: segment.parquet_path.clone(), file_size: segment.parquet_size,
                row_groups: partition_rgs, projection: self.projection.clone(),
                properties, metadata: Arc::clone(&segment.metadata),
                bool_node: Arc::clone(&self.tree),
                collectors: std::sync::Mutex::new(Some(chunk_collectors)),
                predicates: self.predicates.clone(),
                doc_range: Some((chunk.doc_min, chunk.doc_max)),
                metrics: ExecutionPlanMetricsSet::new(),
                stream_metrics: stream_metrics.clone(),
            };
            streams.push(exec.execute(0, Arc::clone(&context))?);
        }

        match streams.len() {
            0 => Ok(Box::pin(datafusion::physical_plan::stream::EmptyRecordBatchStream::new(self.schema.clone()))),
            1 => Ok(streams.into_iter().next().unwrap()),
            _ => {
                let schema = self.schema.clone();
                let chained = futures::stream::iter(streams).flatten();
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, chained)))
            }
        }
    }
}
