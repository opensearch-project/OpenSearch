/**
`IndexedTableProvider` — the main entry point for indexed parquet queries.

Index-agnostic: receives a single `ShardSearcher` (Java handles boolean tree
logic and produces one combined bitset) TODO : support multiple searchers / bitsets
plus a `BitsetMode` that controls how
the bitset relates to page pruner filters (AND = intersect, OR = union).

Query Client :
  1. Creates a ShardSearcher (Java walks boolean tree → single Weight)
  2. Builds SegmentFileInfo list (segment -> parquet mapping)
  3. Passes both + BitsetMode to IndexedTableProvider

IndexedTableProvider::scan() →
  1. Compute partition assignments (RGs across segments) - Each partition can contain one or more
segments but the partitions respects row group boundaries.
  2. Return QueryShardExec (carries the searcher + bitset_mode) - Has the logic to execute the shard
query with N partitions

QueryShardExec::execute(partition) →
  For each SegmentChunk in assignment [ IndexedExec has the logic ]
    1. ShardSearcher.collector(segment, doc_range) → SegmentCollector
    2. IndexedExec with that collector + bitset_mode
    3. Per RG: collect bitset → page prune (AND/OR) → read parquet
    4. Chain streams if multiple chunks
**/

use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion_common::DataFusionError;
use futures::StreamExt;

use super::index::{BitsetMode, ShardSearcher};
use super::metrics::PartitionMetrics;
use super::partitioning::{compute_assignments, PartitionAssignment, SegmentLayout};
use super::stream::{FilterStrategy, IndexedExec, RowGroupInfo};

/// Coerce Binary columns to Utf8 for better query compatibility.
pub fn coerce_binary_to_string(schema: SchemaRef) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if matches!(f.data_type(), DataType::Binary | DataType::LargeBinary) {
                Field::new(f.name(), DataType::Utf8, f.is_nullable())
                    .with_metadata(f.metadata().clone())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Info about a segment and its corresponding parquet file.
#[derive(Debug, Clone)]
pub struct SegmentFileInfo {
    pub segment_ord: i32,
    pub max_doc: i64,
    pub parquet_path: PathBuf,
    pub parquet_size: u64,
    pub row_groups: Vec<RowGroupInfo>,
    pub metadata: Arc<ParquetMetaData>,
}

/// Configuration for an indexed table.
pub struct IndexedTableConfig {
    // The single searcher for this query (Java handles boolean tree → one bitset). TODO : support multi searchers
    pub searcher: Arc<dyn ShardSearcher>,
    // How the bitset relates to parquet filters (AND = intersect, OR = union).
    pub bitset_mode: BitsetMode,
    // Segment-to-parquet mapping with metadata.
    pub segments: Vec<SegmentFileInfo>,
    pub schema: SchemaRef,
    // Number of partitions (None = use segment count).
    pub num_partitions: Option<usize>,
    // Force predicate pushdown on/off (None = adaptive).
    pub force_pushdown: Option<bool>,
    // Force filter strategy (None = adaptive).
    pub force_strategy: Option<FilterStrategy>,
}

impl IndexedTableConfig {
    /// Create a new config with default AND mode.
    pub fn new(
        searcher: Arc<dyn ShardSearcher>,
        segments: Vec<SegmentFileInfo>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            searcher,
            bitset_mode: BitsetMode::default(),
            segments,
            schema,
            num_partitions: None,
            force_pushdown: None,
            force_strategy: None,
        }
    }

    /// Builder: set bitset mode.
    pub fn with_bitset_mode(mut self, mode: BitsetMode) -> Self {
        self.bitset_mode = mode;
        self
    }

    /// Builder: set number of partitions.
    pub fn with_partitions(mut self, n: usize) -> Self {
        self.num_partitions = Some(n.max(1));
        self
    }

    /// Builder: force pushdown on/off.
    pub fn with_pushdown(mut self, pushdown: Option<bool>) -> Self {
        self.force_pushdown = pushdown;
        self
    }

    /// Builder: force filter strategy.
    pub fn with_strategy(mut self, strategy: Option<FilterStrategy>) -> Self {
        self.force_strategy = strategy;
        self
    }
}

/// Multi-segment TableProvider with configurable partitioning.
///
/// This is Index-agnostic — only knows about a `ShardSearcher`, `BitsetMode`, segments,
/// and parquet files.
pub struct IndexedTableProvider {
    schema: SchemaRef,
    segments: Vec<SegmentFileInfo>,
    searcher: Arc<dyn ShardSearcher>,
    bitset_mode: BitsetMode,
    num_partitions: Option<usize>,
    force_pushdown: Option<bool>,
    force_strategy: Option<FilterStrategy>,
}

impl std::fmt::Debug for IndexedTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexedTableProvider")
            .field("segments", &self.segments.len())
            .field("bitset_mode", &self.bitset_mode)
            .finish()
    }
}

impl IndexedTableProvider {
    /// Create from config.
    pub fn try_new(config: IndexedTableConfig) -> Result<Self, DataFusionError> {
        if config.segments.is_empty() {
            return Err(DataFusionError::External("No segments provided".into()));
        }

        Ok(Self {
            schema: config.schema,
            segments: config.segments,
            searcher: config.searcher,
            bitset_mode: config.bitset_mode,
            num_partitions: config.num_partitions,
            force_pushdown: config.force_pushdown,
            force_strategy: config.force_strategy,
        })
    }

    pub fn with_partitions(mut self, n: usize) -> Self {
        self.num_partitions = Some(n.max(1));
        self
    }

    pub fn with_pushdown(mut self, pushdown: Option<bool>) -> Self {
        self.force_pushdown = pushdown;
        self
    }

    // Builder: force filter strategy. Used for testing
    pub fn with_strategy(mut self, strategy: Option<FilterStrategy>) -> Self {
        self.force_strategy = strategy;
        self
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions.unwrap_or(self.segments.len())
    }

    pub fn num_segments(&self) -> usize {
        self.segments.len()
    }

    pub fn total_rows(&self) -> i64 {
        self.segments
            .iter()
            .flat_map(|s| &s.row_groups)
            .map(|rg| rg.num_rows)
            .sum()
    }

    pub fn total_row_groups(&self) -> usize {
        self.segments.iter().map(|s| s.row_groups.len()).sum()
    }
}

#[async_trait]
impl TableProvider for IndexedTableProvider {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let output_schema = match projection {
            Some(proj) => Arc::new(Schema::new(
                proj.iter()
                    .map(|&i| self.schema.field(i).clone())
                    .collect::<Vec<_>>(),
            )),
            None => self.schema.clone(),
        };

        let predicate = if !filters.is_empty() {
            let combined = filters
                .iter()
                .skip(1)
                .fold(filters[0].clone(), |acc, f| acc.and(f.clone()));
            let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
            create_physical_expr(&combined, &df_schema, state.execution_props()).ok()
        } else {
            None
        };

        let num_partitions = self.num_partitions();
        let segInfo: Vec<SegmentLayout> = self
            .segments
            .iter()
            .map(|s| SegmentLayout {
                row_groups: s.row_groups.clone(),
            })
            .collect();
        let assignments = compute_assignments(&segInfo, num_partitions);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(assignments.len()),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Ok(Arc::new(QueryShardExec {
            schema: output_schema,
            full_schema: self.schema.clone(),
            segments: self.segments.clone(),
            assignments,
            projection: projection.cloned(),
            properties,
            filters: filters.to_vec(),
            predicate,
            searcher: Arc::clone(&self.searcher),
            bitset_mode: self.bitset_mode,
            force_pushdown: self.force_pushdown,
            force_strategy: self.force_strategy,
            metrics: ExecutionPlanMetricsSet::new(),
            inner_parquet_metrics: Arc::new(std::sync::Mutex::new(Vec::new())),
        }))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if is_comparison_filter(f) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

fn is_comparison_filter(filter: &Expr) -> bool {
    if let Expr::BinaryExpr(binary) = filter {
        matches!(
            binary.op,
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq | Operator::Eq | Operator::NotEq
        )
    } else {
        false
    }
}

/// Execution plan for shard queries with partition assignments.
///
/// Carries a single `ShardSearcher` + `BitsetMode`. Each partition creates
/// a cheap `SegmentCollector` from the searcher, then reads parquet.
pub struct QueryShardExec {
    schema: SchemaRef,
    full_schema: SchemaRef,
    segments: Vec<SegmentFileInfo>,
    assignments: Vec<PartitionAssignment>,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
    filters: Vec<Expr>,
    predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    searcher: Arc<dyn ShardSearcher>,
    bitset_mode: BitsetMode,
    force_pushdown: Option<bool>,
    force_strategy: Option<FilterStrategy>,
    metrics: ExecutionPlanMetricsSet,
    inner_parquet_metrics: Arc<std::sync::Mutex<Vec<MetricsSet>>>,
}

impl std::fmt::Debug for QueryShardExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryShardExec")
            .field("segments", &self.segments.len())
            .field("partitions", &self.assignments.len())
            .field("bitset_mode", &self.bitset_mode)
            .finish()
    }
}

impl DisplayAs for QueryShardExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let total_rgs: usize = self.segments.iter().map(|s| s.row_groups.len()).sum();
        let total_rows: i64 = self.segments.iter()
            .flat_map(|s| &s.row_groups)
            .map(|rg| rg.num_rows)
            .sum();
        write!(
            f,
            "QueryShardExec: segments={}, partitions={}, rg={}, rows={}, mode={:?}",
            self.segments.len(),
            self.assignments.len(),
            total_rgs,
            total_rows,
            self.bitset_mode,
        )
    }
}

impl ExecutionPlan for QueryShardExec {
    fn name(&self) -> &str { "QueryShardExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn properties(&self) -> &PlanProperties { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let mut combined = self.metrics.clone_inner();
        if let Ok(inner_sets) = self.inner_parquet_metrics.lock() {
            for inner_set in inner_sets.iter() {
                for metric in inner_set.iter() {
                    combined.push(Arc::clone(metric));
                }
            }
        }
        Some(combined)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        let pm = PartitionMetrics::new(&self.metrics, partition);

        if partition >= self.assignments.len() {
            return Ok(Box::pin(
                datafusion::physical_plan::stream::EmptyRecordBatchStream::new(self.schema.clone()),
            ));
        }

        let assignment = &self.assignments[partition];
        if assignment.chunks.is_empty() {
            return Ok(Box::pin(
                datafusion::physical_plan::stream::EmptyRecordBatchStream::new(self.schema.clone()),
            ));
        }

        // Defer scorer creation to poll time so RepartitionExec can create all
        // partition streams instantly, then when it spawns tokio tasks to poll them,
        // the JNI scorer calls run in parallel across worker threads.
        let schema = self.schema.clone();
        let schema_outer = schema.clone();
        let full_schema = self.full_schema.clone();
        let segments = self.segments.clone();
        let chunks = assignment.chunks.clone();
        let projection = self.projection.clone();
        let filters = self.filters.clone();
        let predicate = self.predicate.clone();
        let searcher = Arc::clone(&self.searcher);
        let bitset_mode = self.bitset_mode;
        let force_pushdown = self.force_pushdown;
        let force_strategy = self.force_strategy;

        let stream_metrics =
            pm.into_stream_metrics(Some(Arc::clone(&self.inner_parquet_metrics)));

        let partition_id = partition;
        let lazy_stream = futures::stream::once(async move {
            let mut streams: Vec<datafusion::execution::SendableRecordBatchStream> = Vec::new();

            for (chunk_idx, chunk) in chunks.iter().enumerate() {
                let segment = &segments[chunk.segment_idx];

                let partition_row_groups: Vec<RowGroupInfo> = chunk
                    .row_group_indices
                    .iter()
                    .filter_map(|&rg_idx| segment.row_groups.get(rg_idx).cloned())
                    .collect();

                if partition_row_groups.is_empty() {
                    continue;
                }

                eprintln!("[PARTITION-DEBUG] partition={}, chunk={}/{}, seg={}, doc_range=[{}, {}), rgs={:?}, thread={:?}",
                    partition_id, chunk_idx, chunks.len(), chunk.segment_idx,
                    chunk.doc_min, chunk.doc_max,
                    chunk.row_group_indices, std::thread::current().name());

                let collector = match searcher.collector(
                    segment.segment_ord as usize,
                    chunk.doc_min,
                    chunk.doc_max,
                ) {
                    Ok(c) => c,
                    Err(_) => continue,
                };

                let properties = PlanProperties::new(
                    EquivalenceProperties::new(schema.clone()),
                    Partitioning::UnknownPartitioning(1),
                    datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                    datafusion::physical_plan::execution_plan::Boundedness::Bounded,
                );

                let indexed_exec = IndexedExec {
                    schema: schema.clone(),
                    full_schema: full_schema.clone(),
                    file_path: segment.parquet_path.clone(),
                    file_size: segment.parquet_size,
                    row_groups: partition_row_groups,
                    projection: projection.clone(),
                    properties,
                    metadata: Arc::clone(&segment.metadata),
                    filters: filters.clone(),
                    predicate: predicate.clone(),
                    collector: std::sync::Mutex::new(Some(collector)),
                    bitset_mode,
                    doc_range: Some((chunk.doc_min, chunk.doc_max)),
                    metrics: ExecutionPlanMetricsSet::new(),
                    stream_metrics: stream_metrics.clone(),
                    force_pushdown,
                    force_strategy,
                };

                match indexed_exec.execute(0, Arc::clone(&context)) {
                    Ok(s) => streams.push(s),
                    Err(_) => continue,
                }
            }

            match streams.len() {
                0 => Box::pin(
                    datafusion::physical_plan::stream::EmptyRecordBatchStream::new(schema.clone()),
                ) as datafusion::execution::SendableRecordBatchStream,
                1 => streams.into_iter().next().unwrap(),
                _ => {
                    let chained = futures::stream::iter(streams).flatten();
                    Box::pin(RecordBatchStreamAdapter::new(schema.clone(), chained))
                        as datafusion::execution::SendableRecordBatchStream
                }
            }
        }).flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema_outer, lazy_stream)))
    }
}
