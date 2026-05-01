/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Coordinator-reduce local execution.
//!
//! A [`LocalSession`] holds a DataFusion [`SessionContext`] configured to share
//! the caller-supplied [`RuntimeEnv`] (and therefore its memory pool) with the
//! rest of the node. The session is the Rust-side counterpart of
//! `DatafusionReduceSink` on the Java side:
//!
//! 1. For each declared stage input, [`LocalSession::register_partition`]
//!    creates a [`PartitionStreamSender`] / [`PartitionStreamReceiver`] pair,
//!    wraps the receiver in a [`SingleReceiverPartition`], and registers it as
//!    a [`StreamingTable`] on the session under the input id.
//! 2. [`LocalSession::execute_substrait`] decodes a Substrait plan against the
//!    session (its table references resolve to the streaming tables) and hands
//!    back a [`SendableRecordBatchStream`] the bridge layer can drain.
//!
//! The session has no knowledge of the FFM bridge; it is exposed to Java via a
//! raw `Box::into_raw` pointer managed in `api.rs`, matching the lifecycle
//! model used by `DataFusionRuntime` / `ShardView` / `QueryStreamHandle`.

use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::DataFusionError;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_functions_aggregate::approx_distinct::approx_distinct_udaf;use prost::Message;
use substrait::proto::Plan;

use crate::partition_stream::{channel, PartitionStreamSender, SingleReceiverPartition};

/// Coordinator-reduce DataFusion session.
///
/// Owns a [`SessionContext`] that reuses the caller's [`RuntimeEnv`] so memory
/// accounting shares the node-wide pool. One session corresponds to one reduce
/// stage; it holds the streaming inputs registered by
/// [`Self::register_partition`] and is drained exactly once via
/// [`Self::execute_substrait`].
pub struct LocalSession {
    ctx: SessionContext,
}

impl LocalSession {
    /// Builds a session whose `SessionContext` reuses the given [`RuntimeEnv`].
    ///
    /// The runtime's memory pool, disk manager, and caches are inherited —
    /// every batch consumed or produced by this session counts against the
    /// same limits as the shard-scan path.
    pub fn new(runtime_env: &RuntimeEnv) -> Self {
        // Cheaply clone the env so the session owns a handle independent of
        // the caller. `RuntimeEnv` internally holds `Arc`s — this is a
        // lightweight clone, not a deep copy of the pool or disk manager.
        let runtime_env = Arc::new(runtime_env.clone());
        let mut rules = physical_optimizer_rules_without_combine();
        rules.push(Arc::new(StripPartialAggregateRule));
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_physical_optimizer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        // Register approx_count_distinct as an alias for approx_distinct so that
        // Substrait plans produced by isthmus (which uses the Substrait spec name
        // approx_count_distinct) are accepted by DataFusion's Substrait consumer.
        register_approx_count_distinct_alias(&ctx);
        Self { ctx }
    }

    /// Registers a streaming input on the session under `name` and returns the
    /// producer side of the channel.
    ///
    /// The receiver is wrapped in a [`SingleReceiverPartition`] and registered
    /// as a [`StreamingTable`]; Substrait plans executed through
    /// [`Self::execute_substrait`] resolve table references named `name` to
    /// this streaming table. The caller pushes `RecordBatch`es into the
    /// returned [`PartitionStreamSender`] via
    /// [`PartitionStreamSender::send_blocking`].
    pub fn register_partition(
        &mut self,
        name: &str,
        schema: SchemaRef,
    ) -> Result<PartitionStreamSender, DataFusionError> {
        let (sender, receiver) = channel(Arc::clone(&schema));
        let partition: Arc<dyn PartitionStream> =
            Arc::new(SingleReceiverPartition::new(receiver));
        let table = StreamingTable::try_new(schema, vec![partition])?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to register streaming table '{}': {}",
                    name, e
                ))
            })?;
        Ok(sender)
    }

    /// Registers an in-memory input on the session under `name`, holding all
    /// `batches` in a single [`MemTable`] partition.
    ///
    /// Unlike [`Self::register_partition`], this method does not return a
    /// channel sender — the batches are fully materialized in the table. Used
    /// by the memtable variant of the coordinator-reduce sink, which buffers
    /// shard responses in Java and hands them across in one call.
    pub fn register_memtable(
        &mut self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<(), DataFusionError> {
        let table = MemTable::try_new(schema, vec![batches])?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to register memtable '{}': {}",
                    name, e
                ))
            })?;
        Ok(())
    }

    /// Decodes a Substrait plan against the session and returns the resulting
    /// stream.
    ///
    /// Table references in the plan resolve through the session's registered
    /// streaming tables, so input batches pushed into
    /// [`PartitionStreamSender`]s flow naturally into the DataFusion physical
    /// plan. The returned stream is hot — polling it drives both the reduce
    /// computation and the consumption of the streaming inputs.
    pub async fn execute_substrait(
        &self,
        bytes: &[u8],
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let plan = Plan::decode(bytes).map_err(|e| {
            DataFusionError::Execution(format!("Failed to decode Substrait plan: {}", e))
        })?;
        let logical_plan = from_substrait_plan(&self.ctx.state(), &plan).await?;
        self.ctx
            .execute_logical_plan(logical_plan)
            .await?
            .execute_stream()
            .await
    }

    /// Returns the memory pool the session's `RuntimeEnv` was built with.
    ///
    /// Used by the bridge layer to seed a per-query tracking context so
    /// reduce-stage allocations count against the same pool as the shard-scan
    /// path.
    pub fn memory_pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.ctx.runtime_env().memory_pool)
    }
}

/// Returns the default physical optimizer rules with [`CombinePartialFinalAggregate`] removed.
/// DataFusion's combine rule recombines partial+final aggregates in the same process,
/// undoing the distributed split. We disable it so each side runs independently.
pub fn physical_optimizer_rules_without_combine(
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let combine_name = CombinePartialFinalAggregate::new().name().to_string();
    PhysicalOptimizer::default()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != combine_name)
        .collect()
}

/// Physical optimizer rules for shard execution: removes `CombinePartialFinalAggregate`
/// and adds `StripFinalAggregateRule` so the shard emits partial intermediate state
/// (e.g. HLL sketch bytes) instead of the final aggregated result.
pub fn shard_physical_optimizer_rules(
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules = physical_optimizer_rules_without_combine();
    rules.push(Arc::new(StripFinalAggregateRule));
    rules
}

/// Registers `approx_count_distinct` as an alias for DataFusion's `approx_distinct`.
///
/// Substrait plans produced by isthmus use the Substrait spec name `approx_count_distinct`,
/// but DataFusion's built-in function is named `approx_distinct`. This alias bridges the gap.
pub fn register_approx_count_distinct_alias(ctx: &SessionContext) {
    ctx.register_udaf(
        Arc::unwrap_or_clone(approx_distinct_udaf()).with_aliases(["approx_count_distinct"]),
    );
}

/// Physical optimizer rule that strips the `Final`/`FinalPartitioned` aggregate from a
/// `Final(AggregateExec) → Partial(AggregateExec)` pair, leaving only the `Partial`.
///
/// Used for shard execution: the shard emits partial intermediate state (e.g. HLL sketch
/// bytes for approx_distinct) rather than the final result.
#[derive(Debug)]
pub struct StripFinalAggregateRule;

impl PhysicalOptimizerRule for StripFinalAggregateRule {
    fn optimize(
        &self,
        plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _config: &datafusion_common::config::ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};

        if let Some(final_agg) = plan.as_any().downcast_ref::<AggregateExec>() {
            if matches!(final_agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned)
                // Only strip for scalar aggregates (no group-by keys).
                // With group-by, the shard's Final correctly computes per-group partial
                // states before sending to the coordinator — stripping it would lose grouping.
                && final_agg.group_expr().is_empty()
            {
                // Strip the Final aggregate — return its input (the Partial), recursively optimized
                return self.optimize(Arc::clone(final_agg.input()), _config);
            }
        }
        let new_children: datafusion_common::Result<Vec<_>> = plan
            .children()
            .into_iter()
            .map(|c| self.optimize(Arc::clone(c), _config))
            .collect();
        plan.with_new_children(new_children?)
    }

    fn name(&self) -> &str { "StripFinalAggregateRule" }
    fn schema_check(&self) -> bool { false }
}

/// Physical optimizer rule that strips the `Partial` aggregate from a
/// `Final(AggregateExec) → Partial(AggregateExec)` pair, connecting `Final` directly
/// to the `Partial`'s input.
///
/// Used for coordinator execution: the coordinator receives partial intermediate state
/// (e.g. HLL sketch bytes) from shards and needs to run only the `Final` merge step.
#[derive(Debug)]
pub struct StripPartialAggregateRule;

impl PhysicalOptimizerRule for StripPartialAggregateRule {
    fn optimize(
        &self,
        plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _config: &datafusion_common::config::ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};

        if let Some(final_agg) = plan.as_any().downcast_ref::<AggregateExec>() {
            if matches!(final_agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned)
                // Only strip for scalar aggregates (no group-by keys).
                // With group-by, the coordinator's Partial correctly re-groups incoming
                // partial states by key before Final merges within each group — stripping
                // it would give Final un-grouped rows, producing wrong results.
                && final_agg.group_expr().is_empty()
            {
                // Walk through CoalescePartitionsExec/RepartitionExec to find the Partial
                if let Some(partial_input) = find_partial_agg_input(final_agg.input()) {
                    let partial_input = Arc::clone(partial_input);
                    // Coalesce partitions before Final so it reads from a single partition
                    let coalesced = Arc::new(datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(partial_input));
                    let new_final = plan.with_new_children(vec![coalesced])?;
                    return self.optimize(new_final, _config);
                }
            }
        }
        let new_children: datafusion_common::Result<Vec<_>> = plan
            .children()
            .into_iter()
            .map(|c| self.optimize(Arc::clone(c), _config))
            .collect();
        plan.with_new_children(new_children?)
    }

    fn name(&self) -> &str { "StripPartialAggregateRule" }
    fn schema_check(&self) -> bool { false }
}

/// Walks through single-child passthrough nodes (CoalescePartitionsExec, RepartitionExec)
/// to find the input of a Partial AggregateExec. Returns the Partial's input if found.
fn find_partial_agg_input(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Option<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if matches!(agg.mode(), AggregateMode::Partial) {
            return Some(agg.input());
        }
    }
    // Walk through single-child passthrough nodes
    if plan.children().len() == 1 {
        return find_partial_agg_input(plan.children()[0]);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use futures::StreamExt;
    use tokio::runtime::Handle;

    fn test_runtime_env() -> RuntimeEnv {
        RuntimeEnvBuilder::new()
            .build()
            .expect("runtime env builds")
    }

    fn i64_schema(column: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(column, DataType::Int64, false)]))
    }

    fn i64_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("batch builds")
    }

    #[tokio::test]
    async fn register_partition_makes_table_resolvable() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("x");
        let _sender = session
            .register_partition("input-0", Arc::clone(&schema))
            .expect("register succeeds");

        // A trivial `SELECT * FROM "input-0"` proves the table resolves.
        let df = session.ctx.sql("SELECT x FROM \"input-0\"").await.expect("sql parses");
        assert_eq!(df.schema().fields().len(), 1);
    }

    #[tokio::test]
    async fn execute_substrait_sums_streaming_input() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("x");
        let sender = session
            .register_partition("input-0", Arc::clone(&schema))
            .expect("register succeeds");

        // Build the Substrait bytes from a SQL-built logical plan against a
        // matching session — the plan only references `input-0`, so it is
        // portable onto our real session.
        let substrait_bytes = {
            let env = test_runtime_env();
            let mut producer = LocalSession::new(&env);
            let _unused = producer
                .register_partition("input-0", Arc::clone(&schema))
                .expect("producer register");
            let df = producer
                .ctx
                .sql("SELECT SUM(x) AS total FROM \"input-0\"")
                .await
                .expect("sum parses");
            let plan = df.logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &producer.ctx.state())
                .expect("to_substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        };

        // Push three batches totaling 45 = 1+2+3+4+5+6+7+8+9, then close.
        let producer_schema = Arc::clone(&schema);
        let handle = Handle::current();
        let producer = std::thread::spawn(move || {
            for chunk in &[vec![1i64, 2, 3], vec![4, 5, 6], vec![7, 8, 9]] {
                sender
                    .send_blocking(Ok(i64_batch(&producer_schema, chunk)), &handle)
                    .expect("send");
            }
            drop(sender); // EOF
        });

        let mut stream = session
            .execute_substrait(&substrait_bytes)
            .await
            .expect("execute");

        let mut total: i64 = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch ok");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64 col");
            for i in 0..col.len() {
                total += col.value(i);
            }
        }
        producer.join().expect("producer thread");
        assert_eq!(total, 45);
    }

    #[tokio::test]
    async fn execute_substrait_sums_memtable_input() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("x");

        let batches = vec![
            i64_batch(&schema, &[1, 2, 3]),
            i64_batch(&schema, &[4, 5, 6]),
            i64_batch(&schema, &[7, 8, 9]),
        ];
        session
            .register_memtable("input-0", Arc::clone(&schema), batches)
            .expect("register memtable");

        // Build the Substrait bytes from a SQL-built logical plan against a
        // matching session — the plan only references `input-0`, so it is
        // portable onto our real session.
        let substrait_bytes = {
            let env = test_runtime_env();
            let mut producer = LocalSession::new(&env);
            producer
                .register_memtable("input-0", Arc::clone(&schema), vec![])
                .expect("producer register");
            let df = producer
                .ctx
                .sql("SELECT SUM(x) AS total FROM \"input-0\"")
                .await
                .expect("sum parses");
            let plan = df.logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &producer.ctx.state())
                .expect("to_substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        };

        let mut stream = session
            .execute_substrait(&substrait_bytes)
            .await
            .expect("execute");

        let mut total: i64 = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch ok");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64 col");
            for i in 0..col.len() {
                total += col.value(i);
            }
        }
        assert_eq!(total, 45);
    }
}
