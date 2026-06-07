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
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use native_bridge_common::log_debug;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use prost::Message;
use substrait::proto::Plan;

use crate::partition_stream::{channel_with_capacity, PartitionStreamSender, SingleReceiverPartition};

/// Coordinator-reduce DataFusion session.
///
/// Owns a [`SessionContext`] that reuses the caller's [`RuntimeEnv`] so memory
/// accounting shares the node-wide pool. One session corresponds to one reduce
/// stage; it holds the streaming inputs registered by
/// [`Self::register_partition`] and is drained exactly once via
/// [`Self::execute_substrait`].
pub struct LocalSession {
    ctx: SessionContext,
    /// Pre-prepared physical plan (set by `prepare_final_plan`).
    pub(crate) prepared_plan: Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    /// Phantom reservation held for the lifetime of this session. Accounts for
    /// untracked memory (intermediate buffers, hash table overhead) in the shared
    /// pool so concurrent reduces trigger backpressure before OOM.
    _phantom_reservation: Option<datafusion::execution::memory_pool::MemoryReservation>,
}

impl LocalSession {
    /// Builds a session whose `SessionContext` reuses the given [`RuntimeEnv`].
    ///
    /// The runtime's memory pool, disk manager, and caches are inherited —
    /// every batch consumed or produced by this session counts against the
    /// same limits as the shard-scan path.
    pub fn new(runtime_env: &RuntimeEnv) -> Self {
        let runtime_env = Arc::new(runtime_env.clone());
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = crate::api::get_reduce_target_partitions();
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_physical_optimizer_rules(crate::agg_mode::physical_optimizer_rules_without_combine())
            .build();
        let ctx = SessionContext::new_with_state(state);
        crate::udf::register_all(&ctx);
        crate::udaf::register_all(&ctx);
        Self { ctx, prepared_plan: None, _phantom_reservation: None }
    }

    /// Returns the configured batch_size for this session.
    pub fn batch_size(&self) -> usize {
        self.ctx.copied_config().options().execution.batch_size
    }

    /// Returns the current target_partitions for this session.
    pub fn target_partitions(&self) -> usize {
        self.ctx.copied_config().options().execution.target_partitions
    }

    /// Reduce target_partitions on this session. Only reduces, never increases.
    /// Called serially from register_partition_stream during session setup,
    /// before any query execution begins.
    pub fn reduce_target_partitions(&self, new_partitions: usize) {
        let state_ref = self.ctx.state_ref();
        let mut state = state_ref.write();
        let current = state.config().options().execution.target_partitions;
        if new_partitions < current {
            state.config_mut().options_mut().execution.target_partitions = new_partitions;
        }
    }

    /// Returns the current phantom reservation size, or 0 if none.
    pub fn phantom_size(&self) -> usize {
        self._phantom_reservation.as_ref().map_or(0, |r| r.size())
    }

    /// Sets the phantom reservation for this session. Caller should check
    /// phantom_size() first and only acquire a new reservation if the new
    /// estimate is larger than the current one.
    pub fn set_phantom(&mut self, reservation: datafusion::execution::memory_pool::MemoryReservation) {
        self._phantom_reservation = Some(reservation);
    }

    /// Single-partition shim over [`Self::register_partitions`].
    pub fn register_partition(
        &mut self,
        name: &str,
        schema: SchemaRef,
    ) -> Result<PartitionStreamSender, DataFusionError> {
        let mut senders = self.register_partitions(name, schema, 1)?;
        Ok(senders.pop().expect("register_partitions(1) returns one sender"))
    }

    /// Registers an `n`-partition streaming input under `name`, default capacity.
    /// Use [`Self::register_partitions_with_capacity`] for an explicit per-channel cap.
    pub fn register_partitions(
        &mut self,
        name: &str,
        schema: SchemaRef,
        num_partitions: usize,
    ) -> Result<Vec<PartitionStreamSender>, DataFusionError> {
        self.register_partitions_with_capacity(name, schema, num_partitions, None)
    }

    /// Registers `num_partitions` independent partition streams as one [`StreamingTable`].
    /// Each `(sender, receiver)` is independent — DataFusion schedules its partitions
    /// concurrently so producers pushing into different senders feed parallel input lanes
    /// without an intermediate repartition.
    ///
    /// `capacity = None` uses [`crate::partition_stream::DEFAULT_CHANNEL_CAPACITY`];
    /// `Some(1)` gives strict one-batch-at-a-time backpressure.
    ///
    /// # Panics
    /// `num_partitions == 0` or `capacity == Some(0)`.
    pub fn register_partitions_with_capacity(
        &mut self,
        name: &str,
        schema: SchemaRef,
        num_partitions: usize,
        capacity: Option<usize>,
    ) -> Result<Vec<PartitionStreamSender>, DataFusionError> {
        assert!(num_partitions > 0, "register_partitions: num_partitions must be > 0");
        let cap = capacity.unwrap_or(crate::partition_stream::DEFAULT_CHANNEL_CAPACITY);
        let mut senders = Vec::with_capacity(num_partitions);
        let mut partitions: Vec<Arc<dyn PartitionStream>> = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            let (sender, receiver) = channel_with_capacity(Arc::clone(&schema), cap);
            partitions.push(Arc::new(SingleReceiverPartition::new(receiver)));
            senders.push(sender);
        }
        let table = StreamingTable::try_new(schema, partitions)?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(|e| DataFusionError::Execution(format!("Failed to register streaming table '{}': {}", name, e)))?;
        Ok(senders)
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
                DataFusionError::Execution(format!("Failed to register memtable '{}': {}", name, e))
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
        let dataframe = self.ctx.execute_logical_plan(logical_plan).await?;
        let physical_plan = dataframe.create_physical_plan().await?;

        let target_schema = crate::schema_coerce::coerce_inferred_schema(physical_plan.schema());
        let physical_plan = crate::relabel_exec::wrap_if_relabel_needed(physical_plan, target_schema)?;
        datafusion::physical_plan::execute_stream(physical_plan, self.ctx.task_ctx())
            .map_err(|e| DataFusionError::Execution(format!("execute_substrait: {}", e)))
    }

    /// Returns the memory pool the session's `RuntimeEnv` was built with.
    ///
    /// Used by the bridge layer to seed a per-query tracking context so
    /// reduce-stage allocations count against the same pool as the shard-scan
    /// path.
    pub fn memory_pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.ctx.runtime_env().memory_pool)
    }

    /// Prepares a final-aggregate physical plan on this session.
    ///
    /// Decodes Substrait → LogicalPlan → PhysicalPlan, applies final-mode
    /// stripping, and stores the result for later execution via
    /// [`Self::execute_prepared`].
    pub async fn prepare_final_plan(
        &mut self,
        substrait_bytes: &[u8],
    ) -> Result<(), DataFusionError> {
        let plan = Plan::decode(substrait_bytes).map_err(|e| {
            DataFusionError::Execution(format!(
                "prepare_final_plan: failed to decode Substrait: {}",
                e
            ))
        })?;
        let logical_plan = from_substrait_plan(&self.ctx.state(), &plan).await?;
        let dataframe = self.ctx.execute_logical_plan(logical_plan).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        // Strip first so `force_aggregate_mode(Final)` can find the Final/Partial pair
        // through the raw plan; then derive `target_schema` and wrap with RelabelExec from
        // the stripped output (otherwise the relabel target would carry the pre-strip Final
        // output type tag and fail when wrapping the stripped tree).
        let stripped = crate::agg_mode::apply_aggregate_mode(
            physical_plan,
            crate::agg_mode::Mode::Final,
        )?;

        let target_schema = crate::schema_coerce::coerce_inferred_schema(stripped.schema());
        let stripped = crate::relabel_exec::wrap_if_relabel_needed(stripped, target_schema)?;
        self.prepared_plan = Some(stripped);
        Ok(())
    }

    /// Executes the previously prepared plan and returns the output stream.
    ///
    /// # Panics
    /// Panics if no plan has been prepared via [`Self::prepare_final_plan`].
    pub fn execute_prepared(&self) -> Result<SendableRecordBatchStream, DataFusionError> {
        let plan = self
            .prepared_plan
            .as_ref()
            .expect("execute_prepared called without a prepared plan");
        datafusion::physical_plan::execute_stream(Arc::clone(plan), self.ctx.task_ctx())
    }
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
        Arc::new(Schema::new(vec![Field::new(
            column,
            DataType::Int64,
            false,
        )]))
    }

    fn i64_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("batch builds")
    }

    #[tokio::test]
    async fn register_partitions_multi_streams_parallel_inputs() {
        // Three independent partition senders all push into the same logical input.
        // Substrait `SELECT SUM(x)` over the table sees the union of all three lanes
        // without a coordinator-side mpsc funnel.
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("x");
        let mut senders = session
            .register_partitions("input-0", Arc::clone(&schema), 3)
            .expect("register_partitions(3) succeeds");
        assert_eq!(senders.len(), 3);

        // Build a SUM substrait plan against a matching session.
        let substrait_bytes = {
            let env2 = test_runtime_env();
            let mut producer = LocalSession::new(&env2);
            let _unused = producer
                .register_partitions("input-0", Arc::clone(&schema), 1)
                .expect("producer register");
            let df = producer
                .ctx
                .sql("SELECT SUM(x) AS total FROM \"input-0\"")
                .await
                .expect("sum parses");
            let plan = df.logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &producer.ctx.state()).expect("to_substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        };

        // Push disjoint batches into each sender concurrently. Sums:
        //   lane 0: 1+2+3 = 6
        //   lane 1: 10+20+30 = 60
        //   lane 2: 100+200+300 = 600
        // Expected total: 666.
        let lanes = vec![vec![1i64, 2, 3], vec![10, 20, 30], vec![100, 200, 300]];
        let producer_schema = Arc::clone(&schema);
        let handle = Handle::current();
        let mut joiners = Vec::new();
        for (i, values) in lanes.into_iter().enumerate() {
            let sender = senders.remove(0);
            let schema_clone = Arc::clone(&producer_schema);
            let handle_clone = handle.clone();
            joiners.push(std::thread::spawn(move || {
                let outcome =
                    sender.send_blocking(Ok(i64_batch(&schema_clone, &values)), &handle_clone);
                assert!(matches!(outcome, crate::partition_stream::SendOutcome::Sent), "lane {} send failed", i);
                drop(sender);
            }));
        }

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
            for j in 0..col.len() {
                total += col.value(j);
            }
        }
        for j in joiners {
            j.join().expect("producer thread");
        }
        assert_eq!(total, 666, "SUM across 3 parallel partitions");
    }

    #[test]
    #[should_panic(expected = "num_partitions must be > 0")]
    fn register_partitions_zero_panics() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("x");
        let _ = session.register_partitions("input-0", schema, 0);
    }

    /// Builds the substrait for `sql` against a fresh session that has registered
    /// `name` as an `n`-partition streaming table with `schema`. Used by the
    /// EXPLAIN tests below to materialise a plan we can lower to physical and walk.
    async fn build_substrait_for_streaming_input(
        sql: &str,
        name: &str,
        schema: SchemaRef,
        n: usize,
    ) -> Vec<u8> {
        let env = test_runtime_env();
        let mut producer = LocalSession::new(&env);
        let _u = producer
            .register_partitions(name, Arc::clone(&schema), n)
            .expect("producer register");
        let df = producer.ctx.sql(sql).await.expect("sql parses");
        let substrait = to_substrait_plan(&df.logical_plan().clone(), &producer.ctx.state())
            .expect("to_substrait");
        let mut buf = Vec::new();
        substrait.encode(&mut buf).expect("encode");
        buf
    }

    /// Lowers `substrait_bytes` to a physical plan on `session` and asserts no
    /// `RepartitionExec` sits between any `AggregateExec(Partial)` and a descendant
    /// `StreamingTableExec` leaf. Logs the EXPLAIN under `label` for diagnostics.
    async fn assert_no_repartition_below_partial(
        session: &LocalSession,
        substrait_bytes: &[u8],
        label: &str,
    ) {
        let plan = Plan::decode(substrait_bytes).expect("decode");
        let logical = from_substrait_plan(&session.ctx.state(), &plan).await.expect("logical");
        let dataframe = session.ctx.execute_logical_plan(logical).await.expect("execute_logical");
        let physical = dataframe.create_physical_plan().await.expect("physical");

        let printed = format!("{}", displayable(physical.as_ref()).indent(true));
        eprintln!("=== EXPLAIN: {} ===\n{}", label, printed);

        let bad_paths = collect_partial_to_streaming_paths(physical.as_ref())
            .into_iter()
            .filter(|path| path.iter().any(|name| name.contains("RepartitionExec")))
            .collect::<Vec<_>>();
        assert!(
            bad_paths.is_empty(),
            "{}: must not have RepartitionExec below Partial agg; got paths: {:?}\nfull plan:\n{}",
            label,
            bad_paths,
            printed
        );
    }

    /// EXPLAIN-verify: with `target_partitions == num_partitions`, a pure
    /// `SUM` (no GROUP BY) over an N-partition `StreamingTable` MUST NOT have
    /// a `RepartitionExec` between the streaming scan and the Partial aggregate.
    ///
    /// This is the "scatter shards into N lanes, partial aggregate per lane,
    /// gather to coordinator" shape we want — DataFusion's `EnforceDistribution`
    /// rule only inserts a RoundRobin `RepartitionExec` when
    /// `child.partition_count() < target_partitions` (see
    /// `physical-optimizer/src/ensure_requirements/enforce_distribution.rs`),
    /// and `Partial` aggregates declare `Distribution::UnspecifiedDistribution`,
    /// so neither RoundRobin nor Hash repartition should appear below the
    /// partial aggregate when input partitions == target_partitions.
    #[tokio::test]
    async fn explain_no_repartition_below_partial_for_pure_sum() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        // Match target_partitions (default 4) so EnforceDistribution has nothing to do.
        let n = session.target_partitions();
        let schema = i64_schema("x");
        let _senders = session
            .register_partitions("input-0", Arc::clone(&schema), n)
            .expect("register_partitions");

        let substrait_bytes = build_substrait_for_streaming_input(
            "SELECT SUM(x) AS total FROM \"input-0\"",
            "input-0",
            Arc::clone(&schema),
            n,
        )
        .await;
        assert_no_repartition_below_partial(
            &session,
            &substrait_bytes,
            &format!("pure SUM, target_partitions == {}", n),
        )
        .await;
    }

    /// Walks the plan tree and returns, for every `AggregateExec(Partial)`
    /// found, the operator names BETWEEN that Partial and any descendant
    /// `StreamingTableExec` leaf — the chain we want to keep RepartitionExec-free.
    /// Operators above the Partial (e.g. a Hash RepartitionExec between
    /// FinalPartitioned and Partial) are not included; that's the natural
    /// shuffle point and is expected.
    fn collect_partial_to_streaming_paths(
        plan: &dyn datafusion::physical_plan::ExecutionPlan,
    ) -> Vec<Vec<String>> {
        let mut out = Vec::new();
        walk(plan, &mut Vec::new(), &mut out, None);
        out
    }

    fn walk(
        plan: &dyn datafusion::physical_plan::ExecutionPlan,
        path: &mut Vec<String>,
        out: &mut Vec<Vec<String>>,
        partial_idx: Option<usize>,
    ) {
        let name = plan.name().to_string();
        path.push(name.clone());

        // Track the depth of the nearest enclosing Partial agg. If we see a
        // Partial here, it becomes the new "root" for the slice we collect at
        // a StreamingTableExec leaf below.
        let new_partial_idx = if is_partial_aggregate(plan) {
            Some(path.len() - 1)
        } else {
            partial_idx
        };

        if name.contains("StreamingTableExec") {
            if let Some(start) = new_partial_idx {
                // Slice the path from the Partial down to (and including) this
                // StreamingTableExec. This is what must not contain a RepartitionExec.
                out.push(path[start..].to_vec());
            }
        }

        for child in plan.children() {
            walk(child.as_ref(), path, out, new_partial_idx);
        }
        path.pop();
    }

    fn is_partial_aggregate(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> bool {
        if let Some(agg) = plan
            .as_any()
            .downcast_ref::<datafusion::physical_plan::aggregates::AggregateExec>()
        {
            matches!(
                agg.mode(),
                datafusion::physical_plan::aggregates::AggregateMode::Partial
            )
        } else {
            false
        }
    }

    /// EXPLAIN-verify: GROUP BY plans need a hash-repartition above the partial
    /// aggregate (between Partial and FinalPartitioned) but still must NOT have
    /// any RepartitionExec below the partial aggregate. Documents the expected
    /// plan shape.
    #[tokio::test]
    async fn explain_no_repartition_below_partial_for_group_by() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let n = session.target_partitions();

        // Two-column schema for GROUP BY g, SUM(x).
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("x", DataType::Int64, false),
        ]));
        let _senders = session
            .register_partitions("input-0", Arc::clone(&schema), n)
            .expect("register_partitions");

        let substrait_bytes = build_substrait_for_streaming_input(
            "SELECT g, SUM(x) AS total FROM \"input-0\" GROUP BY g",
            "input-0",
            Arc::clone(&schema),
            n,
        )
        .await;
        assert_no_repartition_below_partial(
            &session,
            &substrait_bytes,
            &format!("GROUP BY, target_partitions == {}", n),
        )
        .await;
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
        let df = session
            .ctx
            .sql("SELECT x FROM \"input-0\"")
            .await
            .expect("sql parses");
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
            let substrait = to_substrait_plan(&plan, &producer.ctx.state()).expect("to_substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        };

        // Push three batches totaling 45 = 1+2+3+4+5+6+7+8+9, then close.
        let producer_schema = Arc::clone(&schema);
        let handle = Handle::current();
        let producer = std::thread::spawn(move || {
            for chunk in &[vec![1i64, 2, 3], vec![4, 5, 6], vec![7, 8, 9]] {
                let outcome = sender.send_blocking(Ok(i64_batch(&producer_schema, chunk)), &handle);
                assert!(matches!(outcome, crate::partition_stream::SendOutcome::Sent));
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
            let substrait = to_substrait_plan(&plan, &producer.ctx.state()).expect("to_substrait");
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

    #[tokio::test]
    async fn prepare_final_plan_stores_plan() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("s");

        // Register a streaming table so the plan can resolve table refs.
        let _sender = session
            .register_partition("input-0", Arc::clone(&schema))
            .expect("register");

        // Build Substrait bytes for SELECT SUM(s) FROM "input-0"
        let substrait_bytes = {
            let env2 = test_runtime_env();
            let mut producer = LocalSession::new(&env2);
            let _unused = producer
                .register_partition("input-0", Arc::clone(&schema))
                .expect("producer register");
            let df = producer
                .ctx
                .sql("SELECT SUM(s) FROM \"input-0\"")
                .await
                .expect("sum parses");
            let plan = df.logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &producer.ctx.state()).expect("to_substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        };

        assert!(session.prepared_plan.is_none());
        session
            .prepare_final_plan(&substrait_bytes)
            .await
            .expect("prepare_final_plan succeeds");
        assert!(session.prepared_plan.is_some());
    }

    /// Coordinator-side task cancellation wiring: once Java calls
    /// `execute_local_plan(session, plan, context_id)` the context is
    /// registered in [`query_tracker::QUERY_REGISTRY`], and a
    /// [`cancel_query(context_id)`] cascade resolves the racing execute
    /// future through the [`cancellation::cancellable`] branch.
    ///
    /// Mirrors the `execute_query` cancel path for the coordinator entry so
    /// a parent `AnalyticsQueryTask.cancel()` interrupts the reduce even
    /// before the first batch is produced.
    #[tokio::test]
    async fn cancel_query_fires_token_registered_from_reduce_path() {
        use crate::cancellation;
        use crate::query_tracker::{self, QueryTrackingContext};
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        use std::sync::Arc;
        use std::time::Duration;

        let ctx_id = 98_765;
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(GreedyMemoryPool::new(10_000));
        let _tracking = QueryTrackingContext::new(ctx_id, pool, query_tracker::QueryType::Coordinator);

        // A future that would block indefinitely — `cancel_query` is the
        // only way out. Mirrors a coord reduce stalled on an input partition
        // that never receives batches.
        let blocked = async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok::<(), String>(())
        };

        let token = query_tracker::get_cancellation_token(ctx_id);
        assert!(
            token.is_some(),
            "QueryTrackingContext::new must register a cancellation token"
        );

        let runner = tokio::spawn(async move {
            cancellation::cancellable(token.as_ref(), ctx_id, blocked).await
        });

        // Brief yield so the runner parks on the sleep before we cancel.
        tokio::time::sleep(Duration::from_millis(20)).await;
        query_tracker::cancel_query(ctx_id);

        let result = runner.await.expect("spawn");
        assert!(result.is_err(), "cancel_query must surface as an error");
        let msg = result.err().unwrap();
        assert!(
            msg.contains(&ctx_id.to_string()) && msg.to_lowercase().contains("cancelled"),
            "error must name the cancelled context: got [{}]",
            msg
        );
    }

    #[test]
    fn reduce_target_partitions_lowers_value() {
        let env = test_runtime_env();
        let session = LocalSession::new(&env);
        assert_eq!(session.target_partitions(), 4);

        session.reduce_target_partitions(2);
        assert_eq!(session.target_partitions(), 2);

        // Never increases
        session.reduce_target_partitions(8);
        assert_eq!(session.target_partitions(), 2);
    }

    #[test]
    fn set_phantom_stores_reservation() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};
        use std::num::NonZeroUsize;

        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(GreedyMemoryPool::new(10_000_000));
        let env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build()
            .unwrap();
        let mut session = LocalSession::new(&env);

        assert_eq!(session.phantom_size(), 0);

        let consumer = MemoryConsumer::new("test_phantom").with_can_spill(true);
        let mut reservation = consumer.register(&pool);
        reservation.try_grow(1000).unwrap();
        session.set_phantom(reservation);

        assert_eq!(session.phantom_size(), 1000);
    }

    #[test]
    fn phantom_released_on_session_drop() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};

        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(GreedyMemoryPool::new(10_000_000));
        let env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build()
            .unwrap();

        let reserved_before = pool.reserved();
        {
            let mut session = LocalSession::new(&env);
            let consumer = MemoryConsumer::new("test_phantom").with_can_spill(true);
            let mut reservation = consumer.register(&pool);
            reservation.try_grow(5000).unwrap();
            session.set_phantom(reservation);
            assert_eq!(pool.reserved(), reserved_before + 5000);
        }
        // Session dropped — phantom must be released
        assert_eq!(pool.reserved(), reserved_before);
    }
}
