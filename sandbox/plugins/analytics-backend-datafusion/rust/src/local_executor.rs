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
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use native_bridge_common::log_debug;
use prost::Message;
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
        config.options_mut().execution.target_partitions =
            crate::api::get_reduce_target_partitions();
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        crate::udf::register_all(&ctx);
        crate::udaf::register_all(&ctx);
        Self {
            ctx,
            prepared_plan: None,
            _phantom_reservation: None,
        }
    }

    /// Returns the configured batch_size for this session.
    pub fn batch_size(&self) -> usize {
        self.ctx.copied_config().options().execution.batch_size
    }

    /// Returns the current target_partitions for this session.
    pub fn target_partitions(&self) -> usize {
        self.ctx
            .copied_config()
            .options()
            .execution
            .target_partitions
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
    pub fn set_phantom(
        &mut self,
        reservation: datafusion::execution::memory_pool::MemoryReservation,
    ) {
        self._phantom_reservation = Some(reservation);
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
        let partition: Arc<dyn PartitionStream> = Arc::new(SingleReceiverPartition::new(receiver));
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
    ) -> Result<
        (
            SendableRecordBatchStream,
            Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        ),
        DataFusionError,
    > {
        let plan = Plan::decode(bytes).map_err(|e| {
            DataFusionError::Execution(format!("Failed to decode Substrait plan: {}", e))
        })?;
        let logical_plan = from_substrait_plan(&self.ctx.state(), &plan).await?;
        log_debug!(
            "DataFusion logical plan:\n{}",
            logical_plan.display_indent()
        );
        let dataframe = self.ctx.execute_logical_plan(logical_plan).await?;
        let physical_plan = dataframe.create_physical_plan().await?;

        let target_schema = crate::schema_coerce::coerce_inferred_schema(physical_plan.schema());
        let physical_plan =
            crate::relabel_exec::wrap_if_relabel_needed(physical_plan, target_schema)?;
        log_debug!(
            "DataFusion coordinator reduce physical plan:\n{}",
            displayable(physical_plan.as_ref()).indent(true)
        );
        let stream =
            datafusion::physical_plan::execute_stream(physical_plan.clone(), self.ctx.task_ctx())
                .map_err(|e| DataFusionError::Execution(format!("execute_substrait: {}", e)))?;
        Ok((stream, physical_plan))
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
            false,
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

    use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
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

    fn two_string_schema(a: &str, b: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(a, DataType::Utf8, false),
            Field::new(b, DataType::Utf8, false),
        ]))
    }

    fn two_string_batch(schema: &SchemaRef, col_a: &[&str], col_b: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(col_a.to_vec())),
                Arc::new(StringArray::from(col_b.to_vec())),
            ],
        )
        .expect("string batch builds")
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
                assert!(matches!(
                    outcome,
                    crate::partition_stream::SendOutcome::Sent
                ));
            }
            drop(sender); // EOF
        });

        let (mut stream, _plan) = session
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

        let (mut stream, _plan) = session
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

    /// `concat(substring(a, ...), '|', substring(b, ...))` over a WHERE filter —
    /// the outer operands are scalar-function calls and the middle operand is a
    /// string literal. Round-trips through SQL → Substrait → `from_substrait_plan`
    /// and asserts one concatenated row per input.
    #[tokio::test]
    async fn execute_substrait_concat_substring_literal_substring() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = two_string_schema("url", "referer");

        // Two rows, both non-empty so they survive the WHERE filter.
        let batch = two_string_batch(
            &schema,
            &["http://alpha", "http://bravo"],
            &["https://gamma", "https://delta"],
        );
        session
            .register_memtable("input-0", Arc::clone(&schema), vec![batch])
            .expect("register memtable");

        let sql = "SELECT concat(substring(url, 1, 7), '|', substring(referer, 1, 8)) AS r \
                   FROM \"input-0\" WHERE url <> '' AND referer <> ''";

        let substrait_bytes = {
            let env = test_runtime_env();
            let mut producer = LocalSession::new(&env);
            producer
                .register_memtable("input-0", Arc::clone(&schema), vec![])
                .expect("producer register");
            let df = producer.ctx.sql(sql).await.expect("concat sql parses");
            let plan = df.logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &producer.ctx.state()).expect("to_substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        };

        let (mut stream, _plan) = session
            .execute_substrait(&substrait_bytes)
            .await
            .expect("execute");

        let mut results: Vec<String> = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch ok");
            // DataFusion's string functions may return Utf8/LargeUtf8/Utf8View depending on
            // version; cast to Utf8 so the assertion is independent of the concrete string type.
            let col = datafusion::arrow::compute::cast(batch.column(0), &DataType::Utf8)
                .expect("cast concat output to Utf8");
            let col = col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 col");
            for i in 0..col.len() {
                results.push(col.value(i).to_string());
            }
        }

        // Each row joins the two substring results with the middle '|' literal.
        results.sort();
        assert_eq!(
            results,
            vec![
                "http://|https://".to_string(),
                "http://|https://".to_string(),
            ],
            "concat(substring, '|', substring) must yield one '|'-joined row per input"
        );
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
        let _tracking =
            QueryTrackingContext::new(ctx_id, pool, query_tracker::QueryType::Coordinator);

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
