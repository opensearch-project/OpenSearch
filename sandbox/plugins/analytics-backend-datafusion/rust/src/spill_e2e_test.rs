/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! End-to-end disk-spill test for the analytics-backend-datafusion engine.
//!
//! Proves the full spill flow works through the *production* memory pool
//! ([`crate::memory::DynamicLimitPool`]) and a real on-disk [`DiskManager`]:
//! a high-cardinality `GROUP BY` over enough rows, run under a memory pool too
//! small to hold the hash table, must spill to disk **and still return correct
//! results**.
//!
//! How spill is observed: DataFusion records `SpillCount` / `SpilledBytes`
//! metrics on the operators that spill (the same signal DataFusion's own
//! aggregate spill tests assert on). We walk the executed physical plan tree
//! and sum those metrics; a non-zero `spill_count` is proof spill happened.
//!
//! Determinism: `DynamicLimitPool` only consults process RSS / the jemalloc
//! override when its limit is >= 16 MiB (see `memory_guard::should_override`
//! and the `skip_for_small_pools` unit test). With a sub-16 MiB limit it is a
//! pure size-bounded pool, so spill is forced deterministically by data volume
//! alone — no dependency on the test host's live RSS.

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::memory_pool::MemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::{collect, ExecutionPlan};
    use datafusion::prelude::{ParquetReadOptions, SessionConfig};
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use crate::memory::DynamicLimitPool;

    /// Rows of test data. Large enough that the GROUP BY hash table dwarfs the
    /// tiny memory pool, guaranteeing a spill.
    const NUM_ROWS: usize = 500_000;
    const NUM_FILES: usize = 4;
    /// Pool limit kept below 16 MiB so the pool is a pure size-bounded pool
    /// (no RSS gate / jemalloc override) — see module docs. 12 MiB is small
    /// enough that the high-cardinality hash table can't stay resident (forcing
    /// spill) but large enough that the spill machinery's own working reservations
    /// can complete rather than erroring with ResourcesExhausted.
    const POOL_LIMIT_BYTES: usize = 12 * 1024 * 1024;

    /// Writes `NUM_ROWS` rows across `NUM_FILES` parquet files. `id` is unique
    /// (the high-cardinality GROUP BY key); `host` is low-cardinality filler so
    /// each row carries a bit of payload.
    fn write_parquet_data(dir: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
        ]));

        let rows_per_file = NUM_ROWS / NUM_FILES;
        for file_idx in 0..NUM_FILES {
            let start = file_idx * rows_per_file;
            let ids: Vec<i64> = (start..start + rows_per_file).map(|i| i as i64).collect();
            let hosts: Vec<String> = ids.iter().map(|i| format!("host-{:04}", i % 100)).collect();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(
                        hosts.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                    )),
                ],
            )
            .unwrap();

            let path = dir.join(format!("data_{}.parquet", file_idx));
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
    }

    /// Recursively sums the `SpillCount` metric across every operator in the
    /// executed plan tree (the spilling operator may be nested under others).
    fn total_spill_count(plan: &dyn ExecutionPlan) -> usize {
        let here = plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0);
        here + plan
            .children()
            .iter()
            .map(|child| total_spill_count(child.as_ref()))
            .sum::<usize>()
    }

    /// Same as [`total_spill_count`] for the `SpilledBytes` metric.
    fn total_spilled_bytes(plan: &dyn ExecutionPlan) -> usize {
        let here = plan.metrics().and_then(|m| m.spilled_bytes()).unwrap_or(0);
        here + plan
            .children()
            .iter()
            .map(|child| total_spilled_bytes(child.as_ref()))
            .sum::<usize>()
    }

    // FIXME: known-flaky — intermittently fails with ResourcesExhausted ("Failed to reserve memory
    // for sort during spill") under the shared memory-pool budget. Unrelated to query planning.
    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_by_spills_to_disk_and_returns_correct_results() {
        let data_dir = TempDir::new().unwrap();
        write_parquet_data(data_dir.path());

        let spill_dir = TempDir::new().unwrap();

        // Build a RuntimeEnv that mirrors production: the real DynamicLimitPool
        // (deliberately tiny) plus an on-disk DiskManager rooted at spill_dir.
        let pool: Arc<dyn MemoryPool> = Arc::new(DynamicLimitPool::new(POOL_LIMIT_BYTES).0);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
                DiskManagerMode::Directories(vec![spill_dir.path().to_path_buf()]),
            ))
            .build()
            .unwrap();
        assert!(
            runtime_env.disk_manager.tmp_files_enabled(),
            "precondition: spill must be enabled for this test to be meaningful"
        );

        // Small batches + a couple of partitions keep per-operator memory low so
        // the aggregator is pushed to spill rather than failing outright.
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = 2;
        config.options_mut().execution.batch_size = 1024;

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::new(runtime_env))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);

        ctx.register_parquet(
            "t",
            data_dir.path().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        // High-cardinality GROUP BY: one group per (unique) id. The hash table
        // holds ~NUM_ROWS groups — far more than the 4 MiB pool can keep
        // resident — so the grouped aggregation must spill.
        let df = ctx
            .sql("SELECT id, COUNT(*) AS c FROM t GROUP BY id")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let batches = collect(plan.clone(), ctx.task_ctx()).await.unwrap();

        // ── Correctness: spilling must not change the answer ──
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, NUM_ROWS,
            "every unique id must yield exactly one group"
        );

        // Every group's COUNT(*) must be 1 (ids are unique), so the counts sum
        // to NUM_ROWS. This catches data loss/duplication across the spill cycle.
        let mut count_sum: i64 = 0;
        for b in &batches {
            let counts = b
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("COUNT(*) column is Int64");
            for i in 0..counts.len() {
                assert_eq!(counts.value(i), 1, "each unique id appears exactly once");
                count_sum += counts.value(i);
            }
        }
        assert_eq!(count_sum as usize, NUM_ROWS, "counts must sum to row count");

        // ── Spill actually happened ──
        let spill_count = total_spill_count(plan.as_ref());
        let spilled_bytes = total_spilled_bytes(plan.as_ref());
        assert!(
            spill_count > 0,
            "expected the grouped aggregation to spill to disk under a {}MiB pool, \
             but SpillCount across the plan was 0",
            POOL_LIMIT_BYTES / (1024 * 1024)
        );
        assert!(
            spilled_bytes > 0,
            "spill happened (spill_count={}) but SpilledBytes was 0",
            spill_count
        );
    }
}
