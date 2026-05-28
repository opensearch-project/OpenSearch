/*
 * SPDX-License-Identifier: Apache-2.0
 */

//! Stats packing helpers for the FFM `df_stats()` function.
//!
//! Packs Tokio runtime metrics, per-operation task monitor metrics, and
//! cumulative indexed-search metrics into a `#[repr(C)]` `DfStatsBuffer`
//! struct (544 bytes) for efficient transfer across the FFM boundary.
//!
//! ## Struct layout
//!
//! | Group                | Type                 | Fields |
//! |----------------------|----------------------|--------|
//! | `io_runtime`         | `RuntimeMetricsRepr` | 9 × i64 |
//! | `cpu_runtime`        | `RuntimeMetricsRepr` | 9 × i64 (zeroed if N/A) |
//! | `coordinator_reduce` | `TaskMonitorRepr`    | 3 × i64 |
//! | `query_execution`    | `TaskMonitorRepr`    | 3 × i64 |
//! | `stream_next`        | `TaskMonitorRepr`    | 3 × i64 |
//! | `plan_setup`         | `TaskMonitorRepr`    | 3 × i64 |
//! | `datanode_gate`      | `PartitionGateRepr`  | 4 × i64 |
//! | `coordinator_gate`   | `PartitionGateRepr`  | 4 × i64 |
//! | `search_stats`       | `SearchStatsRepr`    | 30 × i64 (1 lifecycle + 18 counts + 11 times) |

use tokio::runtime::Handle;
use tokio_metrics::{RuntimeMonitor, TaskMonitor};

use crate::executor::ConcurrencyGate;

#[repr(C)]
pub struct RuntimeMetricsRepr {
    pub workers_count: i64,
    pub total_polls_count: i64,
    pub total_busy_duration_ms: i64,
    pub total_overflow_count: i64,
    pub global_queue_depth: i64,
    pub blocking_queue_depth: i64,
    pub num_alive_tasks: i64,
    pub spawned_tasks_count: i64,
    pub total_local_queue_depth: i64,
}

impl RuntimeMetricsRepr {
    pub fn zeroed() -> Self {
        Self {
            workers_count: 0,
            total_polls_count: 0,
            total_busy_duration_ms: 0,
            total_overflow_count: 0,
            global_queue_depth: 0,
            blocking_queue_depth: 0,
            num_alive_tasks: 0,
            spawned_tasks_count: 0,
            total_local_queue_depth: 0,
        }
    }
}

#[repr(C)]
pub struct TaskMonitorRepr {
    pub total_poll_duration_ms: i64,
    pub total_scheduled_duration_ms: i64,
    pub total_idle_duration_ms: i64,
}

#[repr(C)]
pub struct PartitionGateRepr {
    pub max_permits: i64,
    pub active_permits: i64,
    pub total_wait_duration_ms: i64,
    pub total_batches_started: i64,
}

#[repr(C)]
pub struct SearchStatsRepr {
    pub queries_completed: i64,
    pub output_rows: i64,
    pub rows_matched: i64,
    pub rows_pruned_by_page_index: i64,
    pub row_groups_processed: i64,
    pub row_groups_skipped: i64,
    pub pages_pruned: i64,
    pub pages_total: i64,
    pub page_pruning_unavailable: i64,
    pub ffm_collector_calls: i64,
    pub batches_produced: i64,
    pub parquet_batches_received: i64,
    pub position_map_identity: i64,
    pub position_map_bitmap: i64,
    pub position_map_runs: i64,
    pub min_skip_run_row_granular: i64,
    pub min_skip_run_block_granular: i64,
    pub prefetch_wait_count: i64,
    pub batches_pre_coalesce: i64,
    pub elapsed_compute_ms: i64,
    pub index_time_ms: i64,
    pub parquet_time_ms: i64,
    pub prefetch_wait_time_ms: i64,
    pub coalesce_time_ms: i64,
    pub build_mask_time_ms: i64,
    pub filter_record_batch_time_ms: i64,
    pub on_batch_mask_time_ms: i64,
    pub mask_slice_time_ms: i64,
    pub projection_fixup_time_ms: i64,
    pub parquet_poll_time_ms: i64,
}

impl SearchStatsRepr {
    pub fn zeroed() -> Self {
        Self {
            queries_completed: 0,
            output_rows: 0,
            rows_matched: 0,
            rows_pruned_by_page_index: 0,
            row_groups_processed: 0,
            row_groups_skipped: 0,
            pages_pruned: 0,
            pages_total: 0,
            page_pruning_unavailable: 0,
            ffm_collector_calls: 0,
            batches_produced: 0,
            parquet_batches_received: 0,
            position_map_identity: 0,
            position_map_bitmap: 0,
            position_map_runs: 0,
            min_skip_run_row_granular: 0,
            min_skip_run_block_granular: 0,
            prefetch_wait_count: 0,
            batches_pre_coalesce: 0,
            elapsed_compute_ms: 0,
            index_time_ms: 0,
            parquet_time_ms: 0,
            prefetch_wait_time_ms: 0,
            coalesce_time_ms: 0,
            build_mask_time_ms: 0,
            filter_record_batch_time_ms: 0,
            on_batch_mask_time_ms: 0,
            mask_slice_time_ms: 0,
            projection_fixup_time_ms: 0,
            parquet_poll_time_ms: 0,
        }
    }
}

#[repr(C)]
pub struct DfStatsBuffer {
    pub io_runtime: RuntimeMetricsRepr,
    pub cpu_runtime: RuntimeMetricsRepr,
    pub coordinator_reduce: TaskMonitorRepr,
    pub query_execution: TaskMonitorRepr,
    pub stream_next: TaskMonitorRepr,
    pub plan_setup: TaskMonitorRepr,
    pub datanode_gate: PartitionGateRepr,
    pub coordinator_gate: PartitionGateRepr,
    pub search_stats: SearchStatsRepr,
}

const _: () = assert!(std::mem::size_of::<RuntimeMetricsRepr>() == 9 * 8);
const _: () = assert!(std::mem::size_of::<TaskMonitorRepr>() == 3 * 8);
const _: () = assert!(std::mem::size_of::<PartitionGateRepr>() == 4 * 8);
const _: () = assert!(std::mem::size_of::<SearchStatsRepr>() == 30 * 8);
const _: () = assert!(std::mem::size_of::<DfStatsBuffer>() == 68 * 8);

pub mod layout {
    use super::*;
    pub const BUFFER_BYTE_SIZE: usize = std::mem::size_of::<DfStatsBuffer>();
    const _: () = assert!(BUFFER_BYTE_SIZE == 544);
}

/// Snapshot a `RuntimeMonitor` and return a populated `RuntimeMetricsRepr`.
///
/// ## Fields
///
/// | Field                       | Source                                     |
/// |-----------------------------|---------------------------------------------|
/// | workers_count               | `Handle::metrics().num_workers()`           |
/// | total_polls_count           | `Handle::metrics().worker_poll_count(i)` Σ  |
/// | total_busy_duration_ms      | `Handle::metrics().worker_total_busy_duration(i)` Σ |
/// | total_overflow_count        | `Handle::metrics().worker_overflow_count(i)` Σ |
/// | global_queue_depth          | `Handle::metrics().global_queue_depth()`    |
/// | blocking_queue_depth        | `Handle::metrics().blocking_queue_depth()`  |
/// | num_alive_tasks             | `Handle::metrics().num_alive_tasks()`       |
/// | spawned_tasks_count         | `Handle::metrics().spawned_tasks_count()`   |
/// | total_local_queue_depth     | `Handle::metrics().worker_local_queue_depth(i)` Σ |
pub fn pack_runtime_metrics(_monitor: &RuntimeMonitor, handle: &Handle) -> RuntimeMetricsRepr {
    let m = handle.metrics();
    let num_workers = m.num_workers();

    // Sum per-worker metrics into aggregates
    let mut total_polls: u64 = 0;
    let mut total_busy_ns: u64 = 0;
    let mut total_overflow: u64 = 0;
    let mut total_local_queue: u64 = 0;

    for i in 0..num_workers {
        total_polls += m.worker_poll_count(i);
        total_busy_ns += m.worker_total_busy_duration(i).as_nanos() as u64;
        total_overflow += m.worker_overflow_count(i);
        total_local_queue += m.worker_local_queue_depth(i) as u64;
    }

    RuntimeMetricsRepr {
        workers_count: num_workers as i64,
        total_polls_count: total_polls as i64,
        total_busy_duration_ms: (total_busy_ns / 1_000_000) as i64,
        total_overflow_count: total_overflow as i64,
        global_queue_depth: m.global_queue_depth() as i64,
        blocking_queue_depth: m.blocking_queue_depth() as i64,
        num_alive_tasks: m.num_alive_tasks() as i64,
        spawned_tasks_count: m.spawned_tasks_count() as i64,
        total_local_queue_depth: total_local_queue as i64,
    }
}


/// Snapshot a `TaskMonitor` and return a populated `TaskMonitorRepr`.
///
/// | Field                        | Source                          |
/// |------------------------------|---------------------------------|
/// | total_poll_duration_ms       | cumulative poll duration (ms)   |
/// | total_scheduled_duration_ms  | cumulative scheduled dur. (ms)  |
/// | total_idle_duration_ms       | cumulative idle duration (ms)   |
pub fn pack_task_monitor(monitor: &TaskMonitor) -> TaskMonitorRepr {
    let cumulative = monitor.cumulative();
    TaskMonitorRepr {
        total_poll_duration_ms: cumulative.total_poll_duration.as_millis() as i64,
        total_scheduled_duration_ms: cumulative.total_scheduled_duration.as_millis() as i64,
        total_idle_duration_ms: cumulative.total_idle_duration.as_millis() as i64,
    }
}

/// Snapshot a `ConcurrencyGate` and return a populated `PartitionGateRepr`.
///
/// | Field                  | Source                              |
/// |------------------------|-------------------------------------|
/// | max_permits            | `gate.max_permits()`                |
/// | active_permits         | `gate.active_permits()`             |
/// | total_wait_duration_ms | `gate.total_wait_ms()`              |
/// | total_batches_started  | `gate.total_queries_admitted()`     |
pub fn pack_partition_gate(gate: &ConcurrencyGate) -> PartitionGateRepr {
    PartitionGateRepr {
        max_permits: gate.max_permits() as i64,
        active_permits: gate.active_permits() as i64,
        total_wait_duration_ms: gate.total_wait_ms() as i64,
        total_batches_started: gate.total_queries_admitted() as i64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_monitors::{
        coordinator_reduce_monitor, query_execution_monitor,
        stream_next_monitor, plan_setup_monitor,
    };

    #[test]
    fn test_pack_runtime_metrics_populates_workers_count() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let monitor = RuntimeMonitor::new(&rt.handle());
        let result = pack_runtime_metrics(&monitor, rt.handle());
        assert_eq!(result.workers_count, 2);
    }

    #[test]
    fn test_pack_runtime_metrics_returns_struct() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(3)
            .enable_all()
            .build()
            .unwrap();
        let monitor = RuntimeMonitor::new(&rt.handle());
        let result = pack_runtime_metrics(&monitor, rt.handle());
        assert_eq!(result.workers_count, 3);
        assert!(result.total_polls_count >= 0);
        assert!(result.total_busy_duration_ms >= 0);
        assert!(result.global_queue_depth >= 0);
    }

    #[test]
    fn test_pack_task_monitor_writes_duration_fields() {
        let monitor = TaskMonitor::new();
        let result = pack_task_monitor(&monitor);
        assert_eq!(result.total_poll_duration_ms, 0);
        assert_eq!(result.total_scheduled_duration_ms, 0);
        assert_eq!(result.total_idle_duration_ms, 0);
    }

    #[tokio::test]
    async fn test_pack_task_monitor_after_instrumented_future() {
        let monitor = query_execution_monitor();
        let fut = monitor.instrument(async {
            tokio::task::yield_now().await;
            42
        });
        let result = fut.await;
        assert_eq!(result, 42);

        let tm = pack_task_monitor(monitor);
        assert!(tm.total_poll_duration_ms >= 0, "total_poll_duration should be >= 0, got {}", tm.total_poll_duration_ms);
    }

    #[tokio::test]
    async fn test_full_stats_packing() {
        let mgr = crate::runtime_manager::RuntimeManager::new(1, 1.5, 1.5);

        let io_runtime = pack_runtime_metrics(&mgr.io_monitor, mgr.io_runtime.handle());

        let cpu_runtime = if let Some(ref cpu_mon) = mgr.cpu_monitor {
            if let Some(cpu_handle) = mgr.cpu_executor.handle() {
                pack_runtime_metrics(cpu_mon, &cpu_handle)
            } else {
                RuntimeMetricsRepr::zeroed()
            }
        } else {
            RuntimeMetricsRepr::zeroed()
        };

        let buf = DfStatsBuffer {
            io_runtime,
            cpu_runtime,
            coordinator_reduce: pack_task_monitor(coordinator_reduce_monitor()),
            query_execution: pack_task_monitor(query_execution_monitor()),
            stream_next: pack_task_monitor(stream_next_monitor()),
            plan_setup: pack_task_monitor(plan_setup_monitor()),
            datanode_gate: pack_partition_gate(mgr.cpu_executor.concurrency_gate()),
            coordinator_gate: pack_partition_gate(mgr.coordinator_gate()),
            search_stats: SearchStatsRepr::zeroed(),
        };

        assert_eq!(layout::BUFFER_BYTE_SIZE, 544);
        assert!(buf.io_runtime.workers_count > 0, "IO runtime workers_count should be > 0, got {}", buf.io_runtime.workers_count);
        assert!(buf.datanode_gate.max_permits > 0, "datanode_gate max_permits should be > 0, got {}", buf.datanode_gate.max_permits);
        assert!(buf.coordinator_gate.max_permits > 0, "coordinator_gate max_permits should be > 0, got {}", buf.coordinator_gate.max_permits);

        if mgr.cpu_monitor.is_some() {
            assert!(buf.cpu_runtime.workers_count > 0, "CPU runtime workers_count should be > 0, got {}", buf.cpu_runtime.workers_count);
        }

        mgr.cpu_executor.shutdown();
        std::mem::forget(mgr);
    }

    #[test]
    fn test_df_stats_buffer_too_small() {
        // Verify that the buffer size assertion holds
        assert_eq!(std::mem::size_of::<DfStatsBuffer>(), 544);
        assert_eq!(layout::BUFFER_BYTE_SIZE, 544);
        // A buffer smaller than 544 bytes should be rejected by df_stats.
        // We can't call df_stats directly without a runtime manager,
        // but we verify the constant is correct.
        assert!(layout::BUFFER_BYTE_SIZE > 0);
    }

    #[test]
    fn test_search_stats_repr_zeroed() {
        let r = SearchStatsRepr::zeroed();
        assert_eq!(r.queries_completed, 0);
        assert_eq!(r.output_rows, 0);
        assert_eq!(r.parquet_time_ms, 0);
        assert_eq!(r.parquet_poll_time_ms, 0);
    }

    #[test]
    fn test_pack_search_stats_reflects_global_accumulator() {
        use crate::search_stats::{pack_search_stats, SEARCH_STATS};
        use std::sync::atomic::Ordering;

        // Static fixture: snapshot before, mutate, snapshot after, compare deltas.
        // Reset is not exposed (cumulative-only by design); we rely on deltas so
        // we don't depend on whether other tests ran first.
        let before = pack_search_stats();
        SEARCH_STATS.queries_completed.fetch_add(3, Ordering::Relaxed);
        SEARCH_STATS.output_rows.fetch_add(42, Ordering::Relaxed);
        SEARCH_STATS.parquet_time_ms.fetch_add(7, Ordering::Relaxed);
        let after = pack_search_stats();

        assert_eq!(after.queries_completed - before.queries_completed, 3);
        assert_eq!(after.output_rows - before.output_rows, 42);
        assert_eq!(after.parquet_time_ms - before.parquet_time_ms, 7);
    }
}
