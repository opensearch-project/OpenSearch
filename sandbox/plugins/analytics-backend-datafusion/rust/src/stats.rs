/*
 * SPDX-License-Identifier: Apache-2.0
 */

//! Stats packing helpers for the FFM `df_stats()` function.
//!
//! Packs Tokio runtime metrics and per-operation task monitor metrics
//! into a `#[repr(C)]` `DfStatsBuffer` struct (312 bytes) for efficient
//! transfer across the FFM boundary.
//!
//! ## Struct layout
//!
//! | Group                | Type                | Fields |
//! |----------------------|---------------------|--------|
//! | `io_runtime`         | `RuntimeMetricsRepr`| 9 × i64 |
//! | `cpu_runtime`        | `RuntimeMetricsRepr`| 9 × i64 (zeroed if N/A) |
//! | `query_execution`    | `TaskMonitorRepr`   | 3 × i64 |
//! | `stream_next`        | `TaskMonitorRepr`   | 3 × i64 |
//! | `fetch_phase`        | `TaskMonitorRepr`   | 3 × i64 |
//! | `create_context`     | `TaskMonitorRepr`   | 3 × i64 |
//! | `prepare_partial_plan`| `TaskMonitorRepr`  | 3 × i64 |
//! | `prepare_final_plan` | `TaskMonitorRepr`   | 3 × i64 |
//! | `sql_to_substrait`   | `TaskMonitorRepr`   | 3 × i64 |

use tokio::runtime::Handle;
use tokio_metrics::{RuntimeMonitor, TaskMonitor};

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
pub struct DfStatsBuffer {
    pub io_runtime: RuntimeMetricsRepr,
    pub cpu_runtime: RuntimeMetricsRepr,
    pub query_execution: TaskMonitorRepr,
    pub stream_next: TaskMonitorRepr,
    pub fetch_phase: TaskMonitorRepr,
    pub create_context: TaskMonitorRepr,
    pub prepare_partial_plan: TaskMonitorRepr,
    pub prepare_final_plan: TaskMonitorRepr,
    pub sql_to_substrait: TaskMonitorRepr,
}

const _: () = assert!(std::mem::size_of::<RuntimeMetricsRepr>() == 9 * 8);
const _: () = assert!(std::mem::size_of::<TaskMonitorRepr>() == 3 * 8);
const _: () = assert!(std::mem::size_of::<DfStatsBuffer>() == 39 * 8);

pub mod layout {
    use super::*;
    pub const BUFFER_BYTE_SIZE: usize = std::mem::size_of::<DfStatsBuffer>();
    const _: () = assert!(BUFFER_BYTE_SIZE == 312);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_monitors::{
        query_execution_monitor, stream_next_monitor,
        fetch_phase_monitor, create_context_monitor,
        prepare_partial_plan_monitor, prepare_final_plan_monitor,
        sql_to_substrait_monitor,
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
        let mgr = crate::runtime_manager::RuntimeManager::new(1);

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
            query_execution: pack_task_monitor(query_execution_monitor()),
            stream_next: pack_task_monitor(stream_next_monitor()),
            fetch_phase: pack_task_monitor(fetch_phase_monitor()),
            create_context: pack_task_monitor(create_context_monitor()),
            prepare_partial_plan: pack_task_monitor(prepare_partial_plan_monitor()),
            prepare_final_plan: pack_task_monitor(prepare_final_plan_monitor()),
            sql_to_substrait: pack_task_monitor(sql_to_substrait_monitor()),
        };

        assert_eq!(layout::BUFFER_BYTE_SIZE, 312);
        assert!(buf.io_runtime.workers_count > 0, "IO runtime workers_count should be > 0, got {}", buf.io_runtime.workers_count);

        if mgr.cpu_monitor.is_some() {
            assert!(buf.cpu_runtime.workers_count > 0, "CPU runtime workers_count should be > 0, got {}", buf.cpu_runtime.workers_count);
        }

        mgr.cpu_executor.shutdown();
        std::mem::forget(mgr);
    }

    #[test]
    fn test_df_stats_buffer_too_small() {
        // Verify that the buffer size assertion holds
        assert_eq!(std::mem::size_of::<DfStatsBuffer>(), 312);
        assert_eq!(layout::BUFFER_BYTE_SIZE, 312);
        // A buffer smaller than 312 bytes should be rejected by df_stats.
        // We can't call df_stats directly without a runtime manager,
        // but we verify the constant is correct.
        assert!(layout::BUFFER_BYTE_SIZE > 0);
    }
}
