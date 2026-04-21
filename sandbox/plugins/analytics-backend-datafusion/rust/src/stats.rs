/*
 * SPDX-License-Identifier: Apache-2.0
 */

//! Stats packing helpers for the JNI `stats()` function.
//!
//! Packs Tokio runtime metrics and per-operation task monitor metrics
//! into a fixed-layout `[i64; 28]` buffer for efficient transfer across
//! the JNI boundary.
//!
//! ## Array layout
//!
//! | Offset  | Count | Content                                    |
//! |---------|-------|--------------------------------------------|
//! | 0–7     | 8     | IO runtime (RuntimeValues)                 |
//! | 8–15    | 8     | CPU runtime (RuntimeValues, zeros if N/A)  |
//! | 16–18   | 3     | `query_execution` task monitor             |
//! | 19–21   | 3     | `stream_next` task monitor                 |
//! | 22–24   | 3     | `fetch_phase` task monitor                 |
//! | 25–27   | 3     | `segment_stats` task monitor               |

use tokio::runtime::Handle;
use tokio_metrics::{RuntimeMonitor, TaskMonitor};

pub mod layout {
    pub const RUNTIME_SIZE: usize = 8;
    pub const TASK_MONITOR_SIZE: usize = 3;
    pub const TOTAL_SIZE: usize = RUNTIME_SIZE * 2 + TASK_MONITOR_SIZE * 4;
    const _: () = assert!(TOTAL_SIZE == 28);
}

/// Snapshot a `RuntimeMonitor` and write 8 fields into `buf` starting at `offset`.
///
/// ## Field mapping
///
/// | Sub-offset | Field                          |
/// |------------|--------------------------------|
/// | 0          | workers_count                  |
/// | 1          | total_polls_count              |
/// | 2          | total_busy_duration (ms)       |
/// | 3          | total_overflow_count           |
/// | 4          | global_queue_depth             |
/// | 5          | blocking_queue_depth           |
/// | 6          | num_alive_tasks                |
/// | 7          | spawned_tasks_count            |
pub fn pack_runtime_metrics(monitor: &RuntimeMonitor, handle: &Handle, buf: &mut [i64; layout::TOTAL_SIZE], offset: usize) {
    let mut intervals = monitor.intervals();
    let snapshot = intervals.next().expect("RuntimeMonitor intervals should never be empty");

    buf[offset]     = snapshot.workers_count as i64;
    buf[offset + 1] = snapshot.total_polls_count as i64;
    buf[offset + 2] = snapshot.total_busy_duration.as_millis() as i64;
    buf[offset + 3] = snapshot.total_overflow_count as i64;
    buf[offset + 4] = snapshot.global_queue_depth as i64;
    buf[offset + 5] = handle.metrics().blocking_queue_depth() as i64;
    buf[offset + 6] = handle.metrics().num_alive_tasks() as i64;
    buf[offset + 7] = handle.metrics().spawned_tasks_count() as i64;
}


/// Write 3 fields for a single task monitor into `buf` starting at `offset`.
///
/// | Sub-offset | Field                          |
/// |------------|--------------------------------|
/// | +0         | total_poll_duration (ms)       |
/// | +1         | total_scheduled_duration (ms)  |
/// | +2         | total_idle_duration (ms)       |
pub fn pack_task_monitor(
    monitor: &TaskMonitor,
    buf: &mut [i64; layout::TOTAL_SIZE],
    offset: usize,
) {
    let cumulative = monitor.cumulative();
    buf[offset]     = cumulative.total_poll_duration.as_millis() as i64;
    buf[offset + 1] = cumulative.total_scheduled_duration.as_millis() as i64;
    buf[offset + 2] = cumulative.total_idle_duration.as_millis() as i64;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_monitors::{
        query_execution_monitor, stream_next_monitor,
        fetch_phase_monitor, segment_stats_monitor,
    };

    #[test]
    fn test_pack_runtime_metrics_populates_workers_count() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let monitor = RuntimeMonitor::new(&rt.handle());
        let mut buf = [0i64; layout::TOTAL_SIZE];
        pack_runtime_metrics(&monitor, rt.handle(), &mut buf, 0);
        assert_eq!(buf[0], 2);
    }

    #[test]
    fn test_pack_runtime_metrics_at_offset() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(3)
            .enable_all()
            .build()
            .unwrap();
        let monitor = RuntimeMonitor::new(&rt.handle());
        let mut buf = [0i64; layout::TOTAL_SIZE];
        pack_runtime_metrics(&monitor, rt.handle(), &mut buf, layout::RUNTIME_SIZE);
        assert_eq!(buf[layout::RUNTIME_SIZE], 3);
        assert_eq!(buf[0], 0);
    }

    #[test]
    fn test_pack_task_monitor_writes_duration_fields() {
        let monitor = TaskMonitor::new();
        let mut buf = [0i64; layout::TOTAL_SIZE];
        let offset = layout::RUNTIME_SIZE * 2; // 12
        pack_task_monitor(&monitor, &mut buf, offset);
        assert_eq!(buf[offset], 0);     // total_poll_duration
        assert_eq!(buf[offset + 1], 0); // total_scheduled_duration
        assert_eq!(buf[offset + 2], 0); // total_idle_duration
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

        let mut buf = [0i64; layout::TOTAL_SIZE];
        let offset = layout::RUNTIME_SIZE * 2;
        pack_task_monitor(monitor, &mut buf, offset);
        assert!(buf[offset] >= 0, "total_poll_duration should be >= 0, got {}", buf[offset]);
    }

    #[tokio::test]
    async fn test_full_stats_packing() {
        let mgr = crate::runtime_manager::RuntimeManager::new(1);
        let mut buf = [0i64; layout::TOTAL_SIZE];

        pack_runtime_metrics(&mgr.io_monitor, mgr.io_runtime.handle(), &mut buf, 0);
        if let Some(ref cpu_mon) = mgr.cpu_monitor {
            if let Some(cpu_handle) = mgr.cpu_executor.handle() {
                pack_runtime_metrics(cpu_mon, &cpu_handle, &mut buf, layout::RUNTIME_SIZE);
            }
        }

        let tm_base = layout::RUNTIME_SIZE * 2;
        pack_task_monitor(query_execution_monitor(), &mut buf, tm_base);
        pack_task_monitor(stream_next_monitor(), &mut buf, tm_base + layout::TASK_MONITOR_SIZE);
        pack_task_monitor(fetch_phase_monitor(), &mut buf, tm_base + layout::TASK_MONITOR_SIZE * 2);
        pack_task_monitor(segment_stats_monitor(), &mut buf, tm_base + layout::TASK_MONITOR_SIZE * 3);

        assert_eq!(layout::TOTAL_SIZE, 28);
        assert_eq!(buf.len(), 28);
        assert!(buf[0] > 0, "IO runtime workers_count should be > 0, got {}", buf[0]);

        if mgr.cpu_monitor.is_some() {
            assert!(buf[layout::RUNTIME_SIZE] > 0, "CPU runtime workers_count should be > 0, got {}", buf[layout::RUNTIME_SIZE]);
        }

        mgr.cpu_executor.shutdown();
        std::mem::forget(mgr);
    }
}
