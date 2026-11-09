/*
 * SPDX-License-Identifier: Apache-2.0
 */

//! Stats packing helpers for the FFM `df_stats()` function.
//!
//! Packs Tokio runtime metrics and per-operation task monitor metrics
//! into a `#[repr(C)]` `DfStatsBuffer` struct (680 bytes) for efficient
//! transfer across the FFM boundary.
//!
//! ## Struct layout
//!
//! | Group                | Type                 | Fields |
//! |----------------------|----------------------|--------|
//! | `io_runtime`         | `RuntimeMetricsRepr` | 9 × i64 |
//! | `cpu_runtime`        | `RuntimeMetricsRepr` | 9 × i64 (zeroed if N/A) |
//! | `coordinator_reduce` | `TaskMonitorRepr`    | 5 × i64 |
//! | `query_execution`    | `TaskMonitorRepr`    | 5 × i64 |
//! | `stream_next`        | `TaskMonitorRepr`    | 5 × i64 |
//! | `plan_setup`         | `TaskMonitorRepr`    | 5 × i64 |
//! | `fragment_executor_gate` | `PartitionGateRepr`  | 8 × i64 |
//! | `adaptive_budget`       | `AdaptiveBudgetRepr`    | 2 × i64 |
//! | `cache_stats`        | `CacheStatsRepr`     | 20 × i64 (4 × 5: metadata, statistics, column_index, offset_index) |
//! | `search_stats`       | `SearchStatsRepr`    | 17 × i64 |

use tokio::runtime::Handle;
use tokio_metrics::{RuntimeMonitor, TaskMonitor};

use crate::custom_cache_manager::CustomCacheManager;
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
    pub instrumented_count: i64,
    pub dropped_count: i64,
}

#[repr(C)]
pub struct PartitionGateRepr {
    pub max_permits: i64,
    pub active_permits: i64,
    pub total_wait_duration_ms: i64,
    pub total_batches_started: i64,
    pub poison_permits: i64,
    pub target_max_permits: i64,
    pub pending_acquire_permits: i64,
    pub pending_acquire_batches: i64,
}

#[repr(C)]
pub struct CacheGroupRepr {
    pub hit_count: i64,
    pub miss_count: i64,
    pub entry_count: i64,
    pub memory_bytes: i64,
    pub size_limit_bytes: i64,
}

#[repr(C)]
pub struct CacheStatsRepr {
    pub metadata_cache: CacheGroupRepr,
    pub statistics_cache: CacheGroupRepr,
    pub column_index_cache: CacheGroupRepr,
    pub offset_index_cache: CacheGroupRepr,
}

impl Default for CacheGroupRepr {
    fn default() -> Self {
        Self {
            hit_count: 0,
            miss_count: 0,
            entry_count: 0,
            memory_bytes: 0,
            size_limit_bytes: 0,
        }
    }
}

impl Default for CacheStatsRepr {
    fn default() -> Self {
        Self {
            metadata_cache: CacheGroupRepr::default(),
            statistics_cache: CacheGroupRepr::default(),
            column_index_cache: CacheGroupRepr::default(),
            offset_index_cache: CacheGroupRepr::default(),
        }
    }
}

#[repr(C)]
pub struct SearchStatsRepr {
    pub listing_table_scan: i64,
    pub single_collector_scan: i64,
    pub bitmap_tree_scan: i64,
    pub delegation_calls: i64,
    pub rg_processed: i64,
    pub rg_skipped: i64,
    pub parquet_scan_total_time_ms: i64,
    pub parquet_scan_until_data_time_ms: i64,
    pub parquet_processing_time_ms: i64,
    pub parquet_bytes_scanned: i64,
    pub prefetch_wait_time_ms: i64,
    pub prefetch_wait_count: i64,
    pub elapsed_compute_ms: i64,
    pub build_mask_time_ms: i64,
    pub on_batch_mask_time_ms: i64,
    pub filter_record_batch_time_ms: i64,
    pub object_store_read_time_ms: i64,
}

#[repr(C)]
pub struct DfStatsBuffer {
    pub io_runtime: RuntimeMetricsRepr,
    pub cpu_runtime: RuntimeMetricsRepr,
    pub coordinator_reduce: TaskMonitorRepr,
    pub query_execution: TaskMonitorRepr,
    pub stream_next: TaskMonitorRepr,
    pub plan_setup: TaskMonitorRepr,
    pub fragment_executor_gate: PartitionGateRepr,
    pub adaptive_budget: AdaptiveBudgetRepr,
    pub cache_stats: CacheStatsRepr,
    pub search_stats: SearchStatsRepr,
}

#[repr(C)]
pub struct AdaptiveBudgetRepr {
    pub fallbacks: i64,
    pub rejections: i64,
}

const _: () = assert!(size_of::<RuntimeMetricsRepr>() == 9 * 8);
const _: () = assert!(size_of::<TaskMonitorRepr>() == 5 * 8);
const _: () = assert!(size_of::<PartitionGateRepr>() == 8 * 8);
const _: () = assert!(size_of::<AdaptiveBudgetRepr>() == 2 * 8);
const _: () = assert!(size_of::<CacheGroupRepr>() == 5 * 8);
const _: () = assert!(size_of::<CacheStatsRepr>() == 20 * 8);
const _: () = assert!(size_of::<SearchStatsRepr>() == 17 * 8);
const _: () = assert!(size_of::<DfStatsBuffer>() == 85 * 8);

pub mod layout {
    use super::*;
    pub const BUFFER_BYTE_SIZE: usize = size_of::<DfStatsBuffer>();
    const _: () = assert!(BUFFER_BYTE_SIZE == 680);
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
        instrumented_count: cumulative.instrumented_count as i64,
        dropped_count: cumulative.dropped_count as i64,
    }
}

/// Snapshot a `ConcurrencyGate` and return a populated `PartitionGateRepr`.
///
/// | Field                    | Source                              |
/// |--------------------------|-------------------------------------|
/// | max_permits              | `gate.max_permits()`                |
/// | active_permits           | `gate.active_permits()`             |
/// | total_wait_duration_ms   | `gate.total_wait_ms()`              |
/// | total_batches_started    | `gate.total_queries_admitted()`     |
/// | poison_permits           | `gate.poison_permits_held()`        |
/// | target_max_permits       | `gate.target_max_permits()`         |
/// | pending_acquire_permits  | `gate.pending_acquire_permits()`    |
/// | pending_acquire_batches  | `gate.pending_acquire_batches()`    |
pub fn pack_partition_gate(gate: &ConcurrencyGate) -> PartitionGateRepr {
    PartitionGateRepr {
        max_permits: gate.max_permits() as i64,
        active_permits: gate.active_permits() as i64,
        total_wait_duration_ms: gate.total_wait_ms() as i64,
        total_batches_started: gate.total_queries_admitted() as i64,
        poison_permits: gate.poison_permits_held() as i64,
        target_max_permits: gate.target_max_permits() as i64,
        pending_acquire_permits: gate.pending_acquire_permits() as i64,
        pending_acquire_batches: gate.pending_acquire_batches() as i64,
    }
}

/// Snapshot the global `AdaptiveBudgetStats` counters and return a populated `AdaptiveBudgetRepr`.
pub fn pack_adaptive_budget() -> AdaptiveBudgetRepr {
    use std::sync::atomic::Ordering;
    let stats = crate::query_budget::adaptive_budget();
    AdaptiveBudgetRepr {
        fallbacks: stats.fallbacks.load(Ordering::Relaxed) as i64,
        rejections: stats.rejections.load(Ordering::Relaxed) as i64,
    }
}

/// Snapshot the [`CustomCacheManager`] caches and return a populated
/// [`CacheStatsRepr`]. Disabled caches return all-zeros via the manager's
/// `unwrap_or(0)` accessor fallbacks.
///
/// | Field            | Source                                              |
/// |------------------|-----------------------------------------------------|
/// | hit_count        | `mgr.{metadata,statistics}_cache_hit_count()`       |
/// | miss_count       | `mgr.{metadata,statistics}_cache_miss_count()`      |
/// | entry_count      | `mgr.{metadata,statistics}_cache_entry_count()`     |
/// | memory_bytes     | `mgr.get_memory_consumed_by_type({METADATA,STATS})` |
/// | size_limit_bytes | `mgr.{metadata,statistics}_cache_size_limit()`      |
pub fn pack_cache_stats(mgr: &CustomCacheManager) -> CacheStatsRepr {
    let metadata_memory = mgr
        .get_memory_consumed_by_type(crate::cache::CACHE_TYPE_METADATA)
        .unwrap_or(0) as i64;
    let statistics_memory = mgr
        .get_memory_consumed_by_type(crate::cache::CACHE_TYPE_STATS)
        .unwrap_or(0) as i64;

    let ci = crate::cache::page_index::column_index_cache_stats();
    let oi = crate::cache::page_index::offset_index_cache_stats();

    CacheStatsRepr {
        metadata_cache: CacheGroupRepr {
            hit_count: mgr.metadata_cache_hit_count() as i64,
            miss_count: mgr.metadata_cache_miss_count() as i64,
            entry_count: mgr.metadata_cache_entry_count() as i64,
            memory_bytes: metadata_memory,
            size_limit_bytes: mgr.metadata_cache_size_limit() as i64,
        },
        statistics_cache: CacheGroupRepr {
            hit_count: mgr.statistics_cache_hit_count() as i64,
            miss_count: mgr.statistics_cache_miss_count() as i64,
            entry_count: mgr.statistics_cache_entry_count() as i64,
            memory_bytes: statistics_memory,
            size_limit_bytes: mgr.statistics_cache_size_limit() as i64,
        },
        column_index_cache: CacheGroupRepr {
            hit_count: ci.hits as i64,
            miss_count: ci.misses as i64,
            entry_count: ci.entries as i64,
            memory_bytes: ci.used_bytes as i64,
            size_limit_bytes: ci.limit_bytes as i64,
        },
        offset_index_cache: CacheGroupRepr {
            hit_count: oi.hits as i64,
            miss_count: oi.misses as i64,
            entry_count: oi.entries as i64,
            memory_bytes: oi.used_bytes as i64,
            size_limit_bytes: oi.limit_bytes as i64,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_monitors::{
        coordinator_reduce_monitor, plan_setup_monitor, query_execution_monitor,
        stream_next_monitor,
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
        assert_eq!(result.instrumented_count, 0);
        assert_eq!(result.dropped_count, 0);
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
        assert!(
            tm.total_poll_duration_ms >= 0,
            "total_poll_duration should be >= 0, got {}",
            tm.total_poll_duration_ms
        );
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
            fragment_executor_gate: pack_partition_gate(mgr.cpu_executor.concurrency_gate()),
            adaptive_budget: pack_adaptive_budget(),
            cache_stats: CacheStatsRepr::default(),
            search_stats: crate::search_stats::snapshot(),
        };

        assert_eq!(layout::BUFFER_BYTE_SIZE, 680);
        assert!(
            buf.io_runtime.workers_count > 0,
            "IO runtime workers_count should be > 0, got {}",
            buf.io_runtime.workers_count
        );
        assert!(
            buf.fragment_executor_gate.max_permits > 0,
            "fragment_executor_gate max_permits should be > 0, got {}",
            buf.fragment_executor_gate.max_permits
        );

        if mgr.cpu_monitor.is_some() {
            assert!(
                buf.cpu_runtime.workers_count > 0,
                "CPU runtime workers_count should be > 0, got {}",
                buf.cpu_runtime.workers_count
            );
        }

        mgr.cpu_executor.shutdown();
        std::mem::forget(mgr);
    }

    #[test]
    fn test_df_stats_buffer_too_small() {
        // Verify that the buffer size assertion holds
        assert_eq!(size_of::<DfStatsBuffer>(), 680);
        assert_eq!(layout::BUFFER_BYTE_SIZE, 680);
        // A buffer smaller than 680 bytes should be rejected by df_stats.
        // We can't call df_stats directly without a runtime manager,
        // but we verify the constant is correct.
        assert!(layout::BUFFER_BYTE_SIZE > 0);
    }

    #[test]
    fn test_pack_cache_stats_empty_manager_zeroed() {
        use crate::custom_cache_manager::CustomCacheManager;
        let mgr = CustomCacheManager::new();
        let repr = pack_cache_stats(&mgr);
        for g in [&repr.metadata_cache, &repr.statistics_cache] {
            assert_eq!(g.hit_count, 0);
            assert_eq!(g.miss_count, 0);
            assert_eq!(g.entry_count, 0);
            assert_eq!(g.memory_bytes, 0);
            assert_eq!(g.size_limit_bytes, 0);
        }
    }

    #[test]
    fn test_pack_cache_stats_reflects_underlying_counters() {
        use std::sync::Arc;

        use datafusion::execution::cache::CacheAccessor;
        use datafusion::execution::cache::DefaultFilesMetadataCache;
        use object_store::path::Path;

        use crate::cache::MutexFileMetadataCache;
        use crate::custom_cache_manager::CustomCacheManager;
        use crate::eviction_policy::PolicyType;
        use crate::statistics_cache::CustomStatisticsCache;

        let metadata_cache = Arc::new(MutexFileMetadataCache::new(DefaultFilesMetadataCache::new(
            50 * 1024 * 1024,
        )));
        let stats_cache = Arc::new(CustomStatisticsCache::new(
            PolicyType::Lru,
            10 * 1024 * 1024,
            0.8,
        ));

        // Drive 3 misses on each cache and one extra miss on metadata.
        for i in 0..3 {
            let p = Path::from(format!("/test/missing/{i}.parquet"));
            assert!(metadata_cache.get(&p).is_none());
            assert!(stats_cache.get(&p).is_none());
        }
        assert!(metadata_cache
            .get(&Path::from("/test/missing/extra.parquet"))
            .is_none());

        let mut mgr = CustomCacheManager::new();
        mgr.set_file_metadata_cache(metadata_cache);
        mgr.set_statistics_cache(stats_cache);

        let repr = pack_cache_stats(&mgr);

        assert_eq!(repr.metadata_cache.hit_count, 0);
        assert_eq!(repr.metadata_cache.miss_count, 4);
        assert_eq!(repr.statistics_cache.hit_count, 0);
        assert_eq!(repr.statistics_cache.miss_count, 3);

        assert_eq!(repr.metadata_cache.size_limit_bytes, 50 * 1024 * 1024);
        assert_eq!(repr.statistics_cache.size_limit_bytes, 10 * 1024 * 1024);

        assert_eq!(repr.metadata_cache.entry_count, 0);
        assert_eq!(repr.statistics_cache.entry_count, 0);
    }
}
