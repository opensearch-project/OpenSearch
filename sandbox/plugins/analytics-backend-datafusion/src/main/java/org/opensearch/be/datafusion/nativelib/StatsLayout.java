/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.be.datafusion.stats.AdaptiveBudgetStats;
import org.opensearch.be.datafusion.stats.CacheGroupStats;
import org.opensearch.be.datafusion.stats.CacheStats;
import org.opensearch.be.datafusion.stats.LiquidCacheStats;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.SearchStats;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Defines the {@code MemoryLayout.structLayout} mirroring the Rust {@code DfStatsBuffer}
 * and provides {@link VarHandle} accessors for each field via layout path navigation.
 *
 * <p>The layout contains 10 named groups (2 runtime × 9 fields + 4 task monitor × 5 fields
 * + 1 partition gate × 8 fields + 1 adaptive budget × 2 fields + 1 cache stats × 10 fields
 * + 1 search stats × 17 fields = 85 longs = 680 bytes). The cache stats group holds three sub-caches × 5 fields each: metadata, statistics, and scoped page-index..
 */
public final class StatsLayout {

    private static final String[] RUNTIME_FIELDS = {
        "workers_count",
        "total_polls_count",
        "total_busy_duration_ms",
        "total_overflow_count",
        "global_queue_depth",
        "blocking_queue_depth",
        "num_alive_tasks",
        "spawned_tasks_count",
        "total_local_queue_depth" };

    private static final String[] TASK_MONITOR_FIELDS = {
        "total_poll_duration_ms",
        "total_scheduled_duration_ms",
        "total_idle_duration_ms",
        "instrumented_count",
        "dropped_count" };

    private static final String[] PARTITION_GATE_FIELDS = {
        "max_permits",
        "active_permits",
        "total_wait_duration_ms",
        "total_batches_started",
        "poison_permits",
        "target_max_permits",
        "pending_acquire_permits",
        "pending_acquire_batches" };

    private static final String[] ADAPTIVE_BUDGET_FIELDS = { "fallbacks", "rejections" };

    private static final String[] CACHE_GROUP_FIELDS = { "hit_count", "miss_count", "entry_count", "memory_bytes", "size_limit_bytes" };

    private static final String[] SEARCH_STATS_FIELDS = {
        "listing_table_scan",
        "single_collector_scan",
        "bitmap_tree_scan",
        "delegation_calls",
        "rg_processed",
        "rg_skipped",
        "parquet_scan_total_time_ms",
        "parquet_scan_until_data_time_ms",
        "parquet_processing_time_ms",
        "parquet_bytes_scanned",
        "prefetch_wait_time_ms",
        "prefetch_wait_count",
        "elapsed_compute_ms",
        "build_mask_time_ms",
        "on_batch_mask_time_ms",
        "filter_record_batch_time_ms",
        "object_store_read_time_ms" };

    /** The struct layout mirroring Rust's {@code DfStatsBuffer}. */
    public static final StructLayout LAYOUT = MemoryLayout.structLayout(
        runtimeGroup("io_runtime"),
        runtimeGroup("cpu_runtime"),
        taskMonitorGroup("coordinator_reduce"),
        taskMonitorGroup("query_execution"),
        taskMonitorGroup("stream_next"),
        taskMonitorGroup("plan_setup"),
        partitionGateGroup("fragment_executor_gate"),
        adaptiveBudgetGroup("adaptive_budget"),
        cacheStatsGroup("cache_stats"),
        searchStatsGroup("search_stats"),
        liquidCacheStatsGroup("liquid_cache")
    );

    static {
        if (LAYOUT.byteSize() != 93 * Long.BYTES) {
            throw new AssertionError("StatsLayout size mismatch: expected " + (93 * Long.BYTES) + " but got " + LAYOUT.byteSize());
        }
    }

    // ---- VarHandles for io_runtime fields ----
    private static final VarHandle IO_WORKERS_COUNT = handle("io_runtime", "workers_count");
    private static final VarHandle IO_TOTAL_POLLS_COUNT = handle("io_runtime", "total_polls_count");
    private static final VarHandle IO_TOTAL_BUSY_DURATION_MS = handle("io_runtime", "total_busy_duration_ms");
    private static final VarHandle IO_TOTAL_OVERFLOW_COUNT = handle("io_runtime", "total_overflow_count");
    private static final VarHandle IO_GLOBAL_QUEUE_DEPTH = handle("io_runtime", "global_queue_depth");
    private static final VarHandle IO_BLOCKING_QUEUE_DEPTH = handle("io_runtime", "blocking_queue_depth");
    private static final VarHandle IO_NUM_ALIVE_TASKS = handle("io_runtime", "num_alive_tasks");
    private static final VarHandle IO_SPAWNED_TASKS_COUNT = handle("io_runtime", "spawned_tasks_count");
    private static final VarHandle IO_TOTAL_LOCAL_QUEUE_DEPTH = handle("io_runtime", "total_local_queue_depth");

    // ---- VarHandles for cpu_runtime fields ----
    private static final VarHandle CPU_WORKERS_COUNT = handle("cpu_runtime", "workers_count");
    private static final VarHandle CPU_TOTAL_POLLS_COUNT = handle("cpu_runtime", "total_polls_count");
    private static final VarHandle CPU_TOTAL_BUSY_DURATION_MS = handle("cpu_runtime", "total_busy_duration_ms");
    private static final VarHandle CPU_TOTAL_OVERFLOW_COUNT = handle("cpu_runtime", "total_overflow_count");
    private static final VarHandle CPU_GLOBAL_QUEUE_DEPTH = handle("cpu_runtime", "global_queue_depth");
    private static final VarHandle CPU_BLOCKING_QUEUE_DEPTH = handle("cpu_runtime", "blocking_queue_depth");
    private static final VarHandle CPU_NUM_ALIVE_TASKS = handle("cpu_runtime", "num_alive_tasks");
    private static final VarHandle CPU_SPAWNED_TASKS_COUNT = handle("cpu_runtime", "spawned_tasks_count");
    private static final VarHandle CPU_TOTAL_LOCAL_QUEUE_DEPTH = handle("cpu_runtime", "total_local_queue_depth");

    // ---- VarHandles for coordinator_reduce fields ----
    private static final VarHandle CR_TOTAL_POLL_DURATION_MS = handle("coordinator_reduce", "total_poll_duration_ms");
    private static final VarHandle CR_TOTAL_SCHEDULED_DURATION_MS = handle("coordinator_reduce", "total_scheduled_duration_ms");
    private static final VarHandle CR_TOTAL_IDLE_DURATION_MS = handle("coordinator_reduce", "total_idle_duration_ms");
    private static final VarHandle CR_INSTRUMENTED_COUNT = handle("coordinator_reduce", "instrumented_count");
    private static final VarHandle CR_DROPPED_COUNT = handle("coordinator_reduce", "dropped_count");

    // ---- VarHandles for query_execution fields ----
    private static final VarHandle QE_TOTAL_POLL_DURATION_MS = handle("query_execution", "total_poll_duration_ms");
    private static final VarHandle QE_TOTAL_SCHEDULED_DURATION_MS = handle("query_execution", "total_scheduled_duration_ms");
    private static final VarHandle QE_TOTAL_IDLE_DURATION_MS = handle("query_execution", "total_idle_duration_ms");
    private static final VarHandle QE_INSTRUMENTED_COUNT = handle("query_execution", "instrumented_count");
    private static final VarHandle QE_DROPPED_COUNT = handle("query_execution", "dropped_count");

    // ---- VarHandles for stream_next fields ----
    private static final VarHandle SN_TOTAL_POLL_DURATION_MS = handle("stream_next", "total_poll_duration_ms");
    private static final VarHandle SN_TOTAL_SCHEDULED_DURATION_MS = handle("stream_next", "total_scheduled_duration_ms");
    private static final VarHandle SN_TOTAL_IDLE_DURATION_MS = handle("stream_next", "total_idle_duration_ms");
    private static final VarHandle SN_INSTRUMENTED_COUNT = handle("stream_next", "instrumented_count");
    private static final VarHandle SN_DROPPED_COUNT = handle("stream_next", "dropped_count");

    // ---- VarHandles for plan_setup fields ----
    private static final VarHandle PS_TOTAL_POLL_DURATION_MS = handle("plan_setup", "total_poll_duration_ms");
    private static final VarHandle PS_TOTAL_SCHEDULED_DURATION_MS = handle("plan_setup", "total_scheduled_duration_ms");
    private static final VarHandle PS_TOTAL_IDLE_DURATION_MS = handle("plan_setup", "total_idle_duration_ms");
    private static final VarHandle PS_INSTRUMENTED_COUNT = handle("plan_setup", "instrumented_count");
    private static final VarHandle PS_DROPPED_COUNT = handle("plan_setup", "dropped_count");

    // ---- VarHandles for fragment_executor_gate fields ----
    private static final VarHandle DG_MAX_PERMITS = handle("fragment_executor_gate", "max_permits");
    private static final VarHandle DG_ACTIVE_PERMITS = handle("fragment_executor_gate", "active_permits");
    private static final VarHandle DG_TOTAL_WAIT_DURATION_MS = handle("fragment_executor_gate", "total_wait_duration_ms");
    private static final VarHandle DG_TOTAL_BATCHES_STARTED = handle("fragment_executor_gate", "total_batches_started");
    private static final VarHandle DG_POISON_PERMITS = handle("fragment_executor_gate", "poison_permits");
    private static final VarHandle DG_TARGET_MAX_PERMITS = handle("fragment_executor_gate", "target_max_permits");
    private static final VarHandle DG_PENDING_ACQUIRE_PERMITS = handle("fragment_executor_gate", "pending_acquire_permits");
    private static final VarHandle DG_PENDING_ACQUIRE_BATCHES = handle("fragment_executor_gate", "pending_acquire_batches");

    // ---- VarHandles for adaptive_budget fields ----
    private static final VarHandle AB_FALLBACKS = handle("adaptive_budget", "fallbacks");
    private static final VarHandle AB_REJECTIONS = handle("adaptive_budget", "rejections");

    // ---- VarHandles for cache_stats.metadata_cache fields ----
    private static final VarHandle CACHE_META_HIT_COUNT = cacheHandle("metadata_cache", "hit_count");
    private static final VarHandle CACHE_META_MISS_COUNT = cacheHandle("metadata_cache", "miss_count");
    private static final VarHandle CACHE_META_ENTRY_COUNT = cacheHandle("metadata_cache", "entry_count");
    private static final VarHandle CACHE_META_MEMORY_BYTES = cacheHandle("metadata_cache", "memory_bytes");
    private static final VarHandle CACHE_META_SIZE_LIMIT_BYTES = cacheHandle("metadata_cache", "size_limit_bytes");

    // ---- VarHandles for cache_stats.statistics_cache fields ----
    private static final VarHandle CACHE_STATS_HIT_COUNT = cacheHandle("statistics_cache", "hit_count");
    private static final VarHandle CACHE_STATS_MISS_COUNT = cacheHandle("statistics_cache", "miss_count");
    private static final VarHandle CACHE_STATS_ENTRY_COUNT = cacheHandle("statistics_cache", "entry_count");
    private static final VarHandle CACHE_STATS_MEMORY_BYTES = cacheHandle("statistics_cache", "memory_bytes");
    private static final VarHandle CACHE_STATS_SIZE_LIMIT_BYTES = cacheHandle("statistics_cache", "size_limit_bytes");

    // ---- VarHandles for cache_stats.column_index_cache fields ----
    private static final VarHandle CACHE_CI_HIT_COUNT = cacheHandle("column_index_cache", "hit_count");
    private static final VarHandle CACHE_CI_MISS_COUNT = cacheHandle("column_index_cache", "miss_count");
    private static final VarHandle CACHE_CI_ENTRY_COUNT = cacheHandle("column_index_cache", "entry_count");
    private static final VarHandle CACHE_CI_MEMORY_BYTES = cacheHandle("column_index_cache", "memory_bytes");
    private static final VarHandle CACHE_CI_SIZE_LIMIT_BYTES = cacheHandle("column_index_cache", "size_limit_bytes");

    // ---- VarHandles for cache_stats.offset_index_cache fields ----
    private static final VarHandle CACHE_OI_HIT_COUNT = cacheHandle("offset_index_cache", "hit_count");
    private static final VarHandle CACHE_OI_MISS_COUNT = cacheHandle("offset_index_cache", "miss_count");
    private static final VarHandle CACHE_OI_ENTRY_COUNT = cacheHandle("offset_index_cache", "entry_count");
    private static final VarHandle CACHE_OI_MEMORY_BYTES = cacheHandle("offset_index_cache", "memory_bytes");
    private static final VarHandle CACHE_OI_SIZE_LIMIT_BYTES = cacheHandle("offset_index_cache", "size_limit_bytes");

    // ---- VarHandles for search_stats fields ----
    private static final VarHandle SS_LISTING_TABLE_SCAN = handle("search_stats", "listing_table_scan");
    private static final VarHandle SS_SINGLE_COLLECTOR_SCAN = handle("search_stats", "single_collector_scan");
    private static final VarHandle SS_BITMAP_TREE_SCAN = handle("search_stats", "bitmap_tree_scan");
    private static final VarHandle SS_DELEGATION_CALLS = handle("search_stats", "delegation_calls");
    private static final VarHandle SS_RG_PROCESSED = handle("search_stats", "rg_processed");
    private static final VarHandle SS_RG_SKIPPED = handle("search_stats", "rg_skipped");
    private static final VarHandle SS_PARQUET_SCAN_TOTAL_TIME_MS = handle("search_stats", "parquet_scan_total_time_ms");
    private static final VarHandle SS_PARQUET_SCAN_UNTIL_DATA_TIME_MS = handle("search_stats", "parquet_scan_until_data_time_ms");
    private static final VarHandle SS_PARQUET_PROCESSING_TIME_MS = handle("search_stats", "parquet_processing_time_ms");
    private static final VarHandle SS_PARQUET_BYTES_SCANNED = handle("search_stats", "parquet_bytes_scanned");
    private static final VarHandle SS_PREFETCH_WAIT_TIME_MS = handle("search_stats", "prefetch_wait_time_ms");
    private static final VarHandle SS_PREFETCH_WAIT_COUNT = handle("search_stats", "prefetch_wait_count");
    private static final VarHandle SS_ELAPSED_COMPUTE_MS = handle("search_stats", "elapsed_compute_ms");
    private static final VarHandle SS_BUILD_MASK_TIME_MS = handle("search_stats", "build_mask_time_ms");
    private static final VarHandle SS_ON_BATCH_MASK_TIME_MS = handle("search_stats", "on_batch_mask_time_ms");
    private static final VarHandle SS_FILTER_RECORD_BATCH_TIME_MS = handle("search_stats", "filter_record_batch_time_ms");
    private static final VarHandle SS_OBJECT_STORE_READ_TIME_MS = handle("search_stats", "object_store_read_time_ms");

    // ---- VarHandles for liquid_cache fields ----
    private static final VarHandle LC_CACHE_HIT = handle("liquid_cache", "cache_hit");
    private static final VarHandle LC_CACHE_MISS = handle("liquid_cache", "cache_miss");
    private static final VarHandle LC_PREDICATE_EVALS = handle("liquid_cache", "predicate_evals");
    private static final VarHandle LC_MEMORY_EVICTIONS = handle("liquid_cache", "memory_evictions");
    private static final VarHandle LC_TRANSCODES = handle("liquid_cache", "transcodes");
    private static final VarHandle LC_TOTAL_ENTRIES = handle("liquid_cache", "total_entries");
    private static final VarHandle LC_MEMORY_USAGE_BYTES = handle("liquid_cache", "memory_usage_bytes");
    private static final VarHandle LC_MAX_MEMORY_BYTES = handle("liquid_cache", "max_memory_bytes");

    private StatsLayout() {}

    /**
     * Read a single field from the segment.
     *
     * @param seg   the memory segment containing the DfStatsBuffer
     * @param group the group name (e.g. "io_runtime", "cpu_runtime")
     * @param field the field name (e.g. "workers_count")
     * @return the long value at the specified path
     */
    public static long readField(MemorySegment seg, String group, String field) {
        return (long) handle(group, field).get(seg, 0L);
    }

    /**
     * Read a runtime metrics group (8 fields) from the segment.
     *
     * @param seg   the memory segment containing the DfStatsBuffer
     * @param group "io_runtime" or "cpu_runtime"
     * @return a populated RuntimeMetrics instance
     */
    public static RuntimeMetrics readRuntimeMetrics(MemorySegment seg, String group) {
        VarHandle[] handles = runtimeHandles(group);
        return new RuntimeMetrics(
            (long) handles[0].get(seg, 0L),
            (long) handles[1].get(seg, 0L),
            (long) handles[2].get(seg, 0L),
            (long) handles[3].get(seg, 0L),
            (long) handles[4].get(seg, 0L),
            (long) handles[5].get(seg, 0L),
            (long) handles[6].get(seg, 0L),
            (long) handles[7].get(seg, 0L),
            (long) handles[8].get(seg, 0L)
        );
    }

    /**
     * Read a task monitor group (5 fields) from the segment.
     *
     * @param seg   the memory segment containing the DfStatsBuffer
     * @param group one of the OperationType keys
     * @return a populated TaskMonitorStats instance
     */
    public static TaskMonitorStats readTaskMonitor(MemorySegment seg, String group) {
        VarHandle[] handles = taskMonitorHandles(group);
        return new TaskMonitorStats(
            (long) handles[0].get(seg, 0L),
            (long) handles[1].get(seg, 0L),
            (long) handles[2].get(seg, 0L),
            (long) handles[3].get(seg, 0L),
            (long) handles[4].get(seg, 0L)
        );
    }

    /**
     * Read a partition gate group (8 fields) from the segment.
     *
     * @param seg   the memory segment containing the DfStatsBuffer
     * @param group "fragment_executor_gate"
     * @return a populated PartitionGateStats instance
     */
    public static PartitionGateStats readPartitionGate(MemorySegment seg, String group) {
        return readPartitionGate(seg, group, group);
    }

    /**
     * Read partition gate stats from the native buffer with a custom display name.
     *
     * @param seg         the memory segment containing the DfStatsBuffer
     * @param group       "fragment_executor_gate" (layout key)
     * @param displayName the JSON key name to use when serializing
     * @return a populated PartitionGateStats instance
     */
    public static PartitionGateStats readPartitionGate(MemorySegment seg, String group, String displayName) {
        VarHandle[] handles = partitionGateHandles(group);
        return new PartitionGateStats(
            displayName,
            (long) handles[0].get(seg, 0L),
            (long) handles[1].get(seg, 0L),
            (long) handles[2].get(seg, 0L),
            (long) handles[3].get(seg, 0L),
            (long) handles[4].get(seg, 0L),
            (long) handles[5].get(seg, 0L),
            (long) handles[6].get(seg, 0L),
            (long) handles[7].get(seg, 0L)
        );
    }

    /**
    /**
     * Read the cache_stats group (10 fields, 2 sub-caches × 5 fields each).
     *
     * @param seg the memory segment containing the DfStatsBuffer
     * @return a populated CacheStats instance with metadata + statistics sub-groups
     */
    public static CacheStats readCacheStats(MemorySegment seg) {
        CacheGroupStats metadata = new CacheGroupStats(
            (long) CACHE_META_HIT_COUNT.get(seg, 0L),
            (long) CACHE_META_MISS_COUNT.get(seg, 0L),
            (long) CACHE_META_ENTRY_COUNT.get(seg, 0L),
            (long) CACHE_META_MEMORY_BYTES.get(seg, 0L),
            (long) CACHE_META_SIZE_LIMIT_BYTES.get(seg, 0L)
        );
        CacheGroupStats statistics = new CacheGroupStats(
            (long) CACHE_STATS_HIT_COUNT.get(seg, 0L),
            (long) CACHE_STATS_MISS_COUNT.get(seg, 0L),
            (long) CACHE_STATS_ENTRY_COUNT.get(seg, 0L),
            (long) CACHE_STATS_MEMORY_BYTES.get(seg, 0L),
            (long) CACHE_STATS_SIZE_LIMIT_BYTES.get(seg, 0L)
        );
        CacheGroupStats columnIndex = new CacheGroupStats(
            (long) CACHE_CI_HIT_COUNT.get(seg, 0L),
            (long) CACHE_CI_MISS_COUNT.get(seg, 0L),
            (long) CACHE_CI_ENTRY_COUNT.get(seg, 0L),
            (long) CACHE_CI_MEMORY_BYTES.get(seg, 0L),
            (long) CACHE_CI_SIZE_LIMIT_BYTES.get(seg, 0L)
        );
        CacheGroupStats offsetIndex = new CacheGroupStats(
            (long) CACHE_OI_HIT_COUNT.get(seg, 0L),
            (long) CACHE_OI_MISS_COUNT.get(seg, 0L),
            (long) CACHE_OI_ENTRY_COUNT.get(seg, 0L),
            (long) CACHE_OI_MEMORY_BYTES.get(seg, 0L),
            (long) CACHE_OI_SIZE_LIMIT_BYTES.get(seg, 0L)
        );
        return new CacheStats(metadata, statistics, columnIndex, offsetIndex);
    }

    /**
     * Read the search_stats group (17 fields) from the segment.
     *
     * @param seg the memory segment containing the DfStatsBuffer
     * @return a populated SearchStats instance
     */
    public static SearchStats readSearchStats(MemorySegment seg) {
        return new SearchStats(
            (long) SS_LISTING_TABLE_SCAN.get(seg, 0L),
            (long) SS_SINGLE_COLLECTOR_SCAN.get(seg, 0L),
            (long) SS_BITMAP_TREE_SCAN.get(seg, 0L),
            (long) SS_DELEGATION_CALLS.get(seg, 0L),
            (long) SS_RG_PROCESSED.get(seg, 0L),
            (long) SS_RG_SKIPPED.get(seg, 0L),
            (long) SS_PARQUET_SCAN_TOTAL_TIME_MS.get(seg, 0L),
            (long) SS_PARQUET_SCAN_UNTIL_DATA_TIME_MS.get(seg, 0L),
            (long) SS_PARQUET_PROCESSING_TIME_MS.get(seg, 0L),
            (long) SS_PARQUET_BYTES_SCANNED.get(seg, 0L),
            (long) SS_PREFETCH_WAIT_TIME_MS.get(seg, 0L),
            (long) SS_PREFETCH_WAIT_COUNT.get(seg, 0L),
            (long) SS_ELAPSED_COMPUTE_MS.get(seg, 0L),
            (long) SS_BUILD_MASK_TIME_MS.get(seg, 0L),
            (long) SS_ON_BATCH_MASK_TIME_MS.get(seg, 0L),
            (long) SS_FILTER_RECORD_BATCH_TIME_MS.get(seg, 0L),
            (long) SS_OBJECT_STORE_READ_TIME_MS.get(seg, 0L)
        );
    }

    /**
     * Read the adaptive budget group (2 fields) from the segment.
     *
     * @param seg the memory segment containing the DfStatsBuffer
     * @return a populated AdaptiveBudgetStats instance
     */
    public static AdaptiveBudgetStats readAdaptiveBudgetStats(MemorySegment seg) {
        return new AdaptiveBudgetStats((long) AB_FALLBACKS.get(seg, 0L), (long) AB_REJECTIONS.get(seg, 0L));
    }

    /**
     * Read the liquid_cache group (8 fields) from the segment.
     *
     * @param seg the memory segment containing the DfStatsBuffer
     * @return a populated LiquidCacheStats instance (all-zero when LC is not engaged)
     */
    public static LiquidCacheStats readLiquidCacheStats(MemorySegment seg) {
        return new LiquidCacheStats(
            (long) LC_CACHE_HIT.get(seg, 0L),
            (long) LC_CACHE_MISS.get(seg, 0L),
            (long) LC_PREDICATE_EVALS.get(seg, 0L),
            (long) LC_MEMORY_EVICTIONS.get(seg, 0L),
            (long) LC_TRANSCODES.get(seg, 0L),
            (long) LC_TOTAL_ENTRIES.get(seg, 0L),
            (long) LC_MEMORY_USAGE_BYTES.get(seg, 0L),
            (long) LC_MAX_MEMORY_BYTES.get(seg, 0L)
        );
    }

    // ---- Private helpers ----

    private static StructLayout runtimeGroup(String name) {
        return MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("workers_count"),
            ValueLayout.JAVA_LONG.withName("total_polls_count"),
            ValueLayout.JAVA_LONG.withName("total_busy_duration_ms"),
            ValueLayout.JAVA_LONG.withName("total_overflow_count"),
            ValueLayout.JAVA_LONG.withName("global_queue_depth"),
            ValueLayout.JAVA_LONG.withName("blocking_queue_depth"),
            ValueLayout.JAVA_LONG.withName("num_alive_tasks"),
            ValueLayout.JAVA_LONG.withName("spawned_tasks_count"),
            ValueLayout.JAVA_LONG.withName("total_local_queue_depth")
        ).withName(name);
    }

    private static StructLayout taskMonitorGroup(String name) {
        return MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("total_poll_duration_ms"),
            ValueLayout.JAVA_LONG.withName("total_scheduled_duration_ms"),
            ValueLayout.JAVA_LONG.withName("total_idle_duration_ms"),
            ValueLayout.JAVA_LONG.withName("instrumented_count"),
            ValueLayout.JAVA_LONG.withName("dropped_count")
        ).withName(name);
    }

    private static StructLayout partitionGateGroup(String name) {
        return MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("max_permits"),
            ValueLayout.JAVA_LONG.withName("active_permits"),
            ValueLayout.JAVA_LONG.withName("total_wait_duration_ms"),
            ValueLayout.JAVA_LONG.withName("total_batches_started"),
            ValueLayout.JAVA_LONG.withName("poison_permits"),
            ValueLayout.JAVA_LONG.withName("target_max_permits"),
            ValueLayout.JAVA_LONG.withName("pending_acquire_permits"),
            ValueLayout.JAVA_LONG.withName("pending_acquire_batches")
        ).withName(name);
    }

    private static StructLayout cacheGroup(String name) {
        return MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("hit_count"),
            ValueLayout.JAVA_LONG.withName("miss_count"),
            ValueLayout.JAVA_LONG.withName("entry_count"),
            ValueLayout.JAVA_LONG.withName("memory_bytes"),
            ValueLayout.JAVA_LONG.withName("size_limit_bytes")
        ).withName(name);
    }

    private static StructLayout cacheStatsGroup(String name) {
        return MemoryLayout.structLayout(
            cacheGroup("metadata_cache"),
            cacheGroup("statistics_cache"),
            cacheGroup("column_index_cache"),
            cacheGroup("offset_index_cache")
        ).withName(name);
    }

    private static StructLayout searchStatsGroup(String name) {
        return MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("listing_table_scan"),
            ValueLayout.JAVA_LONG.withName("single_collector_scan"),
            ValueLayout.JAVA_LONG.withName("bitmap_tree_scan"),
            ValueLayout.JAVA_LONG.withName("delegation_calls"),
            ValueLayout.JAVA_LONG.withName("rg_processed"),
            ValueLayout.JAVA_LONG.withName("rg_skipped"),
            ValueLayout.JAVA_LONG.withName("parquet_scan_total_time_ms"),
            ValueLayout.JAVA_LONG.withName("parquet_scan_until_data_time_ms"),
            ValueLayout.JAVA_LONG.withName("parquet_processing_time_ms"),
            ValueLayout.JAVA_LONG.withName("parquet_bytes_scanned"),
            ValueLayout.JAVA_LONG.withName("prefetch_wait_time_ms"),
            ValueLayout.JAVA_LONG.withName("prefetch_wait_count"),
            ValueLayout.JAVA_LONG.withName("elapsed_compute_ms"),
            ValueLayout.JAVA_LONG.withName("build_mask_time_ms"),
            ValueLayout.JAVA_LONG.withName("on_batch_mask_time_ms"),
            ValueLayout.JAVA_LONG.withName("filter_record_batch_time_ms"),
            ValueLayout.JAVA_LONG.withName("object_store_read_time_ms")
        ).withName(name);
    }

    private static StructLayout adaptiveBudgetGroup(String name) {
        return MemoryLayout.structLayout(ValueLayout.JAVA_LONG.withName("fallbacks"), ValueLayout.JAVA_LONG.withName("rejections"))
            .withName(name);
    }

    private static StructLayout liquidCacheStatsGroup(String name) {
        return MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("cache_hit"),
            ValueLayout.JAVA_LONG.withName("cache_miss"),
            ValueLayout.JAVA_LONG.withName("predicate_evals"),
            ValueLayout.JAVA_LONG.withName("memory_evictions"),
            ValueLayout.JAVA_LONG.withName("transcodes"),
            ValueLayout.JAVA_LONG.withName("total_entries"),
            ValueLayout.JAVA_LONG.withName("memory_usage_bytes"),
            ValueLayout.JAVA_LONG.withName("max_memory_bytes")
        ).withName(name);
    }

    private static VarHandle handle(String group, String field) {
        return LAYOUT.varHandle(PathElement.groupElement(group), PathElement.groupElement(field));
    }

    private static VarHandle cacheHandle(String subGroup, String field) {
        return LAYOUT.varHandle(
            PathElement.groupElement("cache_stats"),
            PathElement.groupElement(subGroup),
            PathElement.groupElement(field)
        );
    }

    private static VarHandle[] runtimeHandles(String group) {
        return switch (group) {
            case "io_runtime" -> new VarHandle[] {
                IO_WORKERS_COUNT,
                IO_TOTAL_POLLS_COUNT,
                IO_TOTAL_BUSY_DURATION_MS,
                IO_TOTAL_OVERFLOW_COUNT,
                IO_GLOBAL_QUEUE_DEPTH,
                IO_BLOCKING_QUEUE_DEPTH,
                IO_NUM_ALIVE_TASKS,
                IO_SPAWNED_TASKS_COUNT,
                IO_TOTAL_LOCAL_QUEUE_DEPTH };
            case "cpu_runtime" -> new VarHandle[] {
                CPU_WORKERS_COUNT,
                CPU_TOTAL_POLLS_COUNT,
                CPU_TOTAL_BUSY_DURATION_MS,
                CPU_TOTAL_OVERFLOW_COUNT,
                CPU_GLOBAL_QUEUE_DEPTH,
                CPU_BLOCKING_QUEUE_DEPTH,
                CPU_NUM_ALIVE_TASKS,
                CPU_SPAWNED_TASKS_COUNT,
                CPU_TOTAL_LOCAL_QUEUE_DEPTH };
            default -> throw new IllegalArgumentException("Unknown runtime group: " + group);
        };
    }

    private static VarHandle[] taskMonitorHandles(String group) {
        return switch (group) {
            case "coordinator_reduce" -> new VarHandle[] {
                CR_TOTAL_POLL_DURATION_MS,
                CR_TOTAL_SCHEDULED_DURATION_MS,
                CR_TOTAL_IDLE_DURATION_MS,
                CR_INSTRUMENTED_COUNT,
                CR_DROPPED_COUNT };
            case "query_execution" -> new VarHandle[] {
                QE_TOTAL_POLL_DURATION_MS,
                QE_TOTAL_SCHEDULED_DURATION_MS,
                QE_TOTAL_IDLE_DURATION_MS,
                QE_INSTRUMENTED_COUNT,
                QE_DROPPED_COUNT };
            case "stream_next" -> new VarHandle[] {
                SN_TOTAL_POLL_DURATION_MS,
                SN_TOTAL_SCHEDULED_DURATION_MS,
                SN_TOTAL_IDLE_DURATION_MS,
                SN_INSTRUMENTED_COUNT,
                SN_DROPPED_COUNT };
            case "plan_setup" -> new VarHandle[] {
                PS_TOTAL_POLL_DURATION_MS,
                PS_TOTAL_SCHEDULED_DURATION_MS,
                PS_TOTAL_IDLE_DURATION_MS,
                PS_INSTRUMENTED_COUNT,
                PS_DROPPED_COUNT };
            default -> throw new IllegalArgumentException("Unknown task monitor group: " + group);
        };
    }

    private static VarHandle[] partitionGateHandles(String group) {
        return switch (group) {
            case "fragment_executor_gate" -> new VarHandle[] {
                DG_MAX_PERMITS,
                DG_ACTIVE_PERMITS,
                DG_TOTAL_WAIT_DURATION_MS,
                DG_TOTAL_BATCHES_STARTED,
                DG_POISON_PERMITS,
                DG_TARGET_MAX_PERMITS,
                DG_PENDING_ACQUIRE_PERMITS,
                DG_PENDING_ACQUIRE_BATCHES };
            default -> throw new IllegalArgumentException("Unknown partition gate group: " + group);
        };
    }
}
