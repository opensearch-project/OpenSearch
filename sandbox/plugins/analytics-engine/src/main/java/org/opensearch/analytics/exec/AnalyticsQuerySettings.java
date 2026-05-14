/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * Dynamic cluster-level settings for analytics query execution limits.
 * <p>
 * These control resource usage on the coordinator during query execution
 * and can be updated at runtime via the cluster settings API.
 */
public final class AnalyticsQuerySettings {

    private AnalyticsQuerySettings() {}

    /**
     * Maximum number of concurrent shard requests per analytics query.
     * Controls fan-out parallelism on the coordinator node.
     * <p>
     * Analogous to {@code search.max_concurrent_shard_requests} in the
     * classic DSL path.
     */
    public static final Setting<Integer> MAX_CONCURRENT_SHARD_REQUESTS = Setting.intSetting(
        "analytics.query.max_concurrent_shard_requests",
        5,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Per-query memory limit for Arrow allocations on the coordinator (default 256MB).
     * When exceeded, the query fails with a memory limit error rather than
     * consuming unbounded heap.
     * <p>
     * Analogous to {@code indices.breaker.request.limit} in the classic path.
     */
    public static final Setting<Long> PER_QUERY_MEMORY_LIMIT = Setting.longSetting(
        "analytics.query.per_query_memory_limit_bytes",
        256L * 1024 * 1024,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum number of rows an analytics query can accumulate in the
     * coordinator-reduce sink before rejecting further batches.
     * <p>
     * Analogous to {@code index.max_result_window} in the classic path,
     * but set higher for analytics workloads that aggregate large datasets.
     */
    public static final Setting<Long> MAX_RESULT_ROWS = Setting.longSetting(
        "analytics.query.max_result_rows",
        1_000_000L,
        1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final List<Setting<?>> ALL_SETTINGS = List.of(
        MAX_CONCURRENT_SHARD_REQUESTS,
        PER_QUERY_MEMORY_LIMIT,
        MAX_RESULT_ROWS
    );
}
