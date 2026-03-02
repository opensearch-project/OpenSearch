/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.backpressure;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTask;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.SearchBackpressureTask;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Metrics for search backpressure (task cancellations and limit-reached events).
 * Uses the telemetry MetricsRegistry so plugins like the Prometheus exporter expose these metrics automatically.
 *
 * @opensearch.internal
 */
public final class SearchBackpressureMetrics {

    private static final String COUNTER_UNIT = "1";

    public static final String TASK_TYPE_TAG = "task_type";
    public static final String LIMIT_TYPE_TAG = "limit_type";
    public static final String INDEX_TAG = "index";
    public static final String SHARD_ID_TAG = "shard_id";

    private static final String TASK_TYPE_SEARCH = "search";
    private static final String TASK_TYPE_SEARCH_SHARD = "search_shard";
    private static final String LIMIT_TYPE_CPU = "cpu_usage";
    private static final String LIMIT_TYPE_HEAP = "heap_usage";
    private static final String LIMIT_TYPE_ELAPSED_TIME = "elapsed_time";
    private static final String LIMIT_TYPE_UNKNOWN = "unknown";

    private static final Pattern CPU_PATTERN = Pattern.compile("cpu usage exceeded");
    private static final Pattern HEAP_PATTERN = Pattern.compile("heap usage exceeded");
    private static final Pattern ELAPSED_TIME_PATTERN = Pattern.compile("elapsed time exceeded");
    private static final Pattern SHARD_ID_PATTERN = Pattern.compile("shardId\\[\\[([^\\]]+)\\]\\[(\\d+)\\]\\]");

    private final Counter cancellationCountCounter;
    private final Counter limitReachedCountCounter;

    public SearchBackpressureMetrics(MetricsRegistry metricsRegistry) {
        this.cancellationCountCounter = metricsRegistry.createCounter(
            "search.backpressure.cancellation.count",
            "Number of search tasks cancelled due to resource backpressure",
            COUNTER_UNIT
        );
        this.limitReachedCountCounter = metricsRegistry.createCounter(
            "search.backpressure.limit_reached.count",
            "Number of times search backpressure skipped cancellation due to rate/ratio limits",
            COUNTER_UNIT
        );
    }

    /**
     * Record a task cancellation. Called when a search task is cancelled by the backpressure service.
     *
     * @param task         the cancelled task
     * @param taskType     SearchTask.class or SearchShardTask.class
     * @param reasonString combined cancellation reason (e.g. "cpu usage exceeded [30s >= 30s], heap usage exceeded ...")
     */
    public void recordCancellation(CancellableTask task, Class<? extends SearchBackpressureTask> taskType, String reasonString) {
        String taskTypeTag = getTaskTypeTag(taskType);
        List<String> limitTypes = extractLimitTypes(reasonString);
        ShardInfo shardInfo = (taskType == SearchShardTask.class) ? extractShardInfo(task.getDescription()) : null;

        for (String limitType : limitTypes) {
            Tags tags = Tags.create().addTag(TASK_TYPE_TAG, taskTypeTag).addTag(LIMIT_TYPE_TAG, limitType);
            if (shardInfo != null) {
                tags = tags.addTag(INDEX_TAG, shardInfo.indexName).addTag(SHARD_ID_TAG, shardInfo.shardId);
            }
            cancellationCountCounter.add(1.0, tags);
        }
    }

    /**
     * Record a limit-reached event (cancellation was skipped due to rate/ratio limits).
     *
     * @param taskType SearchTask.class or SearchShardTask.class
     */
    public void recordLimitReached(Class<? extends SearchBackpressureTask> taskType) {
        String taskTypeTag = getTaskTypeTag(taskType);
        limitReachedCountCounter.add(1.0, Tags.create().addTag(TASK_TYPE_TAG, taskTypeTag));
    }

    private static String getTaskTypeTag(Class<? extends SearchBackpressureTask> taskType) {
        if (taskType == SearchTask.class) {
            return TASK_TYPE_SEARCH;
        }
        if (taskType == SearchShardTask.class) {
            return TASK_TYPE_SEARCH_SHARD;
        }
        return "unknown";
    }

    private static List<String> extractLimitTypes(String reasonString) {
        List<String> limitTypes = new ArrayList<>();
        if (reasonString != null) {
            if (CPU_PATTERN.matcher(reasonString).find()) {
                limitTypes.add(LIMIT_TYPE_CPU);
            }
            if (HEAP_PATTERN.matcher(reasonString).find()) {
                limitTypes.add(LIMIT_TYPE_HEAP);
            }
            if (ELAPSED_TIME_PATTERN.matcher(reasonString).find()) {
                limitTypes.add(LIMIT_TYPE_ELAPSED_TIME);
            }
        }
        if (limitTypes.isEmpty()) {
            limitTypes.add(LIMIT_TYPE_UNKNOWN);
        }
        return limitTypes;
    }

    private static ShardInfo extractShardInfo(String description) {
        if (description == null) {
            return null;
        }
        Matcher matcher = SHARD_ID_PATTERN.matcher(description);
        if (matcher.find()) {
            return new ShardInfo(matcher.group(1), matcher.group(2));
        }
        return null;
    }

    private static class ShardInfo {
        final String indexName;
        final String shardId;

        ShardInfo(String indexName, String shardId) {
            this.indexName = indexName;
            this.shardId = shardId;
        }
    }
}
