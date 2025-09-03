/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.autoforcemerge;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Objects;
import java.util.Optional;

/**
 * Class containing metrics (counters/latency) specific to Auto Force merges.
 *
 * @opensearch.internal
 */
public class AutoForceMergeMetrics {

    private static final String LATENCY_METRIC_UNIT_MS = "ms";
    private static final String COUNTER_METRICS_UNIT = "1";
    private static final String SIZE_METRIC_UNIT = "bytes";

    public static final String NODE_ID = "NodeId";
    public static final String SHARD_ID = "ShardId";
    public static final String INDEX_NAME = "IndexName";

    public final Histogram totalSchedulerExecutionTime;
    public final Counter totalMergesTriggered;
    public final Counter totalMergesSkipped;
    public final Counter skipsFromNodeValidator;
    public final Counter totalMergesFailed;

    // Shard specific metrics
    public final Histogram shardForceMergeLatency;
    public final Counter shardSize;
    public final Counter segmentCount;

    public AutoForceMergeMetrics(MetricsRegistry metricsRegistry) {
        totalSchedulerExecutionTime = metricsRegistry.createHistogram(
            "auto_force_merge.scheduler.execution_time",
            "Histogram for tracking total scheduler execution time.",
            LATENCY_METRIC_UNIT_MS
        );

        totalMergesTriggered = metricsRegistry.createCounter(
            "auto_force_merge.merges.triggered",
            "Counter for number of force merges triggered.",
            COUNTER_METRICS_UNIT
        );

        totalMergesSkipped = metricsRegistry.createCounter(
            "auto_force_merge.merges.skipped.total",
            "Counter for number of force merges skipped.",
            COUNTER_METRICS_UNIT
        );

        skipsFromNodeValidator = metricsRegistry.createCounter(
            "auto_force_merge.merges.skipped.node_validator",
            "Counter for number of force merges skipped due to Node Validator.",
            COUNTER_METRICS_UNIT
        );

        totalMergesFailed = metricsRegistry.createCounter(
            "auto_force_merge.merges.failed",
            "Counter for number of force merges failed.",
            COUNTER_METRICS_UNIT
        );

        shardForceMergeLatency = metricsRegistry.createHistogram(
            "auto_force_merge.shard.merge_latency",
            "Histogram for tracking time taken by force merge on individual shards.",
            LATENCY_METRIC_UNIT_MS
        );

        shardSize = metricsRegistry.createCounter(
            "auto_force_merge.shard.size",
            "Counter for tracking shard size during force merge operations.",
            SIZE_METRIC_UNIT
        );

        segmentCount = metricsRegistry.createCounter(
            "auto_force_merge.shard.segment_count",
            "Counter for tracking segment count during force merge operations.",
            COUNTER_METRICS_UNIT
        );
    }

    public void recordInHistogram(Histogram histogram, Double value, Optional<Tags> tags) {
        if (Objects.isNull(tags) || tags.isEmpty()) {
            histogram.record(value);
            return;
        }
        histogram.record(value, tags.get());
    }

    public void incrementCounter(Counter counter, Double value, Optional<Tags> tags) {
        if (Objects.isNull(tags) || tags.isEmpty()) {
            counter.add(value);
            return;
        }
        counter.add(value, tags.get());
    }

    public Optional<Tags> getTags(String nodeId, String shardId) {
        Optional<Tags> tags = Optional.of(Tags.create());
        if (nodeId != null) {
            tags.get().addTag(NODE_ID, nodeId);
        }
        if (shardId != null) {
            tags.get().addTag(SHARD_ID, shardId);
        }
        return tags;
    }
}
