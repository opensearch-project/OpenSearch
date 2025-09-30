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

    public static final String NODE_ID = "node_id";
    public static final String SHARD_ID = "shard_id";

    public final Histogram schedulerExecutionTime;
    public final Counter mergesTriggered;
    public final Counter skipsFromConfigValidator;
    public final Counter skipsFromNodeValidator;
    public final Counter mergesFailed;

    // Shard specific metrics
    public final Histogram shardMergeLatency;
    public final Counter shardSize;
    public final Counter segmentCount;

    public AutoForceMergeMetrics(MetricsRegistry metricsRegistry) {
        schedulerExecutionTime = metricsRegistry.createHistogram(
            "auto_force_merge.scheduler.execution_time",
            "Histogram for tracking total scheduler execution time.",
            LATENCY_METRIC_UNIT_MS
        );

        mergesTriggered = metricsRegistry.createCounter(
            "auto_force_merge.merges.triggered",
            "Counter for number of force merges triggered.",
            COUNTER_METRICS_UNIT
        );

        skipsFromConfigValidator = metricsRegistry.createCounter(
            "auto_force_merge.merges.skipped.config_validator",
            "Counter for number of force merges skipped due to Configuration Validator.",
            COUNTER_METRICS_UNIT
        );

        skipsFromNodeValidator = metricsRegistry.createCounter(
            "auto_force_merge.merges.skipped.node_validator",
            "Counter for number of force merges skipped due to Node Validator.",
            COUNTER_METRICS_UNIT
        );

        mergesFailed = metricsRegistry.createCounter(
            "auto_force_merge.merges.failed",
            "Counter for number of force merges failed.",
            COUNTER_METRICS_UNIT
        );

        shardMergeLatency = metricsRegistry.createHistogram(
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

    public Optional<Tags> getTags(Optional<String> nodeId, Optional<String> shardId) {
        Tags tags = Tags.create();

        if (shardId.isPresent()) {
            tags.addTag(SHARD_ID, shardId.get());
        } else if (nodeId.isPresent()) {
            tags.addTag(NODE_ID, nodeId.get());
        }

        return Optional.of(tags);
    }
}
