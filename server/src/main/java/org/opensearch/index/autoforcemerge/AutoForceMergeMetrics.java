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

    public static final String NODE_ID = "NodeId";
    public static final String SHARD_ID = "ShardId";

    // Overall metrics
    public final Counter totalMergesCompleted;
    public final Counter totalMergesTriggered;
    public final Counter totalMergesSkipped;
    public final Counter totalMergesFailed;
    public final Counter skipsFromConfigValidator;
    public final Counter skipsFromNodeValidator;

    // Metrics related to recent execution
    public final Counter currentActiveMerges;
    public final Histogram mergesTriggeredInLastExecution;
    public final Histogram totalEligibleShards;
    public final Histogram shardForceMergeLatency;
    public final Histogram autoForceMergeSchedulerLatency;

    // Migration Metrics
    public final Counter segmentCountDuringMigration;
    public final Counter shardSizeDuringMigration;

    public AutoForceMergeMetrics(MetricsRegistry metricsRegistry) {
        autoForceMergeSchedulerLatency = metricsRegistry.createHistogram(
            "auto_force_merge.scheduler.latency",
            "Histogram for tracking the start time of scheduler last iteration.",
            LATENCY_METRIC_UNIT_MS
        );
        totalMergesCompleted = metricsRegistry.createCounter(
            "auto_force_merge.merges.completed",
            "Counter for number of force merges completed successfully.",
            COUNTER_METRICS_UNIT
        );
        totalMergesTriggered = metricsRegistry.createCounter(
            "auto_force_merge.merges.triggered",
            "Counter for number of force merges triggered.",
            COUNTER_METRICS_UNIT
        );
        totalMergesFailed = metricsRegistry.createCounter(
            "auto_force_merge.merges.failed",
            "Counter for number of force merges failed.",
            COUNTER_METRICS_UNIT
        );
        currentActiveMerges = metricsRegistry.createUpDownCounter(
            "auto_force_merge.merges.active",
            "Counter for number of active force merges currently running.",
            COUNTER_METRICS_UNIT
        );
        totalMergesSkipped = metricsRegistry.createCounter(
            "auto_force_merge.merges.skipped.total",
            "Counter for number of force merges skipped.",
            COUNTER_METRICS_UNIT
        );
        skipsFromConfigValidator = metricsRegistry.createCounter(
            "auto_force_merge.merges.skipped.config",
            "Counter for number of force merges skipped due to Config Validator.",
            COUNTER_METRICS_UNIT
        );
        skipsFromNodeValidator = metricsRegistry.createCounter(
            "auto_force_merge.merges.skipped.node",
            "Counter for number of force merges skipped due to Node Validator.",
            COUNTER_METRICS_UNIT
        );
        totalEligibleShards = metricsRegistry.createHistogram(
            "auto_force_merge.shard.total",
            "Counter for number of shards eligible for force merges.",
            COUNTER_METRICS_UNIT
        );
        shardForceMergeLatency = metricsRegistry.createHistogram(
            "auto_force_merge.shard.latency",
            "Histogram for tracking the end time of scheduler last iteration.",
            LATENCY_METRIC_UNIT_MS
        );
        mergesTriggeredInLastExecution = metricsRegistry.createHistogram(
            "auto_force_merge.recent.merges.triggered",
            "Counter for number of force merges triggered in last scheduler execution.",
            COUNTER_METRICS_UNIT
        );
        shardSizeDuringMigration = metricsRegistry.createCounter(
            "auto_force_merge.shard.size",
            "Counter for index size during migration.",
            COUNTER_METRICS_UNIT
        );
        segmentCountDuringMigration = metricsRegistry.createCounter(
            "auto_force_merge.shard.segments",
            "Counter for number of segment count during migration.",
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
