/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Objects;
import java.util.Optional;

/**
 * Class containing metrics (counters/latency) specific to ClusterManager.
 *
 * @opensearch.internal
 */
public final class ClusterManagerMetrics {

    private static final String LATENCY_METRIC_UNIT_MS = "ms";
    private static final String COUNTER_METRICS_UNIT = "1";

    public final Histogram clusterStateAppliersHistogram;
    public final Histogram clusterStateListenersHistogram;
    public final Histogram rerouteHistogram;
    public final Histogram clusterStateComputeHistogram;
    public final Histogram clusterStatePublishHistogram;

    public final Counter leaderCheckFailureCounter;
    public final Counter followerChecksFailureCounter;
    public final Counter asyncFetchFailureCounter;
    public final Counter asyncFetchTotalCounter;

    public ClusterManagerMetrics(MetricsRegistry metricsRegistry) {
        clusterStateAppliersHistogram = metricsRegistry.createHistogram(
            "cluster.state.appliers.latency",
            "Histogram for tracking the latency of cluster state appliers",
            LATENCY_METRIC_UNIT_MS
        );
        clusterStateListenersHistogram = metricsRegistry.createHistogram(
            "cluster.state.listeners.latency",
            "Histogram for tracking the latency of cluster state listeners",
            LATENCY_METRIC_UNIT_MS
        );
        rerouteHistogram = metricsRegistry.createHistogram(
            "allocation.reroute.latency",
            "Histogram for recording latency of shard re-routing",
            LATENCY_METRIC_UNIT_MS
        );
        clusterStateComputeHistogram = metricsRegistry.createHistogram(
            "cluster.state.new.compute.latency",
            "Histogram for recording time taken to compute new cluster state",
            LATENCY_METRIC_UNIT_MS
        );
        clusterStatePublishHistogram = metricsRegistry.createHistogram(
            "cluster.state.publish.success.latency",
            "Histogram for recording time taken to publish a new cluster state",
            LATENCY_METRIC_UNIT_MS
        );
        followerChecksFailureCounter = metricsRegistry.createCounter(
            "followers.checker.failure.count",
            "Counter for number of failed follower checks",
            COUNTER_METRICS_UNIT
        );
        leaderCheckFailureCounter = metricsRegistry.createCounter(
            "leader.checker.failure.count",
            "Counter for number of failed leader checks",
            COUNTER_METRICS_UNIT
        );
        asyncFetchFailureCounter = metricsRegistry.createCounter(
            "async.fetch.failure.count",
            "Counter for number of failed async fetches",
            COUNTER_METRICS_UNIT
        );
        asyncFetchTotalCounter = metricsRegistry.createCounter(
            "async.fetch.total.count",
            "Counter for total number of async fetches",
            COUNTER_METRICS_UNIT
        );

    }

    public void recordLatency(Histogram histogram, Double value) {
        histogram.record(value);
    }

    public void recordLatency(Histogram histogram, Double value, Optional<Tags> tags) {
        if (Objects.isNull(tags) || tags.isEmpty()) {
            histogram.record(value);
            return;
        }
        histogram.record(value, tags.get());
    }

    public void incrementCounter(Counter counter, Double value) {
        incrementCounter(counter, value, Optional.empty());
    }

    public void incrementCounter(Counter counter, Double value, Optional<Tags> tags) {
        if (Objects.isNull(tags) || tags.isEmpty()) {
            counter.add(value);
            return;
        }
        counter.add(value, tags.get());
    }
}
