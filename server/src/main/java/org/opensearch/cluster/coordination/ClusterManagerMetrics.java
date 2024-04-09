/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Objects;
import java.util.Optional;

/**
 * Class containing metrics (counters/latency) specific to ClusterManager.
 */
public final class ClusterManagerMetrics {

    private static final String COUNTER_METRICS_UNIT = "1";

    public final Counter leaderCheckFailureCounter;
    public final Counter followerChecksFailureCounter;

    public ClusterManagerMetrics(MetricsRegistry metricsRegistry) {
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
