/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.metrics;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Metrics for tracking tier migration operations including successful migrations,
 * rejections, and latency.
 *
 * @opensearch.experimental
 */
public final class TierActionMetrics {

    private static final String LATENCY_METRIC_UNIT_MS = "ms";
    private static final String COUNTER_METRICS_UNIT = "1";

    /** Tag key for node ID. */
    public static final String NODE_ID = "node_id";
    /** Tag key for index name. */
    public static final String INDEX_NAME = "index_name";
    /** Tag key for tier type. */
    public static final String TIER_TYPE = "tier_type";
    /** Tag key for rejection reason. */
    public static final String REJECTION_REASON = "rejection_reason";

    /** Counter for successful tier migrations. */
    public final Counter successfulMigrations;
    /** Counter for rejected tier migrations. */
    public final Counter rejectionReason;
    /** Histogram for tracking end-to-end migration time. */
    public final Histogram migrationLatency;

    /**
     * Creates a new TierActionMetrics instance.
     * @param metricsRegistry the metrics registry to create counters and histograms
     */
    public TierActionMetrics(MetricsRegistry metricsRegistry) {
        successfulMigrations = metricsRegistry.createCounter(
            "migration_successful",
            "Counter for successful tier migrations",
            COUNTER_METRICS_UNIT
        );

        rejectionReason = metricsRegistry.createCounter(
            "migration_rejection_reason",
            "Counter for rejected tier migrations with their reasons",
            COUNTER_METRICS_UNIT
        );

        migrationLatency = metricsRegistry.createHistogram(
            "migration_latency",
            "Histogram for tracking end-to-end migration time",
            LATENCY_METRIC_UNIT_MS
        );
    }

    /**
     * Records migration latency.
     * @param value the latency value in milliseconds
     * @param nodeId the node ID
     * @param indexName the index name
     * @param tierType the tier type
     */
    public void recordMigrationLatency(Double value, String nodeId, String indexName, String tierType) {
        Tags tags = createBaseTags(nodeId, indexName, tierType);
        migrationLatency.record(value, tags);
    }

    /**
     * Records a successful migration.
     * @param nodeId the node ID
     * @param indexName the index name
     * @param tierType the tier type
     */
    public void recordSuccessfulMigration(String nodeId, String indexName, String tierType) {
        Tags tags = createBaseTags(nodeId, indexName, tierType);
        successfulMigrations.add(1.0, tags);
    }

    /**
     * Records a rejected migration.
     * @param nodeId the node ID
     * @param indexName the index name
     * @param tierType the tier type
     * @param reason the rejection reason
     */
    public void recordRejectedMigration(String nodeId, String indexName, String tierType, String reason) {
        Tags tags = createBaseTags(nodeId, indexName, tierType).addTag(REJECTION_REASON, reason);
        rejectionReason.add(1.0, tags);
    }

    private Tags createBaseTags(String nodeId, String indexName, String tierType) {
        return Tags.create().addTag(NODE_ID, nodeId).addTag(INDEX_NAME, indexName).addTag(TIER_TYPE, tierType);
    }
}
