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

/**
 * Migration Metrics to tracks successful migrations, rejections, and latency.
 *
 * @opensearch.internal
 */
public final class TierActionMetrics {

    private static final String LATENCY_METRIC_UNIT_MS = "ms";
    private static final String COUNTER_METRICS_UNIT = "1";

    /** Tag name for node ID. */
    public static final String NODE_ID = "node_id";
    /** Tag name for index name. */
    public static final String INDEX_NAME = "index_name";
    /** Tag name for tier type. */
    public static final String TIER_TYPE = "tier_type";
    /** Tag name for rejection reason. */
    public static final String REJECTION_REASON = "rejection_reason";

    /** Counter for successful migrations. */
    public final Counter successfulMigrations;
    /** Counter for rejected migrations. */
    public final Counter rejectionReason;
    /** Histogram for migration latency. */
    public final Histogram migrationLatency;

    /**
     * Constructs TierActionMetrics and registers metrics.
     * @param metricsRegistry the metrics registry
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
     * @param value the latency value
     * @param nodeId the node ID
     * @param indexName the index name
     * @param tierType the tier type
     */
    public void recordMigrationLatency(Double value, String nodeId, String indexName, String tierType) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Records a successful migration.
     * @param nodeId the node ID
     * @param indexName the index name
     * @param tierType the tier type
     */
    public void recordSuccessfulMigration(String nodeId, String indexName, String tierType) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Records a rejected migration.
     * @param nodeId the node ID
     * @param indexName the index name
     * @param tierType the tier type
     * @param reason the rejection reason
     */
    public void recordRejectedMigration(String nodeId, String indexName, String tierType, String reason) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
