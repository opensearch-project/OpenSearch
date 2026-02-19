/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexingBackpressureRejectionReason.OperationType;
import org.opensearch.index.IndexingBackpressureRejectionReason.RejectionReason;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.EnumMap;
import java.util.Map;

/**
 * Metrics for indexing backpressure (rejections). Uses the telemetry MetricsRegistry
 * so plugins like the Prometheus exporter expose these metrics automatically.
 *
 * @opensearch.internal
 */
public final class IndexingPressureMetrics {

    private static final String COUNTER_UNIT = "1";
    private static final String BYTES_UNIT = "bytes";

    static final String OPERATION_TYPE_TAG = "operation_type";
    static final String REJECTION_REASON_TAG = "rejection_reason";
    static final String INDEX_TAG = "index";
    static final String SHARD_ID_TAG = "shard_id";

    /**
     * Pre-built Tags for each (OperationType, RejectionReason) pair.
     * Reused directly when shardId is null to avoid per-call allocation.
     */
    private static final Map<OperationType, Map<RejectionReason, Tags>> BASE_TAGS;

    static {
        BASE_TAGS = new EnumMap<>(OperationType.class);
        for (OperationType op : OperationType.values()) {
            Map<RejectionReason, Tags> reasonMap = new EnumMap<>(RejectionReason.class);
            for (RejectionReason reason : RejectionReason.values()) {
                reasonMap.put(
                    reason,
                    Tags.create().addTag(OPERATION_TYPE_TAG, op.getName()).addTag(REJECTION_REASON_TAG, reason.getName())
                );
            }
            BASE_TAGS.put(op, reasonMap);
        }
    }

    private final Counter rejectionCountCounter;
    private final Counter rejectedBytesCounter;

    public IndexingPressureMetrics(MetricsRegistry metricsRegistry) {
        this.rejectionCountCounter = metricsRegistry.createCounter(
            "indexing.backpressure.rejection.count",
            "Number of indexing requests rejected due to memory backpressure",
            COUNTER_UNIT
        );
        this.rejectedBytesCounter = metricsRegistry.createCounter(
            "indexing.backpressure.rejected.bytes",
            "Bytes in indexing requests rejected due to memory backpressure",
            BYTES_UNIT
        );
    }

    /**
     * Record a rejection event. Called when an indexing operation is rejected.
     *
     * @param shardId       shard where rejection occurred (may be null for node-level only)
     * @param operationType the type of operation that was rejected
     * @param reason        the reason for rejection
     * @param bytes         bytes that were rejected
     */
    public void recordRejection(ShardId shardId, OperationType operationType, RejectionReason reason, long bytes) {
        Tags tags;
        if (shardId != null) {
            tags = Tags.create()
                .addTag(OPERATION_TYPE_TAG, operationType.getName())
                .addTag(REJECTION_REASON_TAG, reason.getName())
                .addTag(INDEX_TAG, shardId.getIndexName())
                .addTag(SHARD_ID_TAG, String.valueOf(shardId.id()));
        } else {
            tags = BASE_TAGS.get(operationType).get(reason);
        }
        rejectionCountCounter.add(1.0, tags);
        rejectedBytesCounter.add((double) bytes, tags);
    }
}
