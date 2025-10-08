/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Simple metrics interface for streaming search operations.
 * Provides hooks for optional metrics collection behind feature flags.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StreamingMetrics {

    /**
     * Record the start of a streaming aggregation.
     *
     * @param shardId the shard identifier
     * @param mode the streaming mode (unsorted/sorted)
     */
    void recordAggregationStart(String shardId, String mode);

    /**
     * Record a partial emission event.
     *
     * @param shardId the shard identifier
     * @param sequenceId the emission sequence number
     * @param docCount the number of documents in this emission
     * @param totalDocsSeen the total documents processed so far
     */
    void recordPartialEmission(String shardId, long sequenceId, int docCount, int totalDocsSeen);

    /**
     * Record the time to first emission (TTFB - Time To First Byte).
     *
     * @param shardId the shard identifier
     * @param ttfbMillis time to first emission in milliseconds
     */
    void recordTimeToFirstEmission(String shardId, long ttfbMillis);

    /**
     * Record the completion of a streaming aggregation.
     *
     * @param shardId the shard identifier
     * @param totalDocs total documents collected
     * @param totalEmissions total partial emissions sent
     * @param durationMillis total aggregation duration in milliseconds
     */
    void recordAggregationComplete(String shardId, int totalDocs, int totalEmissions, long durationMillis);

    /**
     * Record a coalescing event (when an emission was skipped due to unchanged results).
     *
     * @param shardId the shard identifier
     * @param sequenceId the sequence that would have been emitted
     */
    void recordCoalescedEmission(String shardId, long sequenceId);

    /**
     * Record a circuit breaker check or trigger.
     *
     * @param shardId the shard identifier
     * @param estimatedBytes the estimated memory usage at check time
     * @param triggered whether the circuit breaker was triggered
     */
    void recordCircuitBreakerCheck(String shardId, long estimatedBytes, boolean triggered);

    /**
     * Record a cancellation event.
     *
     * @param shardId the shard identifier
     * @param reason the reason for cancellation
     */
    void recordCancellation(String shardId, String reason);

    /**
     * No-op implementation for when metrics are disabled.
     */
    StreamingMetrics NOOP = new StreamingMetrics() {
        @Override
        public void recordAggregationStart(String shardId, String mode) {}

        @Override
        public void recordPartialEmission(String shardId, long sequenceId, int docCount, int totalDocsSeen) {}

        @Override
        public void recordTimeToFirstEmission(String shardId, long ttfbMillis) {}

        @Override
        public void recordAggregationComplete(String shardId, int totalDocs, int totalEmissions, long durationMillis) {}

        @Override
        public void recordCoalescedEmission(String shardId, long sequenceId) {}

        @Override
        public void recordCircuitBreakerCheck(String shardId, long estimatedBytes, boolean triggered) {}

        @Override
        public void recordCancellation(String shardId, String reason) {}
    };
}
