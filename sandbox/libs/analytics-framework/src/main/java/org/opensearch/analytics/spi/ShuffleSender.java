/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.action.ActionListener;

/**
 * Framework-provided wrapper around the shuffle transport that hash-shuffle producer sinks call
 * to ship a per-partition payload to its target worker. Supplied to
 * {@link ExchangeSinkProvider#createPartitionedSink} so backends never touch
 * {@link org.opensearch.transport.client.Client} or the transport action types directly.
 *
 * <p>The framework's implementation wraps {@code AnalyticsShuffleDataAction} with
 * {@code ShuffleSenderRetry} so backpressure-rejected sends are automatically retried with
 * exponential backoff. Backends only need to call {@link #send} once per partition payload; the
 * framework owns retry, scheduling, and node-id-to-transport routing.
 *
 * <p>Lifecycle: a backend's {@link ExchangeSink} owns one {@code ShuffleSender} per producer
 * stage. The framework constructs the sender when it sees a
 * {@code ShuffleProducerInstructionNode} on a fragment; the sink calls {@link #send} for each
 * partition's batch payload, then {@link #send} again with {@code isLast=true} on every target
 * to release the consumer's per-side latch.
 */
public interface ShuffleSender {

    /**
     * Ship one shuffle payload to the worker at {@code targetWorkerNodeId}. Producer sinks call
     * this once per partition per fed batch (with {@code isLast=false}), and once per partition
     * at sink close (with empty {@code data} and {@code isLast=true}) to release the consumer's
     * {@code awaitReady} latch.
     *
     * @param targetWorkerNodeId destination node id from the producer instruction's
     *                           {@code targetWorkerNodeIds[partitionIndex]}
     * @param partitionIndex     destination partition (the consumer-side bucket key); the
     *                           consumer uses this to route into its per-partition buffer slice
     * @param data               Arrow IPC bytes for one record batch (or {@code null}/empty when
     *                           {@code isLast=true} carries no payload)
     * @param isLast             marks the final transmission for {@code partitionIndex} on this
     *                           {@code (queryId, targetStageId, side)} channel; consumer
     *                           awaitReady fires when every sender has sent its isLast
     * @param listener           receives one notification per send: {@code onResponse} on
     *                           success (including after backpressure retry), {@code onFailure}
     *                           on transport error or retry exhaustion. Always invoked exactly
     *                           once per call.
     */
    void send(String targetWorkerNodeId, int partitionIndex, byte[] data, boolean isLast, ActionListener<Void> listener);
}
