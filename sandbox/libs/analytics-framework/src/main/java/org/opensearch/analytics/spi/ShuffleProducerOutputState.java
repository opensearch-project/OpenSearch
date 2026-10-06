/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.List;

/**
 * Backend-execution context flag that {@link FragmentInstructionHandler} produces from a
 * {@link ShuffleProducerInstructionNode} to signal the framework to route this fragment's
 * output through {@link ExchangeSinkProvider#createPartitionedSink} instead of the standard
 * streaming response handler.
 *
 * <p>The framework checks this on the way out of the instruction-handler chain. When present,
 * it builds a {@link ShuffleSender} stamped with {@code (queryId, targetStageId, side)} and
 * invokes {@code createPartitionedSink} on the backend, then drives the engine's output
 * iterator into that sink. When absent, output goes back to the originating coordinator via
 * the response handler as usual.
 *
 * <p>Carries no native or closeable resources — it's a pure-data carrier. Backends that need
 * to attach session state to the context should subclass and override {@link #close()}.
 *
 * @opensearch.internal
 */
public class ShuffleProducerOutputState implements BackendExecutionContext {

    private final List<Integer> hashKeyChannels;
    private final int partitionCount;
    private final List<String> targetWorkerNodeIds;
    private final String queryId;
    private final int targetStageId;
    private final String side;
    private final BackendExecutionContext delegate;

    /**
     * @param hashKeyChannels     0-indexed channels to hash-partition on
     * @param partitionCount      number of consumer partitions
     * @param targetWorkerNodeIds {@code size() == partitionCount}; destination per partition
     * @param queryId             stamps every shuffle send so the consumer's buffer routes correctly
     * @param targetStageId       the consumer stage id (the buffer key)
     * @param side                {@code "left"} or {@code "right"}; tells the consumer's
     *                            {@code awaitReady} which half of the join input is arriving
     * @param delegate            optional upstream backend context to compose with (e.g. session
     *                            state from a previous handler). May be {@code null}. The
     *                            framework forwards it to engine construction; this carrier is
     *                            consumed and not passed along.
     */
    public ShuffleProducerOutputState(
        List<Integer> hashKeyChannels,
        int partitionCount,
        List<String> targetWorkerNodeIds,
        String queryId,
        int targetStageId,
        String side,
        BackendExecutionContext delegate
    ) {
        this.hashKeyChannels = List.copyOf(hashKeyChannels);
        this.partitionCount = partitionCount;
        this.targetWorkerNodeIds = List.copyOf(targetWorkerNodeIds);
        this.queryId = queryId;
        this.targetStageId = targetStageId;
        this.side = side;
        this.delegate = delegate;
    }

    public List<Integer> getHashKeyChannels() {
        return hashKeyChannels;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public List<String> getTargetWorkerNodeIds() {
        return targetWorkerNodeIds;
    }

    public String getQueryId() {
        return queryId;
    }

    public int getTargetStageId() {
        return targetStageId;
    }

    public String getSide() {
        return side;
    }

    /** The upstream backend context this carrier wraps; the framework hands this to the engine
     *  factory in place of the carrier itself. */
    public BackendExecutionContext getDelegate() {
        return delegate;
    }

    @Override
    public void close() throws java.io.IOException {
        if (delegate != null) {
            delegate.close();
        }
    }
}
