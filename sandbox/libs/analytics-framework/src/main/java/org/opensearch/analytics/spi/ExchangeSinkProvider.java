/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Factory for creating a coordinator-side {@link ExchangeSink}.
 *
 * <p>A backend that can accept Arrow Record Batches from data nodes and run
 * coordinator-side computation over them (final aggregate, sort, etc.) implements
 * this interface.
 *
 * <p>Returned by {@link AnalyticsSearchBackendPlugin#getExchangeSinkProvider()}.
 * A {@code null} return means the backend cannot act as a coordinator-side executor.
 *
 * @opensearch.internal
 */
public interface ExchangeSinkProvider {

    /**
     * Creates a sink for coordinator-side execution. The backend implementation
     * uses {@link ExchangeSinkContext#fragmentBytes()} as the serialized plan
     * (produced by {@link FragmentConvertor#convertFinalAggFragment}) and
     * writes its reduced output into {@link ExchangeSinkContext#downstream()}.
     *
     * @param context core-provided context carrying plan bytes, allocator, child inputs, and downstream sink
     * @param backendContext backend-opaque state produced by instruction handlers (e.g.
     *        {@code FinalAggregateInstructionHandler}), or {@code null} when no handler ran
     */
    ExchangeSink createSink(ExchangeSinkContext context, BackendExecutionContext backendContext);

    /**
     * Creates a partitioned sink for hash-shuffle producers. The sink consumes the data-node's
     * local scan output and must hash-partition each batch by {@code hashKeyChannels} into
     * {@code partitionCount} buckets, shipping each bucket to its assigned worker via the
     * framework's shuffle transport.
     *
     * <p>Default impl throws {@link UnsupportedOperationException} — backends without shuffle
     * support do not need to opt in. Backends that support shuffle must override AND declare
     * at least one {@link DataTransferCapability} with {@link DataTransferCapability.Kind#PRODUCER}.
     */
    default ExchangeSink createPartitionedSink(
        java.util.List<Integer> hashKeyChannels,
        int partitionCount,
        ExchangeSinkContext context
    ) {
        throw new UnsupportedOperationException(
            "Backend does not support hash-partitioned shuffle sinks. "
                + "Declare a DataTransferCapability(PRODUCER, ...) and override createPartitionedSink."
        );
    }
}
