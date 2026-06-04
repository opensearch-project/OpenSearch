/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;

import java.util.List;

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
     * (produced by {@code FragmentConvertor#convertFragment}) and writes its
     * reduced output into {@link ExchangeSinkContext#downstream()}.
     *
     * <p>The schema of each child's batches is learned at the backend boundary
     * (not pre-declared on {@link ExchangeSinkContext.ChildInput}) — the backend
     * derives it when it registers the child input on its native session, since
     * the producer-side plan bytes already encode the producer schema.
     *
     * @param context core-provided context carrying plan bytes, allocator, child inputs, and downstream sink
     * @param backendContext backend-opaque state produced by instruction handlers (e.g.
     *        {@code FinalAggregateInstructionHandler}), or {@code null} when no handler ran
     */
    ExchangeSink createSink(ExchangeSinkContext context, BackendExecutionContext backendContext);

    /**
     * Creates a partitioned sink for hash-shuffle producers. The sink consumes the data-node's
     * local scan output and must hash-partition each batch by {@code hashKeyChannels} into
     * {@code partitionCount} buckets, shipping each bucket to the worker at
     * {@code targetWorkerNodeIds[partitionIndex]} via the framework-supplied {@link ShuffleSender}.
     *
     * <p>Backends never touch {@code Client} or the transport action types directly — the
     * framework builds the sender, stamps it with {@code (queryId, targetStageId, side)}, and
     * applies backpressure retry via {@code ShuffleSenderRetry} under the hood. Backends own
     * the partitioning algorithm only; the framework owns the wire.
     *
     * <p>Default impl throws {@link UnsupportedOperationException} — backends without shuffle
     * support do not need to opt in. Backends that support shuffle must override AND declare
     * at least one {@link DataTransferCapability} with {@link DataTransferCapability.Kind#PRODUCER}.
     *
     * @param hashKeyChannels     0-indexed input channels to hash on
     * @param partitionCount      total number of consumer partitions; the sink must produce
     *                            partition indices in {@code [0, partitionCount)}
     * @param targetWorkerNodeIds {@code targetWorkerNodeIds.size() == partitionCount}; the sink
     *                            uses {@code targetWorkerNodeIds.get(p)} as the destination for
     *                            partition {@code p}
     * @param sender              framework-provided transport wrapper; the sink calls
     *                            {@link ShuffleSender#send} once per partition per batch and
     *                            once per partition at close with {@code isLast=true}
     * @param context             the standard sink context (allocator, child inputs, etc.)
     */
    default ExchangeSink createPartitionedSink(
        List<Integer> hashKeyChannels,
        int partitionCount,
        List<String> targetWorkerNodeIds,
        ShuffleSender sender,
        ExchangeSinkContext context
    ) {
        throw new UnsupportedOperationException(
            "Backend does not support hash-partitioned shuffle sinks. "
                + "Declare a DataTransferCapability(PRODUCER, ...) and override createPartitionedSink."
        );
    }

    /**
     * Creates a capture sink used by the M1 broadcast-join coordinator to buffer the build
     * stage's output into Arrow IPC bytes. The returned sink must expose a
     * {@code CompletableFuture<byte[]> ipcBytesFuture()} method that completes when
     * {@link ExchangeSink#close()} finishes, delivering the full build-side payload. The
     * dispatcher accesses that method reflectively to avoid a compile-time dependency from
     * analytics-engine on any specific backend's sink class.
     *
     * <p>The {@code buildRowType} is the build stage's Calcite row type. The backend uses it
     * as the fallback schema when the build stage emits zero batches — without it, an
     * all-empty build side would register a zero-column memtable on the probe side and break
     * the join's NamedScan binding. Each backend converts {@code RelDataType} to its Arrow
     * representation in the way it already does for normal scan output.
     *
     * <p>The {@code maxBytes} cap is enforced at runtime: if the accumulated build-side buffer
     * size exceeds it, the sink fails its {@code ipcBytesFuture()} so the dispatcher routes the
     * failure through the query's terminal listener. Pass {@code Long.MAX_VALUE} to disable.
     *
     * <p>Default impl throws {@link UnsupportedOperationException} — backends that don't
     * participate as the coordinator-side broadcast build collector do not need to opt in.
     */
    default ExchangeSink createBroadcastCaptureSink(
        BufferAllocator allocator,
        org.apache.calcite.rel.type.RelDataType buildRowType,
        long maxBytes
    ) {
        throw new UnsupportedOperationException(
            "Backend does not support broadcast-capture sinks. Override createBroadcastCaptureSink "
                + "to return an ExchangeSink whose close() completes ipcBytesFuture() with the build-side Arrow IPC bytes."
        );
    }
}
