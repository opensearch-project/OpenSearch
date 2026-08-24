/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.ShuffleProducerOutputState;
import org.opensearch.analytics.spi.ShuffleSender;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.List;

/**
 * Builds the hash-shuffle PRODUCER sink for a fragment whose instruction chain produced a
 * {@link ShuffleProducerOutputState}: a framework {@link ShuffleSender} plus the backend's partitioned sink.
 *
 * <p>Extracted so every producer locality shares ONE construction path. Shuffle production is
 * instruction-driven, not stage-typed — a shard fragment, a worker fragment, and (with step 5) a coordinator
 * reduce stage can all ship partitions — and having each locality assemble its own sender/sink is how the
 * three drift apart on retry policy, side labelling, or context shape.
 *
 * <p>Deps are the transport {@link Client}, {@link ThreadPool} and {@link ClusterService} the sender needs to
 * route partitions to worker nodes. They are plumbed once at plugin startup; a producer fragment reaching a
 * builder with any of them missing is a startup misconfiguration, so {@link #build} fails with a typed error
 * rather than a null dereference.
 *
 * @opensearch.internal
 */
public final class ShuffleProducerSinkBuilder {

    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    public ShuffleProducerSinkBuilder(Client client, ThreadPool threadPool, ClusterService clusterService) {
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    /** True when this builder has every dep the {@link ShuffleSender} needs. */
    public boolean isReady() {
        return client != null && threadPool != null && clusterService != null;
    }

    /**
     * The partitioned sink {@code producerState} describes.
     *
     * <p>The producer's {@link ExchangeSinkContext} deliberately carries NO child inputs (the producer IS the
     * source) and NO downstream sink — the partitioned sink ships out-of-band through the
     * {@link ShuffleSender} rather than into another in-process sink. {@code fragmentBytes} is empty for the
     * same reason: the partitioning operates on the engine's terminal output, not on a plan.
     *
     * @param taskId    native/parent task id for cancellation attribution; {@code 0} when there is no task
     * @param allocator the fragment's Arrow allocator
     */
    public ExchangeSink build(
        ExchangeSinkProvider provider,
        ShuffleProducerOutputState producerState,
        long taskId,
        BufferAllocator allocator,
        String queryId,
        int stageId
    ) {
        if (!isReady()) {
            throw new IllegalStateException(
                "ShuffleProducerSinkBuilder: shuffle sender deps not plumbed; the transport client, thread pool "
                    + "and cluster service must be supplied at plugin startup before a producer fragment runs"
            );
        }
        ShuffleSender sender = new ShuffleSenderImpl(
            client,
            threadPool,
            clusterService,
            producerState.getQueryId(),
            producerState.getTargetStageId(),
            producerState.getSide()
        );
        ExchangeSinkContext sinkCtx = new ExchangeSinkContext(
            queryId,
            stageId,
            taskId,
            new byte[0],
            allocator,
            List.of(),
            /* downstream */ null
        );
        return provider.createPartitionedSink(
            producerState.getHashKeyChannels(),
            producerState.getPartitionCount(),
            producerState.getTargetWorkerNodeIds(),
            sender,
            sinkCtx
        );
    }
}
