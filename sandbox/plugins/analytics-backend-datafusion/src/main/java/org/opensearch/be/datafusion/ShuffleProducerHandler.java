/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShuffleProducerInstructionNode;
import org.opensearch.analytics.spi.ShuffleProducerOutputState;

/**
 * Handler for {@link ShuffleProducerInstructionNode} on a scan-side data node.
 *
 * <p>Records the producer's partitioning + routing parameters into a
 * {@link ShuffleProducerOutputState} that the framework picks up after the instruction-handler
 * chain. The framework then routes this fragment's engine output through
 * {@code ExchangeSinkProvider.createPartitionedSink} instead of the normal response handler.
 *
 * <p>The handler itself does not partition — partitioning is the backend sink's responsibility,
 * because the hash function must match the consumer-side join's hasher. Wiring partitioning at
 * the framework layer would lock in DataFusion's hash and break a future Velox/Lucene producer.
 *
 * <p>Composes with a previous handler's backend context: if {@code backendContext} is non-null
 * (e.g. session state set up by a prior handler), it is wrapped in the carrier so the engine
 * factory still receives it.
 */
public class ShuffleProducerHandler implements FragmentInstructionHandler<ShuffleProducerInstructionNode> {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleProducerHandler.class);

    @Override
    public BackendExecutionContext apply(
        ShuffleProducerInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        LOGGER.debug(
            "Shuffle producer handler: keys={}, partitions={}, targets={}, query={}, stage={}, side={}",
            node.getHashKeyChannels(),
            node.getPartitionCount(),
            node.getTargetWorkerNodeIds().size(),
            node.getQueryId(),
            node.getTargetStageId(),
            node.getSide()
        );
        if (node.getTargetWorkerNodeIds().size() != node.getPartitionCount()) {
            throw new IllegalStateException(
                "Shuffle producer instruction has partitionCount="
                    + node.getPartitionCount()
                    + " but "
                    + node.getTargetWorkerNodeIds().size()
                    + " target node ids; the planner must emit one target per partition"
            );
        }
        return new ShuffleProducerOutputState(
            node.getHashKeyChannels(),
            node.getPartitionCount(),
            node.getTargetWorkerNodeIds(),
            node.getQueryId(),
            node.getTargetStageId(),
            node.getSide(),
            backendContext
        );
    }
}
