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

/**
 * Handler for {@link ShuffleProducerInstructionNode} on a scan-side data node.
 *
 * <p>Attaches a partitioning sink to the scan fragment's output so each batch is hash-partitioned
 * into {@code partitionCount} buckets, then ships each bucket to its target worker via
 * {@code AnalyticsShuffleDataAction} (with {@code ShuffleSenderRetry} handling
 * backpressure-reject retries).
 *
 * <p>TODO (M2 follow-up): Wire the partitioning logic itself. Two viable paths:
 * <ul>
 *   <li>JVM-side partitioning using Arrow's hash kernels against the key channels — matching
 *       DataFusion's {@code REPARTITION_RANDOM_STATE} seed so worker-side consumers and probe-side
 *       filters compute consistent partition assignments.</li>
 *   <li>Native-side partitioning via a {@code BatchPartitioner::new_hash_partitioner} callback
 *       through the FFM bridge. More work upfront, cheaper at runtime.</li>
 * </ul>
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
            "Shuffle producer handler invoked: keys={}, partitions={}, targets={}, query={}, stage={}, side={}",
            node.getHashKeyChannels(),
            node.getPartitionCount(),
            node.getTargetWorkerNodeIds().size(),
            node.getQueryId(),
            node.getTargetStageId(),
            node.getSide()
        );
        // TODO (M2 follow-up): attach a PartitionedArrowSink wrapper to the backend's output stream
        // that splits each batch by hash and dispatches AnalyticsShuffleDataRequest (with
        // ShuffleSenderRetry for backpressure handling) to the worker identified by
        // targetWorkerNodeIds[partitionIndex].
        return backendContext;
    }
}
