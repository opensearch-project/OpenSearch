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
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;

/**
 * Handler for {@link ShuffleScanInstructionNode} on a hash-shuffle worker.
 *
 * <p>Bridges the node-local {@code ShuffleBufferManager.ShuffleBuffer} to a DataFusion
 * {@code StreamingTableExec} whose {@code PartitionStream} reads Arrow IPC batches from the
 * buffer as they arrive. The handler waits for {@code expectedSenders} completion markers on the
 * assigned side before draining the buffer's accumulated batches.
 *
 * <p>TODO (M2 follow-up): Wire the native registration step. The JVM-side bridge deserializes
 * each received IPC bytes chunk into a {@code VectorSchemaRoot}, exports it via Arrow C Data
 * Interface, and calls {@code NativeBridge.registerPartitionStream(sessionCtxHandle, namedInputId,
 * arrayAddr, schemaAddr)}. This shell validates handler registration — the end-to-end wiring
 * between the buffer and the native session happens alongside the phased scheduler dispatch.
 */
public class ShuffleScanHandler implements FragmentInstructionHandler<ShuffleScanInstructionNode> {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleScanHandler.class);

    @Override
    public BackendExecutionContext apply(
        ShuffleScanInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        LOGGER.debug(
            "Shuffle scan handler invoked: namedInputId={}, partitionIndex={}, expectedSenders={}",
            node.getNamedInputId(),
            node.getShufflePartitionIndex(),
            node.getExpectedSenders()
        );
        // TODO (M2 follow-up): resolve the per-node ShuffleBufferManager, await buffer readiness on
        // this partition, deserialize incoming IPC bytes, export via Arrow C Data, and register with
        // the native session as a PartitionStream bound to the NamedScan this namedInputId targets.
        return backendContext;
    }
}
