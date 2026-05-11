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
import org.opensearch.analytics.spi.BroadcastInjectionInstructionNode;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;

/**
 * Handler for {@link BroadcastInjectionInstructionNode} on the probe-side data node.
 *
 * <p>Receives the coordinator-collected build-side Arrow IPC batches via the instruction node
 * and registers them with the DataFusion session as a {@code MemTable} under the instruction's
 * {@code namedInputId}. The probe-side Substrait plan references this named input via
 * {@code NamedScan}, so the hash join's build side resolves to the in-memory broadcast payload
 * rather than a stage-input partition stream.
 *
 * <p>Memtable registration reuses the same FFM / native bridge path that
 * {@link DatafusionMemtableReduceSink} uses for coordinator-reduce collection — the difference
 * is that for broadcast the payload arrives as pre-serialized Arrow IPC bytes, so the handler
 * deserializes first, then exports via Arrow C Data Interface, then registers the resulting
 * {@code MemTable} on the session.
 *
 * <p>TODO: Complete the FFM wiring. This M1 shell validates the instruction node / handler
 * registration path; the actual memtable registration against the native session will be added
 * when the full broadcast dispatch is wired through the scheduler.
 */
public class BroadcastInjectionHandler implements FragmentInstructionHandler<BroadcastInjectionInstructionNode> {

    private static final Logger LOGGER = LogManager.getLogger(BroadcastInjectionHandler.class);

    @Override
    public BackendExecutionContext apply(
        BroadcastInjectionInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        LOGGER.debug(
            "Broadcast injection handler invoked: namedInputId={}, buildSideIndex={}, bytes={}",
            node.getNamedInputId(),
            node.getBuildSideIndex(),
            node.getBroadcastData() == null ? 0 : node.getBroadcastData().length
        );
        // TODO (M1 follow-up): deserialize Arrow IPC → VectorSchemaRoot, export via Arrow C Data
        // Interface, call NativeBridge.registerMemtable(sessionCtxHandle, namedInputId, arrayAddr,
        // schemaAddr). The existing DatafusionMemtableReduceSink shows the write-side pattern;
        // broadcast injection is the read-side symmetric.
        return backendContext;
    }
}
