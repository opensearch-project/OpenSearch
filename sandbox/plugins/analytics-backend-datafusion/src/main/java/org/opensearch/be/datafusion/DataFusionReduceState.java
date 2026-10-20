/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.spi.BackendExecutionContext;

import java.io.IOException;
import java.util.List;

/**
 * Backend-specific execution context for the coordinator-reduce path when a final-aggregate
 * plan has been prepared. Carries the local session (with the prepared plan stored on the
 * Rust side), the runtime handle, the partition senders used to feed Arrow batches into the
 * streaming input partitions, and the schemas the native session settled on for each input
 * (parallel to {@code senders}; same order as {@code ctx.childInputs()}).
 *
 * <p>Produced by {@link FinalAggregateInstructionHandler} and consumed by
 * {@link DatafusionReduceSink} via the {@link org.opensearch.analytics.spi.ExchangeSinkProvider}
 * contract.
 *
 * @opensearch.internal
 */
public record DataFusionReduceState(DatafusionLocalSession session, NativeRuntimeHandle runtimeHandle, List<
    DatafusionPartitionSender> senders, List<Schema> inputSchemas) implements BackendExecutionContext {

    @Override
    public void close() throws IOException {
        // Close senders first, then session.
        for (DatafusionPartitionSender sender : senders) {
            try {
                sender.close();
            } catch (Exception ignored) {}
        }
        session.close();
    }
}
