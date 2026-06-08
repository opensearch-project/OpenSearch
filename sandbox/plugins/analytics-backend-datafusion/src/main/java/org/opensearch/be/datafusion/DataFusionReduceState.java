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
 * Backend-specific execution context carrying a prepared final-aggregate plan: the
 * local session, runtime handle, per-input sender lanes (one array per child input,
 * each lane = one {@code StreamingTable} partition), and the per-input schemas the
 * native session settled on. Produced by {@link FinalAggregateInstructionHandler},
 * consumed by {@link DatafusionReduceSink}.
 *
 * @opensearch.internal
 */
public record DataFusionReduceState(DatafusionLocalSession session, NativeRuntimeHandle runtimeHandle, List<
    DatafusionPartitionSender[]> senders, List<Schema> inputSchemas) implements BackendExecutionContext {

    /** Returns the parallel sender lanes for input position {@code idx} in {@link #senders}. */
    public DatafusionPartitionSender[] sendersForInput(int idx) {
        return senders.get(idx);
    }

    @Override
    public void close() throws IOException {
        // Close all sender lanes for every input first, then session.
        for (DatafusionPartitionSender[] lanes : senders) {
            for (DatafusionPartitionSender sender : lanes) {
                try {
                    sender.close();
                } catch (Exception ignored) {}
            }
        }
        session.close();
    }
}
