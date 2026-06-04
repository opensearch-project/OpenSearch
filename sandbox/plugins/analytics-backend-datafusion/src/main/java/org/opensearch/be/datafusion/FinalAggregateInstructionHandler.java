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
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.FinalAggregateInstructionNode;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles FinalAggregate instruction for coordinator-reduce stages: creates a local session,
 * registers streaming input partitions from child stages, and prepares the final-aggregate
 * physical plan.
 *
 * <p>Returns a {@link DataFusionReduceState} carrying the session, runtime, and senders so
 * the {@link DatafusionReduceSink} can later execute the prepared plan and feed batches.
 */
public class FinalAggregateInstructionHandler implements FragmentInstructionHandler<FinalAggregateInstructionNode> {

    private final NativeRuntimeHandle runtimeHandle;

    FinalAggregateInstructionHandler(NativeRuntimeHandle runtimeHandle) {
        this.runtimeHandle = runtimeHandle;
    }

    @Override
    public BackendExecutionContext apply(
        FinalAggregateInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ExchangeSinkContext ctx = (ExchangeSinkContext) commonContext;

        DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
        List<DatafusionPartitionSender> senders = new ArrayList<>(ctx.childInputs().size());
        List<Schema> inputSchemas = new ArrayList<>(ctx.childInputs().size());
        try {
            for (ExchangeSinkContext.ChildInput child : ctx.childInputs()) {
                String inputId = "input-" + child.childStageId();
                NativeBridge.RegisteredInput registered = NativeBridge.registerPartitionStream(
                    session.getPointer(),
                    inputId,
                    child.producerPlanBytes()
                );
                senders.add(new DatafusionPartitionSender(registered.pointer()));
                inputSchemas.add(ArrowSchemaIpc.fromBytes(registered.schemaIpc()));
            }
            NativeBridge.prepareFinalPlan(session.getPointer(), ctx.fragmentBytes());
        } catch (RuntimeException e) {
            for (DatafusionPartitionSender sender : senders) {
                try {
                    sender.close();
                } catch (Exception ignored) {}
            }
            session.close();
            throw e;
        }
        return new DataFusionReduceState(session, runtimeHandle, senders, inputSchemas);
    }
}
