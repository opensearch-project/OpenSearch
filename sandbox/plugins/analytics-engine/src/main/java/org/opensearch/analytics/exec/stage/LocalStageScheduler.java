/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds executions for {@link StageExecutionType#COORDINATOR_REDUCE} stages —
 * those that run at the coordinator with a backend-provided {@link ExchangeSink}.
 * Creates the sink via {@link Stage#getExchangeSinkProvider()} using an
 * {@link ExchangeSinkContext} carrying the plan bytes, allocator, per-child
 * input descriptors (one per child stage, each with its stage id + Arrow
 * schema), and the downstream sink. Hands the resulting sink to
 * {@link LocalStageExecution}.
 *
 * <p>Multi-child stages (Union, future Join) are routed via
 * {@link LocalStageExecution#inputSink(int)}, which returns a per-child
 * wrapper that the backend sink uses to register a distinct input partition
 * per child stage id.
 *
 * @opensearch.internal
 */
final class LocalStageScheduler implements StageScheduler {

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        ExchangeSinkProvider provider = stage.getExchangeSinkProvider();
        ExchangeSinkContext context = new ExchangeSinkContext(
            config.queryId(),
            stage.getStageId(),
            chosenBytes(stage),
            config.bufferAllocator(),
            buildChildInputs(stage),
            sink
        );

        // Apply instruction handlers for the reduce stage.
        // Unlike AnalyticsSearchService (shard path) which resolves the factory from its
        // local backends map, the coordinator-reduce path has no backends map — the factory
        // is stored on the Stage during FragmentConversionDriver.convertAll (root stage only,
        // no serialization needed since reduce executes locally at the coordinator).
        // TODO: find a cleaner way to provide the factory without storing it on Stage.
        BackendExecutionContext backendContext = null;
        FragmentInstructionHandlerFactory factory = stage.getInstructionHandlerFactory();
        if (factory != null) {
            Throwable primaryFailure = null;
            try {
                for (InstructionNode node : stage.getPlanAlternatives().getFirst().instructions()) {
                    FragmentInstructionHandler handler = factory.createHandler(node);
                    BackendExecutionContext previous = backendContext;
                    backendContext = handler.apply(node, context, backendContext);
                    // A handler that returns a new reference implicitly abandons the previous
                    // context — close it now so its resources aren't orphaned.
                    if (previous != null && previous != backendContext) {
                        previous.close();
                    }
                }
            } catch (Throwable t) {
                primaryFailure = t;
                // On failure, close the backendContext since it won't be handed to the sink.
                if (backendContext != null) {
                    try {
                        backendContext.close();
                    } catch (Exception closeFailure) {
                        primaryFailure.addSuppressed(closeFailure);
                    }
                }
            }
            if (primaryFailure != null) {
                if (primaryFailure instanceof RuntimeException re) throw re;
                if (primaryFailure instanceof Error err) throw err;
                throw new RuntimeException("Instruction handler failed for stageId=" + stage.getStageId(), primaryFailure);
            }
        }

        ExchangeSink backendSink;
        try {
            backendSink = provider.createSink(context, backendContext);
        } catch (Exception e) {
            // Sink creation failed — close backendContext to avoid resource leak.
            if (backendContext != null) {
                try {
                    backendContext.close();
                } catch (Exception closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw new RuntimeException("Failed to create exchange sink for stageId=" + stage.getStageId(), e);
        }
        return new LocalStageExecution(stage, backendSink, sink);
    }

    /** Picks the plan-alternative bytes bound to the stage's exchange sink provider. */
    private static byte[] chosenBytes(Stage stage) {
        assert stage.getPlanAlternatives().size() == 1 : "COORDINATOR_REDUCE stage "
            + stage.getStageId()
            + " expected exactly one plan alternative, got "
            + stage.getPlanAlternatives().size();
        return stage.getPlanAlternatives().getFirst().convertedBytes();
    }

    /**
     * Builds one {@link ExchangeSinkContext.ChildInput} per child stage. Each entry
     * carries the child's stage id (used by the backend to namespace its registered
     * input, e.g. {@code "input-<stageId>"}) and the Arrow schema derived from the
     * child fragment's row type.
     */
    private static List<ExchangeSinkContext.ChildInput> buildChildInputs(Stage stage) {
        List<Stage> children = stage.getChildStages();
        if (children.isEmpty()) {
            throw new IllegalStateException(
                "COORDINATOR_REDUCE stage " + stage.getStageId() + " expected at least one child stage, got zero"
            );
        }
        List<ExchangeSinkContext.ChildInput> inputs = new ArrayList<>(children.size());
        for (Stage child : children) {
            inputs.add(new ExchangeSinkContext.ChildInput(child.getStageId(), childSchema(stage, child)));
        }
        return inputs;
    }

    /**
     * Asks the backend for the child's physical output schema; falls back to a
     * Calcite row-type derivation when the backend has no opinion.
     */
    private static Schema childSchema(Stage parent, Stage child) {
        var convertor = parent.getFragmentConvertor();
        if (convertor != null) {
            var alts = child.getPlanAlternatives();
            if (!alts.isEmpty()) {
                Schema fromBackend = convertor.partialAggOutputSchema(alts.getFirst().convertedBytes());
                if (fromBackend != null) {
                    return fromBackend;
                }
            }
        }
        return ArrowSchemaFromCalcite.arrowSchemaFromRowType(child.getFragment().getRowType());
    }
}
