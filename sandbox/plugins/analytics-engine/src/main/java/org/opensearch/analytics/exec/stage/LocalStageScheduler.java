/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ExchangeSinkProvider;

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
        ExchangeSink backendSink;
        try {
            backendSink = provider.createSink(context);
        } catch (Exception e) {
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
            inputs.add(
                new ExchangeSinkContext.ChildInput(
                    child.getStageId(),
                    ArrowSchemaFromCalcite.arrowSchemaFromRowType(child.getFragment().getRowType())
                )
            );
        }
        return inputs;
    }
}
