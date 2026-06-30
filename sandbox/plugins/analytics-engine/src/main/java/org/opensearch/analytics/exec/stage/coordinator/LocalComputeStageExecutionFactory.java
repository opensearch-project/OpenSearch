/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.coordinator;

import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionFactory;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.ReducingExchangeSink;

import java.util.List;

/**
 * Builds executions for {@link StageExecutionType#LOCAL_COMPUTE} stages — fragments
 * whose leaf is a coord-only source (today: {@code OpenSearchValues}). Runs the
 * fragment locally on the coordinator via the backend's in-process executor.
 *
 * <p>The {@link ExchangeSinkContext} carries an empty {@code childInputs} list — the
 * backend's {@code ExchangeSink} skips partition registration and goes straight to
 * {@code executeLocalPlan + drain}. Hand the resulting sink to a {@link ReduceStageExecution},
 * whose task body invokes {@link ReducingExchangeSink#reduce} to drain output into the
 * downstream sink (the parent stage's input partition, or the root row-producing sink).
 *
 * @opensearch.internal
 */
public final class LocalComputeStageExecutionFactory implements StageExecutionFactory {

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        ExchangeSinkProvider provider = stage.getExchangeSinkProvider();
        if (provider == null) {
            throw new IllegalStateException(
                "LOCAL_COMPUTE stage " + stage.getStageId() + " has no ExchangeSinkProvider — DAGBuilder must attach one"
            );
        }
        ExchangeSinkContext context = new ExchangeSinkContext(
            config.queryId(),
            stage.getStageId(),
            config.parentTask() != null ? config.parentTask().getId() : 0L,
            chosenBytes(stage),
            config.bufferAllocator(),
            List.of(),
            sink
        );
        ExchangeSink backendSink = provider.createSink(context, null);
        if (backendSink instanceof ReducingExchangeSink reducing) {
            return new ReduceStageExecution(stage, config, reducing, sink);
        }
        throw new IllegalStateException(
            "Backend exchange sink for LOCAL_COMPUTE stage "
                + stage.getStageId()
                + " must implement ReducingExchangeSink, got "
                + backendSink.getClass().getName()
        );
    }

    private static byte[] chosenBytes(Stage stage) {
        if (stage.getPlanAlternatives().isEmpty()) {
            throw new IllegalStateException("LOCAL_COMPUTE stage " + stage.getStageId() + " has no plan alternatives");
        }
        return stage.getPlanAlternatives().getFirst().convertedBytes();
    }
}
