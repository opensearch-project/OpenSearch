/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;

/**
 * {@link StageExecution} implementation for COORDINATOR_REDUCE stages. Holds a
 * backend-provided {@link ExchangeSink} (from {@link org.opensearch.analytics.spi.ExchangeSinkProvider})
 * and routes all child stage output into it via {@link #inputSink(int)}.
 *
 * <p>This is a placeholder shape: the backend sink accepts batches but there is
 * no contract yet for draining its output downstream. The drain/output contract
 * will be re-introduced when a real backend implementation lands.
 *
 * <p>Lifecycle:
 * {@code CREATED → RUNNING → (SUCCEEDED | FAILED | CANCELLED)}
 *
 * @opensearch.internal
 */
final class LocalStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private static final Logger logger = LogManager.getLogger(LocalStageExecution.class);

    private final ExchangeSink backendSink;
    private final ExchangeSink downstream;

    public LocalStageExecution(Stage stage, ExchangeSink backendSink, ExchangeSink downstream) {
        super(stage);
        this.backendSink = backendSink;
        this.downstream = downstream;
        logger.info("[LocalStage] CREATED stageId={} childCount={}", stage.getStageId(), stage.getChildStages().size());
    }

    // All children feed into the single backend sink.
    @Override
    public ExchangeSink inputSink(int childStageId) {
        return backendSink;
    }

    // No output drain contract yet. Will be reintroduced when a real backend
    // implementation is wired up.
    @Override
    public ExchangeSource outputSource() {
        throw new UnsupportedOperationException(
            "LocalStageExecution has no output source yet — backend drain contract pending"
        );
    }

    @Override
    public void start() {
        if (transitionTo(State.RUNNING) == false) return;
        logger.info("[LocalStage] start() stageId={}", stage.getStageId());
        try {
            backendSink.close();
            downstream.close();
            if (transitionTo(State.SUCCEEDED)) {
                logger.info("[LocalStage] SUCCEEDED stageId={}", stage.getStageId());
            }
        } catch (Exception e) {
            captureFailure(e);
            if (transitionTo(State.FAILED)) {
                metrics.incrementTasksFailed();
                logger.info("[LocalStage] FAILED stageId={} cause={}", stage.getStageId(), e.getMessage());
            }
        }
    }

    @Override
    public boolean failFromChild(Exception cause) {
        logger.error("[LocalStage] failFromChild stageId={}", stage.getStageId(), cause);
        captureFailure(cause);
        if (transitionTo(State.FAILED)) {
            try { backendSink.close(); } catch (Exception ignore) {}
            metrics.incrementTasksFailed();
            return true;
        }
        return false;
    }

    @Override
    public void cancel(String reason) {
        logger.info("[LocalStage] cancel stageId={} reason={}", stage.getStageId(), reason);
        if (transitionTo(State.CANCELLED)) {
            try { backendSink.close(); } catch (Exception ignore) {}
        }
    }
}
