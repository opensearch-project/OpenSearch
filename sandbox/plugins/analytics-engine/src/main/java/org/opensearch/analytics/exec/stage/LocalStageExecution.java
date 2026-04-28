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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;

import java.util.concurrent.Executor;

/**
 * {@link StageExecution} implementation for COORDINATOR_REDUCE stages. Holds a
 * backend-provided {@link ExchangeSink} (from {@link org.opensearch.analytics.spi.ExchangeSinkProvider})
 * and routes all child stage output into it via {@link #inputSink(int)}.
 *
 * <p>The drain phase ({@link #start()}) is dispatched to a dedicated {@link Executor}
 * so that the potentially expensive native drain (sender close + output stream read)
 * does not block the state-listener thread that triggered the parent start. This
 * prevents a slow drain from starving the SEARCH thread pool of threads needed for
 * other concurrent queries' shard responses.
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
    private final Executor drainExecutor;

    /**
     * @param stage        the DAG stage
     * @param backendSink  the backend-provided sink (e.g. DatafusionReduceSink)
     * @param downstream   the downstream sink that receives drained output
     * @param drainExecutor executor to dispatch the drain phase onto; avoids blocking
     *                      the state-listener thread that triggers {@link #start()}
     */
    public LocalStageExecution(Stage stage, ExchangeSink backendSink, ExchangeSink downstream, Executor drainExecutor) {
        super(stage);
        this.backendSink = backendSink;
        this.downstream = downstream;
        this.drainExecutor = drainExecutor;
        logger.info("[LocalStage] CREATED stageId={} childCount={}", stage.getStageId(), stage.getChildStages().size());
    }

    // All children feed into the single backend sink.
    @Override
    public ExchangeSink inputSink(int childStageId) {
        return backendSink;
    }

    /**
     * Returns the downstream sink as an {@link ExchangeSource}. The backend sink's
     * {@code close()} drains native batches into this same downstream as the
     * last step of {@link #start()}, so by the time the walker reads via
     * {@code outputSource().readResult()} every result batch is already buffered
     * here.
     */
    @Override
    public ExchangeSource outputSource() {
        if (downstream instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException(
            "downstream sink " + downstream.getClass().getSimpleName() + " does not implement ExchangeSource"
        );
    }

    @Override
    public void start() {
        if (transitionTo(State.RUNNING) == false) return;
        logger.info("[LocalStage] start() stageId={} — dispatching drain to executor", stage.getStageId());
        drainExecutor.execute(() -> {
            try {
                // Closing the backend sink signals EOF to the native side and drains the
                // output stream into `downstream`. Must NOT close `downstream` here —
                // the walker hasn't read its buffered batches yet. Downstream closure
                // happens implicitly when the completion listener's consumer finishes
                // reading each batch.
                backendSink.close();
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
        });
    }

    @Override
    public boolean failFromChild(Exception cause) {
        logger.error(new ParameterizedMessage("[LocalStage] failFromChild stageId={}", stage.getStageId()), cause);
        captureFailure(cause);
        if (transitionTo(State.FAILED)) {
            try {
                backendSink.close();
            } catch (Exception ignore) {}
            metrics.incrementTasksFailed();
            return true;
        }
        return false;
    }

    @Override
    public void cancel(String reason) {
        logger.info("[LocalStage] cancel stageId={} reason={}", stage.getStageId(), reason);
        if (transitionTo(State.CANCELLED)) {
            try {
                backendSink.close();
            } catch (Exception ignore) {}
        }
    }
}
