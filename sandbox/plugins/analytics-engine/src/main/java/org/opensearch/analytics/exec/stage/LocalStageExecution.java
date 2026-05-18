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
import org.opensearch.analytics.spi.MultiInputExchangeSink;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link StageExecution} implementation for COORDINATOR_REDUCE stages. Holds a
 * backend-provided {@link ExchangeSink} (from {@link org.opensearch.analytics.spi.ExchangeSinkProvider})
 * and routes all child stage output into it via {@link #inputSink(int)}.
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
    /**
     * Single-fire gate on {@link #backendSink}.close(). Failure paths
     * ({@link #failFromChild}, {@link #cancel}) are reachable both from the
     * cascade (sibling failures fan in via ChildToParentCascade) and from
     * external cancellation, so the same stage can have its terminal close
     * triggered twice. Streaming reduce sinks block on draining the native
     * pipeline; concurrent closes serialize on the sink's feedLock.
     */
    private final AtomicBoolean backendSinkClosed = new AtomicBoolean(false);

    public LocalStageExecution(Stage stage, ExchangeSink backendSink, ExchangeSink downstream) {
        super(stage);
        this.backendSink = backendSink;
        this.downstream = downstream;
        logger.info("[LocalStage] CREATED stageId={} childCount={}", stage.getStageId(), stage.getChildStages().size());
    }

    /**
     * Per-child input sink resolution. When the backend sink is a
     * {@link MultiInputExchangeSink} (multi-input shapes such as Union), returns the
     * sink for the named child stage so each child writes to its own input partition.
     * Otherwise returns the backend sink unchanged — the single-input case where every
     * child feeds the only registered partition.
     */
    @Override
    public ExchangeSink inputSink(int childStageId) {
        if (backendSink instanceof MultiInputExchangeSink multi) {
            return multi.sinkForChild(childStageId);
        }
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
        logger.info("[LocalStage] start() stageId={}", stage.getStageId());
        try {
            // Mark the gate before close so subsequent failure-path callers don't
            // re-close. Close exceptions surface here so the catch routes to FAILED.
            if (backendSinkClosed.compareAndSet(false, true)) {
                backendSink.close();
            }
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
        logger.error(new ParameterizedMessage("[LocalStage] failFromChild stageId={}", stage.getStageId()), cause);
        captureFailure(cause);
        // Close sink BEFORE transitioning. transitionTo fires the walker terminal
        // listener inline, which tears down the per-query allocator — if the
        // sink's drain task is still importing, it would race a closed allocator.
        // Gated by backendSinkClosed so a sibling-failure cascade or a follow-up
        // external cancel doesn't pile up concurrent closes on the same sink.
        closeBackendSinkOnce();
        if (transitionTo(State.FAILED)) {
            metrics.incrementTasksFailed();
            return true;
        }
        return false;
    }

    @Override
    public void cancel(String reason) {
        logger.info("[LocalStage] cancel stageId={} reason={}", stage.getStageId(), reason);
        closeBackendSinkOnce();
        transitionTo(State.CANCELLED);
    }

    /**
     * Single-fire close on the failure path. Swallows exceptions because callers
     * cannot meaningfully react and the primary cause has already been captured.
     */
    private void closeBackendSinkOnce() {
        if (backendSinkClosed.compareAndSet(false, true)) {
            try {
                backendSink.close();
            } catch (Exception ignore) {}
        }
    }
}
