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
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;

/**
 * {@link StageExecution} implementation for COORDINATOR_REDUCE stages. Owns the
 * {@link LocalStageContext} lifecycle (start, finalize, fail, cancel)
 * and ensures the downstream listener is signaled exactly once.
 *
 * <p>Implements {@link SinkProvidingStageExecution} as both a
 * {@link DataConsumer} (children write into per-child input sinks via
 * {@link #inputSink(int)}) and a {@link DataProducer} (output is
 * produced by the backend into the parent's sink).
 *
 * <p>Lifecycle:
 * {@code CREATED → RUNNING → (SUCCEEDED | FAILED | CANCELLED)}
 *
 * @opensearch.internal
 */
final class LocalStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private static final Logger logger = LogManager.getLogger(LocalStageExecution.class);

    private final LocalStageContext ctx;
    private final ExchangeSink outputSinkRef;

    public LocalStageExecution(Stage stage, LocalStageContext ctx, ExchangeSink outputSink) {
        super(stage);
        this.ctx = ctx;
        this.outputSinkRef = outputSink;
        logger.info("[LocalStage] CREATED stageId={} childCount={}", stage.getStageId(), stage.getChildStages().size());
    }

    /** Returns the backend-provided context for this local stage. */
    public LocalStageContext getCtx() {
        return ctx;
    }

    @Override
    public ExchangeSink inputSink(int childStageId) {
        return ctx.sinkFor(childStageId);
    }

    @Override
    public ExchangeSink outputSink() {
        return outputSinkRef;
    }

    @Override
    public ExchangeSource outputSource() {
        if (outputSinkRef instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException("outputSink does not implement ExchangeSource");
    }

    /**
     * Called by the walker / runner once all children have completed successfully
     * and their batches have landed in the per-child input sinks. Transitions to
     * {@code RUNNING}, then delegates to the backend's
     * {@link LocalStageContext#asyncFinalize} which drains output and signals the
     * listener with the terminal state transition.
     */
    @Override
    public void start() {
        if (transitionTo(State.RUNNING) == false) return;
        logger.info("[LocalStage] start() stageId={}", stage.getStageId());
        ctx.asyncFinalize(ActionListener.wrap(v -> {
            if (transitionTo(State.SUCCEEDED)) {
                logger.info("[LocalStage] listener.onResponse stageId={}", stage.getStageId());
            }
        }, e -> {
            captureFailure(e);
            if (transitionTo(State.FAILED)) {
                metrics.incrementTasksFailed();
                logger.info("[LocalStage] listener.onFailure stageId={} cause={}", stage.getStageId(), e.getMessage());
            }
        }));
    }

    /**
     * Called by the walker when any child stage fails before finalize. Closes
     * the backend context, records metrics, and signals the listener with the
     * failure. Used by {@code PlanWalker}'s per-parent state listener to
     * propagate child failures upward while ensuring the
     * {@link LocalStageContext} is released.
     */
    @Override
    public boolean failFromChild(Exception cause) {
        logger.error("[LocalStage] failFromChild stageId={}", stage.getStageId(), cause);
        captureFailure(cause);
        if (transitionTo(State.FAILED)) {
            try {
                ctx.close();
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
                ctx.close();
            } catch (Exception ignore) {}
        }
    }

}
