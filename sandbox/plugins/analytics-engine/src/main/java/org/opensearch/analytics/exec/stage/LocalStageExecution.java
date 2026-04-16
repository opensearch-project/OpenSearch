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
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;

/**
 * {@link StageExecution} implementation for LOCAL stages. Owns the
 * {@link LocalStageContext} lifecycle (start, finalize, fail, cancel)
 * and ensures the downstream listener is signaled exactly once.
 *
 * <p>Lifecycle:
 * {@code CREATED → RUNNING → (SUCCEEDED | FAILED | CANCELLED)}
 *
 * <p>Instances are one-shot: constructed, {@code start()} called once,
 * listener signaled once, discarded.
 *
 * @opensearch.internal
 */
final class LocalStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private static final Logger logger = LogManager.getLogger(LocalStageExecution.class);

    private final LocalStageContext ctx;

    public LocalStageExecution(Stage stage, LocalStageContext ctx) {
        super(stage);
        this.ctx = ctx;
        logger.info("[LocalStage] CREATED stageId={} childCount={}", stage.getStageId(), stage.getChildStages().size());
    }

    /** Returns the backend-provided context for this local stage. */
    public LocalStageContext getCtx() {
        return ctx;
    }

    @Override
    public ExchangeSink sink(int childStageId) {
        return ctx.sinkFor(childStageId);
    }

    /**
     * LocalStage routes per-child via {@link #sink(int)} / {@code ctx.sinkFor(childStageId)}
     * — there is no single shared sink. Callers must use the childStageId-aware
     * overload; this one is never reached because {@link #sink(int)} is overridden
     * above, bypassing the default delegation in {@link SinkProvidingStageExecution}.
     */
    @Override
    public ExchangeSink sink() {
        throw new UnsupportedOperationException(
            "LocalStageExecution has per-child input sinks — call sink(int childStageId) instead"
        );
    }

    /**
     * Called by the walker / runner once all children have completed successfully
     * and their batches have landed in the per-child input sinks. Transitions to
     * {@code RUNNING}, then delegates to the backend's
     * {@link LocalStageContext#asyncFinalize} which drains output and signals the
     * listener with the terminal state transition.
     *
     * <p>Callers must ensure that by the time {@code start()} is invoked, every
     * child input has been fully fed and closed — there is no separate
     * {@code finalize()} step. In the coordinator-side event-driven driver this
     * is guaranteed by the dependency barrier; in
     * {@code ShuffleReadTaskRunner}
     * it is guaranteed by calling {@code start()} only after all fetches
     * have completed and their sinks have been closed.
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
        logger.info("[LocalStage] failFromChild stageId={} cause={}", stage.getStageId(), cause.getMessage());
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
