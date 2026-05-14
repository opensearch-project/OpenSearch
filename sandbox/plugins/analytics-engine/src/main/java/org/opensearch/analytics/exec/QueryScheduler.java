/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default {@link Scheduler} implementation. Two-phase execution:
 * <ol>
 *   <li>{@link #plan(QueryContext)} — builds the execution graph without
 *       starting any stages. Returns an {@link ExecutionGraph} that can
 *       be inspected for EXPLAIN.</li>
 *   <li>{@link #execute(QueryContext, ActionListener)} — builds and starts
 *       execution in one call (the normal query path).</li>
 * </ol>
 *
 * <p>Also manages a pool of active {@link PlanWalker} instances for
 * observability and cancellation.
 *
 * @opensearch.internal
 */
public class QueryScheduler implements Scheduler {

    private static final Logger logger = LogManager.getLogger(QueryScheduler.class);

    private final StageExecutionBuilder stageExecutionBuilder;
    private final Map<String, PlanWalker> walkerPool = new ConcurrentHashMap<>();

    @Inject
    public QueryScheduler(StageExecutionBuilder stageExecutionBuilder) {
        this.stageExecutionBuilder = stageExecutionBuilder;
    }

    /**
     * Builds the execution graph without starting any stages.
     * Use for EXPLAIN — inspect the returned graph, then discard.
     *
     * @param config the per-query context
     * @return the fully-wired but unstarted execution graph
     */
    public ExecutionGraph plan(QueryContext config) {
        PlanWalker walker = new PlanWalker(config, stageExecutionBuilder, ActionListener.wrap(r -> {}, e -> {}));
        return walker.build();
    }

    @Override
    public void execute(QueryContext config, ActionListener<Iterable<VectorSchemaRoot>> listener) {
        final String queryId = config.queryId();
        final long queryStartNanos = System.nanoTime();
        final AnalyticsOperationListener.CompositeListener opListener = new AnalyticsOperationListener.CompositeListener(
            config.operationListeners()
        );

        PlanWalker walker = createWalker(config, listener, queryId, queryStartNanos, opListener);
        walkerPool.put(queryId, walker);

        final AnalyticsQueryTask queryTask = config.parentTask();

        // Build the graph BEFORE installing the cancel callback. PlanWalker.cancelAll() bails
        // when the graph is null (it's the only state it can know to cascade cancels through),
        // so installing the callback before build() lets a late-cancel replay run cancelAll()
        // against a null graph — a silent no-op that allows the query to keep running.
        // Installing the callback after build() means:
        //   - any cancel landing during build() has nothing installed yet → onCancelled is a
        //     no-op on the task side, but
        //   - setOnCancelCallback below sees isCancelled() and replays the new callback
        //     synchronously, which now has a real graph to cascade cancels through.
        // The subsequent walker.start(graph) calls leaf.start() on each leaf; those that
        // already saw cancelAll transition into CANCELLED and ShardFragmentStageExecution.start
        // / LocalStageExecution.start treat transitionTo(RUNNING) failure as a no-op, so no
        // dispatch happens for cancelled queries.
        ExecutionGraph graph = walker.build();

        queryTask.setOnCancelCallback(() -> {
            String reason = "task cancelled: " + (queryTask.getReasonCancelled() != null ? queryTask.getReasonCancelled() : "unknown");
            logger.info("[QueryScheduler] AnalyticsQueryTask.onCancelled fired, reason={}", reason);
            walker.cancelAll(reason);
        });

        opListener.onQueryStart(queryId, graph.stageCount());

        logger.info("[QueryScheduler] ExecutionGraph built:\n{}", graph.explain());
        walker.start(graph);
    }

    private PlanWalker createWalker(
        QueryContext config,
        ActionListener<Iterable<VectorSchemaRoot>> listener,
        String queryId,
        long queryStartNanos,
        AnalyticsOperationListener opListener
    ) {
        ActionListener<Iterable<VectorSchemaRoot>> wrapped = ActionListener.wrap(result -> {
            walkerPool.remove(queryId);
            opListener.onQuerySuccess(queryId, System.nanoTime() - queryStartNanos, 0);
            listener.onResponse(result);
        }, e -> {
            walkerPool.remove(queryId);
            opListener.onQueryFailure(queryId, e);
            listener.onFailure(e);
        });
        return new PlanWalker(config, stageExecutionBuilder, wrapped);
    }

    /**
     * Package-accessor for the underlying {@link StageExecutionBuilder}. Used by join-strategy
     * dispatchers (e.g. M1 broadcast) that need to run a single stage in isolation with a
     * caller-supplied output sink, bypassing the walker's parent-sink resolution chain.
     */
    public StageExecutionBuilder stageExecutionBuilder() {
        return stageExecutionBuilder;
    }

    /** Pool-level lookup for observability / metrics. */
    public PlanWalker walkerFor(String queryId) {
        return walkerPool.get(queryId);
    }

    /** Pool-level iteration for concurrency limiting. */
    public Collection<PlanWalker> activeWalkers() {
        return walkerPool.values();
    }
}
