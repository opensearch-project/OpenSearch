/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    public void execute(QueryContext config, ActionListener<Iterable<Object[]>> listener) {
        final String queryId = config.queryId();
        PlanWalker walker = createWalker(config, listener, queryId);
        walkerPool.put(queryId, walker);

        final AnalyticsQueryTask queryTask = config.parentTask();
        queryTask.setOnCancelCallback(() -> {
            String reason = "task cancelled: "
                + (queryTask.getReasonCancelled() != null ? queryTask.getReasonCancelled() : "unknown");
            logger.info("[QueryScheduler] AnalyticsQueryTask.onCancelled fired, reason={}", reason);
            walker.cancelAll(reason);
        });

        // Two-phase: build graph, then start execution
        ExecutionGraph graph = walker.build();
        logger.info("[QueryScheduler] ExecutionGraph built:\n{}", graph.explain());
        walker.start(graph);
    }

    private PlanWalker createWalker(QueryContext config, ActionListener<Iterable<Object[]>> listener, String queryId) {
        ActionListener<Iterable<Object[]>> wrapped = ActionListener.wrap(
            result -> {
                walkerPool.remove(queryId);
                listener.onResponse(result);
            },
            e -> {
                walkerPool.remove(queryId);
                listener.onFailure(e);
            }
        );
        return new PlanWalker(config, stageExecutionBuilder, wrapped);
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
