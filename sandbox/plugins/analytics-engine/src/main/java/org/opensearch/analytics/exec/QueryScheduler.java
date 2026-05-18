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

        // Build the graph first. On failure the partial graph cleans itself up via
        // walker.build()'s try-finally; the RuntimeException bubbles to
        // DefaultPlanExecutor's outer catch which fires listener.onFailure with the cause.
        ExecutionGraph graph = walker.build();

        // Wire the completion listener BEFORE registering the cancel callback so a
        // post-build / pre-start cancellation reaches the listener via the cascade.
        // Without this the user-facing listener is never registered and queries hang
        // until the test or REST socket times out.
        walker.wireCompletion();
        walkerPool.put(queryId, walker);

        final AnalyticsQueryTask queryTask = config.parentTask();
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
     * Returns the underlying {@link StageExecutionBuilder} so callers can register a
     * custom {@link org.opensearch.analytics.exec.stage.StageScheduler} for a stage
     * type (e.g. fault-injecting scheduler in resilience tests). Resolving via the
     * singleton scheduler avoids a Guice JIT lookup that would re-instantiate
     * {@link AnalyticsSearchTransportService} (whose ctor registers transport
     * handlers, only legal once per node).
     */
    public StageExecutionBuilder getStageExecutionBuilder() {
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
