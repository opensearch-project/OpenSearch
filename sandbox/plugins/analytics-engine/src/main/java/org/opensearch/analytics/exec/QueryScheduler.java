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
 * Default {@link Scheduler} implementation. Pool manager for per-query
 * {@link PlanWalker} instances. Constructs a walker on each
 * {@link #execute} call, tracks it by query id, and removes it on
 * terminal (success or failure).
 *
 * <p>Responsibilities:
 * <ul>
 *   <li><b>Walker construction</b>: creates a {@link PlanWalker} with the
 *       per-query {@link QueryContext} and the shared {@link StageExecutionBuilder}.</li>
 *   <li><b>Pool tracking</b>: maintains {@link #walkerPool} for
 *       future observability and concurrency limiting.</li>
 *   <li><b>Cancellation wiring</b>: installs a cancel callback on the query
 *       task that calls {@link PlanWalker#cancelAll(String)}.</li>
 *   <li><b>Per-query cleanup</b>: removes the walker from the pool on the
 *       terminal path before firing the caller's listener.</li>
 * </ul>
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

    @Override
    public void execute(QueryContext config, ActionListener<Iterable<Object[]>> listener) {
        final String queryId = config.queryId();
        PlanWalker walker = getPlanWalker(config, listener, queryId);
        walkerPool.put(queryId, walker);

        final AnalyticsQueryTask queryTask = config.parentTask();
        queryTask.setOnCancelCallback(() -> {
                String reason = "task cancelled: "
                    + (queryTask.getReasonCancelled() != null ? queryTask.getReasonCancelled() : "unknown");
                logger.info("[QueryScheduler] AnalyticsQueryTask.onCancelled fired, reason={}", reason);
                walker.cancelAll(reason);
            });
        walker.walk();
    }

    private PlanWalker getPlanWalker(QueryContext config, ActionListener<Iterable<Object[]>> listener, String queryId) {
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

    /** Pool-level lookup for future observability / metrics. */
    public PlanWalker walkerFor(String queryId) {
        return walkerPool.get(queryId);
    }

    /** Pool-level iteration for future concurrency limiting. */
    public Collection<PlanWalker> activeWalkers() {
        return walkerPool.values();
    }
}
