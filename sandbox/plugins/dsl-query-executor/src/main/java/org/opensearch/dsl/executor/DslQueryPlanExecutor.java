/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.dsl.result.ExecutionResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Iterates over {@link QueryPlans}, delegates each RelNode to the analytics engine's
 * {@link QueryPlanExecutor}, and collects results.
 */
public class DslQueryPlanExecutor {

    private static final Logger logger = LogManager.getLogger(DslQueryPlanExecutor.class);

    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> executor;

    /**
     * Creates an executor backed by the given analytics engine plan executor.
     *
     * @param executor analytics engine executor that runs individual RelNode plans
     */
    public DslQueryPlanExecutor(QueryPlanExecutor<RelNode, Iterable<Object[]>> executor) {
        this.executor = executor;
    }

    // TODO: add per-plan error handling so a failure in one plan
    //  doesn't prevent returning partial results from other plans (e.g. HITS)
    /**
     * Executes all plans and returns results in plan order.
     *
     * @param plans the query plans to execute
     * @return execution results, one per plan
     */
    public List<ExecutionResult> execute(QueryPlans plans) {
        List<QueryPlans.QueryPlan> queryPlans = plans.getAll();
        List<ExecutionResult> results = new ArrayList<>(queryPlans.size());

        for (QueryPlans.QueryPlan plan : queryPlans) {
            RelNode relNode = plan.relNode();
            logPlan(relNode);
            // TODO: context param is null, may carry execution hints
            Iterable<Object[]> rows = executor.execute(relNode, null);
            results.add(new ExecutionResult(plan, rows));
        }

        return results;
    }

    // TODO: move plan logging behind a debug flag
    //  invalidateMetadataQuery() and THREAD_PROVIDERS are only needed for explain() output
    private void logPlan(RelNode relNode) {
        if (logger.isInfoEnabled()) {
            org.apache.calcite.rel.metadata.RelMetadataQueryBase.THREAD_PROVIDERS.set(
                org.apache.calcite.rel.metadata.JaninoRelMetadataProvider.of(
                    java.util.Objects.requireNonNull(relNode.getCluster().getMetadataProvider())
                )
            );
            relNode.getCluster().invalidateMetadataQuery();
            logger.info("Executing RelNode:\n{}", relNode.explain());
        }
    }
}
