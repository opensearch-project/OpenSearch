/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;

/**
 * Integration point for the query planner into the search execution pipeline.
 * This class provides methods to analyze queries and generate query plans
 * when the query planner feature flag is enabled.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class QueryPlannerIntegration {

    private static final Logger logger = LogManager.getLogger(QueryPlannerIntegration.class);

    /**
     * Analyzes a query and generates a query plan if the feature is enabled.
     * Returns null if the feature is disabled or if an error occurs.
     *
     * @param queryBuilder The query to analyze
     * @param queryShardContext The query shard context
     * @return The query plan node or null
     */
    public static QueryPlanNode analyzeQuery(QueryBuilder queryBuilder, QueryShardContext queryShardContext) {
        if (!FeatureFlags.isEnabled(FeatureFlags.QUERY_PLANNER_SETTING)) {
            return null;
        }

        try {
            logger.debug("Query planner enabled, analyzing query: {}", queryBuilder.getClass().getSimpleName());
            LogicalPlanBuilder builder = new LogicalPlanBuilder(queryShardContext);
            QueryPlanNode plan = builder.build(queryBuilder);

            if (plan != null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Query plan tree:\n{}", QueryPlanVisualizer.toString(plan));
                } else if (logger.isDebugEnabled()) {
                    logger.debug("Query plan: type={}, cost={}", plan.getType(), plan.getEstimatedCost().getLuceneCost());
                }
            }

            return plan;
        } catch (IOException e) {
            logger.debug("Failed to generate query plan: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Checks if a query plan suggests optimization opportunities.
     *
     * @param plan The query plan to check
     * @return true if optimization opportunities exist
     */
    public static boolean hasOptimizationOpportunities(QueryPlanNode plan) {
        if (plan == null) {
            return false;
        }

        QueryCost cost = plan.getEstimatedCost();

        // Check for expensive queries
        if (cost.getLuceneCost() > 1_000_000 || cost.getCpuCost() > 10.0) {
            logger.debug("Query identified as expensive: lucene={}, cpu={}", cost.getLuceneCost(), cost.getCpuCost());
            return true;
        }

        return false;
    }

    /**
     * Logs query plan statistics for monitoring.
     *
     * @param plan The query plan
     * @param executionTimeNanos The actual execution time in nanoseconds
     */
    public static void logPlanStatistics(QueryPlanNode plan, long executionTimeNanos) {
        if (plan == null || !logger.isDebugEnabled()) {
            return;
        }

        QueryCost estimatedCost = plan.getEstimatedCost();
        double executionTimeMs = executionTimeNanos / 1_000_000.0;

        logger.debug(
            "Query execution statistics - Type: {}, Estimated Lucene cost: {}, Actual time: {}ms",
            plan.getType(),
            estimatedCost.getLuceneCost(),
            executionTimeMs
        );

        // Log if actual execution time significantly differs from estimate
        double expectedTimeMs = estimatedCost.getLuceneCost() / 1000.0; // Rough conversion
        if (executionTimeMs > expectedTimeMs * 10) {
            logger.debug(
                "Query execution took longer than rough estimate. Type: {}, Estimated: ~{}ms, Actual: {}ms",
                plan.getType(),
                expectedTimeMs,
                executionTimeMs
            );
        }
    }
}
