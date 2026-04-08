/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan;

import org.apache.calcite.rel.RelNode;

/**
 * Transforms a raw Calcite logical plan into a resolved plan
 * ready for single-backend dispatch.
 */
public interface QueryPlanner {
    /**
     * Transforms a raw Calcite logical plan into a resolved plan
     * ready for single-backend dispatch.
     *
     * @param logicalPlan the raw Calcite logical plan
     * @param shardCount  number of shards for the target index; when == 1,
     *                    Phase 4 (AggSplit) is skipped entirely
     * @return a resolved plan with a backend name and the rewritten plan root
     * @throws QueryPlanningException if validation or resolution fails
     */
    ResolvedPlan plan(RelNode logicalPlan, int shardCount);
}
