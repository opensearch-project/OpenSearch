/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner.nodes;

import org.apache.lucene.search.Query;
import org.opensearch.search.planner.AbstractQueryPlanNode;
import org.opensearch.search.planner.QueryCost;
import org.opensearch.search.planner.QueryNodeType;

/**
 * Generic plan node for queries without specific implementations.
 *
 * @opensearch.internal
 */
public class GenericPlanNode extends AbstractQueryPlanNode {

    private final long estimatedCost;

    public GenericPlanNode(Query query, QueryNodeType nodeType, long estimatedCost) {
        super(query, nodeType);
        this.estimatedCost = estimatedCost;
    }

    @Override
    protected QueryCost calculateCost() {
        // Use heuristics based on node type
        double cpuMultiplier = 1.0;
        double memoryMultiplier = 1.0;
        double ioMultiplier = 1.0;

        if (nodeType.isComputationallyExpensive()) {
            cpuMultiplier = 5.0;
            memoryMultiplier = 2.0;
        }

        double cpuCost = 0.01 * cpuMultiplier;
        long memoryCost = (long) (estimatedCost * 10 * memoryMultiplier);
        double ioCost = 0.05 * ioMultiplier;

        return new QueryCost(estimatedCost, cpuCost, memoryCost, ioCost, true);
    }
}
