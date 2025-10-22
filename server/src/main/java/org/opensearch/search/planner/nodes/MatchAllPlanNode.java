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
import org.opensearch.search.planner.QueryPlanProfile;

/**
 * Plan node for match_all queries.
 *
 * @opensearch.internal
 */
public class MatchAllPlanNode extends AbstractQueryPlanNode {

    private final long totalDocs;

    public MatchAllPlanNode(Query query, long totalDocs) {
        super(query, QueryNodeType.MATCH_ALL);
        this.totalDocs = totalDocs;
    }

    @Override
    protected QueryCost calculateCost() {
        // Match all is expensive as it matches every document
        // But iteration is sequential and predictable

        // Low CPU cost per doc as no filtering needed
        double cpuCost = totalDocs * 0.000001;

        // Memory cost is minimal - no filtering state
        long memoryCost = 1024; // Just iterator overhead

        // High I/O cost as we read all docs
        double ioCost = totalDocs * 0.00001;

        return new QueryCost(totalDocs, cpuCost, memoryCost, ioCost, true);
    }

    @Override
    protected void addProfileAttributes(QueryPlanProfile profile) {
        super.addProfileAttributes(profile);
        profile.addAttribute("total_docs", totalDocs);
    }
}
