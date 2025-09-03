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
 * Plan node for range queries.
 *
 * @opensearch.internal
 */
public class RangePlanNode extends AbstractQueryPlanNode {

    private final String field;
    private final Object from;
    private final Object to;
    private final boolean includeFrom;
    private final boolean includeTo;
    private final long estimatedDocs;

    public RangePlanNode(Query query, String field, Object from, Object to, boolean includeFrom, boolean includeTo, long estimatedDocs) {
        super(query, QueryNodeType.RANGE);
        this.field = field;
        this.from = from;
        this.to = to;
        this.includeFrom = includeFrom;
        this.includeTo = includeTo;
        this.estimatedDocs = estimatedDocs;
    }

    @Override
    protected QueryCost calculateCost() {
        // Range queries are more expensive than term queries
        // They require scanning through the index

        // CPU cost depends on the range size
        double cpuCost = 0.01; // Base cost

        // Add cost based on estimated docs to scan
        cpuCost += estimatedDocs * 0.00001;

        // Memory cost for storing intermediate results
        long memoryCost = estimatedDocs * 16; // More memory than term query

        // I/O cost is higher for range queries (index scanning)
        double ioCost = 0.1 + (estimatedDocs * 0.00001);

        // Numeric ranges can use points which are more efficient
        if (isNumericField()) {
            cpuCost *= 0.5; // Points are more efficient
            ioCost *= 0.5;
        }

        return new QueryCost(estimatedDocs, cpuCost, memoryCost, ioCost, true);
    }

    private boolean isNumericField() {
        // Simple heuristic - in real implementation would check field mapping
        return from instanceof Number || to instanceof Number;
    }

    @Override
    protected void addProfileAttributes(QueryPlanProfile profile) {
        super.addProfileAttributes(profile);
        profile.addAttribute("field", field);
        if (from != null) {
            profile.addAttribute("from", from.toString());
            profile.addAttribute("include_from", includeFrom);
        }
        if (to != null) {
            profile.addAttribute("to", to.toString());
            profile.addAttribute("include_to", includeTo);
        }
        profile.addAttribute("estimated_docs", estimatedDocs);
        profile.addAttribute("is_numeric", isNumericField());
    }

    public String getField() {
        return field;
    }
}
