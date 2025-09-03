/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner.nodes;

import org.apache.lucene.search.BooleanQuery;
import org.opensearch.search.planner.AbstractQueryPlanNode;
import org.opensearch.search.planner.QueryCost;
import org.opensearch.search.planner.QueryNodeType;
import org.opensearch.search.planner.QueryPlanNode;
import org.opensearch.search.planner.QueryPlanProfile;

import java.util.ArrayList;
import java.util.List;

/**
 * Plan node for boolean queries with must/should/filter/must_not clauses.
 *
 * @opensearch.internal
 */
public class BooleanPlanNode extends AbstractQueryPlanNode {

    private final List<QueryPlanNode> mustClauses;
    private final List<QueryPlanNode> filterClauses;
    private final List<QueryPlanNode> shouldClauses;
    private final List<QueryPlanNode> mustNotClauses;
    private final int minimumShouldMatch;

    public BooleanPlanNode(
        BooleanQuery query,
        List<QueryPlanNode> mustClauses,
        List<QueryPlanNode> filterClauses,
        List<QueryPlanNode> shouldClauses,
        List<QueryPlanNode> mustNotClauses,
        int minimumShouldMatch
    ) {
        super(query, QueryNodeType.BOOLEAN, combineChildren(mustClauses, filterClauses, shouldClauses, mustNotClauses));
        this.mustClauses = mustClauses;
        this.filterClauses = filterClauses;
        this.shouldClauses = shouldClauses;
        this.mustNotClauses = mustNotClauses;
        this.minimumShouldMatch = minimumShouldMatch;
    }

    private static List<QueryPlanNode> combineChildren(
        List<QueryPlanNode> must,
        List<QueryPlanNode> filter,
        List<QueryPlanNode> should,
        List<QueryPlanNode> mustNot
    ) {
        List<QueryPlanNode> combined = new ArrayList<>();
        combined.addAll(must);
        combined.addAll(filter);
        combined.addAll(should);
        combined.addAll(mustNot);
        return combined;
    }

    @Override
    protected QueryCost calculateCost() {
        // Calculate coordination overhead based on clause types and counts
        double coordinationOverhead = calculateCoordinationOverhead();

        // Combine costs from all clauses
        List<QueryCost> allCosts = new ArrayList<>();

        // Must clauses - highest priority, all must match
        for (QueryPlanNode node : mustClauses) {
            allCosts.add(node.getEstimatedCost());
        }

        // Filter clauses - no scoring overhead
        for (QueryPlanNode node : filterClauses) {
            allCosts.add(node.getEstimatedCost());
        }

        // Should clauses - depends on minimumShouldMatch
        if (!shouldClauses.isEmpty()) {
            if (minimumShouldMatch > 0) {
                for (int i = 0; i < Math.min(minimumShouldMatch, shouldClauses.size()); i++) {
                    allCosts.add(shouldClauses.get(i).getEstimatedCost());
                }
            } else if (mustClauses.isEmpty() && filterClauses.isEmpty()) {
                // If no must/filter clauses, at least one should must match
                allCosts.add(shouldClauses.get(0).getEstimatedCost());
            }
        }

        double mustNotPenalty = 0;
        if (!mustNotClauses.isEmpty()) {
            // Must not clauses add overhead proportional to the number of docs that need checking
            long estimatedDocs = allCosts.isEmpty() ? 0 : allCosts.stream().mapToLong(c -> c.getLuceneCost()).min().orElse(0);
            // Bounded, doc-sensitive CPU increment
            double docFactor = Math.min(1.0, estimatedDocs / 1_000_000.0);
            mustNotPenalty = docFactor * 0.25 * mustNotClauses.size();
        }

        QueryCost combined = QueryCost.combine(allCosts, coordinationOverhead);

        // Add must_not penalty
        return new QueryCost(
            combined.getLuceneCost(),
            combined.getCpuCost() + mustNotPenalty,
            combined.getMemoryCost(),
            combined.getIoCost(),
            combined.isEstimate()
        );
    }

    private double calculateCoordinationOverhead() {
        // Base overhead for boolean query coordination
        double overhead = 0.1;

        // Add overhead for each clause type present
        if (!mustClauses.isEmpty()) overhead += 0.05 * mustClauses.size();
        if (!filterClauses.isEmpty()) overhead += 0.02 * filterClauses.size(); // Filters are cheaper
        if (!shouldClauses.isEmpty()) overhead += 0.05 * shouldClauses.size();
        if (!mustNotClauses.isEmpty()) overhead += 0.05 * mustNotClauses.size(); // Reduced from 0.1 to 0.05

        // Additional overhead for complex minimum should match
        if (minimumShouldMatch > 1) {
            overhead += 0.1 * minimumShouldMatch;
        }

        return overhead;
    }

    @Override
    protected void addProfileAttributes(QueryPlanProfile profile) {
        super.addProfileAttributes(profile);
        profile.addAttribute("must_clauses", mustClauses.size());
        profile.addAttribute("filter_clauses", filterClauses.size());
        profile.addAttribute("should_clauses", shouldClauses.size());
        profile.addAttribute("must_not_clauses", mustNotClauses.size());
        if (minimumShouldMatch > 0) {
            profile.addAttribute("minimum_should_match", minimumShouldMatch);
        }
    }

    public List<QueryPlanNode> getMustClauses() {
        return mustClauses;
    }

    public List<QueryPlanNode> getFilterClauses() {
        return filterClauses;
    }

    public List<QueryPlanNode> getShouldClauses() {
        return shouldClauses;
    }

    public List<QueryPlanNode> getMustNotClauses() {
        return mustNotClauses;
    }
}
