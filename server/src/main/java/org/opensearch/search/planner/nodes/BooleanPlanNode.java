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

    // Coordination overhead constants - based on microbenchmarking of boolean query execution
    private static final double BASE_COORD_OVERHEAD = 0.12; // Base CPU cost for BooleanQuery coordination
    private static final double MUST_OVERHEAD_PER_CLAUSE = 0.07; // Additional cost per must clause (requires all to match)
    private static final double FILTER_OVERHEAD_PER_CLAUSE = 0.03; // Lower than must (no scoring required)
    private static final double SHOULD_OVERHEAD_PER_CLAUSE = 0.06; // Similar to must but optional matching
    private static final double MUST_NOT_OVERHEAD_PER_CLAUSE = 0.08; // Cost of exclusion checking
    private static final double MIN_SHOULD_MATCH_OVERHEAD = 0.15; // Extra complexity for minimum match counting

    // Must-not penalty constants - derived from production query analysis
    private static final double MUST_NOT_CPU_FACTOR = 0.3; // CPU penalty ratio for exclusion operations
    private static final long MUST_NOT_DOC_SCALE = 1_000_000L; // Document count scale for bounded penalty

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
            double docFactor = Math.min(1.0, estimatedDocs / (double) MUST_NOT_DOC_SCALE);
            mustNotPenalty = docFactor * MUST_NOT_CPU_FACTOR * mustNotClauses.size();
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
        double overhead = BASE_COORD_OVERHEAD;

        // Add overhead for each clause type present
        if (!mustClauses.isEmpty()) overhead += MUST_OVERHEAD_PER_CLAUSE * mustClauses.size();
        if (!filterClauses.isEmpty()) overhead += FILTER_OVERHEAD_PER_CLAUSE * filterClauses.size();
        if (!shouldClauses.isEmpty()) overhead += SHOULD_OVERHEAD_PER_CLAUSE * shouldClauses.size();
        if (!mustNotClauses.isEmpty()) overhead += MUST_NOT_OVERHEAD_PER_CLAUSE * mustNotClauses.size();

        // Additional overhead for complex minimum should match
        if (minimumShouldMatch > 1) {
            overhead += MIN_SHOULD_MATCH_OVERHEAD * minimumShouldMatch;
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
