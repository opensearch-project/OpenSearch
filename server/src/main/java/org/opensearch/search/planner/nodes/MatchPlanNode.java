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
 * Plan node for match queries - analyzed text search.
 *
 * @opensearch.internal
 */
public class MatchPlanNode extends AbstractQueryPlanNode {

    private final String field;
    private final String text;
    private final String analyzer;
    private final int termCount;
    private final long estimatedDocs;

    public MatchPlanNode(Query query, String field, String text, String analyzer, int termCount, long estimatedDocs) {
        super(query, QueryNodeType.MATCH);
        this.field = field;
        this.text = text;
        this.analyzer = analyzer;
        this.termCount = termCount;
        this.estimatedDocs = estimatedDocs;
    }

    @Override
    protected QueryCost calculateCost() {
        // Match queries have analysis overhead
        double cpuCost = 0.01; // Base cost for analysis

        // Add cost per term (after analysis)
        cpuCost += termCount * 0.005;

        // Memory cost for analysis and term storage
        long memoryCost = text.length() * 10 + // Text analysis buffer
            termCount * 100;       // Per-term overhead

        // I/O cost depends on number of terms
        double ioCost = termCount * 0.02;

        // Additional cost if using complex analyzer
        if (isComplexAnalyzer()) {
            cpuCost *= 1.5;
            memoryCost *= 1.5;
        }

        return new QueryCost(estimatedDocs, cpuCost, memoryCost, ioCost, true);
    }

    private boolean isComplexAnalyzer() {
        // Simple heuristic - certain analyzers are more expensive
        return analyzer != null
            && (analyzer.contains("synonym")
                || analyzer.contains("phonetic")
                || analyzer.contains("ngram")
                || analyzer.contains("shingle"));
    }

    @Override
    protected void addProfileAttributes(QueryPlanProfile profile) {
        super.addProfileAttributes(profile);
        profile.addAttribute("field", field);
        profile.addAttribute("text", text);
        if (analyzer != null) {
            profile.addAttribute("analyzer", analyzer);
        }
        profile.addAttribute("term_count", termCount);
        profile.addAttribute("estimated_docs", estimatedDocs);
    }

    public String getField() {
        return field;
    }

    public String getText() {
        return text;
    }
}
