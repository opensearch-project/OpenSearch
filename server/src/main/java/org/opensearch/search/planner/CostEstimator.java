/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Estimates query costs by combining Lucene's cost model with OpenSearch-specific heuristics.
 *
 * @opensearch.internal
 */
public class CostEstimator {

    private final QueryShardContext queryShardContext;
    private final IndexSearcher searcher;
    private final IndexReader reader;

    // Cost multipliers for different query types
    private static final Map<String, CostMultipliers> QUERY_COST_MULTIPLIERS = new HashMap<>();

    static {
        // Initialize cost multipliers
        QUERY_COST_MULTIPLIERS.put("Script", new CostMultipliers(10.0, 2.0, 1.0));
        QUERY_COST_MULTIPLIERS.put("Fuzzy", new CostMultipliers(5.0, 3.0, 1.0));
        QUERY_COST_MULTIPLIERS.put("Regexp", new CostMultipliers(5.0, 3.0, 1.0));
        QUERY_COST_MULTIPLIERS.put("Wildcard", new CostMultipliers(2.0, 1.0, 2.0));
        QUERY_COST_MULTIPLIERS.put("Prefix", new CostMultipliers(2.0, 1.0, 2.0));
        QUERY_COST_MULTIPLIERS.put("Range", new CostMultipliers(1.0, 1.0, 1.5));
        QUERY_COST_MULTIPLIERS.put("Nested", new CostMultipliers(2.0, 2.0, 1.0));
        QUERY_COST_MULTIPLIERS.put("FunctionScore", new CostMultipliers(3.0, 1.0, 1.0));
    }

    private static class CostMultipliers {
        final double cpu;
        final double memory;
        final double io;

        CostMultipliers(double cpu, double memory, double io) {
            this.cpu = cpu;
            this.memory = memory;
            this.io = io;
        }
    }

    public CostEstimator(QueryShardContext queryShardContext) {
        this.queryShardContext = queryShardContext;
        this.searcher = queryShardContext.searcher();
        this.reader = searcher != null ? searcher.getIndexReader() : null;
    }

    /**
     * Estimates the document frequency for a term.
     */
    public long estimateTermDocFrequency(String field, String value) {
        if (reader == null) {
            // Default estimate if no reader available
            return 100;
        }

        try {
            Term term = new Term(field, value);
            return reader.docFreq(term);
        } catch (IOException e) {
            // Fall back to estimate
            return 100;
        }
    }

    /**
     * Gets the total number of documents in the index.
     */
    public long getTotalDocs() {
        if (reader == null) {
            return 10000; // Default estimate
        }
        return reader.maxDoc();
    }

    /**
     * Estimates cost for a generic query using Lucene's Weight API.
     */
    public long estimateGenericCost(Query query) {
        if (searcher == null) {
            // Fallback estimate based on query type
            return estimateFallbackCost(query);
        }

        try {
            // Create weight to get scorer and cost
            Weight weight = searcher.createWeight(searcher.rewrite(query), org.apache.lucene.search.ScoreMode.COMPLETE, 1);

            // Aggregate costs across all leaves
            if (reader != null && !reader.leaves().isEmpty()) {
                long totalCost = 0;
                for (var leafContext : reader.leaves()) {
                    var scorer = weight.scorer(leafContext);
                    if (scorer != null) {
                        totalCost += scorer.iterator().cost();
                    }
                }
                if (totalCost > 0) {
                    return totalCost;
                }
            }
        } catch (IOException e) {
            // Fall back to estimate
        }

        return estimateFallbackCost(query);
    }

    private long estimateFallbackCost(Query query) {
        // Heuristic estimates when we can't get actual costs
        String queryType = query.getClass().getSimpleName();

        // Base estimates as percentage of total docs
        long totalDocs = getTotalDocs();

        if (queryType.contains("MatchAllDocs")) {
            return totalDocs;
        } else if (queryType.contains("Term")) {
            return totalDocs / 100; // ~1% of docs
        } else if (queryType.contains("Range")) {
            return totalDocs / 10; // ~10% of docs
        } else if (queryType.contains("Wildcard") || queryType.contains("Prefix")) {
            return totalDocs / 5; // ~20% of docs
        } else if (queryType.contains("Fuzzy") || queryType.contains("Regexp")) {
            return totalDocs / 2; // ~50% of docs (expensive)
        }

        // Default: 5% of docs
        return totalDocs / 20;
    }

    /**
     * Estimates additional costs beyond Lucene's document-based cost.
     */
    public QueryCost estimateFullCost(Query query, long luceneCost) {
        String queryType = query.getClass().getSimpleName();

        // Find matching cost multipliers
        CostMultipliers multipliers = null;
        for (Map.Entry<String, CostMultipliers> entry : QUERY_COST_MULTIPLIERS.entrySet()) {
            if (queryType.contains(entry.getKey())) {
                multipliers = entry.getValue();
                break;
            }
        }

        // Use defaults if no specific multipliers found
        double cpuMultiplier = multipliers != null ? multipliers.cpu : 1.0;
        double memoryMultiplier = multipliers != null ? multipliers.memory : 1.0;
        double ioMultiplier = multipliers != null ? multipliers.io : 1.0;

        // Base costs
        double cpuCost = 0.001 * cpuMultiplier;
        long memoryCost = (long) (luceneCost * 8 * memoryMultiplier);
        double ioCost = 0.01 * ioMultiplier;

        return new QueryCost(luceneCost, cpuCost, memoryCost, ioCost, true);
    }
}
