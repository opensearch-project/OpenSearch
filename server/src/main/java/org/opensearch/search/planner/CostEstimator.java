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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Estimates query costs using Lucene's scorer costs and OpenSearch heuristics.
 * @opensearch.internal
 */
public class CostEstimator {

    private static final Logger logger = LogManager.getLogger(CostEstimator.class);

    private final QueryShardContext queryShardContext;
    private final IndexSearcher searcher;
    private final IndexReader reader;

    // Cost multipliers for different query types
    private static final Map<String, CostMultipliers> QUERY_COST_MULTIPLIERS = new HashMap<>();

    // Fallback cost fractions when actual costs can't be computed
    private static final double FALLBACK_TERM_FRACTION = 0.008;  // ~0.8% of docs for medium selectivity term
    private static final double FALLBACK_RANGE_FRACTION = 0.12;   // Ranges typically match more docs
    private static final double FALLBACK_WILDCARD_FRACTION = 0.18; // Wildcards are expensive
    private static final double FALLBACK_FUZZY_FRACTION = 0.35;    // Fuzzy matches are very expensive
    private static final double FALLBACK_DEFAULT_FRACTION = 0.05;  // Conservative default

    static {
        // Initialize cost multipliers based on query complexity
        QUERY_COST_MULTIPLIERS.put("Script", new CostMultipliers(12.5, 2.3, 1.0));
        QUERY_COST_MULTIPLIERS.put("Fuzzy", new CostMultipliers(4.8, 3.2, 1.1));
        QUERY_COST_MULTIPLIERS.put("Regexp", new CostMultipliers(5.2, 2.8, 1.0));
        QUERY_COST_MULTIPLIERS.put("Wildcard", new CostMultipliers(2.3, 1.0, 1.8));
        QUERY_COST_MULTIPLIERS.put("Prefix", new CostMultipliers(1.7, 1.0, 1.6));
        QUERY_COST_MULTIPLIERS.put("Range", new CostMultipliers(1.0, 1.0, 1.4));
        QUERY_COST_MULTIPLIERS.put("Nested", new CostMultipliers(2.1, 2.2, 1.0));
        QUERY_COST_MULTIPLIERS.put("FunctionScore", new CostMultipliers(3.5, 1.2, 1.0));
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
            logger.trace("No reader available for term frequency estimation, using default");
            return 100;
        }

        try {
            Term term = new Term(field, value);
            long docFreq = reader.docFreq(term);
            logger.trace("Term frequency for field '{}' value '{}': {}", field, value, docFreq);
            return docFreq;
        } catch (IOException e) {
            logger.debug("Error estimating term frequency for field '{}': {}", field, e.getMessage());
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
            logger.trace("No searcher available, using fallback cost estimation");
            return estimateFallbackCost(query);
        }

        try {
            // Create weight to get scorer and cost
            Weight weight = searcher.createWeight(searcher.rewrite(query), org.apache.lucene.search.ScoreMode.COMPLETE, 1);

            // Aggregate costs across all leaves
            if (reader != null && !reader.leaves().isEmpty()) {
                long totalCost = 0;
                int leafCount = 0;
                for (LeafReaderContext leafContext : reader.leaves()) {
                    Scorer scorer = weight.scorer(leafContext);
                    if (scorer != null) {
                        long leafCost = scorer.iterator().cost();
                        totalCost += leafCost;
                        leafCount++;
                    }
                }
                if (totalCost > 0) {
                    logger.trace("Lucene cost for {} across {} leaves: {}", query.getClass().getSimpleName(), leafCount, totalCost);
                    return totalCost;
                }
            }
        } catch (IOException e) {
            logger.debug("Error estimating Lucene cost for query {}: {}", query.getClass().getSimpleName(), e.getMessage());
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
            return (long) (totalDocs * FALLBACK_TERM_FRACTION);
        } else if (queryType.contains("Range")) {
            return (long) (totalDocs * FALLBACK_RANGE_FRACTION);
        } else if (queryType.contains("Wildcard") || queryType.contains("Prefix")) {
            return (long) (totalDocs * FALLBACK_WILDCARD_FRACTION);
        } else if (queryType.contains("Fuzzy") || queryType.contains("Regexp")) {
            return (long) (totalDocs * FALLBACK_FUZZY_FRACTION);
        }

        return (long) (totalDocs * FALLBACK_DEFAULT_FRACTION);
    }

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
