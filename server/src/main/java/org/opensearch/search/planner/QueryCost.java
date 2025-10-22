/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;
import java.util.Locale;

/**
 * Represents the estimated cost of executing a query.
 * Combines Lucene's document-based cost with additional factors.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class QueryCost {

    /**
     * Lucene's cost estimate (typically number of matching documents)
     */
    private final long luceneCost;

    /**
     * Additional CPU cost for complex operations (e.g., script evaluation, analysis)
     */
    private final double cpuCost;

    /**
     * Memory cost estimate in bytes
     */
    private final long memoryCost;

    /**
     * I/O cost factor (e.g., for range queries, wildcard queries)
     */
    private final double ioCost;

    /**
     * Whether this cost is an estimate or actual
     */
    private final boolean isEstimate;

    public QueryCost(long luceneCost, double cpuCost, long memoryCost, double ioCost, boolean isEstimate) {
        this.luceneCost = luceneCost;
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.ioCost = ioCost;
        this.isEstimate = isEstimate;
    }

    /**
     * Creates a cost based only on Lucene's estimate
     */
    public static QueryCost fromLucene(long luceneCost) {
        return new QueryCost(luceneCost, 0.0, 0, 0.0, true);
    }

    /**
     * Returns a combined cost score for comparison
     */
    public double getTotalCost() {
        // Weighted combination of different cost factors
        return luceneCost + (cpuCost * 1000) + (memoryCost / 1024.0) + (ioCost * 100);
    }

    public long getLuceneCost() {
        return luceneCost;
    }

    public double getCpuCost() {
        return cpuCost;
    }

    public long getMemoryCost() {
        return memoryCost;
    }

    public double getIoCost() {
        return ioCost;
    }

    public boolean isEstimate() {
        return isEstimate;
    }

    /**
     * Combines costs from multiple sources (e.g., for boolean queries)
     */
    public static QueryCost combine(List<QueryCost> costs, double coordinationOverhead) {
        long totalLucene = 0;
        double totalCpu = coordinationOverhead;
        long totalMemory = 0;
        double totalIo = 0;
        boolean allEstimates = true;

        for (QueryCost cost : costs) {
            totalLucene += cost.luceneCost;
            totalCpu += cost.cpuCost;
            totalMemory += cost.memoryCost;
            totalIo += cost.ioCost;
            allEstimates &= cost.isEstimate;
        }

        return new QueryCost(totalLucene, totalCpu, totalMemory, totalIo, allEstimates);
    }

    @Override
    public String toString() {
        return String.format(
            Locale.ROOT,
            "QueryCost[lucene=%d, cpu=%.2f, memory=%d, io=%.2f, estimate=%s]",
            luceneCost,
            cpuCost,
            memoryCost,
            ioCost,
            isEstimate
        );
    }
}
