/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.dsl.executor.QueryPlans;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Merged result of the request's COUNT plans: the totals that plan-level LIMITs remove from
 * the main plans.
 *
 * @param totalDocs total documents matching the query ({@code COUNT(*)}), or null when no
 *        count plan carried a total
 * @param eligibleDocCounts eligible-doc count per root aggregation name (the total {@code sum_other_doc_count} is subtracted from):
 *        the documents eligible for that aggregation's buckets
 */
public record CountTotals(Long totalDocs, Map<String, Long> eligibleDocCounts) {

    /**
     * Creates count totals.
     *
     * @param totalDocs total matching documents, or null
     * @param eligibleDocCounts eligible-document counts by root aggregation name
     */
    public CountTotals {
        eligibleDocCounts = Map.copyOf(eligibleDocCounts);
    }

    /** Returns the total matching documents, or null when no count plan carried a total. */
    @Override
    public Long totalDocs() {
        return totalDocs;
    }

    /** Returns the eligible-document counts by root aggregation name. */
    @Override
    public Map<String, Long> eligibleDocCounts() {
        return eligibleDocCounts;
    }

    /**
     * Parses and merges the COUNT plans' single result rows by the column-name contract
     * ({@link QueryPlans#COUNT_TOTAL_COLUMN}, {@link QueryPlans#COUNT_ELIGIBLE_COLUMN_PREFIX}).
     * A present-but-null eligible cell (a {@code SUM} over zero passing groups) counts as zero
     * eligible documents.
     *
     * @param countResults the COUNT plans' execution results
     * @return the merged totals
     */
    public static CountTotals from(List<ExecutionResult> countResults) {
        Long totalDocs = null;
        Map<String, Long> eligibleDocCounts = new HashMap<>();
        for (ExecutionResult countResult : countResults) {
            Iterator<Object[]> rows = countResult.getRows().iterator();
            if (!rows.hasNext()) {
                continue;
            }
            Object[] row = rows.next();
            List<String> columnNames = countResult.getFieldNames();
            for (int i = 0; i < columnNames.size() && i < row.length; i++) {
                String column = columnNames.get(i);
                if (QueryPlans.COUNT_TOTAL_COLUMN.equals(column)) {
                    if (row[i] instanceof Number count) {
                        totalDocs = count.longValue();
                    }
                } else if (column.startsWith(QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX)) {
                    String aggName = column.substring(QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX.length());
                    long count = row[i] instanceof Number number ? number.longValue() : 0L;
                    eligibleDocCounts.put(aggName, count);
                }
            }
        }
        return new CountTotals(totalDocs, eligibleDocCounts);
    }
}
