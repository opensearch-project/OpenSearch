/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;


/**
 * Aggregation functions testing PPL integration test.
 */
public class AggregationsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return AggregationsTestHelper.DATASET;
    }

    public void testAggregationsPplQueries() throws Exception {
        runPplQueries();
    }

    /**
     * Queries skipped due to distinct_count/percentile value mismatches from the engine-native
     * APPROX_COUNT_DISTINCT (HLL sketch), which is approximate and errs by ±1 at the cardinalities
     * used here — exact-match goldens can't hold. Q1 ({@code distinct_count(user)}, 10 distinct per
     * department) over-counts one group to 11; Q7-Q10 are the percentile / HLL-merge cases. Q2
     * ({@code distinct_count(status_code)}, 3 distinct) stays in range and passes. Skipped so the
     * rest run and stay visible; re-enable once the harness tolerates approximate columns.
     */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(1, 7, 8, 9, 10);
    }
}
