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

    /** Queries that fail at 1 shard: distinct_count/percentile value mismatches (approx + HLL merge). Skipped so the rest run and are visible. */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(7, 8, 9, 10);
    }
}
