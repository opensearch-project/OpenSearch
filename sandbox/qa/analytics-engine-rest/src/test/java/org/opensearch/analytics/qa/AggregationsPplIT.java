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
}
