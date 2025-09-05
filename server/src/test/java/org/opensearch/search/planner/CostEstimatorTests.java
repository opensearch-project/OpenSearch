/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class CostEstimatorTests extends OpenSearchTestCase {

    private CostEstimator estimator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        QueryShardContext context = mock(QueryShardContext.class);
        estimator = new CostEstimator(context);
    }

    public void testEstimateTermDocFrequency() {
        // Without reader, should return default
        long freq = estimator.estimateTermDocFrequency("field", "value");
        assertEquals(100, freq);
    }

    public void testGetTotalDocs() {
        // Without reader, should return default
        long total = estimator.getTotalDocs();
        assertEquals(10000, total);
    }

    public void testEstimateGenericCost() {
        Query query = new MatchAllDocsQuery();
        long cost = estimator.estimateGenericCost(query);
        assertEquals(10000, cost); // Should return total docs for match all
    }

    public void testEstimateFallbackCost() {
        // Test that generic cost estimation returns reasonable defaults
        Query query = new MatchAllDocsQuery();
        long cost = estimator.estimateGenericCost(query);
        assertTrue(cost > 0);

        // For match all, should return total docs
        assertEquals(estimator.getTotalDocs(), cost);
    }

    public void testEstimateFullCost() {
        Query query = new MatchAllDocsQuery();
        long luceneCost = 1000;

        QueryCost cost = estimator.estimateFullCost(query, luceneCost);

        assertEquals(luceneCost, cost.getLuceneCost());
        assertTrue(cost.getCpuCost() > 0);
        assertTrue(cost.getMemoryCost() > 0);
        assertTrue(cost.getIoCost() > 0);
        assertTrue(cost.isEstimate());
    }
}
