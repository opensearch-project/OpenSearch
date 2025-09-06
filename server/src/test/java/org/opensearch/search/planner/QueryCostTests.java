/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

public class QueryCostTests extends OpenSearchTestCase {

    public void testFromLucene() {
        QueryCost cost = QueryCost.fromLucene(1000);
        assertEquals(1000, cost.getLuceneCost());
        assertEquals(0.0, cost.getCpuCost(), 0.001);
        assertEquals(0, cost.getMemoryCost());
        assertEquals(0.0, cost.getIoCost(), 0.001);
        assertTrue(cost.isEstimate());
    }

    public void testTotalCost() {
        QueryCost cost = new QueryCost(1000, 0.5, 2048, 0.1, true);
        // Total = 1000 + (0.5 * 1000) + (2048 / 1024.0) + (0.1 * 100)
        // Total = 1000 + 500 + 2 + 10 = 1512
        assertEquals(1512.0, cost.getTotalCost(), 0.001);
    }

    public void testCombine() {
        List<QueryCost> costs = Arrays.asList(
            new QueryCost(100, 0.1, 1024, 0.05, true),
            new QueryCost(200, 0.2, 2048, 0.10, true),
            new QueryCost(300, 0.3, 4096, 0.15, true)
        );

        QueryCost combined = QueryCost.combine(costs, 0.5);

        assertEquals(600, combined.getLuceneCost()); // 100 + 200 + 300
        assertEquals(1.1, combined.getCpuCost(), 0.001); // 0.5 + 0.1 + 0.2 + 0.3
        assertEquals(7168, combined.getMemoryCost()); // 1024 + 2048 + 4096
        assertEquals(0.3, combined.getIoCost(), 0.001); // 0.05 + 0.10 + 0.15
        assertTrue(combined.isEstimate());
    }

    public void testCombineEmpty() {
        List<QueryCost> costs = Arrays.asList();
        QueryCost combined = QueryCost.combine(costs, 0.1);

        assertEquals(0, combined.getLuceneCost());
        assertEquals(0.1, combined.getCpuCost(), 0.001);
        assertEquals(0, combined.getMemoryCost());
        assertEquals(0.0, combined.getIoCost(), 0.001);
    }

    public void testToString() {
        QueryCost cost = new QueryCost(1000, 0.5, 2048, 0.1, false);
        String str = cost.toString();
        assertTrue(str.contains("lucene=1000"));
        assertTrue(str.contains("cpu=0.50"));
        assertTrue(str.contains("memory=2048"));
        assertTrue(str.contains("io=0.10"));
        assertTrue(str.contains("estimate=false"));
    }
}
