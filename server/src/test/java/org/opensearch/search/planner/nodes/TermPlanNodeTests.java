/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner.nodes;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.opensearch.search.planner.QueryCost;
import org.opensearch.search.planner.QueryNodeType;
import org.opensearch.search.planner.QueryPlanProfile;
import org.opensearch.test.OpenSearchTestCase;

public class TermPlanNodeTests extends OpenSearchTestCase {

    public void testTermPlanNode() {
        TermQuery query = new TermQuery(new Term("field", "value"));
        TermPlanNode node = new TermPlanNode(query, "field", "value", 100);

        assertEquals(QueryNodeType.TERM, node.getType());
        assertEquals("field", node.getField());
        assertEquals("value", node.getValue());
        assertEquals(query, node.getLuceneQuery());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testCostCalculation() {
        TermQuery query = new TermQuery(new Term("field", "value"));
        TermPlanNode node = new TermPlanNode(query, "field", "value", 100);

        QueryCost cost = node.getEstimatedCost();
        assertNotNull(cost);
        assertEquals(100, cost.getLuceneCost());
        assertEquals(0.001, cost.getCpuCost(), 0.0001); // Very low CPU cost
        assertEquals(800, cost.getMemoryCost()); // 100 * 8 bytes
        assertEquals(0.01, cost.getIoCost(), 0.0001); // Low I/O cost
        assertTrue(cost.isEstimate());
    }

    public void testProfile() {
        TermQuery query = new TermQuery(new Term("status", "active"));
        TermPlanNode node = new TermPlanNode(query, "status", "active", 500);

        QueryPlanProfile profile = node.getProfile();
        assertNotNull(profile);
        assertEquals("TERM", profile.getQueryType());
        assertEquals("status", profile.getAttributes().get("field"));
        assertEquals("active", profile.getAttributes().get("value"));
        assertEquals(500L, profile.getAttributes().get("estimated_doc_frequency"));
    }

    public void testToString() {
        TermQuery query = new TermQuery(new Term("field", "value"));
        TermPlanNode node = new TermPlanNode(query, "field", "value", 100);

        String str = node.toString();
        assertTrue(str.contains("TERM"));
        assertTrue(str.contains("TermQuery"));
        assertTrue(str.contains("cost="));
    }
}
