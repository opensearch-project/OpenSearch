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
import org.opensearch.search.planner.nodes.GenericPlanNode;
import org.opensearch.search.planner.nodes.MatchAllPlanNode;
import org.opensearch.search.planner.nodes.TermPlanNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class QueryPlanVisualizerTests extends OpenSearchTestCase {

    public void testToString() {
        Query query = new MatchAllDocsQuery();
        QueryPlanNode node = new MatchAllPlanNode(query, 10000);

        String visualization = QueryPlanVisualizer.toString(node);
        assertNotNull(visualization);
        assertTrue(visualization.contains("MATCH_ALL"));
        assertTrue(visualization.contains("cost="));
    }

    public void testToStringWithChildren() {
        // Create a simple tree structure
        Query query = new MatchAllDocsQuery();
        TermPlanNode child1 = new TermPlanNode(query, "field1", "value1", 100);
        TermPlanNode child2 = new TermPlanNode(query, "field2", "value2", 200);

        QueryPlanNode root = new GenericPlanNode(query, QueryNodeType.BOOLEAN, 300) {
            @Override
            public List<QueryPlanNode> getChildren() {
                return List.of(child1, child2);
            }
        };

        String visualization = QueryPlanVisualizer.toString(root);
        assertTrue(visualization.contains("BOOLEAN"));
        assertTrue(visualization.contains("TERM"));
        assertTrue(visualization.contains("field1"));
        assertTrue(visualization.contains("field2"));
        // Check tree structure
        assertTrue(visualization.contains("├──"));
        assertTrue(visualization.contains("└──"));
    }

    public void testToMap() {
        Query query = new MatchAllDocsQuery();
        QueryPlanNode node = new MatchAllPlanNode(query, 10000);

        Map<String, Object> map = QueryPlanVisualizer.toMap(node);
        assertNotNull(map);
        assertEquals("MATCH_ALL", map.get("type"));
        assertNotNull(map.get("cost"));

        @SuppressWarnings("unchecked")
        Map<String, Object> costMap = (Map<String, Object>) map.get("cost");
        assertNotNull(costMap.get("total"));
        assertEquals("lucene_docs", costMap.containsKey("lucene_docs"), true);
        assertEquals("cpu", costMap.containsKey("cpu"), true);
        assertEquals("memory_bytes", costMap.containsKey("memory_bytes"), true);
        assertEquals("io", costMap.containsKey("io"), true);
        assertEquals("total", costMap.containsKey("total"), true);
        assertEquals("is_estimate", costMap.containsKey("is_estimate"), true);
    }

    public void testAnalyzePlan() {
        Query query = new MatchAllDocsQuery();

        // Test expensive operation detection
        GenericPlanNode expensiveNode = new GenericPlanNode(query, QueryNodeType.WILDCARD, 20000);
        List<String> suggestions = QueryPlanVisualizer.analyzePlan(expensiveNode);
        assertTrue(suggestions.stream().anyMatch(s -> s.contains("Expensive") && s.contains("WILDCARD")));

        // Test match_all in sub-query
        MatchAllPlanNode matchAllNode = new MatchAllPlanNode(query, 10000);
        GenericPlanNode parent = new GenericPlanNode(query, QueryNodeType.BOOLEAN, 10000) {
            @Override
            public List<QueryPlanNode> getChildren() {
                return List.of(matchAllNode);
            }
        };

        suggestions = QueryPlanVisualizer.analyzePlan(parent);
        assertTrue(suggestions.stream().anyMatch(s -> s.contains("match_all") && s.contains("sub-query")));
    }

    public void testAnalyzePlanEmpty() {
        Query query = new MatchAllDocsQuery();
        TermPlanNode node = new TermPlanNode(query, "field", "value", 100);

        List<String> suggestions = QueryPlanVisualizer.analyzePlan(node);
        assertTrue(suggestions.isEmpty()); // No issues with simple term query
    }
}
