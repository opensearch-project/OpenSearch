/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner.nodes;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.opensearch.search.planner.QueryCost;
import org.opensearch.search.planner.QueryNodeType;
import org.opensearch.search.planner.QueryPlanNode;
import org.opensearch.search.planner.QueryPlanProfile;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BooleanPlanNodeTests extends OpenSearchTestCase {

    public void testBooleanPlanNode() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value")), BooleanClause.Occur.MUST);
        BooleanQuery query = builder.build();

        TermPlanNode mustNode = new TermPlanNode(new TermQuery(new Term("field", "value")), "field", "value", 100);

        BooleanPlanNode node = new BooleanPlanNode(
            query,
            Arrays.asList(mustNode),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            0
        );

        assertEquals(QueryNodeType.BOOLEAN, node.getType());
        assertEquals(1, node.getMustClauses().size());
        assertEquals(0, node.getFilterClauses().size());
        assertEquals(0, node.getShouldClauses().size());
        assertEquals(0, node.getMustNotClauses().size());
        assertEquals(1, node.getChildren().size());
    }

    public void testCostCalculationSimple() {
        BooleanQuery query = new BooleanQuery.Builder().build();

        TermPlanNode term1 = new TermPlanNode(new TermQuery(new Term("field1", "value1")), "field1", "value1", 100);
        TermPlanNode term2 = new TermPlanNode(new TermQuery(new Term("field2", "value2")), "field2", "value2", 200);

        BooleanPlanNode node = new BooleanPlanNode(
            query,
            Arrays.asList(term1),
            Arrays.asList(term2),
            Collections.emptyList(),
            Collections.emptyList(),
            0
        );

        QueryCost cost = node.getEstimatedCost();
        assertNotNull(cost);

        // Should combine costs from both terms plus coordination overhead
        assertTrue(cost.getLuceneCost() >= 300); // At least sum of children
        assertTrue(cost.getCpuCost() > 0.1); // Has coordination overhead
    }

    public void testCostWithMustNot() {
        BooleanQuery query = new BooleanQuery.Builder().build();

        TermPlanNode mustNode = new TermPlanNode(new TermQuery(new Term("field", "value")), "field", "value", 1000);
        TermPlanNode mustNotNode = new TermPlanNode(new TermQuery(new Term("field", "excluded")), "field", "excluded", 100);

        BooleanPlanNode node = new BooleanPlanNode(
            query,
            Arrays.asList(mustNode),
            Collections.emptyList(),
            Collections.emptyList(),
            Arrays.asList(mustNotNode),
            0
        );

        QueryCost cost = node.getEstimatedCost();

        // Must not clauses add extra CPU cost
        assertTrue(cost.getCpuCost() > mustNode.getEstimatedCost().getCpuCost());
    }

    public void testProfile() {
        BooleanQuery query = new BooleanQuery.Builder().build();

        List<QueryPlanNode> must = Arrays.asList(createDummyNode());
        List<QueryPlanNode> filter = Arrays.asList(createDummyNode(), createDummyNode());
        List<QueryPlanNode> should = Arrays.asList(createDummyNode());
        List<QueryPlanNode> mustNot = Collections.emptyList();

        BooleanPlanNode node = new BooleanPlanNode(query, must, filter, should, mustNot, 1);

        QueryPlanProfile profile = node.getProfile();
        assertEquals(1, profile.getAttributes().get("must_clauses"));
        assertEquals(2, profile.getAttributes().get("filter_clauses"));
        assertEquals(1, profile.getAttributes().get("should_clauses"));
        assertEquals(0, profile.getAttributes().get("must_not_clauses"));
        assertEquals(1, profile.getAttributes().get("minimum_should_match"));
    }

    private QueryPlanNode createDummyNode() {
        return new TermPlanNode(new TermQuery(new Term("dummy", "value")), "dummy", "value", 100);
    }

    public void testMustNotPenaltyIsBounded() {
        BooleanQuery query = new BooleanQuery.Builder().build();

        // Create a must clause with large doc count
        TermPlanNode mustNode = new TermPlanNode(
            new TermQuery(new Term("field", "value")),
            "field",
            "value",
            1_000_000  // 1 million docs
        );

        TermPlanNode mustNotNode = new TermPlanNode(new TermQuery(new Term("field", "excluded")), "field", "excluded", 100);

        // First get cost without must_not
        BooleanPlanNode nodeWithoutMustNot = new BooleanPlanNode(
            query,
            Arrays.asList(mustNode),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            0
        );

        double cpuWithoutMustNot = nodeWithoutMustNot.getEstimatedCost().getCpuCost();

        // Now with must_not
        BooleanPlanNode nodeWithMustNot = new BooleanPlanNode(
            query,
            Arrays.asList(mustNode),
            Collections.emptyList(),
            Collections.emptyList(),
            Arrays.asList(mustNotNode),
            0
        );

        double cpuWithMustNot = nodeWithMustNot.getEstimatedCost().getCpuCost();

        // The CPU increase should be bounded (based on MUST_NOT_CPU_FACTOR and clause count)
        double cpuIncrease = cpuWithMustNot - cpuWithoutMustNot;
        assertTrue(
            "Must not penalty should be bounded: " + cpuIncrease + " (with=" + cpuWithMustNot + ", without=" + cpuWithoutMustNot + ")",
            cpuIncrease <= 0.4  // Accounts for factor (0.3) + overhead
        );
        assertTrue("Must not should add some CPU cost", cpuIncrease > 0);
    }
}
