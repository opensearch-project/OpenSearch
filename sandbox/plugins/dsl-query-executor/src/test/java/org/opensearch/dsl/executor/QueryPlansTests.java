/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.calcite.rel.RelNode;
import org.opensearch.dsl.TestUtils;
import org.opensearch.test.OpenSearchTestCase;

public class QueryPlansTests extends OpenSearchTestCase {

    private RelNode relNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        relNode = TestUtils.createTestRelNode();
    }

    public void testBuilderCreatesSinglePlan() {
        QueryPlans plans = new QueryPlans.Builder()
            .add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, relNode))
            .build();

        assertEquals(1, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.AGGREGATION));
    }

    public void testBuilderCreatesMultiplePlans() {
        QueryPlans plans = new QueryPlans.Builder()
            .add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, relNode))
            .add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, relNode))
            .build();

        assertEquals(2, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        assertEquals(1, plans.get(QueryPlans.Type.HITS).size());
        assertEquals(1, plans.get(QueryPlans.Type.AGGREGATION).size());
    }

    public void testGetReturnsMultiplePlansOfSameType() {
        QueryPlans plans = new QueryPlans.Builder()
            .add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, relNode))
            .add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, relNode))
            .build();

        assertEquals(2, plans.get(QueryPlans.Type.AGGREGATION).size());
    }

    public void testBuilderThrowsOnEmpty() {
        expectThrows(IllegalStateException.class, () -> new QueryPlans.Builder().build());
    }

    public void testGetReturnsEmptyForMissingType() {
        QueryPlans plans = new QueryPlans.Builder()
            .add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, relNode))
            .build();

        assertTrue(plans.get(QueryPlans.Type.AGGREGATION).isEmpty());
    }

    public void testPlansAreImmutable() {
        QueryPlans plans = new QueryPlans.Builder()
            .add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, relNode))
            .build();

        expectThrows(UnsupportedOperationException.class,
            () -> plans.getAll().add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, relNode)));
    }

    public void testQueryPlanRejectsNullArguments() {
        expectThrows(NullPointerException.class, () -> new QueryPlans.QueryPlan(QueryPlans.Type.HITS, null));
        expectThrows(NullPointerException.class, () -> new QueryPlans.QueryPlan(null, relNode));
    }
}
