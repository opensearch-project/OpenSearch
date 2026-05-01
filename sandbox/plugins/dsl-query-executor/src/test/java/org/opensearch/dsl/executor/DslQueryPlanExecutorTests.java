/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class DslQueryPlanExecutorTests extends OpenSearchTestCase {

    private LogicalTableScan scan;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        scan = TestUtils.createTestRelNode();
    }

    public void testExecuteDelegatesEachPlanToExecutor() {
        List<Object[]> expectedRows = List.<Object[]>of(new Object[] { "laptop", 1200 });

        DslQueryPlanExecutor executor = new DslQueryPlanExecutor((plan, ctx, listener) -> listener.onResponse(expectedRows));
        QueryPlans plans = new QueryPlans.Builder().add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, scan)).build();

        PlainActionFuture<List<ExecutionResult>> future = new PlainActionFuture<>();
        executor.execute(plans, future);
        List<ExecutionResult> results = future.actionGet();

        assertEquals(1, results.size());
        ExecutionResult result = results.get(0);
        assertSame(expectedRows, result.getRows());
        assertEquals(QueryPlans.Type.HITS, result.getType());
        assertNotNull(result.getPlan());
        assertSame(scan, result.getPlan().relNode());
        assertEquals(List.of("name", "price", "brand", "rating"), result.getFieldNames());
    }

    // TODO: add test with multiple plans (HITS + AGGREGATION) to verify iteration order
    // TODO: add test for executor failure
}
