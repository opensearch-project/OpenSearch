/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ExecutionResultTests extends OpenSearchTestCase {

    public void testExecutionResultCarriesPlanAndRows() {
        QueryPlans.QueryPlan plan = new QueryPlans.QueryPlan(QueryPlans.Type.HITS, TestUtils.createTestRelNode());
        List<Object[]> rows = List.<Object[]>of(new Object[]{"laptop", 1200, "brandX", 4.5});
        ExecutionResult result = new ExecutionResult(plan, rows);

        assertSame(plan, result.getPlan());
        assertSame(rows, result.getRows());
        assertEquals(QueryPlans.Type.HITS, result.getType());
        assertEquals(List.of("name", "price", "brand", "rating"), result.getFieldNames());
    }

    public void testRejectsNullArguments() {
        QueryPlans.QueryPlan plan = new QueryPlans.QueryPlan(QueryPlans.Type.HITS, TestUtils.createTestRelNode());
        expectThrows(NullPointerException.class, () -> new ExecutionResult(null, List.of()));
        expectThrows(NullPointerException.class, () -> new ExecutionResult(plan, null));
    }
}
