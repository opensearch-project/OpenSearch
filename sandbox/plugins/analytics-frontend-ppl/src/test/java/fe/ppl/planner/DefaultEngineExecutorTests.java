/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package fe.ppl.planner;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.opensearch.fe.ppl.planner.DefaultEngineExecutor;
import org.opensearch.plugins.QueryPlanExecutor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DefaultEngineExecutor}.
 */
public class DefaultEngineExecutorTests extends OpenSearchTestCase {

    public void testExecuteDelegatesToQueryExecutorWithCorrectArguments() {
        QueryPlanExecutor mockQueryPlanExecutor = mock(QueryPlanExecutor.class);
        RelNode mockRelNode = mock(RelNode.class);
        DataContext mockDataContext = mock(DataContext.class);

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "value1", 42 });
        when(mockQueryPlanExecutor.execute(mockRelNode, mockDataContext)).thenReturn(rows);

        DefaultEngineExecutor executor = new DefaultEngineExecutor(mockQueryPlanExecutor);
        executor.execute(mockRelNode, mockDataContext);

        verify(mockQueryPlanExecutor).execute(mockRelNode, mockDataContext);
    }

    public void testExecuteConvertsIterableToEnumerable() {
        QueryPlanExecutor mockQueryPlanExecutor = mock(QueryPlanExecutor.class);
        RelNode mockRelNode = mock(RelNode.class);
        DataContext mockDataContext = mock(DataContext.class);

        List<Object[]> rows = Arrays.asList(new Object[] { "a", 1 }, new Object[] { "b", 2 });
        when(mockQueryPlanExecutor.execute(mockRelNode, mockDataContext)).thenReturn(rows);

        DefaultEngineExecutor executor = new DefaultEngineExecutor(mockQueryPlanExecutor);
        Enumerable<Object[]> result = executor.execute(mockRelNode, mockDataContext);

        assertNotNull(result);
        List<Object[]> resultList = result.toList();
        assertEquals(2, resultList.size());
    }

    public void testRowDataIsPreservedThroughDelegation() {
        QueryPlanExecutor mockQueryPlanExecutor = mock(QueryPlanExecutor.class);
        RelNode mockRelNode = mock(RelNode.class);
        DataContext mockDataContext = mock(DataContext.class);

        Object[] row1 = new Object[] { "host-1", 200, 3.14 };
        Object[] row2 = new Object[] { "host-2", 404, 2.71 };
        List<Object[]> rows = Arrays.asList(row1, row2);
        when(mockQueryPlanExecutor.execute(mockRelNode, mockDataContext)).thenReturn(rows);

        DefaultEngineExecutor executor = new DefaultEngineExecutor(mockQueryPlanExecutor);
        Enumerable<Object[]> result = executor.execute(mockRelNode, mockDataContext);

        List<Object[]> resultList = result.toList();
        assertEquals(2, resultList.size());
        assertArrayEquals(row1, resultList.get(0));
        assertArrayEquals(row2, resultList.get(1));
    }

    public void testExecuteWithEmptyResult() {
        QueryPlanExecutor mockQueryPlanExecutor = mock(QueryPlanExecutor.class);
        RelNode mockRelNode = mock(RelNode.class);
        DataContext mockDataContext = mock(DataContext.class);

        when(mockQueryPlanExecutor.execute(mockRelNode, mockDataContext)).thenReturn(Collections.emptyList());

        DefaultEngineExecutor executor = new DefaultEngineExecutor(mockQueryPlanExecutor);
        Enumerable<Object[]> result = executor.execute(mockRelNode, mockDataContext);

        assertNotNull(result);
        assertEquals(0, result.toList().size());
    }
}
