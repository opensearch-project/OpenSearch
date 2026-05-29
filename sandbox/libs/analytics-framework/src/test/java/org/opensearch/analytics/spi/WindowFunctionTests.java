/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.opensearch.test.OpenSearchTestCase;

import org.mockito.Mockito;

/**
 * Asserts each {@link WindowFunction} entry maps to its Calcite {@link SqlKind} and
 * {@link WindowFunction#fromSqlKind} round-trips that mapping.
 */
public class WindowFunctionTests extends OpenSearchTestCase {

    public void testAggregateOverKinds() {
        assertEquals(WindowFunction.SUM, WindowFunction.fromSqlKind(SqlKind.SUM));
        assertEquals(WindowFunction.COUNT, WindowFunction.fromSqlKind(SqlKind.COUNT));
        assertEquals(WindowFunction.AVG, WindowFunction.fromSqlKind(SqlKind.AVG));
        assertEquals(WindowFunction.MIN, WindowFunction.fromSqlKind(SqlKind.MIN));
        assertEquals(WindowFunction.MAX, WindowFunction.fromSqlKind(SqlKind.MAX));
    }

    public void testResolveFunctionUsesSqlKindWhenAvailable() {
        SqlOperator op = Mockito.mock(SqlOperator.class);
        Mockito.when(op.getKind()).thenReturn(SqlKind.SUM);
        assertEquals(WindowFunction.SUM, WindowFunction.resolveFunction(op));
    }

    public void testResolveFunctionFallsBackToNameForOtherKind() {
        SqlOperator op = Mockito.mock(SqlOperator.class);
        Mockito.when(op.getKind()).thenReturn(SqlKind.OTHER);
        Mockito.when(op.getName()).thenReturn("pattern");
        assertEquals(WindowFunction.PATTERN, WindowFunction.resolveFunction(op));
    }

    public void testResolveFunctionReturnsNullForUnknownOperator() {
        SqlOperator op = Mockito.mock(SqlOperator.class);
        Mockito.when(op.getKind()).thenReturn(SqlKind.OTHER);
        Mockito.when(op.getName()).thenReturn("not_a_window_function");
        assertNull(WindowFunction.resolveFunction(op));
    }
}
