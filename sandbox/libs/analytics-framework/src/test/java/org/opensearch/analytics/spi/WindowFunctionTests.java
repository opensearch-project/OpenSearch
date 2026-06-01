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
 *
 * <p>The UDAF-by-name branch of {@link WindowFunction#resolveFunction} (e.g. {@code DISTINCT_COUNT_APPROX}
 * which shares {@link SqlKind#OTHER_FUNCTION} with other UDAFs) is exercised indirectly by
 * {@code WindowPlanShapeTests} — building a real {@link org.apache.calcite.rex.RexOver} here would
 * pull in Calcite cluster / RexBuilder fixtures that this lightweight unit test deliberately avoids.
 */
public class WindowFunctionTests extends OpenSearchTestCase {

    public void testAggregateOverKinds() {
        assertEquals(WindowFunction.SUM, WindowFunction.fromSqlKind(SqlKind.SUM));
        assertEquals(WindowFunction.COUNT, WindowFunction.fromSqlKind(SqlKind.COUNT));
        assertEquals(WindowFunction.AVG, WindowFunction.fromSqlKind(SqlKind.AVG));
        assertEquals(WindowFunction.MIN, WindowFunction.fromSqlKind(SqlKind.MIN));
        assertEquals(WindowFunction.MAX, WindowFunction.fromSqlKind(SqlKind.MAX));
        assertEquals(WindowFunction.ARG_MIN, WindowFunction.fromSqlKind(SqlKind.ARG_MIN));
        assertEquals(WindowFunction.ARG_MAX, WindowFunction.fromSqlKind(SqlKind.ARG_MAX));
        assertEquals(WindowFunction.ROW_NUMBER, WindowFunction.fromSqlKind(SqlKind.ROW_NUMBER));
        assertEquals(WindowFunction.NTH_VALUE, WindowFunction.fromSqlKind(SqlKind.NTH_VALUE));
    }

    public void testFromSqlKindRefusesAmbiguousOtherFunction() {
        // Multiple UDAFs share SqlKind.OTHER_FUNCTION (DISTINCT_COUNT_APPROX is one of them).
        // fromSqlKind cannot pick a winner from kind alone — callers must use resolveFunction.
        assertNull(WindowFunction.fromSqlKind(SqlKind.OTHER_FUNCTION));
    }

    public void testResolveFunctionFallsBackToNameForUdaf() {
        SqlOperator op = Mockito.mock(SqlOperator.class);
        Mockito.when(op.getKind()).thenReturn(SqlKind.OTHER_FUNCTION);
        Mockito.when(op.getName()).thenReturn("DISTINCT_COUNT_APPROX");
        assertEquals(WindowFunction.DISTINCT_COUNT_APPROX, WindowFunction.resolveFunction(op));
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
