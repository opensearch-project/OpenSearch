/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Asserts each {@link WindowFunction} entry maps to its Calcite {@link SqlKind} and
 * {@link WindowFunction#fromSqlKind} round-trips that mapping.
 *
 * <p>The {@code COUNT_DISTINCT} branch of {@link WindowFunction#fromRexOver} is exercised
 * indirectly by {@code WindowPlanShapeTests} — building a real {@link org.apache.calcite.rex.RexOver}
 * here would pull in Calcite cluster / RexBuilder fixtures that this lightweight unit test
 * deliberately avoids.
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
    }

    public void testFromSqlKindRefusesAmbiguousOtherFunction() {
        // Multiple UDAFs share SqlKind.OTHER_FUNCTION (DISTINCT_COUNT_APPROX is one of them).
        // fromSqlKind cannot pick a winner from kind alone — callers must use fromRexOver.
        assertNull(WindowFunction.fromSqlKind(SqlKind.OTHER_FUNCTION));
    }

}
