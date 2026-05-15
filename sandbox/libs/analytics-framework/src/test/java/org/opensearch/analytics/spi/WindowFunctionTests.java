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
 */
public class WindowFunctionTests extends OpenSearchTestCase {

    public void testAggregateOverKinds() {
        assertEquals(WindowFunction.SUM, WindowFunction.fromSqlKind(SqlKind.SUM));
        assertEquals(WindowFunction.COUNT, WindowFunction.fromSqlKind(SqlKind.COUNT));
        assertEquals(WindowFunction.AVG, WindowFunction.fromSqlKind(SqlKind.AVG));
        assertEquals(WindowFunction.MIN, WindowFunction.fromSqlKind(SqlKind.MIN));
        assertEquals(WindowFunction.MAX, WindowFunction.fromSqlKind(SqlKind.MAX));
    }

}
