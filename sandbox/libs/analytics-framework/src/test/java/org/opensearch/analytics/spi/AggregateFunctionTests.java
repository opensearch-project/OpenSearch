/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.opensearch.analytics.spi.AggregateFunction.APPROX_COUNT_DISTINCT;
import static org.opensearch.analytics.spi.AggregateFunction.AVG;
import static org.opensearch.analytics.spi.AggregateFunction.COUNT;
import static org.opensearch.analytics.spi.AggregateFunction.MAX;
import static org.opensearch.analytics.spi.AggregateFunction.MIN;
import static org.opensearch.analytics.spi.AggregateFunction.SUM;

/**
 * Asserts the enum carries the right shape per function for the resolver's three
 * single-field decomposition cases: pass-through (no intermediate), function-swap
 * (reducer ≠ self), engine-native merge (reducer == self, binary intermediate).
 *
 * <p>Multi-field / scalar-final shapes (AVG, STDDEV, VAR) are <em>not</em> encoded on
 * the enum — they're handled by {@code OpenSearchAggregateReduceRule} during HEP
 * marking using Calcite's {@code AggregateReduceFunctionsRule}. The enum entries for
 * those functions intentionally declare {@code intermediateFields == null} so that
 * the resolver's pass-through branch catches any post-reduction primitive calls.
 */
public class AggregateFunctionTests extends OpenSearchTestCase {

    // ── Pass-through: SUM / MIN / MAX ──

    public void testSumHasNoDecomposition() {
        assertFalse(SUM.hasDecomposition());
        assertNull(SUM.intermediateFields());
    }

    // ── COUNT: function-swap (single field, reducer != self) ──

    public void testCountHasDecomposition() {
        assertTrue(COUNT.hasDecomposition());
    }

    public void testCountIntermediateFields() {
        List<AggregateFunction.IntermediateField> fields = COUNT.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("count", fields.get(0).name());
        assertSame(SUM, fields.get(0).reducer());
        assertTrue(fields.get(0).arrowType() instanceof ArrowType.Int);
        assertEquals(64, ((ArrowType.Int) fields.get(0).arrowType()).getBitWidth());
    }

    // ── AVG / STDDEV / VAR: handled by Calcite's reduce rule — no enum metadata ──

    public void testAvgHasNoDecomposition() {
        // AVG decomposition is driven by OpenSearchAggregateReduceRule in HEP, not by the
        // enum. Enum declares no intermediate — post-reduction plan carries primitive SUM/
        // COUNT calls whose enum entries ARE decompositions (function-swap / pass-through).
        assertFalse(AVG.hasDecomposition());
        assertNull(AVG.intermediateFields());
    }

    // ── APPROX_COUNT_DISTINCT: engine-native (single binary field, reducer == self) ──

    public void testApproxCountDistinctHasDecomposition() {
        assertTrue(APPROX_COUNT_DISTINCT.hasDecomposition());
    }

    public void testApproxCountDistinctReducerIsSelf() {
        List<AggregateFunction.IntermediateField> fields = APPROX_COUNT_DISTINCT.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("sketch", fields.get(0).name());
        assertSame(APPROX_COUNT_DISTINCT, fields.get(0).reducer());
        assertTrue(fields.get(0).arrowType() instanceof ArrowType.Binary);
    }

    // ── fromSqlKind still works ──

    public void testFromSqlKindResolvesExistingEntries() {
        assertSame(SUM, AggregateFunction.fromSqlKind(SqlKind.SUM));
        assertSame(MIN, AggregateFunction.fromSqlKind(SqlKind.MIN));
        assertSame(MAX, AggregateFunction.fromSqlKind(SqlKind.MAX));
        assertSame(COUNT, AggregateFunction.fromSqlKind(SqlKind.COUNT));
        assertSame(AVG, AggregateFunction.fromSqlKind(SqlKind.AVG));
    }

    public void testFromSqlKindReturnsNullForOther() {
        assertNull(AggregateFunction.fromSqlKind(SqlKind.OTHER));
    }
}
