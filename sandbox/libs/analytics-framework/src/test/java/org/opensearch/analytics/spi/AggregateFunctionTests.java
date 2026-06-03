/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.opensearch.analytics.spi.AggregateFunction.APPROX_COUNT_DISTINCT;
import static org.opensearch.analytics.spi.AggregateFunction.AVG;
import static org.opensearch.analytics.spi.AggregateFunction.COUNT;
import static org.opensearch.analytics.spi.AggregateFunction.FIRST;
import static org.opensearch.analytics.spi.AggregateFunction.LAST;
import static org.opensearch.analytics.spi.AggregateFunction.LIST;
import static org.opensearch.analytics.spi.AggregateFunction.MAX;
import static org.opensearch.analytics.spi.AggregateFunction.MIN;
import static org.opensearch.analytics.spi.AggregateFunction.SUM;
import static org.opensearch.analytics.spi.AggregateFunction.TAKE;
import static org.opensearch.analytics.spi.AggregateFunction.VALUES;

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

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);

    private RelDataType resolve(AggregateFunction.IntermediateField field, RelDataType arg0) {
        return field.typeResolver().resolve(List.of(arg0), typeFactory);
    }

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
        assertEquals(SqlTypeName.BIGINT, resolve(fields.get(0), integer).getSqlTypeName());
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
        assertEquals(SqlTypeName.VARBINARY, resolve(fields.get(0), integer).getSqlTypeName());
    }

    // ── TAKE: engine-native (single field, reducer == self, parameterised resolver) ──

    public void testTakeHasDecomposition() {
        assertTrue(TAKE.hasDecomposition());
    }

    public void testTakeReducerIsSelfAndResolverIsParameterised() {
        List<AggregateFunction.IntermediateField> fields = TAKE.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("take_state", fields.get(0).name());
        assertSame(TAKE, fields.get(0).reducer());
        assertEquals("passThroughArg0 returns arg0", integer, resolve(fields.get(0), integer));
    }

    // ── FIRST: engine-native (single field, reducer == self, parameterised resolver) ──

    public void testFirstHasDecomposition() {
        assertTrue(FIRST.hasDecomposition());
    }

    public void testFirstReducerIsSelf() {
        List<AggregateFunction.IntermediateField> fields = FIRST.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("first_state", fields.get(0).name());
        assertSame(FIRST, fields.get(0).reducer());
        assertEquals(integer, resolve(fields.get(0), integer));
    }

    // ── LAST: engine-native (single field, reducer == self, parameterised resolver) ──

    public void testLastHasDecomposition() {
        assertTrue(LAST.hasDecomposition());
    }

    public void testLastReducerIsSelf() {
        List<AggregateFunction.IntermediateField> fields = LAST.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("last_state", fields.get(0).name());
        assertSame(LAST, fields.get(0).reducer());
        assertEquals(integer, resolve(fields.get(0), integer));
    }

    // ── LIST: engine-native (single field, reducer == self, parameterised resolver) ──

    public void testListHasDecomposition() {
        assertTrue(LIST.hasDecomposition());
    }

    public void testListReducerIsSelf() {
        List<AggregateFunction.IntermediateField> fields = LIST.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("list_state", fields.get(0).name());
        assertSame(LIST, fields.get(0).reducer());
        assertEquals(integer, resolve(fields.get(0), integer));
    }

    // ── VALUES: engine-native (single field, reducer == self, parameterised resolver) ──

    public void testValuesHasDecomposition() {
        assertTrue(VALUES.hasDecomposition());
    }

    public void testValuesReducerIsSelf() {
        List<AggregateFunction.IntermediateField> fields = VALUES.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("values_state", fields.get(0).name());
        assertSame(VALUES, fields.get(0).reducer());
        assertEquals(integer, resolve(fields.get(0), integer));
    }

    // ── PERCENTILE_APPROX: state-bearing, no decomposition declared ──
    //
    // DataFusion's t-digest state is multi-field and doesn't fit the single-field
    // IntermediateField shape. OpenSearchAggregateSplitRule skips the partial/final split
    // for STATE_EXPANDING aggregates, so PERCENTILE_APPROX runs single-stage on the
    // coordinator after a singleton gather.
    public void testPercentileApproxHasNoDecomposition() {
        assertFalse(AggregateFunction.PERCENTILE_APPROX.hasDecomposition());
        assertNull(AggregateFunction.PERCENTILE_APPROX.intermediateFields());
        assertEquals(AggregateFunction.Type.STATE_EXPANDING, AggregateFunction.PERCENTILE_APPROX.getType());
        assertEquals(SqlKind.OTHER, AggregateFunction.PERCENTILE_APPROX.getSqlKind());
    }

    public void testPercentileApproxResolvesByName() {
        assertSame(AggregateFunction.PERCENTILE_APPROX, AggregateFunction.fromNameOrError("percentile_approx"));
        assertSame(AggregateFunction.PERCENTILE_APPROX, AggregateFunction.fromNameOrError("PERCENTILE_APPROX"));
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
