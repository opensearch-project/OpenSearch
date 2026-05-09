/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.opensearch.analytics.spi.AggregateFunction.APPROX_COUNT_DISTINCT;
import static org.opensearch.analytics.spi.AggregateFunction.AVG;
import static org.opensearch.analytics.spi.AggregateFunction.COUNT;
import static org.opensearch.analytics.spi.AggregateFunction.MAX;
import static org.opensearch.analytics.spi.AggregateFunction.MIN;
import static org.opensearch.analytics.spi.AggregateFunction.SUM;

public class AggregateFunctionTests extends OpenSearchTestCase {

    // ── SUM: pass-through, no decomposition ──

    public void testSumHasNoDecomposition() {
        assertFalse(SUM.hasDecomposition());
        assertFalse(SUM.hasScalarFinal());
        assertFalse(SUM.hasBinaryIntermediate());
        assertNull(SUM.intermediateFields());
        assertNull(SUM.finalExpression());
    }

    // ── COUNT: function-swap (single field, reducer != self) ──

    public void testCountHasDecomposition() {
        assertTrue(COUNT.hasDecomposition());
        assertFalse(COUNT.hasScalarFinal());
        assertFalse(COUNT.hasBinaryIntermediate());
    }

    public void testCountIntermediateFields() {
        List<AggregateFunction.IntermediateField> fields = COUNT.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("count", fields.get(0).name());
        assertSame(SUM, fields.get(0).reducer());
        assertTrue(fields.get(0).arrowType() instanceof ArrowType.Int);
        assertEquals(64, ((ArrowType.Int) fields.get(0).arrowType()).getBitWidth());
    }

    // ── AVG: primitive decomposition (multi-field + scalar final) ──

    public void testAvgHasDecomposition() {
        assertTrue(AVG.hasDecomposition());
        assertTrue(AVG.hasScalarFinal());
        assertFalse(AVG.hasBinaryIntermediate());
    }

    public void testAvgIntermediateFields() {
        List<AggregateFunction.IntermediateField> fields = AVG.intermediateFields();
        assertEquals(2, fields.size());
        assertEquals("count", fields.get(0).name());
        assertSame(SUM, fields.get(0).reducer());
        assertEquals("sum", fields.get(1).name());
        assertSame(SUM, fields.get(1).reducer());
        assertTrue(fields.get(1).arrowType() instanceof ArrowType.FloatingPoint);
    }

    public void testAvgFinalExpressionProducesDivide() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rb = new RexBuilder(typeFactory);
        RexNode ref0 = new RexInputRef(0, typeFactory.createSqlType(SqlTypeName.BIGINT));
        RexNode ref1 = new RexInputRef(1, typeFactory.createSqlType(SqlTypeName.DOUBLE));

        RexNode result = AVG.finalExpression().apply(rb, List.of(ref0, ref1));
        assertTrue(result.isA(SqlKind.DIVIDE));
    }

    // ── APPROX_COUNT_DISTINCT: engine-native (single binary field, reducer == self) ──

    public void testApproxCountDistinctHasDecomposition() {
        assertTrue(APPROX_COUNT_DISTINCT.hasDecomposition());
        assertFalse(APPROX_COUNT_DISTINCT.hasScalarFinal());
        assertTrue(APPROX_COUNT_DISTINCT.hasBinaryIntermediate());
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
