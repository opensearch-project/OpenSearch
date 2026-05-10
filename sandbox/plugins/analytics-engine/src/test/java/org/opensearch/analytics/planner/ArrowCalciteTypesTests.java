/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowCalciteTypesTests extends OpenSearchTestCase {

    private final RelDataTypeFactory factory = new JavaTypeFactoryImpl();

    public void testRoundTripBigint() {
        ArrowType arrow = new ArrowType.Int(64, true);
        RelDataType calcite = ArrowCalciteTypes.toCalcite(arrow, factory);
        assertEquals(SqlTypeName.BIGINT, calcite.getSqlTypeName());
        assertEquals(arrow, ArrowCalciteTypes.toArrow(calcite));
    }

    public void testRoundTripInteger() {
        ArrowType arrow = new ArrowType.Int(32, true);
        RelDataType calcite = ArrowCalciteTypes.toCalcite(arrow, factory);
        assertEquals(SqlTypeName.INTEGER, calcite.getSqlTypeName());
        assertEquals(arrow, ArrowCalciteTypes.toArrow(calcite));
    }

    public void testRoundTripDouble() {
        ArrowType arrow = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        RelDataType calcite = ArrowCalciteTypes.toCalcite(arrow, factory);
        assertEquals(SqlTypeName.DOUBLE, calcite.getSqlTypeName());
        assertEquals(arrow, ArrowCalciteTypes.toArrow(calcite));
    }

    public void testRoundTripReal() {
        ArrowType arrow = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        RelDataType calcite = ArrowCalciteTypes.toCalcite(arrow, factory);
        assertEquals(SqlTypeName.REAL, calcite.getSqlTypeName());
        assertEquals(arrow, ArrowCalciteTypes.toArrow(calcite));
    }

    public void testRoundTripVarchar() {
        ArrowType arrow = ArrowType.Utf8.INSTANCE;
        RelDataType calcite = ArrowCalciteTypes.toCalcite(arrow, factory);
        assertEquals(SqlTypeName.VARCHAR, calcite.getSqlTypeName());
        // Calcite's JavaTypeFactoryImpl clamps precision to its internal max (65536).
        // We pass Integer.MAX_VALUE to request "unlimited"; the factory clamps to its max.
        // The invariant we care about is: precision is at the factory's maximum (i.e. unbounded VARCHAR).
        assertEquals(factory.getTypeSystem().getMaxPrecision(SqlTypeName.VARCHAR), calcite.getPrecision());
        assertEquals(arrow, ArrowCalciteTypes.toArrow(calcite));
    }

    public void testRoundTripVarbinary() {
        ArrowType arrow = ArrowType.Binary.INSTANCE;
        RelDataType calcite = ArrowCalciteTypes.toCalcite(arrow, factory);
        assertEquals(SqlTypeName.VARBINARY, calcite.getSqlTypeName());
        // Same rationale as testRoundTripVarchar — factory clamps precision to its own max.
        assertEquals(factory.getTypeSystem().getMaxPrecision(SqlTypeName.VARBINARY), calcite.getPrecision());
        assertEquals(arrow, ArrowCalciteTypes.toArrow(calcite));
    }

    public void testRoundTripBoolean() {
        ArrowType arrow = ArrowType.Bool.INSTANCE;
        RelDataType calcite = ArrowCalciteTypes.toCalcite(arrow, factory);
        assertEquals(SqlTypeName.BOOLEAN, calcite.getSqlTypeName());
        assertEquals(arrow, ArrowCalciteTypes.toArrow(calcite));
    }

    public void testUnsupportedArrowTypeThrows() {
        ArrowType date = new ArrowType.Date(DateUnit.DAY);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ArrowCalciteTypes.toCalcite(date, factory));
        assertTrue(e.getMessage().contains("Date"));
    }

    public void testUnsupportedArrowTypeTimeThrows() {
        ArrowType time = new ArrowType.Time(TimeUnit.MILLISECOND, 32);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ArrowCalciteTypes.toCalcite(time, factory));
        assertTrue(e.getMessage().contains("Time"));
    }

    public void testUnsupportedCalciteTypeThrows() {
        RelDataType timestamp = factory.createSqlType(SqlTypeName.TIMESTAMP);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ArrowCalciteTypes.toArrow(timestamp));
        assertTrue(e.getMessage().contains("TIMESTAMP"));
    }
}
