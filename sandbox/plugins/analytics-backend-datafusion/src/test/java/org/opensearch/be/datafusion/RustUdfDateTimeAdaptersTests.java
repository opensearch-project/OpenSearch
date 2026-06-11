/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Tests for {@link RustUdfDateTimeAdapters}. Pins the {@code opensearch_extract}
 * rename and the TIME→VARCHAR coercion in {@link RustUdfDateTimeAdapters.ExtractAdapter}.
 */
public class RustUdfDateTimeAdaptersTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    /** Pins the rename: substrait core's enum-arg `extract` would crash isthmus if we shared the name. */
    public void testExtractOperatorNameAvoidsSubstraitCoreCollision() {
        assertEquals("opensearch_extract", RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP.getName());
    }

    /** {@code extract(<unit> FROM <TIME(p)>)} value operand must be CAST to VARCHAR (precision_time has no yaml overload). */
    public void testExtractAdapterCastsTimeOperandToVarchar() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType time9 = typeFactory.createSqlType(SqlTypeName.TIME, 9);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);

        RexNode unit = rexBuilder.makeLiteral("MINUTE", varchar, true);
        RexNode timeVal = rexBuilder.makeInputRef(time9, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP, List.of(unit, timeVal));

        RexNode adapted = new RustUdfDateTimeAdapters.ExtractAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall adaptedCall = (RexCall) adapted;
        assertEquals(unit, adaptedCall.getOperands().get(0));

        RexNode coercedValue = adaptedCall.getOperands().get(1);
        assertEquals(SqlTypeName.VARCHAR, coercedValue.getType().getSqlTypeName());
        assertTrue(coercedValue instanceof RexCall && ((RexCall) coercedValue).getKind() == SqlKind.CAST);
    }

    /**
     * {@code extract(YEAR FROM <TIME>)} must anchor the value to today's UTC date at plan time —
     * otherwise Arrow's TIME→1970-01-01 anchor leaks through and YEAR returns 1970.
     */
    public void testExtractAdapterPrependsTodayForDatePartOnTime() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType time9 = typeFactory.createSqlType(SqlTypeName.TIME, 9);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);

        RexNode unit = rexBuilder.makeLiteral("YEAR", varchar, true);
        RexNode timeVal = rexBuilder.makeInputRef(time9, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP, List.of(unit, timeVal));

        RexNode adapted = new RustUdfDateTimeAdapters.ExtractAdapter().adapt(original, List.of(), cluster);

        RexCall adaptedCall = (RexCall) adapted;
        RexNode coercedValue = adaptedCall.getOperands().get(1);
        assertTrue("expected CONCAT wrapping the TIME cast", coercedValue instanceof RexCall);
        RexCall concat = (RexCall) coercedValue;
        assertEquals(SqlStdOperatorTable.CONCAT, concat.getOperator());
        // First arg of the CONCAT is a literal "<today> " (UTC).
        RexNode prefix = concat.getOperands().get(0);
        assertTrue(prefix instanceof RexLiteral);
        String expected = LocalDate.now(ZoneOffset.UTC).toString() + " ";
        assertEquals(expected, ((RexLiteral) prefix).getValue2());
    }

    /** Pure time-part units (HOUR/MINUTE/SECOND/...) keep the existing TIME→VARCHAR cast path. */
    public void testExtractAdapterKeepsBareCastForTimePartOnTime() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType time9 = typeFactory.createSqlType(SqlTypeName.TIME, 9);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);

        RexNode unit = rexBuilder.makeLiteral("HOUR", varchar, true);
        RexNode timeVal = rexBuilder.makeInputRef(time9, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP, List.of(unit, timeVal));

        RexNode adapted = new RustUdfDateTimeAdapters.ExtractAdapter().adapt(original, List.of(), cluster);
        RexNode coercedValue = ((RexCall) adapted).getOperands().get(1);
        assertTrue(coercedValue instanceof RexCall && ((RexCall) coercedValue).getKind() == SqlKind.CAST);
    }

    /** Non-TIME operands (DATE / TIMESTAMP / VARCHAR) pass through unchanged. */
    public void testExtractAdapterDoesNotCoerceTimestampOperand() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType timestamp6 = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);

        RexNode unit = rexBuilder.makeLiteral("YEAR", varchar, true);
        RexNode tsVal = rexBuilder.makeInputRef(timestamp6, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP, List.of(unit, tsVal));

        RexNode adapted = new RustUdfDateTimeAdapters.ExtractAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall adaptedCall = (RexCall) adapted;
        assertSame(tsVal, adaptedCall.getOperands().get(1));
    }

    /** DAYNAME with a valid date literal lowers without throwing. */
    public void testDaynameAdapterAcceptsValidDateLiteral() {
        RexCall original = makeDateFormatCall("2020-08-26");
        RexNode adapted = new RustUdfDateTimeAdapters.DaynameAdapter().adapt(original, List.of(), cluster);
        assertTrue(adapted instanceof RexCall);
    }

    /** DAYNAME with an invalid date literal rejects at plan time with the format-hint message. */
    public void testDaynameAdapterRejectsInvalidDateLiteral() {
        RexCall original = makeDateFormatCall("2025-13-02");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RustUdfDateTimeAdapters.DaynameAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue(
            "expected unsupported-format hint, got: " + e.getMessage(),
            e.getMessage().contains("2025-13-02") && e.getMessage().contains("unsupported format")
        );
    }

    /** DAYNAME rejects out-of-range time-of-day in a full datetime literal. */
    public void testDaynameAdapterRejectsInvalidDatetimeLiteral() {
        RexCall original = makeDateFormatCall("2025-12-01 15:02:61");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RustUdfDateTimeAdapters.DaynameAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue(e.getMessage().contains("unsupported format"));
    }

    /** MONTHNAME with a valid date literal lowers without throwing. */
    public void testMonthnameAdapterAcceptsValidDateLiteral() {
        RexCall original = makeDateFormatCall("2020-08-26");
        RexNode adapted = new RustUdfDateTimeAdapters.MonthnameAdapter().adapt(original, List.of(), cluster);
        assertTrue(adapted instanceof RexCall);
    }

    /** MONTHNAME with an invalid date literal rejects at plan time with the format-hint message. */
    public void testMonthnameAdapterRejectsInvalidDateLiteral() {
        RexCall original = makeDateFormatCall("2025-13-02");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RustUdfDateTimeAdapters.MonthnameAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue(
            "expected unsupported-format hint, got: " + e.getMessage(),
            e.getMessage().contains("2025-13-02") && e.getMessage().contains("unsupported format")
        );
    }

    /** MONTHNAME rejects out-of-range time-of-day in a full datetime literal — symmetric with DAYNAME's invalid-datetime test. */
    public void testMonthnameAdapterRejectsInvalidDatetimeLiteral() {
        RexCall original = makeDateFormatCall("2025-12-01 15:02:61");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RustUdfDateTimeAdapters.MonthnameAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue(e.getMessage().contains("unsupported format"));
    }

    /**
     * Non-literal operand (column ref) skips validation and passes through unchanged — pins the
     * {@code validateFirstArgIfStringLiteral} early-exit on non-literal inputs so DAYNAME / MONTHNAME
     * over a real column doesn't throw plan-time on values the adapter can't statically analyse.
     */
    public void testDaynameAdapterNonLiteralOperandSkipsValidation() {
        RelDataType ts = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RexNode tsCol = rexBuilder.makeInputRef(ts, 0);
        RexNode fmt = rexBuilder.makeLiteral("%W", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexCall original = (RexCall) rexBuilder.makeCall(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            RustUdfDateTimeAdapters.LOCAL_DATE_FORMAT_OP,
            List.of(tsCol, fmt)
        );

        RexNode adapted = new RustUdfDateTimeAdapters.DaynameAdapter().adapt(original, List.of(), cluster);

        assertTrue("non-literal operand must lower without throwing", adapted instanceof RexCall);
    }

    /** Builds a {@code date_format(<varchar-literal>, <fmt>)} call shaped like the rewritten DAYNAME / MONTHNAME input. */
    private RexCall makeDateFormatCall(String literal) {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode operand = rexBuilder.makeLiteral(literal, varchar, true);
        RexNode fmt = rexBuilder.makeLiteral("%W", varchar, true);
        return (RexCall) rexBuilder.makeCall(varchar, RustUdfDateTimeAdapters.LOCAL_DATE_FORMAT_OP, List.of(operand, fmt));
    }
}
