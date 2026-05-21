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
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link DatePartAdapters}. Covers the VARCHAR-operand coercion
 * that wraps bare-string PPL call shapes (e.g. {@code DAY('2020-09-16')}) so
 * the rewritten {@code date_part('day', CAST(_ AS TIMESTAMP))} resolves against
 * the {@code (string, precision_timestamp<P>)} signature in
 * {@code opensearch_scalar_functions.yaml}.
 */
public class DatePartAdaptersTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    /** PPL-style nullary-arg DAY operator stand-in; the adapter only inspects the operand types. */
    private SqlFunction pplDay() {
        return new SqlFunction(
            "DAY",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
    }

    /** TIMESTAMP operand passes through unchanged — the original two-arg shape from the parent. */
    public void testTimestampOperandPassesThrough() {
        RexNode ts = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDay(), List.of(ts));

        RexCall adapted = (RexCall) DatePartAdapters.day().adapt(original, List.of(), cluster);

        assertSame(SqlLibraryOperators.DATE_PART, adapted.getOperator());
        assertEquals(2, adapted.getOperands().size());
        assertEquals("day", ((RexLiteral) adapted.getOperands().get(0)).getValueAs(String.class));
        assertSame("TIMESTAMP operand must not be wrapped in CAST", ts, adapted.getOperands().get(1));
    }

    /** DATE operand passes through unchanged — the {@code (string, date)} impl handles it. */
    public void testDateOperandPassesThrough() {
        RexNode date = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DATE), 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDay(), List.of(date));

        RexCall adapted = (RexCall) DatePartAdapters.day().adapt(original, List.of(), cluster);

        assertSame(date, adapted.getOperands().get(1));
    }

    /** VARCHAR operand is wrapped in {@code CAST(_ AS TIMESTAMP)}. */
    public void testVarcharLiteralOperandIsCastToTimestamp() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode literal = rexBuilder.makeLiteral("2020-09-16", varcharType, true);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDay(), List.of(literal));

        RexCall adapted = (RexCall) DatePartAdapters.day().adapt(original, List.of(), cluster);

        RexNode wrapped = adapted.getOperands().get(1);
        assertTrue("VARCHAR operand must be wrapped", wrapped instanceof RexCall);
        RexCall cast = (RexCall) wrapped;
        assertEquals("rewrite must use CAST", SqlKind.CAST, cast.getKind());
        assertSame(SqlTypeName.TIMESTAMP, cast.getType().getSqlTypeName());
        assertSame("inner operand of CAST must be the original literal", literal, cast.getOperands().get(0));
    }

    /** CHAR (fixed-width) operand is also coerced — both are SqlTypeFamily.CHARACTER. */
    public void testCharOperandIsCastToTimestamp() {
        RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR, 10);
        RexNode field = rexBuilder.makeInputRef(charType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDay(), List.of(field));

        RexCall adapted = (RexCall) DatePartAdapters.day().adapt(original, List.of(), cluster);

        RexNode wrapped = adapted.getOperands().get(1);
        assertEquals(SqlKind.CAST, ((RexCall) wrapped).getKind());
        assertSame(SqlTypeName.TIMESTAMP, wrapped.getType().getSqlTypeName());
    }

    /** Coerced CAST preserves operand nullability so the outer call's nullable return type stays consistent. */
    public void testCastPreservesNullability() {
        RelDataType nullableVarchar = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode field = rexBuilder.makeInputRef(nullableVarchar, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDay(), List.of(field));

        RexCall adapted = (RexCall) DatePartAdapters.day().adapt(original, List.of(), cluster);

        assertTrue("CAST output must remain nullable", adapted.getOperands().get(1).getType().isNullable());
    }

    /** Each unit factory threads through its own prepended literal. */
    public void testUnitLiteralPropagatedFromFactory() {
        RexNode ts = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDay(), List.of(ts));

        RexCall adaptedYear = (RexCall) DatePartAdapters.year().adapt(original, List.of(), cluster);
        RexCall adaptedQuarter = (RexCall) DatePartAdapters.quarter().adapt(original, List.of(), cluster);
        RexCall adaptedDoy = (RexCall) DatePartAdapters.dayOfYear().adapt(original, List.of(), cluster);

        assertEquals("year", ((RexLiteral) adaptedYear.getOperands().get(0)).getValueAs(String.class));
        assertEquals("quarter", ((RexLiteral) adaptedQuarter.getOperands().get(0)).getValueAs(String.class));
        assertEquals("doy", ((RexLiteral) adaptedDoy.getOperands().get(0)).getValueAs(String.class));
    }

    /** Coercion path also works when invoked through the unit factory for a non-default unit. */
    public void testVarcharCoercionWithHourUnit() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode literal = rexBuilder.makeLiteral("2020-09-16 17:30:00", varcharType, true);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDay(), List.of(literal));

        RexCall adapted = (RexCall) DatePartAdapters.hour().adapt(original, List.of(), cluster);

        assertEquals("hour", ((RexLiteral) adapted.getOperands().get(0)).getValueAs(String.class));
        assertEquals(SqlKind.CAST, ((RexCall) adapted.getOperands().get(1)).getKind());
    }

    /**
     * Invalid string literals (month 13, second 61, garbage) must throw at plan time so the
     * legacy "unsupported format" wording reaches the HTTP body. The Arrow CAST kernel error
     * does not survive Flight RPC serialization on the worker→coordinator hop.
     */
    public void testInvalidDateLiteralRejectedAtPlanTime() {
        assertInvalidLiteralRejected("2025-13-02"); // month out of range
        assertInvalidLiteralRejected("2025-12-01 15:02:61"); // second out of range
        assertInvalidLiteralRejected("16:00:61"); // bare time, second out of range
        assertInvalidLiteralRejected("not-a-date");
    }

    /** NULL literal passes through — column-value semantics handle null at runtime. */
    public void testNullVarcharLiteralPassesThrough() {
        RelDataType nullableVarchar = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode nullLit = rexBuilder.makeNullLiteral(nullableVarchar);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDay(), List.of(nullLit));

        RexCall adapted = (RexCall) DatePartAdapters.day().adapt(original, List.of(), cluster);

        assertSame(SqlLibraryOperators.DATE_PART, adapted.getOperator());
    }

    /** Non-literal VARCHAR (column ref) is not eagerly validated — only literals are. */
    public void testVarcharColumnRefNotValidated() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode columnRef = rexBuilder.makeInputRef(varcharType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDay(), List.of(columnRef));

        // Must not throw despite the value being unknown at plan time.
        RexCall adapted = (RexCall) DatePartAdapters.day().adapt(original, List.of(), cluster);

        assertEquals(SqlKind.CAST, ((RexCall) adapted.getOperands().get(1)).getKind());
    }

    private void assertInvalidLiteralRejected(String value) {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode literal = rexBuilder.makeLiteral(value, varcharType, true);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDay(), List.of(literal));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DatePartAdapters.day().adapt(original, List.of(), cluster)
        );
        assertTrue(
            "message must contain 'unsupported format' for input [" + value + "], got: " + e.getMessage(),
            e.getMessage().contains("unsupported format")
        );
        assertTrue("message must echo the offending input [" + value + "]", e.getMessage().contains(value));
    }
}
