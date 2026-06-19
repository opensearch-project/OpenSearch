/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/** Covers {@link DateAddSubAdapter}: TIME base anchors to today UTC; integer-days form rebuilds as INTERVAL DAY. */
public class DateAddSubAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    private static final SqlFunction DATE_ADD_OP = new SqlFunction(
        "DATE_ADD",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.TIMEDATE
    );

    private static final SqlFunction ADDDATE_OP = new SqlFunction(
        "ADDDATE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.TIMEDATE
    );

    private static final SqlFunction SUBDATE_OP = new SqlFunction(
        "SUBDATE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.TIMEDATE
    );

    /** DATE_ADD(TIME-col, INTERVAL 1 DAY) → DATETIME_PLUS(CAST(CONCAT(today,' ',CAST time AS VARCHAR)) AS TIMESTAMP), 86400000 millis). */
    public void testDateAddTimeOperandAnchoredToToday() {
        RelDataType timeType = typeFactory.createSqlType(SqlTypeName.TIME);
        RexNode timeCol = rexBuilder.makeInputRef(timeType, 0);
        RexNode interval = rexBuilder.makeIntervalLiteral(
            BigDecimal.valueOf(1L),
            new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO)
        );
        RexCall original = (RexCall) rexBuilder.makeCall(DATE_ADD_OP, List.of(timeCol, interval));

        RexNode adapted = new DateAddSubAdapter(true).adapt(original, List.of(), cluster);

        // Outer DATETIME_PLUS (or CAST wrapping it).
        RexCall outer = adapted instanceof RexCall && ((RexCall) adapted).getKind() == SqlKind.CAST
            ? (RexCall) ((RexCall) adapted).getOperands().get(0)
            : (RexCall) adapted;
        assertSame(SqlStdOperatorTable.DATETIME_PLUS, outer.getOperator());
        // First operand is CAST(CONCAT(today,' ',CAST(time AS VARCHAR)) AS TIMESTAMP).
        RexCall castNode = (RexCall) outer.getOperands().get(0);
        assertEquals(SqlKind.CAST, castNode.getKind());
        RexCall concat = (RexCall) castNode.getOperands().get(0);
        assertEquals("||", concat.getOperator().getName());
    }

    /** ADDDATE(DATE-col, 1) → DATETIME_PLUS(CAST(date AS …), INTERVAL 1 DAY). The integer 1 is rebuilt as a DAY interval. */
    public void testAddDateIntegerDaysOnDateRebuiltAsIntervalDay() {
        RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
        RexNode dateCol = rexBuilder.makeInputRef(dateType, 0);
        RexNode oneInt = rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
        RexCall original = (RexCall) rexBuilder.makeCall(ADDDATE_OP, List.of(dateCol, oneInt));

        RexNode adapted = new DateAddSubAdapter(true).adapt(original, List.of(), cluster);

        RexCall outer = adapted instanceof RexCall && ((RexCall) adapted).getKind() == SqlKind.CAST
            ? (RexCall) ((RexCall) adapted).getOperands().get(0)
            : (RexCall) adapted;
        assertSame(SqlStdOperatorTable.DATETIME_PLUS, outer.getOperator());
        RexNode shiftedInterval = outer.getOperands().get(1);
        assertTrue(SqlTypeName.INTERVAL_TYPES.contains(shiftedInterval.getType().getSqlTypeName()));
        assertEquals(TimeUnit.DAY, shiftedInterval.getType().getIntervalQualifier().getUnit());
        // INTERVAL DAY values are stored in millis after the unit-rebuild step; +1 day = +86400000.
        long signed = ((org.apache.calcite.rex.RexLiteral) shiftedInterval).getValueAs(Long.class);
        assertEquals(86_400_000L, signed);
    }

    /** SUBDATE(TIMESTAMP-col, 5) → DATETIME_PLUS(ts, INTERVAL -5 DAY). Sign folded for SUB. */
    public void testSubDateIntegerDaysFoldsSign() {
        RelDataType tsType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RexNode tsCol = rexBuilder.makeInputRef(tsType, 0);
        RexNode fiveInt = rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
        RexCall original = (RexCall) rexBuilder.makeCall(SUBDATE_OP, List.of(tsCol, fiveInt));

        RexNode adapted = new DateAddSubAdapter(false).adapt(original, List.of(), cluster);

        RexCall outer = adapted instanceof RexCall && ((RexCall) adapted).getKind() == SqlKind.CAST
            ? (RexCall) ((RexCall) adapted).getOperands().get(0)
            : (RexCall) adapted;
        assertSame(SqlStdOperatorTable.DATETIME_PLUS, outer.getOperator());
        long signed = ((org.apache.calcite.rex.RexLiteral) outer.getOperands().get(1)).getValueAs(Long.class);
        // 5 days in millis, negated for SUB.
        assertEquals(-5L * 86_400_000L, signed);
    }

    /**
     * Non-literal second operand (an integer column ref, not a literal) cannot be normalized to an
     * INTERVAL by {@code asIntervalLiteral} — the adapter must pass the call through unchanged so
     * isthmus surfaces a binding failure rather than producing a malformed interval.
     */
    public void testAddDateNonLiteralSecondOperandPassesThrough() {
        RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode dateCol = rexBuilder.makeInputRef(dateType, 0);
        RexNode intCol = rexBuilder.makeInputRef(intType, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(ADDDATE_OP, List.of(dateCol, intCol));

        RexNode adapted = new DateAddSubAdapter(true).adapt(original, List.of(), cluster);

        assertSame("non-literal second operand must leave the call unchanged", original, adapted);
    }

    /**
     * MICROSECOND interval falls through to the UDF path — Arrow's IntervalDayTime is
     * millisecond-resolution so the adapter cannot represent a µs increment. The adapter returns
     * the original call so the engine surfaces a loud error rather than silently truncating.
     */
    public void testDateAddMicrosecondIntervalPassesThrough() {
        RelDataType tsType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RexNode tsCol = rexBuilder.makeInputRef(tsType, 0);
        RexNode microInterval = rexBuilder.makeIntervalLiteral(
            BigDecimal.valueOf(500),
            new SqlIntervalQualifier(TimeUnit.MICROSECOND, null, SqlParserPos.ZERO)
        );
        RexCall original = (RexCall) rexBuilder.makeCall(DATE_ADD_OP, List.of(tsCol, microInterval));

        RexNode adapted = new DateAddSubAdapter(true).adapt(original, List.of(), cluster);

        assertSame("MICROSECOND interval must leave the call unchanged", original, adapted);
    }

    /**
     * ADDDATE('1970-01-01', daysExpr) — bin span=Nday lowering shape. Second operand is computed
     * (MOD-based subtract on an integer column ref), not a literal. Adapter must lower to
     * {@code from_unixtime(baseEpochSec + daysExpr * 86400.0)} where baseEpochSec=0.
     */
    public void testAddDateCharLiteralBaseWithComputedI64DaysLowersToFromUnixtime() {
        RelDataType i64 = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexNode epochCharLit = rexBuilder.makeLiteral("1970-01-01");
        RexNode daysCol = rexBuilder.makeInputRef(i64, 0);
        RexNode mod = rexBuilder.makeCall(SqlStdOperatorTable.MOD, daysCol, rexBuilder.makeLiteral(7, i64, false));
        RexNode daysExpr = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, daysCol, mod);
        RexCall original = (RexCall) rexBuilder.makeCall(ADDDATE_OP, List.of(epochCharLit, daysExpr));

        RexNode adapted = new DateAddSubAdapter(true).adapt(original, List.of(), cluster);

        RexCall fromUnixtime = unwrapCast(adapted);
        assertEquals("from_unixtime", fromUnixtime.getOperator().getName());
        // Inner expr: PLUS(baseSec=0.0, MULTIPLY(CAST(daysExpr AS DOUBLE), 86400.0))
        RexCall plus = (RexCall) fromUnixtime.getOperands().get(0);
        assertSame(SqlStdOperatorTable.PLUS, plus.getOperator());
        assertEquals(0.0, ((org.apache.calcite.rex.RexLiteral) plus.getOperands().get(0)).getValueAs(Double.class), 0.0);
        RexCall multiply = (RexCall) plus.getOperands().get(1);
        assertSame(SqlStdOperatorTable.MULTIPLY, multiply.getOperator());
        assertEquals(86_400.0, ((org.apache.calcite.rex.RexLiteral) multiply.getOperands().get(1)).getValueAs(Double.class), 0.0);
    }

    /** SUBDATE('1970-01-01', i64Expr) — sign-flipped multiplier (-86400). */
    public void testSubDateCharLiteralBaseWithI64DaysFlipsMultiplierSign() {
        RelDataType i64 = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexNode epochCharLit = rexBuilder.makeLiteral("1970-01-01");
        RexNode daysCol = rexBuilder.makeInputRef(i64, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(SUBDATE_OP, List.of(epochCharLit, daysCol));

        RexNode adapted = new DateAddSubAdapter(false).adapt(original, List.of(), cluster);

        RexCall fromUnixtime = unwrapCast(adapted);
        assertEquals("from_unixtime", fromUnixtime.getOperator().getName());
        RexCall plus = (RexCall) fromUnixtime.getOperands().get(0);
        RexCall multiply = (RexCall) plus.getOperands().get(1);
        assertEquals(-86_400.0, ((org.apache.calcite.rex.RexLiteral) multiply.getOperands().get(1)).getValueAs(Double.class), 0.0);
    }

    /** DATE-literal base folds to epoch-seconds via days × 86400. */
    public void testAddDateDateLiteralBaseFoldsViaDaysTimes86400() {
        RelDataType i64 = typeFactory.createSqlType(SqlTypeName.BIGINT);
        // 1970-01-02 = 1 day after epoch → baseSec = 86400.
        RexNode dateLit = rexBuilder.makeDateLiteral(new org.apache.calcite.util.DateString("1970-01-02"));
        RexNode daysCol = rexBuilder.makeInputRef(i64, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(ADDDATE_OP, List.of(dateLit, daysCol));

        RexNode adapted = new DateAddSubAdapter(true).adapt(original, List.of(), cluster);

        RexCall fromUnixtime = unwrapCast(adapted);
        assertEquals("from_unixtime", fromUnixtime.getOperator().getName());
        RexCall plus = (RexCall) fromUnixtime.getOperands().get(0);
        assertEquals(86_400.0, ((org.apache.calcite.rex.RexLiteral) plus.getOperands().get(0)).getValueAs(Double.class), 0.0);
    }

    /** TIMESTAMP-literal base folds via millis ÷ 1000. */
    public void testAddDateTimestampLiteralBaseFoldsViaMillisDiv1000() {
        RelDataType i64 = typeFactory.createSqlType(SqlTypeName.BIGINT);
        // 1970-01-01 00:00:01 UTC → baseSec = 1.
        RexNode tsLit = rexBuilder.makeTimestampLiteral(new org.apache.calcite.util.TimestampString("1970-01-01 00:00:01"), 0);
        RexNode daysCol = rexBuilder.makeInputRef(i64, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(ADDDATE_OP, List.of(tsLit, daysCol));

        RexNode adapted = new DateAddSubAdapter(true).adapt(original, List.of(), cluster);

        RexCall fromUnixtime = unwrapCast(adapted);
        assertEquals("from_unixtime", fromUnixtime.getOperator().getName());
        RexCall plus = (RexCall) fromUnixtime.getOperands().get(0);
        assertEquals(1.0, ((org.apache.calcite.rex.RexLiteral) plus.getOperands().get(0)).getValueAs(Double.class), 0.0);
    }

    /** Non-foldable base (column ref, not literal) with computed i64 days passes through. */
    public void testAddDateNonLiteralBaseWithI64DaysPassesThrough() {
        RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
        RelDataType i64 = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexNode dateCol = rexBuilder.makeInputRef(dateType, 0);
        RexNode daysCol = rexBuilder.makeInputRef(i64, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(ADDDATE_OP, List.of(dateCol, daysCol));

        RexNode adapted = new DateAddSubAdapter(true).adapt(original, List.of(), cluster);

        assertSame("non-foldable base must leave the call unchanged", original, adapted);
    }

    /** Unwrap an outer CAST around the from_unixtime call (return-type alignment). */
    private static RexCall unwrapCast(RexNode node) {
        if (node instanceof RexCall call && call.getKind() == SqlKind.CAST) {
            return (RexCall) call.getOperands().get(0);
        }
        return (RexCall) node;
    }
}
