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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link EarliestLatestAdapter}. Verifies the adapter
 * rewrites {@code EARLIEST(literal, ts)} / {@code LATEST(literal, ts)} into the
 * expected RexNode shape — the comparison operator on the outside, the right-hand
 * side a now()-symbolic expression (or a TIMESTAMP_LITERAL for absolute-time
 * literals).
 */
public class EarliestLatestAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType timestampType;
    private SqlUserDefinedFunction earliestUdf;
    private SqlUserDefinedFunction latestUdf;
    private RexNode tsRef;

    private final EarliestLatestAdapter.EarliestAdapter earliestAdapter = new EarliestLatestAdapter.EarliestAdapter();
    private final EarliestLatestAdapter.LatestAdapter latestAdapter = new EarliestLatestAdapter.LatestAdapter();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3), true);
        // RexInputRef ($0) typed as TIMESTAMP — stand-in for the @timestamp column.
        tsRef = rexBuilder.makeInputRef(timestampType, 0);

        earliestUdf = makeUdf("EARLIEST");
        latestUdf = makeUdf("LATEST");
    }

    private SqlUserDefinedFunction makeUdf(String name) {
        return new SqlUserDefinedFunction(
            new SqlIdentifier(name, SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            null,
            null
        );
    }

    private RexCall buildEarliest(String dsl) {
        RexNode lit = rexBuilder.makeLiteral(dsl);
        RelDataType boolType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
        return (RexCall) rexBuilder.makeCall(boolType, earliestUdf, List.of(lit, tsRef));
    }

    private RexCall buildLatest(String dsl) {
        RexNode lit = rexBuilder.makeLiteral(dsl);
        RelDataType boolType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
        return (RexCall) rexBuilder.makeCall(boolType, latestUdf, List.of(lit, tsRef));
    }

    // ── Absolute literal: emits TIMESTAMP_LITERAL, no symbolic now() ───────────

    public void testAbsoluteIsoDateEmitsTimestampLiteral() {
        RexNode result = earliestAdapter.adapt(buildEarliest("2024-01-15"), List.of(), cluster);
        assertCmp(result, SqlKind.GREATER_THAN_OR_EQUAL);
        RexNode rhs = ((RexCall) result).getOperands().get(1);
        assertTrue("expected TIMESTAMP_LITERAL on RHS, got " + rhs, rhs instanceof RexLiteral);
        assertEquals(SqlTypeName.TIMESTAMP, rhs.getType().getSqlTypeName());
    }

    public void testAbsoluteWithTimeEmitsTimestampLiteral() {
        RexNode result = latestAdapter.adapt(buildLatest("2024-01-15 12:00:00"), List.of(), cluster);
        assertCmp(result, SqlKind.LESS_THAN_OR_EQUAL);
        RexNode rhs = ((RexCall) result).getOperands().get(1);
        assertTrue(rhs instanceof RexLiteral);
    }

    // ── 'now' / 'now()': just now() symbolic ──────────────────────────────────

    public void testNowEmitsBareNow() {
        RexNode result = earliestAdapter.adapt(buildEarliest("now"), List.of(), cluster);
        assertCmp(result, SqlKind.GREATER_THAN_OR_EQUAL);
        RexNode rhs = ((RexCall) result).getOperands().get(1);
        assertTrue("expected niladic now() on RHS, got " + rhs, isNowCall(rhs));
    }

    public void testNowParenthesesEmitsBareNow() {
        RexNode result = latestAdapter.adapt(buildLatest("now()"), List.of(), cluster);
        RexNode rhs = ((RexCall) result).getOperands().get(1);
        assertTrue(isNowCall(rhs));
    }

    // ── Pure offset: DATETIME_PLUS(now(), INTERVAL ...) ───────────────────────

    public void testNegativeOffsetDays() {
        RexNode result = earliestAdapter.adapt(buildEarliest("-7d"), List.of(), cluster);
        assertCmp(result, SqlKind.GREATER_THAN_OR_EQUAL);
        RexCall rhs = (RexCall) ((RexCall) result).getOperands().get(1);
        assertEquals(SqlKind.PLUS, rhs.getKind());
        assertTrue("inner LHS must be now()", isNowCall(rhs.getOperands().get(0)));
        assertTrue(
            "inner RHS must be INTERVAL literal",
            rhs.getOperands().get(1) instanceof RexLiteral
                && rhs.getOperands().get(1).getType().getSqlTypeName().getName().startsWith("INTERVAL")
        );
    }

    public void testPositiveOffsetMinutes() {
        RexNode result = earliestAdapter.adapt(buildEarliest("+30m"), List.of(), cluster);
        RexCall rhs = (RexCall) ((RexCall) result).getOperands().get(1);
        assertEquals(SqlKind.PLUS, rhs.getKind());
        assertTrue(isNowCall(rhs.getOperands().get(0)));
    }

    public void testMonthOffsetUsesMonthInterval() {
        // Month offsets cannot be expressed as millisecond INTERVAL_DAY (month length varies),
        // so the adapter emits an INTERVAL_MONTH literal.
        RexNode result = earliestAdapter.adapt(buildEarliest("-1mon"), List.of(), cluster);
        RexCall rhs = (RexCall) ((RexCall) result).getOperands().get(1);
        assertEquals(SqlKind.PLUS, rhs.getKind());
        RexNode interval = rhs.getOperands().get(1);
        assertEquals(SqlTypeName.INTERVAL_MONTH, interval.getType().getSqlTypeName());
    }

    public void testQuarterOffsetUsesMonthInterval() {
        // -1q == -3 months, emitted as INTERVAL_MONTH literal of value -3.
        RexNode result = earliestAdapter.adapt(buildEarliest("-1q"), List.of(), cluster);
        RexCall rhs = (RexCall) ((RexCall) result).getOperands().get(1);
        RexLiteral interval = (RexLiteral) rhs.getOperands().get(1);
        assertEquals(SqlTypeName.INTERVAL_MONTH, interval.getType().getSqlTypeName());
    }

    public void testMultipleOffsetChunksCompose() {
        // -1mon-2d → DATETIME_PLUS(DATETIME_PLUS(now(), INTERVAL -1 MONTH), INTERVAL -2 DAY)
        RexNode result = earliestAdapter.adapt(buildEarliest("-1mon-2d"), List.of(), cluster);
        RexCall outer = (RexCall) ((RexCall) result).getOperands().get(1);
        assertEquals(SqlKind.PLUS, outer.getKind());
        RexCall inner = (RexCall) outer.getOperands().get(0);
        assertEquals(SqlKind.PLUS, inner.getKind());
        assertTrue(isNowCall(inner.getOperands().get(0)));
    }

    // ── Snap: date_trunc(unit, now()) ─────────────────────────────────────────

    public void testSnapDayUsesDateTrunc() {
        RexNode result = earliestAdapter.adapt(buildEarliest("@d"), List.of(), cluster);
        RexCall rhs = (RexCall) ((RexCall) result).getOperands().get(1);
        assertEquals("date_trunc", rhs.getOperator().getName());
        assertEquals("day", ((RexLiteral) rhs.getOperands().get(0)).getValueAs(String.class));
        assertTrue(isNowCall(rhs.getOperands().get(1)));
    }

    public void testSnapMonthUsesDateTrunc() {
        RexNode result = earliestAdapter.adapt(buildEarliest("@mon"), List.of(), cluster);
        RexCall rhs = (RexCall) ((RexCall) result).getOperands().get(1);
        assertEquals("date_trunc", rhs.getOperator().getName());
        assertEquals("month", ((RexLiteral) rhs.getOperands().get(0)).getValueAs(String.class));
    }

    public void testSnapYearUsesDateTrunc() {
        RexNode result = earliestAdapter.adapt(buildEarliest("@y"), List.of(), cluster);
        RexCall rhs = (RexCall) ((RexCall) result).getOperands().get(1);
        assertEquals("date_trunc", rhs.getOperator().getName());
        assertEquals("year", ((RexLiteral) rhs.getOperands().get(0)).getValueAs(String.class));
    }

    public void testSnapQuarterUsesDateTrunc() {
        RexNode result = earliestAdapter.adapt(buildEarliest("@q"), List.of(), cluster);
        RexCall rhs = (RexCall) ((RexCall) result).getOperands().get(1);
        assertEquals("date_trunc", rhs.getOperator().getName());
        assertEquals("quarter", ((RexLiteral) rhs.getOperands().get(0)).getValueAs(String.class));
    }

    // ── Weekday snap: date_trunc('day', now()) - <diff> days ──────────────────

    public void testWeekdaySnapW1IsDayTruncOrShifted() {
        // @w1 (Monday). Either date_trunc('day', now()) directly (if today is Monday)
        // or DATETIME_PLUS(date_trunc('day', now()), INTERVAL_DAY -N) for some N.
        RexNode result = earliestAdapter.adapt(buildEarliest("@w1"), List.of(), cluster);
        RexNode rhs = ((RexCall) result).getOperands().get(1);
        assertWeekdaySnapShape(rhs);
    }

    public void testWeekdaySnapW0SundayIsShifted() {
        RexNode result = earliestAdapter.adapt(buildEarliest("@w0"), List.of(), cluster);
        RexNode rhs = ((RexCall) result).getOperands().get(1);
        assertWeekdaySnapShape(rhs);
    }

    public void testWeekdaySnapWBareIsSundayShape() {
        // @w (no number) is the same as @w0/@w7 (Sunday).
        RexNode result = earliestAdapter.adapt(buildEarliest("@w"), List.of(), cluster);
        RexNode rhs = ((RexCall) result).getOperands().get(1);
        assertWeekdaySnapShape(rhs);
    }

    /**
     * A weekday snap RHS is either:
     *   date_trunc('day', now())                              (if diff == 0)
     * or
     *   DATETIME_PLUS(date_trunc('day', now()), INTERVAL N DAY)
     */
    private static void assertWeekdaySnapShape(RexNode rhs) {
        if (rhs instanceof RexCall call) {
            if (call.getOperator().getName().equals("date_trunc")) {
                // diff == 0 case
                return;
            }
            assertEquals(SqlKind.PLUS, call.getKind());
            RexNode inner = call.getOperands().get(0);
            assertTrue(
                "inner of weekday snap must be date_trunc('day', now())",
                inner instanceof RexCall innerCall && innerCall.getOperator().getName().equals("date_trunc")
            );
            return;
        }
        fail("weekday snap RHS must be a RexCall, got " + rhs);
    }

    // ── Mixed: chunks compose ─────────────────────────────────────────────────

    public void testMixedOffsetThenSnap() {
        // -1h@d → date_trunc('day', DATETIME_PLUS(now(), INTERVAL -1 HOUR))
        RexNode result = earliestAdapter.adapt(buildEarliest("-1h@d"), List.of(), cluster);
        RexCall rhs = (RexCall) ((RexCall) result).getOperands().get(1);
        assertEquals("date_trunc", rhs.getOperator().getName());
        RexCall inner = (RexCall) rhs.getOperands().get(1);
        assertEquals(SqlKind.PLUS, inner.getKind());
        assertTrue(isNowCall(inner.getOperands().get(0)));
    }

    // ── Defensive: malformed first arg ────────────────────────────────────────

    public void testNonLiteralFirstArgIsLeftAlone() {
        // First arg is a column reference, not a literal — adapter must return the
        // call unchanged (defensive; PPL grammar doesn't actually allow this).
        RexNode varcharRef = rexBuilder.makeInputRef(
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true),
            1
        );
        RelDataType boolType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
        RexCall original = (RexCall) rexBuilder.makeCall(boolType, earliestUdf, List.of(varcharRef, tsRef));
        RexNode result = earliestAdapter.adapt(original, List.of(), cluster);
        assertSame("non-literal first arg must short-circuit to the original call", original, result);
    }

    public void testUnparseableLiteralFallsBack() {
        RexCall original = buildEarliest("???totally-bogus");
        RexNode result = earliestAdapter.adapt(original, List.of(), cluster);
        assertSame("parse failure must short-circuit to the original call", original, result);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static void assertCmp(RexNode result, SqlKind expected) {
        assertTrue("expected RexCall, got " + result, result instanceof RexCall);
        assertEquals(expected, result.getKind());
    }

    private static boolean isNowCall(RexNode node) {
        return node instanceof RexCall call && call.getOperator().getName().equals("now");
    }
}
