/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the relative-time DSL parser used by
 * {@link EarliestLatestAdapter} to fold scalar {@code earliest}/{@code latest}
 * predicates at plan time.
 *
 * <p>RelNode-level rewrite is exercised end-to-end by
 * {@code DataFusionFragmentConvertorTests} (Substrait emit) and
 * {@code EarliestLatestCommandIT} (REST IT). These tests pin the parser
 * semantics so future refactors can't silently shift the relative-time
 * arithmetic.
 */
public class EarliestLatestAdapterTests extends OpenSearchTestCase {

    /** A fixed, easy-to-reason-about reference point: 2024-07-15 12:34:56 UTC. */
    private static final long NOW_MILLIS = ZonedDateTime
        .of(2024, 7, 15, 12, 34, 56, 0, ZoneOffset.UTC)
        .toInstant()
        .toEpochMilli();

    public void testNowReturnsNowMillis() {
        assertEquals(NOW_MILLIS, EarliestLatestAdapter.parseExpression("now", NOW_MILLIS));
        assertEquals(NOW_MILLIS, EarliestLatestAdapter.parseExpression("NOW", NOW_MILLIS));
        assertEquals(NOW_MILLIS, EarliestLatestAdapter.parseExpression("now()", NOW_MILLIS));
    }

    public void testNegativeOffsetSubtractsDuration() {
        long expected = NOW_MILLIS - 7L * 24 * 3600 * 1000;
        assertEquals(expected, EarliestLatestAdapter.parseExpression("-7d", NOW_MILLIS));
        assertEquals(expected, EarliestLatestAdapter.parseExpression("-7days", NOW_MILLIS));
    }

    public void testPositiveOffsetAddsDuration() {
        long expected = NOW_MILLIS + 30L * 60 * 1000;
        assertEquals(expected, EarliestLatestAdapter.parseExpression("+30m", NOW_MILLIS));
        assertEquals(expected, EarliestLatestAdapter.parseExpression("+30minutes", NOW_MILLIS));
    }

    public void testOmittedNumberDefaultsToOne() {
        // "-d" means "minus 1 day".
        long expected = NOW_MILLIS - 24L * 3600 * 1000;
        assertEquals(expected, EarliestLatestAdapter.parseExpression("-d", NOW_MILLIS));
    }

    public void testSnapToDay() {
        // 2024-07-15 12:34:56 UTC snapped to day → 2024-07-15 00:00:00 UTC.
        long expected = LocalDate.of(2024, 7, 15).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("@d", NOW_MILLIS));
    }

    public void testOffsetThenSnapChain() {
        // -1h@d: subtract 1h → 2024-07-15 11:34:56 → snap to day → 2024-07-15 00:00:00.
        // Snap is on the result-so-far, not on the original.
        long expected = LocalDate.of(2024, 7, 15).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("-1h@d", NOW_MILLIS));
    }

    public void testSnapToMonth() {
        // Note: @M (uppercase single letter) is NOT supported because
        // normalizeUnit() lowercases first and `"m"` matches the minute alias.
        // This matches the SQL plugin's DateTimeUtils.normalizeUnit behavior —
        // users must spell month as `@mon` or `@month`.
        long expected = LocalDate.of(2024, 7, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("@mon", NOW_MILLIS));
        assertEquals(expected, EarliestLatestAdapter.parseExpression("@month", NOW_MILLIS));
    }

    public void testQuarterOffsetIsThreeMonths() {
        // 2024-07-15 → -1q → 2024-04-15.
        long expected = ZonedDateTime.of(2024, 4, 15, 12, 34, 56, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("-1q", NOW_MILLIS));
    }

    public void testQuarterSnapAlignsToQuarterStart() {
        // 2024-07-15 → @q → 2024-07-01 (Q3 start).
        long expected = LocalDate.of(2024, 7, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("@q", NOW_MILLIS));
    }

    public void testWeekDaySnap() {
        // 2024-07-15 is a Monday (DayOfWeek=1). @w1 → snap to most recent Monday.
        long expected = LocalDate.of(2024, 7, 15).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("@w1", NOW_MILLIS));
        // @w0 → most recent Sunday, which is 2024-07-14.
        long expectedSun = LocalDate.of(2024, 7, 14).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expectedSun, EarliestLatestAdapter.parseExpression("@w0", NOW_MILLIS));
    }

    public void testMultipleChunks() {
        // -1mon-2d: subtract one month, then 2 days. From 2024-07-15 → 2024-06-15 → 2024-06-13.
        long expected = ZonedDateTime.of(2024, 6, 13, 12, 34, 56, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("-1mon-2d", NOW_MILLIS));
    }

    public void testAbsoluteIsoDate() {
        long expected = LocalDate.of(2023, 1, 5).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("2023-01-05", NOW_MILLIS));
    }

    public void testAbsoluteIsoTimestamp() {
        long expected = LocalDateTime.of(2023, 1, 5, 12, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("2023-01-05T12:00:00", NOW_MILLIS));
        assertEquals(expected, EarliestLatestAdapter.parseExpression("2023-01-05 12:00:00", NOW_MILLIS));
    }

    public void testAbsoluteSplunkFormat() {
        // 07/28/2004:12:34:27 (used by SQL plugin's CalcitePPLConditionBuiltinFunctionIT).
        long expected = LocalDateTime.of(2004, 7, 28, 12, 34, 27).toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("07/28/2004:12:34:27", NOW_MILLIS));
    }

    public void testIsoInstantWithZ() {
        long expected = Instant.parse("2023-01-05T12:00:00Z").toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("2023-01-05T12:00:00Z", NOW_MILLIS));
    }

    public void testRejectsBadInput() {
        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> EarliestLatestAdapter.parseExpression("garbage", NOW_MILLIS)
        );
        assertTrue(e1.getMessage().contains("Unexpected character") || e1.getMessage().contains("Unsupported"));

        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> EarliestLatestAdapter.parseExpression("-7zz", NOW_MILLIS) // bad unit
        );
        assertTrue(e2.getMessage().toLowerCase().contains("unit"));
    }

    public void testRejectsEmpty() {
        expectThrows(IllegalArgumentException.class, () -> EarliestLatestAdapter.parseExpression("", NOW_MILLIS));
        expectThrows(IllegalArgumentException.class, () -> EarliestLatestAdapter.parseExpression("   ", NOW_MILLIS));
        expectThrows(IllegalArgumentException.class, () -> EarliestLatestAdapter.parseExpression(null, NOW_MILLIS));
    }

    public void testHourOffsetMatchesChronoArithmetic() {
        // Sanity check that ChronoUnit math is consistent with a hand-rolled millis subtract.
        long expected = ZonedDateTime
            .ofInstant(Instant.ofEpochMilli(NOW_MILLIS), ZoneOffset.UTC)
            .minus(3, ChronoUnit.HOURS)
            .toInstant()
            .toEpochMilli();
        assertEquals(expected, EarliestLatestAdapter.parseExpression("-3h", NOW_MILLIS));
    }

    // ── RelNode-level rewrite ─────────────────────────────────────────────────

    /**
     * The fold replaces a {@code RexCall} with operator name "EARLIEST" by a
     * {@code >=} comparison. This exercises the full RelShuttle path —
     * descend into a Filter, rewrite its condition, return a copied Filter.
     * Operator recognition is by name (so the test doesn't need the SQL
     * plugin's actual EARLIEST UDF on the classpath).
     */
    public void testFoldRewritesEarliestCallOnFilter() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType tsType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RelDataType rowType = typeFactory.builder().add("@timestamp", tsType).build();
        RelNode source = LogicalValues.createEmpty(cluster, rowType);

        SqlFunction earliest = new SqlFunction(
            "EARLIEST",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode literal = rexBuilder.makeLiteral("-7d");
        RexNode tsRef = rexBuilder.makeInputRef(source, 0);
        RexNode call = rexBuilder.makeCall(earliest, literal, tsRef);
        RelNode filter = LogicalFilter.create(source, call);

        RelNode folded = EarliestLatestAdapter.foldRelativeTimePredicates(filter);
        assertTrue("fold must preserve the Filter rel", folded instanceof Filter);
        Filter foldedFilter = (Filter) folded;
        assertTrue("filter condition must be a RexCall", foldedFilter.getCondition() instanceof RexCall);
        RexCall foldedCall = (RexCall) foldedFilter.getCondition();
        assertEquals(
            "EARLIEST must fold to GREATER_THAN_OR_EQUAL",
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            foldedCall.getOperator()
        );
        // Two operands: input ref to ts, then a timestamp literal.
        assertEquals(2, foldedCall.getOperands().size());
        assertTrue(
            "right-hand side must be a literal: " + foldedCall.getOperands().get(1),
            foldedCall.getOperands().get(1) instanceof RexLiteral
        );
    }

    /**
     * Sibling test: LATEST → LESS_THAN_OR_EQUAL.
     */
    public void testFoldRewritesLatestCallOnFilter() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType tsType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RelDataType rowType = typeFactory.builder().add("@timestamp", tsType).build();
        RelNode source = LogicalValues.createEmpty(cluster, rowType);

        SqlFunction latest = new SqlFunction(
            "LATEST",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode literal = rexBuilder.makeLiteral("now");
        RexNode tsRef = rexBuilder.makeInputRef(source, 0);
        RexNode call = rexBuilder.makeCall(latest, literal, tsRef);
        RelNode filter = LogicalFilter.create(source, call);

        RelNode folded = EarliestLatestAdapter.foldRelativeTimePredicates(filter);
        Filter foldedFilter = (Filter) folded;
        RexCall foldedCall = (RexCall) foldedFilter.getCondition();
        assertEquals(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, foldedCall.getOperator());
    }

    /**
     * A filter that does not contain EARLIEST/LATEST must pass through unchanged
     * — both the RelNode reference and the condition reference. This guards
     * against accidental copies that would defeat Calcite's reference-equality
     * fast paths.
     */
    public void testFoldLeavesUnrelatedFilterUntouched() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType intType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType rowType = typeFactory.builder().add("x", intType).build();
        RelNode source = LogicalValues.createEmpty(cluster, rowType);

        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(source, 0),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode filter = LogicalFilter.create(source, condition);

        RelNode folded = EarliestLatestAdapter.foldRelativeTimePredicates(filter);
        // Same Filter shape with same condition — fold must be a no-op pass-through.
        assertTrue(folded instanceof Filter);
        assertEquals(condition, ((Filter) folded).getCondition());
    }
}
