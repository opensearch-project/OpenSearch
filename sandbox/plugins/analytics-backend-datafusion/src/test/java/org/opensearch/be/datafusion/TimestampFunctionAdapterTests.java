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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.test.OpenSearchTestCase;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

public class TimestampFunctionAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private TimestampFunctionAdapter adapter;

    /** Stand-in for PPL's TIMESTAMP function — only the operand shape and the
     *  declared return type matter to the adapter. */
    private static final SqlFunction TIMESTAMP_OP = new SqlFunction(
        "TIMESTAMP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        adapter = new TimestampFunctionAdapter();
    }

    // ── parseTimestamp helper coverage (preserved from the previous suite) ─

    public void testIsoWithTAndZ() {
        assertEquals("2024-01-01 00:00:00", TimestampFunctionAdapter.parseTimestamp("2024-01-01T00:00:00Z").toString());
    }

    public void testIsoWithTNoZ() {
        assertEquals("2024-01-15 10:30:00", TimestampFunctionAdapter.parseTimestamp("2024-01-15T10:30:00").toString());
    }

    public void testDateOnly() {
        assertEquals("2024-01-01 00:00:00", TimestampFunctionAdapter.parseTimestamp("2024-01-01").toString());
    }

    public void testTimezoneOffsetConvertsToUtc() {
        // Both signs in one test — they exercise the same OffsetDateTime → UTC
        // conversion branch; the sign-flip on offset minutes is mathematically
        // symmetric.
        assertEquals("2024-01-01 04:30:00", TimestampFunctionAdapter.parseTimestamp("2024-01-01T10:00:00+05:30").toString());
        assertEquals("2024-01-01 15:00:00", TimestampFunctionAdapter.parseTimestamp("2024-01-01T10:00:00-05:00").toString());
    }

    public void testWithMilliseconds() {
        assertEquals("2024-01-01 10:30:00.123", TimestampFunctionAdapter.parseTimestamp("2024-01-01T10:30:00.123Z").toString());
    }

    public void testWithNanoseconds() {
        assertEquals("2024-01-01 10:30:00.123456789", TimestampFunctionAdapter.parseTimestamp("2024-01-01T10:30:00.123456789Z").toString());
    }

    public void testSpaceSeparatorPassthrough() {
        assertEquals("2024-01-01 10:30:00", TimestampFunctionAdapter.parseTimestamp("2024-01-01 10:30:00").toString());
    }

    public void testUnparseableInputThrowsIllegalArgument() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TimestampFunctionAdapter.parseTimestamp("2025-13-02")
        );
        assertEquals("2025-13-02", e.getMessage());

        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> TimestampFunctionAdapter.parseTimestamp("2025-12-01 15:02:61")
        );
        assertEquals("2025-12-01 15:02:61", e2.getMessage());
    }

    // ── i64-ns range validation ───────────────────────────────────────────

    /**
     * Values past the i64-ns ceiling (~year 2262) must reject up front. Without
     * this guard, DataFusion's simplify_expressions widens TIMESTAMP(3) → ns
     * via {@code * 1000} and the optimizer emits an opaque
     * {@code Arrow error: Arithmetic overflow} at execution.
     */
    public void testYearAfter2262RejectsAtPlanTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TimestampFunctionAdapter.parseTimestamp("3077-04-12 09:07:00")
        );
        assertTrue(
            "error must mention the supported range, got: " + e.getMessage(),
            e.getMessage().contains("outside the supported range")
        );
    }

    /**
     * Right at the i64-ns ceiling — must succeed (boundary inclusive on the
     * second; nano precision past 854775807 would still overflow but that's a
     * sub-second concern not exercised by user input formats).
     */
    public void testYear2262JustInsideBoundaryAccepted() {
        assertEquals("2262-04-11 23:47:16", TimestampFunctionAdapter.parseTimestamp("2262-04-11 23:47:16").toString());
    }

    // ── adapt(): Shape A — TIMESTAMP('<varchar literal>') folds ───────────

    public void testAdaptShapeAFoldsVarcharLiteralToTypedTimestamp() {
        RexCall call = buildOneArgCall(makeVarcharLiteral("2020-01-01 00:00:00"));
        RexNode adapted = adapter.adapt(call, List.of(), cluster);

        // Adapter wraps in a CAST to the call's declared type when precision differs;
        // unwrap one CAST level if present.
        RexLiteral literal = unwrapCastLiteral(adapted);
        assertEquals(SqlTypeName.TIMESTAMP, literal.getType().getSqlTypeName());
        assertEquals("2020-01-01 00:00:00", literal.getValueAs(TimestampString.class).toString());
    }

    public void testAdaptShapeAUnparseableInputThrowsIllegalArgument() {
        RexCall call = buildOneArgCall(makeVarcharLiteral("2025-13-02"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> adapter.adapt(call, List.of(), cluster));
        assertEquals("2025-13-02", e.getMessage());

        RexCall call2 = buildOneArgCall(makeVarcharLiteral("2025-12-01 15:02:61"));
        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> adapter.adapt(call2, List.of(), cluster));
        assertEquals("2025-12-01 15:02:61", e2.getMessage());
    }

    // ── adapt(): Shape B — TIMESTAMP(DATE('<lit>')) ───────────────────────

    public void testAdaptShapeBFoldsDateLiteralToMidnightUtc() {
        RexCall innerDate = buildLocalDateCall("2020-08-26");
        RexCall outerCall = buildOneArgCall(innerDate);
        RexNode adapted = adapter.adapt(outerCall, List.of(), cluster);

        RexLiteral literal = unwrapCastLiteral(adapted);
        assertEquals("2020-08-26 00:00:00", literal.getValueAs(TimestampString.class).toString());
    }

    public void testAdaptShapeBRejectsUnparseableDateInput() {
        // LocalDate.parse rejects datetime-with-time strings — adapter returns null
        // from tryFoldLiteral and falls through to DATETIME_ADAPTER, which renames
        // to LOCAL_TO_TIMESTAMP_OP. We just verify the adapter didn't fold.
        RexCall innerDate = buildLocalDateCall("2020-08-26 10:00:00");
        RexCall outerCall = buildOneArgCall(innerDate);
        RexNode adapted = adapter.adapt(outerCall, List.of(), cluster);

        // Not a literal — fell through to the rename path.
        assertFalse("expected fall-through to DATETIME_ADAPTER, not a typed literal", unwrapCast(adapted) instanceof RexLiteral);
    }

    // ── adapt(): Shape C — TIMESTAMP(TIME('<lit>')) ───────────────────────

    public void testAdaptShapeCFoldsTimeLiteralWithTodayUtc() {
        RexCall innerTime = buildLocalTimeCall("10:20:30");
        RexCall outerCall = buildOneArgCall(innerTime);
        RexNode adapted = adapter.adapt(outerCall, List.of(), cluster);

        RexLiteral literal = unwrapCastLiteral(adapted);
        // Today's UTC date + the time
        String today = LocalDate.now(ZoneOffset.UTC).toString();
        assertEquals(today + " 10:20:30", literal.getValueAs(TimestampString.class).toString());
    }

    // ── adapt(): Shape D — TIMESTAMP(TIMESTAMP('<lit>')) ──────────────────

    /**
     * After bottom-up adapter walk, the inner {@code TIMESTAMP('lit')} has
     * already been folded by this same adapter into a typed TIMESTAMP literal.
     * The outer adapter's operand is therefore a {@code RexLiteral} of type
     * TIMESTAMP — not VARCHAR — so the fold path doesn't catch it. The
     * dispatcher falls through to DATETIME_ADAPTER which renames to
     * {@code LOCAL_TO_TIMESTAMP_OP}; DataFusion's runtime treats
     * {@code to_timestamp(timestamp)} as identity.
     */
    public void testAdaptShapeDTimestampOfTimestampRoutesToDatetimeAdapter() {
        RexLiteral innerTimestamp = (RexLiteral) rexBuilder.makeTimestampLiteral(new TimestampString("2020-01-01 00:00:00"), 3);
        RexCall outerCall = buildOneArgCall(innerTimestamp);
        RexNode adapted = adapter.adapt(outerCall, List.of(), cluster);

        RexNode unwrapped = unwrapCast(adapted);
        assertTrue(
            "expected RexCall fall-through to DATETIME_ADAPTER, got " + unwrapped.getClass().getSimpleName(),
            unwrapped instanceof RexCall
        );
        SqlOperator op = ((RexCall) unwrapped).getOperator();
        assertSame("expected rename to LOCAL_TO_TIMESTAMP_OP", DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP, op);
        // The inner timestamp literal is preserved as the operand.
        assertEquals(innerTimestamp, ((RexCall) unwrapped).getOperands().get(0));
    }

    // ── adapt(): Shape E — TIMESTAMP('<lit>', '<time-lit>') ───────────────

    public void testAdaptShapeETwoArgFoldsTimestampPlusTime() {
        RexCall call = buildTwoArgCall("2020-01-01 10:00:00", "01:30:00");
        RexNode adapted = adapter.adapt(call, List.of(), cluster);

        RexLiteral literal = unwrapCastLiteral(adapted);
        assertEquals("2020-01-01 11:30:00", literal.getValueAs(TimestampString.class).toString());
    }

    public void testAdaptShapeETwoArgUnparseableTimeFallsThrough() {
        RexCall call = buildTwoArgCall("2020-01-01 10:00:00", "not-a-time");
        RexNode adapted = adapter.adapt(call, List.of(), cluster);

        // Bad time → tryFoldTwoArg returns null → DATETIME_ADAPTER rename. Not a literal.
        assertFalse("expected fall-through, not a typed literal", unwrapCast(adapted) instanceof RexLiteral);
    }

    public void testAdaptShapeETwoArgUnparseableTimestampThrowsIllegalArgument() {
        // Bad first arg propagates out of parseTimestamp as IllegalArgumentException
        // with the raw input — same surface as the 1-arg unparseable path.
        RexCall call = buildTwoArgCall("not-a-timestamp", "01:30:00");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> adapter.adapt(call, List.of(), cluster));
        assertEquals("not-a-timestamp", e.getMessage());
    }

    // ── adapt(): Shape F — column ref → DATETIME_ADAPTER ──────────────────

    public void testAdaptShapeFColumnRefRoutesToDatetimeAdapter() {
        RelDataType dateType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);
        RexNode columnRef = rexBuilder.makeInputRef(dateType, 0);
        RexCall call = buildOneArgCall(columnRef);
        RexNode adapted = adapter.adapt(call, List.of(), cluster);

        // Fold did NOT catch — adapter emitted a RexCall over LOCAL_TO_TIMESTAMP_OP
        // (possibly wrapped in a CAST to align with the call's declared type).
        RexNode unwrapped = unwrapCast(adapted);
        assertTrue("expected RexCall, got " + unwrapped.getClass().getSimpleName(), unwrapped instanceof RexCall);
        SqlOperator op = ((RexCall) unwrapped).getOperator();
        assertSame("expected rename to LOCAL_TO_TIMESTAMP_OP", DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP, op);
    }

    // ── helpers ───────────────────────────────────────────────────────────

    /** Build a VARCHAR literal the way the PPL frontend does:
     *  {@code CalciteRexNodeVisitor.visitLiteral} calls
     *  {@code rexBuilder.makeLiteral(value, type, /*allowCast=*\/ true)} for
     *  multi-char strings. The {@code allowCast=true} flag prevents Calcite from
     *  normalizing the requested VARCHAR back to CHAR — without it, the adapter's
     *  {@code SqlTypeName.VARCHAR} match in {@code tryFoldLiteral} doesn't fire. */
    private RexNode makeVarcharLiteral(String value) {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR, value.length());
        return rexBuilder.makeLiteral(value, varcharType, true);
    }

    private RexCall buildOneArgCall(RexNode operand) {
        return (RexCall) rexBuilder.makeCall(TIMESTAMP_OP, List.of(operand));
    }

    private RexCall buildTwoArgCall(String tsLit, String timeLit) {
        return (RexCall) rexBuilder.makeCall(TIMESTAMP_OP, List.of(makeVarcharLiteral(tsLit), makeVarcharLiteral(timeLit)));
    }

    private RexCall buildLocalDateCall(String literal) {
        return (RexCall) rexBuilder.makeCall(DateTimeAdapters.LOCAL_DATE_OP, List.of(makeVarcharLiteral(literal)));
    }

    private RexCall buildLocalTimeCall(String literal) {
        return (RexCall) rexBuilder.makeCall(DateTimeAdapters.LOCAL_TIME_OP, List.of(makeVarcharLiteral(literal)));
    }

    /** Peel all CAST levels. The adapter wraps the folded literal in a CAST to
     *  the call's declared type; some fold paths produce nested CASTs when both
     *  precision and nullability differ from the original. */
    private static RexNode unwrapCast(RexNode node) {
        while (node instanceof RexCall call && call.getKind() == SqlKind.CAST) {
            node = call.getOperands().get(0);
        }
        return node;
    }

    private static RexLiteral unwrapCastLiteral(RexNode node) {
        RexNode unwrapped = unwrapCast(node);
        assertTrue("expected RexLiteral after unwrap, got " + unwrapped.getClass().getSimpleName(), unwrapped instanceof RexLiteral);
        return (RexLiteral) unwrapped;
    }
}
