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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link ConvertTzAdapter}. The adapter has three jobs in
 * priority order: identity short-circuit when both tz operands canonicalize to
 * the same value, plan-time validation/canonicalization of literal tz operands,
 * and rewrite to the locally-declared UDF operator otherwise. DST-correct
 * per-row shifting stays in the Rust UDF since IANA offsets vary per instant.
 */
public class ConvertTzAdapterTests extends OpenSearchTestCase {

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

    private SqlFunction convertTzOp(RelDataType returnType) {
        return new SqlFunction(
            "CONVERT_TZ",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(returnType),
            null,
            OperandTypes.ANY_STRING_STRING,
            SqlFunctionCategory.TIMEDATE
        );
    }

    private RexCall buildConvertTz(String fromLit, String toLit) {
        RelDataType tsType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RexNode tsRef = rexBuilder.makeInputRef(tsType, 0);
        // 2-arg makeLiteral matches PPL's frontend; the nullable 3-arg form would wrap in CAST
        RexNode fromNode = rexBuilder.makeLiteral(fromLit);
        RexNode toNode = rexBuilder.makeLiteral(toLit);
        return (RexCall) rexBuilder.makeCall(convertTzOp(tsType), List.of(tsRef, fromNode, toNode));
    }

    // ── Canonicalization (unit tests on the static helper) ────────────────

    public void testCanonicalizeTzPadsOffsetDigits() {
        assertEquals("+05:30", ConvertTzAdapter.canonicalizeTz("+5:30"));
        assertEquals("-08:00", ConvertTzAdapter.canonicalizeTz("-8:00"));
        assertEquals("+14:00", ConvertTzAdapter.canonicalizeTz("+14:00"));
    }

    public void testCanonicalizeTzAcceptsIanaNames() {
        // ZoneId.of passes through canonical ids unchanged.
        assertEquals("America/New_York", ConvertTzAdapter.canonicalizeTz("America/New_York"));
        assertEquals("Europe/London", ConvertTzAdapter.canonicalizeTz("Europe/London"));
        assertEquals("UTC", ConvertTzAdapter.canonicalizeTz("UTC"));
    }

    public void testCanonicalizeTzPassesOutOfRangeOffsetsThroughUnchanged() {
        // Syntactically-valid but out-of-range offsets are NOT rejected at plan time.
        // They pass through verbatim so the runtime UDF (rust convert_tz::parse_offset_seconds)
        // returns None and the row surfaces as NULL — matching legacy PPL semantics
        // (DateTimeFunctionIT#testConvertTZ expects NULL rows for '-17:00' / '+15:00').
        assertEquals("hours > 14 must pass through, not throw", "+15:00", ConvertTzAdapter.canonicalizeTz("+15:00"));
        assertEquals("minutes > 59 must pass through, not throw", "+05:60", ConvertTzAdapter.canonicalizeTz("+05:60"));
    }

    public void testCanonicalizeTzRejectsUnknownIana() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> ConvertTzAdapter.canonicalizeTz("Mars/Olympus"));
        assertTrue("error must include the bad value for UX: " + ex.getMessage(), ex.getMessage().contains("Mars/Olympus"));
    }

    // ── adapt() behavior ──────────────────────────────────────────────────

    /**
     * Identity fold: when both tz literals canonicalize to the same value, the
     * call reduces to its timestamp operand. No UDF invocation.
     */
    public void testAdaptIdentityFoldReturnsTimestampUnchanged() {
        RexCall original = buildConvertTz("UTC", "UTC");
        RexNode adapted = new ConvertTzAdapter().adapt(original, List.of(), cluster);

        assertSame("identity fold must return the original timestamp operand", original.getOperands().get(0), adapted);
    }

    /**
     * Identity fold must apply *after* canonicalization — `+5:00` and `+05:00`
     * are the same zone but different strings; the adapter must canonicalize
     * first, then compare.
     */
    public void testAdaptIdentityFoldAppliesAfterCanonicalization() {
        RexCall original = buildConvertTz("+5:00", "+05:00");
        RexNode adapted = new ConvertTzAdapter().adapt(original, List.of(), cluster);

        assertSame("identity fold must compare canonical forms", original.getOperands().get(0), adapted);
    }

    /**
     * When literals can't be collapsed (IANA pairs, mixed IANA + offset), the
     * call rewrites to the local UDF operator with canonicalized string
     * operands. The tz strings passed to the UDF are the canonical form.
     */
    public void testAdaptIanaPairRoutesThroughUdfWithCanonicalLiterals() {
        RexCall original = buildConvertTz("America/New_York", "Europe/London");
        RexNode adapted = new ConvertTzAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(
            "adapted call must target LOCAL_CONVERT_TZ_OP so FunctionMappings.Sig binds",
            ConvertTzAdapter.LOCAL_CONVERT_TZ_OP,
            call.getOperator()
        );
        assertEquals(3, call.getOperands().size());
        assertEquals("America/New_York", ((RexLiteral) call.getOperands().get(1)).getValueAs(String.class));
        assertEquals("Europe/London", ((RexLiteral) call.getOperands().get(2)).getValueAs(String.class));
    }

    /**
     * When literal operands need canonicalization (e.g. `+5:00` → `+05:00`),
     * the UDF-bound call sees the canonical form so the Rust side doesn't need
     * to do the padding.
     */
    public void testAdaptPassesCanonicalizedLiteralsToUdf() {
        // Pair of distinct-canonical offsets so the fold path doesn't fire.
        RexCall original = buildConvertTz("+5:00", "+10:00");
        RexNode adapted = new ConvertTzAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, call.getOperator());
        assertEquals("+05:00", ((RexLiteral) call.getOperands().get(1)).getValueAs(String.class));
        assertEquals("+10:00", ((RexLiteral) call.getOperands().get(2)).getValueAs(String.class));
    }

    /**
     * Adapter preserves the original call's return type — matches the
     * {@code AbstractNameMappingAdapter} regression guard. If the rewritten
     * call's Calcite-inferred type differs from the original, the enclosing
     * {@code Project.isValid} compatibleTypes check breaks at fragment
     * conversion.
     */
    public void testAdaptedCallPreservesOriginalReturnType() {
        RelDataType originalType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 0), true);
        RexNode tsRef = rexBuilder.makeInputRef(originalType, 0);
        RexNode fromLit = rexBuilder.makeLiteral("America/New_York");
        RexNode toLit = rexBuilder.makeLiteral("Europe/London");
        RexCall original = (RexCall) rexBuilder.makeCall(convertTzOp(originalType), List.of(tsRef, fromLit, toLit));
        assertEquals(originalType, original.getType());

        RexNode adapted = new ConvertTzAdapter().adapt(original, List.of(), cluster);

        assertEquals(
            "adapted call's return type must equal the original — otherwise Project.rowType assertion fails",
            original.getType(),
            adapted.getType()
        );
    }

    /** unknown IANA literal -> typed NULL (out-of-range offsets fall through to the UDF for runtime NULL). */
    public void testAdaptUnknownIanaLiteralReturnsTypedNull() {
        RexCall original = buildConvertTz("Mars/Olympus", "UTC");
        RexNode adapted = new ConvertTzAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexLiteral);
        assertTrue(((RexLiteral) adapted).isNull());
        assertEquals(original.getType(), adapted.getType());
    }

    /**
     * Column-valued tz operands are not validated at plan time — per-row
     * values can't be inspected until runtime, so they pass through into the
     * UDF which handles them leniently (unparseable → NULL row).
     */
    public void testAdaptColumnValuedTzOperandsPassThroughToUdf() {
        RelDataType tsType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RelDataType stringType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode tsRef = rexBuilder.makeInputRef(tsType, 0);
        // Column refs for the tz slots — not literals, so no canonicalization.
        RexNode fromCol = rexBuilder.makeInputRef(stringType, 1);
        RexNode toCol = rexBuilder.makeInputRef(stringType, 2);
        RexCall original = (RexCall) rexBuilder.makeCall(convertTzOp(tsType), List.of(tsRef, fromCol, toCol));

        RexNode adapted = new ConvertTzAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, call.getOperator());
        assertSame("column-valued from_tz must pass through unmodified", fromCol, call.getOperands().get(1));
        assertSame("column-valued to_tz must pass through unmodified", toCol, call.getOperands().get(2));
    }

    /** Identity short-circuit with VARCHAR operand under a TIMESTAMP call → SAFE-cast. */
    public void testIdentityShortCircuitWrapsInSafeCastWhenOperandTypeDiffers() {
        RelDataType returnType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 9), true);
        RexNode tsLiteral = rexBuilder.makeLiteral("2021-05-12 11:34:50");
        RexNode fromTz = rexBuilder.makeLiteral("UTC");
        RexNode toTz = rexBuilder.makeLiteral("UTC");
        RexCall original = (RexCall) rexBuilder.makeCall(convertTzOp(returnType), List.of(tsLiteral, fromTz, toTz));

        RexNode adapted = new ConvertTzAdapter().adapt(original, List.of(), cluster);

        assertNotSame(tsLiteral, adapted);
        assertEquals(original.getType(), adapted.getType());
        assertTrue(adapted instanceof RexCall);
        assertEquals(org.apache.calcite.sql.SqlKind.SAFE_CAST, ((RexCall) adapted).getOperator().getKind());
    }

    /** UDF fallback with VARCHAR timestamp operand → SAFE-cast slot 0 to TIMESTAMP. */
    public void testUdfFallbackWrapsVarcharOperandInSafeCast() {
        RelDataType returnType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 9), true);
        RexNode tsLiteral = rexBuilder.makeLiteral("2021-05-12 11:34:50");
        RexNode fromTz = rexBuilder.makeLiteral("UTC");
        RexNode toTz = rexBuilder.makeLiteral("America/Los_Angeles");
        RexCall original = (RexCall) rexBuilder.makeCall(convertTzOp(returnType), List.of(tsLiteral, fromTz, toTz));

        RexNode adapted = new ConvertTzAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, call.getOperator());
        assertEquals(original.getType(), call.getType());

        RexNode firstArg = call.getOperands().get(0);
        assertNotSame(tsLiteral, firstArg);
        assertEquals(original.getType(), firstArg.getType());
        assertTrue(firstArg instanceof RexCall);
        assertEquals(org.apache.calcite.sql.SqlKind.SAFE_CAST, ((RexCall) firstArg).getOperator().getKind());
    }
}
