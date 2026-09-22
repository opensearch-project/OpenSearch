/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link IntegerReturnWideningCastAdapter} — verifies that an
 * {@code INTEGER}-returning call (concrete example: {@code ARRAY_LENGTH(arr)}) is rewritten
 * to {@code CAST(fn(...) AS INTEGER)} where the inner call has return type {@code BIGINT}
 * (to match DataFusion's i64 physical), and that already-wide / non-integer returns pass
 * through unchanged.
 */
public class IntegerReturnWideningCastAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    private RelDataType type(SqlTypeName name, boolean nullable) {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(name), nullable);
    }

    /** Builds an {@code ARRAY_LENGTH(arr)} {@link RexCall} whose return type Calcite resolves to
     *  {@code INTEGER NULLABLE} (matching {@code ReturnTypes.INTEGER_NULLABLE}). */
    private RexCall arrayLengthCall(RexNode arrOperand) {
        RelDataType retType = type(SqlTypeName.INTEGER, true);
        return (RexCall) rexBuilder.makeCall(retType, SqlLibraryOperators.ARRAY_LENGTH, List.of(arrOperand));
    }

    private RexNode arrayLiteral() {
        RelDataType intType = type(SqlTypeName.INTEGER, false);
        RelDataType arrType = typeFactory.createArrayType(intType, -1);
        return rexBuilder.makeLiteral(List.of(1, 2, 3), arrType, false);
    }

    public void testArrayLengthIntegerReturnIsWrappedInCasts() {
        // array_length(arr) — outer return must remain INTEGER, inner must be widened to BIGINT,
        // operand passes through unchanged.
        RexNode arr = arrayLiteral();
        RexCall original = arrayLengthCall(arr);
        assertEquals("preconditions: original return type INTEGER", SqlTypeName.INTEGER, original.getType().getSqlTypeName());

        IntegerReturnWideningCastAdapter adapter = new IntegerReturnWideningCastAdapter();
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue("Adapter must return a RexCall", adapted instanceof RexCall);
        RexCall outerCast = (RexCall) adapted;
        assertEquals("Outer node is a CAST", SqlKind.CAST, outerCast.getKind());
        assertEquals("Outer return type INTEGER (matching original)", SqlTypeName.INTEGER, outerCast.getType().getSqlTypeName());

        RexNode inner = outerCast.getOperands().get(0);
        assertTrue("Inner node is a RexCall (the widened ARRAY_LENGTH)", inner instanceof RexCall);
        RexCall innerCall = (RexCall) inner;
        assertSame("Inner operator is the original ARRAY_LENGTH operator", original.getOperator(), innerCall.getOperator());
        assertEquals("Inner return type widened to BIGINT", SqlTypeName.BIGINT, innerCall.getType().getSqlTypeName());
        assertEquals("Inner call has the same number of operands", original.getOperands().size(), innerCall.getOperands().size());
        assertSame("Operand passed through unchanged", arr, innerCall.getOperands().get(0));
    }

    public void testNullabilityIsPreservedOnInnerAndOuter() {
        // Nullable INTEGER return — outer CAST must produce nullable INTEGER (matching original);
        // the inner widened call must be nullable BIGINT.
        RexCall original = arrayLengthCall(arrayLiteral());
        assertTrue("Precondition: original is nullable", original.getType().isNullable());

        IntegerReturnWideningCastAdapter adapter = new IntegerReturnWideningCastAdapter();
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals("Outer return type still INTEGER", SqlTypeName.INTEGER, outerCast.getType().getSqlTypeName());
        assertTrue("Outer cast preserves nullability", outerCast.getType().isNullable());
        RexCall innerCall = (RexCall) outerCast.getOperands().get(0);
        assertTrue("Inner widened call preserves nullability", innerCall.getType().isNullable());
    }

    public void testNonNullableIntegerReturnProducesNonNullableOuterCast() {
        // Forge a non-nullable INTEGER return on the synthetic call to verify nullability flows
        // through both the inner widening and the outer cast unchanged.
        RexNode arr = arrayLiteral();
        RelDataType nonNullInt = type(SqlTypeName.INTEGER, false);
        RexCall original = (RexCall) rexBuilder.makeCall(nonNullInt, SqlLibraryOperators.ARRAY_LENGTH, List.of(arr));
        assertFalse("Precondition: original is non-nullable", original.getType().isNullable());

        IntegerReturnWideningCastAdapter adapter = new IntegerReturnWideningCastAdapter();
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertFalse("Outer cast preserves non-nullability", outerCast.getType().isNullable());
        RexCall innerCall = (RexCall) outerCast.getOperands().get(0);
        assertFalse("Inner widened call preserves non-nullability", innerCall.getType().isNullable());
    }

    public void testBigintReturnIsUnchanged() {
        // If somehow Calcite already types the call as BIGINT (e.g. a future operator with a
        // BIGINT return-type inference), there's nothing to widen — adapter must be a no-op.
        RexNode arr = arrayLiteral();
        RelDataType bigintRet = type(SqlTypeName.BIGINT, true);
        RexCall original = (RexCall) rexBuilder.makeCall(bigintRet, SqlLibraryOperators.ARRAY_LENGTH, List.of(arr));

        IntegerReturnWideningCastAdapter adapter = new IntegerReturnWideningCastAdapter();
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("Already-BIGINT return passes through unchanged", original, adapted);
    }

    public void testReAdaptingInnerWidenedCallIsANoop() {
        // The adapter rebuilds the call as cast(fn(...) [BIGINT], INTEGER). If BackendPlanAdapter
        // recurses and re-visits the inner BIGINT-returning call, the return-type guard must skip
        // the rewrite to avoid infinite expansion.
        RexCall original = arrayLengthCall(arrayLiteral());

        IntegerReturnWideningCastAdapter adapter = new IntegerReturnWideningCastAdapter();
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);
        RexCall innerWidened = (RexCall) outerCast.getOperands().get(0);
        assertEquals("Precondition: inner is BIGINT-returning", SqlTypeName.BIGINT, innerWidened.getType().getSqlTypeName());

        RexNode reAdapted = adapter.adapt(innerWidened, List.of(), cluster);
        assertSame("Re-adapting inner BIGINT-return call is a no-op", innerWidened, reAdapted);
    }

    public void testSmallintReturnIsAlsoWidened() {
        // Defensive: if some other call surfaces with SMALLINT return (same family of i16<->i64
        // mismatches in principle), the adapter must widen it just like INTEGER.
        RexNode arr = arrayLiteral();
        RelDataType smallintRet = type(SqlTypeName.SMALLINT, true);
        RexCall original = (RexCall) rexBuilder.makeCall(smallintRet, SqlLibraryOperators.ARRAY_LENGTH, List.of(arr));

        IntegerReturnWideningCastAdapter adapter = new IntegerReturnWideningCastAdapter();
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals("Outer return type SMALLINT (matching original)", SqlTypeName.SMALLINT, outerCast.getType().getSqlTypeName());
        RexCall innerCall = (RexCall) outerCast.getOperands().get(0);
        assertEquals("Inner return type widened to BIGINT", SqlTypeName.BIGINT, innerCall.getType().getSqlTypeName());
    }

    public void testNonIntegerReturnIsUnchanged() {
        // A DOUBLE-returning call would not exhibit the i32/i64 schema disagreement and is out
        // of scope for this adapter. Adapter must pass through unchanged.
        RexNode arr = arrayLiteral();
        RelDataType doubleRet = type(SqlTypeName.DOUBLE, true);
        RexCall original = (RexCall) rexBuilder.makeCall(doubleRet, SqlLibraryOperators.ARRAY_LENGTH, List.of(arr));

        IntegerReturnWideningCastAdapter adapter = new IntegerReturnWideningCastAdapter();
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("DOUBLE-return call passes through unchanged", original, adapted);
    }
}
