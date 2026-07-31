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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link IntegerRoundingCastAdapter} — verifies that {@code FLOOR(int_col)} /
 * {@code CEIL(int_col)} are wrapped as {@code CAST(floor(CAST(int_col AS DOUBLE)) AS <orig_type>)}
 * with the outer return type preserved, and that non-integer operand types pass through unchanged.
 */
public class IntegerRoundingCastAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    private RelDataType type(SqlTypeName name, boolean nullable) {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(name), nullable);
    }

    private RexCall floorCall(RexNode operand) {
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, List.of(operand));
    }

    private RexCall ceilCall(RexNode operand) {
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.CEIL, List.of(operand));
    }

    public void testFloorOnIntegerOperandIsWrappedInCasts() {
        // floor(INTEGER literal=5) — outer call must remain INTEGER, operand must be CAST'd to
        // DOUBLE, inner FLOOR receives a single DOUBLE operand.
        RexNode intOperand = rexBuilder.makeLiteral(5, type(SqlTypeName.INTEGER, false), false);
        RexCall original = floorCall(intOperand);
        assertEquals("preconditions: original return type is INTEGER", SqlTypeName.INTEGER, original.getType().getSqlTypeName());

        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.FLOOR);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertTrue("Adapter must return a RexCall", adapted instanceof RexCall);
        RexCall outerCast = (RexCall) adapted;
        assertEquals("Outer node is a CAST", SqlKind.CAST, outerCast.getKind());
        assertEquals("Outer return type INTEGER (matching original)", SqlTypeName.INTEGER, outerCast.getType().getSqlTypeName());

        RexNode inner = outerCast.getOperands().get(0);
        assertTrue("Inner node is a RexCall (the FLOOR call)", inner instanceof RexCall);
        RexCall innerFloor = (RexCall) inner;
        assertSame("Inner operator is SqlStdOperatorTable.FLOOR", SqlStdOperatorTable.FLOOR, innerFloor.getOperator());
        assertEquals("Inner FLOOR return type DOUBLE", SqlTypeName.DOUBLE, innerFloor.getType().getSqlTypeName());

        RexNode widenedOperand = innerFloor.getOperands().get(0);
        assertEquals("Original operand widened to DOUBLE", SqlTypeName.DOUBLE, widenedOperand.getType().getSqlTypeName());
    }

    public void testCeilOnIntegerOperandIsWrappedInCasts() {
        RexNode intOperand = rexBuilder.makeLiteral(5, type(SqlTypeName.INTEGER, false), false);
        RexCall original = ceilCall(intOperand);

        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.CEIL);
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals("Outer node is a CAST", SqlKind.CAST, outerCast.getKind());
        assertEquals("Outer return type INTEGER", SqlTypeName.INTEGER, outerCast.getType().getSqlTypeName());
        RexCall innerCeil = (RexCall) outerCast.getOperands().get(0);
        assertSame("Inner operator is SqlStdOperatorTable.CEIL", SqlStdOperatorTable.CEIL, innerCeil.getOperator());
        assertEquals("Inner CEIL return type DOUBLE", SqlTypeName.DOUBLE, innerCeil.getType().getSqlTypeName());
    }

    public void testFloorOnBigintOperandPreservesBigint() {
        // floor(BIGINT) — outer cast must restore BIGINT (not narrow to INTEGER).
        RexNode longOperand = rexBuilder.makeLiteral(5L, type(SqlTypeName.BIGINT, false), false);
        RexCall original = floorCall(longOperand);

        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.FLOOR);
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals("Outer return type BIGINT (matching original)", SqlTypeName.BIGINT, outerCast.getType().getSqlTypeName());
        RexCall innerFloor = (RexCall) outerCast.getOperands().get(0);
        assertEquals("Inner FLOOR return type DOUBLE", SqlTypeName.DOUBLE, innerFloor.getType().getSqlTypeName());
    }

    public void testFloorOnDoubleOperandIsUnchanged() {
        // floor(DOUBLE) — operand already matches the standard yaml's fp64 impl, no rewrite.
        RexNode dblOperand = rexBuilder.makeLiteral(BigDecimal.valueOf(5.5), type(SqlTypeName.DOUBLE, false), false);
        RexCall original = floorCall(dblOperand);

        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.FLOOR);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("Already-DOUBLE-input call returned unchanged", original, adapted);
    }

    public void testFloorOnDecimalOperandIsUnchanged() {
        // floor(DECIMAL) — DataFusion's floor signature accepts Decimal natively; isthmus binds
        // the decimal impl directly. No adapter rewrite needed.
        RelDataType decimalType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2), false);
        RexNode decOperand = rexBuilder.makeLiteral(BigDecimal.valueOf(123, 2), decimalType, false);
        RexCall original = floorCall(decOperand);

        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.FLOOR);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("Decimal operand call returned unchanged", original, adapted);
    }

    public void testNullabilityIsPreservedInOuterCast() {
        // Nullable INTEGER input → outer CAST must produce nullable INTEGER (matching original).
        RexNode nullableInt = rexBuilder.makeNullLiteral(type(SqlTypeName.INTEGER, true));
        RexCall original = floorCall(nullableInt);
        assertTrue("Precondition: original is nullable", original.getType().isNullable());

        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.FLOOR);
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals("Outer return type still INTEGER", SqlTypeName.INTEGER, outerCast.getType().getSqlTypeName());
        assertTrue("Outer cast preserves nullability", outerCast.getType().isNullable());
    }

    public void testNonNullableInputProducesNonNullableOuterCast() {
        RexNode intOperand = rexBuilder.makeLiteral(7, type(SqlTypeName.INTEGER, false), false);
        RexCall original = floorCall(intOperand);
        assertFalse("Precondition: original is non-nullable", original.getType().isNullable());

        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.FLOOR);
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertFalse("Outer cast preserves non-nullability", outerCast.getType().isNullable());
    }

    public void testReAdaptingInnerFloorIsANoop() {
        // The adapter rebuilds the call as cast(floor(cast(x, fp64)), int). If
        // BackendPlanAdapter recurses and re-visits the inner FLOOR (whose operand is now
        // DOUBLE), the operand-type guard must skip the rewrite to avoid infinite expansion.
        RexNode intOperand = rexBuilder.makeLiteral(5, type(SqlTypeName.INTEGER, false), false);
        RexCall original = floorCall(intOperand);

        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.FLOOR);
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);
        RexCall innerFloor = (RexCall) outerCast.getOperands().get(0);

        // Re-applying the adapter to the inner FLOOR (DOUBLE input) should be a no-op.
        RexNode reAdapted = adapter.adapt(innerFloor, List.of(), cluster);
        assertSame("Re-adapting inner DOUBLE-input FLOOR is a no-op", innerFloor, reAdapted);
    }

    // ─── 2-arg flavor coverage (e.g. TRUNCATE(value, scale)) ────────────────────────────

    public void testTruncateOnIntegerWithIntegerScaleWidensOnlyFirstOperand() {
        // truncate(int_col, scale) — 2-arg form. Adapter must widen only the first operand
        // (the value); the scale operand passes through unchanged. Outer cast restores INTEGER.
        RexNode value = rexBuilder.makeLiteral(5, type(SqlTypeName.INTEGER, false), false);
        RexNode scale = rexBuilder.makeLiteral(0, type(SqlTypeName.INTEGER, false), false);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.TRUNCATE, List.of(value, scale));
        assertEquals("Precondition: TRUNCATE return type INTEGER", SqlTypeName.INTEGER, original.getType().getSqlTypeName());

        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.TRUNCATE);
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals("Outer return type INTEGER", SqlTypeName.INTEGER, outerCast.getType().getSqlTypeName());
        RexCall innerTruncate = (RexCall) outerCast.getOperands().get(0);
        assertSame("Inner operator is TRUNCATE", SqlStdOperatorTable.TRUNCATE, innerTruncate.getOperator());
        assertEquals("Inner TRUNCATE has 2 operands", 2, innerTruncate.getOperands().size());
        assertEquals("First operand widened to DOUBLE", SqlTypeName.DOUBLE, innerTruncate.getOperands().get(0).getType().getSqlTypeName());
        assertEquals(
            "Scale operand unchanged (still INTEGER)",
            SqlTypeName.INTEGER,
            innerTruncate.getOperands().get(1).getType().getSqlTypeName()
        );
        assertSame("Scale operand is the same RexNode reference", scale, innerTruncate.getOperands().get(1));
    }

    public void testTruncateOnDoubleFirstOperandIsUnchanged() {
        // truncate(double_col, scale) — already-widened first operand, no rewrite needed.
        RexNode value = rexBuilder.makeLiteral(BigDecimal.valueOf(5.5), type(SqlTypeName.DOUBLE, false), false);
        RexNode scale = rexBuilder.makeLiteral(1, type(SqlTypeName.INTEGER, false), false);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.TRUNCATE, List.of(value, scale));

        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.TRUNCATE);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("Already-DOUBLE first operand returns unchanged", original, adapted);
    }

    public void testZeroOperandCallIsUnchanged() {
        // Defensive: if somehow a no-arg call surfaces, adapter must not blow up.
        // Using PI() as a synthetic stand-in (no operands).
        RexCall original = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.PI, List.of());
        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(SqlStdOperatorTable.PI);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);
        assertSame("Zero-operand call returned unchanged", original, adapted);
    }

    // ─── Operator-rename case (e.g. SIGN → SignumFunction.FUNCTION) ───────────────────

    public void testRenameOnDoubleOperandRebuildsWithoutCasts() {
        // sign(DOUBLE) — DataFusion's UDF is `signum`, not `sign`. The PPL frontend emits
        // SqlStdOperatorTable.SIGN, but isthmus's default mapping serialises it as substrait
        // "sign" (which DataFusion does not register). The adapter is given the renamed
        // target (SignumFunction.FUNCTION → substrait "signum"); on a non-integer operand
        // there's no widening to do, but we still need to rebuild the call with the renamed
        // operator so isthmus emits the correct substrait name.
        RexNode dblOperand = rexBuilder.makeLiteral(BigDecimal.valueOf(-1.5), type(SqlTypeName.DOUBLE, false), false);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.SIGN, List.of(dblOperand));

        // Stand-in for SignumFunction.FUNCTION — any operator different from the original works
        // for this test (we just need to verify the adapter rebuilds when target != original.op).
        org.apache.calcite.sql.SqlOperator renameTarget = new org.apache.calcite.sql.SqlFunction(
            "SIGNUM",
            org.apache.calcite.sql.SqlKind.OTHER_FUNCTION,
            org.apache.calcite.sql.type.ReturnTypes.DOUBLE_NULLABLE,
            null,
            org.apache.calcite.sql.type.OperandTypes.NUMERIC,
            org.apache.calcite.sql.SqlFunctionCategory.NUMERIC
        );
        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(renameTarget);
        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertNotSame("Rename target produces a new call", original, adapted);
        RexCall renamed = (RexCall) adapted;
        assertSame("Operator is the renamed target", renameTarget, renamed.getOperator());
        assertEquals("No outer CAST — operand was already DOUBLE so no widening", 1, renamed.getOperands().size());
        assertSame("Operand passed through unchanged", dblOperand, renamed.getOperands().get(0));
        assertEquals(
            "Return type preserved (matches original.getType())",
            original.getType().getSqlTypeName(),
            renamed.getType().getSqlTypeName()
        );
    }

    public void testRenameOnIntegerOperandWrapsInCastsAndRenames() {
        // sign(INTEGER) — combines both behaviors: widen the operand to DOUBLE, rename the
        // operator to the rename target, then cast result back to INTEGER. Verifies the
        // integer-input path uses the rename target (not the original operator) for the
        // inner call.
        RexNode intOperand = rexBuilder.makeLiteral(-3, type(SqlTypeName.INTEGER, false), false);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.SIGN, List.of(intOperand));

        org.apache.calcite.sql.SqlOperator renameTarget = new org.apache.calcite.sql.SqlFunction(
            "SIGNUM",
            org.apache.calcite.sql.SqlKind.OTHER_FUNCTION,
            org.apache.calcite.sql.type.ReturnTypes.DOUBLE_NULLABLE,
            null,
            org.apache.calcite.sql.type.OperandTypes.NUMERIC,
            org.apache.calcite.sql.SqlFunctionCategory.NUMERIC
        );
        IntegerRoundingCastAdapter adapter = new IntegerRoundingCastAdapter(renameTarget);
        RexCall outerCast = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals("Outer is CAST", SqlKind.CAST, outerCast.getKind());
        assertEquals("Outer return type INTEGER (matching original)", SqlTypeName.INTEGER, outerCast.getType().getSqlTypeName());
        RexCall innerSignum = (RexCall) outerCast.getOperands().get(0);
        assertSame("Inner operator is the renamed target", renameTarget, innerSignum.getOperator());
        assertEquals("Inner operand widened to DOUBLE", SqlTypeName.DOUBLE, innerSignum.getOperands().get(0).getType().getSqlTypeName());
    }
}
