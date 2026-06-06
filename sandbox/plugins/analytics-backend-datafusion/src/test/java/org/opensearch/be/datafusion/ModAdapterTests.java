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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Tests for {@link ModAdapter}. Wraps every {@code MOD(x, y)} in
 * {@code CASE WHEN y = 0 THEN NULL ELSE MOD(x, y) END} so {@code mod(x, 0)} yields NULL.
 */
public class ModAdapterTests extends OpenSearchTestCase {

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

    private RexCall buildMod(RexNode lhs, RexNode rhs) {
        RelDataType retType = typeFactory.createTypeWithNullability(lhs.getType(), true);
        return (RexCall) rexBuilder.makeCall(retType, SqlStdOperatorTable.MOD, List.of(lhs, rhs));
    }

    /** Literal-zero divisor → CASE wrap. */
    public void testLiteralZeroDivisorWrapsInCase() {
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(5), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeExactLiteral(BigDecimal.ZERO, typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall original = buildMod(lhs, rhs);

        RexNode adapted = new ModAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall caseCall = (RexCall) adapted;
        assertEquals(SqlKind.CASE, caseCall.getKind());
        assertTrue(caseCall.getType().isNullable());
    }

    /** Non-zero literal divisor still gets the CASE wrap (unconditional, unlike DivideAdapter). */
    public void testNonZeroLiteralDivisorAlsoWrapsInCase() {
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(5), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(3), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall original = buildMod(lhs, rhs);

        RexNode adapted = new ModAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        assertEquals(SqlKind.CASE, ((RexCall) adapted).getKind());
    }

    /** Mixed-numeric operands (e.g. FLOAT % INTEGER) widen to a common type before MOD. */
    public void testMixedNumericOperandsWidenToCommonType() {
        RexNode lhs = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(5.5), typeFactory.createSqlType(SqlTypeName.FLOAT));
        RexNode rhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(2), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall original = buildMod(lhs, rhs);

        RexNode adapted = new ModAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall caseCall = (RexCall) adapted;
        assertEquals(SqlKind.CASE, caseCall.getKind());
        // CASE then-arm is the third operand; may be wrapped in a CAST for nullability.
        RexNode modBranch = caseCall.getOperands().get(2);
        RexNode innerMod = modBranch instanceof RexCall mc && mc.getKind() != SqlKind.MOD ? mc.getOperands().get(0) : modBranch;
        assertTrue(innerMod instanceof RexCall);
        RexCall innerModCall = (RexCall) innerMod;
        assertEquals(SqlKind.MOD, innerModCall.getKind());
        assertEquals(
            innerModCall.getOperands().get(0).getType().getSqlTypeName(),
            innerModCall.getOperands().get(1).getType().getSqlTypeName()
        );
    }

    /** Column-ref divisor → CASE wrap (zero unknown until runtime). */
    public void testColumnRefDivisorWrapsInCase() {
        RelDataType intNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(5), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeInputRef(intNullable, 0);
        RexCall original = buildMod(lhs, rhs);

        RexNode adapted = new ModAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        assertEquals(SqlKind.CASE, ((RexCall) adapted).getKind());
    }
}
