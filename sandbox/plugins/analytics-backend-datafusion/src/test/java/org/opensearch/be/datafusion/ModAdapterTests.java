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
 * Unit tests for {@link ModAdapter}. PPL semantics require {@code mod(x, 0)}
 * to return NULL; the adapter wraps every {@code MOD(x, y)} in
 * {@code CASE WHEN y = 0 THEN NULL ELSE MOD(x, y) END} so DataFusion's runtime
 * never raises {@code Arrow error: Divide by zero error}.
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

    /**
     * Regression for engine-arrow-divzero q5: literal-zero divisor must wrap
     * the MOD call in {@code CASE WHEN rhs = 0 THEN NULL ELSE MOD(...) END}
     * so DataFusion's Arrow runtime returns NULL instead of crashing the
     * streaming fragment.
     */
    public void testLiteralZeroDivisorWrapsInCase() {
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(5), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeExactLiteral(BigDecimal.ZERO, typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall original = buildMod(lhs, rhs);

        RexNode adapted = new ModAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall (the CASE wrap)", adapted instanceof RexCall);
        RexCall caseCall = (RexCall) adapted;
        assertEquals("wrap must be a CASE node", SqlKind.CASE, caseCall.getKind());
        assertTrue("CASE result must be nullable so NULL fits", caseCall.getType().isNullable());
    }

    /**
     * Non-zero literal divisor still gets the CASE wrap (ModAdapter wraps
     * unconditionally — unlike DivideAdapter which short-circuits non-zero
     * literals — because mod's substrait emit also lacks the
     * {@code on_domain_error: NULL} option threading).
     */
    public void testNonZeroLiteralDivisorAlsoWrapsInCase() {
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(5), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(3), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall original = buildMod(lhs, rhs);

        RexNode adapted = new ModAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        assertEquals(SqlKind.CASE, ((RexCall) adapted).getKind());
    }

    /**
     * Mixed-numeric operands (e.g. {@code FLOAT % INTEGER}) widen to the higher
     * rank type before MOD emission so substrait's signature matcher can bind
     * the call. Substrait's stock {@code modulus} extension is integer-only;
     * {@code opensearch_arithmetic_overloads.yaml} supplies the fp signatures.
     */
    public void testMixedNumericOperandsWidenToCommonType() {
        RexNode lhs = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(5.5), typeFactory.createSqlType(SqlTypeName.FLOAT));
        RexNode rhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(2), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall original = buildMod(lhs, rhs);

        RexNode adapted = new ModAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        // CASE(EQUALS(rhs, 0), null, MOD(widenedLhs, widenedRhs))
        RexCall caseCall = (RexCall) adapted;
        assertEquals(SqlKind.CASE, caseCall.getKind());
        // The MOD branch is the third operand of CASE (then-arm structure: cond, null, expr).
        RexNode modBranch = caseCall.getOperands().get(2);
        // It may be wrapped in a CAST for nullability, but the inner call must be MOD.
        RexNode innerMod = modBranch instanceof RexCall mc && mc.getKind() != SqlKind.MOD ? mc.getOperands().get(0) : modBranch;
        assertTrue(innerMod instanceof RexCall);
        assertEquals("mixed-type MOD must use SqlStdOperatorTable.MOD after widen", SqlKind.MOD, ((RexCall) innerMod).getKind());
        // The widened operands inside MOD must share a type.
        RexCall innerModCall = (RexCall) innerMod;
        assertEquals(
            "after widen, lhs and rhs must share a numeric type",
            innerModCall.getOperands().get(0).getType().getSqlTypeName(),
            innerModCall.getOperands().get(1).getType().getSqlTypeName()
        );
    }

    /**
     * Column-ref divisor wraps in CASE so the runtime per-row zero-check fires.
     */
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
