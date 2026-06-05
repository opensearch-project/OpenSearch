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
 * Unit tests for {@link DivideAdapter}. PPL semantics require divide-by-zero
 * to return NULL, but substrait's {@code on_division_by_zero} option can't be
 * threaded through isthmus's emission, so the adapter wraps each
 * {@code DIVIDE(x, y)} in {@code CASE WHEN y = 0 THEN NULL ELSE DIVIDE(x, y) END}.
 * For known non-zero literal divisors the wrap is skipped so the substrait
 * shape stays the same as before this adapter existed.
 */
public class DivideAdapterTests extends OpenSearchTestCase {

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

    private RexCall buildDivide(RexNode lhs, RexNode rhs) {
        RelDataType retType = typeFactory.createTypeWithNullability(lhs.getType(), true);
        return (RexCall) rexBuilder.makeCall(retType, SqlStdOperatorTable.DIVIDE, List.of(lhs, rhs));
    }

    /**
     * Regression for engine-arrow-divzero q1: literal-zero integer divisor must
     * be wrapped in {@code CASE WHEN rhs = 0 THEN NULL ELSE DIVIDE(...) END}
     * so DataFusion's runtime sees NULL instead of raising
     * {@code Arrow error: Divide by zero error}.
     */
    public void testLiteralZeroIntegerDivisorWrapsInCase() {
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(22), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeExactLiteral(BigDecimal.ZERO, typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall original = buildDivide(lhs, rhs);

        RexNode adapted = new DivideAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall (the CASE wrap)", adapted instanceof RexCall);
        RexCall caseCall = (RexCall) adapted;
        assertEquals("wrap must be a CASE node", SqlKind.CASE, caseCall.getKind());
        assertTrue("CASE result must be nullable so NULL fits", caseCall.getType().isNullable());
    }

    /**
     * Regression for engine-arrow-divzero q3: literal-zero double divisor (the
     * {@code 22.0/0.0} sub-shape) must also be wrapped in CASE.
     */
    public void testLiteralZeroDoubleDivisorWrapsInCase() {
        RexNode lhs = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(22.0), typeFactory.createSqlType(SqlTypeName.DOUBLE));
        RexNode rhs = rexBuilder.makeApproxLiteral(BigDecimal.ZERO, typeFactory.createSqlType(SqlTypeName.DOUBLE));
        RexCall original = buildDivide(lhs, rhs);

        RexNode adapted = new DivideAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        assertEquals(SqlKind.CASE, ((RexCall) adapted).getKind());
    }

    /**
     * Non-zero literal divisor → no CASE wrap. Keeps substrait shape stable for
     * the {@code x / 5} pattern that's the common case in user queries.
     */
    public void testNonZeroLiteralDivisorSkipsCaseWrap() {
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(22), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(5), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall original = buildDivide(lhs, rhs);

        RexNode adapted = new DivideAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        assertEquals(
            "non-zero literal divisor must skip the CASE wrap and emit bare DIVIDE",
            SqlKind.DIVIDE,
            ((RexCall) adapted).getKind()
        );
    }

    /**
     * Column-ref divisor (runtime-unknown value) gets the CASE wrap because we
     * can't tell at plan time whether any row's value is zero.
     */
    public void testColumnRefDivisorWrapsInCase() {
        RelDataType intNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(22), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeInputRef(intNullable, 0);
        RexCall original = buildDivide(lhs, rhs);

        RexNode adapted = new DivideAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        assertEquals("column-ref divisor must get the CASE wrap", SqlKind.CASE, ((RexCall) adapted).getKind());
    }

}
