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
 * Tests for {@link DivideAdapter}. Wraps {@code DIVIDE(x, y)} in
 * {@code CASE WHEN y = 0 THEN NULL ELSE DIVIDE(x, y) END} so divide-by-zero
 * yields NULL. Skips the wrap for known non-zero literal divisors.
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

    /** Literal-zero INTEGER divisor → CASE wrap. */
    public void testLiteralZeroIntegerDivisorWrapsInCase() {
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(22), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeExactLiteral(BigDecimal.ZERO, typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall original = buildDivide(lhs, rhs);

        RexNode adapted = new DivideAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall caseCall = (RexCall) adapted;
        assertEquals(SqlKind.CASE, caseCall.getKind());
        assertTrue(caseCall.getType().isNullable());
    }

    /** Literal-zero DOUBLE divisor → CASE wrap. */
    public void testLiteralZeroDoubleDivisorWrapsInCase() {
        RexNode lhs = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(22.0), typeFactory.createSqlType(SqlTypeName.DOUBLE));
        RexNode rhs = rexBuilder.makeApproxLiteral(BigDecimal.ZERO, typeFactory.createSqlType(SqlTypeName.DOUBLE));
        RexCall original = buildDivide(lhs, rhs);

        RexNode adapted = new DivideAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        assertEquals(SqlKind.CASE, ((RexCall) adapted).getKind());
    }

    /** Non-zero literal divisor → bare DIVIDE (skip the wrap). */
    public void testNonZeroLiteralDivisorSkipsCaseWrap() {
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(22), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(5), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexCall original = buildDivide(lhs, rhs);

        RexNode adapted = new DivideAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        assertEquals(SqlKind.DIVIDE, ((RexCall) adapted).getKind());
    }

    /** Column-ref divisor → CASE wrap (zero unknown until runtime). */
    public void testColumnRefDivisorWrapsInCase() {
        RelDataType intNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode lhs = rexBuilder.makeExactLiteral(BigDecimal.valueOf(22), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode rhs = rexBuilder.makeInputRef(intNullable, 0);
        RexCall original = buildDivide(lhs, rhs);

        RexNode adapted = new DivideAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        assertEquals(SqlKind.CASE, ((RexCall) adapted).getKind());
    }
}
