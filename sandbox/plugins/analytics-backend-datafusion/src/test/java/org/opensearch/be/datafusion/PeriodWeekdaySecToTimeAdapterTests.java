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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link WeekdayAdapter}, {@link SecToTimeAdapter}, and {@link PeriodArithmeticAdapters}.
 * They assert the rewrite <em>shape</em> (raw PPL op replaced, expected primitives present); exact
 * numeric values are covered by the QA ITs against a real DataFusion runtime.
 */
public class PeriodWeekdaySecToTimeAdapterTests extends OpenSearchTestCase {

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

    private static RexCall findOp(RexNode node, SqlOperator operator) {
        if (node instanceof RexCall call) {
            if (call.getOperator() == operator) {
                return call;
            }
            for (RexNode op : call.getOperands()) {
                RexCall found = findOp(op, operator);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    private RexCall unaryCall(String name, RelDataType returnType, SqlTypeName operandType) {
        SqlFunction op = new SqlFunction(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(returnType),
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RelDataType arg = typeFactory.createTypeWithNullability(typeFactory.createSqlType(operandType), true);
        return (RexCall) rexBuilder.makeCall(returnType, op, List.of(rexBuilder.makeInputRef(arg, 0)));
    }

    private RexCall binaryIntCall(String name) {
        RelDataType intType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        SqlFunction op = new SqlFunction(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE,
            null,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode a = rexBuilder.makeExactLiteral(BigDecimal.valueOf(200801), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode b = rexBuilder.makeExactLiteral(BigDecimal.valueOf(2), typeFactory.createSqlType(SqlTypeName.INTEGER));
        return (RexCall) rexBuilder.makeCall(intType, op, List.of(a, b));
    }

    private RelDataType bigint() {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
    }

    private RelDataType time() {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME), true);
    }

    public void testWeekdayRemapsDow() {
        RexCall call = unaryCall("WEEKDAY", typeFactory.createSqlType(SqlTypeName.INTEGER), SqlTypeName.TIMESTAMP);
        RexNode result = new WeekdayAdapter().adapt(call, List.of(), cluster);
        assertTrue("WEEKDAY must be rewritten", result != call);
        assertNotNull("expected date_part('dow', ...)", findOp(result, SqlLibraryOperators.DATE_PART));
        assertNotNull("expected MOD 7 remap", findOp(result, SqlStdOperatorTable.MOD));
    }

    public void testSecToTimeUsesMaketimeNotCast() {
        RexCall call = unaryCall("SEC_TO_TIME", typeFactory.createSqlType(SqlTypeName.TIME), SqlTypeName.BIGINT);
        RexNode result = new SecToTimeAdapter().adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertNotNull("expected maketime(h,m,s)", findOp(result, RustUdfDateTimeAdapters.LOCAL_MAKETIME_OP));
        assertNotNull("expected MOD (wrap to a day / decompose)", findOp(result, SqlStdOperatorTable.MOD));
    }

    public void testPeriodAddLowersToIntegerArithmetic() {
        RexCall call = binaryIntCall("PERIOD_ADD");
        RexNode result = new PeriodArithmeticAdapters.PeriodAddAdapter().adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertNotNull("expected DIVIDE (month index / 12)", findOp(result, SqlStdOperatorTable.DIVIDE));
        assertNotNull("expected MOD (month within year)", findOp(result, SqlStdOperatorTable.MOD));
    }

    public void testPeriodDiffLowersToSubtraction() {
        RexCall call = binaryIntCall("PERIOD_DIFF");
        RexNode result = new PeriodArithmeticAdapters.PeriodDiffAdapter().adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertNotNull("expected MINUS (monthIndex(p1) - monthIndex(p2))", findOp(result, SqlStdOperatorTable.MINUS));
    }
}
