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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link EpochArithmeticAdapters}. These adapters lower PPL TO_DAYS / TO_SECONDS /
 * FROM_DAYS / TIME_TO_SEC to {@code to_unixtime} / {@code from_unixtime} plus integer arithmetic.
 * The tests assert the rewrite <em>shape</em> (the raw PPL op is replaced and the expected
 * arithmetic operator tops the tree) — exact numeric values are covered by the QA ITs against a
 * real DataFusion runtime.
 */
public class EpochArithmeticAdaptersTests extends OpenSearchTestCase {

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
        RexNode ref = rexBuilder.makeInputRef(arg, 0);
        return (RexCall) rexBuilder.makeCall(returnType, op, List.of(ref));
    }

    private RelDataType bigint() {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
    }

    /** Recursively search for the first RexCall using {@code operator}. */
    private static RexCall findOp(RexNode node, org.apache.calcite.sql.SqlOperator operator) {
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

    public void testToDaysLowersToDividePlus() {
        RexCall call = unaryCall("TO_DAYS", bigint(), SqlTypeName.TIMESTAMP);
        RexNode result = new EpochArithmeticAdapters.ToDaysAdapter().adapt(call, List.of(), cluster);
        assertTrue("TO_DAYS must be rewritten", result != call);
        assertNotNull("expected a PLUS (epochDays + 719528)", findOp(result, SqlStdOperatorTable.PLUS));
        assertNotNull("expected a DIVIDE (seconds / 86400)", findOp(result, SqlStdOperatorTable.DIVIDE));
        assertNotNull("expected to_unixtime", findOp(result, UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP));
    }

    public void testToSecondsLowersToPlusOverUnixtime() {
        RexCall call = unaryCall("TO_SECONDS", bigint(), SqlTypeName.TIMESTAMP);
        RexNode result = new EpochArithmeticAdapters.ToSecondsAdapter().adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertNotNull("expected a PLUS (seconds + 62167219200)", findOp(result, SqlStdOperatorTable.PLUS));
        assertNotNull("expected to_unixtime", findOp(result, UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP));
    }

    public void testFromDaysLowersToFromUnixtime() {
        RelDataType dateType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);
        SqlFunction op = new SqlFunction(
            "FROM_DAYS",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(dateType),
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode n = rexBuilder.makeExactLiteral(BigDecimal.valueOf(738049), typeFactory.createSqlType(SqlTypeName.BIGINT));
        RexCall call = (RexCall) rexBuilder.makeCall(dateType, op, List.of(n));
        RexNode result = new EpochArithmeticAdapters.FromDaysAdapter().adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertNotNull("expected from_unixtime", findOp(result, RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP));
        assertNotNull("expected a MULTIPLY (days * 86400)", findOp(result, SqlStdOperatorTable.MULTIPLY));
    }

    /**
     * A TIME operand must be anchored to a TIMESTAMP before to_unixtime (which rejects Time64).
     * The anchoring goes through the CONCAT(today, ' ', CAST(time AS VARCHAR)) → parse path, so the
     * to_unixtime operand must no longer be the raw TIME input ref.
     */
    public void testTimeToSecAnchorsTimeBeforeUnixtime() {
        RexCall call = unaryCall("TIME_TO_SEC", bigint(), SqlTypeName.TIME);
        RexNode result = new EpochArithmeticAdapters.TimeToSecAdapter().adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertNotNull("expected MOD (seconds % 86400)", findOp(result, SqlStdOperatorTable.MOD));
        RexCall unixtime = findOp(result, UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP);
        assertNotNull("expected to_unixtime", unixtime);
        // The to_unixtime operand must NOT be a raw TIME-typed value (that's the unsupported path).
        assertNotEquals(
            "to_unixtime operand must be anchored, not the raw TIME",
            SqlTypeName.TIME,
            unixtime.getOperands().get(0).getType().getSqlTypeName()
        );
    }

    public void testTimeToSecTimestampOperandNotCast() {
        RexCall call = unaryCall("TIME_TO_SEC", bigint(), SqlTypeName.TIMESTAMP);
        RexNode result = new EpochArithmeticAdapters.TimeToSecAdapter().adapt(call, List.of(), cluster);
        assertNotNull("expected MOD", findOp(result, SqlStdOperatorTable.MOD));
    }

    /** Builds {@code FN('<literal>')} with a VARCHAR string-literal operand. */
    private RexCall unaryStringLiteralCall(String name, String literal) {
        SqlFunction op = new SqlFunction(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(bigint()),
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode lit = rexBuilder.makeLiteral(literal, typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        return (RexCall) rexBuilder.makeCall(bigint(), op, List.of(lit));
    }

    /** TO_DAYS rejects a malformed date literal at plan time with the PPL format-hint message. */
    public void testToDaysInvalidLiteralThrowsFormatHint() {
        RexCall call = unaryStringLiteralCall("TO_DAYS", "2025-13-02");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new EpochArithmeticAdapters.ToDaysAdapter().adapt(call, List.of(), cluster)
        );
        assertTrue("message must carry the format hint, got: " + e.getMessage(), e.getMessage().contains("unsupported format"));
    }

    /** TO_SECONDS still accepts a valid full-datetime literal (Kind.DATE accepts date + datetime). */
    public void testToSecondsValidDatetimeLiteralPasses() {
        RexCall call = unaryStringLiteralCall("TO_SECONDS", "2020-09-16 07:40:00");
        RexNode result = new EpochArithmeticAdapters.ToSecondsAdapter().adapt(call, List.of(), cluster);
        assertNotNull("valid datetime literal must lower, not throw", findOp(result, SqlStdOperatorTable.PLUS));
    }
}
