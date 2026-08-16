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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link AddSubTimeAdapter}, {@link TimeDiffAdapter}, and {@link GetFormatAdapter}.
 * Shape-level assertions; exact values are covered by the QA ITs.
 */
public class TimeArithmeticAdapterTests extends OpenSearchTestCase {

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

    private RelDataType nullable(SqlTypeName t) {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(t), true);
    }

    private RexCall binaryCall(String name, RelDataType returnType, SqlTypeName op0, SqlTypeName op1) {
        SqlFunction op = new SqlFunction(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(returnType),
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode a = rexBuilder.makeInputRef(nullable(op0), 0);
        RexNode b = rexBuilder.makeInputRef(nullable(op1), 1);
        return (RexCall) rexBuilder.makeCall(returnType, op, List.of(a, b));
    }

    /** ADDTIME with a TIME result builds the value via maketime (not a CAST to TIME). */
    public void testAddTimeTimeResultUsesMaketime() {
        RexCall call = binaryCall("ADDTIME", nullable(SqlTypeName.TIME), SqlTypeName.TIME, SqlTypeName.TIME);
        RexNode result = new AddSubTimeAdapter(true).adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertNotNull("TIME result must use maketime", findOp(result, RustUdfDateTimeAdapters.LOCAL_MAKETIME_OP));
    }

    /** ADDTIME with a TIMESTAMP result goes through from_unixtime epoch arithmetic. */
    public void testAddTimeTimestampResultUsesFromUnixtime() {
        RexCall call = binaryCall("ADDTIME", nullable(SqlTypeName.TIMESTAMP), SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP);
        RexNode result = new AddSubTimeAdapter(true).adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertNotNull("TIMESTAMP result must use from_unixtime", findOp(result, RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP));
        assertNotNull("must read operand epochs via to_unixtime", findOp(result, UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP));
    }

    /** SUBTIME on a TIMESTAMP subtracts the delta (a MINUS appears over the epoch sum). */
    public void testSubTimeTimestampUsesMinus() {
        RexCall call = binaryCall("SUBTIME", nullable(SqlTypeName.TIMESTAMP), SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP);
        RexNode result = new AddSubTimeAdapter(false).adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertNotNull("subtract path must contain a MINUS", findOp(result, SqlStdOperatorTable.MINUS));
    }

    public void testTimeDiffUsesMaketimeOverMinus() {
        RexCall call = binaryCall("TIME_DIFF", nullable(SqlTypeName.TIME), SqlTypeName.TIME, SqlTypeName.TIME);
        RexNode result = new TimeDiffAdapter().adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertNotNull("TIMEDIFF must build a TIME via maketime", findOp(result, RustUdfDateTimeAdapters.LOCAL_MAKETIME_OP));
        assertNotNull("TIMEDIFF subtracts the two seconds-of-day", findOp(result, SqlStdOperatorTable.MINUS));
    }

    public void testGetFormatFoldsToLiteral() {
        RelDataType varchar = nullable(SqlTypeName.VARCHAR);
        SqlFunction op = new SqlFunction(
            "GET_FORMAT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varchar),
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode type = rexBuilder.makeLiteral("DATE", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode region = rexBuilder.makeLiteral("USA", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexCall call = (RexCall) rexBuilder.makeCall(varchar, op, List.of(type, region));
        RexNode result = new GetFormatAdapter().adapt(call, List.of(), cluster);
        // makeLiteral with a nullable type wraps the literal in a CAST; unwrap to the inner literal.
        RexLiteral literal = asLiteral(result);
        assertNotNull("GET_FORMAT must fold to a (possibly cast-wrapped) literal", literal);
        assertEquals("%m.%d.%Y", literal.getValueAs(String.class));
    }

    private static RexLiteral asLiteral(RexNode node) {
        if (node instanceof RexLiteral literal) {
            return literal;
        }
        if (node instanceof RexCall call && call.getKind() == SqlKind.CAST && call.getOperands().size() == 1) {
            return asLiteral(call.getOperands().get(0));
        }
        return null;
    }

    /**
     * GET_FORMAT(DATETIME, ...) re-maps the type key to TIMESTAMP under the hood — DATETIME is a
     * MySQL alias of TIMESTAMP and shares the same five region entries.
     */
    public void testGetFormatDatetimeAliasResolvesToTimestampEntry() {
        RelDataType varchar = nullable(SqlTypeName.VARCHAR);
        SqlFunction op = new SqlFunction(
            "GET_FORMAT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varchar),
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode type = rexBuilder.makeLiteral("DATETIME", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode region = rexBuilder.makeLiteral("ISO", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexCall call = (RexCall) rexBuilder.makeCall(varchar, op, List.of(type, region));
        RexNode result = new GetFormatAdapter().adapt(call, List.of(), cluster);
        RexLiteral literal = asLiteral(result);
        assertNotNull("GET_FORMAT(DATETIME, ISO) must fold to the TIMESTAMP|ISO format", literal);
        assertEquals("%Y-%m-%d %H:%i:%s", literal.getValueAs(String.class));
    }

    /** A non-literal first operand prevents plan-time fold; the call passes through unchanged. */
    public void testGetFormatNonLiteralOperandPassesThrough() {
        RelDataType varchar = nullable(SqlTypeName.VARCHAR);
        SqlFunction op = new SqlFunction(
            "GET_FORMAT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varchar),
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode columnRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode region = rexBuilder.makeLiteral("USA", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexCall call = (RexCall) rexBuilder.makeCall(varchar, op, List.of(columnRef, region));
        RexNode result = new GetFormatAdapter().adapt(call, List.of(), cluster);
        assertSame("non-literal type operand must leave GET_FORMAT unchanged", call, result);
    }

    public void testGetFormatUnknownPairPassesThrough() {
        RelDataType varchar = nullable(SqlTypeName.VARCHAR);
        SqlFunction op = new SqlFunction(
            "GET_FORMAT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varchar),
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode type = rexBuilder.makeLiteral("DATE", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode region = rexBuilder.makeLiteral("BOGUS", typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexCall call = (RexCall) rexBuilder.makeCall(varchar, op, List.of(type, region));
        RexNode result = new GetFormatAdapter().adapt(call, List.of(), cluster);
        assertSame("unknown (type, region) must pass through unchanged", call, result);
    }
}
