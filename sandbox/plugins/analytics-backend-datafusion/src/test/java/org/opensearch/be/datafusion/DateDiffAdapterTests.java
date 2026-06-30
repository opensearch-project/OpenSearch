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

import java.util.List;

/**
 * Unit tests for {@link DateDiffAdapter}. PPL {@code DATEDIFF(a, b)} is the whole-day
 * difference between the calendar dates of {@code a} and {@code b} (arg1 − arg2), so the
 * adapter must truncate each operand to its day before differencing — not subtract raw
 * instants. The lowering is
 * {@code FLOOR(to_unixtime(date_trunc('day', a)) / 86400) - FLOOR(to_unixtime(date_trunc('day', b)) / 86400)}.
 */
public class DateDiffAdapterTests extends OpenSearchTestCase {

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

    private SqlFunction datediffOp() {
        return new SqlFunction(
            "DATEDIFF",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT_FORCE_NULLABLE,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.TIMEDATE
        );
    }

    private RexCall buildDateDiff() {
        RelDataType ts = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RexNode a = rexBuilder.makeInputRef(ts, 0);
        RexNode b = rexBuilder.makeInputRef(ts, 1);
        RelDataType ret = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        return (RexCall) rexBuilder.makeCall(ret, datediffOp(), List.of(a, b));
    }

    /** DATEDIFF(a, b) lowers to a subtraction of two day-counts. */
    public void testDateDiffLowersToDayCountSubtraction() {
        RexNode result = new DateDiffAdapter().adapt(buildDateDiff(), List.of(), cluster);
        assertTrue("expected a rewritten call, got: " + result, result instanceof RexCall);
        // The top of the rewrite is a MINUS (possibly wrapped in a CAST to the declared return type).
        RexCall out = (RexCall) result;
        RexCall minus = out.getOperator() == SqlStdOperatorTable.MINUS ? out : firstMinusOperand(out);
        assertNotNull("rewrite must contain a MINUS of two day-counts", minus);
        assertEquals(SqlStdOperatorTable.MINUS, minus.getOperator());
    }

    /** The rewrite must not leave the raw DATEDIFF operator in place. */
    public void testDateDiffDoesNotPassThrough() {
        RexCall call = buildDateDiff();
        RexNode result = new DateDiffAdapter().adapt(call, List.of(), cluster);
        assertTrue("DATEDIFF must be rewritten, not returned unchanged", result != call);
    }

    /**
     * TIME operands route through {@code coerceCharacterOperandToTimestamp}: {@code to_unixtime}
     * rejects a Time64, so each TIME must be anchored to a TIMESTAMP first. The to_unixtime
     * operand must therefore NOT be a raw TIME-typed value.
     */
    public void testDateDiffOnTimeOperandsAnchorsBeforeUnixtime() {
        RelDataType time = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME), true);
        RelDataType ret = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RexNode a = rexBuilder.makeInputRef(time, 0);
        RexNode b = rexBuilder.makeInputRef(time, 1);
        RexCall call = (RexCall) rexBuilder.makeCall(ret, datediffOp(), List.of(a, b));

        RexNode result = new DateDiffAdapter().adapt(call, List.of(), cluster);

        // Walk the tree, find every to_unixtime call — none should have a TIME-typed operand.
        assertNoToUnixtimeOnRawTime(result);
    }

    private static void assertNoToUnixtimeOnRawTime(RexNode node) {
        if (!(node instanceof RexCall call)) {
            return;
        }
        if (call.getOperator() == UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP) {
            assertNotEquals(
                "to_unixtime operand must be anchored, not a raw TIME",
                SqlTypeName.TIME,
                call.getOperands().get(0).getType().getSqlTypeName()
            );
        }
        for (RexNode op : call.getOperands()) {
            assertNoToUnixtimeOnRawTime(op);
        }
    }

    private static RexCall firstMinusOperand(RexCall call) {
        for (RexNode op : call.getOperands()) {
            if (op instanceof RexCall c) {
                if (c.getOperator() == SqlStdOperatorTable.MINUS) {
                    return c;
                }
                RexCall nested = firstMinusOperand(c);
                if (nested != null) {
                    return nested;
                }
            }
        }
        return null;
    }
}
