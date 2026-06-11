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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link LastDayAdapter}. The lowering is
 * {@code date_trunc('month', x) + INTERVAL 1 MONTH - INTERVAL 1 DAY}, so each test asserts the
 * rewrite contains both the {@code date_trunc} call and two {@code DATETIME_PLUS} layers — exact
 * date values are covered by the QA IT against a real DataFusion runtime.
 */
public class LastDayAdapterTests extends OpenSearchTestCase {

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

    private RexCall lastDayCall(SqlTypeName operandType) {
        SqlFunction op = new SqlFunction(
            "LAST_DAY",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RelDataType dateType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);
        RelDataType arg = typeFactory.createTypeWithNullability(typeFactory.createSqlType(operandType), true);
        return (RexCall) rexBuilder.makeCall(dateType, op, List.of(rexBuilder.makeInputRef(arg, 0)));
    }

    /** date_trunc + 2× DATETIME_PLUS appears for every operand type. */
    private void assertLastDayShape(RexNode result) {
        assertNotNull("expected date_trunc('month', ...)", findOp(result, DateTimeAdapters.LOCAL_DATE_TRUNC_OP));
        assertNotNull("expected DATETIME_PLUS (+1 month, -1 day)", findOp(result, SqlStdOperatorTable.DATETIME_PLUS));
    }

    public void testLastDayOnDateLowersWithoutAnchor() {
        RexCall call = lastDayCall(SqlTypeName.DATE);
        RexNode result = new LastDayAdapter().adapt(call, List.of(), cluster);
        assertTrue("LAST_DAY must be rewritten", result != call);
        assertLastDayShape(result);
    }

    public void testLastDayOnTimestampLowersWithoutAnchor() {
        RexCall call = lastDayCall(SqlTypeName.TIMESTAMP);
        RexNode result = new LastDayAdapter().adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertLastDayShape(result);
    }

    /**
     * VARCHAR / TIME bases must route through {@code coerceCharacterOperandToTimestamp} so the
     * operand handed to {@code date_trunc} is TIMESTAMP-typed (a direct CAST(time→timestamp) would
     * be rejected by DataFusion's optimizer; the helper uses today-date prefix + CONCAT instead).
     * The invariant we pin: the date_trunc call's value operand is TIMESTAMP, not the source type.
     */
    public void testLastDayOnVarcharAnchorsViaCoercion() {
        RexCall call = lastDayCall(SqlTypeName.VARCHAR);
        RexNode result = new LastDayAdapter().adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertLastDayShape(result);
        RexCall dateTrunc = findOp(result, DateTimeAdapters.LOCAL_DATE_TRUNC_OP);
        assertNotNull(dateTrunc);
        assertEquals(
            "date_trunc value operand must be TIMESTAMP after VARCHAR coercion",
            SqlTypeName.TIMESTAMP,
            dateTrunc.getOperands().get(1).getType().getSqlTypeName()
        );
    }

    public void testLastDayOnTimeAnchorsViaCoercion() {
        RexCall call = lastDayCall(SqlTypeName.TIME);
        RexNode result = new LastDayAdapter().adapt(call, List.of(), cluster);
        assertTrue(result != call);
        assertLastDayShape(result);
        RexCall dateTrunc = findOp(result, DateTimeAdapters.LOCAL_DATE_TRUNC_OP);
        assertNotNull(dateTrunc);
        // TIME path uses CONCAT(today, ' ', CAST(time AS VARCHAR)) → CAST TIMESTAMP — that CONCAT
        // is the load-bearing marker that the TIME-anchoring branch ran (vs the no-op DATE branch).
        assertNotNull("TIME operand must route through CONCAT-anchor", findOp(result, SqlStdOperatorTable.CONCAT));
        assertEquals(
            "date_trunc value operand must be TIMESTAMP after TIME coercion",
            SqlTypeName.TIMESTAMP,
            dateTrunc.getOperands().get(1).getType().getSqlTypeName()
        );
    }

    /** Wrong-arity input (zero or two operands) must pass through unchanged so the engine surfaces it. */
    public void testLastDayWrongArityPassesThrough() {
        SqlFunction op = new SqlFunction(
            "LAST_DAY",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DATE_NULLABLE,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RelDataType dateType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);
        RelDataType ts = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RexCall twoArg = (RexCall) rexBuilder.makeCall(
            dateType,
            op,
            List.of(rexBuilder.makeInputRef(ts, 0), rexBuilder.makeInputRef(ts, 1))
        );
        RexNode result = new LastDayAdapter().adapt(twoArg, List.of(), cluster);
        assertSame("two-arg LAST_DAY must pass through unchanged", twoArg, result);
    }
}
