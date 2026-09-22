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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link TimeOfDayLowering}. The two helpers each emit a floor-mod chain
 * ({@code ((s MOD 86400) + 86400) MOD 86400}) so negative epochs (pre-1970) wrap the MySQL way
 * rather than yielding a negative seconds-of-day. The shape these tests pin is the number of
 * {@code MOD} / {@code PLUS} / {@code DIVIDE} nodes — exact numeric values come out at runtime
 * and are covered by the QA ITs (e.g. {@code testSecToTimeWrapsOverOneDay}).
 */
public class TimeOfDayLoweringTests extends OpenSearchTestCase {

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

    private static int countOps(RexNode node, SqlOperator operator) {
        if (!(node instanceof RexCall call)) {
            return 0;
        }
        int sum = call.getOperator() == operator ? 1 : 0;
        for (RexNode op : call.getOperands()) {
            sum += countOps(op, operator);
        }
        return sum;
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

    /**
     * {@code secondsOfDay(timestamp)} emits the floor-mod chain
     * {@code ((to_unixtime(x) MOD 86400) + 86400) MOD 86400} — exactly two MODs and one PLUS, with
     * a single {@code to_unixtime} reading the operand once.
     */
    public void testSecondsOfDayEmitsFloorModChain() {
        RelDataType ts = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RexNode operand = rexBuilder.makeInputRef(ts, 0);

        RexNode result = TimeOfDayLowering.secondsOfDay(operand, cluster);

        assertEquals("floor-mod requires exactly 2 MOD ops", 2, countOps(result, SqlStdOperatorTable.MOD));
        assertEquals("floor-mod requires exactly 1 PLUS op", 1, countOps(result, SqlStdOperatorTable.PLUS));
        assertEquals("operand should be read via to_unixtime once", 1, countOps(result, UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP));
    }

    /**
     * {@code secondsToTime} emits a normalized hour/minute/second decomposition wrapped in a
     * {@code maketime} call. The hour/minute/second branches each reference {@code norm} (the
     * floor-mod chain {@code ((s MOD 86400) + 86400) MOD 86400}) independently — RexNode trees
     * don't share subtrees, so the chain is duplicated three times: 3 × (2 MOD + 1 PLUS) = 6
     * MOD + 3 PLUS from the floor-mod chains, plus 2 extra MODs for the minute/second tails
     * = 8 MOD total. Two DIVIDEs: hour = norm/3600 and minute = (norm % 3600) / 60.
     */
    public void testSecondsToTimeNormalizesAndBuildsMaketime() {
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType time = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME), true);
        RexNode seconds = rexBuilder.makeInputRef(bigint, 0);

        RexNode result = TimeOfDayLowering.secondsToTime(seconds, time, cluster);

        assertTrue("result must be a RexCall", result instanceof RexCall);
        RexCall top = (RexCall) result;
        assertSame("maketime must be the top of the rewrite", RustUdfDateTimeAdapters.LOCAL_MAKETIME_OP, top.getOperator());
        assertEquals("maketime takes (hour, minute, second)", 3, top.getOperands().size());
        // 3 × floor-mod chain (each branch references `norm` independently) + 2 minute/second tails.
        assertEquals("expected 8 MOD ops (3× floor-mod + minute-mod + second-mod)", 8, countOps(result, SqlStdOperatorTable.MOD));
        assertEquals("expected 3 PLUS ops (floor-mod +86400 in each of 3 branches)", 3, countOps(result, SqlStdOperatorTable.PLUS));
        assertEquals("expected 2 DIVIDE ops (hour and minute)", 2, countOps(result, SqlStdOperatorTable.DIVIDE));
    }

    /** {@code secondsToTime} preserves the caller's TIME type on the maketime call's return. */
    public void testSecondsToTimeReturnTypeMatchesCallerType() {
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType time = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME), true);
        RexNode seconds = rexBuilder.makeInputRef(bigint, 0);

        RexNode result = TimeOfDayLowering.secondsToTime(seconds, time, cluster);

        assertEquals("result type must equal the timeType arg", time, result.getType());
    }

    /** {@code secondsOfDay} on a VARCHAR / TIME operand routes through coerceCharacterOperandToTimestamp. */
    public void testSecondsOfDayAnchorsCharacterOperand() {
        RelDataType varchar = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        RexNode operand = rexBuilder.makeInputRef(varchar, 0);

        RexNode result = TimeOfDayLowering.secondsOfDay(operand, cluster);

        // Anchoring path emits CONCAT('today', ...) / CAST AS TIMESTAMP — to_unixtime sees a TIMESTAMP, not VARCHAR.
        RexCall toUnixtime = findOp(result, UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP);
        assertNotNull("expected to_unixtime in the rewrite", toUnixtime);
        assertNotEquals(
            "to_unixtime operand must be anchored, not the raw VARCHAR",
            SqlTypeName.VARCHAR,
            toUnixtime.getOperands().get(0).getType().getSqlTypeName()
        );
    }
}
