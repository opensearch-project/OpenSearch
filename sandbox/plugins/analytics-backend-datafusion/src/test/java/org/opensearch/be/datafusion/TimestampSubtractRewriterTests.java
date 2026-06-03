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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * Tests for {@link TimestampSubtractRewriter}. A raw {@code timestamp - timestamp} uses the
 * built-in binary {@code MINUS}, which never reaches per-function adapter dispatch and has no
 * Substrait mapping. The rewriter turns it into {@code MINUS(to_unixtime(t1), to_unixtime(t2))}
 * (epoch-second difference), which is natively Substrait-convertible.
 */
public class TimestampSubtractRewriterTests extends OpenSearchTestCase {

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

    public void testTimestampMinusTimestampRewrittenToUnixtimeDifference() {
        RelDataType tsType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        // Input row exposes two TIMESTAMP columns so the MINUS operands are genuinely TIMESTAMP
        // (mirrors the real MAX(ts) - MIN(ts) shape); a string literal would stay CHAR/VARCHAR.
        RelDataType inputRow = typeFactory.builder().add("t1", tsType).add("t2", tsType).build();
        RelNode values = LogicalValues.createEmpty(cluster, inputRow);
        RexNode t1 = rexBuilder.makeInputRef(tsType, 0);
        RexNode t2 = rexBuilder.makeInputRef(tsType, 1);
        RexNode minus = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, t1, t2);
        RelNode project = LogicalProject.create(values, List.of(), List.of(minus), List.of("diff"), Set.of());

        RelNode rewritten = TimestampSubtractRewriter.rewrite(project);

        RexNode rewrittenExpr = ((LogicalProject) rewritten).getProjects().get(0);
        // Result is CAST(MINUS(to_unixtime(t1), to_unixtime(t2))) — unwrap the cast to the MINUS.
        RexCall outer = (RexCall) rewrittenExpr;
        RexCall inner = outer.getKind() == SqlKind.CAST ? (RexCall) outer.getOperands().get(0) : outer;
        assertEquals("inner op must be MINUS", SqlKind.MINUS, inner.getKind());
        for (RexNode operand : inner.getOperands()) {
            RexCall opCall = (RexCall) operand;
            assertSame(
                "each MINUS operand must be wrapped in to_unixtime",
                UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP,
                opCall.getOperator()
            );
        }
    }

    public void testNumericMinusIsUnchanged() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelNode values = LogicalValues.createOneRow(cluster);
        RexNode a = rexBuilder.makeExactLiteral(java.math.BigDecimal.TEN, intType);
        RexNode b = rexBuilder.makeExactLiteral(java.math.BigDecimal.ONE, intType);
        RexNode minus = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, a, b);
        RelNode project = LogicalProject.create(values, List.of(), List.of(minus), List.of("diff"), Set.of());

        RelNode rewritten = TimestampSubtractRewriter.rewrite(project);

        RexNode rewrittenExpr = ((LogicalProject) rewritten).getProjects().get(0);
        // No to_unixtime wrapping — a plain numeric MINUS is left intact.
        RexCall call = (RexCall) rewrittenExpr;
        assertEquals(SqlKind.MINUS, call.getKind());
        for (RexNode operand : call.getOperands()) {
            assertFalse("numeric operands must not be wrapped in to_unixtime", operand instanceof RexCall);
        }
    }
}
