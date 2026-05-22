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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.BinaryType;
import org.opensearch.analytics.schema.IpType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * Tests for {@link IpBinaryOutputCastRewriter}. Builds Calcite RelNode trees that match
 * (and don't match) {@code CAST(<IpType|BinaryType> AS VARCHAR)} in the output Project's
 * direct slots, and asserts the rewriter narrows precisely to those.
 */
public class IpBinaryOutputCastRewriterTests extends OpenSearchTestCase {

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

    /**
     * {@code CAST(<IpType> AS VARCHAR)} in an output Project slot is rewritten to an
     * {@code IP_TO_STRING_OP(col)} call. The result type stays VARCHAR so the
     * surrounding Project's row-type assertion is satisfied.
     */
    public void testIpCastIsRewrittenToIpToString() {
        RelDataType ipType = new IpType(true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RelNode values = singleRowWith("host", ipType);
        RexNode field = rexBuilder.makeInputRef(values, 0);
        RexNode castExpr = rexBuilder.makeCast(varcharType, field);
        RelNode project = LogicalProject.create(values, List.of(), List.of(castExpr), List.of("s"), Set.of());

        RelNode rewritten = IpBinaryOutputCastRewriter.rewrite(project);
        RexCall call = (RexCall) ((LogicalProject) rewritten).getProjects().get(0);

        assertEquals(
            "Slot operator must be IP_TO_STRING_OP",
            IpBinaryOutputCastRewriter.IP_TO_STRING_OP,
            call.getOperator()
        );
        assertEquals("Result type must remain VARCHAR", varcharType, call.getType());
        assertEquals(field.toString(), call.getOperands().get(0).toString());
    }

    /**
     * {@code CAST(<BinaryType> AS VARCHAR)} in an output Project slot is rewritten to a
     * {@code BINARY_TO_BASE64_OP(col)} call.
     */
    public void testBinaryCastIsRewrittenToBinaryToBase64() {
        RelDataType binType = new BinaryType(true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RelNode values = singleRowWith("blob", binType);
        RexNode field = rexBuilder.makeInputRef(values, 0);
        RexNode castExpr = rexBuilder.makeCast(varcharType, field);
        RelNode project = LogicalProject.create(values, List.of(), List.of(castExpr), List.of("s"), Set.of());

        RelNode rewritten = IpBinaryOutputCastRewriter.rewrite(project);
        RexCall call = (RexCall) ((LogicalProject) rewritten).getProjects().get(0);

        assertEquals(
            "Slot operator must be BINARY_TO_BASE64_OP",
            IpBinaryOutputCastRewriter.BINARY_TO_BASE64_OP,
            call.getOperator()
        );
        assertEquals(varcharType, call.getType());
        assertEquals(field.toString(), call.getOperands().get(0).toString());
    }

    /**
     * Casts whose target is not VARCHAR (e.g. IpType → BIGINT) are out of scope; the
     * rewriter must leave them untouched even when the source is one of our UDTs.
     */
    public void testNonVarcharTargetCastIsUntouched() {
        RelDataType ipType = new IpType(true);
        RelDataType bigintType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);

        RelNode values = singleRowWith("host", ipType);
        RexNode field = rexBuilder.makeInputRef(values, 0);
        RexNode castExpr = rexBuilder.makeCast(bigintType, field);
        RelNode project = LogicalProject.create(values, List.of(), List.of(castExpr), List.of("s"), Set.of());

        RelNode rewritten = IpBinaryOutputCastRewriter.rewrite(project);
        // No matching slots → tree must be returned identical.
        assertSame("Non-VARCHAR target cast must round-trip identical", project, rewritten);
    }

    /**
     * Plain VARBINARY (no UDT) source casts are out of scope — the rewriter must leave
     * generic binary→string casts to DataFusion's default cast kernel.
     */
    public void testPlainVarbinaryCastIsUntouched() {
        RelDataType varbinaryType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARBINARY), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RelNode values = singleRowWith("blob", varbinaryType);
        RexNode field = rexBuilder.makeInputRef(values, 0);
        RexNode castExpr = rexBuilder.makeCast(varcharType, field);
        RelNode project = LogicalProject.create(values, List.of(), List.of(castExpr), List.of("s"), Set.of());

        RelNode rewritten = IpBinaryOutputCastRewriter.rewrite(project);
        assertSame("Plain VARBINARY (no UDT) cast must round-trip identical", project, rewritten);
    }

    /**
     * TIMESTAMP→VARCHAR casts are handled by {@link DatetimeOutputCastRewriter}, not
     * this rewriter; verify no overlap regression — the IP/binary rewriter must leave
     * TIMESTAMP casts untouched.
     */
    public void testTimestampCastIsUntouched() {
        RelDataType timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), true);
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

        RelNode values = singleRowWith("ts", timestampType);
        RexNode field = rexBuilder.makeInputRef(values, 0);
        RexNode castExpr = rexBuilder.makeCast(varcharType, field);
        RelNode project = LogicalProject.create(values, List.of(), List.of(castExpr), List.of("s"), Set.of());

        RelNode rewritten = IpBinaryOutputCastRewriter.rewrite(project);
        assertSame("TIMESTAMP cast belongs to DatetimeOutputCastRewriter, not this one", project, rewritten);
    }

    private RelNode singleRowWith(String fieldName, RelDataType fieldType) {
        RelDataType rowType = typeFactory.builder().add(fieldName, fieldType).build();
        return LogicalValues.createEmpty(cluster, rowType);
    }
}
