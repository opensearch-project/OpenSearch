/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.planner.adapter;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link NumericConversionFunctionAdapter}. Verifies that the PPL
 * {@code num / auto / memk / rmcomma / rmunit} unary calls are rewritten to their
 * corresponding synthetic {@link SqlFunction} constants, that the input operand is coerced
 * to VARCHAR, and that the return type is preserved.
 */
public class NumericConversionFunctionAdapterTests extends OpenSearchTestCase {

    /** Synthetic PPL-side operator used to build the "original" call feeding the adapter. */
    private static final SqlFunction PPL_CALL = new SqlFunction(
        "ppl_call",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.family(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private RelOptCluster cluster() {
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        return RelOptCluster.create(planner, rexBuilder);
    }

    /** Builds a one-operand RexCall with the given argument type, wrapped in PPL_CALL. */
    private RexCall callWithOperandType(RelOptCluster cluster, SqlTypeName operandType) {
        RelDataType type = cluster.getTypeFactory().createSqlType(operandType);
        RexNode operand = new RexInputRef(0, type);
        return (RexCall) cluster.getRexBuilder().makeCall(PPL_CALL, List.of(operand));
    }

    private void assertRewritesTo(SqlFunction target, String expectedName) {
        RelOptCluster cluster = cluster();
        RexCall original = callWithOperandType(cluster, SqlTypeName.VARCHAR);

        RexNode rewritten = new NumericConversionFunctionAdapter(target).adapt(original, List.of(), cluster);

        assertTrue("adapter must emit a RexCall, got " + rewritten, rewritten instanceof RexCall);
        RexCall out = (RexCall) rewritten;
        assertEquals(expectedName, out.getOperator().getName());
        assertEquals(1, out.getOperands().size());
        // Operand already VARCHAR — no CAST should be layered on top.
        assertTrue("VARCHAR input must pass through unchanged", out.getOperands().get(0) instanceof RexInputRef);
    }

    public void testNumRewrites() {
        assertRewritesTo(NumericConversionFunctionAdapter.NUM, "num");
    }

    public void testAutoRewrites() {
        assertRewritesTo(NumericConversionFunctionAdapter.AUTO, "auto");
    }

    public void testMemkRewrites() {
        assertRewritesTo(NumericConversionFunctionAdapter.MEMK, "memk");
    }

    public void testRmcommaRewrites() {
        assertRewritesTo(NumericConversionFunctionAdapter.RMCOMMA, "rmcomma");
    }

    public void testRmunitRewrites() {
        assertRewritesTo(NumericConversionFunctionAdapter.RMUNIT, "rmunit");
    }

    public void testDur2secRewrites() {
        assertRewritesTo(NumericConversionFunctionAdapter.DUR2SEC, "dur2sec");
    }

    public void testMstimeRewrites() {
        assertRewritesTo(NumericConversionFunctionAdapter.MSTIME, "mstime");
    }

    /** Numeric input should be wrapped in a CAST AS VARCHAR so the Rust UDF sees a string. */
    public void testNumericInputIsCoercedToVarchar() {
        RelOptCluster cluster = cluster();
        RexCall original = callWithOperandType(cluster, SqlTypeName.INTEGER);

        RexNode rewritten = new NumericConversionFunctionAdapter(NumericConversionFunctionAdapter.NUM).adapt(original, List.of(), cluster);

        RexCall out = (RexCall) rewritten;
        assertEquals("num", out.getOperator().getName());
        RexNode arg = out.getOperands().get(0);
        // makeCast produces a SqlKind.CAST call when the source / target types differ.
        assertTrue("numeric operand must be wrapped in a CAST, got " + arg, arg instanceof RexCall);
        assertEquals(SqlKind.CAST, ((RexCall) arg).getKind());
        assertEquals(SqlTypeName.VARCHAR, arg.getType().getSqlTypeName());
    }

    /** The original call's return type must be preserved so the enclosing Project's rowType
     *  cache (see AbstractNameMappingAdapter javadoc) stays consistent. */
    public void testReturnTypePreserved() {
        RelOptCluster cluster = cluster();
        RexCall original = callWithOperandType(cluster, SqlTypeName.VARCHAR);

        RexNode rewritten = new NumericConversionFunctionAdapter(NumericConversionFunctionAdapter.NUM).adapt(original, List.of(), cluster);

        assertEquals(original.getType(), rewritten.getType());
    }

    /** Arity mismatch pass through */
    public void testWrongArityPassesThrough() {
        RelOptCluster cluster = cluster();
        RelDataType varcharType = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode arg1 = new RexInputRef(0, varcharType);
        RexNode arg2 = new RexInputRef(1, varcharType);
        RexCall twoArg = (RexCall) cluster.getRexBuilder().makeCall(PPL_CALL, List.of(arg1, arg2));

        RexNode rewritten = new NumericConversionFunctionAdapter(NumericConversionFunctionAdapter.NUM).adapt(twoArg, List.of(), cluster);

        assertSame(twoArg, rewritten);
    }
}
