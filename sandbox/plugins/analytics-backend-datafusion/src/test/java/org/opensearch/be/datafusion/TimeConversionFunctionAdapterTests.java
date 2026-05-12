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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
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
 * Unit tests for {@link TimeConversionFunctionAdapter}. Verifies:
 * <ul>
 *   <li>Unary PPL calls ({@code ctime(value)} / {@code mktime(value)}) are rewritten to a
 *       2-arg call with the default format literal filled in.</li>
 *   <li>Binary PPL calls pass the user-supplied format through unchanged.</li>
 *   <li>Numeric value operands are coerced to VARCHAR</li>
 *   <li>Return type (VARCHAR for ctime, DOUBLE for mktime) is preserved.</li>
 * </ul>
 */
public class TimeConversionFunctionAdapterTests extends OpenSearchTestCase {

    private static final SqlFunction PPL_UNARY = new SqlFunction(
        "ppl_unary",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_NULLABLE,
        null,
        OperandTypes.family(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final SqlFunction PPL_BINARY = new SqlFunction(
        "ppl_binary",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_NULLABLE,
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

    /** ctime(value) with no explicit format → rewritten to ctime(value, "%m/%d/%Y %H:%M:%S"). */
    public void testUnaryCtimeFillsInDefaultFormat() {
        RelOptCluster cluster = cluster();
        RelDataType stringType = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode valueRef = new RexInputRef(0, stringType);
        RexCall original = (RexCall) cluster.getRexBuilder().makeCall(PPL_UNARY, List.of(valueRef));

        RexNode rewritten = new TimeConversionFunctionAdapter(TimeConversionFunctionAdapter.CTIME).adapt(original, List.of(), cluster);

        RexCall out = (RexCall) rewritten;
        assertEquals("ctime", out.getOperator().getName());
        assertEquals(2, out.getOperands().size());
        RexLiteral formatLit = (RexLiteral) out.getOperands().get(1);
        assertEquals(TimeConversionFunctionAdapter.DEFAULT_FORMAT, formatLit.getValueAs(String.class));
    }

    /** Explicit format operand is forwarded unchanged. */
    public void testBinaryMktimePreservesExplicitFormat() {
        RelOptCluster cluster = cluster();
        RelDataType stringType = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode valueRef = new RexInputRef(0, stringType);
        RexNode formatLit = cluster.getRexBuilder().makeLiteral("%Y-%m-%d");
        RexCall original = (RexCall) cluster.getRexBuilder().makeCall(PPL_BINARY, List.of(valueRef, formatLit));

        RexNode rewritten = new TimeConversionFunctionAdapter(TimeConversionFunctionAdapter.MKTIME).adapt(original, List.of(), cluster);

        RexCall out = (RexCall) rewritten;
        assertEquals("mktime", out.getOperator().getName());
        assertEquals(2, out.getOperands().size());
        assertSame("explicit format literal must be forwarded verbatim", formatLit, out.getOperands().get(1));
    }

    /** Numeric value operand gets wrapped in CAST(x AS VARCHAR). */
    public void testNumericValueIsCoercedToVarchar() {
        RelOptCluster cluster = cluster();
        RelDataType bigintType = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode epochSeconds = new RexInputRef(0, bigintType);
        RexCall original = (RexCall) cluster.getRexBuilder().makeCall(PPL_UNARY, List.of(epochSeconds));

        RexNode rewritten = new TimeConversionFunctionAdapter(TimeConversionFunctionAdapter.CTIME).adapt(original, List.of(), cluster);

        RexCall out = (RexCall) rewritten;
        RexNode normalized = out.getOperands().get(0);
        assertTrue("numeric operand must be wrapped in a CAST, got " + normalized, normalized instanceof RexCall);
        assertEquals(SqlKind.CAST, ((RexCall) normalized).getKind());
        assertEquals(SqlTypeName.VARCHAR, normalized.getType().getSqlTypeName());
    }

    /** Return type stays aligned with the enclosing Project's cached rowType. */
    public void testReturnTypePreserved() {
        RelOptCluster cluster = cluster();
        RelDataType stringType = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode valueRef = new RexInputRef(0, stringType);
        RexCall original = (RexCall) cluster.getRexBuilder().makeCall(PPL_UNARY, List.of(valueRef));

        RexNode rewritten = new TimeConversionFunctionAdapter(TimeConversionFunctionAdapter.MKTIME).adapt(original, List.of(), cluster);

        assertEquals(original.getType(), rewritten.getType());
    }

    /** Calls with 0 or 3+ operands pass through */
    public void testWrongArityPassesThrough() {
        RelOptCluster cluster = cluster();
        RelDataType stringType = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode arg1 = new RexInputRef(0, stringType);
        RexNode arg2 = new RexInputRef(1, stringType);
        RexNode arg3 = new RexInputRef(2, stringType);
        RexCall threeArg = (RexCall) cluster.getRexBuilder().makeCall(PPL_BINARY, List.of(arg1, arg2, arg3));

        RexNode rewritten = new TimeConversionFunctionAdapter(TimeConversionFunctionAdapter.CTIME).adapt(threeArg, List.of(), cluster);

        assertSame(threeArg, rewritten);
    }
}
