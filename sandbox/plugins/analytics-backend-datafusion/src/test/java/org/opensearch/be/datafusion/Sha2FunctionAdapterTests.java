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

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link Sha2FunctionAdapter}. Verifies that
 * {@code sha2(input, bitLen)} rewrites to {@code encode(digest(input, 'shaN'), 'hex')}
 * for the four supported bit lengths and passes the original call through on invalid
 * or non-literal bit-length arguments.
 */
public class Sha2FunctionAdapterTests extends OpenSearchTestCase {

    private static final SqlFunction SHA2_FN = new SqlFunction(
        "sha2",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR,
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

    private RexNode stringInputRef(RelOptCluster cluster) {
        RelDataType stringType = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        return new RexInputRef(0, stringType);
    }

    private RexCall sha2(RelOptCluster cluster, int bitLen) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexLiteral bitLenLit = rexBuilder.makeExactLiteral(BigDecimal.valueOf(bitLen));
        RexNode call = rexBuilder.makeCall(SHA2_FN, List.of(stringInputRef(cluster), bitLenLit));
        return (RexCall) call;
    }

    /** Rewrites sha2(x, N) → encode(digest(x, 'shaN'), 'hex') for every supported bit length. */
    public void testAllSupportedBitLengthsLowerToDigestEncode() {
        for (int bitLen : new int[] { 224, 256, 384, 512 }) {
            RelOptCluster cluster = cluster();
            RexCall original = sha2(cluster, bitLen);

            RexNode rewritten = new Sha2FunctionAdapter().adapt(original, List.of(), cluster);

            // Outer call must be encode(..., 'hex').
            assertTrue("expected RexCall for bitLen=" + bitLen, rewritten instanceof RexCall);
            RexCall outer = (RexCall) rewritten;
            assertEquals("encode", outer.getOperator().getName());
            assertEquals(2, outer.getOperands().size());
            RexLiteral hexLit = (RexLiteral) outer.getOperands().get(1);
            assertEquals("hex", hexLit.getValueAs(String.class));

            // Inner call must be digest(x, 'shaN').
            RexCall inner = (RexCall) outer.getOperands().get(0);
            assertEquals("digest", inner.getOperator().getName());
            assertEquals(2, inner.getOperands().size());
            assertTrue(inner.getOperands().get(0) instanceof RexInputRef);
            RexLiteral algoLit = (RexLiteral) inner.getOperands().get(1);
            assertEquals("sha" + bitLen, algoLit.getValueAs(String.class));
        }
    }

    /** The original call's return type must be preserved so the enclosing Project's rowType stays consistent. */
    public void testReturnTypePreserved() {
        RelOptCluster cluster = cluster();
        RexCall original = sha2(cluster, 256);

        RexNode rewritten = new Sha2FunctionAdapter().adapt(original, List.of(), cluster);

        assertEquals(original.getType(), rewritten.getType());
    }

    /** Unsupported bit lengths fall through unchanged so the planner raises a clear error. */
    public void testUnsupportedBitLengthPassesThrough() {
        RelOptCluster cluster = cluster();
        RexCall original = sha2(cluster, 128); // not in {224, 256, 384, 512}

        RexNode rewritten = new Sha2FunctionAdapter().adapt(original, List.of(), cluster);

        assertSame(original, rewritten);
    }

    /** Non-literal bit-length operand (e.g. a column reference) can't be rewritten; pass through. */
    public void testNonLiteralBitLengthPassesThrough() {
        RelOptCluster cluster = cluster();
        RelDataType stringType = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RelDataType intType = cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        RexNode input = new RexInputRef(0, stringType);
        RexNode dynamicBitLen = new RexInputRef(1, intType);
        RexCall original = (RexCall) cluster.getRexBuilder().makeCall(SHA2_FN, List.of(input, dynamicBitLen));

        RexNode rewritten = new Sha2FunctionAdapter().adapt(original, List.of(), cluster);

        assertSame(original, rewritten);
    }

    /** Arity mismatches (1-arg or 3-arg calls that somehow reach the adapter) fall through. */
    public void testWrongArityPassesThrough() {
        RelOptCluster cluster = cluster();
        RexCall single = (RexCall) cluster.getRexBuilder().makeCall(SHA2_FN, List.of(stringInputRef(cluster)));

        RexNode rewritten = new Sha2FunctionAdapter().adapt(single, List.of(), cluster);

        assertSame(single, rewritten);
    }
}
