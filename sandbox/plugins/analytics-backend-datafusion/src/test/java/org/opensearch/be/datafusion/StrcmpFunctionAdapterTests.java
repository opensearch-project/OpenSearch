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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link StrcmpFunctionAdapter}.
 *
 * <p>The adapter decomposes {@code strcmp(a, b)} into a CASE expression using built-in
 * comparison operators ({@code <}, {@code =}) and swaps the arguments to undo the PPL
 * frontend's reversal. These tests verify the CASE shape and argument swap.
 */
public class StrcmpFunctionAdapterTests extends OpenSearchTestCase {

    private static final SqlFunction STRCMP = new SqlFunction(
        "STRCMP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
        null,
        OperandTypes.family(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private final StrcmpFunctionAdapter adapter = new StrcmpFunctionAdapter();

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

    private RexNode varcharInputRef(int index) {
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        return rexBuilder.makeInputRef(varcharType, index);
    }

    /** The adapter produces a CASE expression with INTEGER return type. */
    public void testTwoArgProducesCaseExpression() {
        RexNode arg0 = rexBuilder.makeLiteral("Amber");
        RexNode arg1 = varcharInputRef(0);
        RexCall call = (RexCall) rexBuilder.makeCall(STRCMP, arg0, arg1);

        RexNode out = adapter.adapt(call, List.of(), cluster);

        assertTrue("result must be a RexCall", out instanceof RexCall);
        RexCall outCall = (RexCall) out;
        assertEquals("decomposed to CASE", SqlKind.CASE, outCall.getKind());
        assertEquals("return type is INTEGER", SqlTypeName.INTEGER, outCall.getType().getSqlTypeName());
        // CASE has 7 operands: (anyNull, nullLit, lessThan, neg1, equalTo, zero, one)
        assertEquals("CASE has 7 operands (3 WHEN/THEN pairs + ELSE)", 7, outCall.getOperands().size());
    }

    /** Arguments are swapped — arg1 becomes 'a' (lhs) and arg0 becomes 'b' (rhs) in the comparisons. */
    public void testArgumentsAreSwapped() {
        RexNode arg0 = rexBuilder.makeLiteral("literal_rhs");
        RexNode arg1 = varcharInputRef(0); // column — should become lhs after swap
        RexCall call = (RexCall) rexBuilder.makeCall(STRCMP, arg0, arg1);

        RexNode out = adapter.adapt(call, List.of(), cluster);

        RexCall caseCall = (RexCall) out;
        // The LESS_THAN comparison is at operand index 2: WHEN a < b THEN -1
        // After swap: a = arg1 (inputRef), b = arg0 (literal)
        RexCall lessThan = (RexCall) caseCall.getOperands().get(2);
        assertEquals(SqlKind.LESS_THAN, lessThan.getKind());
        // lhs of < should be the column (arg1), rhs should be the literal (arg0)
        assertSame("lhs of < is the column (original arg1)", arg1, lessThan.getOperands().get(0));
        assertSame("rhs of < is the literal (original arg0)", arg0, lessThan.getOperands().get(1));
    }

    /** Non-standard arity (e.g. 1 arg) passes through unchanged. */
    public void testSingleArgPassesThrough() {
        RexCall call = (RexCall) rexBuilder.makeCall(STRCMP, varcharInputRef(0));

        RexNode out = adapter.adapt(call, List.of(), cluster);

        assertSame("non-2-arg call passes through unchanged", call, out);
    }
}
