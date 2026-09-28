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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link Expm1Adapter}. PPL's {@code EXPM1(x)} UDF is defined as
 * {@code exp(x) - 1}. DataFusion's substrait consumer has no {@code expm1} scalar
 * function, but it recognises {@code exp} and {@code subtract}; the adapter
 * expands the UDF to an explicit {@code MINUS(EXP(x), 1)} tree so the plan
 * serialises to native Substrait primitives.
 */
public class Expm1AdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType doubleType;
    private SqlUserDefinedFunction expm1Udf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        expm1Udf = new SqlUserDefinedFunction(
            new SqlIdentifier("EXPM1", SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DOUBLE_NULLABLE,
            null,
            null,
            null
        );
    }

    public void testExpm1RewrittenAsExpMinusOne() {
        RexNode arg = rexBuilder.makeInputRef(doubleType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(expm1Udf, List.of(arg));

        RexNode adapted = new Expm1Adapter().adapt(original, List.of(), cluster);

        // Expected tree: MINUS(EXP(arg), 1)
        assertTrue("expected a MINUS RexCall", adapted instanceof RexCall);
        RexCall minus = (RexCall) adapted;
        assertSame("outermost operator must be MINUS", SqlStdOperatorTable.MINUS, minus.getOperator());
        assertEquals(2, minus.getOperands().size());

        RexNode left = minus.getOperands().get(0);
        assertTrue("left operand of MINUS must be a RexCall", left instanceof RexCall);
        RexCall expCall = (RexCall) left;
        assertSame("left operand must be EXP(...)", SqlStdOperatorTable.EXP, expCall.getOperator());
        assertEquals(1, expCall.getOperands().size());
        assertSame("EXP operand must be the original arg", arg, expCall.getOperands().get(0));

        // Right operand must be numerically 1 (type may be DECIMAL or INTEGER depending on promotion)
        RexNode right = minus.getOperands().get(1);
        assertTrue("right operand must be a literal", right instanceof org.apache.calcite.rex.RexLiteral);
    }

    public void testAdaptPassesThroughUnrelatedCall() {
        RexNode ref = rexBuilder.makeInputRef(doubleType, 0);
        RexCall absCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.ABS, List.of(ref));

        RexNode adapted = new Expm1Adapter().adapt(absCall, List.of(), cluster);

        assertSame(absCall, adapted);
    }
}
