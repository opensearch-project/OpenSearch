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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link ConcatVariadicAdapter}. Pins the type-normalisation contract that lets
 * substrait's CONSISTENT-consistency {@code concat} extension accept Calcite's mixed FixedChar /
 * VARCHAR operand types without rejecting the variadic call.
 */
public class ConcatVariadicAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelDataType varcharType;
    private RelDataType charType;

    private final ConcatVariadicAdapter adapter = new ConcatVariadicAdapter();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        charType = typeFactory.createSqlType(SqlTypeName.CHAR, 5);
    }

    public void testAdaptCastsFixedCharOperandToVarchar() {
        // CONCAT(varcharField, charLiteral, varcharField) — the middle operand is FixedChar/CHAR.
        RexNode field0 = rexBuilder.makeInputRef(varcharType, 0);
        RexNode charLiteral = rexBuilder.makeInputRef(charType, 1);
        RexNode field2 = rexBuilder.makeInputRef(varcharType, 2);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.CONCAT_FUNCTION, field0, charLiteral, field2);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        // Every operand must be VARCHAR after the rewrite.
        for (int i = 0; i < adapted.getOperands().size(); i++) {
            assertEquals(
                "operand " + i + " must be VARCHAR after normalisation",
                SqlTypeName.VARCHAR,
                adapted.getOperands().get(i).getType().getSqlTypeName()
            );
        }
    }

    public void testAdaptPreservesOperatorIdentity() {
        // The rewritten call must keep the original SqlLibraryOperators.CONCAT_FUNCTION operator
        // by reference — substrait emission keys on operator identity, so a fresh binary CONCAT
        // would route to the wrong substrait extension.
        RexNode field0 = rexBuilder.makeInputRef(varcharType, 0);
        RexNode charLiteral = rexBuilder.makeInputRef(charType, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.CONCAT_FUNCTION, field0, charLiteral);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertSame("operator identity must be preserved", original.getOperator(), adapted.getOperator());
    }

    public void testAdaptPreservesCallReturnType() {
        // CASE-style or wrapper rewrites can drift the call's RelDataType; this adapter must not.
        RexNode field0 = rexBuilder.makeInputRef(varcharType, 0);
        RexNode charLiteral = rexBuilder.makeInputRef(charType, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.CONCAT_FUNCTION, field0, charLiteral);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertEquals("call return type must equal the original", original.getType(), adapted.getType());
    }

    public void testAdaptPreservesNullabilityWhenCasting() {
        // A non-nullable CHAR operand must produce a non-nullable VARCHAR cast; a nullable
        // VARCHAR field passes through. Nullability drift would change isthmus' downstream
        // null-handling decisions.
        RelDataType nonNullableChar = typeFactory.createTypeWithNullability(charType, false);
        RexNode field0 = rexBuilder.makeInputRef(varcharType, 0);
        RexNode charLiteral = rexBuilder.makeInputRef(nonNullableChar, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.CONCAT_FUNCTION, field0, charLiteral);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertFalse(
            "non-nullable CHAR operand must produce a non-nullable VARCHAR cast",
            adapted.getOperands().get(1).getType().isNullable()
        );
    }

    public void testAdaptAllVarcharIsNoOp() {
        // Early-return path: if every operand is already VARCHAR, the original RexCall is returned
        // by reference. Avoids gratuitous allocation and downstream rewrite churn.
        RexNode field0 = rexBuilder.makeInputRef(varcharType, 0);
        RexNode field1 = rexBuilder.makeInputRef(varcharType, 1);
        RexNode field2 = rexBuilder.makeInputRef(varcharType, 2);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.CONCAT_FUNCTION, field0, field1, field2);

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("all-VARCHAR call must pass through unchanged", original, adapted);
    }

    public void testAdaptCastsCharLiteralBetweenScalarFunctionOperands() {
        // CONCAT(SUBSTRING(f0,1,3), charLiteral, SUBSTRING(f1,1,3)): the outer operands are
        // scalar-function calls (RexCall), only the middle FixedChar literal needs casting.
        RexNode substr0 = substringCall(0);
        RexNode charLiteral = rexBuilder.makeInputRef(charType, 2);
        RexNode substr1 = substringCall(1);
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.CONCAT_FUNCTION, substr0, charLiteral, substr1);

        RexCall adapted = (RexCall) adapter.adapt(original, List.of(), cluster);

        assertSame("operator identity must be preserved", original.getOperator(), adapted.getOperator());
        for (int i = 0; i < adapted.getOperands().size(); i++) {
            assertEquals(
                "operand " + i + " must be VARCHAR after normalisation",
                SqlTypeName.VARCHAR,
                adapted.getOperands().get(i).getType().getSqlTypeName()
            );
        }
        // The two scalar-function operands were already VARCHAR, so they must pass through by
        // reference — only the middle literal is rewritten.
        assertSame("VARCHAR substring operand 0 must pass through unchanged", substr0, adapted.getOperands().get(0));
        assertSame("VARCHAR substring operand 2 must pass through unchanged", substr1, adapted.getOperands().get(2));
        assertNotSame("CHAR literal operand must be rewritten to a VARCHAR cast", charLiteral, adapted.getOperands().get(1));
    }

    public void testAdaptTwoScalarFunctionOperandsIsNoOp() {
        // CONCAT(SUBSTRING(f0,1,3), SUBSTRING(f1,1,3)): two VARCHAR-typed scalar-function operands,
        // nothing to cast, so the original call passes through by reference.
        RexCall original = (RexCall) rexBuilder.makeCall(SqlLibraryOperators.CONCAT_FUNCTION, substringCall(0), substringCall(1));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame("all-VARCHAR scalar-fn operand call must pass through unchanged", original, adapted);
    }

    /** {@code SUBSTRING(field#idx, 1, 3)} typed VARCHAR — a RexCall operand for concat. */
    private RexCall substringCall(int fieldIndex) {
        RexNode field = rexBuilder.makeInputRef(varcharType, fieldIndex);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode start = rexBuilder.makeExactLiteral(java.math.BigDecimal.ONE, intType);
        RexNode len = rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(3), intType);
        return (RexCall) rexBuilder.makeCall(varcharType, SqlStdOperatorTable.SUBSTRING, List.of(field, start, len));
    }
}
