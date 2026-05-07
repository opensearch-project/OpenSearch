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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link YearAdapter} exercising the reusable rename +
 * literal-arg injection adapter pattern via {@link AbstractNameMappingAdapter}.
 */
public class YearAdapterTests extends OpenSearchTestCase {

    public void testYearRewritesToDatePartWithYearLiteral() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        // Synthesize YEAR(ts) — a one-arg Calcite call of our own SqlFunction
        // so the test doesn't depend on any specific builtin.
        RelDataType tsType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        SqlFunction yearOp = new SqlFunction(
            "YEAR",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true)),
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode tsRef = rexBuilder.makeInputRef(tsType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(yearOp, List.of(tsRef));

        RexNode adapted = new YearAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertEquals("adapted call must target DATE_PART", SqlLibraryOperators.DATE_PART, call.getOperator());
        assertEquals("date_part(unit, value) must have 2 operands after year-literal prepend", 2, call.getOperands().size());
        assertTrue(
            "arg 0 must be a string literal, got " + call.getOperands().get(0).getClass(),
            call.getOperands().get(0) instanceof RexLiteral
        );
        RexLiteral unitLit = (RexLiteral) call.getOperands().get(0);
        assertEquals("year", unitLit.getValueAs(String.class));
        assertSame("arg 1 must be the original operand", tsRef, call.getOperands().get(1));
    }

    /**
     * The adapter MUST preserve the Calcite {@link RelDataType} of the original call.
     * Otherwise the enclosing Project's cached {@code rowType} (derived from the pre-
     * adaptation expression) mismatches the adapted expression's type, tripping
     * {@code Project.isValid}'s {@code RexUtil.compatibleTypes} assertion during
     * fragment conversion. Regression guard for the PR10 IT hang where
     * {@code DATE_PART} produced a different Calcite-inferred type than {@code YEAR}.
     */
    public void testAdaptedCallPreservesOriginalReturnType() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType tsType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        // PPL's YEAR operator is registered with INTEGER_FORCE_NULLABLE — distinct
        // from Calcite's SqlLibraryOperators.DATE_PART (which returns BIGINT via
        // SqlExtractFunction). If the adapter didn't clone with the original's type,
        // the Project's cached rowType (derived from INTEGER) would clash with the
        // adapted DATE_PART's inferred BIGINT, tripping Project.isValid.
        RelDataType integerNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        SqlFunction yearOp = new SqlFunction(
            "YEAR",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(integerNullable),
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode tsRef = rexBuilder.makeInputRef(tsType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(yearOp, List.of(tsRef));
        assertEquals(integerNullable, original.getType());

        RexNode adapted = new YearAdapter().adapt(original, List.of(), cluster);

        assertEquals(
            "adapted call's return type must equal the original call's return type, "
                + "otherwise the enclosing Project.rowType assertion fails in fragment conversion",
            original.getType(),
            adapted.getType()
        );
    }
}
