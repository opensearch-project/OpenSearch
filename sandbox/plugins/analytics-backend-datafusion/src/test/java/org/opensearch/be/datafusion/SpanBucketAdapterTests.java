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
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link SpanBucketAdapter} — pure rename cat-4 adapter
 * exercising {@link AbstractNameMappingAdapter} (no literal injection).
 * Mirrors {@link ConvertTzAdapter}'s shape: PPL-emitted uppercase
 * {@code SPAN_BUCKET} rewritten to our locally-declared lowercase
 * {@code span_bucket} whose Sig resolves to the Rust UDF by name.
 */
public class SpanBucketAdapterTests extends OpenSearchTestCase {

    /**
     * The adapter rewrites {@code SPAN_BUCKET(value, span)} to the
     * locally-declared {@code span_bucket} operator, preserving both
     * operands unchanged (no prepend/append literals).
     */
    public void testSpanBucketRewritesToLocalOperator() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        // Synthesize SPAN_BUCKET(numeric, numeric) — a two-arg Calcite call using
        // a stand-in SqlFunction with the same name the PPL frontend emits.
        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        SqlFunction spanBucketOp = new SqlFunction(
            "SPAN_BUCKET",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varcharNullable),
            null,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode valueRef = rexBuilder.makeInputRef(doubleNullable, 0);
        RexNode spanRef = rexBuilder.makeInputRef(doubleNullable, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(spanBucketOp, List.of(valueRef, spanRef));

        RexNode adapted = new SpanBucketAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(
            "adapted call must target the locally-declared span_bucket operator (the Sig referent)",
            SpanBucketAdapter.LOCAL_SPAN_BUCKET_OP,
            call.getOperator()
        );
        assertEquals("span_bucket(value, span) must have 2 operands — no injection", 2, call.getOperands().size());
        assertSame("arg 0 must be the original value operand", valueRef, call.getOperands().get(0));
        assertSame("arg 1 must be the original span operand", spanRef, call.getOperands().get(1));
    }

    /**
     * The adapter MUST preserve the original call's {@link RelDataType}. The
     * enclosing Project caches its rowType from the pre-adaptation expression;
     * any Calcite-inferred-type drift breaks {@code Project.isValid}'s
     * compatibleTypes assertion during fragment conversion. Regression guard
     * carried from the PR10 audit lesson that motivated {@code YearAdapterTests
     * .testAdaptedCallPreservesOriginalReturnType}.
     */
    public void testAdaptedCallPreservesOriginalReturnType() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        // PPL's SPAN_BUCKET returns VARCHAR(2000) — a distinct type from the
        // LOCAL_SPAN_BUCKET_OP's declared ARG0-nullable inference. Without
        // explicit preservation the adapted call would report DOUBLE instead
        // of VARCHAR(2000) and mismatch the enclosing Project's cached rowType.
        RelDataType varchar2000Nullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000), true);
        SqlFunction spanBucketOp = new SqlFunction(
            "SPAN_BUCKET",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varchar2000Nullable),
            null,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode valueRef = rexBuilder.makeInputRef(doubleNullable, 0);
        RexNode spanRef = rexBuilder.makeInputRef(doubleNullable, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(spanBucketOp, List.of(valueRef, spanRef));
        assertEquals(varchar2000Nullable, original.getType());

        RexNode adapted = new SpanBucketAdapter().adapt(original, List.of(), cluster);

        assertEquals(
            "adapted call's return type must equal the original call's return type, "
                + "otherwise the enclosing Project.rowType assertion fails in fragment conversion",
            original.getType(),
            adapted.getType()
        );
    }
}
