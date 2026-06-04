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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link MinspanBucketAdapter} — pure rename cat-4 adapter.
 * Identical in shape to {@link WidthBucketAdapterTests}: four numeric
 * operands, pure rename via {@link AbstractNameMappingAdapter}, no literal
 * injection, preserves operand order and original call's RelDataType.
 */
public class MinspanBucketAdapterTests extends OpenSearchTestCase {

    public void testMinspanBucketRewritesToLocalOperator() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        SqlFunction minspanBucketOp = new SqlFunction(
            "MINSPAN_BUCKET",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varcharNullable),
            null,
            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode value = rexBuilder.makeInputRef(doubleNullable, 0);
        RexNode minSpan = rexBuilder.makeInputRef(doubleNullable, 1);
        RexNode range = rexBuilder.makeInputRef(doubleNullable, 2);
        RexNode max = rexBuilder.makeInputRef(doubleNullable, 3);
        RexCall original = (RexCall) rexBuilder.makeCall(minspanBucketOp, List.of(value, minSpan, range, max));

        RexNode adapted = new MinspanBucketAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(
            "adapted call must target the locally-declared minspan_bucket operator",
            MinspanBucketAdapter.LOCAL_MINSPAN_BUCKET_OP,
            call.getOperator()
        );
        assertEquals("minspan_bucket(v, ms, range, max) must preserve all 4 operands", 4, call.getOperands().size());
        assertSame("arg 0 must be the original value operand", value, call.getOperands().get(0));
        assertSame("arg 1 must be the original min_span operand", minSpan, call.getOperands().get(1));
        assertSame("arg 2 must be the original data_range operand", range, call.getOperands().get(2));
        assertSame("arg 3 must be the original max_value operand", max, call.getOperands().get(3));
    }

    /**
     * Regression guard on {@link org.apache.calcite.rel.type.RelDataType} preservation —
     * identical contract to {@link SpanBucketAdapterTests#testAdaptedCallPreservesOriginalReturnType}.
     */
    public void testAdaptedCallPreservesOriginalReturnType() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RelDataType varchar2000Nullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000), true);
        SqlFunction minspanBucketOp = new SqlFunction(
            "MINSPAN_BUCKET",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varchar2000Nullable),
            null,
            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode value = rexBuilder.makeInputRef(doubleNullable, 0);
        RexNode minSpan = rexBuilder.makeInputRef(doubleNullable, 1);
        RexNode range = rexBuilder.makeInputRef(doubleNullable, 2);
        RexNode max = rexBuilder.makeInputRef(doubleNullable, 3);
        RexCall original = (RexCall) rexBuilder.makeCall(minspanBucketOp, List.of(value, minSpan, range, max));
        assertEquals(varchar2000Nullable, original.getType());

        RexNode adapted = new MinspanBucketAdapter().adapt(original, List.of(), cluster);

        assertEquals("adapted call's return type must equal the original call's return type", original.getType(), adapted.getType());
    }
}
