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
 * Unit tests for {@link WidthBucketAdapter} — pure rename cat-4 adapter.
 * Same shape as {@link SpanBucketAdapterTests}: PPL-emitted uppercase
 * {@code WIDTH_BUCKET(value, num_bins, data_range, max_value)} rewritten
 * to the locally-declared lowercase {@code width_bucket} whose Sig resolves
 * to the Rust UDF.
 */
public class WidthBucketAdapterTests extends OpenSearchTestCase {

    public void testWidthBucketRewritesToLocalOperator() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RelDataType intNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        SqlFunction widthBucketOp = new SqlFunction(
            "WIDTH_BUCKET",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varcharNullable),
            null,
            OperandTypes.family(
                org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC,
                org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC,
                org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC,
                org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC
            ),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode value = rexBuilder.makeInputRef(doubleNullable, 0);
        RexNode bins = rexBuilder.makeInputRef(intNullable, 1);
        RexNode range = rexBuilder.makeInputRef(doubleNullable, 2);
        RexNode max = rexBuilder.makeInputRef(doubleNullable, 3);
        RexCall original = (RexCall) rexBuilder.makeCall(widthBucketOp, List.of(value, bins, range, max));

        RexNode adapted = new WidthBucketAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(
            "adapted call must target the locally-declared width_bucket operator",
            WidthBucketAdapter.LOCAL_WIDTH_BUCKET_OP,
            call.getOperator()
        );
        assertEquals("width_bucket(v, bins, range, max) must preserve all 4 operands — no injection", 4, call.getOperands().size());
        assertSame("arg 0 must be the original value operand", value, call.getOperands().get(0));
        assertSame("arg 1 must be the original num_bins operand", bins, call.getOperands().get(1));
        assertSame("arg 2 must be the original data_range operand", range, call.getOperands().get(2));
        assertSame("arg 3 must be the original max_value operand", max, call.getOperands().get(3));
    }

    /**
     * The adapter MUST preserve the original call's {@link RelDataType}. PPL's
     * WIDTH_BUCKET is registered with {@code VARCHAR(2000) FORCE_NULLABLE}; the
     * adapted operator's inferred type must not drift, or the enclosing Project's
     * cached rowType mismatches. Regression guard consistent with
     * {@link SpanBucketAdapterTests#testAdaptedCallPreservesOriginalReturnType}.
     */
    public void testAdaptedCallPreservesOriginalReturnType() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RelDataType intNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType varchar2000Nullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000), true);
        SqlFunction widthBucketOp = new SqlFunction(
            "WIDTH_BUCKET",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varchar2000Nullable),
            null,
            OperandTypes.family(
                org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC,
                org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC,
                org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC,
                org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC
            ),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode value = rexBuilder.makeInputRef(doubleNullable, 0);
        RexNode bins = rexBuilder.makeInputRef(intNullable, 1);
        RexNode range = rexBuilder.makeInputRef(doubleNullable, 2);
        RexNode max = rexBuilder.makeInputRef(doubleNullable, 3);
        RexCall original = (RexCall) rexBuilder.makeCall(widthBucketOp, List.of(value, bins, range, max));
        assertEquals(varchar2000Nullable, original.getType());

        RexNode adapted = new WidthBucketAdapter().adapt(original, List.of(), cluster);

        assertEquals(
            "adapted call's return type must equal the original call's return type, "
                + "otherwise the enclosing Project.rowType assertion fails in fragment conversion",
            original.getType(),
            adapted.getType()
        );
    }
}
