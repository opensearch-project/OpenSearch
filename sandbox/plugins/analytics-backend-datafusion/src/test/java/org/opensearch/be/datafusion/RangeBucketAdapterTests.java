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
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link RangeBucketAdapter} — pure rename cat-4 adapter with
 * 5 operands (two of which may be null literals for the optional
 * start_param / end_param slots). Same pattern as {@link SpanBucketAdapterTests},
 * {@link WidthBucketAdapterTests}, {@link MinspanBucketAdapterTests}.
 */
public class RangeBucketAdapterTests extends OpenSearchTestCase {

    /**
     * PPL emits {@code RANGE_BUCKET(value, min, max, start_or_null,
     * end_or_null)}. The adapter must forward all 5 operands unchanged —
     * the Rust UDF's asymmetric null handling (nulls in slots 3/4 are
     * sentinels, not propagation triggers) depends on receiving the null
     * literals as-is.
     */
    public void testRangeBucketRewritesToLocalOperatorPreservingAllFiveOperands() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        SqlFunction rangeBucketOp = new SqlFunction(
            "RANGE_BUCKET",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varcharNullable),
            null,
            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode value = rexBuilder.makeInputRef(doubleNullable, 0);
        RexNode dataMin = rexBuilder.makeInputRef(doubleNullable, 1);
        RexNode dataMax = rexBuilder.makeInputRef(doubleNullable, 2);
        RexNode startParam = rexBuilder.constantNull();
        RexNode endParam = rexBuilder.constantNull();
        RexCall original = (RexCall) rexBuilder.makeCall(rangeBucketOp, List.of(value, dataMin, dataMax, startParam, endParam));

        RexNode adapted = new RangeBucketAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(
            "adapted call must target the locally-declared range_bucket operator",
            RangeBucketAdapter.LOCAL_RANGE_BUCKET_OP,
            call.getOperator()
        );
        assertEquals("range_bucket(v, min, max, start, end) must preserve all 5 operands", 5, call.getOperands().size());
        assertSame("arg 0 must be the original value operand", value, call.getOperands().get(0));
        assertSame("arg 1 must be the original data_min operand", dataMin, call.getOperands().get(1));
        assertSame("arg 2 must be the original data_max operand", dataMax, call.getOperands().get(2));
        assertSame("arg 3 must be the original start_param (null literal)", startParam, call.getOperands().get(3));
        assertSame("arg 4 must be the original end_param (null literal)", endParam, call.getOperands().get(4));
    }

    /**
     * Regression guard on {@link org.apache.calcite.rel.type.RelDataType} preservation
     * — identical contract to the other Group C adapters.
     */
    public void testAdaptedCallPreservesOriginalReturnType() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RelDataType varchar2000Nullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000), true);
        SqlFunction rangeBucketOp = new SqlFunction(
            "RANGE_BUCKET",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(varchar2000Nullable),
            null,
            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode value = rexBuilder.makeInputRef(doubleNullable, 0);
        RexNode dataMin = rexBuilder.makeInputRef(doubleNullable, 1);
        RexNode dataMax = rexBuilder.makeInputRef(doubleNullable, 2);
        RexNode startParam = rexBuilder.makeLiteral(-10.0, doubleNullable, true);
        RexNode endParam = rexBuilder.makeLiteral(110.0, doubleNullable, true);
        RexCall original = (RexCall) rexBuilder.makeCall(rangeBucketOp, List.of(value, dataMin, dataMax, startParam, endParam));
        assertEquals(varchar2000Nullable, original.getType());

        RexNode adapted = new RangeBucketAdapter().adapt(original, List.of(), cluster);

        assertEquals("adapted call's return type must equal the original call's return type", original.getType(), adapted.getType());
    }
}
