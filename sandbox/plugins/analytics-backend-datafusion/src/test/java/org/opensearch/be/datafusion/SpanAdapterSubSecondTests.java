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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for the sub-second multi-unit lowering path in {@link SpanAdapter}.
 *
 * <p>The fixed-second arithmetic path (s/m/h/d/w) is covered by the timechart-route
 * end-to-end suite. This class targets the gap that the timechart PR explicitly left
 * out — {@code us} and {@code ms} with {@code N > 1} — which is what dashboard logs-tab
 * histogram queries hit when bucket width is sub-second (e.g.
 * {@code span(@timestamp, 40ms)}).
 */
public class SpanAdapterSubSecondTests extends OpenSearchTestCase {

    private static RexCall makeSpanCall(RexBuilder rexBuilder, RelDataType tsType, BigDecimal interval, String unit) {
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        SqlFunction spanOp = new SqlFunction(
            "SPAN",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(tsType),
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode fieldRef = rexBuilder.makeInputRef(tsType, 0);
        RexNode intervalLit = rexBuilder.makeLiteral(interval, intType);
        RexNode unitLit = rexBuilder.makeLiteral(unit);
        return (RexCall) rexBuilder.makeCall(spanOp, List.of(fieldRef, intervalLit, unitLit));
    }

    private static RelOptCluster newCluster() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        return RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
    }

    /** The dashboard-trigger case: {@code span(@timestamp, 40ms)} lowers to
     *  {@code date_bin("40 milliseconds", @timestamp)}. */
    public void testMillisecondMultiUnitRewritesToDateBin() {
        RelOptCluster cluster = newCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType tsType = rexBuilder.getTypeFactory()
            .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 0), true);

        RexCall original = makeSpanCall(rexBuilder, tsType, BigDecimal.valueOf(40), "ms");
        RexNode adapted = new SpanAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame("sub-second multi-unit must lower to LOCAL_DATE_BIN_OP", SpanAdapter.LOCAL_DATE_BIN_OP, call.getOperator());
        assertEquals("date_bin takes (stride, source)", 2, call.getOperands().size());
        assertEquals(
            "stride must be DataFusion's string-form interval with the count",
            "40 milliseconds",
            ((RexLiteral) call.getOperands().get(0)).getValueAs(String.class)
        );
    }

    public void testMicrosecondMultiUnitRewritesToDateBin() {
        RelOptCluster cluster = newCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType tsType = rexBuilder.getTypeFactory()
            .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 0), true);

        RexCall original = makeSpanCall(rexBuilder, tsType, BigDecimal.valueOf(250), "us");
        RexCall adapted = (RexCall) new SpanAdapter().adapt(original, List.of(), cluster);

        assertSame(SpanAdapter.LOCAL_DATE_BIN_OP, adapted.getOperator());
        assertEquals("250 microseconds", ((RexLiteral) adapted.getOperands().get(0)).getValueAs(String.class));
    }

    /** N == 1 for sub-second units must NOT take the date_bin path — date_trunc handles it. */
    public void testSubSecondUnitOneStaysOnDateTruncPath() {
        RelOptCluster cluster = newCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType tsType = rexBuilder.getTypeFactory()
            .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 0), true);

        RexCall original = makeSpanCall(rexBuilder, tsType, BigDecimal.ONE, "ms");
        RexCall adapted = (RexCall) new SpanAdapter().adapt(original, List.of(), cluster);

        assertEquals("N==1 ms stays on date_trunc (the cheaper path), NOT date_bin", "DATE_TRUNC", adapted.getOperator().getName());
    }

    /** Non-integer interval for sub-second units must fall through unchanged. The interval
     *  needs a DECIMAL literal type so the fractional value survives; INTEGER would coerce
     *  to 1 and accidentally hit the date_trunc path. */
    public void testFractionalIntervalForSubSecondFallsThrough() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
        RelDataType tsType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 0), true);
        RelDataType decType = typeFactory.createSqlType(SqlTypeName.DECIMAL, 5, 2);

        SqlFunction spanOp = new SqlFunction(
            "SPAN",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(tsType),
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode fieldRef = rexBuilder.makeInputRef(tsType, 0);
        RexNode fractionalInterval = rexBuilder.makeLiteral(new BigDecimal("2.5"), decType);
        RexNode unitLit = rexBuilder.makeLiteral("ms");
        RexCall original = (RexCall) rexBuilder.makeCall(spanOp, List.of(fieldRef, fractionalInterval, unitLit));

        RexNode adapted = new SpanAdapter().adapt(original, List.of(), cluster);
        assertSame("fractional interval must fall through unchanged", original, adapted);
    }

    /**
     * The adapted call MUST preserve the original SPAN call's {@link RelDataType}. The
     * enclosing Project caches its rowType from the pre-adaptation expression; any
     * Calcite-inferred-type drift breaks {@code Project.isValid}'s compatibleTypes
     * assertion during fragment conversion. Regression guard mirroring the same check
     * on {@link SpanBucketAdapterTests#testAdaptedCallPreservesOriginalReturnType}.
     */
    public void testAdaptedDateBinCallPreservesOriginalReturnType() {
        RelOptCluster cluster = newCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        // Use precision-3 nullable timestamp — distinct from LOCAL_DATE_BIN_OP's
        // ARG1_NULLABLE inferred type if the operand was a different type.
        RelDataType tsType = rexBuilder.getTypeFactory()
            .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 3), true);

        RexCall original = makeSpanCall(rexBuilder, tsType, BigDecimal.valueOf(40), "ms");
        assertEquals(tsType, original.getType());

        RexNode adapted = new SpanAdapter().adapt(original, List.of(), cluster);
        assertEquals("adapted date_bin call must declare the same type as the original SPAN call", original.getType(), adapted.getType());
    }
}
