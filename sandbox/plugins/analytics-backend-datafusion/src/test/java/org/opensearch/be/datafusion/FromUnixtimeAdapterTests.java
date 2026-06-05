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

import java.math.BigDecimal;
import java.util.List;

/**
 * Tests for {@link RustUdfDateTimeAdapters.FromUnixtimeAdapter}. Covers:
 * <ul>
 *   <li>1-arg {@code FROM_UNIXTIME(seconds)} → widened to fp64 and renamed.</li>
 *   <li>2-arg {@code FROM_UNIXTIME(seconds, format)} → composed as
 *       {@code date_format(from_unixtime(seconds), format)}.</li>
 *   <li>Already-{@code DOUBLE} seconds operand: passes through without a CAST wrap.</li>
 * </ul>
 */
public class FromUnixtimeAdapterTests extends OpenSearchTestCase {

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

    private SqlFunction fromUnixtimeOp(RelDataType returnType, int arity) {
        return new SqlFunction(
            "FROM_UNIXTIME",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(returnType),
            null,
            arity == 2 ? OperandTypes.ANY_ANY : OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
    }

    /** 1-arg form: seconds widened to DOUBLE, call renamed to local UDF. */
    public void testOneArgFormDelegatesToNumericWiden() {
        RelDataType timestampType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        RexNode seconds = rexBuilder.makeExactLiteral(BigDecimal.valueOf(1662601316), typeFactory.createSqlType(SqlTypeName.BIGINT));
        RexCall original = (RexCall) rexBuilder.makeCall(fromUnixtimeOp(timestampType, 1), List.of(seconds));

        RexNode adapted = new RustUdfDateTimeAdapters.FromUnixtimeAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame(RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, call.getOperator());
        assertEquals(1, call.getOperands().size());
        assertEquals(SqlTypeName.DOUBLE, call.getOperands().get(0).getType().getSqlTypeName());
    }

    /** 2-arg form: rewrites {@code FROM_UNIXTIME(seconds, format)} to {@code date_format(from_unixtime(seconds), format)}. */
    public void testTwoArgFormRewritesToDateFormatOfFromUnixtime() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode seconds = rexBuilder.makeExactLiteral(BigDecimal.valueOf(1662601316), typeFactory.createSqlType(SqlTypeName.BIGINT));
        RexNode format = rexBuilder.makeLiteral("%T");
        RexCall original = (RexCall) rexBuilder.makeCall(fromUnixtimeOp(varcharType, 2), List.of(seconds, format));

        RexNode adapted = new RustUdfDateTimeAdapters.FromUnixtimeAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexCall);
        RexCall outer = (RexCall) adapted;
        assertSame(RustUdfDateTimeAdapters.LOCAL_DATE_FORMAT_OP, outer.getOperator());
        assertEquals(2, outer.getOperands().size());
        assertEquals(original.getType(), outer.getType());

        RexCall inner = (RexCall) outer.getOperands().get(0);
        assertSame(RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, inner.getOperator());
        assertEquals(1, inner.getOperands().size());
        assertEquals(SqlTypeName.DOUBLE, inner.getOperands().get(0).getType().getSqlTypeName());
        assertEquals(SqlTypeName.TIMESTAMP, inner.getType().getSqlTypeName());

        assertSame(format, outer.getOperands().get(1));
    }

    /** DOUBLE seconds operand passes through without a CAST wrap. */
    public void testDoubleSecondsOperandPassesThroughInTwoArgForm() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode seconds = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(1662601316.0), typeFactory.createSqlType(SqlTypeName.DOUBLE));
        RexNode format = rexBuilder.makeLiteral("%Y-%m-%d");
        RexCall original = (RexCall) rexBuilder.makeCall(fromUnixtimeOp(varcharType, 2), List.of(seconds, format));

        RexNode adapted = new RustUdfDateTimeAdapters.FromUnixtimeAdapter().adapt(original, List.of(), cluster);

        RexCall inner = (RexCall) ((RexCall) adapted).getOperands().get(0);
        assertSame(seconds, inner.getOperands().get(0));
    }
}
