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

/** Calendar-unit time spans (M/q/y) lower to {@code date_bin("<N> <unit>", t, '1970-01-01T00:00:00Z')}. */
public class SpanAdapterCalendarUnitTests extends OpenSearchTestCase {

    private static RexCall makeSpanCall(RexBuilder rexBuilder, RelDataType tsType, BigDecimal interval, String unit) {
        RelDataType intType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
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

    /** SPAN(ts, 3, 'M') → date_bin("3 month", ts, '1970-01-01T00:00:00Z'). */
    public void testCalendarMonthUnitRewriteToDateBin() {
        assertCalendarStride(BigDecimal.valueOf(3), "M", "3 month");
    }

    /** SPAN(ts, 5, 'y') → date_bin("5 year", ts, '1970-01-01T00:00:00Z'). N==1 takes the cheaper date_trunc path. */
    public void testCalendarYearUnitRewriteToDateBin() {
        assertCalendarStride(BigDecimal.valueOf(5), "y", "5 year");
    }

    /** SPAN(ts, 2, 'q') → date_bin("2 quarter", ts, '1970-01-01T00:00:00Z'). */
    public void testCalendarQuarterUnitRewriteToDateBin() {
        assertCalendarStride(BigDecimal.valueOf(2), "q", "2 quarter");
    }

    private void assertCalendarStride(BigDecimal n, String unit, String expectedStride) {
        RelOptCluster cluster = newCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType tsType = rexBuilder.getTypeFactory()
            .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 0), true);

        RexCall original = makeSpanCall(rexBuilder, tsType, n, unit);
        RexCall adapted = (RexCall) new SpanAdapter().adapt(original, List.of(), cluster);

        assertSame(SpanAdapter.LOCAL_DATE_BIN_OP, adapted.getOperator());
        assertEquals("calendar-unit date_bin takes (stride, source, origin)", 3, adapted.getOperands().size());
        assertEquals(expectedStride, ((RexLiteral) adapted.getOperands().get(0)).getValueAs(String.class));
        assertEquals("1970-01-01T00:00:00Z", ((RexLiteral) adapted.getOperands().get(2)).getValueAs(String.class));
    }
}
