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
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/** Covers {@link TimestampAddAdapter}: standalone TIMESTAMPADD lowers to DATETIME_PLUS with a base-unit interval; TIME first arg gets today-anchored. */
public class TimestampAddAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    private final TimestampAddAdapter adapter = new TimestampAddAdapter();

    private static final SqlFunction TIMESTAMPADD_OP = new SqlFunction(
        "TIMESTAMPADD",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.TIMEDATE
    );

    /**
     * TIMESTAMPADD(YEAR, 15, ts-col) → DATETIME_PLUS(CAST AS TIMESTAMP, INTERVAL 180 MONTH).
     * Uses a column ref (not a literal) to exercise the interval path; literal-literal hits
     * the calendar-exact fold path which returns a TIMESTAMP literal directly.
     */
    public void testYearUnitLowersToMonthInterval() {
        RexNode unit = rexBuilder.makeLiteral("YEAR");
        RexNode count = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(15L));
        RelDataType timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RexNode tsCol = rexBuilder.makeInputRef(timestampType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(TIMESTAMPADD_OP, List.of(unit, count, tsCol));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        // Either DATETIME_PLUS or a CAST wrapping it depending on type matching.
        RexCall outer = adapted instanceof RexCall && ((RexCall) adapted).getKind() == SqlKind.CAST
            ? (RexCall) ((RexCall) adapted).getOperands().get(0)
            : (RexCall) adapted;
        assertSame(SqlStdOperatorTable.DATETIME_PLUS, outer.getOperator());
    }

    /** TIMESTAMPADD(HOUR, 5, TIME-col) anchors TIME to today UTC via CONCAT before the interval add. */
    public void testTimeOperandAnchoredToToday() {
        RexNode unit = rexBuilder.makeLiteral("HOUR");
        RexNode count = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(5L));
        RelDataType timeType = typeFactory.createSqlType(SqlTypeName.TIME);
        RexNode tsCol = rexBuilder.makeInputRef(timeType, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(TIMESTAMPADD_OP, List.of(unit, count, tsCol));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        // Walk down: outer DATETIME_PLUS (or CAST wrap) → first operand is CAST(CONCAT(...) AS TIMESTAMP).
        RexCall outer = adapted instanceof RexCall && ((RexCall) adapted).getKind() == SqlKind.CAST
            ? (RexCall) ((RexCall) adapted).getOperands().get(0)
            : (RexCall) adapted;
        RexCall castNode = (RexCall) outer.getOperands().get(0);
        assertEquals(SqlKind.CAST, castNode.getKind());
        RexCall concat = (RexCall) castNode.getOperands().get(0);
        assertEquals("||", concat.getOperator().getName());
    }

    /** Unrecognised unit keeps the original RexCall (substrait surfaces the failure). */
    public void testUnknownUnitPassesThrough() {
        RexNode unit = rexBuilder.makeLiteral("FORTNIGHT");
        RexNode count = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(1L));
        RexNode ts = rexBuilder.makeLiteral("2001-03-06 00:00:00");
        RexCall original = (RexCall) rexBuilder.makeCall(TIMESTAMPADD_OP, List.of(unit, count, ts));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        assertSame(original, adapted);
    }
}
