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
import org.apache.calcite.rex.RexLiteral;
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

/** Covers {@link TimestampDiffAdapter} standalone shape: {@code (to_unixtime(t2) - to_unixtime(t1))} scaled by out-unit factor. */
public class TimestampDiffAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    private final TimestampDiffAdapter adapter = new TimestampDiffAdapter();

    private static final SqlFunction TIMESTAMPDIFF_OP = new SqlFunction(
        "TIMESTAMPDIFF",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.TIMEDATE
    );

    /** TIMESTAMPDIFF(DAY, t1, t2) → CAST((to_unixtime(t2) - to_unixtime(t1)) / 86400 AS BIGINT). */
    public void testDayUnitDivisorIs86400() {
        assertStandaloneDivisor("DAY", 86_400L);
    }

    /** Variable MONTH out-unit uses 30-day approximation: divisor = 30 * 86400. */
    public void testMonthUnitUsesThirtyDayApproximation() {
        assertStandaloneDivisor("MONTH", 30L * 86_400L);
    }

    /** Variable YEAR out-unit uses 365-day approximation: divisor = 365 * 86400. */
    public void testYearUnitUsesThreeSixtyFiveDayApproximation() {
        assertStandaloneDivisor("YEAR", 365L * 86_400L);
    }

    /** Build TIMESTAMPDIFF(unit, t1, t2) on TIMESTAMP columns and assert the divisor of the inner DIVIDE. */
    private void assertStandaloneDivisor(String unit, long expectedDivisor) {
        RelDataType tsType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RexNode unitLit = rexBuilder.makeLiteral(unit);
        RexNode t1 = rexBuilder.makeInputRef(tsType, 0);
        RexNode t2 = rexBuilder.makeInputRef(tsType, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(TIMESTAMPDIFF_OP, List.of(unitLit, t1, t2));

        RexNode adapted = adapter.adapt(original, List.of(), cluster);

        // Outer is CAST(scaled AS BIGINT); peel one level.
        RexCall outerCast = (RexCall) adapted;
        assertEquals(SqlKind.CAST, outerCast.getKind());
        RexCall divide = (RexCall) outerCast.getOperands().get(0);
        assertSame(SqlStdOperatorTable.DIVIDE, divide.getOperator());
        RexLiteral divisor = (RexLiteral) divide.getOperands().get(1);
        assertEquals(BigDecimal.valueOf(expectedDivisor), divisor.getValue());
    }
}
