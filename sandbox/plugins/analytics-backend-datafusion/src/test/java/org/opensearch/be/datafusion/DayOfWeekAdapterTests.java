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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * VARCHAR-coercion coverage for {@link DayOfWeekAdapter}. Pre-fix this adapter
 * built {@code date_part('dow', <varchar>)} which the substrait matcher rejected
 * as {@code DATE_PART(string, string)}.
 */
public class DayOfWeekAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    private SqlFunction pplDayOfWeek() {
        return new SqlFunction(
            "DAYOFWEEK",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
    }

    private RexCall innerDatePart(RexCall adapted) {
        // adapted shape: CAST(PLUS(date_part('dow', _), 1) AS retType)
        RexCall outerCast = adapted;
        assertEquals(SqlKind.CAST, outerCast.getKind());
        RexCall plus = (RexCall) outerCast.getOperands().get(0);
        assertEquals(SqlKind.PLUS, plus.getKind());
        RexCall datePart = (RexCall) plus.getOperands().get(0);
        assertSame(SqlLibraryOperators.DATE_PART, datePart.getOperator());
        return datePart;
    }

    public void testTimestampOperandPassesThrough() {
        RexNode ts = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDayOfWeek(), List.of(ts));

        RexCall adapted = (RexCall) new DayOfWeekAdapter().adapt(original, List.of(), cluster);

        assertSame("TIMESTAMP operand must not be wrapped in CAST", ts, innerDatePart(adapted).getOperands().get(1));
    }

    public void testVarcharOperandIsCastToTimestamp() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode literal = rexBuilder.makeLiteral("2020-09-16", varcharType, true);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDayOfWeek(), List.of(literal));

        RexCall adapted = (RexCall) new DayOfWeekAdapter().adapt(original, List.of(), cluster);

        RexNode operand = innerDatePart(adapted).getOperands().get(1);
        assertTrue("VARCHAR operand must be wrapped", operand instanceof RexCall);
        RexCall cast = (RexCall) operand;
        assertEquals(SqlKind.CAST, cast.getKind());
        assertSame(SqlTypeName.TIMESTAMP, cast.getType().getSqlTypeName());
        assertSame(literal, cast.getOperands().get(0));
    }

    public void testInvalidLiteralRejectedAtPlanTime() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode literal = rexBuilder.makeLiteral("2025-13-02", varcharType, true);
        RexCall original = (RexCall) rexBuilder.makeCall(pplDayOfWeek(), List.of(literal));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DayOfWeekAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue(e.getMessage().contains("unsupported format"));
        assertTrue(e.getMessage().contains("2025-13-02"));
    }
}
