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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * VARCHAR-coercion coverage for {@link SecondAdapter}. Pre-fix this adapter
 * built {@code date_part('second', <varchar>)} which the substrait matcher
 * rejected as {@code DATE_PART(string, string)}.
 */
public class SecondAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    private SqlFunction pplSecond() {
        return new SqlFunction(
            "SECOND",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
    }

    private RexCall innerDatePart(RexCall adapted) {
        // adapted shape: CAST(FLOOR(CAST(date_part('second', _) AS DOUBLE)) AS retType)
        assertEquals(SqlKind.CAST, adapted.getKind());
        RexCall floor = (RexCall) adapted.getOperands().get(0);
        assertSame(SqlStdOperatorTable.FLOOR, floor.getOperator());
        RexCall innerCast = (RexCall) floor.getOperands().get(0);
        assertEquals(SqlKind.CAST, innerCast.getKind());
        RexCall datePart = (RexCall) innerCast.getOperands().get(0);
        assertSame(SqlLibraryOperators.DATE_PART, datePart.getOperator());
        return datePart;
    }

    public void testTimestampOperandPassesThrough() {
        RexNode ts = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
        RexCall original = (RexCall) rexBuilder.makeCall(pplSecond(), List.of(ts));

        RexCall adapted = (RexCall) new SecondAdapter().adapt(original, List.of(), cluster);

        assertSame("TIMESTAMP operand must not be wrapped in CAST", ts, innerDatePart(adapted).getOperands().get(1));
    }

    public void testVarcharOperandIsCastToTimestamp() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode literal = rexBuilder.makeLiteral("2020-09-16 17:30:45", varcharType, true);
        RexCall original = (RexCall) rexBuilder.makeCall(pplSecond(), List.of(literal));

        RexCall adapted = (RexCall) new SecondAdapter().adapt(original, List.of(), cluster);

        RexNode operand = innerDatePart(adapted).getOperands().get(1);
        assertTrue("VARCHAR operand must be wrapped", operand instanceof RexCall);
        RexCall cast = (RexCall) operand;
        assertEquals(SqlKind.CAST, cast.getKind());
        assertSame(SqlTypeName.TIMESTAMP, cast.getType().getSqlTypeName());
        assertSame(literal, cast.getOperands().get(0));
    }

    public void testInvalidLiteralRejectedAtPlanTime() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode literal = rexBuilder.makeLiteral("2025-12-01 15:02:61", varcharType, true);
        RexCall original = (RexCall) rexBuilder.makeCall(pplSecond(), List.of(literal));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SecondAdapter().adapt(original, List.of(), cluster)
        );
        assertTrue(e.getMessage().contains("unsupported format"));
        assertTrue(e.getMessage().contains("2025-12-01 15:02:61"));
    }
}
