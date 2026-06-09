/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.avatica.util.TimeUnit;
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
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/** Covers {@link DateAddSubAdapter}: TIME base anchors to today UTC before DATETIME_PLUS. */
public class DateAddSubAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    private static final SqlFunction DATE_ADD_OP = new SqlFunction(
        "DATE_ADD",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.TIMEDATE
    );

    /** DATE_ADD(TIME-col, INTERVAL 1 DAY) → DATETIME_PLUS(CAST(CONCAT(today,' ',CAST time AS VARCHAR)) AS TIMESTAMP), 86400000 millis). */
    public void testDateAddTimeOperandAnchoredToToday() {
        RelDataType timeType = typeFactory.createSqlType(SqlTypeName.TIME);
        RexNode timeCol = rexBuilder.makeInputRef(timeType, 0);
        RexNode interval = rexBuilder.makeIntervalLiteral(
            BigDecimal.valueOf(1L),
            new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO)
        );
        RexCall original = (RexCall) rexBuilder.makeCall(DATE_ADD_OP, List.of(timeCol, interval));

        RexNode adapted = new DateAddSubAdapter(true).adapt(original, List.of(), cluster);

        // Outer DATETIME_PLUS (or CAST wrapping it).
        RexCall outer = adapted instanceof RexCall && ((RexCall) adapted).getKind() == SqlKind.CAST
            ? (RexCall) ((RexCall) adapted).getOperands().get(0)
            : (RexCall) adapted;
        assertSame(SqlStdOperatorTable.DATETIME_PLUS, outer.getOperator());
        // First operand is CAST(CONCAT(today,' ',CAST(time AS VARCHAR)) AS TIMESTAMP).
        RexCall castNode = (RexCall) outer.getOperands().get(0);
        assertEquals(SqlKind.CAST, castNode.getKind());
        RexCall concat = (RexCall) castNode.getOperands().get(0);
        assertEquals("||", concat.getOperator().getName());
    }
}
