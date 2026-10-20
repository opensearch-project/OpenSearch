/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * PPL {@code last_day(x)} — the last day of {@code x}'s month, returned as a DATE.
 *
 * <p>Lowering: {@code date_trunc('month', x) + INTERVAL 1 MONTH - INTERVAL 1 DAY}, cast to the
 * call's declared DATE type. {@code date_trunc('month', x)} snaps to the first of the month,
 * {@code + 1 month} reaches the first of the next month, and {@code - 1 day} lands on the last day
 * of the original month (correct for all month lengths, including leap-year February). Routes
 * through the local {@code date_trunc} UDF ({@link DateTimeAdapters#LOCAL_DATE_TRUNC_OP}) and
 * {@code DATETIME_PLUS}, both of which bind via Substrait (the stock
 * {@code SqlLibraryOperators.DATE_TRUNC} does not).
 *
 * <p>DATE / TIMESTAMP operands cast straight to TIMESTAMP; VARCHAR / TIME operands are anchored via
 * {@link DatePartAdapters#coerceCharacterOperandToTimestamp}.
 *
 * @opensearch.internal
 */
class LastDayAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType tsType = rexBuilder.getTypeFactory()
            .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP), true);

        RexNode operand = original.getOperands().get(0);
        RexNode asTimestamp;
        SqlTypeName operandType = operand.getType().getSqlTypeName();
        if (operandType == SqlTypeName.TIMESTAMP
            || operandType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            || operandType == SqlTypeName.DATE) {
            asTimestamp = rexBuilder.makeCast(tsType, operand);
        } else {
            // VARCHAR date string / TIME — anchor to a TIMESTAMP the engine can read.
            asTimestamp = DatePartAdapters.coerceCharacterOperandToTimestamp(operand, cluster);
        }

        RelDataType varchar = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode monthLiteral = rexBuilder.makeLiteral("month", varchar, true);
        RexNode firstOfMonth = rexBuilder.makeCall(DateTimeAdapters.LOCAL_DATE_TRUNC_OP, monthLiteral, asTimestamp);

        RexNode plusMonth = rexBuilder.makeCall(
            SqlStdOperatorTable.DATETIME_PLUS,
            firstOfMonth,
            rexBuilder.makeIntervalLiteral(BigDecimal.ONE, new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO))
        );
        RexNode lastDay = rexBuilder.makeCall(
            SqlStdOperatorTable.DATETIME_PLUS,
            plusMonth,
            rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(-1), new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO))
        );

        if (lastDay.getType().equals(original.getType())) {
            return lastDay;
        }
        return rexBuilder.makeCast(original.getType(), lastDay, true);
    }
}
