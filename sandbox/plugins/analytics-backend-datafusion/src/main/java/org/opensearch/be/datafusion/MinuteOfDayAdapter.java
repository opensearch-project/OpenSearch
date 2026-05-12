/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * PPL {@code minute_of_day(x)} → {@code CAST(date_part('hour', x) * 60 + date_part('minute', x)
 * AS <retType>)} for TIMESTAMP/DATE/STRING/TIME operands. Reference impl
 * {@code DateTimeFunctions.exprMinuteOfDay} computes {@code MINUTES.between(LocalTime.MIN, time)},
 * which equals {@code hour*60 + minute}.
 *
 * <p>TIME operand handling: see {@link DatePartAdapters}.
 *
 * @opensearch.internal
 */
class MinuteOfDayAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode arg = original.getOperands().get(0);
        if (arg.getType().getSqlTypeName() == SqlTypeName.TIME) {
            RexNode synthesized = DatetimeLiteralHelper.unwrapTimeLiteralToTimestamp(arg, rexBuilder);
            if (synthesized != null) {
                arg = synthesized;
            } else {
                RelDataType nullableVarchar = cluster.getTypeFactory().createTypeWithNullability(varchar, arg.getType().isNullable());
                arg = rexBuilder.makeCast(nullableVarchar, arg);
            }
        }
        RexNode hour = rexBuilder.makeCall(SqlLibraryOperators.DATE_PART, rexBuilder.makeLiteral("hour", varchar, true), arg);
        RexNode minute = rexBuilder.makeCall(SqlLibraryOperators.DATE_PART, rexBuilder.makeLiteral("minute", varchar, true), arg);
        RexNode sixty = rexBuilder.makeExactLiteral(BigDecimal.valueOf(60));
        RexNode product = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, hour, sixty);
        RexNode sum = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, product, minute);
        return rexBuilder.makeCast(original.getType(), sum);
    }
}
