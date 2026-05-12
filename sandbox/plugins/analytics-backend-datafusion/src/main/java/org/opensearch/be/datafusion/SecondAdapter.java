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

import java.util.List;

/**
 * PPL {@code second}/{@code second_of_minute} → {@code CAST(FLOOR(date_part('second', x)) AS ret)}.
 * FLOOR drops {@code date_part}'s fp64 fractional part (integer portion already in [0, 59]); the
 * intermediate CAST to DOUBLE is needed because our substrait YAML declares date_part/floor as
 * fp64-only while Calcite's inference returns BIGINT for {@code part='second'}.
 *
 * <p>TIME operand handling: see {@link DatePartAdapters}.
 *
 * @opensearch.internal
 */
class SecondAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode partLiteral = rexBuilder.makeLiteral("second", varchar, true);
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
        RexNode datePart = rexBuilder.makeCall(SqlLibraryOperators.DATE_PART, partLiteral, arg);
        RelDataType doubleType = cluster.getTypeFactory()
            .createTypeWithNullability(cluster.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), datePart.getType().isNullable());
        RexNode datePartDouble = rexBuilder.makeCast(doubleType, datePart);
        RexNode floored = rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, datePartDouble);
        return rexBuilder.makeCast(original.getType(), floored);
    }
}
