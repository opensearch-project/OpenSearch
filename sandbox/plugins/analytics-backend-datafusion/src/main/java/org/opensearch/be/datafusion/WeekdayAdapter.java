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
 * PPL {@code weekday(x)} → {@code CAST((date_part('dow', x) + 6) MOD 7 AS <retType>)}.
 *
 * <p>PPL/MySQL {@code WEEKDAY} indexes Monday=0 .. Sunday=6, but DataFusion/Postgres
 * {@code date_part('dow')} returns Sunday=0 .. Saturday=6. The {@code (dow + 6) MOD 7} remap shifts
 * Sunday(0)→6 and Monday(1)→0, matching MySQL.
 *
 * <p>VARCHAR / TIME operand handling is shared via
 * {@link DatePartAdapters#coerceCharacterOperandToTimestamp} (see {@link DayOfWeekAdapter}).
 *
 * @opensearch.internal
 */
class WeekdayAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode partLiteral = rexBuilder.makeLiteral("dow", varchar, true);
        RexNode operand = DatePartAdapters.coerceCharacterOperandToTimestamp(original.getOperands().get(0), cluster);
        RexNode dow = rexBuilder.makeCall(SqlLibraryOperators.DATE_PART, partLiteral, operand);
        RexNode shifted = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, dow, rexBuilder.makeExactLiteral(BigDecimal.valueOf(6)));
        RexNode weekday = rexBuilder.makeCall(SqlStdOperatorTable.MOD, shifted, rexBuilder.makeExactLiteral(BigDecimal.valueOf(7)));
        return rexBuilder.makeCast(original.getType(), weekday);
    }
}
