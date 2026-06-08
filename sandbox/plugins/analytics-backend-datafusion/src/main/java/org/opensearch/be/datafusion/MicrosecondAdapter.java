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
 * PPL {@code microsecond(x)} → {@code CAST(MOD(date_part('microsecond', x), 1_000_000) AS <retType>)}:
 * PPL {@code MICROSECOND()} returns only the sub-second microseconds (0..999_999), but
 * DataFusion/Postgres {@code date_part('microsecond', x)} returns
 * {@code seconds * 1_000_000 + microseconds} (e.g. 46_123_456 for {@code 01:34:46.123456}).
 * Wrap with {@code MOD(..., 1_000_000)} to drop the seconds component, matching PPL semantics
 * and what {@code DateTimeFunctionIT#testMicrosecond} expects.
 *
 * @opensearch.internal
 */
class MicrosecondAdapter implements ScalarFunctionAdapter {

    private static final BigDecimal ONE_MILLION = BigDecimal.valueOf(1_000_000L);

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode partLiteral = rexBuilder.makeLiteral("microsecond", varchar, true);
        // Coerce a character/TIME operand to TIMESTAMP at emit time so date_part's Substrait
        // signature resolves (DATE_PART has no (string, ...) impl). Mirrors DayOfWeek/Second.
        RexNode operand = DatePartAdapters.coerceCharacterOperandToTimestamp(original.getOperands().get(0), cluster);
        RexNode datePart = rexBuilder.makeCall(SqlLibraryOperators.DATE_PART, partLiteral, operand);
        RexNode mod = rexBuilder.makeCall(SqlStdOperatorTable.MOD, datePart, rexBuilder.makeExactLiteral(ONE_MILLION));
        return rexBuilder.makeCast(original.getType(), mod);
    }
}
