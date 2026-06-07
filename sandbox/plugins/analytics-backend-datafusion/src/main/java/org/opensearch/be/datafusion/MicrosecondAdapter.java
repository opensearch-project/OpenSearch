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
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType varchar = factory.createSqlType(SqlTypeName.VARCHAR);
        // when the operand is a string/timestamp expression, force a TIMESTAMP(6) coercion so
        // DataFusion doesn't downcast to ms and clip the µs fraction before date_part runs.
        // TIME operands are anchored to today UTC; bare-time strings get the same prefix.
        RexNode operand = original.getOperands().get(0);
        SqlTypeName operandType = operand.getType().getSqlTypeName();
        RelDataType tsMicros = factory.createSqlType(SqlTypeName.TIMESTAMP, 6);
        if (operandType == SqlTypeName.TIME) {
            // TIME → 'today HH:MM:SS.SSSSSS' → TIMESTAMP(6) directly (no intermediate TIMESTAMP cast).
            operand = todayPrefixedTimeAsTimestamp6(operand, rexBuilder, factory, tsMicros);
        } else if (operandType == SqlTypeName.VARCHAR || operandType == SqlTypeName.CHAR) {
            // Cast straight to TIMESTAMP(6) so the µs fraction survives. Bare-time strings would
            // need today-anchoring; route those through the helper, otherwise direct cast.
            operand = stringAsTimestamp6(operand, rexBuilder, factory, tsMicros);
        } else if (operandType == SqlTypeName.TIMESTAMP) {
            operand = rexBuilder.makeCast(tsMicros, operand);
        }
        RexNode partLiteral = rexBuilder.makeLiteral("microsecond", varchar, true);
        RexNode datePart = rexBuilder.makeCall(SqlLibraryOperators.DATE_PART, partLiteral, operand);
        RexNode mod = rexBuilder.makeCall(SqlStdOperatorTable.MOD, datePart, rexBuilder.makeExactLiteral(ONE_MILLION));
        return rexBuilder.makeCast(original.getType(), mod);
    }

    /** TIME → CONCAT('today ', CAST(time AS VARCHAR)) → TIMESTAMP(6). */
    private static RexNode todayPrefixedTimeAsTimestamp6(
        RexNode operand,
        RexBuilder rexBuilder,
        RelDataTypeFactory factory,
        RelDataType tsMicros
    ) {
        RelDataType varchar = factory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType nullableVarchar = factory.createTypeWithNullability(varchar, operand.getType().isNullable());
        RexNode timeAsVarchar = rexBuilder.makeCast(nullableVarchar, operand);
        String prefix = java.time.LocalDate.now(java.time.ZoneOffset.UTC).toString() + " ";
        RexNode prefixLit = rexBuilder.makeLiteral(prefix, varchar, false);
        RexNode concat = rexBuilder.makeCall(nullableVarchar, SqlStdOperatorTable.CONCAT, List.of(prefixLit, timeAsVarchar));
        return rexBuilder.makeCast(tsMicros, concat);
    }

    /** String → TIMESTAMP(6) preserving µs. Bare-time strings get today-anchored first. */
    private static RexNode stringAsTimestamp6(RexNode operand, RexBuilder rexBuilder, RelDataTypeFactory factory, RelDataType tsMicros) {
        if (isBareTimeStringLiteral(operand)) {
            RelDataType varchar = factory.createSqlType(SqlTypeName.VARCHAR);
            String prefix = java.time.LocalDate.now(java.time.ZoneOffset.UTC).toString() + " ";
            RexNode prefixLit = rexBuilder.makeLiteral(prefix, varchar, false);
            RexNode concat = rexBuilder.makeCall(varchar, SqlStdOperatorTable.CONCAT, List.of(prefixLit, operand));
            return rexBuilder.makeCast(tsMicros, concat);
        }
        return rexBuilder.makeCast(tsMicros, operand);
    }

    /** True for a CHAR/VARCHAR {@link org.apache.calcite.rex.RexLiteral} that parses as bare time only. */
    private static boolean isBareTimeStringLiteral(RexNode operand) {
        if (!(operand instanceof org.apache.calcite.rex.RexLiteral literal)) return false;
        String value = literal.getValueAs(String.class);
        if (value == null) return false;
        try {
            java.time.LocalTime.parse(value);
            return true;
        } catch (java.time.format.DateTimeParseException ignored) {
            return false;
        }
    }
}
