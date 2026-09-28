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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * PPL {@code TIMESTAMPADD(unit, n, ts)} → {@code DATETIME_PLUS(CAST AS TIMESTAMP, base-unit interval)}.
 * Mirrors {@link DateAddSubAdapter}'s base-unit normalisation; MICROSECOND / unknown units pass through.
 *
 * <p>Literal-literal fold path: when {@code ts} is a recognizable VARCHAR literal we shift via
 * JDK calendar math ({@link LocalDateTime#plusMonths} etc.) so MONTH / QUARTER / YEAR are
 * calendar-exact (covers {@code timestampadd(YEAR, 15, '<lit>')}). Non-literal {@code ts} falls
 * through to the DATETIME_PLUS interval path, which is what gives standalone TIMESTAMPADD a
 * Substrait binding (mirrors {@link EarliestLatestAdapter}'s wired offset path).
 *
 * <p>TIME operand: anchored to today-UTC before lifting to TIMESTAMP, matching
 * {@link DateAddSubAdapter}'s TIME-operand handling.
 *
 * @opensearch.internal
 */
class TimestampAddAdapter implements ScalarFunctionAdapter {

    private static final long MILLIS_PER_SECOND = 1_000L;
    private static final long MILLIS_PER_MINUTE = 60_000L;
    private static final long MILLIS_PER_HOUR = 3_600_000L;
    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final long MILLIS_PER_WEEK = 7L * MILLIS_PER_DAY;

    private static final Map<String, long[]> UNIT_TO_BASE = Map.ofEntries(
        Map.entry("MILLISECOND", new long[] { 1L, 0L }),
        Map.entry("SECOND", new long[] { MILLIS_PER_SECOND, 0L }),
        Map.entry("MINUTE", new long[] { MILLIS_PER_MINUTE, 0L }),
        Map.entry("HOUR", new long[] { MILLIS_PER_HOUR, 0L }),
        Map.entry("DAY", new long[] { MILLIS_PER_DAY, 0L }),
        Map.entry("WEEK", new long[] { MILLIS_PER_WEEK, 0L }),
        Map.entry("MONTH", new long[] { 1L, 1L }),
        Map.entry("QUARTER", new long[] { 3L, 1L }),
        Map.entry("YEAR", new long[] { 12L, 1L })
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (!original.getOperator().getName().equalsIgnoreCase("TIMESTAMPADD") || original.getOperands().size() != 3) {
            return original;
        }
        RexNode unitOp = unwrapAnnotation(original.getOperands().get(0));
        RexNode countOp = unwrapAnnotation(original.getOperands().get(1));
        RexNode tsOp = unwrapAnnotation(original.getOperands().get(2));
        if (!(unitOp instanceof RexLiteral unitLit) || !(countOp instanceof RexLiteral countLit)) {
            return original;
        }
        String unit = unitLit.getValueAs(String.class);
        if (unit == null) {
            return original;
        }
        String upperUnit = unit.toUpperCase(Locale.ROOT);
        Long n;
        try {
            n = countLit.getValueAs(BigDecimal.class) == null ? null : countLit.getValueAs(BigDecimal.class).longValueExact();
        } catch (ArithmeticException unused) {
            return original;
        }
        if (n == null) {
            return original;
        }

        RexBuilder rb = cluster.getRexBuilder();

        // Literal-literal fold: when ts is a recognizable VARCHAR literal, shift in Java so
        // calendar-aware YEAR / QUARTER / MONTH math is exact (LocalDateTime.plusMonths
        // already handles month-end clamping per JDK contract).
        LocalDateTime tsLiteral = TimestampFunctionAdapter.extractLocalDateTimeLiteral(tsOp);
        if (tsLiteral != null) {
            LocalDateTime shifted = shiftLiteral(tsLiteral, upperUnit, n);
            if (shifted != null) {
                TimestampString tsStr = TimestampFunctionAdapter.toTimestampString(shifted);
                return rb.makeCast(original.getType(), rb.makeTimestampLiteral(tsStr, 3), true);
            }
            return original;  // overflow / unrecognized unit
        }

        long[] baseSpec = UNIT_TO_BASE.get(upperUnit);
        if (baseSpec == null) {
            return original;
        }
        long baseValue;
        try {
            baseValue = Math.multiplyExact(n, baseSpec[0]);
        } catch (ArithmeticException unused) {
            return original;
        }
        TimeUnit baseUnit = baseSpec[1] == 1L ? TimeUnit.MONTH : TimeUnit.DAY;

        // lift ts to call's TIMESTAMP; TIME → today-UTC anchored (matching DateAddSubAdapter)
        RelDataType targetType = original.getType();
        RexNode tsTimestamp;
        SqlTypeName tsKind = tsOp.getType().getSqlTypeName();
        if (tsKind == SqlTypeName.TIME) {
            RelDataType varchar = rb.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
            RelDataType nullableVarchar = rb.getTypeFactory().createTypeWithNullability(varchar, tsOp.getType().isNullable());
            RexNode timeAsVarchar = rb.makeCast(nullableVarchar, tsOp);
            String prefix = java.time.LocalDate.now(java.time.ZoneOffset.UTC).toString() + " ";
            RexNode prefixLit = rb.makeLiteral(prefix, varchar, false);
            RexNode concat = rb.makeCall(nullableVarchar, SqlStdOperatorTable.CONCAT, List.of(prefixLit, timeAsVarchar));
            tsTimestamp = rb.makeAbstractCast(targetType, concat);
        } else {
            tsTimestamp = rb.makeAbstractCast(targetType, tsOp);
        }

        RexNode interval = rb.makeIntervalLiteral(
            BigDecimal.valueOf(baseValue),
            new SqlIntervalQualifier(baseUnit, null, SqlParserPos.ZERO)
        );
        RexNode shifted = rb.makeCall(SqlStdOperatorTable.DATETIME_PLUS, tsTimestamp, interval);
        if (shifted.getType().equals(targetType)) {
            return shifted;
        }
        return rb.makeAbstractCast(targetType, shifted);
    }

    private static RexNode unwrapAnnotation(RexNode node) {
        if (node instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            return annotation.unwrap();
        }
        return node;
    }

    /** Apply {@code n} units of {@code upperUnit} to {@code base} via JDK calendar math. */
    private static LocalDateTime shiftLiteral(LocalDateTime base, String upperUnit, long n) {
        try {
            return switch (upperUnit) {
                case "MICROSECOND" -> base.plusNanos(Math.multiplyExact(n, 1_000L));
                case "MILLISECOND" -> base.plusNanos(Math.multiplyExact(n, 1_000_000L));
                case "SECOND" -> base.plusSeconds(n);
                case "MINUTE" -> base.plusMinutes(n);
                case "HOUR" -> base.plusHours(n);
                case "DAY" -> base.plusDays(n);
                case "WEEK" -> base.plusWeeks(n);
                case "MONTH" -> base.plusMonths(n);
                case "QUARTER" -> base.plusMonths(Math.multiplyExact(n, 3L));
                case "YEAR" -> base.plusYears(n);
                default -> null;
            };
        } catch (ArithmeticException | java.time.DateTimeException unused) {
            return null;
        }
    }
}
