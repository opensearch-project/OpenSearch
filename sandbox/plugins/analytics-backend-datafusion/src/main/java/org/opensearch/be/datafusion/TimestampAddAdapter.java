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
 * Standalone PPL {@code TIMESTAMPADD(unit, n, t)} → {@code DATETIME_PLUS(t, INTERVAL n*<m> <DAY|MONTH>)}.
 * Mirrors {@link EarliestLatestAdapter#applyOffset} which has end-to-end Substrait wiring proof
 * via {@code add(timestamp, interval_day_time | interval_year_month)} → DataFusion's native
 * interval-add. PPL TIMESTAMPADD has no Substrait extension binding, so without this rewrite
 * isthmus rejects every standalone call as
 * {@code "Unable to convert call TIMESTAMPADD(string, i32, precision_timestamp<P>)"}.
 *
 * <p>The peephole {@code TIMESTAMPDIFF(out, t, TIMESTAMPADD(in, n, t))} shape is folded by
 * {@link TimestampDiffAdapter} before this adapter runs, so this adapter only sees standalone
 * calls. Unit / count operand shapes that don't reduce to literals (column ref unit, expression
 * count) fall through unchanged — column-driven units would need a runtime UDF.
 *
 * @opensearch.internal
 */
class TimestampAddAdapter implements ScalarFunctionAdapter {

    /** Fixed-length unit → milliseconds; null entries are variable-length (handled via INTERVAL_YEAR_MONTH). */
    private static final Map<String, Long> FIXED_UNIT_TO_MILLIS = Map.ofEntries(
        Map.entry("MILLISECOND", 1L),
        Map.entry("SECOND", 1_000L),
        Map.entry("MINUTE", 60_000L),
        Map.entry("HOUR", 3_600_000L),
        Map.entry("DAY", 86_400_000L),
        Map.entry("WEEK", 604_800_000L)
    );

    /** Variable-length unit → number of months (for INTERVAL_YEAR_MONTH backing). */
    private static final Map<String, Long> VARIABLE_UNIT_TO_MONTHS = Map.of("MONTH", 1L, "QUARTER", 3L, "YEAR", 12L);

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 3) {
            return original;
        }
        String unit = stringLiteralValue(unwrapAnnotation(original.getOperands().get(0)));
        Long n = integerLiteralValue(unwrapAnnotation(original.getOperands().get(1)));
        RexNode base = unwrapAnnotation(original.getOperands().get(2));
        if (unit == null || n == null) {
            return original;
        }
        String upperUnit = unit.toUpperCase(Locale.ROOT);
        RexBuilder rexBuilder = cluster.getRexBuilder();

        // Literal-literal fold: when base is a VARCHAR literal, parse it and shift in
        // Java, returning a TIMESTAMP literal directly. Skips DATETIME_PLUS entirely so
        // calendar-aware YEAR / QUARTER / MONTH math stays exact (LocalDateTime.plusMonths
        // already handles month-end clamping per JDK contract). This is the path that
        // covers DateTimeFunctionIT#testTimeStampAdd's `timestampadd(YEAR, 15, '<lit>')`.
        LocalDateTime baseLiteral = parseVarcharLiteral(base);
        if (baseLiteral != null) {
            LocalDateTime shifted = shiftLiteral(baseLiteral, upperUnit, n);
            if (shifted != null) {
                TimestampString tsStr = TimestampFunctionAdapter.toTimestampString(shifted);
                return rexBuilder.makeCast(original.getType(), rexBuilder.makeTimestampLiteral(tsStr, 3), true);
            }
            return original;  // overflow / unrecognized unit
        }

        Long fixedMs = FIXED_UNIT_TO_MILLIS.get(upperUnit);
        if (fixedMs != null) {
            long totalMs;
            try {
                totalMs = Math.multiplyExact(n, fixedMs);
            } catch (ArithmeticException unused) {
                return original;
            }
            // INTERVAL_DAY_TIME with milliseconds backing — same shape EarliestLatestAdapter uses
            // for s/m/h/d/w. Calcite represents this as SqlIntervalQualifier(DAY) with a millis value.
            SqlIntervalQualifier dayQualifier = new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
            RexNode intervalLit = rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(totalMs), dayQualifier);
            RexNode added = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, base, intervalLit);
            return rexBuilder.makeCast(original.getType(), added, true);
        }

        Long monthFactor = VARIABLE_UNIT_TO_MONTHS.get(upperUnit);
        if (monthFactor != null) {
            long totalMonths;
            try {
                totalMonths = Math.multiplyExact(n, monthFactor);
            } catch (ArithmeticException unused) {
                return original;
            }
            SqlIntervalQualifier monthQualifier = new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO);
            RexNode intervalLit = rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(totalMonths), monthQualifier);
            RexNode added = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, base, intervalLit);
            return rexBuilder.makeCast(original.getType(), added, true);
        }

        // Unrecognized unit literal — leave the call unchanged so isthmus surfaces the error.
        return original;
    }

    private static RexNode unwrapAnnotation(RexNode node) {
        if (node instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            return annotation.unwrap();
        }
        return node;
    }

    private static String stringLiteralValue(RexNode node) {
        if (!(node instanceof RexLiteral lit)) return null;
        SqlTypeName typeName = lit.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) return null;
        return lit.getValueAs(String.class);
    }

    /**
     * Parse a literal datetime operand to {@link LocalDateTime} via
     * {@link TimestampFunctionAdapter#extractLocalDateTimeLiteral} which recognizes both
     * raw VARCHAR literals and shapes coerced by {@code DatetimeOperandCoerceShuttle}
     * (TIMESTAMP-typed literals or CAST RexCalls). Returns null when the operand isn't a
     * recognizable literal — caller falls through to the non-literal interval-add path.
     */
    private static LocalDateTime parseVarcharLiteral(RexNode node) {
        return TimestampFunctionAdapter.extractLocalDateTimeLiteral(node);
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

    private static Long integerLiteralValue(RexNode node) {
        if (!(node instanceof RexLiteral lit)) return null;
        Object value = lit.getValue();
        if (value instanceof BigDecimal bd) {
            try {
                return bd.longValueExact();
            } catch (ArithmeticException unused) {
                return null;
            }
        }
        if (value instanceof Number num) return num.longValue();
        return null;
    }
}
