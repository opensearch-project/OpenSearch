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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;

/**
 * Rewrites scalar PPL {@code earliest(literal, ts)} / {@code latest(literal, ts)}
 * RexCalls into engine-agnostic timestamp comparisons whose right-hand side is a
 * {@link DateTimeAdapters#LOCAL_NOW_OP now()}-symbolic expression. The expression
 * survives Substrait emit and is folded by DataFusion's {@code SimplifyExpressions}
 * at the engine's own plan time, anchored to {@code query_execution_start_time} —
 * keeping EARLIEST/LATEST coherent with every other PPL datetime function that
 * already lowers to DataFusion's {@code now()}.
 *
 * <h2>Emission shapes</h2>
 *
 * Picked by the relative-time DSL form. {@code BASE} below is
 * {@link DateTimeAdapters#LOCAL_NOW_OP now()}.
 *
 * <ul>
 *   <li><b>Absolute literal</b> ({@code '2024-01-15'}, {@code '2024-01-15 12:00:00'},
 *       ISO with {@code T}/{@code Z}) → {@code ts >= TIMESTAMP_LITERAL}. No "now"
 *       involved; resolved on the JVM side once.</li>
 *   <li><b>Pure offset</b> ({@code '-7d'}, {@code '+30m'}, {@code 'now'},
 *       {@code '-1mon-2d'}) → {@code ts >= DATETIME_PLUS(BASE, INTERVAL_DAY(millis))}
 *       composed left-to-right per chunk.</li>
 *   <li><b>Snap on a coarse unit</b> ({@code '@d'}, {@code '@h'}, {@code '@mon'},
 *       {@code '@y'}, {@code '@q'}) → {@code date_trunc('day'|'hour'|'month'|'year'|
 *       'quarter', BASE)}.</li>
 *   <li><b>Weekday snap</b> ({@code '@w'}, {@code '@w0'}–{@code '@w7'}) →
 *       {@code DATETIME_PLUS(date_trunc('day', BASE), INTERVAL_DAY(-diff))} where
 *       {@code diff} is the day-count from BASE's weekday back to the target weekday.
 *       {@code diff} is computed at adapter time using the JVM clock — see
 *       "Weekday-snap clock skew" below.</li>
 *   <li><b>Mixed</b> ({@code '-1h@d'}, {@code '@d-1h'}, {@code '-1mon@w'}) → chunks
 *       compose: each chunk transforms the previous expression. Snap-after-offset
 *       and offset-after-snap both work because every chunk reduces to a RexCall
 *       with the same TIMESTAMP type.</li>
 * </ul>
 *
 * <h2>Weekday-snap clock skew</h2>
 *
 * <p>Computing {@code diff} requires knowing what weekday "now" falls on. The JVM
 * adapter time and DataFusion's {@code query_execution_start_time} are taken at
 * different moments (different processes, ms-scale skew). For weekday-snap this
 * matters only when the query runs within a few-ms window straddling midnight on
 * a Sunday→Monday (or whichever day) boundary — extremely rare in OLAP workloads.
 * The non-weekday path uses {@code date_trunc} without any adapter-side weekday
 * lookup, so it's fully aligned with the engine's clock.
 *
 * <p>For absolute literals there's no "now" at all, so no skew concern.
 *
 * @opensearch.internal
 */
public final class EarliestLatestAdapter {

    private static final Logger LOGGER = LogManager.getLogger(EarliestLatestAdapter.class);

    private EarliestLatestAdapter() {}

    /** {@code earliest(expr, ts)} → {@code ts >= folded(expr)}. */
    public static final class EarliestAdapter implements ScalarFunctionAdapter {
        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            return rewrite(original, fieldStorage, cluster, true);
        }
    }

    /** {@code latest(expr, ts)} → {@code ts <= folded(expr)}. */
    public static final class LatestAdapter implements ScalarFunctionAdapter {
        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            return rewrite(original, fieldStorage, cluster, false);
        }
    }

    private static RexNode rewrite(RexCall call, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster, boolean isEarliest) {
        if (call.getOperands().size() != 2) {
            return call;
        }
        RexNode exprArg = call.getOperands().get(0);
        if (!(exprArg instanceof RexLiteral literal)) {
            // PPL grammar enforces a string literal here; non-literal is defensive.
            return call;
        }
        Object literalValue = literal.getValue2();
        if (literalValue == null) {
            return call;
        }
        String expression = literalValue.toString();

        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode tsArg = call.getOperands().get(1);

        RexNode rhs;
        try {
            rhs = buildRhs(expression, rexBuilder);
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to fold scalar earliest/latest expression [{}]: {}", expression, e.getMessage());
            return call;
        }

        // Align the column side with a plain TIMESTAMP if needed (it normally is on
        // the analytics-engine path, but be defensive against UDT carryovers).
        RexNode tsAligned = tsArg;
        if (tsArg.getType().getSqlTypeName() != SqlTypeName.TIMESTAMP) {
            RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
            RelDataType plainTs = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3),
                tsArg.getType().isNullable()
            );
            tsAligned = rexBuilder.makeCast(plainTs, tsArg);
        }

        return rexBuilder.makeCall(
            isEarliest ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            tsAligned,
            rhs
        );
    }

    /** Builds the right-hand side of the comparison (a TIMESTAMP-typed expression). */
    private static RexNode buildRhs(String input, RexBuilder rexBuilder) {
        if (input == null) {
            throw new IllegalArgumentException("relative-time expression cannot be null");
        }
        String trimmed = input.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("relative-time expression cannot be empty");
        }

        // Absolute timestamp literal — emit a TIMESTAMP_LITERAL. No "now" needed.
        Long absoluteMillis = tryParseAbsolute(trimmed);
        if (absoluteMillis != null) {
            return makeTimestampMillisLiteral(rexBuilder, absoluteMillis);
        }

        // Symbolic now() base; chunks transform it left-to-right.
        RexNode result = rexBuilder.makeCall(DateTimeAdapters.LOCAL_NOW_OP);

        if ("now".equalsIgnoreCase(trimmed) || "now()".equalsIgnoreCase(trimmed)) {
            return result;
        }

        int i = 0;
        while (i < trimmed.length()) {
            char c = trimmed.charAt(i);
            if (c == '@') {
                int j = i + 1;
                while (j < trimmed.length() && Character.isLetterOrDigit(trimmed.charAt(j))) {
                    j++;
                }
                String rawUnit = trimmed.substring(i + 1, j);
                result = applySnap(result, rawUnit, rexBuilder);
                i = j;
            } else if (c == '+' || c == '-') {
                int j = i + 1;
                while (j < trimmed.length() && Character.isDigit(trimmed.charAt(j))) {
                    j++;
                }
                String valueStr = trimmed.substring(i + 1, j);
                int value = valueStr.isEmpty() ? 1 : Integer.parseInt(valueStr);
                int k = j;
                while (k < trimmed.length() && Character.isLetter(trimmed.charAt(k))) {
                    k++;
                }
                String rawUnit = trimmed.substring(j, k);
                result = applyOffset(result, c, value, rawUnit, rexBuilder);
                i = k;
            } else {
                throw new IllegalArgumentException("Unexpected character '" + c + "' at position " + i + " in input: " + trimmed);
            }
        }
        return result;
    }

    // ── chunk → RexNode transforms ──────────────────────────────────────────────

    /** {@code base + sign * value <unit>} as a RexCall. */
    private static RexNode applyOffset(RexNode base, char sign, int value, String rawUnit, RexBuilder rexBuilder) {
        String unit = normalizeUnit(rawUnit);
        long signedValue = (sign == '-' ? -1L : 1L) * value;
        long millis;
        SqlIntervalQualifier qualifier;

        switch (unit) {
            case "s" -> {
                millis = signedValue * 1000L;
                qualifier = new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
            }
            case "m" -> {
                millis = signedValue * 60_000L;
                qualifier = new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
            }
            case "h" -> {
                millis = signedValue * 3_600_000L;
                qualifier = new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
            }
            case "d" -> {
                millis = signedValue * 86_400_000L;
                qualifier = new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
            }
            case "w" -> {
                millis = signedValue * 7L * 86_400_000L;
                qualifier = new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
            }
            case "M" -> {
                // MONTH and YEAR use INTERVAL_YEAR_MONTH (months as backing unit), not millis.
                long months = signedValue;
                return makeIntervalAdd(
                    base,
                    BigDecimal.valueOf(months),
                    new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO),
                    rexBuilder
                );
            }
            case "y" -> {
                long months = signedValue * 12L;
                return makeIntervalAdd(
                    base,
                    BigDecimal.valueOf(months),
                    new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO),
                    rexBuilder
                );
            }
            case "q" -> {
                long months = signedValue * 3L;
                return makeIntervalAdd(
                    base,
                    BigDecimal.valueOf(months),
                    new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO),
                    rexBuilder
                );
            }
            default -> throw new IllegalArgumentException("Unsupported offset unit: " + rawUnit);
        }

        return makeIntervalAdd(base, BigDecimal.valueOf(millis), qualifier, rexBuilder);
    }

    private static RexNode makeIntervalAdd(RexNode base, BigDecimal value, SqlIntervalQualifier qualifier, RexBuilder rexBuilder) {
        RexNode interval = rexBuilder.makeIntervalLiteral(value, qualifier);
        return rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, base, interval);
    }

    /** {@code @<unit>(base)} → {@code date_trunc(unit, base)} or weekday-shifted variant. */
    private static RexNode applySnap(RexNode base, String rawUnit, RexBuilder rexBuilder) {
        String unit = normalizeUnit(rawUnit);
        return switch (unit) {
            case "s" -> dateTrunc(base, "second", rexBuilder);
            case "m" -> dateTrunc(base, "minute", rexBuilder);
            case "h" -> dateTrunc(base, "hour", rexBuilder);
            case "d" -> dateTrunc(base, "day", rexBuilder);
            case "M" -> dateTrunc(base, "month", rexBuilder);
            case "y" -> dateTrunc(base, "year", rexBuilder);
            case "q" -> dateTrunc(base, "quarter", rexBuilder);
            case "w" -> applyWeekdaySnap(base, 7, rexBuilder); // PPL @w == Sunday
            default -> {
                if (unit.matches("w[0-7]")) {
                    int targetDay = unit.equals("w0") || unit.equals("w7") ? 7 : Integer.parseInt(unit.substring(1));
                    yield applyWeekdaySnap(base, targetDay, rexBuilder);
                }
                throw new IllegalArgumentException("Unsupported snap unit: " + rawUnit);
            }
        };
    }

    /**
     * Weekday snap: most recent (≤ today) day whose ISO weekday matches {@code targetIsoDay}
     * (1=Mon..7=Sun). Compute the day-count delta from the adapter-time clock; emit
     * {@code date_trunc('day', base) - <delta> days}. See class javadoc for the
     * (rare) skew implication.
     */
    private static RexNode applyWeekdaySnap(RexNode base, int targetIsoDay, RexBuilder rexBuilder) {
        int todayIsoDay = ZonedDateTime.now(ZoneOffset.UTC).getDayOfWeek().getValue(); // 1..7
        int diff = (todayIsoDay - targetIsoDay + 7) % 7; // backward day count, 0..6

        RexNode dayTrunc = dateTrunc(base, "day", rexBuilder);
        if (diff == 0) {
            return dayTrunc;
        }
        SqlIntervalQualifier dayQualifier = new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
        RexNode interval = rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(-diff * 86_400_000L), dayQualifier);
        return rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, dayTrunc, interval);
    }

    private static RexNode dateTrunc(RexNode base, String unit, RexBuilder rexBuilder) {
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType varcharType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode unitLit = rexBuilder.makeLiteral(unit, varcharType, false);
        return rexBuilder.makeCall(DateTimeAdapters.LOCAL_DATE_TRUNC_OP, unitLit, base);
    }

    private static RexNode makeTimestampMillisLiteral(RexBuilder rexBuilder, long epochMillis) {
        return rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(epochMillis), 3);
    }

    private static String normalizeUnit(String rawUnit) {
        switch (rawUnit.toLowerCase(Locale.ROOT)) {
            case "m", "min", "mins", "minute", "minutes" -> {
                return "m";
            }
            case "s", "sec", "secs", "second", "seconds" -> {
                return "s";
            }
            case "h", "hr", "hrs", "hour", "hours" -> {
                return "h";
            }
            case "d", "day", "days" -> {
                return "d";
            }
            case "w", "wk", "wks", "week", "weeks" -> {
                return "w";
            }
            case "mon", "month", "months" -> {
                return "M";
            }
            case "y", "yr", "yrs", "year", "years" -> {
                return "y";
            }
            case "q", "qtr", "qtrs", "quarter", "quarters" -> {
                return "q";
            }
            default -> {
                String lower = rawUnit.toLowerCase(Locale.ROOT);
                if (lower.matches("w[0-7]")) return lower;
                throw new IllegalArgumentException("Unsupported unit alias: " + rawUnit);
            }
        }
    }

    // ── Absolute timestamp literal parsing (port from EarliestLatestAdapter) ────

    private static final DateTimeFormatter[] ABSOLUTE_FORMATTERS = new DateTimeFormatter[] {
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ROOT),
        DateTimeFormatter.ISO_DATE_TIME,
        new DateTimeFormatterBuilder().appendPattern("MM/dd/yyyy:HH:mm:ss").toFormatter(Locale.ROOT), };

    /** Parses an absolute timestamp; returns epoch millis or null if the input isn't an absolute literal. */
    private static Long tryParseAbsolute(String input) {
        // ISO date-only first.
        try {
            LocalDate d = LocalDate.parse(input);
            return d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (DateTimeParseException ignored) {
            // not a date-only
        }

        // ISO instant ('2024-01-15T12:00:00Z') first — has zone info.
        try {
            return Instant.parse(input).toEpochMilli();
        } catch (DateTimeParseException ignored) {
            // not an ISO instant
        }

        for (DateTimeFormatter fmt : ABSOLUTE_FORMATTERS) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(input, fmt);
                return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
            } catch (DateTimeParseException ignored) {
                // try next formatter
            }
        }
        return null;
    }

    /** Probes for ChronoUnit for unit tests / debug; not used in adapt path. */
    @SuppressWarnings("unused")
    private static ChronoUnit mapChronoUnit(String unit) {
        return switch (unit) {
            case "s" -> ChronoUnit.SECONDS;
            case "m" -> ChronoUnit.MINUTES;
            case "h" -> ChronoUnit.HOURS;
            case "d" -> ChronoUnit.DAYS;
            case "w" -> ChronoUnit.WEEKS;
            case "M" -> ChronoUnit.MONTHS;
            case "y" -> ChronoUnit.YEARS;
            default -> throw new IllegalArgumentException("Unsupported unit: " + unit);
        };
    }
}
