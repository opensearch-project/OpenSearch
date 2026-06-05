/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Plan-time fold for PPL {@code TIMESTAMPADD(unit, n, t)} calls where:
 * <ul>
 *   <li>{@code unit} is a VARCHAR literal naming a recognised PPL IntervalUnit
 *       ({@code SECOND} / {@code MINUTE} / {@code HOUR} / {@code DAY} /
 *       {@code WEEK} / {@code MONTH} / {@code QUARTER} / {@code YEAR}); and</li>
 *   <li>{@code n} is an INTEGER literal; and</li>
 *   <li>{@code t} is a VARCHAR literal parseable as {@code yyyy-MM-dd HH:mm:ss}
 *       or ISO-{@code yyyy-MM-ddTHH:mm:ss}.</li>
 * </ul>
 *
 * <p>The fold materializes a TIMESTAMP literal at plan time, sidestepping
 * isthmus's "Unable to convert call TIMESTAMPADD(string, i32, string)" error —
 * neither isthmus's default catalog nor the sandbox YAML extension has a
 * substrait binding for {@code TIMESTAMPADD}, so the only way to lower this
 * shape end-to-end is to remove the call entirely.
 *
 * <p>Non-foldable shapes (column refs, mixed literal/column inputs) fall
 * through unchanged; isthmus surfaces them as the original error so users see
 * an actionable message rather than silent wrong answers. End-to-end column
 * arithmetic is a follow-up that requires a Substrait-bound interval-add
 * primitive (see {@link TimestampDiffAdapter}'s notes on
 * {@code DATETIME_PLUS(t, INTERVAL_YEAR_MONTH)} for the long-term path).
 *
 * @opensearch.internal
 */
class TimestampAddAdapter implements ScalarFunctionAdapter {

    /** PPL IntervalUnit name → fold by adding {@code n * unit} to the LocalDateTime. */
    private static final Map<String, java.time.temporal.TemporalUnit> UNIT_MAP = Map.ofEntries(
        Map.entry("SECOND", java.time.temporal.ChronoUnit.SECONDS),
        Map.entry("MINUTE", java.time.temporal.ChronoUnit.MINUTES),
        Map.entry("HOUR", java.time.temporal.ChronoUnit.HOURS),
        Map.entry("DAY", java.time.temporal.ChronoUnit.DAYS),
        Map.entry("WEEK", java.time.temporal.ChronoUnit.WEEKS),
        Map.entry("MONTH", java.time.temporal.ChronoUnit.MONTHS),
        Map.entry("QUARTER", java.time.temporal.ChronoUnit.MONTHS), // ×3 below
        Map.entry("YEAR", java.time.temporal.ChronoUnit.YEARS)
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (!original.getOperator().getName().equalsIgnoreCase("TIMESTAMPADD")) {
            return original;
        }
        if (original.getOperands().size() != 3) {
            return original;
        }
        RexNode unitArg = unwrapAnnotation(original.getOperands().get(0));
        RexNode nArg = unwrapAnnotation(original.getOperands().get(1));
        RexNode tArg = unwrapAnnotation(original.getOperands().get(2));

        String unit = stringLiteralValue(unitArg);
        Long n = integerLiteralValue(nArg);
        String tString = stringLiteralValue(tArg);
        if (unit == null || n == null || tString == null) {
            // Not all literals; can't fold. Leave for isthmus to surface.
            return original;
        }

        java.time.temporal.TemporalUnit chronoUnit = UNIT_MAP.get(unit.toUpperCase(Locale.ROOT));
        if (chronoUnit == null) {
            return original;
        }
        long magnitude = "QUARTER".equalsIgnoreCase(unit) ? Math.multiplyExact(n, 3L) : n;

        LocalDateTime base = parseTimestampLiteral(tString);
        if (base == null) {
            return original;
        }
        LocalDateTime result = base.plus(magnitude, chronoUnit);

        TimestampString tsStr = new TimestampString(
            result.getYear(),
            result.getMonthValue(),
            result.getDayOfMonth(),
            result.getHour(),
            result.getMinute(),
            result.getSecond()
        );
        if (result.getNano() > 0) {
            tsStr = tsStr.withNanos(result.getNano());
        }

        RexBuilder rexBuilder = cluster.getRexBuilder();
        // Use precision 3 (millisecond) to mirror TimestampFunctionAdapter's
        // resolvePrecision default — wide enough to keep distant-future values
        // within i64 nanosecond range.
        int precision = 3;
        RexNode literal = rexBuilder.makeTimestampLiteral(tsStr, precision);
        if (!literal.getType().equals(original.getType())) {
            literal = rexBuilder.makeAbstractCast(original.getType(), literal);
        }
        return literal;
    }

    /** Parse a {@code yyyy-MM-dd HH:mm:ss[.frac]} or ISO timestamp; returns null on failure. */
    static LocalDateTime parseTimestampLiteral(String input) {
        try {
            return LocalDateTime.parse(input, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT));
        } catch (DateTimeParseException ignored) {}
        try {
            return LocalDateTime.parse(input);
        } catch (DateTimeParseException ignored) {}
        if (input.contains(" ") && !input.contains("T")) {
            try {
                return LocalDateTime.parse(input.replace(' ', 'T'));
            } catch (DateTimeParseException ignored) {}
        }
        // Bare-date fallback: 'yyyy-MM-dd' → start of day.
        try {
            LocalDate d = LocalDate.parse(input);
            return d.atStartOfDay();
        } catch (DateTimeParseException ignored) {}
        return null;
    }

    private static RexNode unwrapAnnotation(RexNode node) {
        while (node instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            node = annotation.unwrap();
        }
        return node;
    }

    private static String stringLiteralValue(RexNode node) {
        if (!(node instanceof RexLiteral lit)) return null;
        SqlTypeName t = lit.getType().getSqlTypeName();
        if (t != SqlTypeName.VARCHAR && t != SqlTypeName.CHAR) return null;
        return lit.getValueAs(String.class);
    }

    private static Long integerLiteralValue(RexNode node) {
        if (!(node instanceof RexLiteral lit)) return null;
        Object v = lit.getValue();
        if (v instanceof BigDecimal bd) {
            try {
                return bd.longValueExact();
            } catch (ArithmeticException unused) {
                return null;
            }
        }
        if (v instanceof Number n) {
            return n.longValue();
        }
        return null;
    }
}
