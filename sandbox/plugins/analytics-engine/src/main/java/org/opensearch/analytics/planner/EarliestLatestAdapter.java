/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Plan-time fold for the scalar PPL boolean predicates {@code earliest(expr, ts)} and
 * {@code latest(expr, ts)}.
 *
 * <p>Semantics (matches {@code EarliestFunction} / {@code LatestFunction} in the
 * SQL plugin):
 * <ul>
 *   <li>{@code earliest(expr, ts)} → {@code true} iff the candidate timestamp
 *       {@code ts} is at-or-after the instant produced by parsing {@code expr}
 *       relative to query-start time. Equivalent to {@code ts >= folded(expr)}.</li>
 *   <li>{@code latest(expr, ts)} → {@code true} iff {@code ts} is at-or-before
 *       {@code folded(expr)}. Equivalent to {@code ts <= folded(expr)}.</li>
 * </ul>
 *
 * <p>The first argument must be a string literal (relative-time DSL like
 * {@code -7d}, {@code +1mon}, {@code -1h@d}, or an absolute timestamp like
 * {@code 2023-01-05 12:00:00}). The PPL surface only accepts literals here,
 * so we fold at plan time — this keeps the comparison columnar (just a
 * {@code Timestamp >= TimestampLiteral} that DataFusion handles natively) and
 * avoids reimplementing the relative-time DSL parser in Rust.
 *
 * <p>The fold uses {@link System#currentTimeMillis()} as the query-start
 * reference. Fragment conversion runs once per query at the coordinator, so
 * every shard sees the same baseline — equivalent to how
 * {@code FunctionProperties.getQueryStartClock()} pins the time on the SQL
 * plugin path.
 *
 * <p>The relative-time DSL grammar is a subset of the one in the SQL plugin's
 * {@code DateTimeUtils.getRelativeZonedDateTime}: {@code <chunk>+} where each
 * chunk is either {@code @<unit>} (snap) or {@code [+-]<n><unit>} (offset).
 * Units accepted: {@code s/m/h/d/w/M/y/q} plus {@code w0}–{@code w7} for
 * day-of-week snapping. Aliases like {@code seconds}/{@code minutes}/{@code mon}
 * are normalized to the single-letter canonical forms.
 *
 * @opensearch.internal
 */
public final class EarliestLatestAdapter {

    private static final Logger LOGGER = LogManager.getLogger(EarliestLatestAdapter.class);

    private static final String EARLIEST_NAME = "EARLIEST";
    private static final String LATEST_NAME = "LATEST";

    private EarliestLatestAdapter() {}

    /**
     * Walk the RelNode tree and rewrite scalar {@code earliest}/{@code latest} calls
     * inside any {@link Filter} or {@link Project} into plain comparison expressions.
     *
     * <p>Returns the input unchanged if no rewrites apply, so the calling convertor
     * can chain this freely.
     *
     * <p>TODO: if any other function or operator needs a plan-phase "now" timestamp,
     * lift {@code nowMillis} out of here and onto a planner context so all folds
     * see the same instant for a given query.
     */
    public static RelNode foldRelativeTimePredicates(RelNode root) {
        long nowMillis = System.currentTimeMillis();
        FoldShuttle shuttle = new FoldShuttle(nowMillis);
        return root.accept(shuttle);
    }

    // ── RelNode traversal ───────────────────────────────────────────────────────

    /**
     * Visits every node, applying the {@link RexFold} to filter/project conditions.
     * Only Filter and Project carry RexNodes that could contain {@code earliest}/
     * {@code latest} — Aggregate's argList holds field indices, not RexCalls.
     */
    private static final class FoldShuttle extends RelShuttleImpl {

        private final long nowMillis;

        FoldShuttle(long nowMillis) {
            this.nowMillis = nowMillis;
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            RelNode visited = super.visit(filter);
            if (!(visited instanceof Filter f)) {
                return visited;
            }
            RexFold fold = new RexFold(f.getCluster().getRexBuilder(), nowMillis);
            RexNode rewritten = f.getCondition().accept(fold);
            if (rewritten == f.getCondition()) {
                return f;
            }
            return f.copy(f.getTraitSet(), f.getInput(), rewritten);
        }

        @Override
        public RelNode visit(LogicalProject project) {
            RelNode visited = super.visit(project);
            if (!(visited instanceof Project p)) {
                return visited;
            }
            RexFold fold = new RexFold(p.getCluster().getRexBuilder(), nowMillis);
            List<RexNode> projects = p.getProjects();
            List<RexNode> rewritten = new ArrayList<>(projects.size());
            boolean changed = false;
            for (RexNode projExpr : projects) {
                RexNode r = projExpr.accept(fold);
                changed |= (r != projExpr);
                rewritten.add(r);
            }
            if (!changed) {
                return p;
            }
            return p.copy(p.getTraitSet(), p.getInput(), rewritten, p.getRowType());
        }
    }

    // ── RexNode rewrite ─────────────────────────────────────────────────────────

    /**
     * Replaces {@code earliest(expr, ts)} with {@code ts >= TIMESTAMP_LITERAL} and
     * {@code latest(expr, ts)} with {@code ts <= TIMESTAMP_LITERAL}. Recognition is
     * by operator name to avoid a hard compile-time dep on the SQL plugin (the UDF
     * is loaded by a separate plugin classloader at runtime).
     */
    private static final class RexFold extends RexShuttle {

        private final RexBuilder rexBuilder;
        private final long nowMillis;

        RexFold(RexBuilder rexBuilder, long nowMillis) {
            this.rexBuilder = rexBuilder;
            this.nowMillis = nowMillis;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            // Recurse first so inner calls fold before we look at the outer one.
            RexCall recursed = (RexCall) super.visitCall(call);

            String opName = recursed.getOperator().getName();
            boolean isEarliest = EARLIEST_NAME.equalsIgnoreCase(opName);
            boolean isLatest = LATEST_NAME.equalsIgnoreCase(opName);
            if (!isEarliest && !isLatest) {
                return recursed;
            }
            if (recursed.getOperands().size() != 2) {
                return recursed;
            }

            RexNode exprArg = recursed.getOperands().get(0);
            RexNode tsArg = recursed.getOperands().get(1);
            String exprLiteral = stringLiteralValue(exprArg);
            if (exprLiteral == null) {
                // Non-literal expr — leaving unchanged would still throw at substrait
                // emit (no UDF mapping registered). This is reachable only if the PPL
                // frontend is extended to allow non-literal first args; surface it
                // clearly so we don't silently produce wrong results.
                LOGGER.warn(
                    "Cannot fold scalar {}: first argument must be a string literal, got {}",
                    opName,
                    exprArg
                );
                return recursed;
            }

            long instantMillis;
            try {
                instantMillis = parseExpression(exprLiteral, nowMillis);
            } catch (IllegalArgumentException e) {
                LOGGER.warn(
                    "Cannot fold scalar {}: failed to parse relative-time expression [{}]: {}",
                    opName,
                    exprLiteral,
                    e.getMessage()
                );
                return recursed;
            }

            RelDataType timestampType = rexBuilder
                .getTypeFactory()
                .createTypeWithNullability(
                    rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP),
                    false
                );
            RexLiteral literal = (RexLiteral) rexBuilder.makeTimestampLiteral(
                org.apache.calcite.util.TimestampString.fromMillisSinceEpoch(instantMillis),
                3
            );
            // Calcite's makeTimestampLiteral is non-nullable already, but keep the
            // explicit type so the comparison's type inference matches the column's
            // (timestamp columns from PPL are typically nullable).
            RexNode tsLiteral = rexBuilder.ensureType(timestampType, literal, false);

            return (RexNode) rexBuilder.makeCall(
                isEarliest ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                tsArg,
                tsLiteral
            );
        }
    }

    private static String stringLiteralValue(RexNode node) {
        if (!(node instanceof RexLiteral lit)) {
            return null;
        }
        Object v = lit.getValue2();
        return v == null ? null : v.toString();
    }

    // ── Relative-time parser (port of DateTimeUtils.getRelativeZonedDateTime) ───

    /**
     * Parses a PPL relative-time expression against {@code nowMillis} (epoch millis,
     * UTC) and returns the resolved instant in epoch millis.
     *
     * <p>Accepts:
     * <ul>
     *   <li>{@code now} / {@code now()} → returns {@code nowMillis} unchanged.</li>
     *   <li>An absolute timestamp literal (one of the SQL plugin's accepted formats —
     *       see {@link #tryParseAbsolute}).</li>
     *   <li>One or more chunks of the form {@code @<unit>} (snap) or
     *       {@code [+-]<n><unit>} (offset). {@code <n>} defaults to 1 if omitted.</li>
     * </ul>
     */
    static long parseExpression(String input, long nowMillis) {
        if (input == null) {
            throw new IllegalArgumentException("relative-time expression cannot be null");
        }
        String trimmed = input.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("relative-time expression cannot be empty");
        }

        if ("now".equalsIgnoreCase(trimmed) || "now()".equalsIgnoreCase(trimmed)) {
            return nowMillis;
        }

        Long absolute = tryParseAbsolute(trimmed);
        if (absolute != null) {
            return absolute;
        }

        ZonedDateTime base = ZonedDateTime.ofInstant(Instant.ofEpochMilli(nowMillis), ZoneOffset.UTC);
        ZonedDateTime result = base;
        int i = 0;
        while (i < trimmed.length()) {
            char c = trimmed.charAt(i);
            if (c == '@') {
                int j = i + 1;
                while (j < trimmed.length() && Character.isLetterOrDigit(trimmed.charAt(j))) {
                    j++;
                }
                String rawUnit = trimmed.substring(i + 1, j);
                result = applySnap(result, rawUnit);
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
                result = applyOffset(result, c, value, rawUnit);
                i = k;
            } else {
                throw new IllegalArgumentException(
                    "Unexpected character '" + c + "' at position " + i + " in input: " + trimmed
                );
            }
        }
        return result.toInstant().toEpochMilli();
    }

    private static ZonedDateTime applyOffset(ZonedDateTime base, char sign, int value, String rawUnit) {
        String unit = normalizeUnit(rawUnit);
        if ("q".equals(unit)) {
            int months = value * 3;
            return sign == '-' ? base.minusMonths(months) : base.plusMonths(months);
        }
        ChronoUnit chronoUnit = mapChronoUnit(unit, "Unsupported offset unit: " + rawUnit);
        return sign == '-' ? base.minus(value, chronoUnit) : base.plus(value, chronoUnit);
    }

    private static ZonedDateTime applySnap(ZonedDateTime base, String rawUnit) {
        String unit = normalizeUnit(rawUnit);
        return switch (unit) {
            case "s" -> base.truncatedTo(ChronoUnit.SECONDS);
            case "m" -> base.truncatedTo(ChronoUnit.MINUTES);
            case "h" -> base.truncatedTo(ChronoUnit.HOURS);
            case "d" -> base.truncatedTo(ChronoUnit.DAYS);
            case "w" -> base.minusDays((base.getDayOfWeek().getValue() % 7)).truncatedTo(ChronoUnit.DAYS);
            case "M" -> base.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
            case "y" -> base.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS);
            case "q" -> {
                int month = base.getMonthValue();
                int quarterStart = ((month - 1) / 3) * 3 + 1;
                yield base.withMonth(quarterStart).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
            }
            default -> {
                if (unit.matches("w[0-7]")) {
                    int targetDay = unit.equals("w0") || unit.equals("w7") ? 7 : Integer.parseInt(unit.substring(1));
                    int diff = (base.getDayOfWeek().getValue() - targetDay + 7) % 7;
                    yield base.minusDays(diff).truncatedTo(ChronoUnit.DAYS);
                }
                throw new IllegalArgumentException("Unsupported snap unit: " + rawUnit);
            }
        };
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

    private static ChronoUnit mapChronoUnit(String unit, String error) {
        return switch (unit) {
            case "s" -> ChronoUnit.SECONDS;
            case "m" -> ChronoUnit.MINUTES;
            case "h" -> ChronoUnit.HOURS;
            case "d" -> ChronoUnit.DAYS;
            case "w" -> ChronoUnit.WEEKS;
            case "M" -> ChronoUnit.MONTHS;
            case "y" -> ChronoUnit.YEARS;
            default -> throw new IllegalArgumentException(error);
        };
    }

    // ── Absolute-timestamp parser ───────────────────────────────────────────────

    /**
     * Recognises a small set of common absolute-time literals — same shapes the SQL
     * plugin's {@code DateTimeUtils.tryParseAbsoluteTime} accepts (best-effort
     * subset; expand as needed when ITs surface a missing format). Returns the
     * instant in epoch millis (UTC), or {@code null} if {@code s} isn't an
     * absolute-time literal.
     */
    private static Long tryParseAbsolute(String s) {
        // ISO instant: 2023-01-05T12:00:00Z, 2023-01-05T12:00:00.123Z
        try {
            return Instant.parse(s).toEpochMilli();
        } catch (DateTimeParseException ignored) {}

        // Local datetime with optional fractional seconds, separator T or space.
        DateTimeFormatter localDt = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendPattern("['T'][' ']HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .toFormatter();
        try {
            LocalDateTime ldt = LocalDateTime.parse(s, localDt);
            return ldt.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (DateTimeParseException ignored) {}

        // Splunk-style: MM/dd/yyyy:HH:mm:ss
        DateTimeFormatter splunk = DateTimeFormatter.ofPattern("MM/dd/yyyy:HH:mm:ss", Locale.ROOT);
        try {
            LocalDateTime ldt = LocalDateTime.parse(s, splunk);
            return ldt.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (DateTimeParseException ignored) {}

        // Date-only: yyyy-MM-dd
        try {
            LocalDate ld = LocalDate.parse(s);
            return ld.atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();
        } catch (DateTimeParseException ignored) {}

        return null;
    }
}
