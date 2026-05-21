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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * Routes PPL {@code TIMESTAMP(...)} calls to the right backend path.
 *
 * <table>
 *   <caption>TIMESTAMP shape coverage</caption>
 *   <tr><th>Shape</th><th>Example</th><th>Path</th><th>Status</th></tr>
 *   <tr><td>A</td>
 *       <td>{@code TIMESTAMP('2020-01-01 00:00:00')}</td>
 *       <td>plan-time fold (varchar literal)</td>
 *       <td>works</td></tr>
 *   <tr><td>B</td>
 *       <td>{@code TIMESTAMP(DATE('2020-08-26'))}</td>
 *       <td>plan-time fold (DATE-of-literal): combine date with 00:00:00 UTC,
 *           matches {@code ExprDateValue.timestampValue() = date.atStartOfDay(UTC)}</td>
 *       <td>works</td></tr>
 *   <tr><td>C</td>
 *       <td>{@code TIMESTAMP(TIME('10:20:30'))}</td>
 *       <td>plan-time fold (TIME-of-literal): combine time with today's UTC date,
 *           matches {@code ExprTimeValue.timestampValue} </td>
 *       <td>works</td></tr>
 *   <tr><td>D</td>
 *       <td>{@code TIMESTAMP(TIMESTAMP('lit'))}</td>
 *       <td>inner Shape A fold collapses the call into a typed TIMESTAMP literal;
 *           the outer adapter sees a TIMESTAMP-typed operand, falls through to
 *           {@link DateTimeAdapters.DatetimeAdapter} which renames to DataFusion's
 *           builtin {@code to_timestamp(timestamp)} (identity at runtime)</td>
 *       <td>works</td></tr>
 *   <tr><td>E</td>
 *       <td>{@code TIMESTAMP('2020-01-01 10:00:00', '01:30:00')}</td>
 *       <td>plan-time fold (2-arg literal-literal): parse first as timestamp,
 *           second as time-of-day, add. Matches
 *           {@code TimestampFunction.timestamp(props, dt, addTime) = exprAddTime}</td>
 *       <td>works</td></tr>
 *   <tr><td>F</td>
 *       <td>{@code TIMESTAMP(<column>)}</td>
 *       <td>operand is {@code RexInputRef}; fold doesn't catch. Routes to
 *           {@link DateTimeAdapters.DatetimeAdapter} → DataFusion builtin
 *           {@code to_timestamp}.</td>
 *       <td>works for DATE / TIMESTAMP / VARCHAR columns</td></tr>
 *   <tr><td>G</td>
 *       <td>{@code TIMESTAMP('<bad string>')}</td>
 *       <td>{@link #parseTimestamp} throws {@code IllegalArgumentException}
 *           with the raw input when no format matches (covers month-13,
 *           second-61, garbage). HTTP 400 with the bare input as the message.</td>
 *       <td>rejects with HTTP 400</td></tr>
 * </table>
 *
 * <p>The fold path bypasses runtime evaluation and avoids the
 * {@code to_timestamp(to_date(...))} chain that DataFusion can't execute on
 * this stack ({@code Unsupported data type Date32 for function to_timestamp}).
 *
 * <p>The chosen RexNode is wrapped in a CAST to the call's declared type so
 * the surrounding Project's rowType matches.
 *
 * @opensearch.internal
 */
class TimestampFunctionAdapter implements ScalarFunctionAdapter {

    private static final DateTimeAdapters.DatetimeAdapter DATETIME_ADAPTER = new DateTimeAdapters.DatetimeAdapter();

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexNode folded = tryFoldLiteral(original, fieldStorage, cluster);
        if (folded != null) {
            return wrapWithCallType(folded, original, cluster);
        }
        return wrapWithCallType(DATETIME_ADAPTER.adapt(original, fieldStorage, cluster), original, cluster);
    }

    /**
     * Recognize the four nested-with-literal shapes and fold them at plan time
     * into typed TIMESTAMP literals, matching legacy semantics from the SQL
     * plugin's {@code ExprTimestampValue} / {@code ExprDateValue} /
     * {@code ExprTimeValue} / {@code TimestampFunction}.
     *
     * <p>Returns {@code null} when the call shape isn't a foldable literal —
     * caller falls through to the DatetimeAdapter rename path.
     */
    private static RexNode tryFoldLiteral(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        // Pick precision from field storage: date_nanos → 9, otherwise default to 3
        // (millisecond). Using ms-precision for the common case keeps the folded
        // literal's i64 representation well within range — TIMESTAMP(9) at year
        // 3077 overflows i64 (max ~year 2262), TIMESTAMP(3) does not.
        int precision = resolvePrecision(fieldStorage);
        RexBuilder rexBuilder = cluster.getRexBuilder();

        if (original.getOperands().size() == 2) {
            return tryFoldTwoArg(original, precision, rexBuilder);
        }
        if (original.getOperands().size() != 1) {
            return null;
        }

        RexNode operand = stripOperatorAnnotation(original.getOperands().get(0));

        // Shape A: TIMESTAMP(<varchar literal>) — parse and fold.
        if (operand instanceof RexLiteral literal && literal.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
            String value = literal.getValueAs(String.class);
            if (value == null) {
                return null;
            }
            return rexBuilder.makeTimestampLiteral(parseTimestamp(value), precision);
        }

        // Shapes B and C: TIMESTAMP(DATE/TIME(<varchar literal>)). After bottom-up
        // adapter walk, the inner DATE/TIME has been renamed by DateAdapter /
        // TimeAdapter to LOCAL_DATE_OP / LOCAL_TIME_OP.
        if (operand instanceof RexCall innerCall
            && innerCall.getOperands().size() == 1
            && stripOperatorAnnotation(innerCall.getOperands().get(0)) instanceof RexLiteral innerLiteral
            && innerLiteral.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
            SqlOperator innerOp = innerCall.getOperator();
            String innerValue = innerLiteral.getValueAs(String.class);
            if (innerValue == null) {
                return null;
            }

            // Shape B: TIMESTAMP(DATE('lit')) — combine date with 00:00:00 UTC.
            if (innerOp == DateTimeAdapters.LOCAL_DATE_OP) {
                LocalDate date;
                try {
                    date = LocalDate.parse(innerValue);
                } catch (DateTimeParseException e) {
                    return null;
                }
                return rexBuilder.makeTimestampLiteral(toTimestampString(date.atStartOfDay()), precision);
            }

            // Shape C: TIMESTAMP(TIME('lit')) — combine time with today's UTC date.
            if (innerOp == DateTimeAdapters.LOCAL_TIME_OP) {
                LocalTime time;
                try {
                    time = LocalTime.parse(innerValue);
                } catch (DateTimeParseException e) {
                    return null;
                }
                LocalDate today = LocalDate.now(ZoneOffset.UTC);
                return rexBuilder.makeTimestampLiteral(toTimestampString(LocalDateTime.of(today, time)), precision);
            }
        }

        return null;
    }

    /**
     * Shape E: 2-arg literal-literal {@code TIMESTAMP('<datetime>', '<time>')}.
     * Parses the first arg as a timestamp, the second as a time-of-day,
     * adds the time onto the timestamp. Matches legacy
     * {@code TimestampFunction.timestamp(properties, datetime, addTime)}
     * which calls {@code exprAddTime}.
     */
    private static RexNode tryFoldTwoArg(RexCall original, int precision, RexBuilder rexBuilder) {
        RexNode op0 = stripOperatorAnnotation(original.getOperands().get(0));
        RexNode op1 = stripOperatorAnnotation(original.getOperands().get(1));
        if (!(op0 instanceof RexLiteral lit0) || lit0.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            return null;
        }
        if (!(op1 instanceof RexLiteral lit1) || lit1.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            return null;
        }
        String tsValue = lit0.getValueAs(String.class);
        String timeValue = lit1.getValueAs(String.class);
        if (tsValue == null || timeValue == null) {
            return null;
        }
        TimestampString tsStr = parseTimestamp(tsValue);
        LocalTime addTime;
        try {
            addTime = parseTimeOfDay(timeValue);
        } catch (DateTimeParseException e) {
            return null;
        }
        // Compose: take tsStr, advance by addTime's nano-of-day. Use LocalDateTime
        // for arithmetic, then re-render to TimestampString.
        LocalDateTime base;
        try {
            base = LocalDateTime.parse(tsStr.toString().replace(' ', 'T'));
        } catch (DateTimeParseException e) {
            return null;
        }
        LocalDateTime sum = base.plusNanos(addTime.toNanoOfDay());
        return rexBuilder.makeTimestampLiteral(toTimestampString(sum), precision);
    }

    private static LocalTime parseTimeOfDay(String input) {
        for (java.time.format.DateTimeFormatter formatter : TIME_OF_DAY_FORMATS) {
            try {
                return formatter == null ? LocalTime.parse(input) : LocalTime.parse(input, formatter);
            } catch (DateTimeParseException ignored) {}
        }
        throw new DateTimeParseException("Unsupported time format", input, 0);
    }

    /** {@code null} entry means "use {@link LocalTime#parse(CharSequence)}'s default ISO format".
     *  Locale-pinned formatters avoid the default-locale forbidden-api hit. */
    private static final java.time.format.DateTimeFormatter[] TIME_OF_DAY_FORMATS = new java.time.format.DateTimeFormatter[] {
        null,
        java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss", java.util.Locale.ROOT),
        java.time.format.DateTimeFormatter.ofPattern("HH:mm", java.util.Locale.ROOT) };

    /**
     * Pick a precision for fold output. {@code date_nanos} fields force 9
     * (nanosecond) precision so values can compare equal to native ts values;
     * everything else defaults to 3 (millisecond), which is wide enough for
     * year-of-i64-ns-overflow boundary cases (year 3077+).
     */
    private static int resolvePrecision(List<FieldStorageInfo> fieldStorage) {
        for (FieldStorageInfo field : fieldStorage) {
            if ("date_nanos".equals(field.getMappingType())) {
                return 9;
            }
        }
        return 3;
    }

    /**
     * Lift the chosen RexNode to the call's declared type so the surrounding
     * Project's rowType matches.
     */
    private static RexNode wrapWithCallType(RexNode chosen, RexCall original, RelOptCluster cluster) {
        if (chosen.getType().equals(original.getType())) {
            return chosen;
        }
        return cluster.getRexBuilder().makeAbstractCast(original.getType(), chosen);
    }

    private static RexNode stripOperatorAnnotation(RexNode node) {
        while (node instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            node = annotation.unwrap();
        }
        return node;
    }

    /**
     * Parse a varchar timestamp literal in the formats accepted by legacy
     * {@code ExprTimestampValue}: {@code yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]} and
     * ISO-8601. Mirrors the previous adapter's fall-through chain so cherry-picked
     * cases that used to work still work.
     */
    static TimestampString parseTimestamp(String input) {
        try {
            LocalDate date = LocalDate.parse(input);
            return toTimestampString(date.atStartOfDay());
        } catch (DateTimeParseException ignored) {}

        try {
            OffsetDateTime odt = OffsetDateTime.parse(input);
            return toTimestampString(LocalDateTime.ofInstant(odt.toInstant(), ZoneOffset.UTC));
        } catch (DateTimeParseException ignored) {}

        try {
            Instant instant = Instant.parse(input);
            return toTimestampString(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
        } catch (DateTimeParseException ignored) {}

        try {
            LocalDateTime ldt = LocalDateTime.parse(input);
            return toTimestampString(ldt);
        } catch (DateTimeParseException ignored) {}

        if (input.contains(" ") && !input.contains("T")) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(input.replace(' ', 'T'));
                return toTimestampString(ldt);
            } catch (DateTimeParseException ignored) {}
        }
        throw new IllegalArgumentException(input);
    }

    private static TimestampString toTimestampString(LocalDateTime ldt) {
        rejectIfOutsideI64NsRange(ldt);
        TimestampString ts = new TimestampString(
            ldt.getYear(),
            ldt.getMonthValue(),
            ldt.getDayOfMonth(),
            ldt.getHour(),
            ldt.getMinute(),
            ldt.getSecond()
        );
        int nanos = ldt.getNano();
        if (nanos > 0) {
            ts = ts.withNanos(nanos);
        }
        return ts;
    }

    /**
     * The analytics-engine route serializes TIMESTAMP values as i64 nanoseconds
     * since epoch (Substrait/Arrow). Values past the i64-ns ceiling overflow
     * silently in DataFusion's optimizer (e.g. {@code simplify_expressions}
     * widening a TIMESTAMP(3) to TIMESTAMP(9) via {@code * 1000}), surfacing as
     * an opaque {@code Arrow error: Arithmetic overflow} at execution. Reject
     * up front with a clear message so users see the limit at plan time.
     *
     * <p>Upper bound is the exact i64-ns ceiling — {@code Long.MAX_VALUE} ns is
     * {@code 2262-04-11 23:47:16.854775807 UTC}.
     */
    private static final LocalDateTime I64_NS_MAX = LocalDateTime.ofEpochSecond(
        Long.MAX_VALUE / 1_000_000_000L,
        (int) (Long.MAX_VALUE % 1_000_000_000L),
        ZoneOffset.UTC
    );

    private static void rejectIfOutsideI64NsRange(LocalDateTime ldt) {
        if (ldt.isAfter(I64_NS_MAX)) {
            throw new IllegalArgumentException("timestamp " + ldt + " is outside the supported range");
        }
    }
}
