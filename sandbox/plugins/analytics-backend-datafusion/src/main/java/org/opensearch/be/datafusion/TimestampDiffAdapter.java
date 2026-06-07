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
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Peephole-folds PPL {@code TIMESTAMPDIFF(out_unit, t, TIMESTAMPADD(in_unit, n, t))} when both
 * unit strings are fixed-length (MICROSECOND through WEEK). This is the exact expression
 * shape PPL timechart's {@code per_second / per_minute / per_hour / per_day} aggregations
 * produce — the result is the same constant for every row, so we materialize the BIGINT
 * literal at adapter time and let the literal flow through Substrait unchanged.
 *
 * <p>Two PPL UDFs are involved:
 * <ul>
 *   <li>{@code TIMESTAMPDIFF(unit, t1, t2)} returns LONG number of {@code unit}s between
 *       {@code t1} and {@code t2}.</li>
 *   <li>{@code TIMESTAMPADD(unit, n, t)} returns TIMESTAMP shifted by {@code n} {@code unit}s.</li>
 * </ul>
 * Neither has a Substrait extension binding, so isthmus rejects them as
 * "Unable to convert call TIMESTAMPADD(string, i32, precision_timestamp&lt;0&gt;?)" unless we
 * rewrite the call. Folding the whole {@code TIMESTAMPDIFF(..., TIMESTAMPADD(...))} to a
 * literal removes both UDF references in one step.
 *
 * <p>Variable-length units (MONTH, QUARTER, YEAR) cannot be constant-folded — the
 * milliseconds-per-month value depends on which calendar month the base timestamp lands
 * in (Feb 2025 = 28 days, Oct 2025 = 31 days). For the peephole shape with a
 * variable-length {@code in_unit} and a fixed-length {@code out_unit}, the adapter
 * rewrites atomically to a runtime-evaluated form:
 * <pre>
 *   TIMESTAMPDIFF(out_unit, t, TIMESTAMPADD(MONTH|QUARTER|YEAR, n, t))
 *     →  (to_unixtime(DATETIME_PLUS(t, INTERVAL n*m MONTH)) - to_unixtime(t)) * out_factor
 * </pre>
 * where {@code m} converts QUARTER→3 / YEAR→12 (same idiom as
 * {@code EarliestLatestAdapter#makeIntervalAdd}, which has prior wiring proof that
 * {@code DATETIME_PLUS(t, INTERVAL_MONTH)} binds end-to-end via Substrait's standard
 * {@code add(timestamp, interval_year_month)} into DataFusion's native interval add).
 * For out-unit factors see {@link #OUT_UNIT_MULTIPLIER_FROM_SECONDS} /
 * {@link #OUT_UNIT_DIVISOR_FROM_SECONDS}.
 *
 * <p>Out-unit MONTH/QUARTER/YEAR (computing the month-difference of two timestamps in
 * variable units) is still rejected — the lossy fixed-second math doesn't apply.
 * No timechart per_* shape needs this; left as a follow-up.
 *
 * @opensearch.internal
 */
class TimestampDiffAdapter implements ScalarFunctionAdapter {

    /** PPL IntervalUnit name → milliseconds (only fixed-length units; null means variable-length). */
    private static final Map<String, Long> UNIT_TO_MILLIS = Map.ofEntries(
        Map.entry("MICROSECOND", 0L),  // sub-millisecond; treated separately below
        Map.entry("MILLISECOND", 1L),
        Map.entry("SECOND", 1_000L),
        Map.entry("MINUTE", 60_000L),
        Map.entry("HOUR", 3_600_000L),
        Map.entry("DAY", 86_400_000L),
        Map.entry("WEEK", 604_800_000L)
    );

    /**
     * Variable-length PPL inner unit → number of months it represents. Used by the runtime
     * rewrite path to build the {@code INTERVAL n*<m> MONTH} literal that
     * {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#DATETIME_PLUS} adds to the base
     * timestamp. Substrait's {@code interval_year_month} carries months as its underlying
     * unit, so a 'MONTH' inner is m=1, 'QUARTER' is m=3, 'YEAR' is m=12.
     */
    private static final Map<String, Long> VARIABLE_INNER_MONTHS = Map.of("MONTH", 1L, "QUARTER", 3L, "YEAR", 12L);

    /**
     * Fixed-length out-unit → multiplier applied to {@code (unix_seconds_diff)} to express
     * the difference in that unit. Only sub-minute units are multipliers; minute and larger
     * use {@link #OUT_UNIT_DIVISOR_FROM_SECONDS} instead.
     */
    private static final Map<String, Long> OUT_UNIT_MULTIPLIER_FROM_SECONDS = Map.of(
        "MICROSECOND",
        1_000_000L,
        "MILLISECOND",
        1_000L,
        "SECOND",
        1L
    );

    /** Fixed-length out-unit → divisor applied to {@code (unix_seconds_diff)}. */
    private static final Map<String, Long> OUT_UNIT_DIVISOR_FROM_SECONDS = Map.of(
        "MINUTE",
        60L,
        "HOUR",
        3_600L,
        "DAY",
        86_400L,
        "WEEK",
        604_800L
    );

    /** Variable-length out unit → seconds approximation for standalone TIMESTAMPDIFF. */
    private static final Map<String, Long> VARIABLE_OUT_APPROX_SECONDS = Map.of(
        "MONTH",
        30L * 86_400L,
        "QUARTER",
        90L * 86_400L,
        "YEAR",
        365L * 86_400L
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (!original.getOperator().getName().equalsIgnoreCase("TIMESTAMPDIFF")) {
            return original;
        }
        if (original.getOperands().size() != 3) {
            return original;
        }
        RexNode outUnitArg = unwrapAnnotation(original.getOperands().get(0));
        RexNode startArg = unwrapAnnotation(original.getOperands().get(1));
        RexNode endArg = unwrapAnnotation(original.getOperands().get(2));

        String outUnit = stringLiteralValue(outUnitArg);
        if (outUnit == null) {
            return original;
        }

        // Peephole: TIMESTAMPDIFF(out, t, TIMESTAMPADD(in, n, t)) — fold or rewrite via runtime path.
        if (endArg instanceof RexCall endCall
            && endCall.getOperator().getName().equalsIgnoreCase("TIMESTAMPADD")
            && endCall.getOperands().size() == 3) {
            String inUnit = stringLiteralValue(unwrapAnnotation(endCall.getOperands().get(0)));
            Long inValue = integerLiteralValue(unwrapAnnotation(endCall.getOperands().get(1)));
            RexNode addedBase = unwrapAnnotation(endCall.getOperands().get(2));
            if (inUnit != null && inValue != null && addedBase.equals(startArg)) {
                RexBuilder rb = cluster.getRexBuilder();
                Long foldedDiff = constantFold(outUnit, inUnit, inValue);
                if (foldedDiff != null) {
                    RexNode literal = rb.makeBigintLiteral(BigDecimal.valueOf(foldedDiff));
                    return rb.makeCast(original.getType(), literal, true);
                }
                Long innerMonths = VARIABLE_INNER_MONTHS.get(inUnit.toUpperCase(Locale.ROOT));
                if (innerMonths != null) {
                    RexNode rewritten = rewriteVariableInner(rb, startArg, inValue, innerMonths, outUnit, original.getType());
                    if (rewritten != null) {
                        return rewritten;
                    }
                }
            }
        }

        // Standalone TIMESTAMPDIFF(out_unit, t1, t2): rewrite via to_unixtime delta + scale.
        return rewriteStandaloneDiff(cluster, original, outUnit, startArg, endArg);
    }

    /**
     * Standalone TIMESTAMPDIFF(out_unit, t1, t2): cast both args to TIMESTAMP, take the second-delta
     * via to_unixtime, then scale. For variable-length out units (MONTH/QUARTER/YEAR), use a
     * 30-day / 90-day / 365-day approximation matching legacy SQL plugin behavior.
     */
    private static RexNode rewriteStandaloneDiff(RelOptCluster cluster, RexCall original, String outUnit, RexNode start, RexNode end) {
        RexBuilder rb = cluster.getRexBuilder();
        RexNode t1 = liftToTimestamp(rb, start, original.getType());
        RexNode t2 = liftToTimestamp(rb, end, original.getType());
        RexNode endSeconds = rb.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, t2);
        RexNode startSeconds = rb.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, t1);
        RexNode diffSeconds = rb.makeCall(SqlStdOperatorTable.MINUS, endSeconds, startSeconds);
        String upper = outUnit.toUpperCase(Locale.ROOT);
        Long mult = OUT_UNIT_MULTIPLIER_FROM_SECONDS.get(upper);
        Long div = mult == null ? OUT_UNIT_DIVISOR_FROM_SECONDS.get(upper) : null;
        Long approxSeconds = mult == null && div == null ? VARIABLE_OUT_APPROX_SECONDS.get(upper) : null;
        RexNode scaled;
        if (mult != null && mult > 1L) {
            RexNode multLit = rb.makeBigintLiteral(BigDecimal.valueOf(mult));
            scaled = rb.makeCall(SqlStdOperatorTable.MULTIPLY, diffSeconds, multLit);
        } else if (div != null && div > 1L) {
            RexNode divLit = rb.makeBigintLiteral(BigDecimal.valueOf(div));
            scaled = rb.makeCall(SqlStdOperatorTable.DIVIDE, diffSeconds, divLit);
        } else if (approxSeconds != null) {
            // Variable-length out unit — approximate (MONTH≈30d, QUARTER≈90d, YEAR≈365d).
            RexNode divLit = rb.makeBigintLiteral(BigDecimal.valueOf(approxSeconds));
            scaled = rb.makeCall(SqlStdOperatorTable.DIVIDE, diffSeconds, divLit);
        } else {
            scaled = diffSeconds;
        }
        return rb.makeCast(original.getType(), scaled, true);
    }

    /** Cast a string/date/timestamp expression to TIMESTAMP matching {@code resultType}'s nullability. */
    private static RexNode liftToTimestamp(RexBuilder rb, RexNode operand, org.apache.calcite.rel.type.RelDataType resultType) {
        if (operand.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP) {
            return operand;
        }
        org.apache.calcite.rel.type.RelDataType tsType = rb.getTypeFactory()
            .createTypeWithNullability(
                rb.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP),
                operand.getType().isNullable() || resultType.isNullable()
            );
        return rb.makeCast(tsType, operand, true);
    }

    private static RexNode rewriteVariableInner(
        RexBuilder rexBuilder,
        RexNode startArg,
        long inValue,
        long innerMonths,
        String outUnit,
        org.apache.calcite.rel.type.RelDataType resultType
    ) {
        String upperOut = outUnit.toUpperCase(Locale.ROOT);
        Long outMultiplier = OUT_UNIT_MULTIPLIER_FROM_SECONDS.get(upperOut);
        Long outDivisor = outMultiplier == null ? OUT_UNIT_DIVISOR_FROM_SECONDS.get(upperOut) : null;
        if (outMultiplier == null && outDivisor == null) {
            // Out-unit MONTH/QUARTER/YEAR — variable on both sides; fixed-second math
            // doesn't apply. Leave the call unchanged; isthmus will surface the failure.
            return null;
        }

        // Build addedTs = DATETIME_PLUS(startArg, INTERVAL (inValue * innerMonths) MONTH)
        // Mirrors EarliestLatestAdapter.makeIntervalAdd — INTERVAL_YEAR_MONTH backing unit.
        long totalMonths;
        try {
            totalMonths = Math.multiplyExact(inValue, innerMonths);
        } catch (ArithmeticException unused) {
            return null;
        }
        SqlIntervalQualifier monthQualifier = new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO);
        RexNode intervalLit = rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(totalMonths), monthQualifier);
        RexNode addedTs = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, startArg, intervalLit);

        // unix_seconds_diff = to_unixtime(addedTs) - to_unixtime(startArg)
        // Use the locally-declared substrait-mapped operator (same one UnixTimestampAdapter
        // and SpanAdapter route through to DataFusion's native to_unixtime UDF).
        RexNode endSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, addedTs);
        RexNode startSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, startArg);
        RexNode diffSeconds = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, endSeconds, startSeconds);

        // Scale to the requested out-unit. PPL timechart's per_function uses MILLISECOND as
        // out-unit so the multiplier path (×1000) is the hot case; the divisor path (e.g.
        // MINUTE→÷60) covers consumer code shapes that compute coarser durations.
        RexNode scaled;
        if (outMultiplier != null && outMultiplier > 1L) {
            RexNode multLit = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(outMultiplier));
            scaled = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, diffSeconds, multLit);
        } else if (outDivisor != null && outDivisor > 1L) {
            RexNode divLit = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(outDivisor));
            scaled = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, diffSeconds, divLit);
        } else {
            scaled = diffSeconds;  // out-unit SECOND
        }
        return rexBuilder.makeCast(resultType, scaled, true);
    }

    /**
     * Fold {@code n * in_ms / out_ms} when both units are fixed-length. Returns null when
     * either unit is variable-length (MONTH / QUARTER / YEAR) or when the result is not an
     * exact integer (e.g. {@code TIMESTAMPDIFF('SECOND', t, t + 500 MILLISECOND)} = 0.5).
     */
    private static Long constantFold(String outUnit, String inUnit, long inValue) {
        Long inMs = UNIT_TO_MILLIS.get(inUnit.toUpperCase(Locale.ROOT));
        Long outMs = UNIT_TO_MILLIS.get(outUnit.toUpperCase(Locale.ROOT));
        if (inMs == null || outMs == null || outMs == 0L) {
            return null;
        }
        // PPL's IntervalUnit treats MILLISECOND as the base; PPL TIMESTAMPDIFF computes
        // (t2 - t1) in milliseconds and divides by out_unit's millisecond factor. The
        // formula in_value * in_ms / out_ms reproduces that for fixed-length unit pairs.
        long totalMs = Math.multiplyExact(inValue, inMs);
        if (totalMs % outMs != 0) {
            return null;
        }
        return totalMs / outMs;
    }

    /** Peel a single OperatorAnnotation wrapper if present. */
    private static RexNode unwrapAnnotation(RexNode node) {
        if (node instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            return annotation.unwrap();
        }
        return node;
    }

    private static String stringLiteralValue(RexNode node) {
        if (!(node instanceof RexLiteral lit)) {
            return null;
        }
        if (lit.getType().getSqlTypeName() != SqlTypeName.CHAR && lit.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            return null;
        }
        return lit.getValueAs(String.class);
    }

    private static Long integerLiteralValue(RexNode node) {
        if (!(node instanceof RexLiteral lit)) {
            return null;
        }
        Object value = lit.getValue();
        if (value instanceof BigDecimal bd) {
            try {
                return bd.longValueExact();
            } catch (ArithmeticException unused) {
                return null;
            }
        }
        if (value instanceof Number n) {
            return n.longValue();
        }
        return null;
    }
}
