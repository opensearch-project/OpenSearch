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
 * PPL {@code TIMESTAMPDIFF(out_unit, t1, t2)} rewrites — neither {@code TIMESTAMPDIFF} nor
 * {@code TIMESTAMPADD} have substrait bindings, so isthmus rejects them unless rewritten.
 *
 * <p>Three shapes:
 * <ul>
 *   <li>Peephole {@code TIMESTAMPDIFF(out, t, TIMESTAMPADD(in, n, t))}: fixed/fixed unit pair
 *       constant-folds to a BIGINT literal; variable-length {@code in_unit} (MONTH/QUARTER/YEAR)
 *       with fixed {@code out_unit} → {@code (to_unixtime(t + INTERVAL n*m MONTH) - to_unixtime(t)) * factor}.</li>
 *   <li>Standalone: {@code (to_unixtime(t2) - to_unixtime(t1))} scaled by the out-unit factor;
 *       variable-length out units use 30/90/365-day approximations matching legacy SQL plugin behavior.</li>
 * </ul>
 *
 * <p>Out-unit MONTH/QUARTER/YEAR for the peephole shape is rejected (lossy fixed-second math) — follow-up.
 *
 * @opensearch.internal
 */
class TimestampDiffAdapter implements ScalarFunctionAdapter {

    /** Fixed-length PPL unit → ms (MICROSECOND=0 means sub-ms; handled separately). */
    private static final Map<String, Long> UNIT_TO_MILLIS = Map.ofEntries(
        Map.entry("MICROSECOND", 0L),
        Map.entry("MILLISECOND", 1L),
        Map.entry("SECOND", 1_000L),
        Map.entry("MINUTE", 60_000L),
        Map.entry("HOUR", 3_600_000L),
        Map.entry("DAY", 86_400_000L),
        Map.entry("WEEK", 604_800_000L)
    );

    /** Variable inner unit → months for the {@code INTERVAL n*m MONTH} literal. */
    private static final Map<String, Long> VARIABLE_INNER_MONTHS = Map.of("MONTH", 1L, "QUARTER", 3L, "YEAR", 12L);

    /** Fixed out-unit → multiplier on unix-seconds-diff (sub-minute units). */
    private static final Map<String, Long> OUT_UNIT_MULTIPLIER_FROM_SECONDS = Map.of(
        "MICROSECOND",
        1_000_000L,
        "MILLISECOND",
        1_000L,
        "SECOND",
        1L
    );

    /** Fixed out-unit → divisor on unix-seconds-diff. */
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

    /** Variable out unit → seconds approximation (legacy SQL plugin parity). */
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

    /** Standalone TIMESTAMPDIFF: {@code (to_unixtime(t2) - to_unixtime(t1))} scaled to out-unit. */
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
            // variable-length out unit — MONTH≈30d, QUARTER≈90d, YEAR≈365d
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
            // out MONTH/QUARTER/YEAR — variable on both sides; pass through, isthmus surfaces the failure
            return null;
        }

        // INTERVAL (inValue * innerMonths) MONTH, mirroring EarliestLatestAdapter#makeIntervalAdd
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
        RexNode endSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, addedTs);
        RexNode startSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, startArg);
        RexNode diffSeconds = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, endSeconds, startSeconds);

        // scale to out-unit; ×1000 for MILLISECOND (timechart per_*), ÷ for coarser units
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

    /** Fold {@code n * in_ms / out_ms} for fixed/fixed unit pairs; null if non-integral or variable-length. */
    private static Long constantFold(String outUnit, String inUnit, long inValue) {
        Long inMs = UNIT_TO_MILLIS.get(inUnit.toUpperCase(Locale.ROOT));
        Long outMs = UNIT_TO_MILLIS.get(outUnit.toUpperCase(Locale.ROOT));
        if (inMs == null || outMs == null || outMs == 0L) {
            return null;
        }
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
