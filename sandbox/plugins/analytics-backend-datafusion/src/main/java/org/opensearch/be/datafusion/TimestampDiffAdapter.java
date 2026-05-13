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
 * <p>Variable-length units (MONTH, QUARTER, YEAR) are intentionally not folded — the
 * milliseconds-per-month value depends on which month the base timestamp lands in. Calls
 * with those units fall through to the original PPL UDF and surface as a substrait conversion
 * error, which is the same behavior as standalone {@code TIMESTAMPADD}. Full interval-aware
 * support requires Substrait {@code add(timestamp, interval) -> timestamp} wiring with
 * a {@code SqlIntervalQualifier}-built RexLiteral, deferred to a follow-up.
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

        // The peephole only fires when end is TIMESTAMPADD(in_unit_literal, n_int_literal, start).
        // `start` here must be the *same* RexNode reference as the outer TIMESTAMPDIFF's start
        // (typically a RexInputRef into @timestamp). RexInputRef.equals compares by ordinal,
        // so structurally-equal refs to the same input position match. OperatorAnnotation
        // wrappers (e.g. AnnotatedProjectExpression introduced by OpenSearchProjectRule) are
        // peeled at each operand before structural comparison so the wrapped TIMESTAMPADD
        // call remains recognizable as a TIMESTAMPADD instead of looking like an annotation
        // RexCall whose operator is ANNOTATED_PROJECT_EXPR.
        if (!(endArg instanceof RexCall endCall)
            || !endCall.getOperator().getName().equalsIgnoreCase("TIMESTAMPADD")
            || endCall.getOperands().size() != 3) {
            return original;
        }
        String inUnit = stringLiteralValue(unwrapAnnotation(endCall.getOperands().get(0)));
        Long inValue = integerLiteralValue(unwrapAnnotation(endCall.getOperands().get(1)));
        RexNode addedBase = unwrapAnnotation(endCall.getOperands().get(2));
        if (inUnit == null || inValue == null || !addedBase.equals(startArg)) {
            return original;
        }

        Long foldedDiff = constantFold(outUnit, inUnit, inValue);
        if (foldedDiff == null) {
            return original;
        }

        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode literal = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(foldedDiff));
        // Pin the literal back to the original call's declared return type so the surrounding
        // Project's typeMatchesInferred check doesn't see a NOT NULL vs FORCE_NULLABLE mismatch.
        return rexBuilder.makeCast(original.getType(), literal, true);
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
        if (lit.getType().getSqlTypeName() != SqlTypeName.CHAR
            && lit.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
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
