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

/**
 * Rewrites PPL {@code DATE_ADD(base, INTERVAL n unit)} / {@code DATE_SUB(base, INTERVAL n unit)}
 * into {@code DATETIME_PLUS(CAST(base AS TIMESTAMP), interval)}.
 *
 * <p>The PPL {@code DATE_ADD} / {@code DATE_SUB} UDFs (named operators from
 * {@code DateAddSubFunction}) have no Substrait extension binding — isthmus rejects the raw call
 * with {@code "Unrecognized scalar function [DATE_ADD]"} / {@code "Unable to convert call
 * DATE_ADD(date?, interval_year)"}. {@code DATETIME_PLUS}, by contrast, lowers to Substrait's
 * standard {@code add(timestamp, interval)} which DataFusion executes natively — the same path
 * {@link EarliestLatestAdapter} and {@link TimestampDiffAdapter} already rely on for interval
 * arithmetic.
 *
 * <p><b>Interval value units.</b> PPL's {@code visitInterval} builds the literal with the
 * <em>leading-field value</em> (e.g. {@code 90} for {@code INTERVAL 90 day}). Substrait /
 * DataFusion, however, carry a day-time interval as <em>milliseconds</em> and a year-month interval
 * as <em>months</em>. Passing the PPL literal through unchanged therefore adds 90&nbsp;ms, not
 * 90&nbsp;days. This adapter reconstructs the interval in the backend's expected base units —
 * milliseconds under a {@link TimeUnit#DAY} qualifier for day-time units, months under a
 * {@link TimeUnit#MONTH} qualifier for the month family — exactly as {@link EarliestLatestAdapter}
 * does. The {@code DATE_SUB} sign is folded into the converted value.
 *
 * <p>The base is cast to the call's declared TIMESTAMP type first: PPL {@code DATE_ADD} always
 * returns TIMESTAMP (midnight UTC when the base is a DATE), and the DATE-to-TIMESTAMP cast maps to
 * arrow's Date32 to Timestamp(Nanosecond) kernel (see {@code TimestampFunctionAdapter}'s Shape B).
 *
 * <p><b>Scope.</b> Only DATE and TIMESTAMP bases are lowered. A TIME base is anchored to the
 * query-start date by the PPL UDF before the interval is added; this adapter has no access to that
 * date, so it leaves TIME bases (and any non-temporal base) on the UDF path rather than emitting an
 * epoch-anchored result. Likewise, sub-millisecond interval units (MICROSECOND) aren't representable
 * in Arrow's millisecond-granular day-time interval and are left for the UDF path. In both cases the
 * call is returned unchanged so Substrait conversion surfaces a precise "unrecognized function"
 * error rather than silently mis-lowering.
 *
 * <p>Returns the call unchanged when the shape isn't the expected {@code (base, INTERVAL literal)}
 * pair as well.
 *
 * @opensearch.internal
 */
class DateAddSubAdapter implements ScalarFunctionAdapter {

    private static final long MILLIS_PER_SECOND = 1_000L;
    private static final long MILLIS_PER_MINUTE = 60_000L;
    private static final long MILLIS_PER_HOUR = 3_600_000L;
    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final long MILLIS_PER_WEEK = 7L * MILLIS_PER_DAY;

    private final boolean isAdd;

    DateAddSubAdapter(boolean isAdd) {
        this.isAdd = isAdd;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            return original;
        }
        RexNode base = original.getOperands().get(0);
        RexNode intervalOperand = stripOperatorAnnotation(original.getOperands().get(1));
        if (!(intervalOperand instanceof RexLiteral intervalLiteral)
            || !SqlTypeName.INTERVAL_TYPES.contains(intervalLiteral.getType().getSqlTypeName())) {
            return original;
        }
        SqlIntervalQualifier qualifier = intervalLiteral.getType().getIntervalQualifier();
        BigDecimal leadingValue = intervalLiteral.getValueAs(BigDecimal.class);
        if (qualifier == null || leadingValue == null) {
            return original;
        }

        // PPL DATE_ADD/DATE_SUB accept DATE / TIMESTAMP / TIME bases, but we only lower DATE and
        // TIMESTAMP. A DATE base casts to midnight UTC of the call's declared TIMESTAMP type and a
        // TIMESTAMP base passes through; a TIME base, however, is anchored to the query-start DATE
        // by the PPL UDF before the interval is added — a plain CAST(time AS TIMESTAMP) would anchor
        // it to the epoch instead, silently shifting the result onto 1970-01-01. We can't reproduce
        // query-date anchoring here, so leave TIME (and any other base type) for the UDF path:
        // returning the original call surfaces a loud "Unrecognized scalar function" error rather
        // than a wrong date.
        SqlTypeName baseSqlType = base.getType().getSqlTypeName();
        if (baseSqlType != SqlTypeName.DATE
            && baseSqlType != SqlTypeName.TIMESTAMP
            && baseSqlType != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return original;
        }

        RexBuilder rexBuilder = cluster.getRexBuilder();
        long signedLeading = (isAdd ? 1L : -1L) * leadingValue.longValueExact();

        // Convert the leading-field value into the backend's base unit (millis for day-time, months
        // for the month family) and emit a normalized qualifier — see class javadoc.
        long baseValue;
        TimeUnit baseUnit;
        switch (qualifier.getUnit()) {
            case MILLISECOND -> {
                baseValue = signedLeading;
                baseUnit = TimeUnit.DAY;
            }
            case SECOND -> {
                baseValue = signedLeading * MILLIS_PER_SECOND;
                baseUnit = TimeUnit.DAY;
            }
            case MINUTE -> {
                baseValue = signedLeading * MILLIS_PER_MINUTE;
                baseUnit = TimeUnit.DAY;
            }
            case HOUR -> {
                baseValue = signedLeading * MILLIS_PER_HOUR;
                baseUnit = TimeUnit.DAY;
            }
            case DAY -> {
                baseValue = signedLeading * MILLIS_PER_DAY;
                baseUnit = TimeUnit.DAY;
            }
            case WEEK -> {
                baseValue = signedLeading * MILLIS_PER_WEEK;
                baseUnit = TimeUnit.DAY;
            }
            case MONTH -> {
                baseValue = signedLeading;
                baseUnit = TimeUnit.MONTH;
            }
            case QUARTER -> {
                baseValue = signedLeading * 3L;
                baseUnit = TimeUnit.MONTH;
            }
            case YEAR -> {
                baseValue = signedLeading * 12L;
                baseUnit = TimeUnit.MONTH;
            }
            default -> {
                // MICROSECOND (and any sub-millisecond unit) can't be represented exactly in
                // Arrow's millisecond-granular IntervalDayTime, so don't silently truncate — fall
                // through to the UDF path, which surfaces a loud error rather than a wrong result.
                return original;
            }
        }

        // PPL DATE_ADD/DATE_SUB return TIMESTAMP; lift the (possibly DATE) base to that type so
        // DATETIME_PLUS yields a TIMESTAMP and the surrounding rowType matches the call's type.
        RexNode baseTimestamp = rexBuilder.makeAbstractCast(original.getType(), base);
        RexNode interval = rexBuilder.makeIntervalLiteral(
            BigDecimal.valueOf(baseValue),
            new SqlIntervalQualifier(baseUnit, null, SqlParserPos.ZERO)
        );

        RexNode shifted = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, baseTimestamp, interval);
        if (shifted.getType().equals(original.getType())) {
            return shifted;
        }
        return rexBuilder.makeAbstractCast(original.getType(), shifted);
    }

    private static RexNode stripOperatorAnnotation(RexNode node) {
        while (node instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            node = annotation.unwrap();
        }
        return node;
    }
}
