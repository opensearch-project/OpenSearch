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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrites PPL {@code DATE_ADD(base, INTERVAL n unit)} / {@code DATE_SUB(base, INTERVAL n unit)}
 * into {@code DATETIME_PLUS(CAST(base AS TIMESTAMP), interval)}, which lowers to Substrait's
 * {@code add(timestamp, interval)} that DataFusion executes natively. The raw PPL UDFs have no
 * Substrait binding, so isthmus rejects them.
 *
 * <p>PPL's interval literal carries the leading-field value (e.g. {@code 90} for
 * {@code INTERVAL 90 day}), but Substrait/DataFusion expect day-time intervals in milliseconds and
 * year-month intervals in months. This adapter rebuilds the interval in those base units (under a
 * {@link TimeUnit#DAY} or {@link TimeUnit#MONTH} qualifier) and folds the {@code DATE_SUB} sign in,
 * the same way {@link EarliestLatestAdapter} does.
 *
 * <p>Only DATE and TIMESTAMP bases are lowered. TIME bases (the PPL UDF anchors them to the
 * query-start date, which this adapter can't reproduce) and sub-millisecond units (MICROSECOND,
 * unrepresentable in Arrow's millisecond-granular interval) are left on the UDF path, as is any
 * unexpected shape — the call is returned unchanged so Substrait conversion raises a loud
 * "unrecognized function" error rather than mis-lowering.
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

        RexBuilder rexBuilder = cluster.getRexBuilder();

        // PPL DATE_ADD/DATE_SUB accept DATE / TIMESTAMP / TIME bases, but we only lower DATE and
        // TIMESTAMP. A DATE base casts to midnight UTC of the call's declared TIMESTAMP type and a
        // TIMESTAMP base passes through; a TIME base, however, is anchored to the query-start DATE
        // by the PPL UDF before the interval is added — a plain CAST(time AS TIMESTAMP) would anchor
        // it to the epoch instead, silently shifting the result onto 1970-01-01. We can't reproduce
        // query-date anchoring here, so leave TIME (and any other base type) for the UDF path:
        // returning the original call surfaces a loud "Unrecognized scalar function" error rather
        // than a wrong date.
        //
        // A CHARACTER base appears when subquery decorrelation constant-folds the PPL DATE() UDF
        // wrapper off the literal (e.g. `date_add(date('1994-01-01'), interval 1 year)` inside an
        // EXISTS / IN / scalar subsearch becomes `DATE_ADD('1994-01-01':VARCHAR, interval)`). Coerce
        // it back to TIMESTAMP so the same DATETIME_PLUS lowering applies — mirrors the comparison
        // path's recovery. See ComparisonTemporalCoercionAdapter for the broader rationale.
        SqlTypeName baseSqlType = base.getType().getSqlTypeName();
        // A character base coerces to a plain Calcite TIMESTAMP (isthmus reads it as
        // precision_timestamp); feed that straight to DATETIME_PLUS rather than re-casting it to the
        // EXPR_TIMESTAMP UDT, whose VARCHAR storage isthmus would otherwise see as a string operand
        // ("Unable to convert call +(string, interval)").
        boolean characterBase = SqlTypeFamily.CHARACTER.contains(base.getType());
        if (characterBase) {
            base = DatePartAdapters.coerceCharacterOperandToTimestamp(base, cluster);
        } else if (baseSqlType != SqlTypeName.DATE
            && baseSqlType != SqlTypeName.TIMESTAMP
            && baseSqlType != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                return original;
            }
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
        // DATETIME_PLUS yields a TIMESTAMP and the surrounding rowType matches the call's type. A
        // character base is already a plain Calcite TIMESTAMP (see above) — keep it as-is so isthmus
        // sees precision_timestamp, not the UDT's VARCHAR storage.
        RexNode baseTimestamp = characterBase ? base : rexBuilder.makeAbstractCast(original.getType(), base);
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
