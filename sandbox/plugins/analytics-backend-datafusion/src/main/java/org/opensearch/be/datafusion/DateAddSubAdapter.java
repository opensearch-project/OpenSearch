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

        // PPL DATE_ADD/DATE_SUB accept DATE / TIMESTAMP / TIME bases.
        // DATE / TIMESTAMP cast straight to TIMESTAMP. TIME is anchored to today's UTC date as a
        // TIMESTAMP, matching the PPL UDF's query-start-clock semantics for the common case where
        // the query starts on the same calendar day as today.
        SqlTypeName baseSqlType = base.getType().getSqlTypeName();
        if (baseSqlType != SqlTypeName.DATE
            && baseSqlType != SqlTypeName.TIMESTAMP
            && baseSqlType != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            && baseSqlType != SqlTypeName.TIME) {
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

        // PPL DATE_ADD/DATE_SUB return TIMESTAMP; lift the (possibly DATE/TIME) base to that type so
        // DATETIME_PLUS yields a TIMESTAMP and the surrounding rowType matches the call's type.
        // For TIME, prepend today's UTC date so the timestamp anchors to the current calendar day.
        RexNode baseTimestamp;
        if (baseSqlType == SqlTypeName.TIME) {
            org.apache.calcite.rel.type.RelDataType varchar = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
            org.apache.calcite.rel.type.RelDataType nullableVarchar = rexBuilder.getTypeFactory()
                .createTypeWithNullability(varchar, base.getType().isNullable());
            RexNode timeAsVarchar = rexBuilder.makeCast(nullableVarchar, base);
            String prefix = java.time.LocalDate.now(java.time.ZoneOffset.UTC).toString() + " ";
            RexNode prefixLit = rexBuilder.makeLiteral(prefix, varchar, false);
            RexNode concat = rexBuilder.makeCall(
                nullableVarchar,
                org.apache.calcite.sql.fun.SqlStdOperatorTable.CONCAT,
                List.of(prefixLit, timeAsVarchar)
            );
            baseTimestamp = rexBuilder.makeAbstractCast(original.getType(), concat);
        } else {
            baseTimestamp = rexBuilder.makeAbstractCast(original.getType(), base);
        }
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
