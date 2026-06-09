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
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Rewrites PPL {@code DATE_ADD(base, INTERVAL n unit)} / {@code DATE_SUB(base, INTERVAL n unit)}
 * into {@code DATETIME_PLUS(CAST(base AS TIMESTAMP), interval)}, which lowers to Substrait's
 * {@code add(timestamp, interval)} that DataFusion executes natively. The raw PPL UDFs have no
 * Substrait binding, so isthmus rejects them.
 *
 * <p>Also handles ADDDATE/SUBDATE's integer form: {@code ADDDATE(base, N)} is treated as
 * {@code INTERVAL N DAY} per MySQL semantics — the integer second operand is normalized to a
 * day-interval literal before the standard lowering proceeds.
 *
 * <p>PPL's interval literal carries the leading-field value (e.g. {@code 90} for
 * {@code INTERVAL 90 day}), but Substrait/DataFusion expect day-time intervals in milliseconds and
 * year-month intervals in months. This adapter rebuilds the interval in those base units (under a
 * {@link TimeUnit#DAY} or {@link TimeUnit#MONTH} qualifier) and folds the {@code DATE_SUB} sign in,
 * the same way {@link EarliestLatestAdapter} does.
 *
 * <p>DATE / TIMESTAMP / TIME / character bases are all lowered. TIME anchors to today-UTC at plan
 * time, which can drift from the PPL UDF's query-start anchor across UTC midnight or cached plans
 * (TODO: thread {@code FunctionProperties#getQueryStartClock} through to adapters). MICROSECOND
 * intervals can't be represented in Arrow's millisecond IntervalDayTime; the call is returned
 * unchanged so the UDF path raises a loud error rather than mis-lowering.
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
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode base = original.getOperands().get(0);
        RexNode intervalOperand = stripOperatorAnnotation(original.getOperands().get(1));

        SqlIntervalQualifier qualifier;
        BigDecimal leadingValue;
        if (intervalOperand instanceof RexLiteral intervalLiteral
            && SqlTypeName.INTERVAL_TYPES.contains(intervalLiteral.getType().getSqlTypeName())) {
            qualifier = intervalLiteral.getType().getIntervalQualifier();
            leadingValue = intervalLiteral.getValueAs(BigDecimal.class);
        } else if (SqlTypeFamily.NUMERIC.contains(intervalOperand.getType()) && intervalOperand instanceof RexLiteral numericLiteral) {
            // ADDDATE/SUBDATE integer form: ADDDATE(base, N) is treated as INTERVAL N DAY.
            qualifier = new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
            leadingValue = numericLiteral.getValueAs(BigDecimal.class);
        } else {
            return original;
        }
        if (qualifier == null || leadingValue == null) {
            return original;
        }

        // Character base appears post-decorrelation when the PPL DATE() wrapper is folded off a
        // literal inside EXISTS/IN/scalar subqueries — coerce back to plain Calcite TIMESTAMP (not
        // the UDT) so isthmus sees precision_timestamp.
        SqlTypeName baseSqlType = base.getType().getSqlTypeName();
        boolean characterBase = SqlTypeFamily.CHARACTER.contains(base.getType());
        if (characterBase) {
            base = DatePartAdapters.coerceCharacterOperandToTimestamp(base, cluster);
        } else if (baseSqlType != SqlTypeName.DATE
            && baseSqlType != SqlTypeName.TIMESTAMP
            && baseSqlType != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            && baseSqlType != SqlTypeName.TIME) {
                return original;
            }
        long signedLeading = (isAdd ? 1L : -1L) * leadingValue.longValueExact();

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
            // MICROSECOND and unknown units fall through to the UDF path — see class javadoc.
            default -> {
                return original;
            }
        }

        RexNode baseTimestamp = liftToTimestamp(base, baseSqlType, characterBase, original.getType(), rexBuilder);
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

    /** Lift the base to the call's TIMESTAMP type. TIME prepends today-UTC; character base passes through. */
    private static RexNode liftToTimestamp(
        RexNode base,
        SqlTypeName baseSqlType,
        boolean characterBase,
        RelDataType targetType,
        RexBuilder rexBuilder
    ) {
        if (characterBase) {
            return base;
        }
        if (baseSqlType == SqlTypeName.TIME) {
            RelDataType varchar = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
            RelDataType nullableVarchar = rexBuilder.getTypeFactory().createTypeWithNullability(varchar, base.getType().isNullable());
            RexNode timeAsVarchar = rexBuilder.makeCast(nullableVarchar, base);
            RexNode prefixLit = rexBuilder.makeLiteral(LocalDate.now(ZoneOffset.UTC) + " ", varchar, false);
            RexNode concat = rexBuilder.makeCall(nullableVarchar, SqlStdOperatorTable.CONCAT, List.of(prefixLit, timeAsVarchar));
            return rexBuilder.makeAbstractCast(targetType, concat);
        }
        return rexBuilder.makeAbstractCast(targetType, base);
    }

    private static RexNode stripOperatorAnnotation(RexNode node) {
        while (node instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            node = annotation.unwrap();
        }
        return node;
    }
}
