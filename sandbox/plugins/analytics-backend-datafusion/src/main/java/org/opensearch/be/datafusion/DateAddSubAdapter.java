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
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
 * (and the alias forms {@code ADDDATE} / {@code SUBDATE}) into
 * {@code DATETIME_PLUS(CAST(base AS TIMESTAMP), interval)}, which lowers to Substrait's
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
 * <p>{@code ADDDATE(base, n_days)} / {@code SUBDATE(base, n_days)} share the same lowering: the
 * integer second operand is rebuilt as an {@code INTERVAL n DAY} literal before the standard
 * interval path runs. This matches the SQL plugin's {@code AddSubDateFunction} semantics.
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
        RexNode rawSecond = stripOperatorAnnotation(original.getOperands().get(1));
        // ADDDATE/SUBDATE accept INTEGER days; rebuild as INTERVAL n DAY so the interval path runs.
        RexLiteral intervalLiteral = asIntervalLiteral(rawSecond, rexBuilder);
        if (intervalLiteral == null) {
            // Non-literal integer days (e.g. `bin span=Nday`): see adaptNonLiteralIntegerDays.
            if (rawSecond != null && SqlTypeName.INT_TYPES.contains(rawSecond.getType().getSqlTypeName())) {
                return adaptNonLiteralIntegerDays(original, base, rawSecond, cluster);
            }
            return original;
        }
        SqlIntervalQualifier qualifier = intervalLiteral.getType().getIntervalQualifier();
        BigDecimal leadingValue = intervalLiteral.getValueAs(BigDecimal.class);
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

    /**
     * Non-literal integer-days form (e.g. {@code bin span=Nday}). DataFusion can't evaluate
     * {@code Int64 * Interval(DayTime)} at runtime, so lower to
     * {@code from_unixtime(baseEpochSec + daysExpr * 86400.0)}; base must be a foldable literal.
     */
    private RexNode adaptNonLiteralIntegerDays(RexCall original, RexNode base, RexNode daysExpr, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        Long baseEpochSeconds = tryFoldBaseToEpochSeconds(base);
        if (baseEpochSeconds == null) {
            // Non-literal base — leave the call unchanged so the UDF path raises a
            // clearer error than a downstream DF planner failure.
            return original;
        }
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        // daysExpr * 86400.0 → DOUBLE epoch-seconds delta.
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RexNode daysAsDouble = rexBuilder.makeAbstractCast(
            typeFactory.createTypeWithNullability(doubleType, daysExpr.getType().isNullable()),
            daysExpr
        );
        long signedSecPerDay = isAdd ? 86_400L : -86_400L;
        RexNode secPerDayLit = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(signedSecPerDay), doubleType);
        RexNode deltaSec = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, daysAsDouble, secPerDayLit);
        RexNode baseSec = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(baseEpochSeconds), doubleType);
        RexNode totalSec = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, baseSec, deltaSec);
        RexNode ts = rexBuilder.makeCall(
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3), daysExpr.getType().isNullable()),
            RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP,
            List.of(totalSec)
        );
        if (ts.getType().equals(original.getType())) {
            return ts;
        }
        return rexBuilder.makeAbstractCast(original.getType(), ts);
    }

    /**
     * Folds a CHAR ({@code YYYY-MM-DD}) / DATE / TIMESTAMP literal to epoch-seconds at midnight UTC.
     * Returns null when {@code base} is not such a literal.
     */
    private static Long tryFoldBaseToEpochSeconds(RexNode base) {
        if (!(base instanceof RexLiteral lit)) {
            return null;
        }
        Object value = lit.getValue2();
        if (value == null) {
            return null;
        }
        try {
            if (SqlTypeFamily.CHARACTER.contains(lit.getType())) {
                String s = value.toString().trim();
                if (s.length() == 10) {
                    return LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toEpochSecond();
                }
                return null;
            }
            SqlTypeName tn = lit.getType().getSqlTypeName();
            if (tn == SqlTypeName.DATE) {
                int days = ((Number) value).intValue();
                return days * 86_400L;
            }
            if (tn == SqlTypeName.TIMESTAMP || tn == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                long millis = ((Number) value).longValue();
                return millis / 1_000L;
            }
        } catch (RuntimeException ignored) {
            return null;
        }
        return null;
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

    /**
     * Returns {@code node} when it's already an interval literal; for an integer literal returns a
     * synthetic {@code INTERVAL n DAY} literal (the ADDDATE/SUBDATE integer-days form). Anything
     * else returns null so the caller can pass the call through to the UDF path.
     */
    private static RexLiteral asIntervalLiteral(RexNode node, RexBuilder rexBuilder) {
        if (node instanceof RexLiteral lit) {
            if (SqlTypeName.INTERVAL_TYPES.contains(lit.getType().getSqlTypeName())) {
                return lit;
            }
            if (SqlTypeName.INT_TYPES.contains(lit.getType().getSqlTypeName())) {
                BigDecimal days = lit.getValueAs(BigDecimal.class);
                if (days == null) {
                    return null;
                }
                return rexBuilder.makeIntervalLiteral(days, new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO));
            }
        }
        return null;
    }
}
