/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Rewrites PPL {@code SPAN(field, interval, unit)} into a Substrait-friendly tree.
 *
 * <p>Numeric span (unit = typed-NULL): {@code (field / interval) * interval} (FLOOR for non-integer).
 * Time span (unit = single letter): {@code interval == 1} → {@code DATE_TRUNC}; fixed-length s/m/h/d/w
 * with N&gt;1 → integer-seconds arithmetic; sub-second us/ms → {@code date_bin("<N> <unit>", t)}; calendar
 * M/q/y → {@code date_bin("<N> <unit>", t, '1970-01-01T00:00:00Z')}.
 * Unit letters: us/ms/s/m/h/d/w/M/q/y → microsecond/millisecond/second/minute/hour/day/week/month/quarter/year.
 *
 * @opensearch.internal
 */
class SpanAdapter implements ScalarFunctionAdapter {

    /** Single-letter PPL span unit → DataFusion {@code date_trunc} unit name. */
    private static final Map<String, String> UNIT_TO_DATE_TRUNC = Map.ofEntries(
        Map.entry("us", "microsecond"),
        Map.entry("ms", "millisecond"),
        Map.entry("s", "second"),
        Map.entry("m", "minute"),
        Map.entry("h", "hour"),
        Map.entry("d", "day"),
        Map.entry("w", "week"),
        Map.entry("M", "month"),
        Map.entry("q", "quarter"),
        Map.entry("y", "year")
    );

    /** Fixed-length PPL span units → seconds (M/q/y excluded — calendar-dependent). */
    private static final Map<String, Long> FIXED_UNIT_SECONDS = Map.ofEntries(
        Map.entry("s", 1L),
        Map.entry("m", 60L),
        Map.entry("h", 3600L),
        Map.entry("d", 86400L),
        Map.entry("w", 604800L)
    );

    /** Sub-second PPL units → date_bin stride suffix (multi-unit path; N=1 uses date_trunc). */
    private static final Map<String, String> SUB_SECOND_UNIT_TO_DATE_BIN_STRIDE = Map.of("us", "microseconds", "ms", "milliseconds");

    /** Calendar PPL units → date_bin stride suffix; bucketed against month-aligned epoch (1970-01-01). */
    private static final Map<String, String> CALENDAR_UNIT_TO_DATE_BIN_STRIDE = Map.of("M", "month", "q", "quarter", "y", "year");

    /** date_bin target — name matches DataFusion native; ARG1_NULLABLE return preserves the source timestamp type. */
    static final SqlOperator LOCAL_DATE_BIN_OP = new SqlFunction(
        "date_bin",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG1_NULLABLE,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.TIMEDATE
    );

    // TODO: replace with a backend-neutral bucketing primitive emitted upstream so this adapter can go.
    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (!original.getOperator().getName().equalsIgnoreCase("SPAN")) {
            return original;
        }
        if (original.getOperands().size() != 3) {
            return original;
        }
        RexNode field = original.getOperands().get(0);
        RexNode interval = original.getOperands().get(1);
        RexNode unit = original.getOperands().get(2);
        RexBuilder rexBuilder = cluster.getRexBuilder();

        // Numeric span: unit operand is typed-NULL.
        if (unit.getType().getSqlTypeName() == SqlTypeName.NULL || (unit instanceof RexLiteral lit && lit.isNull())) {
            return rewriteNumericSpan(rexBuilder, field, interval, original.getType());
        }

        // Time span: unit operand is a string literal.
        if (unit instanceof RexLiteral lit && lit.getValue() != null) {
            String unitText = lit.getValueAs(String.class);
            if (unitText != null) {
                String dateTruncUnit = UNIT_TO_DATE_TRUNC.get(unitText);
                if (dateTruncUnit != null && isUnitInterval(interval)) {
                    RexNode unitArg = rexBuilder.makeLiteral(dateTruncUnit);
                    return rexBuilder.makeCall(original.getType(), SqlLibraryOperators.DATE_TRUNC, List.of(unitArg, field));
                }
                // multi-unit fixed-length time span: bucket via integer seconds since epoch (B = N * unit_seconds)
                Long unitSeconds = FIXED_UNIT_SECONDS.get(unitText);
                Long bucketSeconds = bucketSecondsIfPositiveInteger(interval, unitSeconds);
                if (bucketSeconds != null && bucketSeconds > 0L) {
                    return rewriteFixedLengthTimeBucket(rexBuilder, field, bucketSeconds, original.getType());
                }
                // sub-second multi-unit (us/ms): date_bin with string stride preserves precision
                String dateBinStrideUnit = SUB_SECOND_UNIT_TO_DATE_BIN_STRIDE.get(unitText);
                if (dateBinStrideUnit != null) {
                    Long n = extractPositiveInteger(interval);
                    if (n != null) {
                        RexNode stride = rexBuilder.makeLiteral(n + " " + dateBinStrideUnit);
                        return rexBuilder.makeCall(original.getType(), LOCAL_DATE_BIN_OP, List.of(stride, field));
                    }
                }
                // calendar multi-unit (M/q/y): date_bin with month-aligned origin '1970-01-01T00:00:00Z'
                String calendarStrideUnit = CALENDAR_UNIT_TO_DATE_BIN_STRIDE.get(unitText);
                if (calendarStrideUnit != null) {
                    Long n = extractPositiveInteger(interval);
                    if (n != null) {
                        RexNode stride = rexBuilder.makeLiteral(n + " " + calendarStrideUnit);
                        RexNode origin = rexBuilder.makeLiteral("1970-01-01T00:00:00Z");
                        return rexBuilder.makeCall(original.getType(), LOCAL_DATE_BIN_OP, List.of(stride, field, origin));
                    }
                }
            }
        }

        // Anything else falls through unchanged.
        return original;
    }

    /** Positive whole-number Long from a numeric literal; null otherwise. */
    private static Long extractPositiveInteger(RexNode interval) {
        if (!(interval instanceof RexLiteral lit)) {
            return null;
        }
        Object value = lit.getValue();
        long n;
        if (value instanceof BigDecimal bd) {
            // stripTrailingZeros: `1`, `1.0`, `1E2` collapse to scale ≤ 0; fractional → scale > 0
            if (bd.stripTrailingZeros().scale() > 0) {
                return null;
            }
            try {
                n = bd.longValueExact();
            } catch (ArithmeticException e) {
                return null;
            }
        } else if (value instanceof Number num) {
            if (!isIntegralDouble(num.doubleValue())) {
                return null;
            }
            n = (long) num.doubleValue();
        } else {
            return null;
        }
        return n > 0 ? n : null;
    }

    /** {@code N * unit_seconds} when {@code N} is a positive integer literal; {@code null} otherwise. */
    private static Long bucketSecondsIfPositiveInteger(RexNode interval, Long unitSeconds) {
        if (unitSeconds == null) {
            return null;
        }
        Long n = extractPositiveInteger(interval);
        if (n == null) {
            return null;
        }
        try {
            return Math.multiplyExact(n, unitSeconds);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                "SPAN interval is too large: " + n + " * " + unitSeconds + " seconds exceeds the long range",
                e
            );
        }
    }

    /** True when {@code d} is finite and integral. */
    private static boolean isIntegralDouble(double d) {
        return Double.isFinite(d) && d == Math.floor(d);
    }

    private static RexNode rewriteFixedLengthTimeBucket(RexBuilder rexBuilder, RexNode field, long bucketSeconds, RelDataType resultType) {
        RexNode bucketSizeLit = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(bucketSeconds));
        // route through LOCAL_TO_UNIXTIME_OP / LOCAL_FROM_UNIXTIME_OP — Calcite stdlib's UNIX/TIMESTAMP_SECONDS bind BigQuery-named
        // substrait fns
        RexNode epochSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, field);
        // i64/i64 already truncates toward zero; FLOOR would force fp64 (substrait floor is fp-only)
        RexNode bucketIndex = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, epochSeconds, bucketSizeLit);
        RexNode bucketStart = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, bucketIndex, bucketSizeLit);
        // from_unixtime sig is (fp64) → precision_timestamp<6>
        RelDataType fp64 = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
        RexNode bucketStartDouble = rexBuilder.makeCast(fp64, bucketStart, true);
        RexNode asTimestamp = rexBuilder.makeCall(RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, bucketStartDouble);
        return rexBuilder.makeCast(resultType, asTimestamp, true);
    }

    private static RexNode rewriteNumericSpan(RexBuilder rexBuilder, RexNode field, RexNode interval, RelDataType resultType) {
        SqlTypeName resultTypeName = resultType.getSqlTypeName();
        boolean integerResult = resultTypeName == SqlTypeName.INTEGER
            || resultTypeName == SqlTypeName.BIGINT
            || resultTypeName == SqlTypeName.SMALLINT
            || resultTypeName == SqlTypeName.TINYINT;
        RexNode quotient = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, field, interval);
        RexNode bucket = integerResult ? quotient : rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, quotient);
        RexNode product = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, bucket, interval);
        // pin to call's declared type — Calcite's mul-precision inference can widen the DECIMAL
        return rexBuilder.makeCast(resultType, product, true);
    }

    /** True when the interval RexNode is a numeric literal exactly equal to 1. */
    private static boolean isUnitInterval(RexNode interval) {
        if (!(interval instanceof RexLiteral lit)) {
            return false;
        }
        Object value = lit.getValue();
        if (value instanceof BigDecimal bd) {
            return bd.compareTo(BigDecimal.ONE) == 0;
        }
        if (value instanceof Number n) {
            return n.doubleValue() == 1.0;
        }
        return false;
    }
}
