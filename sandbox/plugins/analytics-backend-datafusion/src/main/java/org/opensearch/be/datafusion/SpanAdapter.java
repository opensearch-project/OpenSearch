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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Rewrites PPL's {@code SPAN(field, interval, unit)} UDF into a Substrait-friendly
 * expression tree that DataFusion can execute natively.
 *
 * <p>SPAN's third argument distinguishes the two modes:
 * <ul>
 *   <li><b>Numeric span</b> ({@code unit} is a typed-NULL literal): rewritten to
 *       {@code FLOOR(field / interval) * interval} for non-integer numerics, or to
 *       {@code (field / interval) * interval} for integer types (where Calcite's
 *       integer division already truncates).</li>
 *   <li><b>Time span</b> ({@code unit} is a single-letter unit string like
 *       {@code "y"}, {@code "M"}, {@code "d"}, etc.): rewritten to
 *       {@code DATE_TRUNC(<unit>, field)} when {@code interval == 1}. Multi-unit
 *       intervals like {@code 12h} aren't expressible as {@code date_trunc} and
 *       fall through to the original UDF, which surfaces as a normal substrait
 *       binding error rather than a silent wrong-result.</li>
 * </ul>
 *
 * <p>The unit-letter mapping mirrors PPL's {@code SpanUnit} enum (defined in the SQL
 * plugin so not directly referenced here):
 * {@code us → microsecond, ms → millisecond, s → second, m → minute, h → hour,
 *  d → day, w → week, M → month, q → quarter, y → year}. DataFusion's
 * {@code date_trunc} accepts the long-form names.
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

    /**
     * Fixed-length PPL span units → seconds. Used by the multi-unit rewrite path
     * (e.g. {@code span=2m}, {@code span=12h}). Month / quarter / year are excluded
     * because their length depends on the calendar position of {@code t} — bucketing
     * those needs DataFusion {@code date_bin} with an interval-month argument rather
     * than a fixed-second multiplier, and is tracked separately.
     */
    private static final Map<String, Long> FIXED_UNIT_SECONDS = Map.ofEntries(
        Map.entry("s", 1L),
        Map.entry("m", 60L),
        Map.entry("h", 3600L),
        Map.entry("d", 86400L),
        Map.entry("w", 604800L)
    );

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
                // Multi-unit fixed-length time span: bucket via integer seconds since epoch.
                // SPAN(t, N, '<unit>') → TIMESTAMP_SECONDS(FLOOR(UNIX_SECONDS(t) / B) * B)
                // where B = N * unit_seconds. date_trunc handles N=1 above; for N>=2 there is no
                // single date_trunc that aligns to an arbitrary multiple. This rewrite is exact for
                // fixed-length units (s/m/h/d/w) because the epoch is a fixed reference; multi-unit
                // month/quarter/year fall through to the original SPAN UDF (variable bucket length).
                Long unitSeconds = FIXED_UNIT_SECONDS.get(unitText);
                Long bucketSeconds = bucketSecondsIfPositiveInteger(interval, unitSeconds);
                if (bucketSeconds != null && bucketSeconds > 0L) {
                    return rewriteFixedLengthTimeBucket(rexBuilder, field, bucketSeconds, original.getType());
                }
            }
        }

        // Anything else falls through unchanged.
        return original;
    }

    /**
     * Returns {@code N * unit_seconds} when both inputs are present and {@code N} is a
     * positive integer literal; {@code null} otherwise.
     */
    private static Long bucketSecondsIfPositiveInteger(RexNode interval, Long unitSeconds) {
        if (unitSeconds == null || !(interval instanceof RexLiteral lit)) {
            return null;
        }
        Object value = lit.getValue();
        long n;
        if (value instanceof BigDecimal bd) {
            if (bd.scale() > 0 && bd.stripTrailingZeros().scale() > 0) {
                return null; // non-integer interval not supported
            }
            n = bd.longValueExact();
        } else if (value instanceof Number num) {
            double d = num.doubleValue();
            if (d != Math.floor(d)) {
                return null;
            }
            n = (long) d;
        } else {
            return null;
        }
        if (n <= 0) {
            return null;
        }
        return n * unitSeconds;
    }

    private static RexNode rewriteFixedLengthTimeBucket(RexBuilder rexBuilder, RexNode field, long bucketSeconds, RelDataType resultType) {
        RexNode bucketSizeLit = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(bucketSeconds));
        // Use the locally-declared substrait-mapped operators (same ones UnixTimestampAdapter
        // and RustUdfDateTimeAdapters.FromUnixtimeAdapter rewrite to). Calcite's stdlib
        // UNIX_SECONDS / TIMESTAMP_SECONDS would bind to BigQuery-named substrait functions
        // that DataFusion's substrait consumer does not have entries for; the LOCAL_*_OP
        // pair routes through the analytics-backend-datafusion FunctionMappings.s entries
        // to DataFusion's native `to_unixtime` and `from_unixtime` UDFs.
        RexNode epochSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, field);
        // Integer / integer division already truncates toward zero (and toward negative
        // infinity for non-negative epoch seconds, which all our timechart-tested data is),
        // so the FLOOR step the numeric-span rewrite uses is redundant here. Skipping it
        // also avoids the "Unable to convert call FLOOR(i64?)" substrait gap — DataFusion's
        // floor function binds for floating-point only.
        RexNode bucketIndex = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, epochSeconds, bucketSizeLit);
        RexNode bucketStart = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, bucketIndex, bucketSizeLit);
        // from_unixtime's substrait signature is `(fp64) -> precision_timestamp<6>` (see
        // opensearch_scalar_functions.yaml); cast the i64 bucket start to fp64 to match.
        RelDataType fp64 = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
        RexNode bucketStartDouble = rexBuilder.makeCast(fp64, bucketStart, true);
        RexNode asTimestamp = rexBuilder.makeCall(RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, bucketStartDouble);
        // Pin to the SPAN call's declared return type — matches the numeric rewrite path's
        // typeMatchesInferred safeguard.
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
        // Pin the rewritten expression to the SPAN call's declared return type. Calcite's
        // multiplication-precision inference can produce a wider DECIMAL than the SPAN UDF
        // declared (e.g. DECIMAL(31,1) vs DECIMAL(20,1)), and the surrounding Project's
        // typeMatchesInferred check throws AssertionError if the substituted expression's
        // type differs from the original call site's type.
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
