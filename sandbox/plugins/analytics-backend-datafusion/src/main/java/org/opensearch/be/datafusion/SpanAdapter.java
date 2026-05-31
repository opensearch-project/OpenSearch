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
 *       {@code "y"}, {@code "M"}, {@code "d"}, etc.) lowers through three paths,
 *       checked in order:
 *       <ul>
 *         <li><b>{@code interval == 1}, any unit:</b> rewritten to
 *             {@code DATE_TRUNC(<unit>, field)} — the cheapest DataFusion path.</li>
 *         <li><b>Fixed-length unit (s/m/h/d/w) with {@code interval > 1}:</b> bucketed
 *             via integer-seconds arithmetic ({@code TIMESTAMP_SECONDS(FLOOR(UNIX_SECONDS(t) / B) * B)}).
 *             Exact because the epoch is a fixed reference for these units.</li>
 *         <li><b>Sub-second unit (us/ms) with {@code interval > 1}:</b> rewritten to
 *             DataFusion's native {@code date_bin("<N> <unit>", field)}. The arithmetic
 *             path can't be used here because {@code to_unixtime} returns BIGINT seconds
 *             and loses sub-second precision. Emitting the stride as a plain string
 *             literal (e.g. {@code "40 milliseconds"}) avoids substrait interval-type
 *             plumbing and matches how DataFusion's native parser accepts strides.</li>
 *       </ul>
 *       Multi-unit month / quarter / year intervals (e.g. {@code 12M}) still fall through
 *       to the original UDF — their bucket length is calendar-dependent, requiring a
 *       {@code date_bin} with an interval-month stride; tracked separately.
 *   </li>
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

    /**
     * Sub-second PPL span units → DataFusion {@code date_bin} stride suffix. Only used
     * for the multi-unit {@code date_bin} rewrite path (N &gt; 1); N == 1 cases use the
     * cheaper {@code date_trunc} path above. The fixed-second arithmetic path can't be
     * used here because {@code to_unixtime} returns BIGINT seconds, losing sub-second
     * precision; emitting a string-form stride to DataFusion's native {@code date_bin}
     * preserves the millisecond / microsecond resolution.
     */
    private static final Map<String, String> SUB_SECOND_UNIT_TO_DATE_BIN_STRIDE = Map.of("us", "microseconds", "ms", "milliseconds");

    /**
     * Locally-declared target operator for the sub-second {@code date_bin} path. Name
     * matches DataFusion's native {@code date_bin}; the operand checker is informational
     * (the adapter constructs the call directly and isthmus resolves the substrait
     * function by name). Return type is pinned to ARG1 (the source-timestamp operand)
     * so the rewritten call's declared type matches the original SPAN call (and thus
     * the enclosing Project's cached rowType).
     */
    static final SqlOperator LOCAL_DATE_BIN_OP = new SqlFunction(
        "date_bin",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG1_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.TIMEDATE
    );

    // TODO: SPAN is a PPL-shaped UDF; matching by operator name and decomposing its operands here
    // couples this backend adapter to the SQL/PPL plugin's frontend representation. Long term,
    // Analytics-Engine should hand the backend a backend-neutral bucketing primitive (e.g. a typed
    // DATE_BIN / FLOOR-divide expression already lowered upstream) so this adapter can disappear
    // and we stop replicating SPAN's semantics per-backend.
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
                // Sub-second multi-unit time span: bucket via DataFusion's native date_bin.
                // SPAN(t, N, 'us'|'ms') → date_bin("<N> microseconds"|"<N> milliseconds", t).
                // The arithmetic path above can't be used because to_unixtime returns BIGINT
                // seconds and would truncate sub-second precision. date_bin accepts the
                // stride as a plain string literal natively in DataFusion's SQL parser,
                // which sidesteps the substrait interval-type plumbing.
                String dateBinStrideUnit = SUB_SECOND_UNIT_TO_DATE_BIN_STRIDE.get(unitText);
                if (dateBinStrideUnit != null) {
                    Long n = extractPositiveInteger(interval);
                    if (n != null) {
                        RexNode stride = rexBuilder.makeLiteral(n + " " + dateBinStrideUnit);
                        return rexBuilder.makeCall(original.getType(), LOCAL_DATE_BIN_OP, List.of(stride, field));
                    }
                }
            }
        }

        // Anything else falls through unchanged.
        return original;
    }

    /**
     * Returns the interval as a positive whole-number {@code Long} if it is a numeric
     * literal whose value is a positive integer; {@code null} otherwise. Sibling of
     * {@link #bucketSecondsIfPositiveInteger} without the unit-seconds multiplier — the
     * sub-second date_bin path bakes the unit into the stride string instead.
     */
    private static Long extractPositiveInteger(RexNode interval) {
        if (!(interval instanceof RexLiteral lit)) {
            return null;
        }
        Object value = lit.getValue();
        long n;
        if (value instanceof BigDecimal bd) {
            // stripTrailingZeros canonicalises both forms — `1`, `1.0`, `1E2` all collapse
            // to scale ≤ 0; any fractional component leaves scale > 0.
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

    /**
     * Returns {@code N * unit_seconds} when both inputs are present and {@code N} is a
     * positive integer literal; {@code null} otherwise.
     */
    private static Long bucketSecondsIfPositiveInteger(RexNode interval, Long unitSeconds) {
        if (unitSeconds == null) {
            return null;
        }
        Long n = extractPositiveInteger(interval);
        return n == null ? null : n * unitSeconds;
    }

    /**
     * True when {@code d} is finite and equal to its floor — i.e. a whole-number double
     * with no fractional part. Centralises the rounding check used by the literal
     * extractors above.
     */
    private static boolean isIntegralDouble(double d) {
        return Double.isFinite(d) && d == Math.floor(d);
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
