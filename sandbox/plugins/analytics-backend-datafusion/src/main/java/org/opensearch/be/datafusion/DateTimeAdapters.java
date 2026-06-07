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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapters for PPL datetime functions that map 1:1 to a DataFusion builtin; signatures
 * registered in {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS}.
 *
 * @opensearch.internal
 */
final class DateTimeAdapters {

    private DateTimeAdapters() {}

    static final SqlOperator LOCAL_NOW_OP = new SqlFunction(
        "now",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE
    );

    static final SqlOperator LOCAL_CURRENT_DATE_OP = new SqlFunction(
        "current_date",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE,
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE
    );

    static final SqlOperator LOCAL_CURRENT_TIME_OP = new SqlFunction(
        "current_time",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIME,
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.TIMEDATE
    );

    static final SqlOperator LOCAL_TIME_OP = new SqlFunction(
        "to_time",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIME_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    static final SqlOperator LOCAL_DATE_OP = new SqlFunction(
        "to_date",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    static final SqlOperator LOCAL_TO_TIMESTAMP_OP = new SqlFunction(
        "to_timestamp",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    /**
     * 2-arg {@code date_trunc(granularity, ts)} — granularity is a VARCHAR literal
     * ({@code 'second'}, {@code 'minute'}, {@code 'hour'}, {@code 'day'}, {@code 'month'},
     * {@code 'quarter'}, {@code 'year'}), ts is a TIMESTAMP. Used by
     * {@code EarliestFunctionAdapter} / {@code LatestFunctionAdapter} to express PPL's
     * {@code @<unit>} snap chunks symbolically — the call rides through Substrait so
     * DataFusion's {@code SimplifyExpressions} folds it together with {@code now()} at
     * the engine's own plan time, keeping the truncation aligned with the engine's
     * {@code query_execution_start_time}.
     */
    static final SqlOperator LOCAL_DATE_TRUNC_OP = new SqlFunction(
        "date_trunc",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG1_NULLABLE,
        null,
        OperandTypes.family(org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER, org.apache.calcite.sql.type.SqlTypeFamily.TIMESTAMP),
        SqlFunctionCategory.TIMEDATE
    );

    /** PPL {@code now()} / {@code sysdate([fsp])}: drop any FSP integer operand before mapping to DF builtin {@code now()}. */
    static final class NowAdapter extends AbstractNameMappingAdapter {
        NowAdapter() {
            super(LOCAL_NOW_OP, List.of(), List.of());
        }

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            if (original.getOperands().isEmpty()) {
                return super.adapt(original, fieldStorage, cluster);
            }
            // SYSDATE(fsp) — DataFusion's now() is niladic; ignore the fractional-seconds-precision arg.
            RexCall niladic = (RexCall) cluster.getRexBuilder().makeCall(original.getType(), original.getOperator(), List.of());
            return super.adapt(niladic, fieldStorage, cluster);
        }
    }

    static final class CurrentDateAdapter extends AbstractNameMappingAdapter {
        CurrentDateAdapter() {
            super(LOCAL_CURRENT_DATE_OP, List.of(), List.of());
        }
    }

    static final class CurrentTimeAdapter extends AbstractNameMappingAdapter {
        CurrentTimeAdapter() {
            super(LOCAL_CURRENT_TIME_OP, List.of(), List.of());
        }
    }

    /**
     * Cluster F case 7: PPL accepts a datetime-string ({@code '1985-10-09 12:00:00'})
     * for {@code TIME(x)} and {@code cast(x AS TIME)}, returning just the time
     * component. DataFusion's stock {@code to_time} parses {@code HH:MM:SS}
     * formats only — strings with a date prefix throw. We strip the leading
     * {@code YYYY-MM-DD } portion via {@code regexp_replace} for character
     * operands; non-string operands route through unchanged.
     */
    static final class TimeAdapter implements ScalarFunctionAdapter {

        private static final String DATE_PREFIX_PATTERN = "^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ]";

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            List<RexNode> operands = original.getOperands();
            if (operands.size() == 1 && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getType().getSqlTypeName())) {
                RexNode arg = operands.get(0);
                // invalid time literal → typed format-hint exception at plan time
                validateTimeLiteralAcceptingDatetime(arg);
                RexNode pattern = rexBuilder.makeLiteral(DATE_PREFIX_PATTERN);
                RexNode replacement = rexBuilder.makeLiteral("");
                RexNode stripped = rexBuilder.makeCall(
                    arg.getType(),
                    SqlLibraryOperators.REGEXP_REPLACE_3,
                    List.of(arg, pattern, replacement)
                );
                return rexBuilder.makeCall(original.getType(), LOCAL_TIME_OP, List.of(stripped));
            }
            return rexBuilder.makeCall(original.getType(), LOCAL_TIME_OP, operands);
        }

        /** TIME(...) accepts bare time, datetime (date+time), and timestamp strings; rejects bare date. */
        private static void validateTimeLiteralAcceptingDatetime(RexNode operand) {
            if (!(operand instanceof org.apache.calcite.rex.RexLiteral literal)) {
                return;
            }
            String value = literal.getValueAs(String.class);
            if (value == null) {
                return;
            }
            // try datetime first (PPL allows TIME('1985-10-09 12:00:00') → 12:00:00)
            try {
                java.time.LocalDateTime.parse(value.replace(' ', 'T'));
                return;
            } catch (java.time.format.DateTimeParseException ignored) {}
            // bare time
            DatetimeLiteralValidator.validate(value, DatetimeLiteralValidator.Kind.TIME);
        }
    }

    static final class DateAdapter implements ScalarFunctionAdapter {
        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            // invalid date literal → typed format-hint exception at plan time
            if (original.getOperands().size() == 1) {
                RexNode operand = original.getOperands().get(0);
                if (SqlTypeName.CHAR_TYPES.contains(operand.getType().getSqlTypeName())) {
                    DatetimeLiteralValidator.validate(operand, DatetimeLiteralValidator.Kind.DATE);
                }
            }
            return cluster.getRexBuilder().makeCall(original.getType(), LOCAL_DATE_OP, original.getOperands());
        }
    }

    /**
     * Single-arg {@code DATETIME(string)} strips the trailing offset (+HH:MM / -HHMM / Z) so the
     * result is wall-clock, not UTC-converted. Two-arg {@code DATETIME(string, tz)} extracts the
     * source offset from the string (default UTC), then routes through {@code convert_tz} to
     * shift the wall-clock value into {@code tz}.
     *
     * <p>Invalid string literals fold to a NULL TIMESTAMP at plan time so DATETIME(invalid) returns
     * a null row rather than failing the query — matches legacy SQL plugin behavior.
     *
     * <p>This belongs in the PPL frontend ({@code PPLBuiltinOperators.DATETIME}) so every
     * backend inherits the wall-clock semantic; lives here until the frontend owns it.
     */
    static final class DatetimeAdapter implements ScalarFunctionAdapter {

        private static final String OFFSET_SUFFIX_PATTERN = "([+-][0-9]{2}:?[0-9]{2}|Z)$";
        private static final java.util.regex.Pattern OFFSET_SUFFIX_REGEX = java.util.regex.Pattern.compile(OFFSET_SUFFIX_PATTERN);

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            List<RexNode> operands = original.getOperands();
            // 2-arg DATETIME(string, tz) — extract source offset (default UTC), shift via convert_tz.
            // Only handle calls actually named DATETIME; TimestampFunctionAdapter's TIMESTAMP(ts, time)
            // fall-through reuses this adapter and must not be tz-converted.
            if (operands.size() == 2 && "DATETIME".equalsIgnoreCase(original.getOperator().getName())) {
                return adaptTwoArg(original, cluster);
            }
            if (operands.size() == 1 && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getType().getSqlTypeName())) {
                RexNode arg = operands.get(0);
                // invalid literal → fold to NULL TIMESTAMP (matches legacy SQL DATETIME-of-invalid semantics)
                RexNode foldedNull = tryFoldInvalidLiteralToNull(arg, original, cluster);
                if (foldedNull != null) {
                    return foldedNull;
                }
                RexNode pattern = rexBuilder.makeLiteral(OFFSET_SUFFIX_PATTERN);
                RexNode replacement = rexBuilder.makeLiteral("");
                RexNode stripped = rexBuilder.makeCall(
                    arg.getType(),
                    SqlLibraryOperators.REGEXP_REPLACE_3,
                    List.of(arg, pattern, replacement)
                );
                return rexBuilder.makeCall(original.getType(), LOCAL_TO_TIMESTAMP_OP, List.of(stripped));
            }
            return rexBuilder.makeCall(original.getType(), LOCAL_TO_TIMESTAMP_OP, operands);
        }

        /**
         * Cluster B 2-arg DATETIME(s, tz): if both operands are string literals, fold to a typed
         * TIMESTAMP literal at plan time (or NULL on invalid tz / value). Otherwise rewrite to
         * {@code convert_tz(to_timestamp(stripped_s), from_tz_literal_or_UTC, tz)} so the call
         * binds to convert_tz's substrait sig.
         */
        private RexNode adaptTwoArg(RexCall original, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode value = original.getOperands().get(0);
            RexNode tzArg = original.getOperands().get(1);

            // Plan-time fold: literal value, literal tz → typed TIMESTAMP literal or NULL.
            if (value instanceof org.apache.calcite.rex.RexLiteral valLit
                && SqlTypeName.CHAR_TYPES.contains(valLit.getType().getSqlTypeName())
                && tzArg instanceof org.apache.calcite.rex.RexLiteral tzLit
                && SqlTypeName.CHAR_TYPES.contains(tzLit.getType().getSqlTypeName())) {
                return foldTwoArgLiteral(valLit.getValueAs(String.class), tzLit.getValueAs(String.class), original, cluster);
            }

            // Non-literal path: strip source offset (regex over the string column), then convert_tz.
            // from_tz extraction needs the source offset — for column input we approximate with UTC,
            // matching legacy SQL plugin behavior (column-typed DATETIME ignores any embedded offset
            // and treats the string as UTC wall clock before tz conversion).
            String fromTzLiteral = "+00:00";
            RexNode strippedValue;
            if (SqlTypeName.CHAR_TYPES.contains(value.getType().getSqlTypeName())) {
                RexNode pattern = rexBuilder.makeLiteral(OFFSET_SUFFIX_PATTERN);
                RexNode replacement = rexBuilder.makeLiteral("");
                strippedValue = rexBuilder.makeCall(
                    value.getType(),
                    SqlLibraryOperators.REGEXP_REPLACE_3,
                    List.of(value, pattern, replacement)
                );
            } else {
                strippedValue = value;
            }
            RexNode asTimestamp = rexBuilder.makeCall(original.getType(), LOCAL_TO_TIMESTAMP_OP, List.of(strippedValue));
            RexNode fromTz = rexBuilder.makeLiteral(fromTzLiteral);
            return rexBuilder.makeCall(original.getType(), ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, List.of(asTimestamp, fromTz, tzArg));
        }

        /** Plan-time fold of DATETIME(value-literal, tz-literal). Returns a typed TIMESTAMP literal or NULL on invalid input. */
        private static RexNode foldTwoArgLiteral(String value, String tz, RexCall original, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            if (value == null || tz == null) {
                return rexBuilder.makeNullLiteral(original.getType());
            }
            // Extract source offset (default UTC).
            String sourceTz = "+00:00";
            String stripped = value;
            java.util.regex.Matcher m = OFFSET_SUFFIX_REGEX.matcher(value);
            if (m.find()) {
                sourceTz = "Z".equals(m.group()) ? "+00:00" : m.group();
                stripped = value.substring(0, m.start());
            }
            java.time.LocalDateTime ldt;
            try {
                ldt = java.time.LocalDateTime.parse(stripped.replace(' ', 'T'));
            } catch (java.time.format.DateTimeParseException e) {
                return rexBuilder.makeNullLiteral(original.getType());
            }
            // Validate / canonicalize both timezones via convert_tz adapter; either invalid → NULL.
            // Then apply MySQL DATETIME bounds [-13:59, +14:00] — stricter than convert_tz's ±14:59.
            String canonicalFrom;
            String canonicalTo;
            try {
                canonicalFrom = ConvertTzAdapter.canonicalizeTz(sourceTz);
                canonicalTo = ConvertTzAdapter.canonicalizeTz(tz);
            } catch (IllegalArgumentException invalidTz) {
                return rexBuilder.makeNullLiteral(original.getType());
            }
            if (!isWithinMysqlDatetimeTzBounds(canonicalFrom) || !isWithinMysqlDatetimeTzBounds(canonicalTo)) {
                return rexBuilder.makeNullLiteral(original.getType());
            }
            // Use the first valid offset for ldt at canonicalFrom (handles DST gaps unambiguously).
            java.time.ZonedDateTime targetZoned;
            try {
                java.time.ZoneId fromZone = java.time.ZoneId.of(canonicalFrom);
                java.time.ZoneId toZone = java.time.ZoneId.of(canonicalTo);
                java.util.List<java.time.ZoneOffset> validOffsets = fromZone.getRules().getValidOffsets(ldt);
                if (validOffsets.isEmpty()) {
                    return rexBuilder.makeNullLiteral(original.getType());
                }
                targetZoned = java.time.ZonedDateTime.ofInstant(ldt.toInstant(validOffsets.get(0)), toZone);
            } catch (java.time.DateTimeException invalid) {
                return rexBuilder.makeNullLiteral(original.getType());
            }
            java.time.LocalDateTime result = targetZoned.toLocalDateTime();
            org.apache.calcite.util.TimestampString ts = new org.apache.calcite.util.TimestampString(
                result.getYear(),
                result.getMonthValue(),
                result.getDayOfMonth(),
                result.getHour(),
                result.getMinute(),
                result.getSecond()
            );
            int nanos = result.getNano();
            if (nanos > 0) {
                ts = ts.withNanos(nanos);
            }
            int precision = original.getType().getPrecision() < 0 ? 6 : Math.min(original.getType().getPrecision(), 9);
            RexNode literal = rexBuilder.makeTimestampLiteral(ts, precision);
            if (literal.getType().equals(original.getType())) {
                return literal;
            }
            return rexBuilder.makeAbstractCast(original.getType(), literal);
        }

        /** MySQL DATETIME tz bound [-13:59, +14:00]; named zones map to their fixed UTC offset for the bounds check. */
        private static boolean isWithinMysqlDatetimeTzBounds(String canonical) {
            try {
                java.time.ZoneId zone = java.time.ZoneId.of(canonical);
                java.time.ZoneOffset offset = zone.getRules().getOffset(java.time.Instant.now());
                int totalSeconds = offset.getTotalSeconds();
                // [-13:59:00, +14:00:00] inclusive → seconds [-50340, +50400].
                return totalSeconds >= -50340 && totalSeconds <= 50400;
            } catch (java.time.DateTimeException e) {
                return false;
            }
        }

        /** Returns NULL TIMESTAMP literal for any string literal that isn't a strict 'yyyy-MM-dd HH:mm:ss[.fraction]'. */
        private static RexNode tryFoldInvalidLiteralToNull(RexNode operand, RexCall original, RelOptCluster cluster) {
            if (!(operand instanceof org.apache.calcite.rex.RexLiteral literal)) {
                return null;
            }
            String value = literal.getValueAs(String.class);
            if (value == null) {
                return null;
            }
            // strip offset suffix, then require a full datetime (date+time) — bare date / bare time
            // → null, matching legacy SQL DATETIME parse semantics.
            String stripped = value.replaceAll(OFFSET_SUFFIX_PATTERN, "");
            try {
                java.time.LocalDateTime.parse(stripped.replace(' ', 'T'));
                return null;
            } catch (java.time.format.DateTimeParseException ignored) {
                return cluster.getRexBuilder().makeNullLiteral(original.getType());
            }
        }
    }
}
