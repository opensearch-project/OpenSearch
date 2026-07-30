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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
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

    /** 2-arg {@code date_trunc(granularity, ts)}; engine folds with {@code now()} at its plan time. */
    static final SqlOperator LOCAL_DATE_TRUNC_OP = new SqlFunction(
        "date_trunc",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG1_NULLABLE,
        null,
        OperandTypes.family(org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER, org.apache.calcite.sql.type.SqlTypeFamily.TIMESTAMP),
        SqlFunctionCategory.TIMEDATE
    );

    /** PPL {@code now()} / {@code sysdate([fsp])}: drop FSP arg — DataFusion {@code now()} is niladic. */
    static final class NowAdapter extends AbstractNameMappingAdapter {
        NowAdapter() {
            super(LOCAL_NOW_OP, List.of(), List.of());
        }

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            if (original.getOperands().isEmpty()) {
                return super.adapt(original, fieldStorage, cluster);
            }
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

    /** PPL {@code TIME(string)} accepts a datetime-string; strip the date prefix so {@code to_time} parses. */
    static final class TimeAdapter implements ScalarFunctionAdapter {

        private static final String DATE_PREFIX_PATTERN = "^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ]";

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            List<RexNode> operands = original.getOperands();
            if (operands.size() == 1 && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getType().getSqlTypeName())) {
                RexNode arg = operands.get(0);
                DatetimeLiteralValidator.validate(arg, DatetimeLiteralValidator.Kind.TIME);
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
    }

    static final class DateAdapter implements ScalarFunctionAdapter {
        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
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
     * 1-arg DATETIME(string): strip trailing offset, fold invalid literals to NULL.
     * 2-arg DATETIME(string, tz): extract source offset (default UTC), route through convert_tz.
     */
    static final class DatetimeAdapter implements ScalarFunctionAdapter {

        private static final String OFFSET_SUFFIX_PATTERN = "([+-][0-9]{2}:?[0-9]{2}|Z)$";
        private static final java.util.regex.Pattern OFFSET_SUFFIX_REGEX = java.util.regex.Pattern.compile(OFFSET_SUFFIX_PATTERN);

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            List<RexNode> operands = original.getOperands();
            // 2-arg DATETIME(s, tz); guard the name so TIMESTAMP(ts, time) fall-through stays 1-arg
            if (operands.size() == 2 && "DATETIME".equalsIgnoreCase(original.getOperator().getName())) {
                return adaptTwoArg(original, cluster);
            }
            if (operands.size() == 1 && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getType().getSqlTypeName())) {
                RexNode arg = operands.get(0);
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
            // 1-arg DATETIME with a TIME operand — Calcite emits stock TIME as substrait
            // precision_time<P>, which has no to_timestamp yaml binding. Anchor to a TIMESTAMP
            // first via the shared coercion path so isthmus binds the precision_timestamp<P> arm.
            // Same workaround TimeFormatAdapter / DatePartAdapters use.
            if (operands.size() == 1 && operands.get(0).getType().getSqlTypeName() == SqlTypeName.TIME) {
                RexNode anchored = DatePartAdapters.coerceCharacterOperandToTimestamp(operands.get(0), cluster);
                return rexBuilder.makeCall(original.getType(), LOCAL_TO_TIMESTAMP_OP, List.of(anchored));
            }
            return rexBuilder.makeCall(original.getType(), LOCAL_TO_TIMESTAMP_OP, operands);
        }

        /** 2-arg DATETIME(s, tz): all-literal -> typed TIMESTAMP (or NULL); else rewrite to convert_tz. */
        private RexNode adaptTwoArg(RexCall original, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode value = original.getOperands().get(0);
            RexNode tzArg = original.getOperands().get(1);

            // literal/literal → typed TIMESTAMP literal (or NULL)
            if (value instanceof org.apache.calcite.rex.RexLiteral valLit
                && SqlTypeName.CHAR_TYPES.contains(valLit.getType().getSqlTypeName())
                && tzArg instanceof org.apache.calcite.rex.RexLiteral tzLit
                && SqlTypeName.CHAR_TYPES.contains(tzLit.getType().getSqlTypeName())) {
                return foldTwoArgLiteral(valLit.getValueAs(String.class), tzLit.getValueAs(String.class), original, cluster);
            }

            // Column input with no embedded TZ in the value: tz arg is the SOURCE zone (MySQL
            // semantics — value is interpreted AT the given tz, then expressed in UTC). The legacy
            // SQL plugin path treated tz as target, which inverted the offset direction.
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
            // convert_tz yaml binds tz operands as unbounded `string`; cast both away from CHAR(N).
            RelDataType varchar = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
            RexNode tzArgVarchar = SqlTypeName.CHAR_TYPES.contains(tzArg.getType().getSqlTypeName())
                ? rexBuilder.makeCast(varchar, tzArg, true)
                : tzArg;
            RexNode toTz = rexBuilder.makeLiteral("+00:00", varchar, true);
            return rexBuilder.makeCall(original.getType(), ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, List.of(asTimestamp, tzArgVarchar, toTz));
        }

        /** Plan-time fold of DATETIME(value-literal, tz-literal). Returns a typed TIMESTAMP literal or NULL on invalid input. */
        private static RexNode foldTwoArgLiteral(String value, String tz, RexCall original, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            if (value == null || tz == null) {
                return rexBuilder.makeNullLiteral(original.getType());
            }
            // MySQL DATETIME semantics: when value has an embedded offset, that's the source zone
            // and the user's tz arg is the target. When value has NO embedded offset, the user's tz
            // arg is the SOURCE (value is interpreted AT that tz) and the target is UTC.
            String sourceTz;
            String targetTz;
            String stripped = value;
            java.util.regex.Matcher m = OFFSET_SUFFIX_REGEX.matcher(value);
            if (m.find()) {
                sourceTz = "Z".equals(m.group()) ? "+00:00" : m.group();
                targetTz = tz;
                stripped = value.substring(0, m.start());
            } else {
                sourceTz = tz;
                targetTz = "+00:00";
            }
            java.time.LocalDateTime ldt;
            try {
                ldt = java.time.LocalDateTime.parse(stripped.replace(' ', 'T'));
            } catch (java.time.format.DateTimeParseException e) {
                return rexBuilder.makeNullLiteral(original.getType());
            }
            // canonicalize tzs (invalid → NULL); enforce MySQL DATETIME bound [-13:59, +14:00]
            String canonicalFrom;
            String canonicalTo;
            try {
                canonicalFrom = ConvertTzAdapter.canonicalizeTz(sourceTz);
                canonicalTo = ConvertTzAdapter.canonicalizeTz(targetTz);
            } catch (IllegalArgumentException invalidTz) {
                return rexBuilder.makeNullLiteral(original.getType());
            }
            if (!isWithinMysqlDatetimeTzBounds(canonicalFrom) || !isWithinMysqlDatetimeTzBounds(canonicalTo)) {
                return rexBuilder.makeNullLiteral(original.getType());
            }
            // first valid offset at canonicalFrom — DST gap is treated as NULL
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

        /** MySQL DATETIME tz bound [-13:59, +14:00] in seconds = [-50340, +50400]. */
        private static boolean isWithinMysqlDatetimeTzBounds(String canonical) {
            try {
                java.time.ZoneId zone = java.time.ZoneId.of(canonical);
                int totalSeconds = zone.getRules().getOffset(java.time.Instant.now()).getTotalSeconds();
                return totalSeconds >= -50340 && totalSeconds <= 50400;
            } catch (java.time.DateTimeException e) {
                return false;
            }
        }

        /** Non-full-datetime string literal -> NULL TIMESTAMP. */
        private static RexNode tryFoldInvalidLiteralToNull(RexNode operand, RexCall original, RelOptCluster cluster) {
            if (!(operand instanceof RexLiteral literal)) {
                return null;
            }
            String value = literal.getValueAs(String.class);
            if (value == null) {
                return null;
            }
            String stripped = value.replaceAll(OFFSET_SUFFIX_PATTERN, "");
            try {
                LocalDateTime.parse(stripped.replace(' ', 'T'));
                return null;
            } catch (DateTimeParseException ignored) {
                return cluster.getRexBuilder().makeNullLiteral(original.getType());
            }
        }
    }
}
