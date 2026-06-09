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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.NumericToDoubleAdapter;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Adapters for PPL datetime functions routed to Rust UDFs. Each {@code LOCAL_*_OP}
 * names a Calcite {@link SqlFunction} matching a UDF in {@code rust/src/udf/mod.rs};
 * Substrait sigs live in {@code opensearch_scalar_functions.yaml} +
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS}.
 *
 * @opensearch.internal
 */
final class RustUdfDateTimeAdapters {

    private RustUdfDateTimeAdapters() {}

    private static SqlOperator udf(String name, SqlReturnTypeInference ret, SqlOperandTypeChecker operands) {
        return new SqlFunction(name, SqlKind.OTHER_FUNCTION, ret, null, operands, SqlFunctionCategory.TIMEDATE);
    }

    static final SqlOperator LOCAL_EXTRACT_OP = udf("opensearch_extract", ReturnTypes.BIGINT_NULLABLE, OperandTypes.ANY_ANY);
    static final SqlOperator LOCAL_FROM_UNIXTIME_OP = udf("from_unixtime", ReturnTypes.TIMESTAMP_NULLABLE, OperandTypes.ANY);
    static final SqlOperator LOCAL_MAKETIME_OP = udf(
        "maketime",
        ReturnTypes.TIME_NULLABLE,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY)
    );
    static final SqlOperator LOCAL_MAKEDATE_OP = udf("makedate", ReturnTypes.DATE_NULLABLE, OperandTypes.ANY_ANY);
    static final SqlOperator LOCAL_DATE_FORMAT_OP = udf("date_format", ReturnTypes.VARCHAR_NULLABLE, OperandTypes.ANY_ANY);
    static final SqlOperator LOCAL_TIME_FORMAT_OP = udf("time_format", ReturnTypes.VARCHAR_NULLABLE, OperandTypes.ANY_ANY);
    static final SqlOperator LOCAL_STR_TO_DATE_OP = udf("str_to_date", ReturnTypes.TIMESTAMP_NULLABLE, OperandTypes.ANY_ANY);

    /** WEEK(date [, mode]) target — MySQL semantics, accepts 1- and 2-arg forms. */
    static final SqlOperator LOCAL_OS_WEEK_OP = udf("os_week", ReturnTypes.INTEGER_NULLABLE, OperandTypes.VARIADIC);

    /**
     * Adapter for PPL {@code extract(<unit> FROM <expr>)}.
     * <p>For TIME(p) operands, CASTs to VARCHAR so the call binds to the {@code (string, string)}
     * yaml overload — substrait-java 0.89.1 can't emit a {@code precision_time<P>} signature.
     * <p>When the unit is a known date-part (YEAR/MONTH/DAY/...), the value is anchored to today's
     * UTC date at plan time before the VARCHAR cast — matches SQL plugin's {@code FunctionProperties
     * .getQueryStartClock()} snapshot semantics so date-parts on TIME values yield the current date
     * rather than the 1970 epoch implied by Arrow's TIME representation.
     * Other operand types (DATE / TIMESTAMP / VARCHAR) pass through unchanged.
     */
    static final class ExtractAdapter extends AbstractNameMappingAdapter {

        /** Units that need a date context; pure time-parts (HOUR/MINUTE/...) are absent. */
        private static final Set<String> DATE_PART_UNITS = Set.of(
            "YEAR",
            "MONTH",
            "DAY",
            "WEEK",
            "QUARTER",
            "DOW",
            "DOY",
            "YEAR_MONTH",
            "DAY_HOUR",
            "DAY_MINUTE",
            "DAY_SECOND",
            "DAY_MICROSECOND"
        );

        ExtractAdapter() {
            super(LOCAL_EXTRACT_OP, List.of(), List.of());
        }

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexCall toAdapt = isTimeValueOperand(original) ? rewriteTimeOperand(original, cluster) : original;
            return super.adapt(toAdapt, fieldStorage, cluster);
        }

        private static boolean isTimeValueOperand(RexCall call) {
            return call.getOperands().size() == 2 && call.getOperands().get(1).getType().getSqlTypeName() == SqlTypeName.TIME;
        }

        /**
         * Builds the rewritten call for a TIME value operand. For date-part units, prepends today's
         * UTC date to the time string so Rust parses it as a full timestamp; otherwise just casts
         * TIME→VARCHAR.
         */
        private static RexCall rewriteTimeOperand(RexCall original, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode unit = original.getOperands().get(0);
            RexNode value = original.getOperands().get(1);
            RelDataType varcharType = rexBuilder.getTypeFactory()
                .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), value.getType().isNullable());
            RexNode timeAsVarchar = rexBuilder.makeCast(varcharType, value);
            RexNode rewrittenValue = needsDateContext(unit) ? prependTodayLiteral(rexBuilder, varcharType, timeAsVarchar) : timeAsVarchar;
            return (RexCall) rexBuilder.makeCall(original.getType(), original.getOperator(), List.of(unit, rewrittenValue));
        }

        /** True if the unit is a literal naming a date-part (the only case we can specialize). */
        private static boolean needsDateContext(RexNode unit) {
            if (!(unit instanceof RexLiteral)) {
                return false;
            }
            Object raw = ((RexLiteral) unit).getValue2();
            return raw != null && DATE_PART_UNITS.contains(raw.toString().toUpperCase(Locale.ROOT));
        }

        /** Returns {@code "<today> " || cast(time as varchar)}; today snapshotted at plan time. */
        private static RexNode prependTodayLiteral(RexBuilder rexBuilder, RelDataType varcharType, RexNode timeAsVarchar) {
            String todayPrefix = LocalDate.now(ZoneOffset.UTC).toString() + " ";
            RelDataType nonNullVarchar = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
            RexNode prefix = rexBuilder.makeLiteral(todayPrefix, nonNullVarchar, false);
            return rexBuilder.makeCall(varcharType, SqlStdOperatorTable.CONCAT, List.of(prefix, timeAsVarchar));
        }
    }

    static final class DateFormatAdapter extends AbstractNameMappingAdapter {
        DateFormatAdapter() {
            super(LOCAL_DATE_FORMAT_OP, List.of(), List.of());
        }

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            validateFirstArgIfStringLiteral(original, DatetimeLiteralValidator.Kind.TIMESTAMP);
            return super.adapt(original, fieldStorage, cluster);
        }
    }

    // DAYNAME(x) → date_format(x, '%W'). %W renders full weekday name (e.g. "Monday")
    // via mysql_format::weekday_full — matches legacy PPL DAYNAME semantics.
    static final class DaynameAdapter extends AbstractNameMappingAdapter {
        DaynameAdapter() {
            super(LOCAL_DATE_FORMAT_OP, List.of(), List.of("%W"));
        }
    }

    // MONTHNAME(x) → date_format(x, '%M'). %M renders full month name (e.g. "September").
    static final class MonthnameAdapter extends AbstractNameMappingAdapter {
        MonthnameAdapter() {
            super(LOCAL_DATE_FORMAT_OP, List.of(), List.of("%M"));
        }
    }

    static final class TimeFormatAdapter extends AbstractNameMappingAdapter {
        TimeFormatAdapter() {
            super(LOCAL_TIME_FORMAT_OP, List.of(), List.of());
        }

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            validateFirstArgIfStringLiteral(original, DatetimeLiteralValidator.Kind.TIMESTAMP);
            return super.adapt(original, fieldStorage, cluster);
        }
    }

    /** PPL str_to_date returns NULL on parse failure; invalid month/day values still need to error rather than silent-null. */
    static final class StrToDateAdapter extends AbstractNameMappingAdapter {
        StrToDateAdapter() {
            super(LOCAL_STR_TO_DATE_OP, List.of(), List.of());
        }

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            rejectOutOfRangeMonthOrDay(original);
            return super.adapt(original, fieldStorage, cluster);
        }
    }

    private static void validateFirstArgIfStringLiteral(RexCall original, DatetimeLiteralValidator.Kind kind) {
        if (original.getOperands().isEmpty()) {
            return;
        }
        RexNode firstArg = original.getOperands().get(0);
        if (!SqlTypeFamily.CHARACTER.contains(firstArg.getType())) {
            return;
        }
        DatetimeLiteralValidator.validate(firstArg, kind);
    }

    private static final java.util.regex.Pattern FORMAT_TOKEN = java.util.regex.Pattern.compile("%[a-zA-Z]");
    private static final java.util.regex.Pattern VALUE_NUMBER = java.util.regex.Pattern.compile("\\d+");

    /** Mirrors legacy DateTimeFormatterUtil: reject %m/%c values outside 1..12 and %d/%e values outside 1..31. */
    private static void rejectOutOfRangeMonthOrDay(RexCall original) {
        if (original.getOperands().size() != 2) {
            return;
        }
        if (!(original.getOperands().get(0) instanceof RexLiteral valueLit)
            || !(original.getOperands().get(1) instanceof RexLiteral fmtLit)) {
            return;
        }
        String value = valueLit.getValueAs(String.class);
        String format = fmtLit.getValueAs(String.class);
        if (value == null || format == null) {
            return;
        }
        java.util.regex.Matcher fmtMatcher = FORMAT_TOKEN.matcher(format);
        java.util.regex.Matcher valMatcher = VALUE_NUMBER.matcher(value);
        while (fmtMatcher.find() && valMatcher.find()) {
            String token = fmtMatcher.group();
            int n;
            try {
                n = Integer.parseInt(valMatcher.group());
            } catch (NumberFormatException ignored) {
                continue;
            }
            if (("%m".equals(token) || "%c".equals(token)) && (n < 1 || n > 12)) {
                throw outOfRange("month", n, 1, 12, value, format);
            }
            if (("%d".equals(token) || "%e".equals(token)) && (n < 1 || n > 31)) {
                throw outOfRange("day", n, 1, 31, value, format);
            }
        }
    }

    private static IllegalArgumentException outOfRange(String component, int n, int lo, int hi, String value, String format) {
        return new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "%s value %d is out of range (%d..%d) in str_to_date('%s', '%s')",
                component,
                n,
                lo,
                hi,
                value,
                format
            )
        );
    }

    /**
     * Rewrites WEEK(date [, mode]) to os_week(...). String operands cast to TIMESTAMP because the
     * substrait resolver rejects VARCHAR for the YAML date/timestamp impls.
     */
    static final class OsWeekAdapter extends AbstractNameMappingAdapter {
        OsWeekAdapter() {
            super(LOCAL_OS_WEEK_OP, List.of(), List.of());
        }

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            List<RexNode> originalOperands = original.getOperands();
            if (originalOperands.isEmpty() || !SqlTypeFamily.CHARACTER.contains(originalOperands.get(0).getType())) {
                return super.adapt(original, fieldStorage, cluster);
            }
            RexBuilder rexBuilder = cluster.getRexBuilder();
            DatePartAdapters.validateDatetimeLiteral(originalOperands.get(0));
            RelDataType timestampType = rexBuilder.getTypeFactory()
                .createTypeWithNullability(
                    rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP),
                    originalOperands.get(0).getType().isNullable()
                );
            List<RexNode> coerced = new java.util.ArrayList<>(originalOperands.size());
            coerced.add(rexBuilder.makeCast(timestampType, originalOperands.get(0)));
            for (int i = 1; i < originalOperands.size(); i++) {
                coerced.add(originalOperands.get(i));
            }
            return rexBuilder.makeCall(original.getType(), LOCAL_OS_WEEK_OP, coerced);
        }
    }

    /** PPL FROM_UNIXTIME(epoch [, format]); 2-arg → date_format(from_unixtime(epoch), format). */
    static final class FromUnixtimeAdapter extends NumericToDoubleAdapter {
        FromUnixtimeAdapter() {
            super(LOCAL_FROM_UNIXTIME_OP);
        }

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            if (original.getOperands().size() != 2) {
                return super.adapt(original, fieldStorage, cluster);
            }
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode epoch = NumericToDoubleAdapter.widenToDoubleIfNumeric(original.getOperands().get(0), cluster);
            RexNode format = original.getOperands().get(1);
            RelDataType tsType = rexBuilder.getTypeFactory()
                .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP), true);
            RexNode ts = rexBuilder.makeCall(tsType, LOCAL_FROM_UNIXTIME_OP, List.of(epoch));
            return rexBuilder.makeCall(original.getType(), LOCAL_DATE_FORMAT_OP, List.of(ts, format));
        }
    }

    static final class MaketimeAdapter extends NumericToDoubleAdapter {
        MaketimeAdapter() {
            super(LOCAL_MAKETIME_OP);
        }
    }

    static final class MakedateAdapter extends NumericToDoubleAdapter {
        MakedateAdapter() {
            super(LOCAL_MAKEDATE_OP);
        }
    }
}
