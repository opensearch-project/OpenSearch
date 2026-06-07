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

    static final class NowAdapter extends AbstractNameMappingAdapter {
        NowAdapter() {
            super(LOCAL_NOW_OP, List.of(), List.of());
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
     * result is wall-clock, not UTC-converted. Two-arg {@code DATETIME(string, tz)} and
     * non-string operands route through {@code to_timestamp} unchanged.
     *
     * <p>Invalid string literals fold to a NULL TIMESTAMP at plan time so DATETIME(invalid) returns
     * a null row rather than failing the query — matches legacy SQL plugin behavior.
     *
     * <p>This belongs in the PPL frontend ({@code PPLBuiltinOperators.DATETIME}) so every
     * backend inherits the wall-clock semantic; lives here until the frontend owns it.
     */
    static final class DatetimeAdapter implements ScalarFunctionAdapter {

        private static final String OFFSET_SUFFIX_PATTERN = "([+-][0-9]{2}:?[0-9]{2}|Z)$";

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            List<RexNode> operands = original.getOperands();
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
