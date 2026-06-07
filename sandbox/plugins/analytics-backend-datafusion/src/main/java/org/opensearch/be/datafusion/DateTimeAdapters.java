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

    /** PPL {@code TIME(string)} accepts a datetime-string; strip the date prefix so {@code to_time} parses. */
    static final class TimeAdapter implements ScalarFunctionAdapter {

        private static final String DATE_PREFIX_PATTERN = "^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ]";

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            List<RexNode> operands = original.getOperands();
            if (operands.size() == 1 && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getType().getSqlTypeName())) {
                RexNode arg = operands.get(0);
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

    static final class DateAdapter extends AbstractNameMappingAdapter {
        DateAdapter() {
            super(LOCAL_DATE_OP, List.of(), List.of());
        }
    }

    /**
     * Single-arg {@code DATETIME(string)} strips the trailing offset (+HH:MM / -HHMM / Z) so the
     * result is wall-clock, not UTC-converted. Two-arg {@code DATETIME(string, tz)} and
     * non-string operands route through {@code to_timestamp} unchanged.
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
    }
}
