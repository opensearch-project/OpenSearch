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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;

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

    static final class TimeAdapter extends AbstractNameMappingAdapter {
        TimeAdapter() {
            super(LOCAL_TIME_OP, List.of(), List.of());
        }
    }

    static final class DateAdapter extends AbstractNameMappingAdapter {
        DateAdapter() {
            super(LOCAL_DATE_OP, List.of(), List.of());
        }
    }

    /**
     * PPL {@code DATETIME(expr)} / {@code DATETIME(expr, tz)}.
     *
     * <p>1-arg: name-maps to DataFusion's builtin {@code to_timestamp(value)}. The
     * single-arg signature is YAML-declared (string / varchar / date /
     * precision_timestamp).
     *
     * <p>2-arg ({@code DATETIME('<lit>', '<tz>')}): folds at plan time when both
     * operands are VARCHAR literals. The first arg is parsed as a timestamp,
     * extracting any embedded TZ offset; if the parsed string has no TZ, we treat
     * it as UTC (matching the analytics-engine cluster's UTC default — the plugin
     * doesn't expose {@code FunctionProperties.getCurrentZoneId()} at the adapter
     * level). The instant is then converted to the target TZ and emitted as a
     * TIMESTAMP literal at the call's declared precision.
     *
     * <p>The 2-arg column case (e.g. {@code DATETIME(<varchar_col>, '<tz>')}) is
     * not yet supported — needs a Rust UDF; left as a follow-up.
     */
    static final class DatetimeAdapter extends AbstractNameMappingAdapter {
        DatetimeAdapter() {
            super(LOCAL_TO_TIMESTAMP_OP, List.of(), List.of());
        }

        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            if (original.getOperands().size() == 2) {
                RexNode folded = tryFoldTwoArgLiteral(original, cluster);
                if (folded != null) {
                    return folded;
                }
                // 2-arg with non-literal operand: fall through. The single-arg
                // YAML signature won't match, so isthmus will surface a clear
                // "Unable to convert call" — better than silent wrong answers.
                // Returning the original call leaves it for isthmus to handle.
                return original;
            }
            return super.adapt(original, fieldStorage, cluster);
        }
    }

    /**
     * Fold 2-arg {@code DATETIME('<datetime>', '<tz>')} when both args are VARCHAR
     * literals. Returns null when the call shape doesn't match the literal-literal
     * fold; caller falls through.
     */
    private static RexNode tryFoldTwoArgLiteral(RexCall original, RelOptCluster cluster) {
        RexNode op0 = original.getOperands().get(0);
        RexNode op1 = original.getOperands().get(1);
        if (!(op0 instanceof RexLiteral lit0) || lit0.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            return null;
        }
        if (!(op1 instanceof RexLiteral lit1) || lit1.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            return null;
        }
        String tsValue = lit0.getValueAs(String.class);
        String tzValue = lit1.getValueAs(String.class);
        if (tsValue == null || tzValue == null) {
            return null;
        }

        ZoneId targetZone;
        try {
            targetZone = ZoneId.of(tzValue);
        } catch (java.time.DateTimeException e) {
            return null;
        }

        ZonedDateTime targetZdt = parseAsZoned(tsValue, targetZone);
        if (targetZdt == null) {
            return null;
        }

        LocalDateTime ldt = targetZdt.toLocalDateTime();
        TimestampString tsStr = new TimestampString(
            ldt.getYear(),
            ldt.getMonthValue(),
            ldt.getDayOfMonth(),
            ldt.getHour(),
            ldt.getMinute(),
            ldt.getSecond()
        );
        if (ldt.getNano() > 0) {
            tsStr = tsStr.withNanos(ldt.getNano());
        }

        // Use precision 3 (millisecond) — sufficient for most cases and avoids
        // i64-ns overflow at far-future timestamps. Mirrors TimestampFunctionAdapter.
        int precision = 3;
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode literal = rexBuilder.makeTimestampLiteral(tsStr, precision);
        if (!literal.getType().equals(original.getType())) {
            literal = rexBuilder.makeAbstractCast(original.getType(), literal);
        }
        return literal;
    }

    /**
     * Parse a timestamp string into a ZonedDateTime in {@code targetZone}.
     *
     * <p>Three input formats are handled in order:
     * <ol>
     *   <li>{@code yyyy-MM-dd[ T]HH:mm:ss[.frac]±HH:mm} — embedded offset</li>
     *   <li>ISO {@code yyyy-MM-ddTHH:mm:ss[.frac]Z} or with offset</li>
     *   <li>{@code yyyy-MM-dd HH:mm:ss[.frac]} — naive, treated as UTC</li>
     * </ol>
     *
     * <p>Returns null if no format matches.
     */
    private static ZonedDateTime parseAsZoned(String input, ZoneId targetZone) {
        // Try with offset embedded.
        try {
            OffsetDateTime odt = OffsetDateTime.parse(input);
            return odt.atZoneSameInstant(targetZone);
        } catch (DateTimeParseException ignored) {}

        // Try with space separator + offset (yyyy-MM-dd HH:mm:ss±HH:mm).
        if (input.contains(" ") && !input.contains("T")) {
            try {
                OffsetDateTime odt = OffsetDateTime.parse(input.replace(' ', 'T'));
                return odt.atZoneSameInstant(targetZone);
            } catch (DateTimeParseException ignored) {}
        }

        // Naive (no offset) — treat as UTC, then convert to target zone.
        try {
            LocalDateTime ldt = LocalDateTime.parse(input, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT));
            return ldt.toInstant(java.time.ZoneOffset.UTC).atZone(targetZone);
        } catch (DateTimeParseException ignored) {}

        try {
            LocalDateTime ldt = LocalDateTime.parse(input);
            return ldt.toInstant(java.time.ZoneOffset.UTC).atZone(targetZone);
        } catch (DateTimeParseException ignored) {}

        return null;
    }
}
