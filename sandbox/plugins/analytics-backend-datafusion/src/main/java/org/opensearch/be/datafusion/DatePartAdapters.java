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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Date-part extractor adapters — rewrite {@code FN(ts)} to {@code date_part('<unit>', ts)}.
 * Character-family operands cast to TIMESTAMP so substrait binds the (string, precision_timestamp&lt;P&gt;) sig;
 * TIME and bare-time strings get today-UTC anchored first.
 *
 * @opensearch.internal
 */
final class DatePartAdapters extends AbstractNameMappingAdapter {

    private final String unit;

    DatePartAdapters(String unit) {
        super(SqlLibraryOperators.DATE_PART, List.of(unit), List.of());
        this.unit = unit;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().stream().noneMatch(DatePartAdapters::needsCoercion)) {
            return super.adapt(original, fieldStorage, cluster);
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> coerced = new ArrayList<>(original.getOperands().size());
        for (RexNode operand : original.getOperands()) {
            if (isCharacterOperand(operand)) {
                validateDatetimeLiteralForUnit(operand, unit);
                coerced.add(castToTimestampWithTodayPrefixForBareTime(operand, cluster));
            } else if (operand.getType().getSqlTypeName() == SqlTypeName.TIME) {
                coerced.add(castTimeToTimestamp(operand, cluster));
            } else {
                coerced.add(operand);
            }
        }
        RelDataType unitType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        List<RexNode> args = new ArrayList<>(coerced.size() + 1);
        args.add(rexBuilder.makeLiteral(unit, unitType, true));
        args.addAll(coerced);
        return rexBuilder.makeCall(original.getType(), SqlLibraryOperators.DATE_PART, args);
    }

    /** Operand needs coercion if it is a character or TIME — TIMESTAMP/DATE bind directly. */
    private static boolean needsCoercion(RexNode operand) {
        return isCharacterOperand(operand) || operand.getType().getSqlTypeName() == SqlTypeName.TIME;
    }

    private static boolean isCharacterOperand(RexNode operand) {
        return SqlTypeFamily.CHARACTER.contains(operand.getType());
    }

    private static RexNode castToTimestamp(RexNode operand, RelOptCluster cluster) {
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType timestampType = factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.TIMESTAMP),
            operand.getType().isNullable()
        );
        return cluster.getRexBuilder().makeCast(timestampType, operand);
    }

    /** bare-time string ({@code '17:30:00'}) → CONCAT(today-UTC, ' ', s) → CAST TIMESTAMP. */
    private static RexNode castToTimestampWithTodayPrefixForBareTime(RexNode operand, RelOptCluster cluster) {
        if (!isBareTimeString(operand)) {
            return castToTimestamp(operand, cluster);
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType varchar = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        String prefix = LocalDate.now(java.time.ZoneOffset.UTC).toString() + " ";
        RexNode prefixLit = rexBuilder.makeLiteral(prefix, varchar, false);
        RexNode concat = rexBuilder.makeCall(varchar, org.apache.calcite.sql.fun.SqlStdOperatorTable.CONCAT, List.of(prefixLit, operand));
        return castToTimestamp(concat, cluster);
    }

    /** True for a string {@link RexLiteral} that parses as bare time only (HH:mm:ss[.SSSSSSSSS]). */
    private static boolean isBareTimeString(RexNode operand) {
        if (!(operand instanceof RexLiteral literal)) {
            return false;
        }
        String value = literal.getValueAs(String.class);
        if (value == null) {
            return false;
        }
        try {
            LocalTime.parse(value);
            return true;
        } catch (DateTimeParseException ignored) {
            return false;
        }
    }

    /** TIME → CONCAT(today-UTC, ' ', CAST AS VARCHAR) → CAST AS TIMESTAMP. */
    private static RexNode castTimeToTimestamp(RexNode operand, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType varchar = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RelDataType nullableVarchar = rexBuilder.getTypeFactory().createTypeWithNullability(varchar, operand.getType().isNullable());
        RexNode timeAsVarchar = rexBuilder.makeCast(nullableVarchar, operand);
        String prefix = LocalDate.now(java.time.ZoneOffset.UTC).toString() + " ";
        RexNode prefixLit = rexBuilder.makeLiteral(prefix, varchar, false);
        RexNode concat = rexBuilder.makeCall(
            nullableVarchar,
            org.apache.calcite.sql.fun.SqlStdOperatorTable.CONCAT,
            List.of(prefixLit, timeAsVarchar)
        );
        return castToTimestamp(concat, cluster);
    }

    /** Shared coercion for sibling adapters: TIME → today-anchored TIMESTAMP, VARCHAR → validated CAST. */
    static RexNode coerceCharacterOperandToTimestamp(RexNode operand, RelOptCluster cluster) {
        if (operand.getType().getSqlTypeName() == SqlTypeName.TIME) {
            return castTimeToTimestamp(operand, cluster);
        }
        if (!isCharacterOperand(operand)) {
            return operand;
        }
        validateDatetimeLiteral(operand);
        return castToTimestampWithTodayPrefixForBareTime(operand, cluster);
    }

    /** Time-only units reject bare-date literals (HOUR('2020-08-26') must throw, not return 0). */
    private static final Set<String> TIME_ONLY_UNITS = Set.of("hour", "minute", "second", "microsecond");

    /** Date-part units reject bare-time literals (DAY('12:00:00') must throw, not silent-fail). */
    private static final Set<String> DATE_ONLY_UNITS = Set.of("year", "quarter", "month", "day", "week", "doy", "dow");

    static void validateDatetimeLiteralForUnit(RexNode operand, String unit) {
        if (TIME_ONLY_UNITS.contains(unit)) {
            DatetimeLiteralValidator.validate(operand, DatetimeLiteralValidator.Kind.TIME);
            return;
        }
        if (DATE_ONLY_UNITS.contains(unit)) {
            DatetimeLiteralValidator.validate(operand, DatetimeLiteralValidator.Kind.DATE);
            return;
        }
        validateDatetimeLiteral(operand);
    }

    /** Plan-time validation for string-literal operands; accepts datetime / date / time, rejects garbage. */
    static void validateDatetimeLiteral(RexNode operand) {
        if (!(operand instanceof RexLiteral literal)) {
            return;
        }
        String value = literal.getValueAs(String.class);
        if (value == null) {
            return;
        }
        try {
            LocalDateTime.parse(value.replace(' ', 'T'));
            return;
        } catch (DateTimeParseException ignored) {}

        try {
            LocalDate.parse(value);
            return;
        } catch (DateTimeParseException ignored) {}

        try {
            LocalTime.parse(value);
            return;
        } catch (DateTimeParseException ignored) {
            // causeless — see DatetimeLiteralValidator#fail.
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "timestamp:%s in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'", value)
            );
        }
    }

    static DatePartAdapters year() {
        return new DatePartAdapters("year");
    }

    static DatePartAdapters quarter() {
        return new DatePartAdapters("quarter");
    }

    static DatePartAdapters month() {
        return new DatePartAdapters("month");
    }

    static DatePartAdapters day() {
        return new DatePartAdapters("day");
    }

    static DatePartAdapters dayOfYear() {
        return new DatePartAdapters("doy");
    }

    static DatePartAdapters hour() {
        return new DatePartAdapters("hour");
    }

    static DatePartAdapters minute() {
        return new DatePartAdapters("minute");
    }

    static DatePartAdapters microsecond() {
        return new DatePartAdapters("microsecond");
    }

    static DatePartAdapters week() {
        return new DatePartAdapters("week");
    }
}
