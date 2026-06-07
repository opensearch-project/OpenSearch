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

/**
 * Date-part extractor adapters — rewrite {@code FN(ts)} to {@code date_part('<unit>', ts)}.
 * Alias pairs (e.g. MONTH_OF_YEAR → MONTH) share an adapter instance at registration.
 *
 * <p>Operand coercion: PPL allows bare string-literal datetime arguments
 * (e.g. {@code DAY('2020-09-16')}). Calcite types those operands as
 * {@code VARCHAR}, but {@code opensearch_scalar_functions.yaml} declares
 * {@code date_part} only for {@code (string, precision_timestamp<P>)} and
 * {@code (string, date)}. Without coercion, isthmus' signature matcher fails
 * with {@code "Unable to convert call DATE_PART(string, string)"}. This adapter
 * wraps any character-family operand in {@code CAST(operand AS TIMESTAMP)} so
 * the call resolves to the precision_timestamp impl. DataFusion's CAST kernel
 * parses ISO-8601 datetime strings; malformed inputs surface as a runtime
 * Arrow cast error from the engine.
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
        if (original.getOperands().stream().noneMatch(DatePartAdapters::isCharacterOperand)) {
            return super.adapt(original, fieldStorage, cluster);
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> coerced = new ArrayList<>(original.getOperands().size());
        for (RexNode operand : original.getOperands()) {
            if (isCharacterOperand(operand)) {
                validateDatetimeLiteral(operand);
                coerced.add(castToTimestamp(operand, cluster));
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

    /**
     * Shared coercion helper for sibling adapters ({@link DayOfWeekAdapter}, {@link SecondAdapter})
     * that build {@code date_part('<unit>', operand)} calls directly. Wraps a character-family
     * operand in {@code CAST(_ AS TIMESTAMP)}; non-character operands are returned unchanged.
     * String {@link RexLiteral} operands are eagerly validated to surface the legacy
     * {@code "unsupported format"} error at plan time (see {@link #validateDatetimeLiteral}).
     */
    static RexNode coerceCharacterOperandToTimestamp(RexNode operand, RelOptCluster cluster) {
        if (!isCharacterOperand(operand)) {
            return operand;
        }
        validateDatetimeLiteral(operand);
        return castToTimestamp(operand, cluster);
    }

    /**
     * Eagerly parses string {@link RexLiteral} operands so an invalid literal surfaces as a
     * coordinator-side {@link IllegalArgumentException} during planning, before the value reaches
     * DataFusion's CAST kernel. The native error message ({@code "Arrow error: Parser error: ..."})
     * is dropped by Flight RPC serialization on the worker→coordinator hop, so without this check
     * users see {@code "Failed to start streaming fragment on ..."} instead of the legacy
     * {@code "timestamp:<v> in unsupported format"} wording.
     *
     * <p>Non-literal operands (column refs, expressions) and NULL literals pass through — column
     * value validation is a separate concern (Arrow CAST per-row error handling, tracked
     * separately).
     *
     * <p>Accept-set mirrors legacy {@code DateTimeParser.parse}: try {@link LocalDateTime} (date+time
     * with optional nanos), {@link LocalDate} (bare date), {@link LocalTime} (bare time), throw on
     * all-failed. Same try/catch-fall-through shape used by
     * {@link TimestampFunctionAdapter#parseTimestamp}.
     */
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
            // SQL plugin's ErrorMessageFactory.unwrapCause walks to the deepest cause for the response
            // type/details, so attaching DateTimeParseException as cause would surface its stock JDK
            // message instead of this one. Mirrors legacy DateTimeParser.parse, which throws causeless.
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
