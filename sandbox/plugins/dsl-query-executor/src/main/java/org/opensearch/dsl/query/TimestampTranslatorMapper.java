/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.query.range.BoundRequest;
import org.opensearch.dsl.query.range.RangeDateParsing;

import java.util.Optional;

/**
 * Translator mapper for TIMESTAMP and DATE fields. Owns both halves of date bound translation:
 * parsing the raw bound value via {@link RangeDateParsing} and building the timestamp literal.
 *
 * <p>Overrides the wide {@link #translateBound(BoundRequest)} because it is the only mapper
 * that needs query-level {@code format} and {@code timeZone}. Mirrors legacy
 * {@code DateFieldType.rangeQuery} which overrides the wide form while
 * {@code SimpleMappedFieldType} strips those params for scalar types.
 *
 * <p>Rounding uses {@link BoundRequest#roundUp()}: lower bounds round up when exclusive,
 * upper bounds round up when inclusive, collapsing the two separate {@code roundUp}
 * computations from the former {@code convert} method into one derived accessor.
 *
 * <p>Stateless singleton; per-field state (precision) is read from the {@code RelDataType}
 * on each call.
 */
final class TimestampTranslatorMapper extends BaseTranslatorMapper {

    /** Singleton instance. */
    static final TimestampTranslatorMapper INSTANCE = new TimestampTranslatorMapper();

    private TimestampTranslatorMapper() {}

    /**
     * Wide entry point: parses the raw bound value as a date and builds the timestamp literal.
     * Delegates parsing to {@link RangeDateParsing#parseDateValue} preserving the rounding rule
     * from legacy {@code DateFieldMapper.dateRangeQuery}.
     */
    @Override
    public RexNode translateBound(BoundRequest r) throws ConversionException {
        Object rawValue = r.value();
        if (rawValue == null) {
            return null;
        }

        RelDataTypeField field = r.field();
        ConversionContext ctx = r.ctx();
        int fieldPrecision = field.getType().getPrecision();

        // Parse the raw value to an epoch long (millis or nanos depending on precision).
        // For non-String values (already numeric), this mirrors processValue which returned them as-is.
        Object parsedValue = processDateValue(rawValue, r.format(), r.timeZone(), r.roundUp(), fieldPrecision);

        // Build the timestamp literal from the parsed long value.
        RexNode literal = createTimestampLiteral(parsedValue, field, ctx);
        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());

        SqlOperator op;
        if (r.isLower()) {
            op = r.inclusive() ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.GREATER_THAN;
        } else {
            op = r.inclusive() ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN;
        }

        return ctx.getRexBuilder().makeCall(op, fieldRef, literal);
    }

    /**
     * Term queries on timestamp/date fields are not yet supported on this path.
     * Legacy {@code DateFieldMapper.termQuery} supports them, but implementing without verified
     * parity would replace a loud crash (ClassCastException from Calcite's checkcast Long on
     * a String) with a possibly silently-wrong answer.
     *
     * @throws ConversionException always, with a clear message for HTTP 400 surfacing
     */
    @Override
    public Optional<RexNode> toTermLiteral(Object value, RelDataTypeField field, ConversionContext ctx) throws ConversionException {
        throw new ConversionException(
            "Term queries on date fields are not yet supported on the DSL conversion path. Field: [" + field.getName() + "]"
        );
    }

    /**
     * Processes a raw bound value for a date/timestamp field. String values are parsed via
     * {@link RangeDateParsing#parseDateValue}; non-String values pass through unchanged
     * (matching former {@code processValue} semantics where non-String returned as-is).
     *
     * @param value the raw bound value from the query
     * @param format optional date format pattern
     * @param timeZone optional timezone ID
     * @param roundUp rounding direction derived from bound inclusivity
     * @param fieldPrecision the precision of the field (3 for millis, 9 for nanos)
     * @return epoch-millis (Long) for precision 3, epoch-nanos (Long) for precision 9,
     *         or the original value if not a String
     * @throws ConversionException if date parsing fails
     */
    private Object processDateValue(Object value, String format, String timeZone, boolean roundUp, int fieldPrecision)
        throws ConversionException {
        if (!(value instanceof String)) {
            return value;
        }

        String strValue = (String) value;

        // If format/timeZone specified or value is date-math, always date-parse.
        // Otherwise, for TIMESTAMP/DATE fields, all strings are date-parsed.
        // Both branches delegate to the same parseDateValue - the gate in the former
        // processValue always took one of these paths for date-typed fields.
        return RangeDateParsing.parseDateValue(strValue, format, timeZone, roundUp, resolutionFor(fieldPrecision));
    }

    /**
     * Creates a timestamp literal from a parsed epoch value. For precision 9 (nanoseconds),
     * builds a {@link TimestampString} with nine fractional digits by splitting into a millis
     * base and nanoOfSecond portion. For precision 3 (milliseconds), uses standard makeLiteral
     * which interprets Long as epoch-millis.
     *
     * <p>The nanosecond path avoids {@code makeLiteral(Long, TIMESTAMP)} which would interpret
     * the Long as millis and overflow.
     *
     * @param value the parsed epoch value (Long for dates, or original non-String value)
     * @param field the field definition from the schema
     * @param ctx the conversion context
     * @return RexNode literal with appropriate type and precision
     */
    private RexNode createTimestampLiteral(Object value, RelDataTypeField field, ConversionContext ctx) {
        int precision = field.getType().getPrecision();
        if (value instanceof Long) {
            long longValue = (Long) value;
            if (isNanoPrecision(precision)) {
                // Nanosecond epoch: build TimestampString with 9 fractional digits to avoid
                // makeLiteral(Long, TIMESTAMP) which interprets the Long as millis and overflows.
                // Split: millis for the date/time base, nanoOfSecond for the fractional portion.
                long epochMillis = longValue / 1_000_000L;
                int nanoOfSecond = (int) (longValue % 1_000_000_000L);
                // Handle negative modulo edge case (should not occur for valid nanos since epoch)
                if (nanoOfSecond < 0) {
                    nanoOfSecond += 1_000_000_000;
                    epochMillis -= 1;
                }
                TimestampString ts = TimestampString.fromMillisSinceEpoch(epochMillis).withNanos(nanoOfSecond);
                return ctx.getRexBuilder().makeTimestampLiteral(ts, precision);
            }
            // Precision 3 (millis): use standard makeLiteral which interprets Long as epoch-millis.
            RelDataType timestampType = ctx.getRexBuilder().getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, precision);
            return ctx.getRexBuilder().makeLiteral(value, timestampType, true);
        }
        // Non-Long value (should not normally occur for date fields after parsing, but preserves safety)
        return ctx.getRexBuilder().makeLiteral(value, field.getType(), true);
    }

    /** OpenSearch maps {@code date} to TIMESTAMP(3) and {@code date_nanos} to TIMESTAMP(9); any precision above 3 is nanosecond resolution. */
    private static boolean isNanoPrecision(int precision) {
        return precision > 3;
    }

    /** Selects the date resolution matching the field precision. */
    private static RangeDateParsing.DateResolution resolutionFor(int precision) {
        return isNanoPrecision(precision) ? RangeDateParsing.DateResolution.NANOSECONDS : RangeDateParsing.DateResolution.MILLISECONDS;
    }
}
