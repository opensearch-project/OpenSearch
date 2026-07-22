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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link RangeQueryBuilder} to Calcite comparison RexNodes.
 * Supports gte, gt, lte, lt operators, format, time_zone, and relation parameters.
 * Implements date math expressions, inclusivity-keyed rounding, and millisecond precision.
 * <p>
 * Date rounding follows legacy {@code DateFieldMapper.dateRangeQuery}: lower bound parsed with
 * roundUp=!includeLower; upper bound parsed with roundUp=includeUpper. The DateMathParser itself
 * handles explicit rounding operators (/d etc.) using the roundUp flag.
 * <p>
 * Unmapped fields return literal false (no matches), matching legacy behavior where
 * null fieldType yields DISJOINT relation and MatchNoneQueryBuilder.
 * <p>
 * No-bounds queries return IS_NOT_NULL (exists semantics), matching legacy
 * RangeQueryBuilder.doToQuery which rewrites to ExistsQueryBuilder.
 * <p>
 * Decimal bounds on integer-typed fields are truncated and adjusted per legacy
 * {@code NumberFieldMapper.NumberType.INTEGER.rangeQuery} semantics.
 */
public class RangeQueryTranslator implements QueryTranslator {

    /**
     * Returns the query type this translator handles.
     *
     * @return RangeQueryBuilder.class
     */
    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return RangeQueryBuilder.class;
    }

    /**
     * Converts a RangeQueryBuilder to a Calcite RexNode expression.
     * <p>
     * Handles:
     * - Numeric and date range comparisons (gte, gt, lte, lt)
     * - Date format conversion (format parameter)
     * - Timezone handling (time_zone parameter, defaults to UTC)
     * - Date math expressions (now-7d, now/d, etc.)
     * - Inclusivity-keyed rounding per legacy DateFieldMapper.dateRangeQuery
     * - Millisecond precision timestamps (TIMESTAMP(3))
     * - Decimal-to-integer truncation per legacy NumberFieldMapper INTEGER.rangeQuery
     * - Unmapped fields return literal false (match-none)
     * - No-bounds queries return IS_NOT_NULL (exists semantics)
     * - Rejection of unsupported _name (queryName) parameter
     * <p>
     * Relation handling: per legacy SimpleMappedFieldType behavior, the relation parameter is
     * silently ignored for scalar fields (INTERSECTS, CONTAINS, WITHIN all produce identical
     * queries). DISJOINT is rejected, though RangeQueryBuilder itself rejects it at builder
     * level before our translator is invoked.
     * <p>
     * Field-type gating for string values (when no format/timeZone/date-math indicators):
     * - TIMESTAMP/DATE fields: parsed through DateMathParser
     * - Numeric fields: coerced to number using the field's type
     * - VARCHAR/CHAR fields: kept as-is for lexicographic comparison
     *
     * @param query the RangeQueryBuilder to convert
     * @param ctx the conversion context containing schema and RexBuilder
     * @return RexNode representing the range comparison(s)
     * @throws ConversionException if boost specified, queryName specified, or DISJOINT relation
     */
    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        RangeQueryBuilder rangeQuery = (RangeQueryBuilder) query;
        String fieldName = rangeQuery.fieldName();

        if (rangeQuery.boost() != 1.0f) {
            throw new ConversionException("Range query 'boost' parameter is not supported");
        }

        if (rangeQuery.queryName() != null) {
            throw new ConversionException("Range query '_name' parameter is not supported");
        }

        // Relation check: per legacy SimpleMappedFieldType.rangeQuery() and DateFieldType.rangeQuery(),
        // DISJOINT is rejected; INTERSECTS/CONTAINS/WITHIN are silently ignored (scalar fields produce
        // identical queries regardless of relation). Note: RangeQueryBuilder itself rejects DISJOINT
        // at builder level, so this is a defensive check.
        if (rangeQuery.relation() != null && rangeQuery.relation() == ShapeRelation.DISJOINT) {
            throw new ConversionException("Range query 'relation' parameter does not support DISJOINT");
        }

        // Unmapped field -> match-none (literal false), matching legacy DISJOINT for null fieldType.
        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            return ctx.getRexBuilder().makeLiteral(false);
        }

        // Guard: ip and binary fields are not supported for range queries.
        // Legacy IpFieldMapper.rangeQuery relies on InetAddress-order comparison; lexicographic
        // string comparison would produce incorrect results. Legacy BinaryFieldMapper has no
        // rangeQuery implementation at all.
        if (field.getType().getSqlTypeName() == SqlTypeName.VARBINARY) {
            throw new ConversionException("Range queries on ip and binary fields are not supported by the DSL conversion path");
        }

        // Guard: nanosecond-precision date fields (date_nanos mapped to TIMESTAMP(9)) are not
        // yet supported. The translator builds TIMESTAMP(3) literals which would silently truncate
        // nanosecond precision (see DateFieldMapper.Resolution.NANOSECONDS in legacy).
        if (isDateType(field.getType().getSqlTypeName()) && field.getType().getPrecision() > 3) {
            throw new ConversionException("Nanosecond-precision date fields (date_nanos) are not yet supported by the DSL conversion path");
        }

        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        List<RexNode> conditions = new ArrayList<>();

        SqlTypeName fieldTypeName = field.getType().getSqlTypeName();
        String format = rangeQuery.format();
        String timeZone = rangeQuery.timeZone();

        // Rounding keyed on bound inclusivity, matching DateFieldMapper.dateRangeQuery:
        // lower bound parsed with roundUp=!includeLower; upper bound parsed with roundUp=includeUpper.
        Object from = processValue(rangeQuery.from(), format, timeZone, !rangeQuery.includeLower(), fieldTypeName);
        if (from != null) {
            // Decimal bounds on integer fields per NumberFieldMapper INTEGER.rangeQuery:
            // truncate to int and adjust: positive decimal lower bound -> increment; negative -> no adjust.
            Object adjustedFrom = from;
            boolean fromInclusive = rangeQuery.includeLower();
            if (isIntegerType(fieldTypeName) && hasDecimalPart(from)) {
                long truncated = toLongValue(from);
                if (signum(from) > 0) {
                    // Overflow guard: if truncated == type MAX, ++l would overflow -> match-none
                    if (truncated >= getMaxValueForType(fieldTypeName)) {
                        return ctx.getRexBuilder().makeLiteral(false);
                    }
                    adjustedFrom = narrowToFieldType(truncated + 1, fieldTypeName);
                } else {
                    adjustedFrom = narrowToFieldType(truncated, fieldTypeName);
                }
                fromInclusive = true; // decimal adjustment makes bound inclusive
            } else if (isIntegerType(fieldTypeName) && !hasDecimalPart(from) && from instanceof Number) {
                // Whole numeric value on integer field: narrow to field-appropriate type for Calcite
                adjustedFrom = narrowToFieldType(((Number) from).longValue(), fieldTypeName);
            }
            RexNode fromLiteral = createLiteral(adjustedFrom, field, ctx, fieldTypeName);
            conditions.add(
                ctx.getRexBuilder()
                    .makeCall(
                        fromInclusive ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.GREATER_THAN,
                        fieldRef,
                        fromLiteral
                    )
            );
        }

        Object to = processValue(rangeQuery.to(), format, timeZone, rangeQuery.includeUpper(), fieldTypeName);
        if (to != null) {
            // Decimal bounds on integer fields per NumberFieldMapper INTEGER.rangeQuery:
            // truncate to int and adjust: negative decimal upper bound -> decrement; positive -> no adjust.
            Object adjustedTo = to;
            boolean toInclusive = rangeQuery.includeUpper();
            if (isIntegerType(fieldTypeName) && hasDecimalPart(to)) {
                long truncated = toLongValue(to);
                if (signum(to) < 0) {
                    // Overflow guard: if truncated == type MIN, --u would overflow -> match-none
                    if (truncated <= getMinValueForType(fieldTypeName)) {
                        return ctx.getRexBuilder().makeLiteral(false);
                    }
                    adjustedTo = narrowToFieldType(truncated - 1, fieldTypeName);
                } else {
                    adjustedTo = narrowToFieldType(truncated, fieldTypeName);
                }
                toInclusive = true; // decimal adjustment makes bound inclusive
            } else if (isIntegerType(fieldTypeName) && !hasDecimalPart(to) && to instanceof Number) {
                // Whole numeric value on integer field: narrow to field-appropriate type for Calcite
                adjustedTo = narrowToFieldType(((Number) to).longValue(), fieldTypeName);
            }
            RexNode toLiteral = createLiteral(adjustedTo, field, ctx, fieldTypeName);
            conditions.add(
                ctx.getRexBuilder()
                    .makeCall(toInclusive ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN, fieldRef, toLiteral)
            );
        }

        // No bounds -> IS_NOT_NULL (exists semantics), matching legacy RangeQueryBuilder.doToQuery
        // which rewrites to ExistsQueryBuilder when both from and to are null.
        if (conditions.isEmpty()) {
            return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.IS_NOT_NULL, fieldRef);
        }

        RexNode result = conditions.size() == 1 ? conditions.get(0) : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, conditions);

        return result;
    }

    /**
     * Creates a literal RexNode with appropriate type based on the field type and value.
     * <p>
     * For Long values on TIMESTAMP/DATE fields, creates a TIMESTAMP(3) type to preserve
     * millisecond precision. For Long values on non-date fields, uses the field's original type.
     * For CoercedNumber values (from string-to-number coercion), uses makeLiteral with the
     * field's type; Calcite canonically types exact-numeric literals as DECIMAL, which is
     * semantically equivalent for comparisons.
     * For other types, uses the field's original type.
     *
     * @param value the value to create a literal for
     * @param field the field definition from the schema
     * @param ctx the conversion context
     * @param fieldTypeName the SqlTypeName of the field
     * @return RexNode literal with appropriate type and precision
     */
    private RexNode createLiteral(Object value, RelDataTypeField field, ConversionContext ctx, SqlTypeName fieldTypeName) {
        if (value instanceof Long && isDateType(fieldTypeName)) {
            RelDataType timestampType = ctx.getRexBuilder().getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 3);
            return ctx.getRexBuilder().makeLiteral(value, timestampType, true);
        }
        if (value instanceof CoercedNumber) {
            // For string-coerced numbers, use Calcite's standard makeLiteral.
            // Calcite canonically types exact-numeric literals as DECIMAL which is correct.
            Number num = ((CoercedNumber) value).value;
            return ctx.getRexBuilder().makeLiteral(num, field.getType(), true);
        }
        return ctx.getRexBuilder().makeLiteral(value, field.getType(), true);
    }

    /**
     * Processes a range query value, with behavior gated on context and field type.
     * <p>
     * Processing logic:
     * - If format or timeZone is specified, or the string is a date-math expression
     *   (starts with "now" or contains "||"), it is always parsed as a date value.
     * - Otherwise, for TIMESTAMP/DATE fields: string values are parsed through DateMathParser.
     * - For numeric fields (INTEGER, BIGINT, DOUBLE, etc.): string values are coerced to numbers.
     * - For VARCHAR/CHAR fields: string values are kept as-is for lexicographic comparison.
     * <p>
     * Non-string values are returned as-is regardless of field type.
     * <p>
     * For epoch_millis format, timezone is ignored since epoch is absolute.
     *
     * @param value the value to process (can be String, Long, or other types)
     * @param format optional date format pattern (e.g., "dd/MM/yyyy")
     * @param timeZone optional timezone ID (e.g., "America/New_York", defaults to "UTC")
     * @param roundUp whether to round up to end of time unit (true) or down to start (false)
     * @param fieldTypeName the SqlTypeName of the target field
     * @return processed value as epoch milliseconds (Long) for dates, CoercedNumber for string-to-number, or original value
     * @throws ConversionException if date parsing fails or numeric coercion fails
     */
    private Object processValue(Object value, String format, String timeZone, boolean roundUp, SqlTypeName fieldTypeName)
        throws ConversionException {
        if (value == null) {
            return null;
        }

        if (!(value instanceof String)) {
            return value;
        }

        String strValue = (String) value;

        // If format/timeZone specified or value is date-math, always date-parse
        if (format != null || timeZone != null || isDateMathExpression(strValue)) {
            return parseDateValue(strValue, format, timeZone, roundUp);
        }

        // Gate on field type
        if (isDateType(fieldTypeName)) {
            return parseDateValue(strValue, null, null, roundUp);
        } else if (isNumericType(fieldTypeName)) {
            return new CoercedNumber(coerceToNumber(strValue, fieldTypeName));
        } else {
            // VARCHAR/CHAR and other types: keep string as-is for lexicographic comparison
            return strValue;
        }
    }

    /**
     * Determines if a string value is a date-math expression.
     * Date-math expressions start with "now" or contain "||" (anchored date-math).
     */
    private boolean isDateMathExpression(String value) {
        return value.startsWith("now") || value.contains("||");
    }

    /**
     * Parses a string value as a date using DateMathParser.
     * Handles epoch_millis format specially: timezone is ignored since epoch is absolute.
     */
    private Long parseDateValue(String strValue, String format, String timeZone, boolean roundUp) throws ConversionException {
        try {
            if ("epoch_millis".equals(format)) {
                // epoch_millis: parse as raw long, timezone is irrelevant (epoch is absolute)
                try {
                    return Long.parseLong(strValue);
                } catch (NumberFormatException e) {
                    throw new ConversionException("Failed to parse epoch_millis value '" + strValue + "': not a valid number");
                }
            }

            DateFormatter formatter = format != null
                ? DateFormatter.forPattern(format)
                : DateFormatter.forPattern("strict_date_optional_time");
            ZoneId zoneId = timeZone != null ? ZoneId.of(timeZone) : ZoneId.of("UTC");

            return formatter.toDateMathParser().parse(strValue, System::currentTimeMillis, roundUp, zoneId).toEpochMilli();
        } catch (ConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new ConversionException("Failed to parse date value '" + strValue + "': " + e.getMessage());
        }
    }

    /**
     * Coerces a string value to a numeric type matching the field.
     * For integer-family types, if the string contains a decimal point, it is parsed as a
     * Double and wrapped in a CoercedNumber so the decimal-adjust logic in convert() applies.
     */
    private Number coerceToNumber(String strValue, SqlTypeName fieldTypeName) throws ConversionException {
        try {
            switch (fieldTypeName) {
                case INTEGER:
                case BIGINT:
                case SMALLINT:
                case TINYINT:
                    // If the string has a decimal part, parse as Double to allow decimal-adjust logic
                    if (strValue.contains(".")) {
                        return Double.valueOf(strValue);
                    }
                    if (fieldTypeName == SqlTypeName.BIGINT) {
                        return Long.valueOf(strValue);
                    }
                    return Integer.valueOf(strValue);
                case DOUBLE:
                    return Double.valueOf(strValue);
                case FLOAT:
                case REAL:
                    return Float.valueOf(strValue);
                case DECIMAL:
                    return new BigDecimal(strValue);
                default:
                    return Double.valueOf(strValue);
            }
        } catch (NumberFormatException e) {
            throw new ConversionException(
                "Failed to coerce value '" + strValue + "' to numeric type " + fieldTypeName + ": " + e.getMessage()
            );
        }
    }

    /** Returns true if the SqlTypeName represents a date/timestamp family type. */
    private boolean isDateType(SqlTypeName typeName) {
        return typeName == SqlTypeName.TIMESTAMP
            || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            || typeName == SqlTypeName.DATE
            || typeName == SqlTypeName.TIME;
    }

    /** Returns true if the SqlTypeName represents a numeric type. */
    private boolean isNumericType(SqlTypeName typeName) {
        return typeName == SqlTypeName.INTEGER
            || typeName == SqlTypeName.BIGINT
            || typeName == SqlTypeName.SMALLINT
            || typeName == SqlTypeName.TINYINT
            || typeName == SqlTypeName.DOUBLE
            || typeName == SqlTypeName.FLOAT
            || typeName == SqlTypeName.REAL
            || typeName == SqlTypeName.DECIMAL;
    }

    /**
     * Wrapper to distinguish string-coerced numbers from raw numbers in createLiteral.
     * Coerced values are built via makeLiteral with the field's type (Calcite canonically
     * types exact-numeric literals as DECIMAL), never as TIMESTAMP.
     */
    private static class CoercedNumber {
        final Number value;

        CoercedNumber(Number value) {
            this.value = value;
        }
    }

    // ========== Decimal bounds on integer fields ==========
    // Replicates NumberFieldMapper INTEGER.rangeQuery truncate+adjust semantics.

    /** Returns true if the SqlTypeName represents an integer-family type (not float/double/decimal). */
    private boolean isIntegerType(SqlTypeName typeName) {
        return typeName == SqlTypeName.INTEGER
            || typeName == SqlTypeName.BIGINT
            || typeName == SqlTypeName.SMALLINT
            || typeName == SqlTypeName.TINYINT;
    }

    /**
     * Returns true if the numeric value has a non-zero fractional part.
     * Mirrors legacy NumberFieldMapper.hasDecimalPart.
     * Accepts raw Number instances and CoercedNumber wrappers (from string-to-number coercion).
     */
    private boolean hasDecimalPart(Object value) {
        if (value instanceof CoercedNumber) {
            double d = ((CoercedNumber) value).value.doubleValue();
            return d % 1 != 0;
        }
        if (value instanceof Number) {
            double d = ((Number) value).doubleValue();
            return d % 1 != 0;
        }
        return false;
    }

    /**
     * Returns the signum (-1, 0, or 1) of a numeric value.
     * Mirrors legacy NumberFieldMapper.signum.
     * Accepts raw Number instances and CoercedNumber wrappers.
     */
    private double signum(Object value) {
        if (value instanceof CoercedNumber) {
            return Math.signum(((CoercedNumber) value).value.doubleValue());
        }
        if (value instanceof Number) {
            return Math.signum(((Number) value).doubleValue());
        }
        return 0;
    }

    /**
     * Truncates a numeric value to long (floor toward zero), supporting all integer family widths.
     * Used as the base truncation before narrowing to the specific integer type.
     * Accepts raw Number instances and CoercedNumber wrappers.
     */
    private long toLongValue(Object value) {
        if (value instanceof CoercedNumber) {
            return ((CoercedNumber) value).value.longValue();
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0;
    }

    /**
     * Narrows a long value to the appropriate Java type for the given SqlTypeName.
     * INTEGER/SMALLINT/TINYINT -> Integer; BIGINT -> Long.
     */
    private Number narrowToFieldType(long value, SqlTypeName typeName) {
        if (typeName == SqlTypeName.BIGINT) {
            return value;
        }
        return (int) value;
    }

    /**
     * Returns the maximum value for the given integer-family SqlTypeName.
     * Used for overflow guard checks before incrementing truncated values.
     */
    private long getMaxValueForType(SqlTypeName typeName) {
        switch (typeName) {
            case BIGINT:
                return Long.MAX_VALUE;
            case INTEGER:
                return Integer.MAX_VALUE;
            case SMALLINT:
                return Short.MAX_VALUE;
            case TINYINT:
                return Byte.MAX_VALUE;
            default:
                return Long.MAX_VALUE;
        }
    }

    /**
     * Returns the minimum value for the given integer-family SqlTypeName.
     * Used for overflow guard checks before decrementing truncated values.
     */
    private long getMinValueForType(SqlTypeName typeName) {
        switch (typeName) {
            case BIGINT:
                return Long.MIN_VALUE;
            case INTEGER:
                return Integer.MIN_VALUE;
            case SMALLINT:
                return Short.MIN_VALUE;
            case TINYINT:
                return Byte.MIN_VALUE;
            default:
                return Long.MIN_VALUE;
        }
    }
}
