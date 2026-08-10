/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link RangeQueryBuilder} to Calcite comparison RexNodes.
 * Supports gte, gt, lte, lt operators, format, time_zone, and relation parameters.
 * Implements date math expressions, inclusivity-keyed rounding, and millisecond/nanosecond precision.
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
 * <p>
 * IP fields use 16-byte IPv6-mapped sortable encoding matching legacy
 * {@code IpFieldMapper.rangeQuery} with {@code InetAddressPoint.encode} byte ordering.
 */
public class RangeQueryTranslator implements QueryTranslator {

    private static final TranslatorMapperRegistry REGISTRY = TranslatorMapperRegistry.INSTANCE;

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
     * - Nanosecond precision timestamps for date_nanos (TIMESTAMP(9))
     * - IP field range with InetAddress-order byte comparison
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
     * - TIMESTAMP/DATE and IP fields: delegated to per-type mapper classes via the TranslatorMapperRegistry
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

        if (rangeQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Range query 'boost' parameter is not supported");
        }

        if (rangeQuery.queryName() != null) {
            throw new ConversionException("Range query '_name' parameter is not supported");
        }

        // Relation check: per legacy SimpleMappedFieldType.rangeQuery() and DateFieldType.rangeQuery(),
        // DISJOINT is rejected; INTERSECTS/CONTAINS/WITHIN are silently ignored (scalar fields produce
        // identical queries regardless of relation). Note: RangeQueryBuilder itself rejects DISJOINT
        // at builder level, so this is a defensive check.
        if (rangeQuery.relation() == ShapeRelation.DISJOINT) {
            throw new ConversionException("Range query 'relation' parameter does not support DISJOINT");
        }

        // Unmapped field -> match-none (literal false), matching legacy DISJOINT for null fieldType.
        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            return ctx.getRexBuilder().makeLiteral(false);
        }

        SqlTypeName fieldTypeName = field.getType().getSqlTypeName();
        int fieldPrecision = field.getType().getPrecision();
        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        List<RexNode> conditions = new ArrayList<>();

        String format = rangeQuery.format();
        String timeZone = rangeQuery.timeZone();
        boolean isDateField = RangeDateParsing.isDateType(fieldTypeName);

        // Lower bound: rounding keyed on inclusivity per DateFieldMapper.dateRangeQuery
        if (rangeQuery.from() != null) {
            // For date fields, the mapper owns parsing - pass raw value with format/timeZone.
            // For non-date fields, pre-process here as before (numeric coercion, string passthrough).
            Object fromValue = isDateField
                ? rangeQuery.from()
                : processValue(rangeQuery.from(), format, timeZone, !rangeQuery.includeLower(), fieldTypeName, fieldPrecision);
            RexNode bound = translateBound(fromValue, true, rangeQuery.includeLower(), format, timeZone, field, ctx);
            if (bound != null) {
                if (bound instanceof RexLiteral && Boolean.FALSE.equals(((RexLiteral) bound).getValueAs(Boolean.class))) {
                    return bound; // overflow guard: match-none
                }
                conditions.add(bound);
            }
        }

        // Upper bound: rounding keyed on inclusivity per DateFieldMapper.dateRangeQuery
        if (rangeQuery.to() != null) {
            Object toValue = isDateField
                ? rangeQuery.to()
                : processValue(rangeQuery.to(), format, timeZone, rangeQuery.includeUpper(), fieldTypeName, fieldPrecision);
            RexNode bound = translateBound(toValue, false, rangeQuery.includeUpper(), format, timeZone, field, ctx);
            if (bound != null) {
                if (bound instanceof RexLiteral && Boolean.FALSE.equals(((RexLiteral) bound).getValueAs(Boolean.class))) {
                    return bound; // overflow guard: match-none
                }
                conditions.add(bound);
            }
        }

        // No bounds -> IS_NOT_NULL (exists semantics), matching legacy RangeQueryBuilder.doToQuery
        // which rewrites to ExistsQueryBuilder when both from and to are null.
        if (conditions.isEmpty()) {
            return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.IS_NOT_NULL, fieldRef);
        }

        return conditions.size() == 1 ? conditions.get(0) : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, conditions);
    }

    /**
     * Translates a single range bound (lower or upper) into a comparison RexNode.
     * Applies decimal truncation and overflow guards for integer-typed fields per legacy
     * {@code NumberFieldMapper.NumberType.INTEGER.rangeQuery} semantics.
     *
     * @param value the processed bound value (may be null)
     * @param isLower true if this is the lower bound, false for upper
     * @param inclusive true if the original bound is inclusive (gte/lte vs gt/lt)
     * @param format optional date format from the range query
     * @param timeZone optional time zone from the range query
     * @param field the field definition from the schema
     * @param ctx the conversion context
     * @return RexNode comparison, literal false for overflow match-none, or null if value is null
     */
    private RexNode translateBound(
        Object value,
        boolean isLower,
        boolean inclusive,
        String format,
        String timeZone,
        RelDataTypeField field,
        ConversionContext ctx
    ) throws ConversionException {
        if (value == null) {
            return null;
        }

        // Delegate remaining types (including scaled_float, unsigned_long via tier 1,
        // integer decimal-adjust, whole-integer, and generic tail) to the registry-resolved
        // mapper. The catch-all DefaultTranslatorMapper carries the entire non-UDT path
        // including VARCHAR/CHAR keyword ranges.
        return REGISTRY.resolve(field.getType()).translateBound(new BoundRequest(value, isLower, inclusive, format, timeZone, field, ctx));
    }

    /**
     * Processes a range query value, with behavior gated on context and field type.
     * <p>
     * Processing logic:
     * - If format or timeZone is specified, or the string is a date-math expression
     *   (starts with "now" or contains "||"), it is always parsed as a date value.
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
     * @param fieldPrecision the precision of the field (3 for date/millis, 9 for date_nanos)
     * @return processed value as epoch milliseconds (Long) for date, epoch nanos (Long) for date_nanos,
     *         Number for string-to-number coercion, or original value
     * @throws ConversionException if date parsing fails or numeric coercion fails
     */
    private Object processValue(
        Object value,
        String format,
        String timeZone,
        boolean roundUp,
        SqlTypeName fieldTypeName,
        int fieldPrecision
    ) throws ConversionException {
        if (value == null) {
            return null;
        }

        if (!(value instanceof String)) {
            return value;
        }

        String strValue = (String) value;

        // If format/timeZone specified or value is date-math, always date-parse
        if (format != null || timeZone != null || RangeDateParsing.isDateMathExpression(strValue)) {
            return RangeDateParsing.parseDateValue(strValue, format, timeZone, roundUp, resolutionFor(fieldPrecision));
        }

        // Gate on field type
        if (RangeBoundMath.isNumericType(fieldTypeName)) {
            return coerceToNumber(strValue, fieldTypeName);
        } else {
            // VARCHAR/CHAR and other types: keep string as-is for lexicographic comparison
            return strValue;
        }
    }

    /**
     * Coerces a string value to a numeric type matching the field.
     * For integer-family types, if the string contains a decimal point, it is parsed as a
     * Double so the decimal-adjust logic in convert() applies.
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

    /** OpenSearch maps {@code date} to TIMESTAMP(3) and {@code date_nanos} to TIMESTAMP(9) (see OpenSearchSchemaBuilder.mapFieldType); any precision above 3 is nanosecond resolution. */
    private static boolean isNanoPrecision(int precision) {
        return precision > 3;
    }

    /** Selects the date resolution matching the field precision. */
    private static RangeDateParsing.DateResolution resolutionFor(int precision) {
        return isNanoPrecision(precision) ? RangeDateParsing.DateResolution.NANOSECONDS : RangeDateParsing.DateResolution.MILLISECONDS;
    }

}
