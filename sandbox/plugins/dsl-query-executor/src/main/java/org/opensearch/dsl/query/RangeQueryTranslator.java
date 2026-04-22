/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link RangeQueryBuilder} to Calcite comparison RexNodes.
 * Supports gte, gt, lte, lt operators, format, time_zone, and relation parameters.
 */
public class RangeQueryTranslator implements QueryTranslator {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return RangeQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        RangeQueryBuilder rangeQuery = (RangeQueryBuilder) query;
        String fieldName = rangeQuery.fieldName();

        if (rangeQuery.boost() != 1.0f) {
            throw new ConversionException("Range query 'boost' parameter is not supported in Calcite");
        }

        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Field '" + fieldName + "' not found in schema");
        }

        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        List<RexNode> conditions = new ArrayList<>();

        Object from = processValue(rangeQuery.from(), rangeQuery.format(), rangeQuery.timeZone(), false);
        if (from != null) {
            RexNode fromLiteral = createTimestampLiteral(from, field, ctx);
            conditions.add(
                ctx.getRexBuilder()
                    .makeCall(
                        rangeQuery.includeLower() ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.GREATER_THAN,
                        fieldRef,
                        fromLiteral
                    )
            );
        }

        boolean shouldRoundUp = !(rangeQuery.to() instanceof String && ((String) rangeQuery.to()).contains("/"));
        Object to = processValue(rangeQuery.to(), rangeQuery.format(), rangeQuery.timeZone(), shouldRoundUp);
        if (to != null) {
            RexNode toLiteral = createTimestampLiteral(to, field, ctx);
            conditions.add(
                ctx.getRexBuilder()
                    .makeCall(
                        rangeQuery.includeUpper() ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN,
                        fieldRef,
                        toLiteral
                    )
            );
        }

        if (conditions.isEmpty()) {
            throw new ConversionException("Range query must specify at least one bound (from/to)");
        }

        RexNode result = conditions.size() == 1 ? conditions.get(0) : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, conditions);

        if (rangeQuery.relation() != null && rangeQuery.relation() != ShapeRelation.INTERSECTS) {
            throw new ConversionException("Range query 'relation' parameter only supports INTERSECTS in Calcite");
        }

        return result;
    }

    private RexNode createTimestampLiteral(Object value, RelDataTypeField field, ConversionContext ctx) {
        if (value instanceof Long) {
            org.apache.calcite.rel.type.RelDataType timestampType = ctx.getRexBuilder()
                .getTypeFactory()
                .createSqlType(org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP, 3);
            return ctx.getRexBuilder().makeLiteral(value, timestampType, true);
        }
        return ctx.getRexBuilder().makeLiteral(value, field.getType(), true);
    }

    private Object processValue(Object value, String format, String timeZone, boolean roundUp) throws ConversionException {
        if (value == null) {
            return null;
        }

        if (!(value instanceof String)) {
            return value;
        }

        String strValue = (String) value;

        try {
            DateFormatter formatter = format != null
                ? DateFormatter.forPattern(format)
                : DateFormatter.forPattern("strict_date_optional_time");
            ZoneId zoneId = timeZone != null ? ZoneId.of(timeZone) : ZoneId.of("UTC");

            return formatter.toDateMathParser().parse(strValue, System::currentTimeMillis, roundUp, zoneId).toEpochMilli();
        } catch (Exception e) {
            throw new ConversionException("Failed to parse date value '" + strValue + "': " + e.getMessage());
        }
    }
}
