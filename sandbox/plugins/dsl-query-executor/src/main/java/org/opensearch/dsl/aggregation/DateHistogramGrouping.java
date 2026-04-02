/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.util.List;

public class DateHistogramGrouping implements ExpressionGrouping {

    private static final String BUCKET_SUFFIX = "date_histogram_bucket";

    private final String aggregationName;
    private final String fieldName;
    private final DateHistogramInterval calendarInterval;
    private final DateHistogramInterval fixedInterval;
    private final String uniqueId;

    public DateHistogramGrouping(String aggregationName, String fieldName, DateHistogramInterval calendarInterval,
                                  DateHistogramInterval fixedInterval) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
        this.calendarInterval = calendarInterval;
        this.fixedInterval = fixedInterval;

        String intervalStr = calendarInterval != null ? calendarInterval.toString() : fixedInterval.toString();
        String key = aggregationName + ":" + fieldName + ":" + intervalStr;
        this.uniqueId = Integer.toHexString(key.hashCode());
    }

    @Override
    public List<String> getFieldNames() {
        return List.of(fieldName);
    }

    @Override
    public String getProjectedColumnName() {
        return aggregationName + "$$" + fieldName + "$$" + uniqueId + "$$" + BUCKET_SUFFIX;
    }

    @Override
    public RexNode buildExpression(RelDataType inputRowType, RexBuilder builder) throws ConversionException {
        RelDataTypeField field = inputRowType.getField(fieldName, true, false);
        if (field == null) {
            throw new ConversionException("Field not found: " + fieldName);
        }

        RexNode fieldRef = builder.makeInputRef(field.getType(), field.getIndex());

        if (calendarInterval != null) {
            return buildCalendarExpression(fieldRef, builder);
        } else {
            return buildFixedExpression(fieldRef, builder);
        }
    }

    private RexNode buildCalendarExpression(RexNode fieldRef, RexBuilder builder) {
        TimeUnit unit = mapToTimeUnit(calendarInterval.toString());
        return builder.makeCall(SqlStdOperatorTable.FLOOR, fieldRef, builder.makeFlag(unit));
    }

    private RexNode buildFixedExpression(RexNode fieldRef, RexBuilder builder) {
        long intervalMs = parseFixedInterval(fixedInterval.toString());
        RexNode intervalLiteral = builder.makeBigintLiteral(java.math.BigDecimal.valueOf(intervalMs));

        RexNode divided = builder.makeCall(SqlStdOperatorTable.DIVIDE, fieldRef, intervalLiteral);
        RexNode floored = builder.makeCall(SqlStdOperatorTable.FLOOR, divided);
        return builder.makeCall(SqlStdOperatorTable.MULTIPLY, floored, intervalLiteral);
    }

    private TimeUnit mapToTimeUnit(String interval) {
        return switch (interval) {
            case "1y" -> TimeUnit.YEAR;
            case "1M" -> TimeUnit.MONTH;
            case "1w" -> TimeUnit.WEEK;
            case "1d" -> TimeUnit.DAY;
            case "1h" -> TimeUnit.HOUR;
            case "1m" -> TimeUnit.MINUTE;
            case "1s" -> TimeUnit.SECOND;
            default -> throw new IllegalArgumentException("Unsupported calendar interval: " + interval);
        };
    }

    private long parseFixedInterval(String interval) {
        String unit = interval.substring(interval.length() - 1);
        long value = Long.parseLong(interval.substring(0, interval.length() - 1));

        return switch (unit) {
            case "ms" -> value;
            case "s" -> value * 1000;
            case "m" -> value * 60 * 1000;
            case "h" -> value * 60 * 60 * 1000;
            case "d" -> value * 24 * 60 * 60 * 1000;
            default -> throw new IllegalArgumentException("Unsupported fixed interval: " + interval);
        };
    }
}
