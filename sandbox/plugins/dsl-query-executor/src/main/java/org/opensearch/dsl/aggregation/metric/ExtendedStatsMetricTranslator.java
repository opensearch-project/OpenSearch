/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Translator for extended_stats aggregation.
 * Extended stats produces: count, min, max, sum, sum_of_squares, variance, std_deviation, std_deviation_bounds.
 * Note: avg, stddev are not computed in Calcite - InternalExtendedStats calculates them dynamically.
 * We compute variance to derive sum_of_squares (Calcite lacks SUM(x²) function).
 */
public class ExtendedStatsMetricTranslator implements MetricTranslator<ExtendedStatsAggregationBuilder> {

    private static final int METRIC_COUNT = 5;
    private static final double DEFAULT_SIGMA = 2.0;
    private static final String COUNT_SUFFIX = "_count";
    private static final String MIN_SUFFIX = "_min";
    private static final String MAX_SUFFIX = "_max";
    private static final String SUM_SUFFIX = "_sum";
    private static final String VARIANCE_SUFFIX = "_variance";

    @Override
    public Class<ExtendedStatsAggregationBuilder> getAggregationType() {
        return ExtendedStatsAggregationBuilder.class;
    }

    @Override
    public List<AggregateCall> toAggregateCalls(ExtendedStatsAggregationBuilder agg, RelDataType rowType)
            throws ConversionException {
        String fieldName = agg.field();
        RelDataTypeField field = rowType.getField(fieldName, true, false);
        if (field == null) {
            throw new ConversionException("Field '" + fieldName + "' not found");
        }
        int fieldIndex = field.getIndex();
        String baseName = agg.getName();

        List<AggregateCall> calls = new ArrayList<>(METRIC_COUNT);

        calls.add(createAggregateCall(SqlStdOperatorTable.COUNT, Collections.singletonList(fieldIndex),
            field.getType(), baseName + COUNT_SUFFIX));
        calls.add(createAggregateCall(SqlStdOperatorTable.MIN, Collections.singletonList(fieldIndex),
            field.getType(), baseName + MIN_SUFFIX));
        calls.add(createAggregateCall(SqlStdOperatorTable.MAX, Collections.singletonList(fieldIndex),
            field.getType(), baseName + MAX_SUFFIX));
        calls.add(createAggregateCall(SqlStdOperatorTable.SUM, Collections.singletonList(fieldIndex),
            field.getType(), baseName + SUM_SUFFIX));
        calls.add(createAggregateCall(SqlStdOperatorTable.VAR_POP, Collections.singletonList(fieldIndex),
            field.getType(), baseName + VARIANCE_SUFFIX));

        return calls;
    }

    private AggregateCall createAggregateCall(SqlAggFunction function, List<Integer> argList,
            RelDataType type, String name) {
        return AggregateCall.create(
            function,
            false, false, true,
            argList,
            -1,
            RelCollations.EMPTY,
            type,
            name
        );
    }

    @Override
    public List<String> getAggregateFieldNames(ExtendedStatsAggregationBuilder agg) {
        String baseName = agg.getName();
        return List.of(
            baseName + COUNT_SUFFIX,
            baseName + MIN_SUFFIX,
            baseName + MAX_SUFFIX,
            baseName + SUM_SUFFIX,
            baseName + VARIANCE_SUFFIX
        );
    }

    @Override
    public InternalAggregation toInternalAggregation(String name, Map<String, Object> values) {
        if (values == null) {
            return new InternalExtendedStats(name, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, DEFAULT_SIGMA, DocValueFormat.RAW, Map.of());
        }
        String baseName = name;
        long count = values.get(baseName + COUNT_SUFFIX) != null ? ((Number) values.get(baseName + COUNT_SUFFIX)).longValue() : 0;
        double min = values.get(baseName + MIN_SUFFIX) != null ? ((Number) values.get(baseName + MIN_SUFFIX)).doubleValue() : Double.POSITIVE_INFINITY;
        double max = values.get(baseName + MAX_SUFFIX) != null ? ((Number) values.get(baseName + MAX_SUFFIX)).doubleValue() : Double.NEGATIVE_INFINITY;
        double sum = values.get(baseName + SUM_SUFFIX) != null ? ((Number) values.get(baseName + SUM_SUFFIX)).doubleValue() : 0;
        double variance = values.get(baseName + VARIANCE_SUFFIX) != null ? ((Number) values.get(baseName + VARIANCE_SUFFIX)).doubleValue() : 0;

        double avg = count > 0 ? sum / count : 0;
        double sumOfSquares = count > 0 ? (variance * count) + (avg * avg * count) : 0;

        return new InternalExtendedStats(name, count, sum, min, max, sumOfSquares, DEFAULT_SIGMA, DocValueFormat.RAW, Map.of());
    }
}
