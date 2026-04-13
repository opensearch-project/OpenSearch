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
import org.opensearch.search.aggregations.metrics.InternalStats;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Translator for stats aggregation.
 * Stats produces multiple metrics: count, min, max, sum.
 * Note: avg is not computed in Calcite - InternalStats calculates it as sum/count.
 */
public class StatsMetricTranslator implements MetricTranslator<StatsAggregationBuilder> {

    private static final int METRIC_COUNT = 4;
    private static final String COUNT_SUFFIX = "_count";
    private static final String MIN_SUFFIX = "_min";
    private static final String MAX_SUFFIX = "_max";
    private static final String SUM_SUFFIX = "_sum";

    @Override
    public Class<StatsAggregationBuilder> getAggregationType() {
        return StatsAggregationBuilder.class;
    }

    @Override
    public List<AggregateCall> toAggregateCalls(StatsAggregationBuilder agg, RelDataType rowType)
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
    public List<String> getAggregateFieldNames(StatsAggregationBuilder agg) {
        String baseName = agg.getName();
        return List.of(
            baseName + COUNT_SUFFIX,
            baseName + MIN_SUFFIX,
            baseName + MAX_SUFFIX,
            baseName + SUM_SUFFIX
        );
    }

    @Override
    public InternalAggregation toInternalAggregation(String name, Map<String, Object> values) {
        if (values == null) {
            return new InternalStats(name, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, DocValueFormat.RAW, Map.of());
        }
        String baseName = name;
        long count = values.get(baseName + COUNT_SUFFIX) != null ? ((Number) values.get(baseName + COUNT_SUFFIX)).longValue() : 0;
        double min = values.get(baseName + MIN_SUFFIX) != null ? ((Number) values.get(baseName + MIN_SUFFIX)).doubleValue() : Double.POSITIVE_INFINITY;
        double max = values.get(baseName + MAX_SUFFIX) != null ? ((Number) values.get(baseName + MAX_SUFFIX)).doubleValue() : Double.NEGATIVE_INFINITY;
        double sum = values.get(baseName + SUM_SUFFIX) != null ? ((Number) values.get(baseName + SUM_SUFFIX)).doubleValue() : 0;

        return new InternalStats(name, count, sum, min, max, DocValueFormat.RAW, Map.of());
    }
}
