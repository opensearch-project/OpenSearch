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
 * Translator for the stats aggregation. One request node fans out to four SQL aggregate
 * calls (COUNT, MIN, MAX, SUM), output columns named {@code <aggName>_<suffix>}.
 * avg is not computed by the engine — {@link InternalStats} derives it as sum/count.
 */
public class StatsMetricTranslator implements MetricTranslator<StatsAggregationBuilder> {

    static final String COUNT_SUFFIX = "_count";
    static final String MIN_SUFFIX = "_min";
    static final String MAX_SUFFIX = "_max";
    static final String SUM_SUFFIX = "_sum";

    /** Creates a stats metric translator. */
    public StatsMetricTranslator() {}

    @Override
    public Class<StatsAggregationBuilder> getAggregationType() {
        return StatsAggregationBuilder.class;
    }

    @Override
    public List<AggregateCall> toAggregateCalls(StatsAggregationBuilder agg, RelDataType rowType) throws ConversionException {
        String fieldName = agg.field();
        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Aggregation field '" + fieldName + "' not found in schema");
        }
        String baseName = agg.getName();

        List<AggregateCall> calls = new ArrayList<>(4);
        calls.add(createCall(SqlStdOperatorTable.COUNT, field, baseName + COUNT_SUFFIX));
        calls.add(createCall(SqlStdOperatorTable.MIN, field, baseName + MIN_SUFFIX));
        calls.add(createCall(SqlStdOperatorTable.MAX, field, baseName + MAX_SUFFIX));
        calls.add(createCall(SqlStdOperatorTable.SUM, field, baseName + SUM_SUFFIX));
        return calls;
    }

    static AggregateCall createCall(SqlAggFunction function, RelDataTypeField field, String name) {
        // Calcite enforces the return type to be same as input type; numeric widening happens in the response layer.
        return AggregateCall.create(
            function,
            false,
            false,
            false,
            Collections.singletonList(field.getIndex()),
            -1,
            RelCollations.EMPTY,
            field.getType(),
            name
        );
    }

    @Override
    public List<String> getAggregateFieldNames(StatsAggregationBuilder agg) {
        String baseName = agg.getName();
        return List.of(baseName + COUNT_SUFFIX, baseName + MIN_SUFFIX, baseName + MAX_SUFFIX, baseName + SUM_SUFFIX);
    }

    @Override
    public InternalAggregation toInternalAggregation(StatsAggregationBuilder agg, Map<String, Object> values) {
        String name = agg.getName();
        long count = longValue(values, name + COUNT_SUFFIX);
        double min = doubleValue(values, name + MIN_SUFFIX, Double.POSITIVE_INFINITY);
        double max = doubleValue(values, name + MAX_SUFFIX, Double.NEGATIVE_INFINITY);
        double sum = doubleValue(values, name + SUM_SUFFIX, 0.0);
        return new InternalStats(name, count, sum, min, max, DocValueFormat.RAW, null);
    }

    /** Reads a long cell; missing map or SQL NULL degrades to 0 (empty-set count). */
    static long longValue(Map<String, Object> values, String key) {
        Object value = values == null ? null : values.get(key);
        return value == null ? 0L : ((Number) value).longValue();
    }

    /** Reads a double cell; missing map or SQL NULL degrades to the empty-set sentinel. */
    static double doubleValue(Map<String, Object> values, String key, double emptyValue) {
        Object value = values == null ? null : values.get(key);
        return value == null ? emptyValue : ((Number) value).doubleValue();
    }
}
