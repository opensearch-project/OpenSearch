/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.dsl.aggregation.metric.StatsMetricTranslator.COUNT_SUFFIX;
import static org.opensearch.dsl.aggregation.metric.StatsMetricTranslator.MAX_SUFFIX;
import static org.opensearch.dsl.aggregation.metric.StatsMetricTranslator.MIN_SUFFIX;
import static org.opensearch.dsl.aggregation.metric.StatsMetricTranslator.SUM_SUFFIX;
import static org.opensearch.dsl.aggregation.metric.StatsMetricTranslator.createCall;
import static org.opensearch.dsl.aggregation.metric.StatsMetricTranslator.doubleValue;
import static org.opensearch.dsl.aggregation.metric.StatsMetricTranslator.longValue;

/**
 * Translator for the extended_stats aggregation: the four stats calls plus VAR_POP.
 * sum_of_squares is derived from population variance (VAR_POP = E[x²] − mean², so
 * SS = (variance + avg²) × count) because the engine has no SUM(x²) aggregate.
 * avg, std_deviation, and std_deviation_bounds are derived by
 * {@link InternalExtendedStats} from count/sum/variance and the request's sigma.
 */
public class ExtendedStatsMetricTranslator implements MetricTranslator<ExtendedStatsAggregationBuilder> {

    static final String VARIANCE_SUFFIX = "_variance";

    /** Creates an extended_stats metric translator. */
    public ExtendedStatsMetricTranslator() {}

    @Override
    public Class<ExtendedStatsAggregationBuilder> getAggregationType() {
        return ExtendedStatsAggregationBuilder.class;
    }

    @Override
    public List<AggregateCall> toAggregateCalls(ExtendedStatsAggregationBuilder agg, RelDataType rowType) throws ConversionException {
        String fieldName = agg.field();
        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Aggregation field '" + fieldName + "' not found in schema");
        }
        String baseName = agg.getName();

        List<AggregateCall> calls = new ArrayList<>(5);
        calls.add(createCall(SqlStdOperatorTable.COUNT, field, baseName + COUNT_SUFFIX));
        calls.add(createCall(SqlStdOperatorTable.MIN, field, baseName + MIN_SUFFIX));
        calls.add(createCall(SqlStdOperatorTable.MAX, field, baseName + MAX_SUFFIX));
        calls.add(createCall(SqlStdOperatorTable.SUM, field, baseName + SUM_SUFFIX));
        calls.add(createCall(SqlStdOperatorTable.VAR_POP, field, baseName + VARIANCE_SUFFIX));
        return calls;
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
    public InternalAggregation toInternalAggregation(ExtendedStatsAggregationBuilder agg, Map<String, Object> values) {
        String name = agg.getName();
        double sigma = agg.sigma();
        long count = longValue(values, name + COUNT_SUFFIX);
        double min = doubleValue(values, name + MIN_SUFFIX, Double.POSITIVE_INFINITY);
        double max = doubleValue(values, name + MAX_SUFFIX, Double.NEGATIVE_INFINITY);
        double sum = doubleValue(values, name + SUM_SUFFIX, 0.0);
        double variance = doubleValue(values, name + VARIANCE_SUFFIX, 0.0);

        double avg = count > 0 ? sum / count : 0;
        double sumOfSquares = count > 0 ? (variance + avg * avg) * count : 0;

        return new InternalExtendedStats(name, count, sum, min, max, sumOfSquares, sigma, DocValueFormat.RAW, null);
    }
}
