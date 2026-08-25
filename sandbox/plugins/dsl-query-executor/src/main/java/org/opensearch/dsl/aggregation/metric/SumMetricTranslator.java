/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;

import java.util.Map;
import java.util.function.Supplier;

/** Translates SUM metric aggregation to Calcite. */
public class SumMetricTranslator extends AbstractMetricTranslator<SumAggregationBuilder> {

    /**
     * Creates a SUM metric translator.
     *
     * @param mapperServiceSupplier supplies the target index's MapperService for value format resolution
     */
    public SumMetricTranslator(Supplier<MapperService> mapperServiceSupplier) {
        super(mapperServiceSupplier);
    }

    @Override
    public Class<SumAggregationBuilder> getAggregationType() {
        return SumAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.SUM;
    }

    @Override
    protected String getFieldName(SumAggregationBuilder agg) {
        return agg.field();
    }

    /** Null (no matching docs) becomes 0.0 — legacy sum-of-nothing semantics. */
    @Override
    public InternalAggregation toInternalAggregation(SumAggregationBuilder agg, Map<String, Object> values) {
        Object value = singleValue(agg, values);
        double sum = value == null ? 0.0 : toDouble(value);
        return new InternalSum(agg.getName(), sum, resolveFormat(agg), AggregationTranslator.userMetadata(agg));
    }
}
