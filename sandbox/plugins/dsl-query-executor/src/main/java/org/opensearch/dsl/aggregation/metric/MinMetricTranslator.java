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
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;

import java.util.Map;
import java.util.function.Supplier;

/** Translates MIN metric aggregation to Calcite. */
public class MinMetricTranslator extends AbstractMetricTranslator<MinAggregationBuilder> {

    /**
     * Creates a MIN metric translator.
     *
     * @param mapperServiceSupplier supplies the target index's MapperService for value format resolution
     */
    public MinMetricTranslator(Supplier<MapperService> mapperServiceSupplier) {
        super(mapperServiceSupplier);
    }

    @Override
    public Class<MinAggregationBuilder> getAggregationType() {
        return MinAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.MIN;
    }

    @Override
    protected String getFieldName(MinAggregationBuilder agg) {
        return agg.field();
    }

    /** Null (no matching docs) becomes +Infinity — legacy sentinel, rendered as {@code "value": null}. */
    @Override
    public InternalAggregation toInternalAggregation(MinAggregationBuilder agg, Map<String, Object> values) {
        Object value = singleValue(agg, values);
        double min = value == null ? Double.POSITIVE_INFINITY : toDouble(value);
        return new InternalMin(agg.getName(), min, resolveFormat(agg), AggregationTranslator.userMetadata(agg));
    }
}
