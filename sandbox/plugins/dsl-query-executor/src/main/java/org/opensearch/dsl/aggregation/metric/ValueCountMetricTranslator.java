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
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;

import java.util.Map;

/**
 * Translator for value_count aggregation.
 * Counts the number of values for a field.
 */
public class ValueCountMetricTranslator extends AbstractMetricTranslator<ValueCountAggregationBuilder> {

    /** Creates a value_count metric translator. */
    public ValueCountMetricTranslator() {}

    @Override
    public Class<ValueCountAggregationBuilder> getAggregationType() {
        return ValueCountAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.COUNT;
    }

    @Override
    protected String getFieldName(ValueCountAggregationBuilder agg) {
        return agg.field();
    }

    /** Null (no matching docs) becomes 0 — a count over nothing is zero. */
    @Override
    public InternalAggregation toInternalAggregation(ValueCountAggregationBuilder agg, Map<String, Object> values) {
        Object value = singleValue(agg, values);
        long count = value == null ? 0 : ((Number) value).longValue();
        return new InternalValueCount(agg.getName(), count, null);
    }
}
