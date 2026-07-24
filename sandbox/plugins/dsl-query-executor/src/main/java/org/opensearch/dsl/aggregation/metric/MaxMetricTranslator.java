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
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;

import java.util.Map;

/** Translates MAX metric aggregation to Calcite. */
public class MaxMetricTranslator extends AbstractMetricTranslator<MaxAggregationBuilder> {

    /** Creates a MAX metric translator. */
    public MaxMetricTranslator() {}

    @Override
    public Class<MaxAggregationBuilder> getAggregationType() {
        return MaxAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.MAX;
    }

    @Override
    protected String getFieldName(MaxAggregationBuilder agg) {
        return agg.field();
    }

    /** Null (no matching docs) becomes -Infinity — legacy sentinel, rendered as {@code "value": null}. */
    @Override
    public InternalAggregation toInternalAggregation(MaxAggregationBuilder agg, Map<String, Object> values) {
        Object value = singleValue(agg, values);
        double max = value == null ? Double.NEGATIVE_INFINITY : toDouble(value);
        return new InternalMax(agg.getName(), max, DocValueFormat.RAW, null);
    }
}
