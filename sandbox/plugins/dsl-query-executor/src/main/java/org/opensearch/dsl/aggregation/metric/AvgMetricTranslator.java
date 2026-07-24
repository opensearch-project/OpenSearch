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
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;

import java.util.Map;

/** Translates AVG metric aggregation to Calcite. */
public class AvgMetricTranslator extends AbstractMetricTranslator<AvgAggregationBuilder> {

    /** Creates an AVG metric translator. */
    public AvgMetricTranslator() {}

    @Override
    public Class<AvgAggregationBuilder> getAggregationType() {
        return AvgAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.AVG;
    }

    @Override
    protected String getFieldName(AvgAggregationBuilder agg) {
        return agg.field();
    }

    /**
     * The engine returns the final average; encoded as (sum=value, count=1) so
     * {@code InternalAvg.getValue()} reproduces it. Null (no matching docs) uses
     * count=0, which renders as {@code "value": null} like legacy.
     */
    @Override
    public InternalAggregation toInternalAggregation(AvgAggregationBuilder agg, Map<String, Object> values) {
        Object value = singleValue(agg, values);
        if (value == null) {
            return new InternalAvg(agg.getName(), 0.0, 0, DocValueFormat.RAW, null);
        }
        return new InternalAvg(agg.getName(), toDouble(value), 1, DocValueFormat.RAW, null);
    }
}
