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
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;

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
}
