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
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;

/** Translates SUM metric aggregation to Calcite. */
public class SumMetricTranslator extends AbstractMetricTranslator<SumAggregationBuilder> {

    /** Creates a SUM metric translator. */
    public SumMetricTranslator() {}

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
}
