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
import org.opensearch.dsl.aggregation.AggregationType;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

import java.util.List;
import java.util.Map;

/**
 * Translates a metric aggregation to Calcite AggregateCall(s),
 * and converts raw result values back to OpenSearch InternalAggregation for response building.
 */
public interface MetricTranslator<T extends AggregationBuilder> extends AggregationType<T> {

    /**
     * Converts the metric aggregation to Calcite AggregateCall(s).
     *
     * @param agg the metric aggregation builder
     * @param rowType the index row type for field lookup
     * @return list of Calcite AggregateCalls
     * @throws ConversionException if conversion fails
     */
    List<AggregateCall> toAggregateCalls(T agg, RelDataType rowType) throws ConversionException;

    /**
     * Returns the output field names for this aggregation.
     *
     * @param agg the metric aggregation builder
     * @return list of aggregate field names
     */
    List<String> getAggregateFieldNames(T agg);

    /**
     * Converts raw result values from execution into an OpenSearch InternalAggregation.
     *
     * @param name the aggregation name
     * @param values map of field names to their computed values
     * @return the corresponding InternalAggregation
     */
    InternalAggregation toInternalAggregation(String name, Map<String, Object> values);
}
