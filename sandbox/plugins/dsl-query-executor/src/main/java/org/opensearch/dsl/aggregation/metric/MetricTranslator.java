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

/**
 * Translates a metric aggregation (AVG, SUM, MIN, MAX, etc.) to a Calcite AggregateCall,
 * and converts raw result values back to OpenSearch InternalAggregation for response building.
 */
public interface MetricTranslator<T extends AggregationBuilder> extends AggregationType<T> {

    /**
     * Converts the metric aggregation to a Calcite AggregateCall.
     *
     * @param agg the metric aggregation builder
     * @param rowType the index row type for field lookup
     * @return the Calcite AggregateCall
     * @throws ConversionException if conversion fails
     */
    AggregateCall toAggregateCall(T agg, RelDataType rowType) throws ConversionException;

    /**
     * Returns the output field name for this aggregation.
     *
     * @param agg the metric aggregation builder
     * @return the aggregate field name
     */
    String getAggregateFieldName(T agg);

    /**
     * Converts a raw result value from execution into an OpenSearch InternalAggregation.
     *
     * @param name the aggregation name
     * @param value the raw value (may be null)
     * @return the corresponding InternalAggregation
     */
    InternalAggregation toInternalAggregation(String name, Object value);
}
