/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

import java.util.Collections;

/**
 * Base class for metric translators. Provides the common {@link #toAggregateCall}
 * logic — subclasses supply the SQL aggregate function, field name, and optionally
 * override the return type.
 */
public abstract class AbstractMetricTranslator<T extends AggregationBuilder> implements MetricTranslator<T> {

    /** Creates a metric translator. */
    protected AbstractMetricTranslator() {}

    /** Returns the SQL aggregate function (e.g., AVG, SUM, MIN, MAX). */
    protected abstract SqlAggFunction getAggFunction();

    /**
     * Returns the field name from the aggregation builder.
     *
     * @param agg the aggregation builder
     * @return the field name
     */
    protected abstract String getFieldName(T agg);

    @Override
    public AggregateCall toAggregateCall(T agg, RelDataType rowType) throws ConversionException {
        String fieldName = getFieldName(agg);
        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Aggregation field '" + fieldName + "' not found in schema");
        }

        return AggregateCall.create(
            getAggFunction(),
            false,
            false,
            false,
            Collections.singletonList(field.getIndex()),
            -1,
            RelCollations.EMPTY,
            field.getType(),
            agg.getName()
        );
    }

    @Override
    public String getAggregateFieldName(T agg) {
        return agg.getName();
    }

    // TODO: implement response conversion per metric type (InternalAvg, InternalSum, etc.)
    @Override
    public InternalAggregation toInternalAggregation(String name, Object value) {
        throw new UnsupportedOperationException("toInternalAggregation not yet implemented for " + getClass().getSimpleName());
    }
}
