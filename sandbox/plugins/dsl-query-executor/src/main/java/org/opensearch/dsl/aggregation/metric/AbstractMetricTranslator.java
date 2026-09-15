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
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.Collections;

/**
 * Base class for metric translators. Provides the common {@link #toAggregateCall}
 * logic — subclasses supply the SQL aggregate function, field name, and optionally
 * override the return type.
 */
public abstract class AbstractMetricTranslator<T extends ValuesSourceAggregationBuilder<T>> implements MetricTranslator<T> {

    /** Creates a metric translator. */
    protected AbstractMetricTranslator() {}

    /**
     * Rejects {@code missing} (substitutes a value for docs lacking the field) and
     * {@code script} (computes the metric input from a script): the translation implements
     * neither — it emits plain {@code fn(field)}, whose result differs from classic search
     * when either parameter is present.
     */
    @Override
    public void validate(T agg) throws ConversionException {
        if (agg.missing() != null) {
            throw new ConversionException(
                "[missing] on metric aggregation [" + agg.getName() + "] is not supported by the DSL execution path"
            );
        }
        if (agg.script() != null) {
            throw new ConversionException(
                "[script] on metric aggregation [" + agg.getName() + "] is not supported by the DSL execution path"
            );
        }
    }

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

        // Calcite enforces the return type to be same as input type; eg: AVG int→double coercion happens in response layer.
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

    /**
     * Coerces an engine result cell to double. Calcite keeps the input column type (AVG over
     * an INTEGER column returns an integral value), so the int→double widening happens here.
     *
     * @param value the raw cell value (must be a {@link Number})
     */
    protected static double toDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        throw new IllegalStateException(
            "Expected numeric aggregation result but got " + (value == null ? "null" : value.getClass().getSimpleName())
        );
    }
}
